import os
import re
import io
import json
import requests
import pandas as pd
import streamlit as st
from typing import Optional, Any
from dotenv import load_dotenv
from db import get_connection

load_dotenv()

ENV: str = "development"
USER_EMAIL: str = "test@example.com"
DEFAULT_GEMINI_URL: str = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

SYSTEM_KEYWORD_RULES: dict[str, list[str]] = {
    "엔진": ["엔진", "부조", "노킹", "진동", "소음", "터보", "부스트"],
    "점화": ["점화", "코일", "플러그", "미스파이어", "실화"],
    "연료": ["연료", "인젝터", "펌프", "연비", "시동지연"],
    "냉각": ["냉각", "수온", "과열", "냉각수", "라디에이터"],
    "배기": ["배기", "매니폴드", "촉매", "머플러", "매연"],
    "제동": ["브레이크", "제동", "abs", "패드", "디스크"],
    "공조": ["에어컨", "히터", "송풍", "냉방", "공조"],
    "변속기": ["변속", "미션", "기어", "변속충격", "슬립"],
    "전기충전": ["배터리", "충전", "알터네이터", "발전기", "전압", "크랭킹"],
    "조향현가": ["핸들", "조향", "현가", "하체", "쇼크", "얼라인먼트", "쏠림"],
    "시동 시스템": ["시동모터", "스타터"],
    "바디전장": ["도어", "창문", "와이퍼", "등화", "계기판", "스마트키"],
}
CONSUMABLE_PART_KEYWORDS = [
    "점화플러그", "엔진오일", "오일필터", "에어필터", "캐빈필터", "에어컨필터",
    "브레이크패드", "브레이크라이닝", "와이퍼", "냉각수", "미션오일", "부동액",
]


# ─────────────────────────────────────────────
# 1. PAGE CONFIG & GLOBAL CSS
# ─────────────────────────────────────────────

def set_page_config() -> None:
    st.set_page_config(
        page_title="CarCheck — 과잉정비 진단",
        page_icon="🔧",
        layout="centered",
        initial_sidebar_state="collapsed",
    )


def inject_global_css() -> None:
    st.markdown("""
    <style>
    @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');

    :root {
        --navy-900: #0a1628;
        --navy-800: #0f2044;
        --accent:        #2563eb;
        --danger:        #dc2626;
        --danger-light:  #fef2f2;
        --danger-border: #fca5a5;
        --success:        #15803d;
        --success-light:  #f0fdf4;
        --success-border: #86efac;
        --gray-50:  #f8fafc;
        --gray-100: #f1f5f9;
        --gray-200: #e2e8f0;
        --gray-300: #cbd5e1;
        --gray-400: #94a3b8;
        --gray-500: #64748b;
        --gray-700: #334155;
        --gray-900: #0f172a;
    }

    html, body, [class*="css"] {
        font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }
    #MainMenu, footer, header { visibility: hidden; }
    .block-container { padding-top: 0 !important; padding-bottom: 0 !important; }
    .stApp { background: var(--gray-50) !important; }

    .topbar {
        position: sticky; top: 0; z-index: 200;
        background: var(--navy-900);
        border-bottom: 1px solid rgba(255,255,255,0.07);
        display: flex; align-items: center; justify-content: space-between;
        padding: 0 28px; height: 56px;
        margin-left: -4rem; margin-right: -4rem;
    }
    .topbar-logo {
        display: flex; align-items: center; gap: 10px;
        color: #fff; font-size: 15px; font-weight: 700;
    }
    .topbar-logo-mark {
        width: 28px; height: 28px; background: var(--accent);
        border-radius: 7px; display: flex; align-items: center;
        justify-content: center; font-size: 14px;
    }
    .topbar-tag {
        font-size: 11px; font-weight: 600; letter-spacing: 0.5px;
        text-transform: uppercase; color: rgba(255,255,255,0.35);
        border: 1px solid rgba(255,255,255,0.12); padding: 4px 10px; border-radius: 4px;
    }

    .page-wrap { padding: 28px 0 72px; }

    /* 판정 배너 */
    .top-verdict {
        border-radius: 16px; padding: 24px 26px; margin-bottom: 14px;
        border: 2px solid; display: flex; align-items: center; gap: 18px;
    }
    .top-verdict.danger { background: var(--danger-light); border-color: var(--danger-border); }
    .top-verdict.safe   { background: var(--success-light); border-color: var(--success-border); }
    .top-verdict-icon   { font-size: 36px; flex-shrink: 0; }
    .top-verdict-body   { flex: 1; }
    .top-verdict-title  { font-size: 21px; font-weight: 900; letter-spacing: -0.7px; margin-bottom: 5px; }
    .top-verdict.danger .top-verdict-title { color: var(--danger); }
    .top-verdict.safe   .top-verdict-title { color: var(--success); }
    .top-verdict-sub    { font-size: 13px; color: var(--gray-500); line-height: 1.6; }
    .top-verdict-badge  {
        text-align: center; background: rgba(0,0,0,0.06);
        border-radius: 12px; padding: 12px 16px; flex-shrink: 0;
    }
    .top-verdict-num    { font-size: 32px; font-weight: 900; line-height: 1; letter-spacing: -1.5px; }
    .top-verdict.danger .top-verdict-num { color: var(--danger); }
    .top-verdict.safe   .top-verdict-num { color: var(--success); }
    .top-verdict-num-label { font-size: 11px; color: var(--gray-500); margin-top: 3px; font-weight: 600; }

    /* 이슈 카드 */
    .issue-card {
        background: #fff; border: 1.5px solid var(--danger-border);
        border-radius: 12px; padding: 15px 18px; margin-bottom: 9px;
    }
    .issue-card-header { display: flex; align-items: center; gap: 9px; margin-bottom: 7px; }
    .issue-card-icon   { font-size: 16px; }
    .issue-card-title  { font-size: 14px; font-weight: 700; color: var(--danger); }
    .issue-card-body   { font-size: 13px; color: var(--gray-700); line-height: 1.7; }

    /* chips */
    .chips-row { display: flex; gap: 7px; flex-wrap: wrap; margin-bottom: 16px; margin-top: 6px; }
    .chip {
        display: inline-flex; align-items: center; gap: 6px;
        padding: 5px 11px; border-radius: 4px;
        font-size: 12px; font-weight: 600; border: 1px solid;
    }
    .chip-dot { width: 6px; height: 6px; border-radius: 50%; }
    .chip-danger  { background: var(--danger-light);  border-color: var(--danger-border);  color: #b91c1c; }
    .chip-danger  .chip-dot { background: var(--danger); }
    .chip-success { background: var(--success-light); border-color: var(--success-border); color: #166534; }
    .chip-success .chip-dot { background: var(--success); }

    /* 섹션 카드 / 아코디언 */
    .section-card {
        background: #fff; border: 1px solid var(--gray-200);
        border-radius: 12px; margin-bottom: 10px; overflow: hidden;
        transition: box-shadow 0.2s;
    }
    .section-card:hover { box-shadow: 0 4px 20px rgba(0,0,0,0.07); }
    .acc-header { padding: 16px 20px; display: flex; align-items: center; gap: 13px; }
    .acc-icon {
        width: 36px; height: 36px; border-radius: 9px;
        display: flex; align-items: center; justify-content: center;
        font-size: 16px; flex-shrink: 0;
    }
    .icon-blue  { background: #eff6ff; }
    .icon-amber { background: #fffbeb; }
    .icon-teal  { background: #f0fdfa; }
    .acc-text   { flex: 1; }
    .acc-title  { font-size: 14px; font-weight: 700; color: var(--gray-900); margin-bottom: 1px; }
    .acc-sub    { font-size: 11px; color: var(--gray-400); }
    .acc-badge  { font-size: 11px; font-weight: 700; padding: 3px 9px; border-radius: 4px; flex-shrink: 0; }
    .badge-danger  { background: #fee2e2; color: #b91c1c; }
    .badge-success { background: #dcfce7; color: #166534; }
    .badge-warning { background: #fef9c3; color: #92400e; }
    .acc-chevron   { font-size: 11px; color: var(--gray-300); flex-shrink: 0; }

    div[data-testid="stButton"] > button[kind="secondary"] {
        background: transparent !important; border: none !important;
        border-top: 1px solid var(--gray-100) !important; border-radius: 0 !important;
        color: var(--gray-500) !important; font-size: 12px !important;
        font-weight: 600 !important; height: 38px !important;
        padding: 0 20px !important; width: 100% !important;
        text-align: center !important; transition: background 0.12s !important;
    }
    div[data-testid="stButton"] > button[kind="secondary"]:hover {
        background: var(--gray-50) !important; color: var(--gray-900) !important;
    }

    .acc-body {
        padding: 14px 20px 18px; border-top: 1px solid var(--gray-100);
        animation: fadeDown 0.18s ease;
    }
    @keyframes fadeDown {
        from { opacity: 0; transform: translateY(-4px); }
        to   { opacity: 1; transform: translateY(0); }
    }

    /* part bar */
    .part-item { padding: 14px 0; border-bottom: 1px solid var(--gray-100); }
    .part-item:last-child { border-bottom: none; }
    .part-row-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 14px; }
    .part-name { font-size: 13px; font-weight: 600; color: var(--gray-700); }
    .part-tag  { font-size: 11px; font-weight: 700; padding: 3px 8px; border-radius: 4px; }
    .tag-over   { background: #fee2e2; color: #b91c1c; }
    .tag-ok     { background: #dcfce7; color: #166534; }
    .tag-low    { background: #eff6ff; color: #1d4ed8; }
    .tag-nodata { background: var(--gray-100); color: var(--gray-500); }
    .range-wrap { position: relative; height: 42px; }
    .range-track {
        position: absolute; left: 0; right: 0; top: 50%; transform: translateY(-50%);
        height: 5px; background: var(--gray-100); border-radius: 3px;
    }
    .range-zone { position: absolute; top: 50%; transform: translateY(-50%); height: 5px; border-radius: 3px; }
    .range-dot {
        position: absolute; top: 50%; transform: translate(-50%, -50%);
        width: 13px; height: 13px; border-radius: 50%;
        border: 2.5px solid #fff; box-shadow: 0 1px 6px rgba(0,0,0,0.2); z-index: 2;
    }
    .range-dot-price {
        position: absolute; top: 2px; transform: translateX(-50%);
        font-size: 11px; font-weight: 700; white-space: nowrap;
    }
    .range-label-min, .range-label-max {
        position: absolute; bottom: 1px; transform: translateX(-50%);
        font-size: 10px; color: var(--gray-400); white-space: nowrap;
    }

    /* labor */
    .labor-item {
        display: flex; align-items: stretch; gap: 13px;
        padding: 13px 0; border-bottom: 1px solid var(--gray-100);
    }
    .labor-item:last-child { border-bottom: none; }
    .labor-bar   { width: 3px; border-radius: 2px; flex-shrink: 0; }
    .labor-info  { flex: 1; }
    .labor-name  { font-size: 13px; font-weight: 700; color: var(--gray-900); margin-bottom: 3px; }
    .labor-status-text  { font-size: 12px; font-weight: 600; margin-bottom: 5px; }
    .labor-detail-text  { font-size: 11px; color: var(--gray-400); line-height: 1.6; }
    .labor-amounts      { text-align: right; flex-shrink: 0; }
    .labor-charged      { font-size: 15px; font-weight: 800; letter-spacing: -0.4px; margin-bottom: 2px; }
    .labor-standard-text{ font-size: 11px; color: var(--gray-400); }
    .labor-nodata       { padding: 8px 0; font-size: 12px; color: var(--gray-400); font-style: italic; }

    /* cycle */
    .cycle-item { padding: 13px 0; border-bottom: 1px solid var(--gray-100); }
    .cycle-item:last-child { border-bottom: none; }
    .cycle-row-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
    .cycle-name          { font-size: 13px; font-weight: 700; color: var(--gray-900); }
    .cycle-status-badge  { font-size: 11px; font-weight: 700; padding: 3px 9px; border-radius: 4px; }
    .cycle-prog-track    { background: var(--gray-100); border-radius: 4px; height: 7px; overflow: hidden; margin-bottom: 8px; }
    .cycle-prog-fill     { height: 100%; border-radius: 4px; }
    .cycle-meta-row      { display: flex; justify-content: space-between; font-size: 11px; color: var(--gray-400); }

    .empty-msg { font-size: 12px; color: var(--gray-400); padding: 12px 0; text-align: center; font-style: italic; }
    .stButton > button { font-family: 'Pretendard', sans-serif !important; font-weight: 600 !important; }
    </style>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────
# 2. SESSION STATE
# ─────────────────────────────────────────────

def init_session_state() -> None:
    defaults = {
        "page": "upload",
        "estimate_id": None,
        "is_test_mode": False,
        "symptom_text": "",
        "rag_result": None,
        "rag_result_key": "",
        "acc_rag": False,
        "acc_parts": False,
        "acc_labor": False,
        "acc_cycle": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ─────────────────────────────────────────────
# 3. BUSINESS LOGIC
# ─────────────────────────────────────────────

def get_prev_mileage(conn, repair_content: str, estimate_id: str) -> Optional[int]:
    df = pd.read_sql("""
        SELECT e.car_mileage
        FROM test.labor l
        JOIN test.estimates e ON l.estimate_id = e.id
        WHERE l.repair_content = %s
          AND e.customer_id   = %s
          AND e.id            <> %s
        ORDER BY e.service_finish_at DESC
        LIMIT 1
    """, conn, params=(repair_content, USER_EMAIL, estimate_id))
    return int(df.iloc[0]["car_mileage"]) if not df.empty else None


def get_diagnosis_summary(parts_df: pd.DataFrame, labor_df: pd.DataFrame, conn) -> dict:
    p_issue = (
        any(parts_df["unit_price"] > parts_df["max_price"])
        if not parts_df.empty and "max_price" in parts_df.columns else False
    )
    l_issue = (
        any(labor_df["tech_fee"] > (labor_df["standard_repair_time"] * labor_df["hour_labor_rate"]))
        if not labor_df.empty else False
    )
    c_issue = False
    if not labor_df.empty:
        curr_m = labor_df.iloc[0]["car_mileage"]
        eid    = st.session_state.estimate_id
        for _, row in labor_df.iterrows():
            if pd.notna(row.get("change_cycle")):
                prev = get_prev_mileage(conn, row["repair_content"], eid)
                if prev is not None and (curr_m - prev) < row["change_cycle"] * 0.8:
                    c_issue = True
                    break
    return {
        "is_over":     sum([p_issue, l_issue, c_issue]) >= 2,
        "issue_count": sum([p_issue, l_issue, c_issue]),
        "p_issue": p_issue,
        "l_issue": l_issue,
        "c_issue": c_issue,
    }


def norm_space(v: Any) -> str:
    return " ".join(str(v or "").split())


def parse_llm_overrepair_verdict(diagnosis_text: str) -> Optional[bool]:
    text = re.sub(r"^\[[^\]]+\]\s*", "", norm_space(diagnosis_text or ""))
    if text.startswith("견적서는 다음 이유로 과잉정비입니다."):
        return True
    if text.startswith("견적서는 현재 근거 기준 표준 범위입니다."):
        return False
    return None


def split_diagnosis_text_for_display(diagnosis_text: str) -> tuple[str, str]:
    evidence_label_map = {
        "hyundai_model_pdf_plus_common": "현대 정비 지침서 + 일반 정비 지침",
        "hyundai_model_pdf_only": "현대 정비 지침서",
        "common_only": "일반 정비 지침",
        "no_evidence": "증거 불충분",
    }
    text = norm_space(diagnosis_text or "")
    m = re.match(r"^\[(근거:\s*[^\]]+)\]\s*(.*)$", text)
    if not m:
        return text, ""
    code_match = re.match(r"근거:\s*(.+)$", m.group(1))
    evidence_code = code_match.group(1).strip() if code_match else ""
    return m.group(2).strip(), evidence_label_map.get(evidence_code, evidence_code)


def norm_part(v: str) -> str:
    return re.sub(r"[\s_\-/(),.]+", "", norm_space(v).lower())


def split_parts_text(text: str) -> list[str]:
    items = re.split(r"[,/|\n]+", text or "")
    out, seen = [], set()
    for item in items:
        t = norm_space(item)
        if not t:
            continue
        k = norm_part(t)
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def split_symptoms(text: str) -> list[str]:
    parts = [norm_space(x) for x in re.split(r"\|\||\n+", text or "")]
    return [p for p in parts if p]


def extract_keywords(text: str) -> list[str]:
    tokens = re.split(r"[\s,./()\-_\[\]{}]+", norm_space(text))
    return [t for t in tokens if len(t) >= 2]


def infer_system_filters(symptom_text: str) -> list[str]:
    text = norm_space(symptom_text).lower()
    return [name for name, kws in SYSTEM_KEYWORD_RULES.items() if any(k.lower() in text for k in kws)]


def count_direct_matches(symptom_text: str, docs: list[dict[str, Any]]) -> int:
    keys = extract_keywords(symptom_text)
    if not keys:
        return 0
    return sum(1 for d in docs if any(k in f"{d.get('symptom_text','')} {d.get('evidence_text','')}" for k in keys))


def retrieve_lexical(conn, symptom_text: str, model_code: str, top_k: int, systems: list[str] | None = None) -> list[dict[str, Any]]:
    # ── 수정: test.repair_doc_chunks → repair_doc_chunks (SET search_path TO test 의존) ──
    where_sql = "WHERE vehicle_model = %s"
    params: list[Any] = [norm_space(symptom_text), norm_space(symptom_text), norm_space(symptom_text), model_code]
    if systems:
        where_sql += " AND system_category = ANY(%s)"
        params.append(systems)
    params.append(top_k)
    df = pd.read_sql(
        f"""
        SELECT
            id,
            document_source,
            vehicle_model,
            (
                ts_rank_cd(
                    to_tsvector('simple',
                        coalesce(symptom_text, '') || ' ' || coalesce(evidence_text, '')
                    ),
                    plainto_tsquery('simple', %s)
                ) * 0.7
                +
                GREATEST(
                    similarity(coalesce(symptom_text, ''), %s),
                    similarity(coalesce(evidence_text, ''), %s)
                ) * 0.3
            ) AS score,
            symptom_text,
            system_category,
            repair_parts,
            pre_replace_check_rule,
            evidence_text
        FROM  test.repair_doc_chunks
        {where_sql}
        ORDER BY score DESC
        LIMIT %s
        """,
        conn,
        params=tuple(params),
    )
    return df.to_dict(orient="records") if not df.empty else []


def part_matches_expected(quote_part: str, repair_parts: str) -> bool:
    qk = norm_part(quote_part)
    if not qk:
        return False
    expected_keys = [k for k in (norm_part(p) for p in split_parts_text(repair_parts)) if k]
    return any((ek in qk) or (qk in ek) for ek in expected_keys)


def is_consumable_part(part: str) -> bool:
    key = norm_part(part)
    return any(k in key for k in CONSUMABLE_PART_KEYWORDS)


def find_unrelated_quote_parts(quote_parts: list[str], matching_results: list[dict[str, Any]]) -> list[str]:
    if not matching_results or any(len(x.get("evidence_docs", [])) == 0 for x in matching_results):
        return []
    unrelated = []
    for qp in quote_parts:
        if is_consumable_part(qp):
            continue
        matched = any(
            part_matches_expected(qp, d.get("repair_parts", ""))
            for sr in matching_results
            for d in sr.get("match_docs", [])
        )
        if not matched:
            unrelated.append(qp)
    return unrelated


def strip_json_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`").strip()
        if t.lower().startswith("json"):
            t = t[4:].strip()
    return t


# ─────────────────────────────────────────────
# Gemini API
# ─────────────────────────────────────────────

def llm_diagnose_multi(api_key: str, quote_parts: list[str], symptom_results: list[dict[str, Any]], timeout_sec: int = 60) -> dict[str, Any]:
    system_prompt = """너는 자동차 정비 '견적서 진단/감수' 전문가다.
역할:
- 입력된 증상과 근거 문서를 바탕으로, 견적서의 각 정비 항목이 타당한지 점검한다.
- 정비소를 대리하지도, 고객을 대리하지도 말고 문서 근거 중심으로 중립적으로 판단한다.

작성 원칙:
- 증상별로 근거를 분리해서 해석하고, 마지막에 견적서 관점으로 종합한다.
- 소모품은 이번 과잉정비 판단의 핵심 대상이 아니므로 소모품 자체의 교체 필요를 단정하지 않는다.
- 각 증상 문구를 명시적으로 언급하고, 해당 증상과 견적 항목의 연관성을 직접 설명한다.
- 증상과의 직접 연관 근거가 약하더라도 가능한 인과가 있으면 과잉정비로 단정하지 않는다.
- 과잉정비 판정은 매우 보수적으로 한다. 명확한 무관 근거가 있을 때만 과잉정비로 표현한다.

문체:
- 견적서 감수 리포트처럼 간결하고 실무적으로 작성한다.
- 진단문 첫 문장에 최종 판정을 명시한다.
  - 과잉 가능성이 높으면: "견적서는 다음 이유로 과잉정비입니다."
  - 과잉 단정이 어려우면: "견적서는 현재 근거 기준 표준 범위입니다."
- 최소 2개 증상이 있으면 각 증상을 모두 1회 이상 직접 언급한다.

출력은 JSON 객체만:
{"diagnosis_text": "짧은 1문단(2~3문장)."}
"""
    symptom_blocks = []
    for idx, sr in enumerate(symptom_results, start=1):
        lines = [
            f"[{i}] source={d['document_source']} score={float(d.get('score',0)):.4f} | "
            f"system={d.get('system_category','')} | expected={d.get('repair_parts','')} | "
            f"evidence={d.get('evidence_text','')}"
            for i, d in enumerate(sr["evidence_docs"], start=1)
        ]
        symptom_blocks.append(
            f"증상{idx}: {sr['symptom_text']}\n직접매칭수: {sr['direct_match_count_model']}\n근거:\n"
            + ("\n".join(lines) if lines else "(없음)")
        )

    full_prompt = (
        system_prompt + "\n\n"
        + f"견적 부품: {', '.join(quote_parts) if quote_parts else '(없음)'}\n\n"
        + "\n\n".join(symptom_blocks)
    )
    payload = {
        "contents": [{"parts": [{"text": full_prompt}]}],
        "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"},
    }
    resp = requests.post(
        f"{DEFAULT_GEMINI_URL}?key={api_key}",
        headers={"Content-Type": "application/json"},
        json=payload, timeout=timeout_sec,
    )
    if not resp.ok:
        raise RuntimeError(f"Gemini API 호출 실패(status={resp.status_code}): {resp.text[:300]}")
    txt = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
    parsed = json.loads(strip_json_fence(txt))
    if not isinstance(parsed, dict):
        raise RuntimeError("Gemini 응답 JSON 파싱 실패")
    return parsed


def run_symptom_rag_diagnosis(conn, symptom_text: str, model_code: str, quote_parts: list[str]) -> dict[str, Any]:
    symptoms = split_symptoms(symptom_text)
    if not symptoms:
        return {"diagnosis_text": "증상 입력이 없어 진단을 수행하지 않았습니다.", "symptom_results": [], "llm_called": False}

    symptom_results, matching_results = [], []
    total_model_docs = total_common_docs = 0

    for symptom in symptoms:
        inferred = infer_system_filters(symptom)
        model_docs = retrieve_lexical(conn, symptom, model_code, top_k=8, systems=inferred or None)
        direct_match_count = count_direct_matches(symptom, model_docs)
        common_docs = []
        if len(model_docs) < 3 or direct_match_count < 1:
            common_docs = retrieve_lexical(conn, symptom, "common", top_k=5, systems=inferred or None)

        total_model_docs += len(model_docs)
        total_common_docs += len(common_docs)

        merged = sorted(model_docs + common_docs, key=lambda x: float(x.get("score", 0) or 0), reverse=True)
        filtered = [d for d in merged if float(d.get("score", 0) or 0) >= 0.02]
        matching_results.append({"symptom_text": symptom, "match_docs": merged, "evidence_docs": filtered[:3]})
        symptom_results.append({"symptom_text": symptom, "direct_match_count_model": direct_match_count, "evidence_docs": filtered[:3]})

    evidence_scope = (
        "hyundai_model_pdf_plus_common" if total_model_docs > 0 and total_common_docs > 0
        else "hyundai_model_pdf_only" if total_model_docs > 0
        else "common_only" if total_common_docs > 0
        else "no_evidence"
    )

    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        return {
            "diagnosis_text": f"[근거: {evidence_scope}] GEMINI_API_KEY가 없어 LLM 진단을 생략했습니다.",
            "evidence_scope": evidence_scope, "symptom_results": symptom_results,
            "possibly_unrelated_quote_parts": find_unrelated_quote_parts(quote_parts, matching_results),
            "llm_called": False,
        }
    try:
        verdict = llm_diagnose_multi(api_key, quote_parts, symptom_results)
        diagnosis_text = norm_space(verdict.get("diagnosis_text", "")) or "견적서는 현재 근거 기준 표준 범위입니다."
        llm_called = True
    except Exception as e:
        diagnosis_text = f"LLM 호출 실패. ({e})"
        llm_called = False

    return {
        "diagnosis_text": f"[근거: {evidence_scope}] {diagnosis_text}",
        "evidence_scope": evidence_scope, "symptom_results": symptom_results,
        "possibly_unrelated_quote_parts": find_unrelated_quote_parts(quote_parts, matching_results),
        "llm_called": llm_called,
    }


def precompute_rag_for_estimate(conn, estimate_id: str, symptom_text: str) -> str:
    eid = estimate_id
    meta = pd.read_sql("SELECT car_type FROM test.estimates WHERE id = %s LIMIT 1", conn, params=(eid,))
    if meta.empty and eid == "EST_FROM_UPLOAD" and ENV == "development":
        eid = "EST_20260216_001"
        meta = pd.read_sql("SELECT car_type FROM test.estimates WHERE id = %s LIMIT 1", conn, params=(eid,))

    car_type = meta.iloc[0]["car_type"] if not meta.empty else "차량 정보 없음"
    parts_df = pd.read_sql("SELECT part_official_name FROM test.parts WHERE estimate_id = %s", conn, params=(eid,))
    quote_parts = list(dict.fromkeys(
        x for x in (norm_space(v) for v in parts_df["part_official_name"].dropna()) if x
    ))

    symptom_text = norm_space(symptom_text)
    if symptom_text and car_type != "차량 정보 없음":
        cache_key = f"{eid}|{car_type}|{symptom_text}|{'|'.join(quote_parts)}"
        if st.session_state.get("rag_result_key") != cache_key or st.session_state.get("rag_result") is None:
            st.session_state.rag_result = run_symptom_rag_diagnosis(conn, symptom_text, car_type, quote_parts)
            st.session_state.rag_result_key = cache_key
    else:
        st.session_state.rag_result = None
        st.session_state.rag_result_key = ""
    return eid


# ─────────────────────────────────────────────
# 4. UI COMPONENTS
# ─────────────────────────────────────────────

def render_topbar() -> None:
    st.markdown("""
    <div class="topbar">
        <div class="topbar-logo">
            <div class="topbar-logo-mark">C</div>
            CarCheck
        </div>
        <span class="topbar-tag">AI 정비 진단</span>
    </div>
    """, unsafe_allow_html=True)


def render_part_bar(label: str, actual: float, min_p: float, max_p: float) -> None:
    if pd.isna(min_p) or pd.isna(max_p) or min_p == 0 or max_p == 0:
        st.markdown(f"""
        <div class="part-item">
            <div class="part-row-top">
                <span class="part-name">{label}</span>
                <span class="part-tag tag-nodata">기준가 없음</span>
            </div>
            <div style="font-size:11px;color:var(--gray-400);padding-bottom:4px;">
                {actual:,.0f}원 청구 · 비교 기준 데이터 미등록
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    if actual > max_p:
        color, tag_cls, tag_lbl = "#dc2626", "tag-over", "과다 청구"
    elif actual < min_p:
        color, tag_cls, tag_lbl = "#1d4ed8", "tag-low", "저렴"
    else:
        color, tag_cls, tag_lbl = "#15803d", "tag-ok", "적정"

    B_START, B_END, B_WIDTH = 20, 80, 60
    if actual < min_p:
        dot_pct = B_START - ((min_p - actual) / min_p) * 50
    elif actual > max_p:
        dot_pct = B_END + ((actual - max_p) / max_p) * 50
    else:
        inner   = (actual - min_p) / (max_p - min_p) if max_p != min_p else 0.5
        dot_pct = B_START + inner * B_WIDTH
    dot_pct    = max(5, min(95, dot_pct))
    zone_color = "#dcfce7" if actual <= max_p else "#fee2e2"

    st.markdown(f"""
    <div class="part-item">
        <div class="part-row-top">
            <span class="part-name">{label}</span>
            <span class="part-tag {tag_cls}">{actual:,.0f}원 &nbsp;·&nbsp; {tag_lbl}</span>
        </div>
        <div class="range-wrap">
            <div class="range-track"></div>
            <div class="range-zone" style="left:{B_START}%;width:{B_WIDTH}%;background:{zone_color};"></div>
            <div class="range-dot-price" style="left:{dot_pct}%;color:{color};">{actual:,.0f}원</div>
            <div class="range-dot" style="left:{dot_pct}%;background:{color};"></div>
            <div class="range-label-min" style="left:{B_START}%;">최저 {min_p:,.0f}원</div>
            <div class="range-label-max" style="left:{B_END}%;">최고 {max_p:,.0f}원</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_labor_card(content: str, actual_fee: float, std_time: Optional[float], hourly_rate: Optional[float]) -> None:
    if pd.isna(std_time) or pd.isna(hourly_rate):
        st.markdown(f"""
        <div class="labor-item">
            <div class="labor-bar" style="background:var(--gray-200);"></div>
            <div class="labor-info">
                <div class="labor-name">{content}</div>
                <div class="labor-nodata">기준 공임 데이터 미등록 · 비교 불가</div>
            </div>
            <div class="labor-amounts">
                <div class="labor-charged" style="color:var(--gray-500);">{actual_fee:,.0f}원</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    expected = std_time * hourly_rate
    diff     = actual_fee - expected
    pct      = (diff / expected * 100) if expected > 0 else 0

    if diff > 0:
        bar_c, status, amt_c = "#dc2626", f"기준가 대비 {diff:,.0f}원 ({pct:+.1f}%) 초과", "#dc2626"
    elif diff < 0:
        bar_c, status, amt_c = "#1d4ed8", f"기준가 대비 {abs(diff):,.0f}원 ({abs(pct):.1f}%) 낮음", "#1d4ed8"
    else:
        bar_c, status, amt_c = "#15803d", "시장 기준가와 일치", "#15803d"

    st.markdown(f"""
    <div class="labor-item">
        <div class="labor-bar" style="background:{bar_c};"></div>
        <div class="labor-info">
            <div class="labor-name">{content}</div>
            <div class="labor-status-text" style="color:{bar_c};">{status}</div>
            <div class="labor-detail-text">
                기준가 {expected:,.0f}원 (표준 {std_time}h &times; {hourly_rate:,.0f}원/h) | 청구액 {actual_fee:,.0f}원
            </div>
        </div>
        <div class="labor-amounts">
            <div class="labor-charged" style="color:{amt_c};">{actual_fee:,.0f}원</div>
            <div class="labor-standard-text">기준 {expected:,.0f}원</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_cycle_card(content: str, current_mileage: int, prev_mileage: Optional[int], cycle: Optional[int]) -> None:
    if cycle is None or (isinstance(cycle, float) and pd.isna(cycle)):
        return
    if prev_mileage is None:
        st.markdown(f"""
        <div class="cycle-item">
            <div class="cycle-row-top">
                <span class="cycle-name">{content}</span>
                <span class="cycle-status-badge badge-success">첫 교체 기록</span>
            </div>
            <div style="font-size:12px;color:var(--gray-500);font-style:italic;">
                권장 주기 {cycle:,}km · 이번이 첫 교체 기록입니다
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    usage  = current_mileage - prev_mileage
    ratio  = usage / cycle if cycle > 0 else 0
    fill_w = min(int(ratio * 100), 100)

    if ratio >= 1.0:
        badge_cls, badge_lbl, bar_color = "badge-success", "교체 적절",     "#15803d"
        note = f"권장 주기 {cycle:,}km 충족 후 교체"
    elif ratio >= 0.8:
        badge_cls, badge_lbl, bar_color = "badge-warning", "교체 권장 시기", "#d97706"
        note = f"권장 주기까지 {cycle - usage:,}km 남음"
    else:
        badge_cls, badge_lbl, bar_color = "badge-danger",  "조기 교체 의심", "#dc2626"
        note = f"권장 주기 대비 {cycle - usage:,}km 조기 교체"

    st.markdown(f"""
    <div class="cycle-item">
        <div class="cycle-row-top">
            <span class="cycle-name">{content}</span>
            <span class="cycle-status-badge {badge_cls}">{badge_lbl}</span>
        </div>
        <div class="cycle-prog-track">
            <div class="cycle-prog-fill" style="width:{fill_w}%;background:{bar_color};"></div>
        </div>
        <div class="cycle-meta-row">
            <span>{note}</span>
            <span>실제 {usage:,}km / 권장 {cycle:,}km ({fill_w}%)</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_accordion(section_id: str, icon: str, icon_cls: str,
                     title: str, subtitle: str, badge_label: str, badge_cls: str) -> bool:
    key     = f"acc_{section_id}"
    is_open = st.session_state.get(key, False)
    chev    = "▲" if is_open else "▼"

    st.markdown(f"""
    <div class="acc-header">
        <div class="acc-icon {icon_cls}">{icon}</div>
        <div class="acc-text">
            <div class="acc-title">{title}</div>
            <div class="acc-sub">{subtitle}</div>
        </div>
        <span class="acc-badge {badge_cls}">{badge_label}</span>
        <span class="acc-chevron">{chev}</span>
    </div>
    """, unsafe_allow_html=True)

    btn_label = "▲ 접기" if is_open else "▼ 상세보기"
    if st.button(btn_label, key=f"btn_{section_id}", use_container_width=True):
        st.session_state[key] = not is_open
        st.rerun()
    return is_open


# ─────────────────────────────────────────────
# 5. PAGES
# ─────────────────────────────────────────────

def render_upload_page() -> None:
    st.markdown("""
    <style>
    .stApp {
        background: linear-gradient(155deg, #060e1f 0%, #0f2044 55%, #132040 100%) !important;
    }
    [data-testid="stFileUploaderDropzone"] {
        background: rgba(255,255,255,0.07) !important;
        border: 1.5px dashed rgba(255,255,255,0.25) !important;
        border-radius: 10px !important;
    }
    [data-testid="stFileUploaderDropzone"] p,
    [data-testid="stFileUploaderDropzone"] span,
    [data-testid="stFileUploaderDropzone"] small { color: rgba(255,255,255,0.6) !important; }
    [data-testid="stFileUploaderDropzone"] button {
        background: rgba(255,255,255,0.15) !important;
        border: 1px solid rgba(255,255,255,0.3) !important;
        color: white !important; border-radius: 8px !important;
    }
    /* 수정 1: 텍스트 영역 글자 흰색 */
    /* 텍스트 영역 스타일 수정 */
    .stTextArea textarea {
        background: #ffffff !important;   /* 배경 흰색 */
        border: 1px solid rgba(0,0,0,0.15) !important;
        color: #000000 !important;        /* ✅ 실제 입력 텍스트 검은색 */
        border-radius: 8px !important;
        caret-color: #000000 !important;
    }

    /* ✅ placeholder만 회색 */
    .stTextArea textarea::placeholder {
        color: #9ca3af !important;   /* 연회색 */
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="text-align: center; padding: 68px 20px 36px;">
        <h1 style="
            font-size: 30px;
            font-weight: 900;
            color: #fff;
            letter-spacing: -0.8px;
            line-height: 1.28;
            margin-bottom: 18px;
        ">
            정비 후 받은 견적서,<br>
            <span style="color: #93c5fd;">믿을 수 있나요?</span>
        </h1>
    </div>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown("""
        <div style="background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.10);
                    border-radius:14px;padding:22px 20px 16px;">
            <div style="font-size:14px;font-weight:700;color:#fff;margin-bottom:3px;">견적서 업로드</div>
            <div style="font-size:12px;color:rgba(255,255,255,0.38);margin-bottom:14px;">
                JPG · PNG 형식을 지원합니다
            </div>
        """, unsafe_allow_html=True)

        uploaded = st.file_uploader("견적서", type=["jpg", "jpeg", "png"], label_visibility="collapsed")
        symptom_text = st.text_area(
            "증상",
            value=st.session_state.get("symptom_text", ""),
            placeholder="예: 달릴 때 덜그덕거리는 소리가 남",
            height=100,
        )

        if st.button("진단 시작", use_container_width=True, type="primary", disabled=(uploaded is None)):
            st.session_state.symptom_text = symptom_text.strip()
            st.session_state.rag_result = None
            st.session_state.rag_result_key = ""
            conn = get_connection()
            if not conn:
                st.error("데이터베이스에 연결할 수 없습니다.")
            else:
                try:
                    with st.spinner("분석 중..."):
                        resolved_eid = precompute_rag_for_estimate(conn, "EST_FROM_UPLOAD", st.session_state.symptom_text)
                    st.session_state.estimate_id = resolved_eid
                    st.session_state.page = "analysis"
                    st.rerun()
                except Exception as e:
                    st.error(f"오류: {e}")
                finally:
                    conn.close()

        st.markdown('</div>', unsafe_allow_html=True)

        if ENV == "development":
            st.markdown("""
            <div style="height:1px;background:rgba(255,255,255,0.1);margin:14px 0 12px;"></div>
            <div style="font-size:11px;color:rgba(255,255,255,0.3);text-align:center;margin-bottom:8px;">
                샘플 데이터로 먼저 체험
            </div>
            """, unsafe_allow_html=True)
            if st.button("샘플 데이터로 체험하기", use_container_width=True):
                st.session_state.symptom_text = symptom_text.strip()
                st.session_state.rag_result = None
                st.session_state.rag_result_key = ""
                conn = get_connection()
                if not conn:
                    st.error("데이터베이스에 연결할 수 없습니다.")
                else:
                    try:
                        with st.spinner("분석 중..."):
                            resolved_eid = precompute_rag_for_estimate(conn, "EST_20260216_001", st.session_state.symptom_text)
                        st.session_state.estimate_id = resolved_eid
                        st.session_state.is_test_mode = True
                        st.session_state.page = "analysis"
                        st.rerun()
                    except Exception as e:
                        st.error(f"오류: {e}")
                    finally:
                        conn.close()


def render_analysis_page() -> None:
    render_topbar()
    conn = get_connection()
    if not conn:
        st.error("데이터베이스에 연결할 수 없습니다.")
        return

    try:
        eid = st.session_state.estimate_id

        estimate_meta_df = pd.read_sql(
            "SELECT car_type, service_finish_at FROM test.estimates WHERE id = %s LIMIT 1",
            conn, params=(eid,)
        )
        if estimate_meta_df.empty and eid == "EST_FROM_UPLOAD" and ENV == "development":
            st.info("업로드 견적 파싱이 아직 연결되지 않아 샘플 견적으로 진단을 표시합니다.")
            eid = "EST_20260216_001"
            st.session_state.estimate_id = eid
            estimate_meta_df = pd.read_sql(
                "SELECT car_type, service_finish_at FROM test.estimates WHERE id = %s LIMIT 1",
                conn, params=(eid,)
            )

        parts_df = pd.read_sql(
            """
            SELECT
                p.part_official_name,
                p.unit_price,
                pm.min_price,
                pm.max_price
            FROM  test.parts p
            JOIN  test.estimates e ON p.estimate_id = e.id
            LEFT  JOIN LATERAL (
                SELECT min_price, max_price
                FROM   test.parts_master pm
                WHERE  pm.part_official_name = p.part_official_name
                  AND  pm.car_type           = e.car_type
                ORDER  BY pm.extracted_at DESC
                LIMIT  1
            ) pm ON TRUE
            WHERE p.estimate_id = %s
            """,
            conn,
            params=(eid,),
        )

        labor_df = pd.read_sql(
            """
            SELECT
                l.repair_content,
                l.tech_fee,
                lm.standard_repair_time,
                lm.hour_labor_rate,
                lm.change_cycle,
                e.car_mileage,
                e.car_type,
                e.service_finish_at
            FROM  test.labor l
            JOIN  test.estimates e ON l.estimate_id = e.id
            LEFT  JOIN test.labor_master lm
                ON  lm.repair_content = l.repair_content
                AND lm.car_type       = e.car_type
                AND e.service_finish_at BETWEEN lm.start_date AND lm.end_date
            WHERE l.estimate_id = %s
            """,
            conn,
            params=(eid,),
        )

        summary       = get_diagnosis_summary(parts_df, labor_df, conn)
        car_type      = estimate_meta_df.iloc[0]["car_type"] if not estimate_meta_df.empty else "차량 정보 없음"
        svc_date      = str(estimate_meta_df.iloc[0]["service_finish_at"])[:10] if not estimate_meta_df.empty else ""
        symptom_text  = norm_space(st.session_state.get("symptom_text", ""))
        rag_result: dict[str, Any] = st.session_state.get("rag_result") or {}
        llm_overrepair        = parse_llm_overrepair_verdict(rag_result.get("diagnosis_text", ""))
        llm_issue             = (llm_overrepair is True)
        effective_issue_count = summary["issue_count"] + (1 if llm_issue else 0)
        effective_is_over     = summary["is_over"] or llm_issue

        st.markdown('<div class="page-wrap">', unsafe_allow_html=True)

        # ── 판정 배너 ──
        v_cls  = "danger" if effective_is_over else "safe"
        
        v_title = "과잉정비 의심" if effective_is_over else "이상 없음"
        v_sub   = "아래 항목에서 이상이 감지되었습니다." if effective_is_over else "부품비·공임비·교체주기 모두 정상입니다."

        st.markdown(f"""
        <div class="top-verdict {v_cls}">
            <div class="top-verdict-body">
                <div class="top-verdict-title">{v_title}</div>
                <div class="top-verdict-sub">{v_sub}</div>
            </div>
            <div class="top-verdict-badge">
                <div class="top-verdict-num">{effective_issue_count}</div>
                <div class="top-verdict-num-label">이상 항목</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # ── 이슈 카드 ──
        if effective_is_over:
            if llm_issue:
                diagnosis_body, _ = split_diagnosis_text_for_display(rag_result.get("diagnosis_text", ""))
                st.markdown(f"""
                <div class="issue-card">
                    <div class="issue-card-header">
                        <span class="issue-card-title">증상 무관 정비 포함</span>
                    </div>
                    <div class="issue-card-body">{diagnosis_body}</div>
                </div>
                """, unsafe_allow_html=True)

            if summary["p_issue"]:
                over_parts = "<br>".join(
                    f"{row['part_official_name']} — {row['unit_price']:,.0f}원 (최고 기준가 {row['max_price']:,.0f}원)"
                    for _, row in parts_df.iterrows()
                    if pd.notna(row.get("max_price")) and row["unit_price"] > row["max_price"]
                )
                st.markdown(f"""
                <div class="issue-card">
                    <div class="issue-card-header">
                        <span class="issue-card-title">부품비 과다 청구</span>
                    </div>
                    <div class="issue-card-body">{over_parts}</div>
                </div>
                """, unsafe_allow_html=True)

            if summary["l_issue"]:
                over_labor = "<br>".join(
                    f"{row['repair_content']} — {row['tech_fee']:,.0f}원 (기준 {row['standard_repair_time']*row['hour_labor_rate']:,.0f}원)"
                    for _, row in labor_df.iterrows()
                    if pd.notna(row.get("standard_repair_time")) and pd.notna(row.get("hour_labor_rate"))
                    and row["tech_fee"] > row["standard_repair_time"] * row["hour_labor_rate"]
                )
                st.markdown(f"""
                <div class="issue-card">
                    <div class="issue-card-header">
                        <span class="issue-card-title">공임비 기준 초과</span>
                    </div>
                    <div class="issue-card-body">{over_labor}</div>
                </div>
                """, unsafe_allow_html=True)

            if summary["c_issue"]:
                st.markdown("""
                <div class="issue-card">
                    <div class="issue-card-header">
                        <span class="issue-card-title">소모품 조기 교체 의심</span>
                    </div>
                    <div class="issue-card-body">권장 교체 주기 이전에 소모품이 교체되었습니다. 아래 상세보기에서 확인하세요.</div>
                </div>
                """, unsafe_allow_html=True)

        # ── 수정 3: chips (HTML 렌더링 문제 수정 — f-string 직접 삽입 방식 유지하되 chips-row를 단일 markdown으로) ──
        def make_chip(label: str, is_issue: bool) -> str:
            cls = "chip-danger" if is_issue else "chip-success"
            return f'<div class="chip {cls}"><div class="chip-dot"></div>{label}</div>'

        chips_html = ""
        if llm_overrepair is not None:
            chips_html += make_chip("증상 무관 정비 포함" if llm_overrepair else "증상 무관 정비 없음", llm_overrepair)
        chips_html += make_chip("부품비 과다" if summary["p_issue"] else "부품비 적정", summary["p_issue"])
        chips_html += make_chip("공임비 초과" if summary["l_issue"] else "공임비 적정", summary["l_issue"])
        chips_html += make_chip("조기 교체 의심" if summary["c_issue"] else "교체주기 적정", summary["c_issue"])

        st.markdown(f'<div class="chips-row">{chips_html}</div>', unsafe_allow_html=True)

        # ── 섹션: 증상-정비 적합성 ──
        rag_badge_cls = "badge-danger" if llm_overrepair is True else "badge-success"
        rag_badge_lbl = "증상 무관 정비 포함" if llm_overrepair is True else "증상 무관 정비 없음"

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        if render_accordion("rag", "AI", "icon-blue", "증상-정비 적합성 진단", "증상·차종·견적 부품을 근거 문서와 비교", rag_badge_lbl, rag_badge_cls):
            st.markdown('<div class="acc-body">', unsafe_allow_html=True)
            if not symptom_text:
                st.markdown('<div class="empty-msg">증상 설명이 없어 진단을 건너뜁니다</div>', unsafe_allow_html=True)
            else:
                st.caption("입력 증상")
                st.write(symptom_text)
                st.caption("진단 결과")
                body, evidence = split_diagnosis_text_for_display(rag_result.get("diagnosis_text", "진단 결과 없음"))
                st.write(body)
                if evidence:
                    st.caption(f"근거: {evidence}")
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── 섹션: 부품비 ──
        over_cnt    = sum(1 for _, r in parts_df.iterrows() if pd.notna(r.get("max_price")) and r["unit_price"] > r["max_price"]) if not parts_df.empty else 0
        p_badge_cls = "badge-danger" if summary["p_issue"] else "badge-success"
        p_badge_lbl = f"{over_cnt}건 과다" if summary["p_issue"] else "모두 적정"

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        if render_accordion("parts", "W", "icon-blue", "부품비 적정성 분석", "시장 기준가 범위와 청구 금액 비교", p_badge_lbl, p_badge_cls):
            st.markdown('<div class="acc-body">', unsafe_allow_html=True)
            if parts_df.empty:
                st.markdown('<div class="empty-msg">부품비 데이터가 없습니다</div>', unsafe_allow_html=True)
            else:
                for _, row in parts_df.iterrows():
                    render_part_bar(row["part_official_name"], row["unit_price"], row.get("min_price", float("nan")), row.get("max_price", float("nan")))
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── 섹션: 공임비 ──
        l_badge_cls = "badge-danger" if summary["l_issue"] else "badge-success"
        l_badge_lbl = "기준 초과 있음" if summary["l_issue"] else "모두 적정"

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        if render_accordion("labor", "T", "icon-amber", "공임비 적정성 진단", "표준 작업시간 × 시간당 공임 기준 비교", l_badge_lbl, l_badge_cls):
            st.markdown('<div class="acc-body">', unsafe_allow_html=True)
            if labor_df.empty:
                st.markdown('<div class="empty-msg">공임비 데이터가 없습니다</div>', unsafe_allow_html=True)
            else:
                for _, row in labor_df.iterrows():
                    render_labor_card(row["repair_content"], row["tech_fee"], row.get("standard_repair_time"), row.get("hour_labor_rate"))
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── 섹션: 교체주기 ──
        c_badge_cls = "badge-danger" if summary["c_issue"] else "badge-success"
        c_badge_lbl = "조기 교체 의심" if summary["c_issue"] else "주기 적정"

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        if render_accordion("cycle", "R", "icon-teal", "소모품 교체주기 점검", "이전 교체 기록과 권장 주기 비교", c_badge_lbl, c_badge_cls):
            st.markdown('<div class="acc-body">', unsafe_allow_html=True)
            has_cycle = False
            if not labor_df.empty:
                curr_m = int(labor_df.iloc[0]["car_mileage"])
                for _, row in labor_df.iterrows():
                    cyc = row.get("change_cycle")
                    if cyc is None or (isinstance(cyc, float) and pd.isna(cyc)):
                        continue
                    has_cycle = True
                    prev_m = get_prev_mileage(conn, row["repair_content"], eid)
                    render_cycle_card(row["repair_content"], curr_m, prev_m, int(cyc))
            if not has_cycle:
                st.markdown('<div class="empty-msg">교체주기 기준 데이터가 없습니다</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)
        if st.button("처음으로"):
            st.session_state.update({
                "page": "upload", "estimate_id": None, "is_test_mode": False,
                "symptom_text": "", "rag_result": None, "rag_result_key": "",
                "acc_rag": False, "acc_parts": False, "acc_labor": False, "acc_cycle": False,
            })
            st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

    except Exception as e:
        st.error(f"분석 중 오류가 발생했습니다: {e}")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# 6. MAIN
# ─────────────────────────────────────────────

def main() -> None:
    set_page_config()
    inject_global_css()
    init_session_state()

    if st.session_state.page == "upload":
        render_upload_page()
    elif st.session_state.page == "analysis":
        render_analysis_page()


if __name__ == "__main__":
    main()