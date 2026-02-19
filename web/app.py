import os
import re
import json
import requests
import pandas as pd
import streamlit as st
from typing import Optional, Any
from dotenv import load_dotenv
from db import get_connection

load_dotenv()

# ─────────────────────────────────────────────
# 환경 설정
# "development" → 샘플 데이터 버튼 노출
# "production"  → 샘플 데이터 버튼 숨김
# ─────────────────────────────────────────────
ENV: str = "development"
USER_EMAIL: str = "test@example.com"
DEFAULT_RESPONSES_URL: str = "https://api.openai.com/v1/responses"

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
        --navy-950: #060e1f;
        --navy-900: #0a1628;
        --navy-800: #0f2044;
        --navy-700: #1a3260;
        --accent:        #2563eb;
        --accent-light:  #eff6ff;
        --danger:        #dc2626;
        --danger-light:  #fef2f2;
        --danger-border: #fca5a5;
        --success:        #15803d;
        --success-light:  #f0fdf4;
        --success-border: #86efac;
        --warning:        #b45309;
        --warning-light:  #fffbeb;
        --warning-border: #fde68a;
        --gray-50:  #f8fafc;
        --gray-100: #f1f5f9;
        --gray-200: #e2e8f0;
        --gray-300: #cbd5e1;
        --gray-400: #94a3b8;
        --gray-500: #64748b;
        --gray-700: #334155;
        --gray-900: #0f172a;
    }

    /* ── Streamlit chrome 초기화 ── */
    html, body, [class*="css"] {
        font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }
    #MainMenu, footer, header { visibility: hidden; }
    .block-container {
        padding-top: 0 !important;
        padding-bottom: 0 !important;
    }
    .stApp { background: var(--gray-50) !important; }

    /* ══════════════════════════════
       업로드 페이지
    ══════════════════════════════ */
    /* ══ 업로드 페이지 ══ */

    /* 전체 배경: stApp에 네이비 그라데이션 적용 (업로드 페이지일 때만 body class로 제어 불가하므로 별도 div 사용) */
    .upload-bg {
        background: linear-gradient(155deg, var(--navy-950) 0%, var(--navy-800) 55%, #132040 100%);
        min-height: 100vh;
        margin: 0 -4rem;
        padding: 64px 4rem 60px;
        display: flex;
        flex-direction: column;
        align-items: center;
    }
    .upload-eyebrow {
        font-size: 11px; font-weight: 700; letter-spacing: 1.5px;
        text-transform: uppercase; color: rgba(255,255,255,0.35);
        margin-bottom: 16px; text-align: center;
    }
    .upload-headline {
        font-size: 30px; font-weight: 800; color: #fff;
        letter-spacing: -0.8px; line-height: 1.22;
        text-align: center; margin-bottom: 12px;
    }
    .upload-headline span { color: #60a5fa; }
    .upload-desc {
        font-size: 14px; color: rgba(255,255,255,0.4);
        text-align: center; line-height: 1.75; margin-bottom: 32px;
    }
    .upload-card-label {
        font-size: 15px; font-weight: 700; color: var(--gray-900);
        letter-spacing: -0.3px; margin-bottom: 3px;
    }
    .upload-card-sub {
        font-size: 12px; color: var(--gray-400); margin-bottom: 12px;
    }
    .upload-divider {
        height: 1px; background: var(--gray-100); margin: 14px 0 12px;
    }
    .upload-sample-label {
        font-size: 11px; color: var(--gray-400);
        text-align: center; margin-bottom: 8px;
    }
    .trust-footer {
        display: flex; justify-content: center;
        gap: 24px; margin-top: 28px;
    }
    .trust-item {
        font-size: 11px; color: rgba(255,255,255,0.3); font-weight: 500;
    }

    /* 업로드 페이지 컬럼 내부 흰 배경 카드 */
    .upload-widget-wrap {
        background: white;
        border-radius: 14px;
        padding: 24px 22px 18px;
        box-shadow: 0 20px 60px rgba(0,0,0,0.28);
    }

    /* ══════════════════════════════
       분석 페이지 — 상단 바
    ══════════════════════════════ */
    .topbar {
        position: sticky;
        top: 0;
        z-index: 200;
        background: var(--navy-900);
        border-bottom: 1px solid rgba(255,255,255,0.07);
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0 28px;
        height: 56px;
        margin-left: -4rem;
        margin-right: -4rem;
    }
    .topbar-logo {
        display: flex;
        align-items: center;
        gap: 10px;
        color: #fff;
        font-size: 15px;
        font-weight: 700;
        letter-spacing: -0.3px;
    }
    .topbar-logo-mark {
        width: 28px; height: 28px;
        background: var(--accent);
        border-radius: 7px;
        display: flex; align-items: center; justify-content: center;
        font-size: 14px;
    }
    .topbar-tag {
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        color: rgba(255,255,255,0.35);
        border: 1px solid rgba(255,255,255,0.12);
        padding: 4px 10px;
        border-radius: 4px;
    }

    /* ── 분석 콘텐츠 래퍼: 좌우는 centered layout 기본 여백 사용 ── */
    .page-wrap { padding: 28px 0 72px; }

    /* ── Estimate meta strip ── */
    .meta-strip {
        background: var(--navy-800);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 12px;
        padding: 20px 22px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 14px;
        gap: 12px;
    }
    .meta-strip-label {
        font-size: 10px; font-weight: 700; letter-spacing: 1.2px;
        text-transform: uppercase; color: rgba(255,255,255,0.35); margin-bottom: 5px;
    }
    .meta-strip-title { font-size: 17px; font-weight: 700; color: #fff; letter-spacing: -0.4px; }
    .meta-strip-sub   { font-size: 12px; color: rgba(255,255,255,0.38); margin-top: 3px; }
    .meta-strip-car   { font-size: 40px; opacity: 0.22; }

    /* ── Verdict banner ── */
    .verdict-banner {
        border-radius: 12px; padding: 20px 24px; margin-bottom: 12px;
        display: flex; align-items: center; gap: 18px; border: 1.5px solid;
    }
    .verdict-banner.danger { background: var(--danger-light); border-color: var(--danger-border); }
    .verdict-banner.safe   { background: var(--success-light); border-color: var(--success-border); }
    .verdict-icon {
        width: 44px; height: 44px; border-radius: 11px;
        display: flex; align-items: center; justify-content: center;
        font-size: 20px; flex-shrink: 0;
    }
    .danger .verdict-icon { background: #fee2e2; }
    .safe   .verdict-icon { background: #dcfce7; }
    .verdict-main { flex: 1; }
    .verdict-title { font-size: 17px; font-weight: 800; letter-spacing: -0.5px; margin-bottom: 4px; }
    .danger .verdict-title { color: var(--danger); }
    .safe   .verdict-title { color: var(--success); }
    .verdict-desc  { font-size: 13px; color: var(--gray-500); line-height: 1.6; }
    .verdict-count { text-align: center; flex-shrink: 0; }
    .verdict-num   { font-size: 30px; font-weight: 800; line-height: 1; letter-spacing: -1px; }
    .danger .verdict-num { color: var(--danger); }
    .safe   .verdict-num { color: var(--success); }
    .verdict-num-label { font-size: 11px; color: var(--gray-400); margin-top: 3px; font-weight: 500; }

    /* ── Summary chips ── */
    .chips-row { display: flex; gap: 7px; flex-wrap: wrap; margin-bottom: 18px; }
    .chip {
        display: inline-flex; align-items: center; gap: 6px;
        padding: 5px 11px; border-radius: 4px;
        font-size: 12px; font-weight: 600; border: 1px solid; letter-spacing: -0.1px;
    }
    .chip-dot { width: 6px; height: 6px; border-radius: 50%; }
    .chip-danger  { background: var(--danger-light);  border-color: var(--danger-border);  color: #b91c1c; }
    .chip-danger  .chip-dot { background: var(--danger); }
    .chip-success { background: var(--success-light); border-color: var(--success-border); color: #166534; }
    .chip-success .chip-dot { background: var(--success); }

    /* ── Section card + Accordion ── */
    .section-card {
        background: #fff; border: 1px solid var(--gray-200);
        border-radius: 12px; margin-bottom: 10px; overflow: hidden;
        transition: box-shadow 0.2s;
    }
    .section-card:hover { box-shadow: 0 4px 20px rgba(0,0,0,0.07); }
    .acc-header {
        padding: 16px 20px; display: flex; align-items: center; gap: 13px;
        user-select: none;
    }
    .acc-icon {
        width: 36px; height: 36px; border-radius: 9px;
        display: flex; align-items: center; justify-content: center;
        font-size: 16px; flex-shrink: 0;
    }
    .icon-blue  { background: #eff6ff; }
    .icon-amber { background: #fffbeb; }
    .icon-teal  { background: #f0fdfa; }
    .acc-text   { flex: 1; }
    .acc-title  { font-size: 14px; font-weight: 700; color: var(--gray-900); letter-spacing: -0.2px; margin-bottom: 1px; }
    .acc-sub    { font-size: 11px; color: var(--gray-400); }
    .acc-badge  { font-size: 11px; font-weight: 700; padding: 3px 9px; border-radius: 4px; flex-shrink: 0; }
    .badge-danger  { background: #fee2e2; color: #b91c1c; }
    .badge-success { background: #dcfce7; color: #166534; }
    .badge-warning { background: #fef9c3; color: #92400e; }
    .acc-chevron   { font-size: 11px; color: var(--gray-300); flex-shrink: 0; }

    /* 토글 버튼: 헤더 전체 영역처럼 보이게 */
    div[data-testid="stButton"] > button[kind="secondary"] {
        background: transparent !important;
        border: none !important;
        border-top: 1px solid var(--gray-100) !important;
        border-radius: 0 !important;
        color: var(--gray-500) !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        height: 38px !important;
        letter-spacing: -0.1px !important;
        padding: 0 20px !important;
        width: 100% !important;
        text-align: center !important;
        transition: background 0.12s !important;
    }
    div[data-testid="stButton"] > button[kind="secondary"]:hover {
        background: var(--gray-50) !important;
        color: var(--gray-900) !important;
    }

    .acc-body {
        padding: 14px 20px 18px;
        border-top: 1px solid var(--gray-100);
        animation: fadeDown 0.18s ease;
    }
    @keyframes fadeDown {
        from { opacity: 0; transform: translateY(-4px); }
        to   { opacity: 1; transform: translateY(0); }
    }

    /* ── Part bar ── */
    .part-item { padding: 14px 0; border-bottom: 1px solid var(--gray-100); }
    .part-item:last-child { border-bottom: none; }
    .part-row-top {
        display: flex; align-items: center; justify-content: space-between; margin-bottom: 14px;
    }
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
    .range-zone {
        position: absolute; top: 50%; transform: translateY(-50%);
        height: 5px; border-radius: 3px;
    }
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

    /* ── Labor card ── */
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

    /* ── Cycle card ── */
    .cycle-item { padding: 13px 0; border-bottom: 1px solid var(--gray-100); }
    .cycle-item:last-child { border-bottom: none; }
    .cycle-row-top {
        display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px;
    }
    .cycle-name          { font-size: 13px; font-weight: 700; color: var(--gray-900); }
    .cycle-status-badge  { font-size: 11px; font-weight: 700; padding: 3px 9px; border-radius: 4px; }
    .cycle-prog-track    { background: var(--gray-100); border-radius: 4px; height: 7px; overflow: hidden; margin-bottom: 8px; }
    .cycle-prog-fill     { height: 100%; border-radius: 4px; }
    .cycle-meta-row      { display: flex; justify-content: space-between; font-size: 11px; color: var(--gray-400); }

    /* ── Empty state ── */
    .empty-msg { font-size: 12px; color: var(--gray-400); padding: 12px 0; text-align: center; font-style: italic; }

    /* ── 공통 버튼 ── */
    .stButton > button {
        font-family: 'Pretendard', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: -0.2px !important;
    }
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
    """해당 정비 항목의 직전 교체 시 주행거리를 반환합니다."""
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
    """2-of-3 규칙: 부품비·공임비·교체주기 중 2개 이상 이상 → 과잉정비 의심."""
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

    reasons = []
    if p_issue: reasons.append("부품비 과다 청구")
    if l_issue: reasons.append("공임비 기준 초과")
    if c_issue: reasons.append("소모품 조기 교체")

    return {
        "is_over":     sum([p_issue, l_issue, c_issue]) >= 2,
        "issue_count": sum([p_issue, l_issue, c_issue]),
        "p_issue": p_issue,
        "l_issue": l_issue,
        "c_issue": c_issue,
        "reasons": " / ".join(reasons) if reasons else "모든 항목이 정상 범위 내에 있습니다",
    }


def norm_space(v: Any) -> str:
    return " ".join(str(v or "").split())


def parse_llm_overrepair_verdict(diagnosis_text: str) -> Optional[bool]:
    """
    LLM 진단문의 첫 문장 강제 포맷을 해석합니다.
    - True: 과잉정비
    - False: 표준 범위
    - None: 판독 불가
    """
    text = re.sub(r"^\[[^\]]+\]\s*", "", norm_space(diagnosis_text or ""))
    if text.startswith("견적서는 다음 이유로 과잉정비입니다."):
        return True
    if text.startswith("견적서는 현재 근거 기준 표준 범위입니다."):
        return False
    return None


def split_diagnosis_text_for_display(diagnosis_text: str) -> tuple[str, str]:
    """
    '[근거: ...] 본문' 형태를 (본문, 근거)로 분리합니다.
    """
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
    evidence_raw = m.group(1)
    code_match = re.match(r"근거:\s*(.+)$", evidence_raw)
    evidence_code = code_match.group(1).strip() if code_match else ""
    evidence_kor = evidence_label_map.get(evidence_code, evidence_code)
    evidence = evidence_kor
    body = m.group(2).strip()
    return body, evidence


def norm_part(v: str) -> str:
    return re.sub(r"[\s_\-/(),.]+", "", norm_space(v).lower())


def split_parts_text(text: str) -> list[str]:
    items = re.split(r"[,/|\n]+", text or "")
    out: list[str] = []
    seen: set[str] = set()
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
    matched: list[str] = []
    for system_name, keywords in SYSTEM_KEYWORD_RULES.items():
        if any(keyword.lower() in text for keyword in keywords):
            matched.append(system_name)
    return matched


def count_direct_matches(symptom_text: str, docs: list[dict[str, Any]]) -> int:
    keys = extract_keywords(symptom_text)
    if not keys:
        return 0
    matched = 0
    for d in docs:
        hay = f"{d.get('symptom_text','')} {d.get('evidence_text','')}"
        if any(k in hay for k in keys):
            matched += 1
    return matched


def retrieve_lexical(
    conn,
    symptom_text: str,
    model_code: str,
    top_k: int,
    systems: list[str] | None = None,
) -> list[dict[str, Any]]:
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
                    to_tsvector('simple', coalesce(symptom_text, '') || ' ' || coalesce(evidence_text, '')),
                    plainto_tsquery('simple', %s)
                ) * 0.7
                + GREATEST(
                    similarity(coalesce(symptom_text, ''), %s),
                    similarity(coalesce(evidence_text, ''), %s)
                ) * 0.3
            ) AS score,
            symptom_text,
            system_category,
            repair_parts,
            pre_replace_check_rule,
            evidence_text
        FROM test.repair_doc_chunks
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
    expected_keys = [norm_part(p) for p in split_parts_text(repair_parts)]
    expected_keys = [k for k in expected_keys if k]
    return any((ek in qk) or (qk in ek) for ek in expected_keys)


def is_consumable_part(part: str) -> bool:
    key = norm_part(part)
    return any(k in key for k in CONSUMABLE_PART_KEYWORDS)


def find_unrelated_quote_parts(quote_parts: list[str], matching_results: list[dict[str, Any]]) -> list[str]:
    if not matching_results or any(len(x.get("evidence_docs", [])) == 0 for x in matching_results):
        return []
    unrelated: list[str] = []
    for qp in quote_parts:
        if is_consumable_part(qp):
            continue
        matched = False
        for sr in matching_results:
            for d in sr.get("match_docs", []):
                if part_matches_expected(qp, d.get("repair_parts", "")):
                    matched = True
                    break
            if matched:
                break
        if not matched:
            unrelated.append(qp)
    return unrelated


def extract_output_text(data: dict[str, Any]) -> str:
    text = norm_space(data.get("output_text", ""))
    if text:
        return text
    chunks: list[str] = []
    for item in data.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                chunks.append(content.get("text", ""))
    return norm_space("".join(chunks))


def strip_json_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`").strip()
        if t.lower().startswith("json"):
            t = t[4:].strip()
    return t


def llm_diagnose_multi(api_key: str, quote_parts: list[str], symptom_results: list[dict[str, Any]], timeout_sec: int = 60) -> dict[str, Any]:
    system_prompt = """너는 자동차 정비 '견적서 진단/감수' 전문가다.
역할:
- 입력된 증상과 근거 문서를 바탕으로, 견적서의 각 정비 항목이 타당한지 점검한다.
- 정비소를 대리하지도, 고객을 대리하지도 말고 문서 근거 중심으로 중립적으로 판단한다.

작성 원칙:
- 증상별로 근거를 분리해서 해석하고, 마지막에 견적서 관점으로 종합한다.
- 소모품은 이번 과잉정비 판단의 핵심 대상이 아니므로, 소모품 자체의 교체 필요를 단정하지 않는다.
- 각 증상 문구를 명시적으로 언급하고, 해당 증상과 견적 항목의 연관성을 직접 설명한다.
- 증상과의 직접 연관 근거가 약하더라도 가능한 인과가 있으면 과잉정비로 단정하지 않는다.
- 과잉정비 판정은 매우 보수적으로 한다. 명확한 무관 근거가 있을 때만 과잉정비로 표현한다.

문체:
- 견적서 감수 리포트처럼 간결하고 실무적으로 작성한다.
- 불필요한 수식어를 줄이고, 어떤 항목을 왜 그렇게 판단했는지 이유를 명시한다.
- 진단문 첫 문장에 최종 판정을 명시한다.
  - 과잉 가능성이 높으면: "견적서는 다음 이유로 과잉정비입니다."
  - 과잉 단정이 어려우면: "견적서는 현재 근거 기준 표준 범위입니다."
- 증상별 설명이 빠지면 안 되며, 최소 2개 증상이 있으면 각 증상을 모두 1회 이상 직접 언급한다.

출력은 JSON 객체만:
{
  "diagnosis_text": "짧은 1문단(2~3문장). 첫 문장은 반드시 위 두 시작문장 중 하나로 시작. 과도한 상세 항목 나열은 금지"
}
"""
    symptom_blocks: list[str] = []
    for idx, sr in enumerate(symptom_results, start=1):
        lines = []
        for i, d in enumerate(sr["evidence_docs"], start=1):
            lines.append(
                f"[{i}] source={d['document_source']} score={float(d.get('score', 0.0)):.4f} | "
                f"system={d.get('system_category', '')} | expected={d.get('repair_parts', '')} | "
                f"pre_check={d.get('pre_replace_check_rule', '')} | evidence={d.get('evidence_text', '')}"
            )
        symptom_blocks.append(
            f"증상{idx}: {sr['symptom_text']}\n"
            f"직접매칭수: {sr['direct_match_count_model']}\n"
            f"근거:\n" + ("\n".join(lines) if lines else "(없음)")
        )

    payload = {
        "model": os.getenv("OPENAI_MODEL", "gpt-5-mini"),
        "input": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"견적 부품: {', '.join(quote_parts) if quote_parts else '(없음)'}\n\n" + "\n\n".join(symptom_blocks)},
        ],
    }
    resp = requests.post(
        DEFAULT_RESPONSES_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=timeout_sec,
    )
    if not resp.ok:
        raise RuntimeError(f"LLM API 호출 실패(status={resp.status_code})")
    txt = extract_output_text(resp.json())
    parsed = json.loads(strip_json_fence(txt))
    if not isinstance(parsed, dict):
        raise RuntimeError("LLM 응답 JSON 객체 파싱 실패")
    return parsed


def run_symptom_rag_diagnosis(conn, symptom_text: str, model_code: str, quote_parts: list[str]) -> dict[str, Any]:
    symptoms = split_symptoms(symptom_text)
    if not symptoms:
        return {
            "diagnosis_text": "증상 입력이 없어 증상-정비 적합성 진단을 수행하지 않았습니다.",
            "symptom_results": [],
            "llm_called": False,
        }

    symptom_results: list[dict[str, Any]] = []
    matching_results: list[dict[str, Any]] = []
    total_model_docs = 0
    total_common_docs = 0
    for symptom in symptoms:
        inferred_systems = infer_system_filters(symptom)
        model_docs = retrieve_lexical(conn, symptom, model_code, top_k=8, systems=inferred_systems or None)
        direct_match_count = count_direct_matches(symptom, model_docs)
        common_docs: list[dict[str, Any]] = []
        if len(model_docs) < 3 or direct_match_count < 1:
            common_docs = retrieve_lexical(conn, symptom, "common", top_k=5, systems=inferred_systems or None)

        total_model_docs += len(model_docs)
        total_common_docs += len(common_docs)

        merged_docs = model_docs + common_docs
        merged_docs.sort(key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
        filtered_docs = [d for d in merged_docs if float(d.get("score", 0.0) or 0.0) >= 0.02]
        matching_results.append(
            {
                "symptom_text": symptom,
                "match_docs": merged_docs,
                "evidence_docs": filtered_docs[:3],
            }
        )
        symptom_results.append(
            {
                "symptom_text": symptom,
                "direct_match_count_model": direct_match_count,
                "evidence_docs": filtered_docs[:3],
            }
        )

    evidence_scope = "hyundai_model_pdf_plus_common" if total_model_docs > 0 and total_common_docs > 0 else (
        "hyundai_model_pdf_only" if total_model_docs > 0 else ("common_only" if total_common_docs > 0 else "no_evidence")
    )
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return {
            "diagnosis_text": f"[근거: {evidence_scope}] OPENAI_API_KEY가 없어 LLM 최종 진단을 생략했습니다.",
            "evidence_scope": evidence_scope,
            "symptom_results": symptom_results,
            "possibly_unrelated_quote_parts": find_unrelated_quote_parts(quote_parts, matching_results),
            "llm_called": False,
        }
    try:
        verdict = llm_diagnose_multi(api_key, quote_parts, symptom_results, timeout_sec=60)
        diagnosis_text = norm_space(verdict.get("diagnosis_text", ""))
        if not diagnosis_text:
            diagnosis_text = "견적서는 현재 근거 기준 표준 범위입니다."
        llm_called = True
    except Exception:
        diagnosis_text = "LLM 호출에 실패해 근거 기반 임시 진단만 제공합니다."
        llm_called = False
    return {
        "diagnosis_text": f"[근거: {evidence_scope}] {diagnosis_text}",
        "evidence_scope": evidence_scope,
        "symptom_results": symptom_results,
        "possibly_unrelated_quote_parts": find_unrelated_quote_parts(quote_parts, matching_results),
        "llm_called": llm_called,
    }


def precompute_rag_for_estimate(conn, estimate_id: str, symptom_text: str) -> str:
    """
    업로드 화면에서 분석 페이지 진입 전 RAG/LLM 결과를 미리 생성합니다.
    반환값은 fallback이 반영된 최종 estimate_id입니다.
    """
    eid = estimate_id
    estimate_meta_df = pd.read_sql("""
        SELECT car_type
        FROM test.estimates
        WHERE id = %s
        LIMIT 1
    """, conn, params=(eid,))

    if estimate_meta_df.empty and eid == "EST_FROM_UPLOAD" and ENV == "development":
        eid = "EST_20260216_001"
        estimate_meta_df = pd.read_sql("""
            SELECT car_type
            FROM test.estimates
            WHERE id = %s
            LIMIT 1
        """, conn, params=(eid,))

    car_type = estimate_meta_df.iloc[0]["car_type"] if not estimate_meta_df.empty else "차량 정보 없음"
    parts_df = pd.read_sql("""
        SELECT part_official_name
        FROM test.parts
        WHERE estimate_id = %s
    """, conn, params=(eid,))
    quote_parts = [norm_space(x) for x in parts_df["part_official_name"].dropna().tolist()] if not parts_df.empty else []
    quote_parts = list(dict.fromkeys([x for x in quote_parts if x]))

    symptom_text = norm_space(symptom_text)
    if symptom_text and car_type != "차량 정보 없음":
        cache_key = f"{eid}|{car_type}|{symptom_text}|{'|'.join(quote_parts)}"
        if st.session_state.get("rag_result_key") != cache_key or st.session_state.get("rag_result") is None:
            st.session_state.rag_result = run_symptom_rag_diagnosis(
                conn, symptom_text, car_type, quote_parts
            )
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
            <div class="topbar-logo-mark">🔧</div>
            CarCheck
        </div>
        <span class="topbar-tag">AI 정비 진단</span>
    </div>
    """, unsafe_allow_html=True)


def render_part_bar(label: str, actual: float, min_p: float, max_p: float) -> None:
    """
    가격 범위 바 렌더링.
    - 바 내부 20%~80% 구간 = 최저가~최고가
    - 실제 가격 위치에 점 표시 (범위 밖으로도 이동)
    - 최솟값·최댓값 레이블 표시
    """
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
        color, tag_cls, tag_lbl = "#1d4ed8", "tag-low",  "저렴"
    else:
        color, tag_cls, tag_lbl = "#15803d", "tag-ok",   "적정"

    B_START, B_END = 20, 80
    B_WIDTH = B_END - B_START

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


def render_labor_card(content: str, actual_fee: float,
                      std_time: Optional[float], hourly_rate: Optional[float]) -> None:
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


def render_cycle_card(content: str, current_mileage: int,
                      prev_mileage: Optional[int], cycle: Optional[int]) -> None:
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
                     title: str, subtitle: str,
                     badge_label: str, badge_cls: str) -> bool:
    """
    HTML 헤더 + Streamlit 버튼으로 아코디언을 구현합니다.
    버튼은 항상 헤더 아래 "상세보기 / 접기" 텍스트로 표시됩니다.
    Returns True if section is open.
    """
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
    """
    업로드 페이지.
    - stApp 전체 배경을 네이비로 오버라이드 → 모든 위젯이 네이비 위에 뜸
    - 업로드 존은 반투명 밝은 박스로 구별
    - file_uploader Streamlit 기본 UI 그대로 (기능 완전 보장)
    - ENV == "production" 시 샘플 버튼 미노출
    """
    # ── 이 페이지에서만 stApp 배경 네이비로 전환 ──
    st.markdown("""
    <style>
    .stApp {
        background: linear-gradient(155deg, #060e1f 0%, #0f2044 55%, #132040 100%) !important;
        color: white !important;
    }
    /* 업로더 드롭존: 네이비 위에서 구별되도록 밝게 */
    [data-testid="stFileUploaderDropzone"] {
        background: rgba(255,255,255,0.07) !important;
        border: 1.5px dashed rgba(255,255,255,0.25) !important;
        border-radius: 10px !important;
    }
    [data-testid="stFileUploaderDropzone"] p,
    [data-testid="stFileUploaderDropzone"] span,
    [data-testid="stFileUploaderDropzone"] small {
        color: rgba(255,255,255,0.6) !important;
    }
    [data-testid="stFileUploaderDropzone"] button {
        background: rgba(255,255,255,0.15) !important;
        border: 1px solid rgba(255,255,255,0.3) !important;
        color: white !important;
        border-radius: 8px !important;
    }
    [data-testid="stFileUploaderDropzone"] button:hover {
        background: rgba(255,255,255,0.25) !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # ── 헤드라인 ──
    st.markdown("""
    <div style="text-align:center; padding: 64px 20px 36px;">
        <div style="font-size:11px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;
                    color:rgba(255,255,255,0.35);margin-bottom:16px;">
            AI 기반 정비 비용 자동 분석
        </div>
        <h1 style="font-size:30px;font-weight:800;color:#fff;letter-spacing:-0.8px;
                   line-height:1.22;margin-bottom:12px;">
            내 차, 제대로<br>정비받았을까요?<br>
            <span style="color:#60a5fa;">지금 바로 확인하세요</span>
        </h1>
        <p style="font-size:14px;color:rgba(255,255,255,0.4);line-height:1.75;margin-bottom:0;">
            정비소 견적서를 업로드하면 부품비·공임비·교체주기를<br>
            시장 기준 데이터와 자동 비교해 드립니다.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ── 업로드 카드 (컬럼으로 가운데 정렬) ──
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown("""
        <div style="background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.10);
                    border-radius:14px;padding:22px 20px 16px;margin-bottom:4px;">
            <div style="font-size:14px;font-weight:700;color:#fff;margin-bottom:3px;">견적서 업로드</div>
            <div style="font-size:12px;color:rgba(255,255,255,0.38);margin-bottom:14px;">
                PDF · JPG · PNG 형식 지원
            </div>
        """, unsafe_allow_html=True)

        uploaded = st.file_uploader(
            label="견적서",
            type=["pdf", "jpg", "jpeg", "png"],
            label_visibility="collapsed",
        )

        symptom_text = st.text_area(
            "증상 설명",
            value=st.session_state.get("symptom_text", ""),
            placeholder="예: 차량이 한쪽으로 쏠린다\n예: 제동 시 소음이 발생한다",
            height=110,
        )

        if st.button(
            "진단 시작하기 →",
            use_container_width=True,
            type="primary",
            disabled=(uploaded is None),
        ):
            st.session_state.symptom_text = symptom_text.strip()
            st.session_state.rag_result = None
            st.session_state.rag_result_key = ""
            conn = get_connection()
            if not conn:
                st.error("데이터베이스에 연결할 수 없습니다. 잠시 후 다시 시도해 주세요.")
            else:
                try:
                    with st.spinner("진단 생성 중입니다. 잠시만 기다려 주세요..."):
                        resolved_eid = precompute_rag_for_estimate(
                            conn, "EST_FROM_UPLOAD", st.session_state.symptom_text
                        )
                    st.session_state.estimate_id = resolved_eid
                    st.session_state.page = "analysis"
                    st.rerun()
                except Exception as e:
                    st.error(f"진단 생성 중 오류가 발생했습니다: {e}")
                finally:
                    conn.close()

        st.markdown('</div>', unsafe_allow_html=True)

        if ENV == "development":
            st.markdown("""
            <div style="height:1px;background:rgba(255,255,255,0.1);margin:14px 0 12px;"></div>
            <div style="font-size:11px;color:rgba(255,255,255,0.3);text-align:center;margin-bottom:8px;">
                또는 샘플 데이터로 먼저 체험해보세요
            </div>
            """, unsafe_allow_html=True)
            if st.button("샘플 데이터로 체험하기", use_container_width=True):
                st.session_state.symptom_text = symptom_text.strip()
                st.session_state.rag_result = None
                st.session_state.rag_result_key = ""
                conn = get_connection()
                if not conn:
                    st.error("데이터베이스에 연결할 수 없습니다. 잠시 후 다시 시도해 주세요.")
                else:
                    try:
                        with st.spinner("진단 생성 중입니다. 잠시만 기다려 주세요..."):
                            resolved_eid = precompute_rag_for_estimate(
                                conn, "EST_20260216_001", st.session_state.symptom_text
                            )
                        st.session_state.estimate_id = resolved_eid
                        st.session_state.is_test_mode = True
                        st.session_state.page = "analysis"
                        st.rerun()
                    except Exception as e:
                        st.error(f"진단 생성 중 오류가 발생했습니다: {e}")
                    finally:
                        conn.close()

    st.markdown("""
    <div style="display:flex;justify-content:center;gap:24px;margin-top:32px;">
        <span style="font-size:11px;color:rgba(255,255,255,0.28);">개인정보 보호</span>
        <span style="font-size:11px;color:rgba(255,255,255,0.28);">30초 내 분석</span>
        <span style="font-size:11px;color:rgba(255,255,255,0.28);">무료 진단</span>
    </div>
    """, unsafe_allow_html=True)


def render_analysis_page() -> None:
    render_topbar()

    conn = get_connection()
    if not conn:
        st.error("데이터베이스에 연결할 수 없습니다. 잠시 후 다시 시도해 주세요.")
        return

    try:
        eid = st.session_state.estimate_id

        estimate_meta_df = pd.read_sql("""
            SELECT car_type, service_finish_at
            FROM test.estimates
            WHERE id = %s
            LIMIT 1
        """, conn, params=(eid,))

        if estimate_meta_df.empty and eid == "EST_FROM_UPLOAD" and ENV == "development":
            st.info("업로드 견적 파싱이 아직 연결되지 않아 샘플 견적으로 진단을 표시합니다.")
            eid = "EST_20260216_001"
            st.session_state.estimate_id = eid
            estimate_meta_df = pd.read_sql("""
                SELECT car_type, service_finish_at
                FROM test.estimates
                WHERE id = %s
                LIMIT 1
            """, conn, params=(eid,))

        parts_df = pd.read_sql("""
            SELECT
                p.part_official_name,
                p.unit_price,
                pm.min_price,
                pm.max_price
            FROM test.parts p
            JOIN test.estimates e ON p.estimate_id = e.id
            LEFT JOIN LATERAL (
                SELECT min_price, max_price
                FROM test.parts_master pm
                WHERE pm.part_official_name = p.part_official_name
                  AND pm.car_type = e.car_type
                ORDER BY pm.extracted_at DESC
                LIMIT 1
            ) pm ON TRUE
            WHERE p.estimate_id = %s;
        """, conn, params=(eid,))

        labor_df = pd.read_sql("""
            SELECT
                l.repair_content,
                l.tech_fee,
                lm.standard_repair_time,
                lm.hour_labor_rate,
                lm.change_cycle,
                e.car_mileage,
                e.car_type,
                e.service_finish_at
            FROM test.labor l
            JOIN test.estimates e ON l.estimate_id = e.id
            LEFT JOIN test.labor_master lm
                ON  lm.repair_content = l.repair_content
                AND lm.car_type       = e.car_type
                AND e.service_finish_at BETWEEN lm.start_date AND lm.end_date
            WHERE l.estimate_id = %s;
        """, conn, params=(eid,))

        summary = get_diagnosis_summary(parts_df, labor_df, conn)
        car_type = estimate_meta_df.iloc[0]["car_type"] if not estimate_meta_df.empty else "차량 정보 없음"
        svc_date = (
            str(estimate_meta_df.iloc[0]["service_finish_at"])[:10]
            if not estimate_meta_df.empty else ""
        )
        quote_parts = [norm_space(x) for x in parts_df["part_official_name"].dropna().tolist()] if not parts_df.empty else []
        quote_parts = list(dict.fromkeys([x for x in quote_parts if x]))
        symptom_text = norm_space(st.session_state.get("symptom_text", ""))
        rag_result: dict[str, Any] = st.session_state.get("rag_result") or {}
        llm_overrepair = parse_llm_overrepair_verdict(rag_result.get("diagnosis_text", ""))
        llm_issue = (llm_overrepair is True)
        effective_issue_count = summary["issue_count"] + (1 if llm_issue else 0)
        effective_is_over = summary["is_over"] or llm_issue

        st.markdown('<div class="page-wrap">', unsafe_allow_html=True)

        # ── Estimate meta strip ──
        st.markdown(f"""
        <div class="meta-strip">
            <div>
                <div class="meta-strip-label">진단 대상 견적서</div>
                <div class="meta-strip-title">{car_type}</div>
                <div class="meta-strip-sub">견적번호 {eid} &nbsp;·&nbsp; {svc_date}</div>
            </div>
            <div class="meta-strip-car">🚗</div>
        </div>
        """, unsafe_allow_html=True)

        # ── Verdict banner ──
        v_cls   = "danger" if effective_is_over else "safe"
        v_icon  = "⚠️"     if effective_is_over else "✅"
        v_title = "과잉정비 의심" if effective_is_over else "적정 정비 확인"
        summary_reason_items: list[str] = []
        if llm_issue:
            summary_reason_items.append("증상 무관 정비 포함")
        if summary["p_issue"]:
            summary_reason_items.append("부품비 과다 청구")
        if summary["l_issue"]:
            summary_reason_items.append("공임비 기준 초과")
        if summary["c_issue"]:
            summary_reason_items.append("소모품 조기 교체")
        summary_reasons = " / ".join(summary_reason_items) if summary_reason_items else "모든 항목이 정상 범위 내에 있습니다"
        st.markdown(f"""
        <div class="verdict-banner {v_cls}">
            <div class="verdict-icon">{v_icon}</div>
            <div class="verdict-main">
                <div class="verdict-title">{v_title}</div>
                <div class="verdict-desc">{summary_reasons}</div>
            </div>
            <div class="verdict-count">
                <div class="verdict-num">{effective_issue_count}</div>
                <div class="verdict-num-label">이상 항목</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Summary chips ──
        def chip(label: str, is_issue: bool) -> str:
            cls = "chip-danger" if is_issue else "chip-success"
            return f'<div class="chip {cls}"><div class="chip-dot"></div>{label}</div>'

        llm_chip_html = ""
        if llm_overrepair is not None:
            llm_chip_html = chip(
                "증상 무관 정비 포함" if llm_overrepair else "증상 무관 정비 불포함",
                llm_overrepair,
            )

        st.markdown(f"""
        <div class="chips-row">
            {llm_chip_html}
            {chip("부품비 과다" if summary["p_issue"] else "부품비 적정", summary["p_issue"])}
            {chip("공임비 초과" if summary["l_issue"] else "공임비 적정", summary["l_issue"])}
            {chip("조기 교체 의심" if summary["c_issue"] else "교체주기 적정", summary["c_issue"])}
        </div>
        """, unsafe_allow_html=True)

        # ── SECTION: 증상-정비 적합성 진단 ──
        rag_badge_label = "증상 무관 정비 포함" if llm_overrepair is True else "증상 무관 정비 불포함"
        rag_badge_cls = "badge-danger" if llm_overrepair is True else "badge-success"

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        rag_open = render_accordion(
            "rag", "🧠", "icon-blue",
            "증상-정비 적합성 진단", "증상 설명 + 차종 + 견적 부품을 근거 문서와 비교합니다",
            rag_badge_label, rag_badge_cls,
        )
        if rag_open:
            st.markdown('<div class="acc-body">', unsafe_allow_html=True)
            if not symptom_text:
                st.markdown('<div class="empty-msg">증상 설명이 없어 증상-정비 적합성 진단을 건너뜁니다</div>', unsafe_allow_html=True)
            elif car_type == "차량 정보 없음":
                st.markdown('<div class="empty-msg">차량 정보가 없어 증상-정비 적합성 진단을 수행할 수 없습니다</div>', unsafe_allow_html=True)
            else:
                st.caption("입력 증상")
                st.write(symptom_text)
                st.caption("진단 결과")
                diagnosis_body, diagnosis_evidence = split_diagnosis_text_for_display(
                    rag_result.get("diagnosis_text", "진단 결과가 없습니다.")
                )
                st.write(diagnosis_body)
                if diagnosis_evidence:
                    st.caption("근거")
                    st.write(diagnosis_evidence)
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SECTION 1: 부품비 ──
        over_cnt    = sum(
            1 for _, r in parts_df.iterrows()
            if pd.notna(r.get("max_price")) and r["unit_price"] > r["max_price"]
        ) if not parts_df.empty else 0
        p_badge_cls = "badge-danger"       if summary["p_issue"] else "badge-success"
        p_badge_lbl = f"{over_cnt}건 과다"  if summary["p_issue"] else "모두 적정"

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        parts_open = render_accordion(
            "parts", "🔩", "icon-blue",
            "부품비 적정성 분석", "시장 기준가 범위와 청구 금액을 비교합니다",
            p_badge_lbl, p_badge_cls,
        )
        if parts_open:
            st.markdown('<div class="acc-body">', unsafe_allow_html=True)
            if parts_df.empty:
                st.markdown('<div class="empty-msg">부품비 데이터가 없습니다</div>', unsafe_allow_html=True)
            else:
                for _, row in parts_df.iterrows():
                    render_part_bar(
                        row["part_official_name"], row["unit_price"],
                        row.get("min_price", float("nan")), row.get("max_price", float("nan")),
                    )
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SECTION 2: 공임비 ──
        l_badge_cls = "badge-danger"    if summary["l_issue"] else "badge-success"
        l_badge_lbl = "기준 초과 있음"  if summary["l_issue"] else "모두 적정"

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        labor_open = render_accordion(
            "labor", "🛠️", "icon-amber",
            "공임비 적정성 진단", "표준 작업시간 × 시간당 공임 기준으로 비교합니다",
            l_badge_lbl, l_badge_cls,
        )
        if labor_open:
            st.markdown('<div class="acc-body">', unsafe_allow_html=True)
            if labor_df.empty:
                st.markdown('<div class="empty-msg">공임비 데이터가 없습니다</div>', unsafe_allow_html=True)
            else:
                for _, row in labor_df.iterrows():
                    render_labor_card(
                        row["repair_content"], row["tech_fee"],
                        row.get("standard_repair_time"), row.get("hour_labor_rate"),
                    )
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SECTION 3: 교체주기 ──
        c_badge_cls = "badge-danger"    if summary["c_issue"] else "badge-success"
        c_badge_lbl = "조기 교체 의심"  if summary["c_issue"] else "주기 적정"

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        cycle_open = render_accordion(
            "cycle", "📅", "icon-teal",
            "소모품 교체주기 점검", "이전 교체 기록과 권장 주기를 비교합니다",
            c_badge_lbl, c_badge_cls,
        )
        if cycle_open:
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
                st.markdown('<div class="empty-msg">교체주기 기준 데이터가 없습니다</div>',
                            unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── 돌아가기 ──
        st.markdown('<div style="height:20px;"></div>', unsafe_allow_html=True)
        if st.button("← 처음으로 돌아가기"):
            st.session_state.update({
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
            })
            st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)  # /page-wrap

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
