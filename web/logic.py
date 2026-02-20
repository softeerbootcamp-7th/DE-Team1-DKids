import re
import json
import requests
import pandas as pd
import streamlit as st
from typing import Optional, Any

from config import (
    ENV, USER_EMAIL, DEFAULT_GEMINI_URL,
    SYSTEM_KEYWORD_RULES, CONSUMABLE_PART_KEYWORDS,
)
import os


# ─────────────────────────────────────────────
# 문자열 유틸
# ─────────────────────────────────────────────

def norm_space(v: Any) -> str:
    return " ".join(str(v or "").split())


def norm_part(v: str) -> str:
    return re.sub(r"[\s_\-/(),.]+", "", norm_space(v).lower())


def strip_json_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`").strip()
        if t.lower().startswith("json"):
            t = t[4:].strip()
    return t


# ─────────────────────────────────────────────
# 텍스트 파싱
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# 키워드 / 소모품 판별
# ─────────────────────────────────────────────

def infer_system_filters(symptom_text: str) -> list[str]:
    text = norm_space(symptom_text).lower()
    return [name for name, kws in SYSTEM_KEYWORD_RULES.items() if any(k.lower() in text for k in kws)]


def is_consumable_part(part: str) -> bool:
    key = norm_part(part)
    return any(k in key for k in CONSUMABLE_PART_KEYWORDS)


def part_matches_expected(quote_part: str, repair_parts: str) -> bool:
    qk = norm_part(quote_part)
    if not qk:
        return False
    expected_keys = [k for k in (norm_part(p) for p in split_parts_text(repair_parts)) if k]
    return any((ek in qk) or (qk in ek) for ek in expected_keys)


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


# ─────────────────────────────────────────────
# DB 쿼리
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


def retrieve_lexical(
    conn, symptom_text: str, model_code: str,
    top_k: int, systems: list[str] | None = None,
) -> list[dict[str, Any]]:
    where_sql = "WHERE vehicle_model = %s"
    params: list[Any] = [
        norm_space(symptom_text), norm_space(symptom_text),
        norm_space(symptom_text), model_code,
    ]
    if systems:
        where_sql += " AND system_category = ANY(%s)"
        params.append(systems)
    params.append(top_k)

    df = pd.read_sql(
        f"""
        SELECT
            id, document_source, vehicle_model,
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
            symptom_text, system_category, repair_parts,
            pre_replace_check_rule, evidence_text
        FROM  test.repair_doc_chunks
        {where_sql}
        ORDER BY score DESC
        LIMIT %s
        """,
        conn, params=tuple(params),
    )
    return df.to_dict(orient="records") if not df.empty else []


def count_direct_matches(symptom_text: str, docs: list[dict[str, Any]]) -> int:
    keys = extract_keywords(symptom_text)
    if not keys:
        return 0
    return sum(
        1 for d in docs
        if any(k in f"{d.get('symptom_text','')} {d.get('evidence_text','')}" for k in keys)
    )


# ─────────────────────────────────────────────
# 진단 요약
# ─────────────────────────────────────────────

def get_diagnosis_summary(
    parts_df: pd.DataFrame, labor_df: pd.DataFrame,
    conn, estimate_id: str,
) -> dict:
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
        for _, row in labor_df.iterrows():
            if pd.notna(row.get("change_cycle")):
                prev = get_prev_mileage(conn, row["repair_content"], estimate_id)
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


def build_cycle_issues(labor_df: pd.DataFrame, conn, eid: str) -> list[dict]:
    if labor_df.empty:
        return []
    curr_m = int(labor_df.iloc[0]["car_mileage"])
    issues = []
    for _, row in labor_df.iterrows():
        cyc = row.get("change_cycle")
        if cyc is None or (isinstance(cyc, float) and pd.isna(cyc)):
            continue
        cyc    = int(cyc)
        prev_m = get_prev_mileage(conn, row["repair_content"], eid)
        usage  = (curr_m - prev_m) if prev_m is not None else None

        if usage is None:
            verdict = "첫 교체"
        elif usage >= cyc:
            verdict = "교체 적절"
        elif usage >= cyc * 0.8:
            verdict = "권장 시기"
        else:
            verdict = "조기 교체"

        issues.append({
            "repair_content":  row["repair_content"],
            "current_mileage": curr_m,
            "prev_mileage":    prev_m,
            "usage":           usage if usage is not None else 0,
            "cycle":           cyc,
            "verdict":         verdict,
        })
    return issues


# ─────────────────────────────────────────────
# Gemini LLM
# ─────────────────────────────────────────────

def llm_diagnose_multi(
    api_key: str, quote_parts: list[str],
    symptom_results: list[dict[str, Any]], timeout_sec: int = 60,
) -> dict[str, Any]:
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
    txt    = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
    parsed = json.loads(strip_json_fence(txt))
    if not isinstance(parsed, dict):
        raise RuntimeError("Gemini 응답 JSON 파싱 실패")
    return parsed


# ─────────────────────────────────────────────
# RAG 진단 파이프라인
# ─────────────────────────────────────────────

def run_symptom_rag_diagnosis(
    conn, symptom_text: str, model_code: str, quote_parts: list[str],
) -> dict[str, Any]:
    symptoms = split_symptoms(symptom_text)
    if not symptoms:
        return {"diagnosis_text": "증상 입력이 없어 진단을 수행하지 않았습니다.", "symptom_results": [], "llm_called": False}

    symptom_results, matching_results = [], []
    total_model_docs = total_common_docs = 0

    for symptom in symptoms:
        inferred   = infer_system_filters(symptom)
        model_docs = retrieve_lexical(conn, symptom, model_code, top_k=8, systems=inferred or None)
        direct_match_count = count_direct_matches(symptom, model_docs)
        common_docs = []
        if len(model_docs) < 3 or direct_match_count < 1:
            common_docs = retrieve_lexical(conn, symptom, "common", top_k=5, systems=inferred or None)

        total_model_docs += len(model_docs)
        total_common_docs += len(common_docs)

        merged   = sorted(model_docs + common_docs, key=lambda x: float(x.get("score", 0) or 0), reverse=True)
        filtered = [d for d in merged if float(d.get("score", 0) or 0) >= 0.02]
        matching_results.append({"symptom_text": symptom, "match_docs": merged, "evidence_docs": filtered[:3]})
        symptom_results.append({"symptom_text": symptom, "direct_match_count_model": direct_match_count, "evidence_docs": filtered[:3]})

    evidence_scope = (
        "hyundai_model_pdf_plus_common" if total_model_docs > 0 and total_common_docs > 0
        else "hyundai_model_pdf_only"   if total_model_docs > 0
        else "common_only"              if total_common_docs > 0
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
        verdict        = llm_diagnose_multi(api_key, quote_parts, symptom_results)
        diagnosis_text = norm_space(verdict.get("diagnosis_text", "")) or "견적서는 현재 근거 기준 표준 범위입니다."
        llm_called     = True
    except Exception as e:
        diagnosis_text = f"LLM 호출 실패. ({e})"
        llm_called     = False

    return {
        "diagnosis_text": f"[근거: {evidence_scope}] {diagnosis_text}",
        "evidence_scope": evidence_scope,
        "symptom_results": symptom_results,
        "possibly_unrelated_quote_parts": find_unrelated_quote_parts(quote_parts, matching_results),
        "llm_called": llm_called,
    }


def precompute_rag_for_estimate(conn, estimate_id: str, symptom_text: str) -> str:
    eid  = estimate_id
    meta = pd.read_sql("SELECT car_type FROM test.estimates WHERE id = %s LIMIT 1", conn, params=(eid,))
    if meta.empty and eid == "EST_FROM_UPLOAD" and ENV == "development":
        eid  = "EST_20260216_001"
        meta = pd.read_sql("SELECT car_type FROM test.estimates WHERE id = %s LIMIT 1", conn, params=(eid,))

    car_type   = meta.iloc[0]["car_type"] if not meta.empty else "차량 정보 없음"
    parts_df   = pd.read_sql("SELECT part_official_name FROM test.parts WHERE estimate_id = %s", conn, params=(eid,))
    quote_parts = list(dict.fromkeys(
        x for x in (norm_space(v) for v in parts_df["part_official_name"].dropna()) if x
    ))

    symptom_text = norm_space(symptom_text)
    if symptom_text and car_type != "차량 정보 없음":
        cache_key = f"{eid}|{car_type}|{symptom_text}|{'|'.join(quote_parts)}"
        if st.session_state.get("rag_result_key") != cache_key or st.session_state.get("rag_result") is None:
            st.session_state.rag_result     = run_symptom_rag_diagnosis(conn, symptom_text, car_type, quote_parts)
            st.session_state.rag_result_key = cache_key
    else:
        st.session_state.rag_result     = None
        st.session_state.rag_result_key = ""
    return eid