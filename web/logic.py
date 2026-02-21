import re
import json
import requests
import pandas as pd
import streamlit as st
from typing import Optional, Any
from functools import lru_cache

from config import (
    ENV, USER_EMAIL, DEFAULT_GEMINI_URL,
    SYSTEM_KEYWORD_RULES, CONSUMABLE_PART_KEYWORDS,
)
import os

DEFAULT_EMBED_MODEL = "intfloat/multilingual-e5-large-instruct"


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


def to_e5_instruction_text(text: str) -> str:
    clean = norm_space(text)
    return f"Instruct: Match semantically similar automotive symptom descriptions.\nQuery: {clean}"


@lru_cache(maxsize=2)
def get_embedder(model_name: str):
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer(model_name)


def embed_query_text(text: str, embed_model: str) -> list[float]:
    embedder = get_embedder(embed_model)
    vec = embedder.encode(to_e5_instruction_text(text), normalize_embeddings=True)
    return vec.tolist()


def _to_pgvector_literal(values: list[float]) -> str:
    return "[" + ",".join(f"{float(v):.8f}" for v in values) + "]"


def retrieve_vector(
    conn,
    query_text: str,
    model_code: str,
    embed_model: str,
    top_k: int,
    systems: list[str] | None = None,
) -> list[dict[str, Any]]:
    qvec = _to_pgvector_literal(embed_query_text(query_text, embed_model))
    where_sql = "WHERE vehicle_model = %s"
    params: list[Any] = [qvec, model_code]
    if systems:
        where_sql += " AND system_category = ANY(%s)"
        params.append(systems)
    params.extend([qvec, top_k])

    df = pd.read_sql(
        f"""
        SELECT
            id, document_source, vehicle_model,
            (1 - (symptom_embedding <=> %s::vector)) AS score,
            symptom_text, system_category, repair_parts,
            pre_replace_check_rule, evidence_text
        FROM test.repair_doc_chunks
        {where_sql}
        ORDER BY symptom_embedding <=> %s::vector
        LIMIT %s
        """,
        conn, params=tuple(params),
    )
    return df.to_dict(orient="records") if not df.empty else []


def keyword_overlap_count(symptom_text: str, doc: dict[str, Any]) -> int:
    keys = set(extract_keywords(symptom_text))
    if not keys:
        return 0
    hay = norm_space(doc.get("symptom_text", ""))
    return sum(1 for k in keys if k in hay)


def apply_keyword_boost(
    docs: list[dict[str, Any]], symptom_text: str, weight: float,
) -> list[dict[str, Any]]:
    boosted: list[dict[str, Any]] = []
    for d in docs:
        hits = keyword_overlap_count(symptom_text, d)
        item = dict(d)
        item["vector_score"] = float(d.get("score", 0.0) or 0.0)
        item["keyword_hits"] = hits
        item["keyword_boost"] = hits * weight
        item["score"] = item["vector_score"] + item["keyword_boost"]
        boosted.append(item)
    boosted.sort(key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
    return boosted


def retrieve_hybrid(
    conn,
    symptom_text: str,
    model_code: str,
    embed_model: str,
    top_k: int,
    systems: list[str] | None = None,
) -> list[dict[str, Any]]:
    fetch_k = max(top_k * 4, top_k)
    vector_docs = retrieve_vector(
        conn, f"증상: {symptom_text}", model_code, embed_model, fetch_k, systems=systems
    )
    lexical_docs = retrieve_lexical(conn, symptom_text, model_code, fetch_k, systems=systems)

    vector_rank_by_id = {int(d["id"]): idx for idx, d in enumerate(vector_docs, start=1)}
    lexical_rank_by_id = {int(d["id"]): idx for idx, d in enumerate(lexical_docs, start=1)}

    docs_by_id: dict[int, dict[str, Any]] = {}
    for d in vector_docs:
        docs_by_id[int(d["id"])] = dict(d)
    for d in lexical_docs:
        doc_id = int(d["id"])
        if doc_id not in docs_by_id:
            docs_by_id[doc_id] = dict(d)

    rrf_k = 60.0
    fused: list[dict[str, Any]] = []
    for doc_id, d in docs_by_id.items():
        rv = 1.0 / (rrf_k + vector_rank_by_id[doc_id]) if doc_id in vector_rank_by_id else 0.0
        rl = 1.0 / (rrf_k + lexical_rank_by_id[doc_id]) if doc_id in lexical_rank_by_id else 0.0
        item = dict(d)
        item["vector_rrf"] = rv
        item["lexical_rrf"] = rl
        item["score"] = rv + rl
        fused.append(item)

    fused.sort(key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
    return fused[:top_k]


def count_direct_matches(symptom_text: str, docs: list[dict[str, Any]]) -> int:
    keys = extract_keywords(symptom_text)
    if not keys:
        return 0
    return sum(
        1 for d in docs
        if any(k in f"{d.get('symptom_text','')} {d.get('evidence_text','')}" for k in keys)
    )


def compact_query(text: str, max_keywords: int = 10) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for token in extract_keywords(text):
        k = token.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(token)
        if len(out) >= max_keywords:
            break
    return " ".join(out) if out else norm_space(text)


def validate_symptom_similarity_batch(
    api_key: str,
    user_symptom: str,
    candidates: list[dict[str, Any]],
    keep_cutoff: float,
    timeout_sec: int = 60,
) -> list[dict[str, Any]]:
    if not candidates:
        return []

    items = []
    for c in candidates:
        items.append({
            "id": int(c["id"]),
            "candidate_symptom_text": norm_space(c.get("symptom_text", "")),
            "evidence_text": norm_space(c.get("evidence_text", "")),
            "system_category": norm_space(c.get("system_category", "")),
            "document_source": norm_space(c.get("document_source", "")),
        })

    prompt = (
        "너는 자동차 증상 유사도 검증 에이전트다.\n"
        "입력된 user_symptom과 candidate_symptom_text가 실제로 같은 증상 계열인지 판정하라.\n"
        "JSON만 출력:\n"
        "{\n"
        '  "results":[{"id":123,"similarity_score":0.0,"keep":true,"reason":"한 줄"}]\n'
        "}\n"
        f"keep 기준: similarity_score >= {keep_cutoff:.2f}\n"
        "주의: 단순 키워드 겹침만으로 keep 금지.\n\n"
        f"[user_symptom]\n{norm_space(user_symptom)}\n\n"
        f"[candidates]\n{json.dumps(items, ensure_ascii=False)}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.0, "responseMimeType": "application/json"},
    }
    resp = requests.post(
        f"{DEFAULT_GEMINI_URL}?key={api_key}",
        headers={"Content-Type": "application/json"},
        json=payload, timeout=timeout_sec,
    )
    if not resp.ok:
        raise RuntimeError(f"유사도 검증 API 실패(status={resp.status_code}): {resp.text[:300]}")

    text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
    parsed = json.loads(strip_json_fence(text))
    if not isinstance(parsed, dict):
        return []

    results = []
    for r in parsed.get("results", []):
        try:
            doc_id = int(r.get("id"))
            score = float(r.get("similarity_score", 0.0) or 0.0)
            keep = bool(r.get("keep", False)) and score >= keep_cutoff
            reason = norm_space(r.get("reason", ""))
            results.append({
                "id": doc_id,
                "similarity_score": score,
                "keep": keep,
                "reason": reason,
            })
        except Exception:
            continue
    return results


def iterative_retrieve_and_validate(
    conn,
    api_key: str,
    user_symptom: str,
    model_code: str,
    embed_model: str,
    systems: list[str] | None = None,
    top_k: int = 20,
    max_iterations: int = 3,
    keep_cutoff: float = 0.72,
    min_keep: int = 2,
    max_keep: int = 5,
    min_avg_score: float = 0.65,
    keyword_boost_weight: float = 0.0,
) -> dict[str, Any]:
    query = norm_space(user_symptom)
    traces: list[dict[str, Any]] = []
    final_kept: list[dict[str, Any]] = []
    status = "insufficient_evidence"
    used_model_docs = 0
    used_common_docs = 0
    model_phase_max_iter = min(2, max_iterations)

    for i in range(1, max_iterations + 1):
        in_model_phase = i <= model_phase_max_iter
        phase = "model_only" if in_model_phase else "common_fallback"
        phase_model = model_code if in_model_phase else "common"

        candidates = retrieve_hybrid(
            conn, query, phase_model, embed_model, top_k, systems=systems or None
        )
        candidates = apply_keyword_boost(candidates, query, keyword_boost_weight)

        if in_model_phase:
            used_model_docs += len(candidates)
        else:
            used_common_docs += len(candidates)

        if not api_key:
            kept = candidates[:max_keep]
            traces.append({
                "iteration": i,
                "phase": phase,
                "model_used": phase_model,
                "query": query,
                "candidate_count": len(candidates),
                "kept_count": len(kept),
                "avg_similarity_score": 0.0,
            })
            final_kept = kept
            status = "insufficient_evidence"
            break

        validated = validate_symptom_similarity_batch(api_key, user_symptom, candidates, keep_cutoff)
        vmap = {int(v["id"]): v for v in validated}
        scored = []
        for c in candidates:
            did = int(c["id"])
            v = vmap.get(did, {"similarity_score": 0.0, "keep": False, "reason": "검증 결과 없음"})
            item = dict(c)
            item["similarity_score"] = float(v.get("similarity_score", 0.0) or 0.0)
            item["keep"] = bool(v.get("keep", False))
            item["reason"] = norm_space(v.get("reason", ""))
            scored.append(item)

        kept = [x for x in scored if x.get("keep", False)]
        kept.sort(
            key=lambda x: (float(x.get("similarity_score", 0.0) or 0.0), float(x.get("score", 0.0) or 0.0)),
            reverse=True,
        )
        kept = kept[:max_keep]
        avg_score = (
            sum(float(x.get("similarity_score", 0.0) or 0.0) for x in kept) / len(kept)
            if kept else 0.0
        )
        traces.append({
            "iteration": i,
            "phase": phase,
            "model_used": phase_model,
            "query": query,
            "candidate_count": len(candidates),
            "kept_count": len(kept),
            "avg_similarity_score": round(avg_score, 4),
        })

        final_kept = kept
        if len(kept) >= min_keep and avg_score >= min_avg_score:
            status = "ok"
            break

        if i < max_iterations:
            if i == model_phase_max_iter:
                query = norm_space(user_symptom)
            else:
                query = compact_query(query)

    return {
        "status": status,
        "kept_docs": final_kept,
        "traces": traces,
        "used_model_docs": used_model_docs,
        "used_common_docs": used_common_docs,
    }


# ─────────────────────────────────────────────
# 진단 요약
# ─────────────────────────────────────────────

def get_diagnosis_summary(
    parts_df: pd.DataFrame, labor_df: pd.DataFrame,
    conn, estimate_id: str,
) -> dict:
    p_issue = False
    if not parts_df.empty and "max_price" in parts_df.columns:
        valid_parts = parts_df[
            parts_df["unit_price"].notna() &
            parts_df["max_price"].notna()
        ]
        if not valid_parts.empty:
            p_issue = any(valid_parts["unit_price"] > valid_parts["max_price"])
    
    l_issue = False
    if not labor_df.empty:
        valid_labor = labor_df[
            labor_df["tech_fee"].notna() &
            labor_df["standard_repair_time"].notna() &
            labor_df["hour_labor_rate"].notna()
        ]
        if not valid_labor.empty:
            l_issue = any(
                valid_labor["tech_fee"] >
                valid_labor["standard_repair_time"] *
                valid_labor["hour_labor_rate"]
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
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    embed_model = os.getenv("RAG_EMBED_MODEL", DEFAULT_EMBED_MODEL).strip() or DEFAULT_EMBED_MODEL
    min_model_score = 0.02
    min_evidence_score = 0.02
    evidence_per_symptom = 3
    keyword_boost_weight = 0.0
    top_k_candidates = 20
    max_iterations = 3
    keep_cutoff = 0.72
    min_keep = 2
    max_keep = 5
    min_avg_score = 0.65

    for symptom in symptoms:
        inferred = infer_system_filters(symptom)
        iter_result = iterative_retrieve_and_validate(
            conn=conn,
            api_key=api_key,
            user_symptom=symptom,
            model_code=model_code,
            embed_model=embed_model,
            systems=inferred or None,
            top_k=top_k_candidates,
            max_iterations=max_iterations,
            keep_cutoff=keep_cutoff,
            min_keep=min_keep,
            max_keep=max_keep,
            min_avg_score=min_avg_score,
            keyword_boost_weight=keyword_boost_weight,
        )
        merged = sorted(
            iter_result.get("kept_docs", []),
            key=lambda x: (
                float(x.get("similarity_score", 0.0) or 0.0),
                float(x.get("score", 0.0) or 0.0),
            ),
            reverse=True,
        )
        filtered = [d for d in merged if float(d.get("score", 0) or 0) >= min_evidence_score]
        direct_match_count = count_direct_matches(symptom, merged)
        total_model_docs += int(iter_result.get("used_model_docs", 0) or 0)
        total_common_docs += int(iter_result.get("used_common_docs", 0) or 0)

        if not merged:
            fallback = retrieve_hybrid(conn, symptom, model_code, embed_model, 5, systems=inferred or None)
            fallback = apply_keyword_boost(fallback, symptom, keyword_boost_weight)
            merged = fallback
            filtered = [d for d in merged if float(d.get("score", 0) or 0) >= min_model_score][:evidence_per_symptom]

        matching_results.append({
            "symptom_text": symptom,
            "match_docs": merged,
            "evidence_docs": filtered[:evidence_per_symptom],
        })
        symptom_results.append({
            "symptom_text": symptom,
            "direct_match_count_model": direct_match_count,
            "inferred_system_filters": inferred,
            "used_model_docs": int(iter_result.get("used_model_docs", 0) or 0),
            "used_common_docs": int(iter_result.get("used_common_docs", 0) or 0),
            "match_status": iter_result.get("status", "insufficient_evidence"),
            "match_traces": iter_result.get("traces", []),
            "evidence_docs": filtered[:evidence_per_symptom],
        })

    evidence_scope = (
        "hyundai_model_pdf_plus_common" if total_model_docs > 0 and total_common_docs > 0
        else "hyundai_model_pdf_only"   if total_model_docs > 0
        else "common_only"              if total_common_docs > 0
        else "no_evidence"
    )

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
    
# 기존 import 그대로 유지
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
from datetime import datetime
import base64


# -----------------------------
# (기존 logic.py 내용 전부 동일)
# -----------------------------
# ⚠️ 기존 코드들은 그대로 두면 된다.
# 여기서는 추가된 함수들만 아래에 붙인다.
# -----------------------------


# =========================================================
# OCR → JSON 구조화
# =========================================================

def extract_estimate_from_image(image_bytes: bytes) -> dict:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY 없음")

    b64 = base64.b64encode(image_bytes).decode()

    prompt = """
    자동차 정비 견적서를 분석하여 JSON으로 반환하라.

    {
        "car_type": "...",
        "car_mileage": 120000,
        "service_finish_at": "YYYY-MM-DD",
        "parts": [
            {"name": "...", "unit_price": 100000}
        ],
        "labor": [
            {"repair_content": "...", "tech_fee": 80000}
        ]
    }

    JSON만 출력.
    """

    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {
                    "inlineData": {
                        "mimeType": "image/png",
                        "data": b64
                    }
                }
            ]
        }],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json"
        }
    }

    resp = requests.post(
        f"{DEFAULT_GEMINI_URL}?key={api_key}",
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=60
    )

    if not resp.ok:
        raise RuntimeError(resp.text)

    text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
    parsed = json.loads(text.strip("` \n"))

    if isinstance(parsed, list):
        if len(parsed) > 0 and isinstance(parsed[0], dict):
            parsed = parsed[0]
        else:
            raise RuntimeError("OCR 결과 형식이 올바르지 않습니다.")

    if not isinstance(parsed, dict):
        raise RuntimeError("OCR 결과가 JSON 객체가 아닙니다.")

    return parsed


# =========================================================
# DB INSERT
# =========================================================

def insert_estimate(conn, estimate_id: str, data: dict):
    cur = conn.cursor()

    # 중복 체크
    cur.execute("SELECT 1 FROM test.estimates WHERE id = %s", (estimate_id,))
    if cur.fetchone():
        raise RuntimeError("이미 존재하는 견적서 ID입니다.")

    # estimates
    cur.execute("""
        INSERT INTO test.estimates
        (id, customer_id, image_url, car_type, car_mileage, service_finish_at, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        estimate_id,
        USER_EMAIL,
        None,
        data.get("car_type"),
        int(data.get("car_mileage", 0)),
        data.get("service_finish_at"),
        datetime.now()
    ))

    # parts
    for idx, p in enumerate(data.get("parts", []), start=1):
        cur.execute("""
            INSERT INTO test.parts
            (estimate_id, no, part_official_name, unit_price)
            VALUES (%s, %s, %s, %s)
        """, (
            estimate_id,
            idx,
            p.get("name"),
            int(str(p.get("unit_price", 0)).replace(",", ""))
        ))

    # labor
    for idx, l in enumerate(data.get("labor", []), start=1):
        cur.execute("""
            INSERT INTO test.labor
            (estimate_id, no, repair_content, tech_fee)
            VALUES (%s, %s, %s, %s)
        """, (
            estimate_id,
            idx,
            l.get("repair_content"),
            int(str(l.get("tech_fee", 0)).replace(",", ""))
        ))

    conn.commit()
