import pandas as pd
import streamlit as st
from typing import Any
from datetime import datetime
from html import escape
from config import ENV
from db import get_connection
from logic import (
    norm_space, parse_llm_overrepair_verdict, split_diagnosis_text_for_display,
    get_diagnosis_summary, build_cycle_issues, get_prev_mileage,
    precompute_rag_for_estimate,
    extract_estimate_from_image, insert_estimate,
)
from components import (
    render_topbar, render_part_bar, render_labor_card,
    render_cycle_card, render_accordion, render_pdf_button,
)


# ─────────────────────────────────────────────
# 업로드 페이지
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
    .stTextArea label, .stTextArea label p,
    div[data-testid="stTextArea"] label,
    div[data-testid="stTextArea"] label p {
        color: #ffffff !important; font-size: 14px !important;
        font-weight: 700 !important; opacity: 1 !important;
    }
    .stTextArea textarea {
        background: #ffffff !important; border: 1px solid rgba(0,0,0,0.15) !important;
        color: #000000 !important; border-radius: 8px !important; caret-color: #000000 !important;
    }
    .stTextArea textarea::placeholder { color: #9ca3af !important; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="text-align:center; padding:68px 20px 36px;">
        <h1 style="font-size:30px; font-weight:900; color:#fff;
                   letter-spacing:-0.8px; line-height:1.28; margin-bottom:18px;">
            정비 후 받은 견적서,<br>
            <span style="color:#93c5fd;">믿을 수 있나요?</span>
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

        uploaded     = st.file_uploader("견적서", type=["jpg", "jpeg", "png"], label_visibility="collapsed")
        estimate_id_input = st.text_input(
            "견적서 ID (선택 입력)",
            placeholder="예: 2026-02-16-A001"
        )
        symptom_text = st.text_area(
            "증상",
            value=st.session_state.get("symptom_text", ""),
            placeholder="예: 달릴 때 덜그덕거리는 소리가 남",
            height=100,
        )

        if st.button("진단 시작", use_container_width=True, type="primary", disabled=(uploaded is None)):

            if uploaded is None:
                st.warning("이미지를 업로드하세요.")
                return

            conn = get_connection()
            if not conn:
                st.error("데이터베이스에 연결할 수 없습니다.")
                return

            try:
                with st.spinner("OCR 분석 및 저장 중..."):

                    image_bytes = uploaded.read()

                    # 1️⃣ OCR 수행
                    estimate_data = extract_estimate_from_image(image_bytes)

                    # 2️⃣ 견적서 ID 결정
                    if estimate_id_input.strip():
                        new_estimate_id = estimate_id_input.strip()
                    else:
                        new_estimate_id = "EST_" + datetime.now().strftime("%Y%m%d%H%M%S")

                    # 3️⃣ DB 저장
                    insert_estimate(conn, new_estimate_id, estimate_data)

                st.success("견적서 저장 완료")

                # 4️⃣ 기존 분석 흐름 유지
                _start_diagnosis(symptom_text, new_estimate_id)

            except Exception as e:
                conn.rollback()
                st.error(f"OCR 또는 저장 중 오류 발생: {e}")
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
                _start_diagnosis(symptom_text, "EST_20260216_001", is_test=True)


def _start_diagnosis(symptom_text: str, estimate_id: str, is_test: bool = False) -> None:
    st.session_state.symptom_text  = symptom_text.strip()
    st.session_state.rag_result    = None
    st.session_state.rag_result_key = ""
    conn = get_connection()
    if not conn:
        st.error("데이터베이스에 연결할 수 없습니다.")
        return
    try:
        with st.spinner("분석 중..."):
            resolved_eid = precompute_rag_for_estimate(conn, estimate_id, st.session_state.symptom_text)
        st.session_state.estimate_id  = resolved_eid
        st.session_state.is_test_mode = is_test
        st.session_state.page         = "analysis"
        st.rerun()
    except Exception as e:
        st.error(f"오류: {e}")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# 분석 페이지
# ─────────────────────────────────────────────

def render_analysis_page() -> None:
    render_topbar()
    conn = get_connection()
    if not conn:
        st.error("데이터베이스에 연결할 수 없습니다.")
        return

    try:
        eid = st.session_state.estimate_id
        eid, parts_df, labor_df, car_type, svc_date = _load_estimate_data(conn, eid)

        summary      = get_diagnosis_summary(parts_df, labor_df, conn, eid)
        symptom_text = norm_space(st.session_state.get("symptom_text", ""))
        rag_result: dict[str, Any] = st.session_state.get("rag_result") or {}
        cycle_issues = build_cycle_issues(labor_df, conn, eid)

        llm_overrepair        = parse_llm_overrepair_verdict(rag_result.get("diagnosis_text", ""))
        llm_issue             = (llm_overrepair is True)
        effective_issue_count = summary["issue_count"] + (1 if llm_issue else 0)
        effective_is_over     = summary["is_over"] or llm_issue

        st.markdown('<div class="page-wrap">', unsafe_allow_html=True)

        # PDF 저장 버튼
        render_pdf_button(
            parts_df, labor_df, summary, rag_result,
            symptom_text, car_type, svc_date, eid, cycle_issues,
        )

        # 판정 배너
        _render_verdict_banner(effective_is_over, effective_issue_count)

        # 이슈 카드
        if effective_is_over:
            _render_issue_cards(summary, rag_result, parts_df, labor_df, llm_issue)

        # 상태 칩
        _render_chips(summary, llm_overrepair)

        # 섹션: 증상 분석
        _render_rag_section(rag_result, symptom_text, llm_overrepair)

        # 섹션: 부품비
        _render_parts_section(summary, parts_df)

        # 섹션: 공임비
        _render_labor_section(summary, labor_df)

        # 섹션: 교체주기
        _render_cycle_section(summary, labor_df, conn, eid)

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
# 내부 헬퍼
# ─────────────────────────────────────────────

def _load_estimate_data(conn, eid: str):
    meta = pd.read_sql(
        "SELECT car_type, service_finish_at FROM test.estimates WHERE id = %s LIMIT 1",
        conn, params=(eid,),
    )
    if meta.empty and eid == "EST_FROM_UPLOAD" and ENV == "development":
        st.info("업로드 견적 파싱이 아직 연결되지 않아 샘플 견적으로 진단을 표시합니다.")
        eid  = "EST_20260216_001"
        st.session_state.estimate_id = eid
        meta = pd.read_sql(
            "SELECT car_type, service_finish_at FROM test.estimates WHERE id = %s LIMIT 1",
            conn, params=(eid,),
        )

    parts_df = pd.read_sql("""
        SELECT p.part_official_name, p.unit_price, pm.min_price, pm.max_price
        FROM  test.parts p
        JOIN  test.estimates e ON p.estimate_id = e.id
        LEFT  JOIN LATERAL (
            SELECT min_price, max_price
            FROM   test.parts_master pm
            WHERE  pm.part_official_name = p.part_official_name
              AND  pm.car_type           = e.car_type
              AND  pm.extracted_at       = e.service_finish_at
        ) pm ON TRUE
        WHERE p.estimate_id = %s
    """, conn, params=(eid,))

    labor_df = pd.read_sql("""
        SELECT l.repair_content, l.tech_fee,
               lm.standard_repair_time, lm.hour_labor_rate, lm.change_cycle,
               e.car_mileage, e.car_type, e.service_finish_at
        FROM  test.labor l
        JOIN  test.estimates e ON l.estimate_id = e.id
        LEFT  JOIN test.labor_master lm
            ON  lm.repair_content = l.repair_content
            AND lm.car_type       = e.car_type
            AND e.service_finish_at BETWEEN lm.start_date AND lm.end_date
        WHERE l.estimate_id = %s
    """, conn, params=(eid,))

    car_type = meta.iloc[0]["car_type"] if not meta.empty else "차량 정보 없음"
    svc_date = str(meta.iloc[0]["service_finish_at"])[:10] if not meta.empty else ""
    return eid, parts_df, labor_df, car_type, svc_date


def _render_verdict_banner(is_over: bool, issue_count: int) -> None:
    v_cls   = "danger" if is_over else "safe"
    v_title = "과잉정비 의심" if is_over else "이상 없음"
    v_sub   = "아래 항목에서 이상이 감지되었습니다." if is_over else "부품비·공임비·교체주기 모두 정상입니다."
    st.markdown(f"""
    <div class="top-verdict {v_cls}">
        <div class="top-verdict-body">
            <div class="top-verdict-title">{v_title}</div>
            <div class="top-verdict-sub">{v_sub}</div>
        </div>
        <div class="top-verdict-badge">
            <div class="top-verdict-num">{issue_count}</div>
            <div class="top-verdict-num-label">이상 항목</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_issue_cards(summary, rag_result, parts_df, labor_df, llm_issue: bool) -> None:
    if llm_issue:
        body, _ = split_diagnosis_text_for_display(rag_result.get("diagnosis_text", ""))
        st.markdown(f"""
        <div class="issue-card">
            <div class="issue-card-header"><span class="issue-card-title">증상 무관 정비 포함</span></div>
            <div class="issue-card-body">{body}</div>
        </div>
        """, unsafe_allow_html=True)

    if summary["p_issue"]:
        rows = "<br>".join(
            f"{r['part_official_name']} — {r['unit_price']:,.0f}원 (최고 기준가 {r['max_price']:,.0f}원)"
            for _, r in parts_df.iterrows()
            if pd.notna(r.get("max_price")) and r["unit_price"] > r["max_price"]
        )
        st.markdown(f"""
        <div class="issue-card">
            <div class="issue-card-header"><span class="issue-card-title">부품비 과다 청구</span></div>
            <div class="issue-card-body">{rows}</div>
        </div>
        """, unsafe_allow_html=True)

    if summary["l_issue"]:
        rows = "<br>".join(
            f"{r['repair_content']} — {r['tech_fee']:,.0f}원 (기준 {r['standard_repair_time']*r['hour_labor_rate']:,.0f}원)"
            for _, r in labor_df.iterrows()
            if pd.notna(r.get("standard_repair_time")) and pd.notna(r.get("hour_labor_rate"))
            and r["tech_fee"] > r["standard_repair_time"] * r["hour_labor_rate"]
        )
        st.markdown(f"""
        <div class="issue-card">
            <div class="issue-card-header"><span class="issue-card-title">공임비 기준 초과</span></div>
            <div class="issue-card-body">{rows}</div>
        </div>
        """, unsafe_allow_html=True)

    if summary["c_issue"]:
        st.markdown("""
        <div class="issue-card">
            <div class="issue-card-header"><span class="issue-card-title">소모품 조기 교체 의심</span></div>
            <div class="issue-card-body">권장 교체 주기 이전에 소모품이 교체되었습니다. 아래 상세보기에서 확인하세요.</div>
        </div>
        """, unsafe_allow_html=True)


def _render_chips(summary, llm_overrepair) -> None:
    def chip(label, is_issue):
        cls = "chip-danger" if is_issue else "chip-success"
        return f'<div class="chip {cls}"><div class="chip-dot"></div>{label}</div>'

    html = ""
    if llm_overrepair is not None:
        html += chip("증상 무관 정비 포함" if llm_overrepair else "증상 무관 정비 없음", llm_overrepair)
    html += chip("부품비 과다" if summary["p_issue"] else "부품비 적정", summary["p_issue"])
    html += chip("공임비 초과" if summary["l_issue"] else "공임비 적정", summary["l_issue"])
    html += chip("조기 교체 의심" if summary["c_issue"] else "교체주기 적정", summary["c_issue"])
    st.markdown(f'<div class="chips-row">{html}</div>', unsafe_allow_html=True)


def _render_rag_section(rag_result, symptom_text, llm_overrepair) -> None:
    badge_cls = "badge-danger" if llm_overrepair is True else "badge-success"
    badge_lbl = "증상 무관 정비 포함" if llm_overrepair is True else "증상 무관 정비 없음"
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    if render_accordion("rag", "AI", "icon-blue", "증상-정비 적합성 진단", "증상·차종·견적 부품을 근거 문서와 비교", badge_lbl, badge_cls):
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
            evidence_items = rag_result.get("evidence_explanations") or []
            if evidence_items:
                st.caption("근거 상세")
                for item in evidence_items:
                    source_label = item.get("source_label", "문서 근거")
                    related_symptom = item.get("related_symptom", "")
                    evidence_excerpt = item.get("evidence_excerpt", "")
                    repair_parts = item.get("repair_parts", "")
                    blocks = [
                        f'<div class="evidence-source">{escape(str(source_label))}</div>',
                    ]
                    if related_symptom:
                        blocks.append(
                            f'<div class="evidence-row"><span class="evidence-label">관련 증상</span>'
                            f'<span class="evidence-value">{escape(str(related_symptom))}</span></div>'
                        )
                    if evidence_excerpt:
                        blocks.append(
                            f'<div class="evidence-row"><span class="evidence-label">근거 요약</span>'
                            f'<span class="evidence-value">{escape(str(evidence_excerpt))}</span></div>'
                        )
                    if repair_parts:
                        blocks.append(
                            f'<div class="evidence-row"><span class="evidence-label">예상 정비항목</span>'
                            f'<span class="evidence-value">{escape(str(repair_parts))}</span></div>'
                        )
                    st.markdown(
                        f'<div class="evidence-card">{"".join(blocks)}</div>',
                        unsafe_allow_html=True,
                    )
        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def _render_parts_section(summary, parts_df) -> None:
    over_cnt  = sum(1 for _, r in parts_df.iterrows() if pd.notna(r.get("max_price")) and r["unit_price"] > r["max_price"]) if not parts_df.empty else 0
    badge_cls = "badge-danger" if summary["p_issue"] else "badge-success"
    badge_lbl = f"{over_cnt}건 과다" if summary["p_issue"] else "모두 적정"
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    if render_accordion("parts", "P", "icon-blue", "부품비 적정성 분석", "시장 기준가 범위와 청구 금액 비교", badge_lbl, badge_cls):
        st.markdown('<div class="acc-body">', unsafe_allow_html=True)
        if parts_df.empty:
            st.markdown('<div class="empty-msg">부품비 데이터가 없습니다</div>', unsafe_allow_html=True)
        else:
            for _, row in parts_df.iterrows():
                render_part_bar(row["part_official_name"], row["unit_price"], row.get("min_price", float("nan")), row.get("max_price", float("nan")))
        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def _render_labor_section(summary, labor_df) -> None:
    badge_cls = "badge-danger" if summary["l_issue"] else "badge-success"
    badge_lbl = "기준 초과 있음" if summary["l_issue"] else "모두 적정"
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    if render_accordion("labor", "L", "icon-amber", "공임비 적정성 진단", "표준 작업시간 × 시간당 공임 기준 비교", badge_lbl, badge_cls):
        st.markdown('<div class="acc-body">', unsafe_allow_html=True)
        if labor_df.empty:
            st.markdown('<div class="empty-msg">공임비 데이터가 없습니다</div>', unsafe_allow_html=True)
        else:
            for _, row in labor_df.iterrows():
                render_labor_card(row["repair_content"], row["tech_fee"], row.get("standard_repair_time"), row.get("hour_labor_rate"))
        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def _render_cycle_section(summary, labor_df, conn, eid) -> None:
    badge_cls = "badge-danger" if summary["c_issue"] else "badge-success"
    badge_lbl = "조기 교체 의심" if summary["c_issue"] else "주기 적정"
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    if render_accordion("cycle", "R", "icon-teal", "소모품 교체주기 점검", "이전 교체 기록과 권장 주기 비교", badge_lbl, badge_cls):
        st.markdown('<div class="acc-body">', unsafe_allow_html=True)
        has_cycle = False
        if not labor_df.empty:
            curr_m = int(labor_df.iloc[0]["car_mileage"])
            for _, row in labor_df.iterrows():
                cyc = row.get("change_cycle")
                if cyc is None or (isinstance(cyc, float) and pd.isna(cyc)):
                    continue
                has_cycle = True
                prev_m    = get_prev_mileage(conn, row["repair_content"], eid)
                render_cycle_card(row["repair_content"], curr_m, prev_m, int(cyc))
        if not has_cycle:
            st.markdown('<div class="empty-msg">교체주기 기준 데이터가 없습니다</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
