import pandas as pd
import streamlit as st
from typing import Optional
from datetime import datetime

from logic import norm_space, split_diagnosis_text_for_display
from pdf_report import generate_diagnosis_pdf


# ─────────────────────────────────────────────
# 네비바
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


# ─────────────────────────────────────────────
# 부품비 바
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# 공임비 카드
# ─────────────────────────────────────────────

def render_labor_card(
    content: str, actual_fee: float,
    std_time: Optional[float], hourly_rate: Optional[float],
) -> None:
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


# ─────────────────────────────────────────────
# 교체주기 카드
# ─────────────────────────────────────────────

def render_cycle_card(
    content: str, current_mileage: int,
    prev_mileage: Optional[int], cycle: Optional[int],
) -> None:
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
        badge_cls, badge_lbl, bar_color = "badge-danger", "조기 교체 의심", "#dc2626"
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


# ─────────────────────────────────────────────
# 아코디언
# ─────────────────────────────────────────────

def render_accordion(
    section_id: str, icon: str, icon_cls: str,
    title: str, subtitle: str, badge_label: str, badge_cls: str,
) -> bool:
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
# PDF 저장 버튼
# ─────────────────────────────────────────────

def _get_pdf_bytes(
    parts_df, labor_df, summary, rag_result,
    symptom_text, car_type, svc_date, estimate_id, cycle_issues,
) -> bytes:
    @st.cache_data(show_spinner=False)
    def _cached(eid: str, sym: str) -> bytes:
        return generate_diagnosis_pdf(
            parts_df=parts_df, labor_df=labor_df, summary=summary,
            rag_result=rag_result, symptom_text=symptom_text,
            car_type=car_type, svc_date=svc_date,
            estimate_id=eid, cycle_issues=cycle_issues,
        )
    return _cached(estimate_id, symptom_text)


def render_pdf_button(
    parts_df, labor_df, summary, rag_result,
    symptom_text, car_type, svc_date, estimate_id, cycle_issues,
) -> None:
    """판정 배너 위 우측 정렬 PDF 저장 버튼."""
    try:
        pdf_bytes = _get_pdf_bytes(
            parts_df, labor_df, summary, rag_result,
            symptom_text, car_type, svc_date, estimate_id, cycle_issues,
        )
        filename = f"CarCheck_{estimate_id}_{datetime.now().strftime('%Y%m%d')}.pdf"
        _, col = st.columns([6, 2])
        with col:
            st.download_button(
                label="📄 진단 보고서 저장",
                data=pdf_bytes,
                file_name=filename,
                mime="application/pdf",
                use_container_width=True,
                key="pdf_top",
            )
    except Exception:
        pass