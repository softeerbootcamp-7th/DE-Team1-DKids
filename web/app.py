import os
import re
import pandas as pd
import streamlit as st
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from db import get_connection

# Load environmental variables
load_dotenv()

# Global environment toggle: Set to "production" to hide debug features
ENV: str = "development"
USER_EMAIL: str = "test@example.com"  # Placeholder for session-based user email

def init_session_state() -> None:
    """Initializes the Streamlit session state variables.
    
    Sets up the initial page, test mode flags, and estimate ID within the session.
    """
    if "page" not in st.session_state:
        st.session_state.page = "upload"
    if "is_test_mode" not in st.session_state:
        st.session_state.is_test_mode = False
    if "estimate_id" not in st.session_state:
        st.session_state.estimate_id = None

def render_part_bar(label: str, actual: float, min_p: float, max_p: float) -> None:
    """Renders a dynamic price bar where dots move proportionally even outside limits.

    Args:
        label (str): Name of the part.
        actual (float): Actual price from the estimate.
        min_p (float): Minimum reference price.
        max_p (float): Maximum reference price.
    """
    if pd.isna(min_p) or pd.isna(max_p) or min_p == 0 or max_p == 0:
        st.warning(f"{label}: 가격 기준 데이터 없음")
        return

    if actual < min_p:
        color, status = "#1976d2", "저렴"
    elif actual > max_p:
        color, status = "#d32f2f", "비쌈"
    else:
        color, status = "#2e7d32", "적정"

    bar_start, bar_end = 20, 80
    bar_width = bar_end - bar_start
    
    if actual < min_p:
        ratio = (min_p - actual) / min_p
        position = bar_start - (ratio * 50)
    elif actual > max_p:
        ratio = (actual - max_p) / max_p
        position = bar_end + (ratio * 50)
    else:
        price_range = max_p - min_p
        inner_ratio = (actual - min_p) / price_range if price_range > 0 else 0.5
        position = bar_start + (inner_ratio * bar_width)
    
    position = max(5, min(95, position))

    html = f"""
    <div style="display:flex; align-items:center; padding:18px 0; border-bottom:1px solid #f0f0f0;">
        <div style="width:200px; font-weight:600; font-size:14px; flex-shrink:0; color:#333;">{label}</div>
        <div style="flex:1; position:relative; height:46px; margin:0 30px; background:#fafafa; border-radius:8px;">
            <div style="position:absolute; width:{bar_width}%; left:{bar_start}%; height:8px; background:#ddd; top:50%; transform:translateY(-50%); border-radius:4px;"></div>
            <div style="position:absolute; left:{position}%; top:50%; transform:translate(-50%,-50%); width:14px; height:14px; border-radius:50%; background:{color}; box-shadow:0 0 8px {color}66; z-index:2;"></div>
            <div style="position:absolute; left:{bar_start}%; bottom:2px; font-size:10px; color:#888; transform:translateX(-50%);">{min_p:,.0f}</div>
            <div style="position:absolute; left:{bar_end}%; bottom:2px; font-size:10px; color:#888; transform:translateX(-50%);">{max_p:,.0f}</div>
            <div style="position:absolute; left:{position}%; top:2px; transform:translateX(-50%); font-size:12px; font-weight:700; color:{color}; white-space:nowrap;">{actual:,.0f}원</div>
        </div>
        <div style="width:60px; text-align:right; font-weight:700; color:{color}; font-size:14px; flex-shrink:0;">{status}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def render_labor_card(content: str, actual_fee: float, std_time: Optional[float], hourly_rate: Optional[float]) -> None:
    """Renders a card-style UI for labor cost comparison.

    Args:
        content (str): Name of the repair labor.
        actual_fee (float): Charged technical fee.
        std_time (Optional[float]): Standard repair time (hours).
        hourly_rate (Optional[float]): Standard hourly labor rate.
    """
    if pd.isna(std_time) or pd.isna(hourly_rate):
        st.warning(f"{content}: 기준 공임 데이터 없음")
        return

    expected_fee = std_time * hourly_rate
    diff = actual_fee - expected_fee
    percent = (diff / expected_fee * 100) if expected_fee > 0 else 0
    
    if diff > 0:
        status_text, color = f"기준가보다 {diff:,.0f}원 ({percent:+.1f}%) 높음", "#d32f2f"
    elif diff < 0:
        status_text, color = f"기준가보다 {abs(diff):,.0f}원 ({abs(percent):.1f}%) 낮음", "#1976d2"
    else:
        status_text, color = "시장 기준가와 일치", "#2e7d32"

    st.markdown(f"""
    <div style="padding:16px; margin:10px 0; background:#fdfdfd; border-left:5px solid {color}; border-radius:6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
        <div style="font-weight:700; font-size:15px; color:#333; margin-bottom:8px;">{content}</div>
        <div style="color:{color}; font-size:14px; font-weight:600;">{status_text}</div>
        <div style="color:#666; font-size:12px; margin-top:4px;">
            기준가 {expected_fee:,.0f}원 (표준시간 {std_time}h × 시간당 공임 {hourly_rate:,.0f}원) | 청구액 {actual_fee:,.0f}원
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_cycle_card(content: str, current_mileage: int, prev_mileage: Optional[int], cycle: Optional[int]) -> None:
    """Renders a card-style UI for maintenance cycle diagnosis.

    Args:
        content (str): Name of the repair labor.
        current_mileage (int): Current vehicle mileage.
        prev_mileage (Optional[int]): Mileage at the time of the previous replacement.
        cycle (Optional[int]): Recommended replacement cycle (km).
    """
    if pd.isna(cycle) or cycle is None:
        return

    if prev_mileage is None:
        status_text, color = f"권장 주기 {cycle:,}km | 이번이 첫 교체 기록입니다", "#2e7d32"
    else:
        usage = current_mileage - prev_mileage
        if usage >= cycle:
            status_text, color = f"권장 주기 {cycle:,}km 대비 {usage - cycle:,}km 초과 사용 (교체 적절)", "#2e7d32"
        elif usage >= cycle * 0.8:
            status_text, color = f"권장 주기 {cycle:,}km 대비 {cycle - usage:,}km 남음 (교체 권장 시기)", "#ff9800"
        else:
            status_text, color = f"권장 주기 {cycle:,}km 대비 {cycle - usage:,}km 조기 교체 (과잉 정비 의심)", "#d32f2f"

    st.markdown(f"""
    <div style="padding:16px; margin:10px 0; background:#f9f9f9; border-left:5px solid {color}; border-radius:6px;">
        <div style="font-weight:700; font-size:15px; color:#333; margin-bottom:6px;">{content}</div>
        <div style="color:{color}; font-size:13px; font-weight:600;">{status_text}</div>
        <div style="color:#666; font-size:12px; margin-top:4px;">
            이전 교체: {f"{prev_mileage:,}km" if prev_mileage else "기록 없음"} | 현재 주행: {current_mileage:,}km | 실제 사용: {current_mileage - (prev_mileage or 0):,}km
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_upload_page() -> None:
    """Renders the simplified upload page with a diagnosis trigger."""
    st.title("과잉정비 진단")
    st.write("견적서를 분석하여 정비 비용 및 교체 시기의 적정성을 진단합니다.")

    if ENV == "development":
        st.divider()
        if st.button("테스트 데이터로 진단 시작", use_container_width=True):
            st.session_state.is_test_mode = True
            st.session_state.estimate_id = "EST_20260216_001"
            st.session_state.page = "analysis"
            st.rerun()

def render_analysis_page() -> None:
    """Renders the complete diagnosis report page."""
    st.title("진단 결과 리포트")
    
    estimate_id: Optional[str] = st.session_state.estimate_id
    conn = get_connection()

    if not conn:
        st.error("데이터베이스 연결에 실패했습니다.")
        return

    try:
        # 1. Fetch parts analysis
        parts_df = pd.read_sql("""
            SELECT p.part_official_name, p.unit_price, pm.min_price, pm.max_price
            FROM test.parts p
            JOIN test.estimates e ON p.estimate_id = e.id
            LEFT JOIN LATERAL (
                SELECT min_price, max_price FROM test.parts_master pm
                WHERE pm.part_official_name = p.part_official_name AND pm.car_type = e.car_type
                ORDER BY pm.extracted_at DESC LIMIT 1
            ) pm ON TRUE
            WHERE p.estimate_id = %s;
        """, conn, params=(estimate_id,))

        # 2. Fetch labor and mileage info
        labor_df = pd.read_sql("""
            SELECT l.repair_content, l.tech_fee, lm.standard_repair_time, 
                   lm.hour_labor_rate, lm.change_cycle, e.car_mileage
            FROM test.labor l
            JOIN test.estimates e ON l.estimate_id = e.id
            LEFT JOIN test.labor_master lm
              ON lm.repair_content = l.repair_content AND lm.car_type = e.car_type
              AND e.service_finish_at BETWEEN lm.start_date AND lm.end_date
            WHERE l.estimate_id = %s;
        """, conn, params=(estimate_id,))

        # --- Section 1: Parts Cost Analysis ---
        st.subheader("부품비 적정성 분석")
        if parts_df.empty: st.info("데이터 없음")
        else:
            for _, row in parts_df.iterrows():
                render_part_bar(row["part_official_name"], row["unit_price"], row["min_price"], row["max_price"])
        st.divider()

        # --- Section 2: Labor Fee Analysis ---
        st.subheader("공임비 적정성 분석")
        if labor_df.empty: st.info("데이터 없음")
        else:
            for _, row in labor_df.iterrows():
                render_labor_card(row["repair_content"], row["tech_fee"], row["standard_repair_time"], row["hour_labor_rate"])
        st.divider()

        # --- Section 3: Maintenance Cycle Analysis ---
        st.subheader("소모품 교체주기 진단")
        if labor_df.empty: st.info("데이터 없음")
        else:
            current_mileage = labor_df.iloc[0]["car_mileage"]
            for _, row in labor_df.iterrows():
                prev_repair = pd.read_sql("""
                    SELECT e.car_mileage FROM test.labor l JOIN test.estimates e ON l.estimate_id = e.id
                    WHERE l.repair_content = %s AND e.customer_id = %s AND e.id <> %s
                    ORDER BY e.service_finish_at DESC LIMIT 1
                """, conn, params=(row["repair_content"], USER_EMAIL, estimate_id))
                
                prev_mileage = prev_repair.iloc[0]["car_mileage"] if not prev_repair.empty else None
                render_cycle_card(row["repair_content"], current_mileage, prev_mileage, row["change_cycle"])

    except Exception as e:
        st.error(f"오류 발생: {e}")
    finally:
        conn.close()

    st.divider()
    if st.button("처음으로 돌아가기", use_container_width=True):
        st.session_state.is_test_mode, st.session_state.page = False, "upload"
        st.rerun()

def main() -> None:
    """Main entry point for the application."""
    st.set_page_config(page_title="과잉정비 진단 서비스", layout="wide")
    init_session_state()

    if st.session_state.page == "upload": render_upload_page()
    elif st.session_state.page == "analysis": render_analysis_page()

if __name__ == "__main__":
    main()