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

    # Status and Color mapping
    if actual < min_p:
        color, status = "#1976d2", "저렴"
    elif actual > max_p:
        color, status = "#d32f2f", "비쌈"
    else:
        color, status = "#2e7d32", "적정"

    # Define bar boundaries (20% to 80% to allow space for outliers)
    bar_start, bar_end = 20, 80
    bar_width = bar_end - bar_start
    
    # Proportional position calculation
    if actual < min_p:
        # Move left based on the discount ratio compared to min_p
        ratio = (min_p - actual) / min_p
        position = bar_start - (ratio * 50)
    elif actual > max_p:
        # Move right based on the excess ratio compared to max_p
        ratio = (actual - max_p) / max_p
        position = bar_end + (ratio * 50)
    else:
        # Linear interpolation within the reference range
        price_range = max_p - min_p
        inner_ratio = (actual - min_p) / price_range if price_range > 0 else 0.5
        position = bar_start + (inner_ratio * bar_width)
    
    # Bound the dot within 5% to 95% of the container width
    position = max(5, min(95, position))

    html = f"""
    <div style="display:flex; align-items:center; padding:18px 0; border-bottom:1px solid #f0f0f0;">
        <div style="width:200px; font-weight:600; font-size:14px; flex-shrink:0; color:#333;">
            {label}
        </div>
        <div style="flex:1; position:relative; height:46px; margin:0 30px; background:#fafafa; border-radius:8px;">
            <div style="position:absolute; width:{bar_width}%; left:{bar_start}%; height:8px; background:#ddd; 
                        top:50%; transform:translateY(-50%); border-radius:4px;"></div>
            <div style="position:absolute; left:{position}%; top:50%; transform:translate(-50%,-50%);
                        width:14px; height:14px; border-radius:50%; background:{color}; 
                        box-shadow:0 0 8px {color}66; z-index:2;"></div>
            <div style="position:absolute; left:{bar_start}%; bottom:2px; font-size:10px; color:#888; transform:translateX(-50%);">
                {min_p:,.0f}
            </div>
            <div style="position:absolute; left:{bar_end}%; bottom:2px; font-size:10px; color:#888; transform:translateX(-50%);">
                {max_p:,.0f}
            </div>
            <div style="position:absolute; left:{position}%; top:2px; transform:translateX(-50%);
                        font-size:12px; font-weight:700; color:{color}; white-space:nowrap;">
                {actual:,.0f}원
            </div>
        </div>
        <div style="width:60px; text-align:right; font-weight:700; color:{color}; font-size:14px; flex-shrink:0;">
            {status}
        </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def render_upload_page() -> None:
    """Renders the simplified upload page with a diagnosis trigger."""
    st.title("과잉정비 진단")
    st.write("견적서를 분석하여 정비 비용의 적정성을 진단합니다.")

    if ENV == "development":
        st.divider()
        if st.button("테스트 데이터로 진단 시작", use_container_width=True):
            st.session_state.is_test_mode = True
            st.session_state.estimate_id = "EST_20260216_001"
            st.session_state.page = "analysis"
            st.rerun()

def render_analysis_page() -> None:
    """Renders the diagnosis report page including proportional parts analysis."""
    st.title("진단 결과 리포트")
    
    estimate_id: Optional[str] = st.session_state.estimate_id
    conn = get_connection()

    if not conn:
        st.error("데이터베이스 연결에 실패했습니다. 환경 설정을 확인해주세요.")
        return

    try:
        # Fetch parts data with lateral join for the latest master price
        parts_df = pd.read_sql("""
            SELECT p.part_official_name, p.unit_price, pm.min_price, pm.max_price
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
        """, conn, params=(estimate_id,))
    except Exception as e:
        st.error(f"데이터 조회 중 오류가 발생했습니다: {e}")
        parts_df = pd.DataFrame()
    finally:
        conn.close()

    st.subheader("부품비 적정성 분석")
    st.write("회색 바는 시장 기준 가격 범위를 나타내며, 점의 위치는 해당 범위 대비 현재 가격 수준을 의미합니다.")

    if parts_df.empty:
        st.info("진단할 부품 데이터가 존재하지 않습니다.")
    else:
        for _, row in parts_df.iterrows():
            render_part_bar(
                label=row["part_official_name"],
                actual=row["unit_price"],
                min_p=row["min_price"],
                max_p=row["max_price"]
            )

    st.divider()
    if st.button("처음으로 돌아가기", use_container_width=True):
        st.session_state.is_test_mode = False
        st.session_state.page = "upload"
        st.rerun()

def main() -> None:
    """Main entry point for the application.
    
    Configures the page settings and performs routing.
    """
    st.set_page_config(page_title="과잉정비 진단 서비스", layout="wide")
    init_session_state()

    if st.session_state.page == "upload":
        render_upload_page()
    elif st.session_state.page == "analysis":
        render_analysis_page()

if __name__ == "__main__":
    main()