import os
import streamlit as st
from typing import Optional
from dotenv import load_dotenv
from db import get_connection

# Load environmental variables
load_dotenv()

# Global environment toggle: Set to "production" to hide debug features
ENV: str = "development"

def init_session_state() -> None:
    """Initializes the Streamlit session state variables.
    
    Sets up the initial page and test mode flags within the session.
    """
    if "page" not in st.session_state:
        st.session_state.page = "upload"
    if "is_test_mode" not in st.session_state:
        st.session_state.is_test_mode = False

def render_upload_page() -> None:
    """Renders the simplified upload page.
    
    Provides a entry point for testing connectivity in development mode.
    """
    # Main title visible to the user
    st.title("과잉정비 진단")
    st.write("분석 로직은 2단계에서 구현될 예정입니다.")

    # Show debug features only in development
    if ENV == "development":
        st.divider()
        if st.button("DB 연결 테스트", use_container_width=True):
            st.session_state.is_test_mode = True
            st.session_state.page = "analysis"
            st.rerun()

def render_analysis_page() -> None:
    """Renders the analysis result and connection status page.
    
    Verifies and displays the status of the database connection.
    """
    # Main title visible to the user
    st.title("진단 결과 리포트")
    
    if st.session_state.get("is_test_mode"):
        st.info("데이터베이스 연결 상태를 확인 중입니다...")

    # Validate database connection
    conn = get_connection()
    if conn:
        st.success("데이터베이스 연결에 성공했습니다.")
        conn.close()
    else:
        st.error("데이터베이스 연결에 실패했습니다. .env 파일과 도커 설정을 확인해주세요.")

    if st.button("처음으로 돌아가기", use_container_width=True):
        st.session_state.is_test_mode = False
        st.session_state.page = "upload"
        st.rerun()

def main() -> None:
    """Main entry point for the application.
    
    Configures the page settings and handles basic routing.
    """
    # Set the browser tab title
    st.set_page_config(page_title="과잉정비 진단 서비스", layout="wide")
    
    init_session_state()

    # Routing based on session state
    if st.session_state.page == "upload":
        render_upload_page()
    elif st.session_state.page == "analysis":
        render_analysis_page()

if __name__ == "__main__":
    main()