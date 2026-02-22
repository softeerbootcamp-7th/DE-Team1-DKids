import streamlit as st
from dotenv import load_dotenv

from styles import inject_global_css

from pages import render_upload_page, render_analysis_page

load_dotenv()


def set_page_config() -> None:
    st.set_page_config(
        page_title="CarCheck — 과잉정비 진단",
        page_icon="🔧",
        layout="centered",
        initial_sidebar_state="collapsed",
    )


def init_session_state() -> None:
    defaults = {
        "page":          "upload",
        "estimate_id":   None,
        "is_test_mode":  False,
        "symptom_text":  "",
        "rag_result":    None,
        "rag_result_key": "",
        "acc_rag":       False,
        "acc_parts":     False,
        "acc_labor":     False,
        "acc_cycle":     False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


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