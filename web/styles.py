import streamlit as st


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

    /* ── 상단 네비바 ── */
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

    /* ── 페이지 래퍼 ── */
    .page-wrap { padding: 28px 0 72px; }

    /* ── 판정 배너 ── */
    .top-verdict {
        border-radius: 16px; padding: 24px 26px; margin-bottom: 14px;
        border: 2px solid; display: flex; align-items: center; gap: 18px;
    }
    .top-verdict.danger { background: var(--danger-light); border-color: var(--danger-border); }
    .top-verdict.safe   { background: var(--success-light); border-color: var(--success-border); }
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

    /* ── 이슈 카드 ── */
    .issue-card {
        background: #fff; border: 1.5px solid var(--danger-border);
        border-radius: 12px; padding: 15px 18px; margin-bottom: 9px;
    }
    .issue-card-header { display: flex; align-items: center; gap: 9px; margin-bottom: 7px; }
    .issue-card-title  { font-size: 14px; font-weight: 700; color: var(--danger); }
    .issue-card-body   { font-size: 13px; color: var(--gray-700); line-height: 1.7; }

    /* ── 상태 칩 ── */
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

    /* ── 아코디언 섹션 카드 ── */
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

    /* ── 부품비 바 ── */
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

    /* ── 공임비 카드 ── */
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

    /* ── 교체주기 카드 ── */
    .cycle-item { padding: 13px 0; border-bottom: 1px solid var(--gray-100); }
    .cycle-item:last-child { border-bottom: none; }
    .cycle-row-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
    .cycle-name         { font-size: 13px; font-weight: 700; color: var(--gray-900); }
    .cycle-status-badge { font-size: 11px; font-weight: 700; padding: 3px 9px; border-radius: 4px; }
    .cycle-prog-track   { background: var(--gray-100); border-radius: 4px; height: 7px; overflow: hidden; margin-bottom: 8px; }
    .cycle-prog-fill    { height: 100%; border-radius: 4px; }
    .cycle-meta-row     { display: flex; justify-content: space-between; font-size: 11px; color: var(--gray-400); }

    /* ── 기타 ── */
    .empty-msg { font-size: 12px; color: var(--gray-400); padding: 12px 0; text-align: center; font-style: italic; }
    .stButton > button { font-family: 'Pretendard', sans-serif !important; font-weight: 600 !important; }
    </style>
    """, unsafe_allow_html=True)