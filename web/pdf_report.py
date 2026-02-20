"""
pdf_report.py
과잉정비 의심 항목만을 추출하여 정비소 반박/문의 자료로 활용할 수 있는
PDF 보고서를 생성합니다. (공문서 포맷)
"""
import io
import os
import re
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageTemplate, Paragraph,
    Spacer, Table, TableStyle, HRFlowable, KeepTogether,
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ─────────────────────────────────────────────
# 폰트
# ─────────────────────────────────────────────
FONT_R = "NanumGothic"
FONT_B = "NanumGothicBold"
_FONT_CANDIDATES = [
    ("/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
     "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf"),
    ("/usr/share/fonts/nanum/NanumGothic.ttf",
     "/usr/share/fonts/nanum/NanumGothicBold.ttf"),
    ("/Library/Fonts/NanumGothic.ttf", "/Library/Fonts/NanumGothicBold.ttf"),
    ("NanumGothic.ttf", "NanumGothicBold.ttf"),
    ("/app/fonts/NanumGothic.ttf", "/app/fonts/NanumGothicBold.ttf"),
]


def _register_fonts() -> bool:
    for rp, bp in _FONT_CANDIDATES:
        if os.path.exists(rp):
            try:
                pdfmetrics.registerFont(TTFont(FONT_R, rp))
                pdfmetrics.registerFont(TTFont(FONT_B, bp if os.path.exists(bp) else rp))
                return True
            except Exception:
                continue
    return False


_KO = _register_fonts()


def fr(bold=False):
    if _KO:
        return FONT_B if bold else FONT_R
    return "Helvetica-Bold" if bold else "Helvetica"


# ─────────────────────────────────────────────
# 색상 — 절제된 공문서 팔레트
# ─────────────────────────────────────────────
INK        = colors.HexColor("#1a1a2e")   # 본문 텍스트 (진한 네이비)
INK_LIGHT  = colors.HexColor("#3d3d5c")   # 부제목
RULE       = colors.HexColor("#1a1a2e")   # 굵은 구분선
RULE_LIGHT = colors.HexColor("#c8ccd8")   # 보조 구분선
CREAM      = colors.HexColor("#f7f6f2")   # 표 짝수행 배경
WHITE      = colors.white
RED        = colors.HexColor("#b91c1c")   # 위험 텍스트
RED_BG     = colors.HexColor("#fdf2f2")
RED_BD     = colors.HexColor("#e4a5a5")
AMBER      = colors.HexColor("#92400e")   # 주의 텍스트
AMBER_BG   = colors.HexColor("#fefce8")
AMBER_BD   = colors.HexColor("#d4b96a")
GREEN_T    = colors.HexColor("#166534")
GREEN_BG   = colors.HexColor("#f0fdf4")
GREEN_BD   = colors.HexColor("#86efac")
SLATE      = colors.HexColor("#475569")
SLATE_LIGHT= colors.HexColor("#94a3b8")


# ─────────────────────────────────────────────
# 스타일
# ─────────────────────────────────────────────
def _s(name, bold=False, size=9, color=INK, align=TA_LEFT,
       leading=None, sb=0, sa=0) -> ParagraphStyle:
    return ParagraphStyle(
        name,
        fontName=fr(bold),
        fontSize=size,
        textColor=color,
        alignment=align,
        leading=leading or max(size * 1.5, size + 3),
        spaceBefore=sb,
        spaceAfter=sa,
    )


def _make_styles() -> dict:
    return {
        "sub_label":    _s("sub_label",    bold=True,  size=7,  color=SLATE,  sa=2),
        "body":         _s("body",         bold=False, size=9,  color=INK,    leading=15, align=TA_JUSTIFY),
        "caption":      _s("caption",      bold=False, size=7,  color=SLATE_LIGHT, align=TA_CENTER),
        "note":         _s("note",         bold=False, size=7.5,color=GREEN_T),
        "th":           _s("th",           bold=True,  size=7.5,color=WHITE,  align=TA_CENTER),
        "th_left":      _s("th_left",      bold=True,  size=7.5,color=WHITE,  align=TA_LEFT),
        "td":           _s("td",           bold=False, size=8,  color=INK),
        "td_r":         _s("td_r",         bold=False, size=8,  color=INK,    align=TA_RIGHT),
        "td_red":       _s("td_red",       bold=True,  size=8,  color=RED,    align=TA_RIGHT),
        "td_c_red":     _s("td_c_red",     bold=True,  size=8,  color=RED,    align=TA_CENTER),
        "td_amber":     _s("td_amber",     bold=True,  size=8,  color=AMBER,  align=TA_CENTER),
        "sec_num":      _s("sec_num",      bold=True,  size=8,  color=WHITE,  align=TA_CENTER),
        "sec_title":    _s("sec_title",    bold=True,  size=11, color=INK),
        "q_head":       _s("q_head",       bold=True,  size=8,  color=INK_LIGHT),
        "q_num":        _s("q_num",        bold=True,  size=8,  color=INK),
        "q_item":       _s("q_item",       bold=False, size=8,  color=INK,    leading=13),
        "disclaimer":   _s("disclaimer",   bold=False, size=7,  color=SLATE,  align=TA_JUSTIFY),
        "no_issue":     _s("no_issue",     bold=True,  size=11, color=GREEN_T,align=TA_CENTER),
    }


# ─────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────
def _parse_llm_verdict(text: str) -> Optional[bool]:
    t = re.sub(r"^\[[^\]]+\]\s*", "", " ".join(str(text or "").split()))
    if t.startswith("견적서는 다음 이유로 과잉정비입니다."):
        return True
    if t.startswith("견적서는 현재 근거 기준 표준 범위입니다."):
        return False
    return None


def _strip_evidence_tag(text: str) -> str:
    t = " ".join(str(text or "").split())
    m = re.match(r"^\[[^\]]+\]\s*(.*)$", t)
    return m.group(1).strip() if m else t


def _evidence_label(text: str) -> str:
    label_map = {
        "hyundai_model_pdf_plus_common": "현대 정비 지침서 + 일반 정비 지침",
        "hyundai_model_pdf_only":        "현대 정비 지침서",
        "common_only":                   "일반 정비 지침",
        "no_evidence":                   "근거 문서 없음",
    }
    t = " ".join(str(text or "").split())
    m = re.match(r"^\[근거:\s*([^\]]+)\]", t)
    code = m.group(1).strip() if m else ""
    return label_map.get(code, code)


def _thin_rule(color=RULE_LIGHT, thickness=0.4, sb=6, sa=6):
    return HRFlowable(width="100%", thickness=thickness, color=color,
                      spaceBefore=sb, spaceAfter=sa)


def _bold_rule(sb=4, sa=8):
    return HRFlowable(width="100%", thickness=1.2, color=RULE,
                      spaceBefore=sb, spaceAfter=sa)


def _tbl_base(extra=None):
    base = [
        ("BACKGROUND",     (0, 0), (-1, 0),  INK),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, CREAM]),
        ("LINEBELOW",      (0, 0), (-1, -1), 0.3, RULE_LIGHT),
        ("TOPPADDING",     (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",  (0, 0), (-1, -1), 5),
        ("LEFTPADDING",    (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",   (0, 0), (-1, -1), 8),
        ("VALIGN",         (0, 0), (-1, -1), "MIDDLE"),
        ("BOX",            (0, 0), (-1, -1), 0.5, RULE_LIGHT),
    ]
    return TableStyle(base + (extra or []))


# ─────────────────────────────────────────────
# 섹션 헤더
# ─────────────────────────────────────────────
def _sec_header(seq: int, title: str, badge: str,
                badge_color: colors.Color, st: dict) -> list:
    num_cell = Table(
        [[Paragraph(str(seq), st["sec_num"])]],
        colWidths=[14]
    )
    num_cell.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), INK),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING",   (0, 0), (-1, -1), 3),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 3),
    ]))
    badge_p = Paragraph(badge, ParagraphStyle(
        "sbdg", fontName=fr(True), fontSize=7.5,
        textColor=badge_color, alignment=TA_RIGHT
    ))
    row = Table(
        [[num_cell, Paragraph(title, st["sec_title"]), badge_p]],
        colWidths=[20, None, 110]
    )
    row.setStyle(TableStyle([
        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING",   (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
        ("LEFTPADDING",  (1, 0), (1, 0),   8),
    ]))
    return [row, _bold_rule(sb=5, sa=8)]


# ─────────────────────────────────────────────
# 문의 포인트 박스
# ─────────────────────────────────────────────
def _inquiry_box(questions: list[str], st: dict) -> Table:
    header = Paragraph("▣  정비소 문의 사항", st["q_head"])
    items_col = [header, Spacer(1, 4)]
    for i, q in enumerate(questions, 1):
        row = Table(
            [[Paragraph(f"{i}.", st["q_num"]),
              Paragraph(q, st["q_item"])]],
            colWidths=[14, None]
        )
        row.setStyle(TableStyle([
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING",   (0, 0), (-1, -1), 0),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
            ("TOPPADDING",    (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        items_col.append(row)
    outer = Table([[items_col]], colWidths=["100%"])
    outer.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), CREAM),
        ("BOX",           (0, 0), (-1, -1), 0.8, RULE_LIGHT),
        ("LINEABOVE",     (0, 0), (-1, 0),  2.5, INK),
        ("LEFTPADDING",   (0, 0), (-1, -1), 14),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 14),
        ("TOPPADDING",    (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    return outer


def _ok_note(count: int, label: str, st: dict) -> Table:
    t = Table([[Paragraph(
        f"※  {label} {count}건은 기준 범위 내 이상 없음으로 본 보고서에서 제외되었습니다.",
        st["note"]
    )]], colWidths=["100%"])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), GREEN_BG),
        ("BOX",           (0, 0), (-1, -1), 0.5, GREEN_BD),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return t


# ─────────────────────────────────────────────
# 페이지 헤더 / 푸터
# ─────────────────────────────────────────────
def _on_page(canvas, doc, meta: dict):
    W, H = A4
    canvas.saveState()

    # 상단 헤더 바
    HDR_H = 22 * mm
    canvas.setFillColor(INK)
    canvas.rect(0, H - HDR_H, W, HDR_H, fill=1, stroke=0)

    canvas.setFont(fr(True), 11)
    canvas.setFillColor(WHITE)
    canvas.drawString(18 * mm, H - 9 * mm, "CarCheck")
    canvas.setFont(fr(), 7.5)
    canvas.setFillColor(SLATE_LIGHT)
    canvas.drawString(18 * mm, H - 15 * mm, "AI 과잉정비 진단 보고서")

    canvas.setFont(fr(), 7.5)
    canvas.setFillColor(SLATE_LIGHT)
    canvas.drawRightString(W - 18 * mm, H - 9 * mm,
                           f"생성일: {meta.get('report_date', '')}")
    canvas.drawRightString(W - 18 * mm, H - 15 * mm,
                           f"페이지 {doc.page}")

    # 하단 푸터
    canvas.setStrokeColor(RULE_LIGHT)
    canvas.setLineWidth(0.4)
    canvas.line(18 * mm, 12 * mm, W - 18 * mm, 12 * mm)
    canvas.setFont(fr(), 6.5)
    canvas.setFillColor(SLATE_LIGHT)
    canvas.drawString(18 * mm, 7 * mm,
                      "본 문서는 AI 분석 기반 참고 자료입니다. 법적 효력이 없으며 최종 판단은 공인 정비 전문가의 의견을 따르시기 바랍니다.")
    canvas.drawRightString(W - 18 * mm, 7 * mm,
                           f"견적번호: {meta.get('estimate_id', '–')}")
    canvas.restoreState()


# ─────────────────────────────────────────────
# 커버 블록 (첫 페이지 상단 전체)
# ─────────────────────────────────────────────
def _build_cover(meta: dict, issue_count: int,
                 summary_items: list[str], st: dict) -> list:
    fl = []

    # 1. 차량·견적 정보 테이블
    info_rows = [
        [Paragraph("차  종",    st["sub_label"]),
         Paragraph(meta.get("car_type", "–"),
                   ParagraphStyle("mv1", fontName=fr(True), fontSize=9, textColor=INK)),
         Paragraph("정 비 일",  st["sub_label"]),
         Paragraph(meta.get("svc_date", "–"),
                   ParagraphStyle("mv2", fontName=fr(True), fontSize=9, textColor=INK))],
        [Paragraph("견 적 ID",  st["sub_label"]),
         Paragraph(meta.get("estimate_id", "–"),
                   ParagraphStyle("mv3", fontName=fr(), fontSize=8, textColor=SLATE)),
         Paragraph("진 단 일 시", st["sub_label"]),
         Paragraph(meta.get("report_date", "–"),
                   ParagraphStyle("mv4", fontName=fr(), fontSize=8, textColor=SLATE))],
    ]
    info_tbl = Table(info_rows, colWidths=["18%", "32%", "18%", "32%"])
    info_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), CREAM),
        ("BOX",           (0, 0), (-1, -1), 0.6, RULE_LIGHT),
        ("INNERGRID",     (0, 0), (-1, -1), 0.3, RULE_LIGHT),
        ("TOPPADDING",    (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]))
    fl.append(info_tbl)
    fl.append(Spacer(1, 10))

    # 2. 판정 블록
    is_over = issue_count > 0
    v_bg  = RED_BG  if is_over else GREEN_BG
    v_bd  = RED_BD  if is_over else GREEN_BD
    v_col = RED     if is_over else GREEN_T

    verdict_left = [
        Paragraph(
            "과잉정비 의심" if is_over else "이상 없음",
            ParagraphStyle("vt", fontName=fr(True), fontSize=14,
                           textColor=v_col, leading=18)
        ),
        Spacer(1, 3),
        Paragraph(
            f"총 {issue_count}개 카테고리에서 이상이 감지되었습니다." if is_over
            else "부품비·공임비·교체주기 모두 기준 범위 내로 확인되었습니다.",
            ParagraphStyle("vs", fontName=fr(), fontSize=8,
                           textColor=SLATE, leading=12)
        ),
    ]
    verdict_right = [
        Paragraph(str(issue_count),
                  ParagraphStyle("cnt", fontName=fr(True), fontSize=28,
                                 textColor=v_col, alignment=TA_CENTER, leading=32)),
        Paragraph("이상 항목",
                  ParagraphStyle("cntl", fontName=fr(), fontSize=7,
                                 textColor=v_col, alignment=TA_CENTER)),
    ]
    verdict_tbl = Table([[verdict_left, verdict_right]], colWidths=[None, 55])
    verdict_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), v_bg),
        ("BOX",           (0, 0), (-1, -1), 1.2, v_bd),
        ("LEFTPADDING",   (0, 0), (0, 0),   14),
        ("RIGHTPADDING",  (0, 0), (0, 0),   8),
        ("LEFTPADDING",   (1, 0), (1, 0),   0),
        ("RIGHTPADDING",  (1, 0), (1, 0),   14),
        ("TOPPADDING",    (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("LINEAFTER",     (0, 0), (0, 0),   0.5, v_bd),
    ]))
    fl.append(verdict_tbl)

    # 3. 의심 항목 목록
    if summary_items:
        fl.append(Spacer(1, 8))
        fl.append(Paragraph("■  감지된 과잉정비 의심 항목", st["sub_label"]))
        fl.append(Spacer(1, 4))

        sum_rows = []
        for i, item in enumerate(summary_items, 1):
            sum_rows.append([
                Paragraph(str(i), ParagraphStyle(
                    f"sn{i}", fontName=fr(True), fontSize=8,
                    textColor=WHITE, alignment=TA_CENTER)),
                Paragraph(item, ParagraphStyle(
                    f"si{i}", fontName=fr(False), fontSize=8.5,
                    textColor=INK, leading=13)),
            ])
        sum_tbl = Table(sum_rows, colWidths=[16, None])
        sum_tbl.setStyle(TableStyle([
            ("BACKGROUND",     (0, 0), (0, -1),  INK),
            ("ROWBACKGROUNDS", (1, 0), (1, -1),  [WHITE, CREAM]),
            ("BOX",            (0, 0), (-1, -1), 0.6, RULE_LIGHT),
            ("LINEBELOW",      (0, 0), (-1, -2), 0.3, RULE_LIGHT),
            ("TOPPADDING",     (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING",  (0, 0), (-1, -1), 6),
            ("LEFTPADDING",    (0, 0), (0, -1),  4),
            ("RIGHTPADDING",   (0, 0), (0, -1),  4),
            ("LEFTPADDING",    (1, 0), (1, -1),  10),
            ("RIGHTPADDING",   (1, 0), (1, -1),  10),
            ("VALIGN",         (0, 0), (-1, -1), "MIDDLE"),
        ]))
        fl.append(sum_tbl)

    fl.append(Spacer(1, 12))
    fl.append(_bold_rule(sb=0, sa=12))
    return fl


# ─────────────────────────────────────────────
# 이슈 섹션 빌더
# ─────────────────────────────────────────────
def _build_ai_section(rag_result: dict, symptom_text: str, seq: int, st: dict) -> list:
    diagnosis_text = rag_result.get("diagnosis_text", "")
    body   = _strip_evidence_tag(diagnosis_text)
    ev_lbl = _evidence_label(diagnosis_text)

    fl = []
    fl += _sec_header(seq, "증상과 무관한 정비 항목 포함", "AI 판정", RED, st)

    if symptom_text:
        fl.append(Paragraph("고객 입력 증상", st["sub_label"]))
        fl.append(Spacer(1, 3))
        sym = Table([[Paragraph(symptom_text, st["body"])]], colWidths=["100%"])
        sym.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), CREAM),
            ("BOX",           (0, 0), (-1, -1), 0.5, RULE_LIGHT),
            ("LEFTPADDING",   (0, 0), (-1, -1), 10),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
            ("TOPPADDING",    (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        fl.append(sym)
        fl.append(Spacer(1, 8))

    fl.append(Paragraph("AI 진단 결과", st["sub_label"]))
    fl.append(Spacer(1, 3))
    diag = Table([[Paragraph(body, st["body"])]], colWidths=["100%"])
    diag.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), RED_BG),
        ("LINEABOVE",     (0, 0), (-1, 0),  2, RED),
        ("BOX",           (0, 0), (-1, -1), 0.5, RED_BD),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
        ("TOPPADDING",    (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    fl.append(diag)
    if ev_lbl:
        fl.append(Spacer(1, 3))
        fl.append(Paragraph(f"참조 근거: {ev_lbl}", st["caption"]))

    symptom_results = rag_result.get("symptom_results", [])
    has_docs = any(sr.get("evidence_docs") for sr in symptom_results)
    if has_docs:
        fl.append(Spacer(1, 8))
        fl.append(Paragraph("참조 근거 문서", st["sub_label"]))
        fl.append(Spacer(1, 4))
        doc_rows = [[
            Paragraph("증상",      st["th_left"]),
            Paragraph("출처",      st["th_left"]),
            Paragraph("근거 내용", st["th_left"]),
        ]]
        for sr in symptom_results:
            for d in sr.get("evidence_docs", [])[:2]:
                ev = d.get("evidence_text", "")
                ev_short = (ev[:70] + "…") if len(ev) > 70 else ev
                doc_rows.append([
                    Paragraph(sr["symptom_text"],           st["td"]),
                    Paragraph(d.get("document_source", ""), st["td"]),
                    Paragraph(ev_short,                     st["td"]),
                ])
        if len(doc_rows) > 1:
            t = Table(doc_rows, colWidths=["25%", "20%", "55%"])
            t.setStyle(_tbl_base())
            fl.append(t)

    fl.append(Spacer(1, 10))
    fl.append(_inquiry_box([
        "이번 정비에서 교체된 부품이 고객이 호소한 증상과 어떤 직접적 연관이 있습니까?",
        "증상 해결을 위해 해당 부품 교체가 반드시 필요하다는 기술적 근거를 제시해 주십시오.",
        "사전에 고객 동의 없이 증상과 무관한 예방적 정비가 진행된 경우 그 사유를 설명해 주십시오.",
    ], st))
    return fl


def _build_parts_section(parts_df: pd.DataFrame, seq: int, st: dict) -> list:
    over_rows = [
        r for _, r in parts_df.iterrows()
        if pd.notna(r.get("max_price")) and r["unit_price"] > r["max_price"]
    ]
    if not over_rows:
        return []

    total_over    = sum(r["unit_price"] - r["max_price"] for r in over_rows)
    total_charged = sum(r["unit_price"] for r in over_rows)
    total_max     = sum(r["max_price"]  for r in over_rows)

    fl = []
    fl += _sec_header(
        seq, "부품비 시장 기준가 초과 청구",
        f"{len(over_rows)}건  /  초과 합계 +{total_over:,.0f}원", RED, st
    )

    rows = [[
        Paragraph("부품명",      st["th_left"]),
        Paragraph("청구가 (원)", st["th"]),
        Paragraph("최고 기준가", st["th"]),
        Paragraph("초과 금액",   st["th"]),
        Paragraph("초과율",      st["th"]),
    ]]
    for r in over_rows:
        diff = r["unit_price"] - r["max_price"]
        pct  = diff / r["max_price"] * 100
        rows.append([
            Paragraph(r["part_official_name"],   st["td"]),
            Paragraph(f"{r['unit_price']:,.0f}", st["td_red"]),
            Paragraph(f"{r['max_price']:,.0f}",  st["td_r"]),
            Paragraph(f"+{diff:,.0f}",           st["td_red"]),
            Paragraph(f"+{pct:.1f}%",            st["td_c_red"]),
        ])
    n = len(rows)
    rows.append([
        Paragraph("합  계",
                  ParagraphStyle("stot", fontName=fr(True), fontSize=8, textColor=INK)),
        Paragraph(f"{total_charged:,.0f}", st["td_red"]),
        Paragraph(f"{total_max:,.0f}",     st["td_r"]),
        Paragraph(f"+{total_over:,.0f}",   st["td_red"]),
        Paragraph("",                      st["td"]),
    ])
    tbl = Table(rows, colWidths=["36%", "16%", "16%", "17%", "15%"])
    tbl.setStyle(_tbl_base([
        ("BACKGROUND", (0, n), (-1, n), CREAM),
        ("LINEABOVE",  (0, n), (-1, n), 0.8, RULE_LIGHT),
        ("FONTNAME",   (0, n), (0, n),  fr(True)),
    ]))
    fl.append(tbl)

    ok_count = len(parts_df) - len(over_rows) if not parts_df.empty else 0
    if ok_count > 0:
        fl.append(Spacer(1, 5))
        fl.append(_ok_note(ok_count, "기준가 범위 내 정상 부품", st))

    fl.append(Spacer(1, 10))
    questions = [
        f'"{r["part_official_name"]}" 부품이 시장 최고 기준가 ({r["max_price"]:,.0f}원) 대비 '
        f'{r["unit_price"] - r["max_price"]:,.0f}원 초과 청구된 근거를 제시해 주십시오.'
        for r in over_rows
    ]
    questions.append("사용된 부품의 등급(정품 / OEM / 재제조)과 구매 단가 영수증을 확인할 수 있습니까?")
    fl.append(_inquiry_box(questions, st))
    return fl


def _build_labor_section(labor_df: pd.DataFrame, seq: int, st: dict) -> list:
    over_rows = [
        r for _, r in labor_df.iterrows()
        if pd.notna(r.get("standard_repair_time")) and pd.notna(r.get("hour_labor_rate"))
        and r["tech_fee"] > r["standard_repair_time"] * r["hour_labor_rate"]
    ]
    if not over_rows:
        return []

    total_over = sum(
        r["tech_fee"] - r["standard_repair_time"] * r["hour_labor_rate"]
        for r in over_rows
    )
    fl = []
    fl += _sec_header(
        seq, "공임비 표준 기준가 초과 청구",
        f"{len(over_rows)}건  /  초과 합계 +{total_over:,.0f}원", RED, st
    )

    rows = [[
        Paragraph("정비 항목",   st["th_left"]),
        Paragraph("청구 공임",   st["th"]),
        Paragraph("표준 기준가", st["th"]),
        Paragraph("초과 금액",   st["th"]),
        Paragraph("기준 산출",   st["th_left"]),
    ]]
    for r in over_rows:
        expected = r["standard_repair_time"] * r["hour_labor_rate"]
        diff = r["tech_fee"] - expected
        pct  = diff / expected * 100
        rows.append([
            Paragraph(r["repair_content"],             st["td"]),
            Paragraph(f"{r['tech_fee']:,.0f}",         st["td_red"]),
            Paragraph(f"{expected:,.0f}",              st["td_r"]),
            Paragraph(f"+{diff:,.0f} ({pct:+.1f}%)",  st["td_red"]),
            Paragraph(
                f"{r['standard_repair_time']}h × {r['hour_labor_rate']:,.0f}원",
                st["td"]
            ),
        ])
    tbl = Table(rows, colWidths=["28%", "14%", "15%", "22%", "21%"])
    tbl.setStyle(_tbl_base())
    fl.append(tbl)

    ok_count = len(labor_df) - len(over_rows) if not labor_df.empty else 0
    if ok_count > 0:
        fl.append(Spacer(1, 5))
        fl.append(_ok_note(ok_count, "기준가 범위 내 정상 공임", st))

    fl.append(Spacer(1, 10))
    questions = [
        f'"{r["repair_content"]}" 작업의 표준 작업시간 {r["standard_repair_time"]}h 기준 공임은 '
        f'{r["standard_repair_time"] * r["hour_labor_rate"]:,.0f}원이나, '
        f'{r["tech_fee"]:,.0f}원이 청구되었습니다. '
        f'초과 청구 ({r["tech_fee"] - r["standard_repair_time"] * r["hour_labor_rate"]:,.0f}원)의 사유를 제시해 주십시오.'
        for r in over_rows
    ]
    questions.append("실제 작업시간 기록(작업지시서)을 확인할 수 있습니까?")
    fl.append(_inquiry_box(questions, st))
    return fl


def _build_cycle_section(cycle_issues: list[dict], seq: int, st: dict) -> list:
    early = [c for c in cycle_issues if c["verdict"] == "조기 교체"]
    if not early:
        return []

    fl = []
    fl += _sec_header(
        seq, "소모품 권장 교체주기 미달 조기 교체",
        f"{len(early)}건", AMBER, st
    )

    rows = [[
        Paragraph("정비 항목",  st["th_left"]),
        Paragraph("권장 주기",  st["th"]),
        Paragraph("실제 주행",  st["th"]),
        Paragraph("달성률",     st["th"]),
        Paragraph("이전 교체",  st["th"]),
    ]]
    for c in early:
        pct    = int(c["usage"] / c["cycle"] * 100) if c["cycle"] else 0
        prev_s = f"{c['prev_mileage']:,}km" if c.get("prev_mileage") else "기록 없음"
        rows.append([
            Paragraph(c["repair_content"], st["td"]),
            Paragraph(f"{c['cycle']:,}km", st["td_r"]),
            Paragraph(f"{c['usage']:,}km", st["td_r"]),
            Paragraph(f"{pct}%",           st["td_amber"]),
            Paragraph(prev_s,              st["td_r"]),
        ])
    tbl = Table(rows, colWidths=["34%", "15%", "15%", "14%", "22%"])
    tbl.setStyle(_tbl_base())
    fl.append(tbl)

    ok_count = len(cycle_issues) - len(early)
    if ok_count > 0:
        fl.append(Spacer(1, 5))
        fl.append(_ok_note(ok_count, "교체주기 적절·권장 시기 항목", st))

    fl.append(Spacer(1, 10))
    questions = [
        f'"{c["repair_content"]}"은 권장 주기 {c["cycle"]:,}km 대비 '
        f'{c["cycle"] - c["usage"]:,}km 조기인 {c["usage"]:,}km 시점에 교체되었습니다. '
        f'조기 교체가 필요했던 기술적 사유와 소모품 상태 점검 기록을 제시해 주십시오.'
        for c in early
    ]
    questions.append("소모품 상태 점검 기록(사진 또는 측정값 포함)을 제공해 주실 수 있습니까?")
    fl.append(_inquiry_box(questions, st))
    return fl


# ─────────────────────────────────────────────
# 메인 생성 함수
# ─────────────────────────────────────────────
def generate_diagnosis_pdf(
    parts_df: pd.DataFrame,
    labor_df: pd.DataFrame,
    summary: dict,
    rag_result: dict,
    symptom_text: str,
    car_type: str,
    svc_date: str,
    estimate_id: str,
    cycle_issues: list[dict],
) -> bytes:
    buf = io.BytesIO()
    st  = _make_styles()
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 과잉 여부 직접 판단
    llm_over  = _parse_llm_verdict(rag_result.get("diagnosis_text", ""))
    llm_issue = (llm_over is True)

    over_parts = [
        r for _, r in parts_df.iterrows()
        if pd.notna(r.get("max_price")) and r["unit_price"] > r["max_price"]
    ] if not parts_df.empty else []

    over_labor = [
        r for _, r in labor_df.iterrows()
        if pd.notna(r.get("standard_repair_time")) and pd.notna(r.get("hour_labor_rate"))
        and r["tech_fee"] > r["standard_repair_time"] * r["hour_labor_rate"]
    ] if not labor_df.empty else []

    early_cycle = [c for c in cycle_issues if c["verdict"] == "조기 교체"]

    issue_count = sum([
        1 if llm_issue   else 0,
        1 if over_parts  else 0,
        1 if over_labor  else 0,
        1 if early_cycle else 0,
    ])

    summary_items = []
    if llm_issue:   summary_items.append("증상과 무관한 정비 항목 포함")
    if over_parts:  summary_items.append(f"부품비 과다 청구  —  {len(over_parts)}건")
    if over_labor:  summary_items.append(f"공임비 기준 초과  —  {len(over_labor)}건")
    if early_cycle: summary_items.append(f"소모품 조기 교체 의심  —  {len(early_cycle)}건")

    meta = dict(
        car_type=car_type or "–",
        svc_date=svc_date or "–",
        estimate_id=estimate_id or "–",
        report_date=report_date,
        issue_count=issue_count,
    )

    MT, MB, ML, MR = 30 * mm, 20 * mm, 18 * mm, 18 * mm
    doc = BaseDocTemplate(
        buf, pagesize=A4,
        leftMargin=ML, rightMargin=MR,
        topMargin=MT, bottomMargin=MB,
    )
    frame = Frame(ML, MB, A4[0] - ML - MR, A4[1] - MT - MB, id="body")
    doc.addPageTemplates([PageTemplate(
        id="main", frames=[frame],
        onPage=lambda c, d: _on_page(c, d, meta),
    )])

    story = []

    # 커버 (차량정보 + 판정 + 의심항목 목록)
    story += _build_cover(meta, issue_count, summary_items, st)

    # 과잉 항목 없을 때
    if issue_count == 0:
        story.append(Spacer(1, 20))
        story.append(Paragraph("분석 결과 과잉정비 의심 항목이 없습니다.", st["no_issue"]))
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            "부품비·공임비·교체주기 모두 기준 범위 내로 확인되었습니다.",
            ParagraphStyle("nosub", fontName=fr(), fontSize=9,
                           textColor=SLATE, alignment=TA_CENTER)
        ))
        doc.build(story)
        return buf.getvalue()

    # 이슈 섹션 (과잉 의심 항목만)
    sections: list[list] = []
    seq = 1
    if llm_issue:   sections.append(_build_ai_section(rag_result, symptom_text, seq, st)); seq += 1
    if over_parts:  sections.append(_build_parts_section(parts_df, seq, st));              seq += 1
    if over_labor:  sections.append(_build_labor_section(labor_df, seq, st));              seq += 1
    if early_cycle: sections.append(_build_cycle_section(cycle_issues, seq, st));          seq += 1

    for i, section in enumerate(sections):
        story.extend(section)
        if i < len(sections) - 1:
            story.append(Spacer(1, 10))
            story.append(_thin_rule(sb=0, sa=10))

    # 면책 안내
    story.append(Spacer(1, 16))
    story.append(_thin_rule())
    disc = Table([[Paragraph(
        "본 보고서는 공개된 시장 데이터 및 제조사 정비 지침서를 기반으로 AI가 자동 생성한 참고 자료입니다. "
        "법적 효력이 없으며, 이의 제기 시 한국소비자원(1372) 또는 자동차 관련 소비자 단체의 전문 상담을 권장합니다. "
        "최종 판단은 공인 정비 전문가의 의견을 따르시기 바랍니다.",
        st["disclaimer"]
    )]], colWidths=["100%"])
    disc.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), CREAM),
        ("BOX",           (0, 0), (-1, -1), 0.4, RULE_LIGHT),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
        ("TOPPADDING",    (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
    ]))
    story.append(disc)

    doc.build(story)
    return buf.getvalue()