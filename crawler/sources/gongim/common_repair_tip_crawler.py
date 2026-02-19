import argparse
import csv
import os
import re
import time
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    def tqdm(iterable=None, *args, **kwargs):
        return iterable


DEFAULT_LIST_URL = "https://www.gongim.com/front/html/community.php?boSeq=51"


def clean_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = text.replace("\r", "")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def flat_text(text: str) -> str:
    text = clean_text(text)
    # CSV 뷰어에서 행 깨짐 방지를 위해 줄바꿈을 공백으로 평탄화
    text = re.sub(r"\s*\n\s*", " ", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )
    return s


def get_html(session: requests.Session, url: str, timeout: int = 20) -> str:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    if resp.apparent_encoding:
        resp.encoding = resp.apparent_encoding
    return resp.text


def parse_seq_from_href(href: str) -> str:
    query = parse_qs(urlparse(href).query)
    return (query.get("seq") or [""])[0]


def extract_total_pages(list_html: str) -> int:
    soup = BeautifulSoup(list_html, "html.parser")
    max_page = 0
    for a in soup.select(".paging a[href*='page=']"):
        href = a.get("href") or ""
        try:
            page = int((parse_qs(urlparse(href).query).get("page") or ["0"])[0])
        except ValueError:
            continue
        if page > max_page:
            max_page = page
    return max_page


def extract_paging_numbers(list_html: str) -> list[int]:
    soup = BeautifulSoup(list_html, "html.parser")
    pages: set[int] = set()
    for a in soup.select(".paging a[href*='page=']"):
        href = a.get("href") or ""
        try:
            page = int((parse_qs(urlparse(href).query).get("page") or ["0"])[0])
        except ValueError:
            continue
        if page > 0:
            pages.add(page)
    return sorted(pages)


def iter_post_links(list_html: str, list_url: str, include_notice: bool) -> list[tuple[str, str]]:
    soup = BeautifulSoup(list_html, "html.parser")
    rows = soup.select("tbody tr")
    links: list[tuple[str, str]] = []

    for row in rows:
        row_class = " ".join(row.get("class", []))
        if (not include_notice) and ("notice_bg" in row_class):
            continue

        a = row.select_one("td.subject a[href*='view.php?']")
        if not a:
            continue

        href = (a.get("href") or "").strip()
        if not href:
            continue

        abs_url = urljoin(list_url, href)
        seq = parse_seq_from_href(abs_url)
        if not seq:
            continue
        links.append((seq, abs_url))

    return links


def extract_title(soup: BeautifulSoup) -> str:
    selectors = [
        "div.detail.view .head.pc-view .subject",
        "div.detail.view .head.mo-view .subject",
        "div.detail.view .subject",
    ]
    for sel in selectors:
        node = soup.select_one(sel)
        if node:
            text = flat_text(node.get_text(" ", strip=True))
            if text:
                return text
    if soup.title:
        return flat_text(soup.title.get_text(" ", strip=True))
    return ""


def extract_body(soup: BeautifulSoup) -> str:
    node = soup.select_one("div.detail.view .cont")
    if not node:
        return ""
    for bad in node.select("script, style, noscript"):
        bad.decompose()
    return flat_text(node.get_text("\n", strip=True))


def extract_comments(soup: BeautifulSoup) -> str:
    comments: list[str] = []
    for li in soup.select("#commentSkinBox .commentBox > li"):
        c = li.select_one(".commentCont")
        if not c:
            continue
        txt = flat_text(c.get_text("\n", strip=True))
        if txt:
            comments.append(txt)
    return " || ".join(comments)


def parse_detail(html: str) -> tuple[str, str, str]:
    soup = BeautifulSoup(html, "html.parser")
    title = extract_title(soup)
    body = extract_body(soup)
    comments = extract_comments(soup)
    return title, body, comments


def load_seen_seq(state_file: str) -> set[str]:
    if not os.path.exists(state_file):
        return set()
    seen: set[str] = set()
    with open(state_file, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                seen.add(s)
    return seen


def append_seen_seq(state_file: str, seq: str) -> None:
    with open(state_file, "a", encoding="utf-8") as f:
        f.write(seq + "\n")


def discover_pages(
    session: requests.Session,
    list_url: str,
    start_page: int,
    max_pages: int,
    delay_sec: float,
) -> list[int]:
    pending_pages: list[int] = [start_page]
    seen_pages: set[int] = set()
    scan_bar = tqdm(total=max_pages if max_pages > 0 else None, desc="페이지 스캔", unit="page")

    while pending_pages:
        if max_pages > 0 and len(seen_pages) >= max_pages:
            break

        page = pending_pages.pop(0)
        if page in seen_pages:
            continue
        seen_pages.add(page)

        page_url = f"{list_url}{'&' if '?' in list_url else '?'}page={page}"
        list_html = get_html(session, page_url)

        for p in extract_paging_numbers(list_html):
            if p >= start_page and p not in seen_pages and p not in pending_pages:
                pending_pages.append(p)
        pending_pages.sort()

        if max_pages == 0 and scan_bar.total is not None:
            discovered_max = max([*seen_pages, *pending_pages], default=start_page)
            wanted_total = discovered_max - start_page + 1
            if wanted_total > scan_bar.total:
                scan_bar.total = wanted_total
                scan_bar.refresh()

        scan_bar.update(1)
        if delay_sec > 0:
            time.sleep(delay_sec)

    scan_bar.close()
    pages = sorted(seen_pages)
    if max_pages > 0:
        pages = pages[:max_pages]
    return pages


def crawl_all(
    list_url: str,
    out_csv: str,
    include_notice: bool,
    start_page: int,
    max_pages: int,
    delay_sec: float,
    resume: bool,
    state_file: str,
) -> None:
    session = make_session()
    seen_seq: set[str] = load_seen_seq(state_file) if resume else set()
    saved = 0
    pages = discover_pages(
        session=session,
        list_url=list_url,
        start_page=start_page,
        max_pages=max_pages,
        delay_sec=0.0,
    )
    if not pages:
        raise RuntimeError("페이지를 찾지 못했습니다. URL/접근 권한을 확인하세요.")

    write_mode = "a" if resume and os.path.exists(out_csv) else "w"
    with open(out_csv, write_mode, encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        if write_mode == "w":
            writer.writerow(["제목", "내용", "답글"])

        page_bar = tqdm(pages, total=len(pages), desc="전체 페이지", unit="page")
        for page in page_bar:
            page_url = f"{list_url}{'&' if '?' in list_url else '?'}page={page}"
            try:
                list_html = get_html(session, page_url)
            except requests.RequestException as e:
                print(f"[WARN] page fetch 실패 page={page}: {e}")
                continue
            post_links = iter_post_links(list_html, page_url, include_notice)

            new_in_page = 0
            for seq, detail_url in post_links:
                if seq in seen_seq:
                    continue
                seen_seq.add(seq)
                new_in_page += 1

                try:
                    detail_html = get_html(session, detail_url)
                    title, body, comments = parse_detail(detail_html)
                    writer.writerow([title, body, comments])
                    saved += 1
                    seen_seq.add(seq)
                    append_seen_seq(state_file, seq)
                except requests.RequestException as e:
                    print(f"[WARN] detail fetch 실패 seq={seq}: {e}")
                    continue

                if delay_sec > 0:
                    time.sleep(delay_sec)

            print(f"[PAGE {page}] found={len(post_links)} new={new_in_page} saved_total={saved}")
        page_bar.close()

    print(f"완료: {out_csv} / rows={saved}")


def main() -> None:
    parser = argparse.ArgumentParser(description="공임나라 boSeq=51 전체 크롤러 (CSV)")
    parser.add_argument("--url", default=DEFAULT_LIST_URL, help="목록 페이지 URL")
    parser.add_argument("--out", default="gongim_qna.csv", help="출력 CSV 파일")
    parser.add_argument("--start-page", type=int, default=1, help="시작 페이지")
    parser.add_argument("--max-pages", type=int, default=0, help="최대 페이지 수(0이면 끝까지)")
    parser.add_argument("--delay", type=float, default=0.2, help="요청 간 대기(초)")
    parser.add_argument(
        "--exclude-notice",
        action="store_true",
        help="공지글 제외 (기본은 공지 포함)",
    )
    parser.add_argument("--resume", action="store_true", help="기존 CSV/상태파일 기준 이어서 진행")
    parser.add_argument("--state-file", default="", help="resume용 상태파일 경로 (기본: <out>.state)")
    args = parser.parse_args()
    state_file = args.state_file.strip() or f"{args.out}.state"

    crawl_all(
        list_url=args.url,
        out_csv=args.out,
        include_notice=not args.exclude_notice,
        start_page=args.start_page,
        max_pages=args.max_pages,
        delay_sec=args.delay,
        resume=args.resume,
        state_file=state_file,
    )


if __name__ == "__main__":
    main()
