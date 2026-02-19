import argparse
import base64
import os
import re
import time
import urllib.error
import urllib.request

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

DEFAULT_URL = "https://gsw.hyundai.com/manualV2/cnts/view/SHOP"


def _safe_name(text: str) -> str:
    text = (text or "").strip()
    return re.sub(r"[\\/:*?\"<>|]+", "_", text).replace("\n", " ").strip() or "unknown"


def check_debugger_endpoint(debugger_address: str, timeout_sec: int = 2) -> tuple[bool, str]:
    url = f"http://{debugger_address}/json/version"
    try:
        with urllib.request.urlopen(url, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return True, raw
    except (urllib.error.URLError, TimeoutError, ValueError) as e:
        return False, str(e)


def attach_driver(debugger_address: str) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_experimental_option("debuggerAddress", debugger_address)
    return webdriver.Chrome(options=options)


def switch_to_matching_tab_or_get(driver: webdriver.Chrome, url: str, no_get: bool) -> None:
    urls: list[str] = []
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        cur = driver.current_url or ""
        urls.append(cur)
        if url in cur:
            return

    if not no_get:
        driver.get(url)
        return

    raise RuntimeError(
        "--no-get 옵션인데 대상 탭을 찾지 못했습니다.\n"
        f"target={url}\n"
        + "opened=\n - "
        + "\n - ".join(urls)
    )


def wait_for_model_select(driver: webdriver.Chrome, wait_sec: int):
    wait = WebDriverWait(driver, wait_sec)
    return wait.until(EC.presence_of_element_located((By.ID, "mdlCd")))


def get_models(driver: webdriver.Chrome, wait_sec: int) -> list[tuple[str, str]]:
    mdl = wait_for_model_select(driver, wait_sec)
    rows: list[tuple[str, str]] = []
    for opt in mdl.find_elements(By.TAG_NAME, "option"):
        value = (opt.get_attribute("value") or "").strip()
        text = (opt.text or "").strip()
        if not value or value == "select":
            continue
        rows.append((value, text))
    return rows


def select_model(driver: webdriver.Chrome, value: str) -> None:
    driver.execute_script(
        """
        const el = document.getElementById('mdlCd');
        if (!el) return;
        el.value = arguments[0];
        el.dispatchEvent(new Event('change', { bubbles: true }));
        if (typeof selectMdlCd === 'function') {
            selectMdlCd(arguments[0], 'false');
        }
        """,
        value,
    )


def wait_toc_ready(driver: webdriver.Chrome, wait_sec: int) -> None:
    wait = WebDriverWait(driver, wait_sec)
    wait.until(EC.presence_of_element_located((By.ID, "tocData")))
    wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "#tocData > dt > span[id^='IN_']")) > 0)


def get_top_trim_ids(driver: webdriver.Chrome) -> list[str]:
    ids = driver.execute_script(
        """
        return Array.from(document.querySelectorAll('#tocData > dt > span[id^="IN_"]'))
          .map(x => x.id)
          .filter(Boolean);
        """
    )
    return list(ids or [])


def click_and_wait_sub_loaded(driver: webdriver.Chrome, span_id: str, wait_sec: int) -> str:
    sub_id = driver.execute_script(
        """
        const sid = arguments[0];
        const span = document.getElementById(sid);
        if (!span) return '';
        const dt = span.closest('dt');
        if (!dt) return '';
        let dl = dt.querySelector(':scope > dl[id]');
        if (!dl && sid.startsWith('IN_')) {
          dl = document.getElementById(sid.replace('IN_', 'SUB_'));
        }
        return dl && dl.id ? dl.id : '';
        """,
        span_id,
    )
    sub_id = str(sub_id or "")
    if not sub_id:
        raise RuntimeError(f"sub id 탐색 실패: {span_id}")

    def _is_ready() -> bool:
        return bool(
            driver.execute_script(
                """
                const sid = arguments[0];
                const subId = arguments[1];
                const span = document.getElementById(sid);
                const el = document.getElementById(subId);
                if (!span || !el) return false;
                const cs = window.getComputedStyle(el);
                if (cs.display === 'none' || cs.visibility === 'hidden') return false;
                // direct child만 검사해서 하위 depth의 Loading...에 영향받지 않게 함
                const nodeCount = el.querySelectorAll(':scope > dt > span[id^="IN_"], :scope > dd[id^="LEAF_"]').length;
                const klass = (span.className || '').toLowerCase();
                return (klass.includes('open') || klass.includes('selected')) && nodeCount > 0;
                """,
                span_id,
                sub_id,
            )
        )

    # 이미 열려 있고 내용이 있으면 그대로 사용
    if _is_ready():
        return sub_id

    # 닫힘/미로딩이면 클릭 후 준비 상태 대기 (토글 UI라 최대 2회)
    wait = WebDriverWait(driver, wait_sec)
    for _ in range(2):
        driver.execute_script(
            """
            const el = document.getElementById(arguments[0]);
            if (el) el.click();
            """,
            span_id,
        )
        try:
            wait.until(lambda d: _is_ready())
            return sub_id
        except TimeoutException:
            continue

    raise TimeoutException(f"노드 로딩 타임아웃: span_id={span_id}, sub_id={sub_id}")
    return sub_id


def get_span_text(driver: webdriver.Chrome, span_id: str) -> str:
    text = driver.execute_script(
        """
        const el = document.getElementById(arguments[0]);
        return el ? (el.textContent || '').trim() : '';
        """,
        span_id,
    )
    return str(text or "")


def get_child_span_ids(driver: webdriver.Chrome, sub_id: str) -> list[str]:
    ids = driver.execute_script(
        """
        const dl = document.getElementById(arguments[0]);
        if (!dl) return [];
        return Array.from(dl.querySelectorAll(':scope > dt > span[id^="IN_"]'))
          .map(x => x.id)
          .filter(Boolean);
        """,
        sub_id,
    )
    return list(ids or [])


def get_diagnosis_leafs(driver: webdriver.Chrome, sub_id: str) -> list[dict[str, str]]:
    rows = driver.execute_script(
        """
        const dl = document.getElementById(arguments[0]);
        if (!dl) return [];
        return Array.from(dl.querySelectorAll(':scope > dd[id^="LEAF_"]'))
          .map(dd => ({
            id: dd.id || '',
            title: (dd.textContent || '').trim(),
            onclick: dd.getAttribute('onclick') || ''
          }))
          .filter(x => x.title === '고장진단');
        """,
        sub_id,
    )
    return list(rows or [])


def click_leaf(driver: webdriver.Chrome, leaf_id: str, wait_sec: int) -> None:
    driver.execute_script(
        """
        const el = document.getElementById(arguments[0]);
        if (el) el.click();
        """,
        leaf_id,
    )
    wait = WebDriverWait(driver, wait_sec)
    try:
        wait.until(
            lambda d: d.execute_script(
                """
                const t = document.getElementById('titleId');
                return t && (t.textContent || '').includes('고장진단');
                """
            )
        )
    except TimeoutException:
        pass
    time.sleep(0.8)


def write_pdf_from_cdp(driver: webdriver.Chrome, out_path: str) -> None:
    # GSW 본문이 고정 높이 + 내부 스크롤 구조라 인쇄 전 강제로 전체 펼침
    driver.execute_script(
        """
        const setStyle = (el) => {
          if (!el) return;
          el.style.setProperty('height', 'auto', 'important');
          el.style.setProperty('max-height', 'none', 'important');
          el.style.setProperty('overflow', 'visible', 'important');
          el.style.setProperty('position', 'static', 'important');
        };

        setStyle(document.documentElement);
        setStyle(document.body);

        [
          '.absoluteBox',
          '#sample2',
          '#contentArea',
          '.manual_contentBlock',
          '.manual_content',
          '#contentDatatable',
          '#contentData',
          '.manual_main',
          '#diagramTable',
          '#diagramCnts'
        ].forEach((sel) => {
          document.querySelectorAll(sel).forEach(setStyle);
        });

        window.scrollTo(0, 0);
        window.dispatchEvent(new Event('resize'));
        """
    )
    time.sleep(0.35)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    result = driver.execute_cdp_cmd(
        "Page.printToPDF",
        {
            "printBackground": True,
            "preferCSSPageSize": False,
            "landscape": False,
            "paperWidth": 8.27,
            "paperHeight": 11.69,
            "marginTop": 0.2,
            "marginBottom": 0.2,
            "marginLeft": 0.2,
            "marginRight": 0.2,
        },
    )
    data = result.get("data")
    if not data:
        raise RuntimeError("printToPDF 결과가 비어 있습니다.")
    with open(out_path, "wb") as f:
        f.write(base64.b64decode(data))


def collect_and_save_for_model(
    driver: webdriver.Chrome,
    model_text: str,
    wait_sec: int,
    out_dir: str,
    verbose: bool,
    all_trims: bool,
) -> int:
    wait_toc_ready(driver, wait_sec)
    trim_ids = get_top_trim_ids(driver)
    if not all_trims and trim_ids:
        trim_ids = trim_ids[:1]
    total_saved = 0

    for trim_idx, trim_id in enumerate(trim_ids, start=1):
        trim_text = get_span_text(driver, trim_id)
        if verbose:
            print(f"[LOG] trim[{trim_idx}/{len(trim_ids)}]: {trim_text} ({trim_id})")

        trim_sub_id = click_and_wait_sub_loaded(driver, trim_id, wait_sec)
        system_ids = get_child_span_ids(driver, trim_sub_id)

        for sys_id in system_ids:
            sys_text = get_span_text(driver, sys_id)
            sys_sub_id = click_and_wait_sub_loaded(driver, sys_id, wait_sec)
            topic_ids = get_child_span_ids(driver, sys_sub_id)

            for topic_id in topic_ids:
                topic_text = get_span_text(driver, topic_id)
                topic_sub_id = click_and_wait_sub_loaded(driver, topic_id, wait_sec)
                leafs = get_diagnosis_leafs(driver, topic_sub_id)

                for leaf in leafs:
                    leaf_id = str(leaf.get("id") or "")
                    if not leaf_id:
                        continue
                    click_leaf(driver, leaf_id, wait_sec)
                    filename = f"{_safe_name(topic_text)}_고장진단_{leaf_id}.pdf"
                    out_path = os.path.join(
                        out_dir,
                        _safe_name(model_text),
                        _safe_name(trim_text),
                        _safe_name(sys_text),
                        filename,
                    )
                    write_pdf_from_cdp(driver, out_path)
                    total_saved += 1
                    if verbose:
                        print(f"[LOG] saved={out_path}")

    return total_saved


def main() -> None:
    parser = argparse.ArgumentParser(description="GSW 차종별 고장진단 PDF 수집기")
    parser.add_argument("--debugger-address", default="127.0.0.1:9222")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--wait", type=int, default=20)
    parser.add_argument("--sleep", type=float, default=1.0)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0, help="0이면 끝까지")
    parser.add_argument("--no-get", action="store_true")
    parser.add_argument("--list-only", action="store_true")
    parser.add_argument("--out-dir", default="downloads")
    parser.add_argument("--all-trims", action="store_true", help="기본은 최상위 트림 1개만 처리, 지정 시 전체 트림 처리")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    reachable, detail = check_debugger_endpoint(args.debugger_address)
    if not reachable:
        raise RuntimeError(
            "Chrome 원격 디버깅 포트에 연결할 수 없습니다.\n"
            f"debugger-address={args.debugger_address}\n"
            "open -na \"Google Chrome\" --args --remote-debugging-port=9222"
        )
    if args.verbose:
        print(f"[LOG] debugger ok: {detail[:120]}")

    driver = attach_driver(args.debugger_address)
    switch_to_matching_tab_or_get(driver, args.url, args.no_get)

    models = get_models(driver, args.wait)
    if args.list_only:
        for i, (value, text) in enumerate(models):
            print(f"[{i:03d}] {text} ({value})")
        return

    if args.start_index >= len(models):
        raise RuntimeError(f"start-index({args.start_index}) >= model_count({len(models)})")

    end_index = len(models) if args.limit == 0 else min(len(models), args.start_index + args.limit)

    grand_total = 0
    for i in range(args.start_index, end_index):
        value, model_text = models[i]
        if args.verbose:
            print(f"[LOG] model[{i}] {model_text} ({value})")

        select_model(driver, value)
        wait_toc_ready(driver, args.wait)

        saved = collect_and_save_for_model(
            driver=driver,
            model_text=model_text,
            wait_sec=args.wait,
            out_dir=args.out_dir,
            verbose=args.verbose,
            all_trims=args.all_trims,
        )
        grand_total += saved
        print(f"[{i:03d}] {model_text} ({value}) -> saved_pdf={saved}")
        time.sleep(args.sleep)

    print(f"완료 total_saved_pdf={grand_total}")


if __name__ == "__main__":
    main()
