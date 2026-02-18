#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Extract detail URLs for Hyunki Store and upload to S3.

This Lambda reads list/category pages via the site's API, collects detail URLs,
then stores them as a JSON array in S3 for downstream Map processing.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from typing import Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import boto3
import requests


logger = logging.getLogger(__name__)

# Default User-Agent for HTTP requests.
DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
# Default list (category) URLs for Hyunki Store.
DEFAULT_CATEGORY_URLS = [
    "https://hyunkistore.com/category/%ED%8A%B8%EB%A6%BC/54/",
    "https://hyunkistore.com/category/%EC%97%94%EC%A7%84/50/",
    "https://hyunkistore.com/category/%EC%A0%84%EA%B8%B0%EC%9E%A5%EC%B9%98/55/",
    "https://hyunkistore.com/category/%EB%AF%B8%EC%85%98/51/",
    "https://hyunkistore.com/category/%EC%83%A4%EC%8B%9C/52/",
    "https://hyunkistore.com/category/%EB%B0%94%EB%94%94/53/",
    "https://hyunkistore.com/category/%EA%B8%B0%ED%83%80/57/",
]
# Default request count for the API.
DEFAULT_COUNT = 500
# Default supplier code for the API.
DEFAULT_SUPPLIER_CODE = "S0000000"
# Default S3 prefix for URL list outputs.
DEFAULT_KEY_PREFIX = "raw/hyunki_store/urls"
# Default log level.
DEFAULT_LOG_LEVEL = "INFO"


def _configure_logging() -> None:
    """Configure logging for Lambda."""
    level_name = os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def fetch(url: str, timeout: int = 15) -> str:
    """Fetch a URL and return the response body.

    Args:
        url: Target URL.
        timeout: Request timeout in seconds.

    Returns:
        Response body as text.
    """
    user_agent = os.environ.get("HTTP_USER_AGENT", DEFAULT_UA)
    resp = requests.get(url, headers={"User-Agent": user_agent}, timeout=timeout)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    return resp.text


def normalize_list_url(list_url: str) -> str:
    """Remove whitespace from list URLs.

    Args:
        list_url: Raw list URL string.

    Returns:
        Normalized URL without whitespace.
    """
    return re.sub(r"\s+", "", list_url)


def extract_cate_no(list_url: str) -> str:
    """Extract cate_no from query or /category/.../<id>/ path.

    Args:
        list_url: Category/list URL.

    Returns:
        Category number as string.
    """
    list_url = normalize_list_url(list_url)
    parts = urlparse(list_url)
    q = parse_qs(parts.query)
    cate_no = q.get("cate_no", [None])[0]
    if cate_no:
        return cate_no
    match = re.search(r"/category/[^/]+/(\d+)/", parts.path)
    if match:
        return match.group(1)
    raise ValueError("list_url must include cate_no or /category/.../<id>/ path")


def build_api_url(
    list_url: str,
    page: int,
    count: int,
    supplier_code: str,
    b_init_more: str = "F",
) -> str:
    """Build the site API URL for a list page.

    Args:
        list_url: Category/list URL.
        page: Page number (1-based).
        count: Items per page.
        supplier_code: Supplier code parameter.
        b_init_more: API flag.

    Returns:
        Fully qualified API URL.
    """
    parts = urlparse(normalize_list_url(list_url))
    cate_no = extract_cate_no(list_url)
    api_q = {
        "cate_no": cate_no,
        "supplier_code": supplier_code,
        "page": str(page),
        "bInitMore": b_init_more,
        "count": str(count),
    }
    return urlunparse((parts.scheme, parts.netloc, "/exec/front/Product/ApiProductNormal", "", urlencode(api_q), ""))


def parse_api_payload(payload: str, base_url: str) -> Tuple[List[str], bool]:
    """Parse the JSON payload returned by the web page API.

    The web page provides JSON like:
      {"rtn_code":"1000","rtn_data":{"data":[{...}],"end":false}}

    Args:
        payload: Raw JSON string.
        base_url: Base URL for resolving relative links.

    Returns:
        Tuple of (detail URLs, is_end flag).
    """
    try:
        data = json.loads(payload)
    except Exception:
        return [], True

    if not isinstance(data, dict):
        return [], True

    rtn = data.get("rtn_data")
    if not isinstance(rtn, dict):
        return [], True

    items: List[str] = []
    raw_list = rtn.get("data")
    if isinstance(raw_list, list):
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            href = item.get("link_product_detail") or ""
            if not href:
                continue
            items.append(urljoin(base_url, href))

    is_end = bool(rtn.get("end"))
    return items, is_end


def iter_detail_urls(
    list_url: str,
    max_pages: Optional[int],
    count: int,
    supplier_code: str,
) -> Iterable[str]:
    """Yield detail URLs for a single category/list URL.

    Args:
        list_url: Category/list URL.
        max_pages: Optional max number of pages to scan.
        count: Items per page.
        supplier_code: Supplier code parameter.

    Yields:
        Detail page URLs.
    """
    page = 1
    while True:
        if max_pages is not None and page > max_pages:
            break
        api_url = build_api_url(
            list_url=list_url,
            page=page,
            count=count,
            supplier_code=supplier_code,
            b_init_more="F",
        )
        payload = fetch(api_url)
        items, is_end = parse_api_payload(payload, list_url)
        if not items:
            break
        for url in items:
            yield url
        if is_end:
            break
        page += 1


def _collect_list_urls(event: dict) -> List[str]:
    """Resolve category/list URLs from event, environment, or defaults.

    Args:
        event: Lambda event payload.

    Returns:
        List of category/list URLs to scan.
    """
    if isinstance(event.get("category_urls"), list):
        return [str(u).strip() for u in event["category_urls"] if str(u).strip()]

    list_url = event.get("list_url")
    if isinstance(list_url, str) and list_url.strip():
        return [list_url.strip()]

    env_json = os.environ.get("CATEGORY_URLS_JSON")
    if env_json:
        try:
            data = json.loads(env_json)
            if isinstance(data, list):
                return [str(u).strip() for u in data if str(u).strip()]
        except Exception:
            logger.warning("Invalid CATEGORY_URLS_JSON, ignoring")

    env_csv = os.environ.get("CATEGORY_URLS")
    if env_csv:
        return [u.strip() for u in env_csv.split(",") if u.strip()]

    return list(DEFAULT_CATEGORY_URLS)


def handler(event, context):
    """AWS Lambda handler to extract and store detail URLs.

    Args:
        event: Lambda event payload.
        context: Lambda runtime context.

    Returns:
        Metadata about the stored URL list.
    """
    _configure_logging()
    event = event or {}

    bucket = event.get("bucket") or os.environ.get("URLS_BUCKET")
    if not bucket:
        raise ValueError("Missing S3 bucket. Provide event.bucket or URLS_BUCKET env.")

    key_prefix = event.get("key_prefix") or os.environ.get(
        "URLS_KEY_PREFIX", DEFAULT_KEY_PREFIX
    )
    run_id = event.get("run_id") or datetime.now().strftime("%Y%m%d_%H%M%S")
    max_pages = event.get("max_pages") or os.environ.get("MAX_PAGES")
    if max_pages is not None:
        max_pages = int(max_pages)

    count = int(event.get("count") or os.environ.get("COUNT", DEFAULT_COUNT))
    supplier_code = event.get("supplier_code") or os.environ.get(
        "SUPPLIER_CODE", DEFAULT_SUPPLIER_CODE
    )

    list_urls = _collect_list_urls(event)
    if not list_urls:
        raise ValueError("No list URLs provided")

    logger.info(
        "Start extraction: list_urls=%d count=%s supplier_code=%s max_pages=%s run_id=%s",
        len(list_urls),
        count,
        supplier_code,
        max_pages,
        run_id,
    )

    detailed_urls: List[str] = []
    seen = set()
    for list_url in list_urls:
        for url in iter_detail_urls(list_url, max_pages, count, supplier_code):
            if url in seen:
                continue
            seen.add(url)
            detailed_urls.append(url)

    key = f"{key_prefix.rstrip('/')}/{run_id}/urls.json"
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(detailed_urls, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )

    logger.info("Upload complete: bucket=%s key=%s count=%d", bucket, key, len(detailed_urls))
    return {
        "status": "ok",
        "count": len(detailed_urls),
        "s3_bucket": bucket,
        "urls_key": key,
        "run_id": run_id,
    }
