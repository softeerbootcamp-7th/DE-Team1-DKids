#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Fetch Hyunki Store detail pages and write CSV parts to S3."""

from __future__ import annotations

import csv
import html
import io
import json
import logging
import os
import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
import requests


logger = logging.getLogger(__name__)

# Default User-Agent for HTTP requests.
DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
# Default S3 prefix for result CSV parts.
DEFAULT_RESULT_PREFIX = "maintenance_parts/hyunki_store/results"
# Default S3 prefix for skipped URL records.
DEFAULT_SKIP_PREFIX = "maintenance_parts/hyunki_store/skipped"
# Default log level.
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_TIMEOUT_SECONDS = 20
# CSV columns in order.
FIELDNAMES = [
    "extracted_at",
    "category",
    "name",
    "price",
    "official_name",
    "part_no",
    "applicable",
]


def _configure_logging() -> None:
    """Configure logging for Lambda."""
    level_name = os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def fetch(url: str, timeout: int = 20) -> str:
    """Fetch a URL and return the response body as text.

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


def clean_text(raw: str) -> str:
    """Remove HTML tags and normalize whitespace.

    Args:
        raw: Raw HTML string.

    Returns:
        Cleaned text.
    """
    text = re.sub(r"<[^>]+>", " ", raw)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def find_first(html_text: str, patterns: List[str]) -> str:
    """Find the first regex match from a list of patterns.

    Args:
        html_text: Full HTML content.
        patterns: List of regex patterns to search.

    Returns:
        First matched group text or empty string.
    """
    for pattern in patterns:
        match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return clean_text(match.group(1))
    return ""


def extract_fields(html_text: str) -> Dict[str, str]:
    """Extract fields from a Hyunki Store detail HTML page.

    Args:
        html_text: Detail page HTML.

    Returns:
        Dict of extracted fields.
    """
    official_name = find_first(
        html_text,
        [
            r"상품명</span>\s*</span>\s*<span class=\"con\">\s*<span[^>]*>(.*?)</span>",
            r"<div class=\"product_title[^>]*\">(.*?)<span class=\"delivery",
            r"var\s+product_name\s*=\s*'([^']*)'",
        ],
    )

    price = find_first(
        html_text,
        [
            r"id=\"span_product_price_text\">(.*?)</strong>",
            r"판매가</span>\s*</span>\s*<span class=\"con\">\s*<span[^>]*>(.*?)</span>",
        ],
    )
    if not price:
        match = re.search(r"var\s+product_price\s*=\s*'(\d+)'", html_text)
        if match:
            price = f"{int(match.group(1)):,}원"

    part_no = find_first(
        html_text,
        [
            r"부품번호</span>\s*</span>\s*<span class=\"con\">\s*<span[^>]*>(.*?)</span>",
            r"\(([\w\-]+)\)\s*<span class=\"delivery",
        ],
    )

    applicable = find_first(
        html_text,
        [
            r"\[차종\s*및\s*연식\]\s*</b>\s*<br>\s*(.*?)</span>",
            r"simple_desc_css[^>]*>.*?<span class=\"con\">\s*<span[^>]*>(.*?)</span>",
        ],
    )
    applicable = applicable.replace("[차종 및 연식]", "").strip()

    category = find_first(
        html_text,
        [
            r"<li class=\"\"><a href=\"/category/[^/]+/\d+/\">(.*?)</a></li>\s*<li class=\"displaynone\">",
            r"var\s+gfa_cate2\s*=\s*\"([^\"]*)\"",
            r"category_name\":\"([^\"]+)\"",
        ],
    )

    name = re.sub(r"\s*\([^)]*\)\s*$", "", official_name).strip()
    if not name:
        name = official_name

    return {
        "category": category,
        "name": name,
        "price": price,
        "official_name": official_name,
        "part_no": part_no,
        "applicable": applicable,
    }


def build_csv_lines(rows: List[Dict[str, str]]) -> str:
    """Build CSV lines without a header.

    Args:
        rows: List of row dictionaries.

    Returns:
        CSV text without a header row.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    for row in rows:
        writer.writerow([row.get(col, "") for col in FIELDNAMES])
    return buf.getvalue()


def _extract_urls(event: dict) -> Tuple[List[str], Optional[dict]]:
    """Extract URL list from event or Distributed Map batch format.

    Args:
        event: Lambda event payload.

    Returns:
        Tuple of (URL list, optional first item context).
    """
    items = event.get("Items") or event.get("items")
    if isinstance(items, list) and items:
        urls: List[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            raw = item.get("urls") or item.get("url")
            if isinstance(raw, list):
                urls.extend([str(u).strip() for u in raw if str(u).strip()])
            elif isinstance(raw, str) and raw.strip():
                urls.append(raw.strip())
        return urls, items[0] if isinstance(items[0], dict) else None

    raw_urls = event.get("urls") or event.get("url")
    if isinstance(raw_urls, list):
        return [str(u).strip() for u in raw_urls if str(u).strip()], None
    if isinstance(raw_urls, str) and raw_urls.strip():
        return [raw_urls.strip()], None
    return [], None


def _resolve_context(event: dict, item_ctx: Optional[dict]) -> dict:
    """Resolve common context values from event or item context.

    Args:
        event: Lambda event payload.
        item_ctx: First item context if using Distributed Map.

    Returns:
        Dict of resolved context values.
    """
    def pick(key: str, default=None):
        if key in event and event.get(key) is not None:
            return event.get(key)
        if item_ctx and item_ctx.get(key) is not None:
            return item_ctx.get(key)
        return default

    return {
        "bucket": pick("bucket"),
        "run_id": pick("run_id") or pick("run_id_fallback"),
        "key_prefix": pick("key_prefix", os.environ.get("RESULT_PREFIX", DEFAULT_RESULT_PREFIX)),
        "skip_prefix": pick("skip_prefix", os.environ.get("SKIP_PREFIX", DEFAULT_SKIP_PREFIX)),
        "batch_index": pick("batch_index"),
        "extracted_at": pick("extracted_at"),
        "execution_name": pick("execution_name"),
    }


def _write_skip_file(
    *,
    s3,
    bucket: str,
    skip_prefix: str,
    run_id: str,
    part_id: str,
    skipped: List[dict],
) -> Optional[str]:
    """Write skipped URL records to S3 (one JSON file per batch).

    Args:
        s3: Boto3 S3 client.
        bucket: Target bucket.
        skip_prefix: Prefix for skip records.
        run_id: Execution run id.
        part_id: Batch/part identifier.
        skipped: List of skipped URL records.

    Returns:
        S3 key for the skip record file, or None when empty.
    """
    if not skipped:
        return None
    key = f"{skip_prefix.rstrip('/')}/{run_id}/skip-{part_id}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(skipped, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    return key


def handler(event, context):
    """AWS Lambda handler to process Hyunki Store detail pages.

    Args:
        event: Lambda event payload.
        context: Lambda runtime context.

    Returns:
        Metadata about the written CSV part and skipped records.
    """
    _configure_logging()
    event = event or {}

    url_list, item_ctx = _extract_urls(event)
    if not url_list:
        raise ValueError("Missing url(s) in event")

    ctx = _resolve_context(event, item_ctx)
    bucket = ctx.get("bucket") or os.environ.get("RESULT_BUCKET")
    if not bucket:
        raise ValueError("Missing S3 bucket. Provide event.bucket or RESULT_BUCKET env.")

    run_id = ctx.get("run_id")
    if not run_id:
        raise ValueError("Missing run_id in event")

    key_prefix = ctx.get("key_prefix") or DEFAULT_RESULT_PREFIX
    skip_prefix = ctx.get("skip_prefix") or DEFAULT_SKIP_PREFIX

    part_id = ctx.get("batch_index")
    if part_id is None:
        part_id = ctx.get("execution_name") or datetime.now().strftime("%H%M%S")
    part_id = str(part_id)

    extracted_at = ctx.get("extracted_at") or datetime.now().isoformat(timespec="seconds")
    timeout_seconds = int(os.environ.get("REQUEST_TIMEOUT", DEFAULT_TIMEOUT_SECONDS))

    rows: List[Dict[str, str]] = []
    skipped: List[dict] = []

    for url in url_list:
        try:
            html_text = fetch(url, timeout=timeout_seconds)
            fields = extract_fields(html_text)
            fields["extracted_at"] = extracted_at
            rows.append(fields)
        except requests.Timeout:
            skipped.append(
                {
                    "url": url,
                    "status_code": None,
                    "reason": "timeout",
                    "skipped_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
            continue
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            if status_code == 404:
                skipped.append(
                    {
                        "url": url,
                        "status_code": 404,
                        "reason": "not_found",
                        "skipped_at": datetime.now().isoformat(timespec="seconds"),
                    }
                )
                continue
            raise

    if not rows and not skipped:
        logger.warning("No rows or skips produced for part_id=%s", part_id)

    csv_body = build_csv_lines(rows)
    result_key = f"{key_prefix.rstrip('/')}/{run_id}/part-{part_id}.csv"

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=result_key,
        Body=csv_body.encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )

    skip_key = _write_skip_file(
        s3=s3,
        bucket=bucket,
        skip_prefix=skip_prefix,
        run_id=run_id,
        part_id=part_id,
        skipped=skipped,
    )

    logger.info(
        "Worker complete: run_id=%s part=%s rows=%d skipped=%d",
        run_id,
        part_id,
        len(rows),
        len(skipped),
    )

    return {
        "status": "ok",
        "run_id": run_id,
        "s3_bucket": bucket,
        "s3_key": result_key,
        "rows": len(rows),
        "skipped": len(skipped),
        "skip_key": skip_key,
    }
