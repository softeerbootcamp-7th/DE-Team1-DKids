#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Fetch Partsro detail pages and write CSV parts to S3."""

from __future__ import annotations

import csv  # CSV formatting for output rows.
import hashlib  # Hashing for DynamoDB keys.
import io  # In-memory text buffer for CSV lines.
import json  # JSON serialization for skip records.
import logging  # Structured logging for Lambda.
import os  # Environment variable access.
import re  # Regex utilities for text cleanup.
import time  # TTL timestamps.
from datetime import datetime  # Timestamps for extracted/skip records.
from typing import Dict  # Type hints for parsed fields.

import boto3  # AWS SDK for S3 writes.
import requests  # HTTP client for fetching pages.
from bs4 import BeautifulSoup  # HTML parser.
from urllib.parse import parse_qs, urlparse  # URL parsing helpers.


logger = logging.getLogger(__name__)  # Module-level logger.

# Default User-Agent for HTTP requests.
DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
# S3 prefix for result CSV parts.
DEFAULT_RESULT_PREFIX = "raw/partsro/parts"
# S3 prefix for skipped URL records.
DEFAULT_SKIP_PREFIX = "raw/partsro/skipped"
# Default log level.
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_TIMEOUT_SECONDS = 20
# DynamoDB status tracking (optional).
DEFAULT_DDB_TTL_DAYS = 30
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
# Category ID to display name mapping.
CATEGORY_MAP = {
    177: "엔진",
    178: "미션",
    179: "샤시",
    180: "바디",
    181: "트림",
}
# Regex to extract category from URL path.
_CATEGORY_PATH_RE = re.compile(r"/category/(\d+)/")


def _configure_logging() -> None:
    """Configure logging for Lambda.

    Sets the global logging format and level based on LOG_LEVEL.
    """
    level_name = os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()  # Read level.
    level = getattr(logging, level_name, logging.INFO)  # Fallback to INFO.
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def fetch(url: str, timeout: int = 20) -> str:
    """Fetch a URL and return the response body as text.

    Args:
        url: Target URL.
        timeout: Request timeout in seconds.

    Returns:
        Response body as a decoded string.

    Raises:
        requests.exceptions.RequestException: For network/HTTP errors.
    """
    user_agent = os.environ.get("HTTP_USER_AGENT", DEFAULT_UA)  # Optional UA override.
    resp = requests.get(url, headers={"User-Agent": user_agent}, timeout=timeout)  # HTTP GET.
    resp.raise_for_status()  # Raise for 4xx/5xx.
    resp.encoding = resp.apparent_encoding  # Best-effort encoding.
    return resp.text  # Return decoded HTML.


def soup_from_html(html: str) -> BeautifulSoup:
    """Build BeautifulSoup parser from HTML.

    Args:
        html: Raw HTML string.

    Returns:
        BeautifulSoup instance.
    """
    return BeautifulSoup(html, "html.parser")  # Use standard parser.


def clean_text(s: str) -> str:
    """Normalize whitespace and trim.

    Args:
        s: Input string.

    Returns:
        Cleaned string.
    """
    return re.sub(r"\s+", " ", s).strip()  # Collapse whitespace and trim.


def infer_category_from_url(url: str) -> str:
    """Infer category label from URL path or query.

    Args:
        url: Product detail URL.

    Returns:
        Category name or empty string if not found.
    """
    match = _CATEGORY_PATH_RE.search(url)  # Try path-based category.
    if match:
        try:
            return CATEGORY_MAP.get(int(match.group(1)), "")  # Map to label.
        except ValueError:
            return ""  # Bad integer format.

    parts = urlparse(url)  # Parse query string if path failed.
    q = parse_qs(parts.query)  # Extract query parameters.
    cate_no = q.get("cate_no", [None])[0]  # Read cate_no.
    try:
        return CATEGORY_MAP.get(int(cate_no), "") if cate_no else ""  # Map to label.
    except ValueError:
        return ""  # Bad integer format.


def parse_detail(html: str) -> Dict[str, str]:
    """Parse detail page HTML into a dict of fields.

    Args:
        html: Detail page HTML.

    Returns:
        Dict with parsed fields (Korean labels as keys).
    """
    s = soup_from_html(html)  # Parse HTML.

    # Extract fields from the detail table.
    rows = s.select("div.xans-product-detaildesign table tr")  # Table rows.
    data: Dict[str, str] = {}  # Output dict.
    for tr in rows:  # Iterate each row.
        th = tr.find("th")  # Header cell.
        td = tr.find("td")  # Value cell.
        if not th or not td:
            continue  # Skip malformed rows.
        key = clean_text(th.get_text())  # Normalize header.
        for br in td.find_all("br"):  # Replace <br> with newline.
            br.replace_with("\n")
        val = clean_text(td.get_text().replace("\n", " / "))  # Normalize value.

        if key in ("상품명", "정식 부품명", "부품번호", "판매가", "적용차(생산연도)", "브랜드"):
            data[key] = val  # Keep only known keys.

    # Fallback: JSON-LD description may include applicable vehicles.
    if "적용차(생산연도)" not in data:
        ld = s.find("script", {"type": "application/ld+json"})  # JSON-LD block.
        if ld and ld.string:
            m = re.search(r'"description"\s*:\s*"([^"]+)"', ld.string)  # Find description.
            if m:
                data["적용차(생산연도)"] = clean_text(m.group(1))  # Store fallback.

    return data  # Return parsed fields.


def build_csv_lines(rows: list[Dict[str, str]]) -> str:
    """Build CSV lines (no header).

    Args:
        rows: List of row dicts.

    Returns:
        CSV string with one line per row.
    """
    buf = io.StringIO()  # In-memory buffer.
    writer = csv.writer(buf, lineterminator="\n")  # CSV writer.
    for row in rows:  # Write each row.
        writer.writerow([row.get(col, "") for col in FIELDNAMES])  # Preserve column order.
    return buf.getvalue()  # Return CSV text.


def _write_skip_file(
    *,
    s3,
    bucket: str,
    key_prefix: str,
    run_id: str,
    part_id: str,
    skipped: list[dict],
) -> str | None:
    """Write skipped URL records to S3.

    Args:
        s3: Boto3 S3 client.
        bucket: Target bucket.
        key_prefix: Prefix for skip records.
        run_id: Execution run id.
        part_id: Part identifier.
        skipped: List of skipped URL records.

    Returns:
        S3 key of the skip record file or None when empty.
    """
    if not skipped:
        return None
    key = f"{key_prefix.rstrip('/')}/{run_id}/skip-{part_id}.json"  # Build key.
    s3.put_object(  # Write JSON to S3.
        Bucket=bucket,
        Key=key,
        Body=json.dumps(skipped, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    return key  # Return S3 key.


def _ddb_table():
    """Return DynamoDB table if enabled."""
    name = os.environ.get("DDB_TABLE")
    if not name:
        return None
    return boto3.resource("dynamodb").Table(name)


def _ddb_item(
    *,
    source: str,
    url: str,
    status: str,
    reason: str,
    http_status: int | None,
    run_id: str,
    attempt: int,
    extracted_at: str,
) -> dict:
    dt = extracted_at[:10] if extracted_at else datetime.utcnow().strftime("%Y-%m-%d")
    ttl_days = int(os.environ.get("DDB_TTL_DAYS", DEFAULT_DDB_TTL_DAYS))
    ttl = int(time.time()) + ttl_days * 86400
    return {
        "pk": f"{source}#dt={dt}",
        "sk": hashlib.sha1(url.encode("utf-8")).hexdigest(),
        "source": source,
        "dt": dt,
        "run_id": run_id,
        "url": url,
        "status": status,
        "reason": reason or "",
        "http_status": http_status,
        "attempt": attempt,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
        "ttl": ttl,
    }


def handler(event, context):
    """AWS Lambda handler to process one or more detail URLs.

    Args:
        event: Lambda event payload.
        context: Lambda runtime context.

    Returns:
        Status and S3 key for the written CSV lines.
    """
    _configure_logging()  # Initialize logging.
    event = event or {}  # Normalize event.

    # Required inputs.
    url = event.get("url") or event.get("detail_url")  # Single URL input.
    urls = event.get("urls")  # Batch URL input (possible list or string).
    items = event.get("Items") or event.get("items")  # Distributed Map batch input.

    url_list: list[str] = []  # Final list of URLs.
    item_ctx = None  # First item context for shared fields.

    if isinstance(items, list) and items:  # Distributed Map batch input.
        item_ctx = items[0] if isinstance(items[0], dict) else None  # First item for metadata.
        for item in items:  # Iterate each item.
            if not isinstance(item, dict):
                continue  # Skip non-dict items.
            raw = item.get("urls") or item.get("url")  # URL value.
            if isinstance(raw, list):
                url_list.extend([str(u).strip() for u in raw if str(u).strip()])
            elif isinstance(raw, str) and raw.strip():
                url_list.append(raw.strip())
    elif isinstance(urls, list) and urls:  # Simple list input.
        url_list = [str(u).strip() for u in urls if str(u).strip()]
    elif isinstance(urls, str) and urls.strip():  # Single URL string.
        url_list = [urls.strip()]
    elif isinstance(url, str) and url.strip():  # Fallback to single URL field.
        url_list = [url.strip()]

    if not url_list:
        raise ValueError("Missing url(s) in event")  # Fail if no URLs.

    run_id = event.get("run_id") or event.get("run_id_fallback") or event.get("execution_name")
    if not run_id and isinstance(item_ctx, dict):  # Try per-item metadata.
        run_id = item_ctx.get("run_id") or item_ctx.get("run_id_fallback") or item_ctx.get("execution_name")
    if not run_id:  # Fallback to ExtractUrls payload if present.
        extract = event.get("extract") or {}
        payload = extract.get("payload") if isinstance(extract, dict) else None
        if isinstance(payload, dict):
            run_id = payload.get("run_id")
    if not run_id:
        raise ValueError("Missing run_id in event")  # Fail if still missing.

    index = event.get("batch_index")  # Batch index if provided.
    if index is None and isinstance(item_ctx, dict):  # Fall back to item context.
        index = item_ctx.get("batch_index") or item_ctx.get("index")
    if index is None:
        index = event.get("index")  # Fallback to single index.
    if index is None:
        index = abs(hash(url_list[0])) % 1_000_000  # Stable fallback for key.
    try:
        index_int = int(index)
        part_id = f"{index_int:06d}"  # Zero-padded id.
    except (TypeError, ValueError):
        part_id = str(index)  # Last-resort id.

    extracted_at = event.get("extracted_at")  # Timestamp override.
    if not extracted_at and isinstance(item_ctx, dict):
        extracted_at = item_ctx.get("extracted_at")  # Batch timestamp.
    if not extracted_at:
        extracted_at = datetime.now().isoformat(timespec="seconds")  # Default timestamp.

    attempt = event.get("attempt")
    if attempt is None and isinstance(item_ctx, dict):
        attempt = item_ctx.get("attempt")
    try:
        attempt = int(attempt) if attempt is not None else 1
    except (TypeError, ValueError):
        attempt = 1

    bucket = event.get("bucket")  # Bucket from top-level event.
    if not bucket and isinstance(item_ctx, dict):
        bucket = item_ctx.get("bucket")  # Bucket from item.
    if not bucket:
        bucket = os.environ.get("RESULT_BUCKET")  # Bucket from env.
    if not bucket:
        raise ValueError("Missing S3 bucket. Provide event.bucket or RESULT_BUCKET env.")

    key_prefix = event.get("key_prefix")  # Results prefix override.
    if not key_prefix and isinstance(item_ctx, dict):
        key_prefix = item_ctx.get("key_prefix")
    if not key_prefix:
        key_prefix = os.environ.get("RESULT_PREFIX", DEFAULT_RESULT_PREFIX)

    skip_prefix = event.get("skip_prefix")  # Skip prefix override.
    if not skip_prefix and isinstance(item_ctx, dict):
        skip_prefix = item_ctx.get("skip_prefix")
    if not skip_prefix:
        skip_prefix = os.environ.get("SKIP_PREFIX", DEFAULT_SKIP_PREFIX)

    s3 = boto3.client("s3")  # S3 client.
    ddb = _ddb_table()
    ddb_records: list[dict] = []

    rows: list[Dict[str, str]] = []  # Accumulated rows.
    skipped_records: list[dict] = []
    timeout_seconds = int(os.environ.get("REQUEST_TIMEOUT", DEFAULT_TIMEOUT_SECONDS))

    # Extract detail pages.
    for detail_url in url_list:
        category = event.get("category") or infer_category_from_url(detail_url)  # Infer category.
        try:
            detail_html = fetch(detail_url, timeout=timeout_seconds)  # Fetch HTML.
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 404:  # Skip missing pages.
                skipped_records.append(
                    {
                        "url": detail_url,
                        "status_code": status,
                        "reason": "not_found",
                        "skipped_at": datetime.now().isoformat(timespec="seconds"),
                    }
                )
                if ddb:
                    ddb_records.append(
                        _ddb_item(
                            source="partsro",
                            url=detail_url,
                            status="FAILED",
                            reason="not_found",
                            http_status=status,
                            run_id=run_id,
                            attempt=attempt,
                            extracted_at=extracted_at,
                        )
                    )
                logger.warning("skip 404: url=%s", detail_url)
                continue
            if ddb:
                ddb_records.append(
                    _ddb_item(
                        source="partsro",
                        url=detail_url,
                        status="FAILED",
                        reason="http_error",
                        http_status=status,
                        run_id=run_id,
                        attempt=attempt,
                        extracted_at=extracted_at,
                    )
                )
            raise  # Re-raise other HTTP errors.
        except requests.Timeout:
            skipped_records.append(
                {
                    "url": detail_url,
                    "status_code": None,
                    "reason": "timeout",
                    "skipped_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
            if ddb:
                ddb_records.append(
                    _ddb_item(
                        source="partsro",
                        url=detail_url,
                        status="FAILED",
                        reason="timeout",
                        http_status=None,
                        run_id=run_id,
                        attempt=attempt,
                        extracted_at=extracted_at,
                    )
                )
            continue
        detail = parse_detail(detail_html)  # Parse fields.

        rows.append(  # Build row dict.
            {
                "extracted_at": extracted_at,
                "category": category,
                "name": detail.get("상품명", ""),
                "price": detail.get("판매가", ""),
                "official_name": detail.get("정식 부품명", ""),
                "part_no": detail.get("부품번호", ""),
                "applicable": detail.get("적용차(생산연도)", ""),
            }
        )
        if ddb:
            ddb_records.append(
                _ddb_item(
                    source="partsro",
                    url=detail_url,
                    status="SUCCESS",
                    reason="",
                    http_status=200,
                    run_id=run_id,
                    attempt=attempt,
                    extracted_at=extracted_at,
                )
            )

    skipped = len(skipped_records)
    skip_key = _write_skip_file(
        s3=s3,
        bucket=bucket,
        key_prefix=skip_prefix,
        run_id=run_id,
        part_id=part_id,
        skipped=skipped_records,
    )

    if not rows:  # If everything was skipped.
        if ddb and ddb_records:
            with ddb.batch_writer() as batch:
                for item in ddb_records:
                    batch.put_item(Item=item)
        return {
            "status": "skipped",
            "run_id": run_id,
            "s3_bucket": bucket,
            "skipped": skipped,
            "skip_key": skip_key,
        }

    line = build_csv_lines(rows)  # Build CSV lines.

    key = f"{key_prefix.rstrip('/')}/{run_id}/part-{part_id}.csv"  # Output key.

    s3.put_object(  # Upload CSV part.
        Bucket=bucket,
        Key=key,
        Body=line.encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )

    if ddb and ddb_records:
        with ddb.batch_writer() as batch:
            for item in ddb_records:
                batch.put_item(Item=item)

    logger.info(
        "worker complete: run_id=%s key=%s rows=%d skipped=%d",
        run_id,
        key,
        len(rows),
        skipped,
    )
    return {
        "status": "ok",
        "run_id": run_id,
        "s3_bucket": bucket,
        "s3_key": key,
        "rows": len(rows),
        "skipped": skipped,
        "skip_key": skip_key,
    }
