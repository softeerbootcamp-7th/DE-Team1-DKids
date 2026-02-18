#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Extract detail URLs for Partsro and upload to S3."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from extractor_core import extract_all_detail_urls
from s3_uploader import upload_json_to_s3

DEFAULT_LIST_URL = "https://m.partsro.com/product/list_thumb.html?cate_no=177"
DEFAULT_SUPPLIER_CODE = "S0000000"
DEFAULT_COUNT = 500
DEFAULT_KEY_PREFIX = "raw/partsro/urls"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_CATEGORY_MAP = {
    177: "엔진",
    178: "미션",
    179: "샤시",
    180: "바디",
    181: "트림",
}
_LOG = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Configure logging for Lambda."""
    level_name = os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def _load_category_map() -> dict[int, str]:
    """Load category map from environment if provided.

    Expected env: CATEGORY_MAP_JSON={"177":"엔진", ...}

    Returns:
        Mapping of category id to label.
    """
    raw = os.environ.get("CATEGORY_MAP_JSON")
    if not raw:
        return DEFAULT_CATEGORY_MAP
    try:
        data = json.loads(raw)
    except Exception:
        _LOG.warning("Invalid CATEGORY_MAP_JSON, using defaults")
        return DEFAULT_CATEGORY_MAP
    if not isinstance(data, dict):
        _LOG.warning("CATEGORY_MAP_JSON is not a dict, using defaults")
        return DEFAULT_CATEGORY_MAP
    mapped: dict[int, str] = {}
    for key, value in data.items():
        try:
            mapped[int(key)] = str(value)
        except Exception:
            continue
    return mapped or DEFAULT_CATEGORY_MAP


def handler(event, context):
    """AWS Lambda handler entry point.

    Args:
        event: Lambda event payload.
        context: Lambda runtime context.

    Returns:
        Status and S3 upload metadata.
    """
    _configure_logging()

    # cold start / invocation log
    _LOG.info("Lambda invocation started")

    # normalize event
    event = event or {}

    # Determine base list URL
    # Priority: event value -> environment variable -> default value
    base_list_url = event.get("list_url") or os.environ.get("LIST_URL", DEFAULT_LIST_URL)

    # Optional max_pages parameter
    max_pages = event.get("max_pages") or os.environ.get("MAX_PAGES")
    if max_pages is not None:
        max_pages = int(max_pages)
    
    # Items per page
    count = int(event.get("count") or os.environ.get("COUNT", DEFAULT_COUNT))

    # Supplier code used by the API
    supplier_code = event.get("supplier_code") or os.environ.get(
        "SUPPLIER_CODE", DEFAULT_SUPPLIER_CODE
    )

    # Determine S3 bucket to upload
    bucket = event.get("bucket") or os.environ.get("URLS_BUCKET")
    if not bucket:
        raise ValueError("Missing S3 bucket. Provide event.bucket or URLS_BUCKET env.")
    
    # Build output S3 key using run_id to avoid overwrite
    key_prefix = event.get("key_prefix") or os.environ.get(
        "URLS_KEY_PREFIX", DEFAULT_KEY_PREFIX
    )
    run_id = event.get("run_id") or datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"{key_prefix.rstrip('/')}/{run_id}/urls.json"

    _LOG.info(
        "Start extraction: list_url=%s count=%s supplier_code=%s max_pages=%s run_id=%s",
        base_list_url,
        count,
        supplier_code,
        max_pages,
        run_id,
    )

    try:
        # Run extraction pipeline
        detailed_urls = extract_all_detail_urls(
            base_list_url=base_list_url,
            max_pages=max_pages,
            count=count,
            supplier_code=supplier_code,
            category_map=_load_category_map(),
        )

        # Upload result JSON to S3
        upload_json_to_s3(bucket=bucket, key=key, payload=detailed_urls)
    except Exception:
        _LOG.exception("Extraction failed")
        raise

    _LOG.info(
        "Upload complete: bucket=%s key=%s count=%s",
        bucket,
        key,
        len(detailed_urls),
    )

    # Return execution metadata
    return {
        "status": "ok",
        "count": len(detailed_urls),
        "s3_bucket": bucket,
        "urls_key": key,
        "run_id": run_id,
    }
