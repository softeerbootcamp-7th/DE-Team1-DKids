from __future__ import annotations

import json
from typing import Dict


def parse_category_urls(raw_category_urls: str | None):
    if not raw_category_urls:
        return None
    try:
        parsed = json.loads(raw_category_urls)
    except Exception:
        return None
    return parsed if isinstance(parsed, list) else None


def build_source_input(
    *,
    source: str,
    data_bucket: str,
    effective_ds: str,
    run_id: str,
    list_url: str | None,
    raw_category_urls: str | None,
    max_pages,
    count: int,
    supplier_code: str,
):
    base_prefix = f"raw/{source}"
    return {
        "bucket": data_bucket,
        "urls_prefix": f"{base_prefix}/urls/dt={effective_ds}",
        "result_prefix": f"{base_prefix}/parts/dt={effective_ds}",
        "skip_prefix": f"{base_prefix}/skipped/dt={effective_ds}",
        "final_prefix": f"{base_prefix}/final/dt={effective_ds}",
        "run_id": run_id,
        "list_url": list_url,
        "category_urls": parse_category_urls(raw_category_urls),
        "max_pages": max_pages,
        "count": count,
        "supplier_code": supplier_code,
    }


def build_retry_input(
    *,
    source: str,
    data_bucket: str,
    effective_ds: str,
    retry_run_id: str,
    urls_key: str,
    max_pages,
    count: int,
    supplier_code: str,
    retry_attempt: int,
) -> Dict:
    base_prefix = f"raw/{source}"
    return {
        "bucket": data_bucket,
        "urls_prefix": f"{base_prefix}/urls/dt={effective_ds}",
        "result_prefix": f"{base_prefix}/parts/dt={effective_ds}",
        "skip_prefix": f"{base_prefix}/skipped/dt={effective_ds}",
        "final_prefix": f"{base_prefix}/final/dt={effective_ds}",
        "run_id": retry_run_id,
        "list_url": None,
        "category_urls": None,
        "max_pages": max_pages,
        "count": count,
        "supplier_code": supplier_code,
        "override_urls_key": urls_key,
        "attempt": retry_attempt,
    }


def write_retry_manifest(s3, bucket: str, key: str, urls: list[str]) -> None:
    body = json.dumps(urls, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )
