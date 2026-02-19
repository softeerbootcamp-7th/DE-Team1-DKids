from __future__ import annotations

import json
from typing import Dict, Iterable


def has_data_rows(s3, bucket: str, key: str, max_bytes: int = 1024 * 1024) -> bool:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read(max_bytes)
    return len(body.splitlines()) > 1


def list_keys(s3, bucket: str, prefix: str) -> Iterable[str]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not key or key.endswith("/"):
                continue
            yield key


def count_csv_rows(s3, bucket: str, prefix: str) -> int:
    total = 0
    for key in list_keys(s3, bucket, prefix):
        if not key.lower().endswith(".csv"):
            continue
        body = s3.get_object(Bucket=bucket, Key=key)["Body"]
        for _ in body.iter_lines():
            total += 1
    return total


def count_skipped_with_reasons(s3, bucket: str, prefix: str) -> tuple[int, Dict[str, int]]:
    total = 0
    reasons: Dict[str, int] = {}
    for key in list_keys(s3, bucket, prefix):
        if not key.lower().endswith(".json"):
            continue
        payload = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if not payload:
            continue
        data = json.loads(payload)
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = [data]
        else:
            items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            total += 1
            reason = item.get("reason") or "unknown"
            reasons[reason] = reasons.get(reason, 0) + 1
    return total, reasons
