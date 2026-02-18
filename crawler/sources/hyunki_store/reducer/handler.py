#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Reduce Hyunki Store CSV parts into a single CSV file."""

from __future__ import annotations

import logging
import os
from typing import Iterable, List

import boto3


logger = logging.getLogger(__name__)

DEFAULT_RESULT_PREFIX = "raw/hyunki_store/parts"
DEFAULT_FINAL_PREFIX = "raw/hyunki_store/final"
DEFAULT_LOG_LEVEL = "INFO"
MIN_PART_SIZE = 5 * 1024 * 1024
UTF8_BOM = "\ufeff"
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


def _list_result_keys(s3, bucket: str, prefix: str) -> List[str]:
    """List CSV part keys under the given prefix.

    Args:
        s3: Boto3 S3 client.
        bucket: Bucket name.
        prefix: Prefix for part files.

    Returns:
        Sorted list of S3 keys for part files.
    """
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not key or key.endswith("/"):
                continue
            if not key.lower().endswith(".csv"):
                continue
            keys.append(key)
    keys.sort()
    return keys


def _upload_parts(
    *,
    s3,
    bucket: str,
    key: str,
    content_type: str,
    source_keys: Iterable[str],
    header_line: str,
) -> None:
    """Stream parts into a single object via multipart upload.

    Args:
        s3: Boto3 S3 client.
        bucket: Bucket name.
        key: Output object key.
        content_type: Content type for output.
        source_keys: Source CSV part keys.
        header_line: CSV header line (written once).
    """
    create_resp = s3.create_multipart_upload(
        Bucket=bucket,
        Key=key,
        ContentType=content_type,
    )
    upload_id = create_resp["UploadId"]
    parts = []
    part_number = 1
    buffer = bytearray()

    def flush_buffer() -> None:
        nonlocal part_number, buffer, parts
        if not buffer:
            return
        resp = s3.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=bytes(buffer),
        )
        parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
        part_number += 1
        buffer = bytearray()

    try:
        buffer.extend(header_line.encode("utf-8"))

        for source_key in source_keys:
            obj = s3.get_object(Bucket=bucket, Key=source_key)
            body = obj["Body"]
            for chunk in body.iter_chunks(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                buffer.extend(chunk)
                if len(buffer) >= MIN_PART_SIZE:
                    flush_buffer()

        flush_buffer()

        if not parts:
            raise RuntimeError("No parts uploaded; nothing to complete")

        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        logger.exception("Multipart upload failed, aborting")
        s3.abort_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
        )
        raise


def handler(event, context):
    """AWS Lambda handler to reduce CSV parts into a single file.

    Args:
        event: Lambda event payload.
        context: Lambda runtime context.

    Returns:
        Metadata about the merged CSV file.
    """
    _configure_logging()
    event = event or {}

    run_id = event.get("run_id")
    if not run_id:
        raise ValueError("Missing run_id in event")

    bucket = event.get("bucket") or os.environ.get("RESULT_BUCKET")
    if not bucket:
        raise ValueError("Missing S3 bucket. Provide event.bucket or RESULT_BUCKET env.")

    result_prefix = event.get("result_prefix") or os.environ.get(
        "RESULT_PREFIX", DEFAULT_RESULT_PREFIX
    )
    final_prefix = event.get("final_prefix") or os.environ.get(
        "FINAL_PREFIX", DEFAULT_FINAL_PREFIX
    )

    results_prefix = f"{result_prefix.rstrip('/')}/{run_id}/"
    final_key = f"{final_prefix.rstrip('/')}/{run_id}/final.csv"

    s3 = boto3.client("s3")
    keys = _list_result_keys(s3, bucket, results_prefix)
    if not keys:
        raise RuntimeError("No result CSV parts found for run_id")

    logger.info("Reduce start: bucket=%s prefix=%s parts=%d", bucket, results_prefix, len(keys))
    header_line = UTF8_BOM + ",".join(FIELDNAMES) + "\n"

    _upload_parts(
        s3=s3,
        bucket=bucket,
        key=final_key,
        content_type="text/csv; charset=utf-8",
        source_keys=keys,
        header_line=header_line,
    )

    logger.info("Reduce complete: bucket=%s key=%s", bucket, final_key)
    return {
        "status": "ok",
        "run_id": run_id,
        "s3_bucket": bucket,
        "s3_key": final_key,
        "parts": len(keys),
    }
