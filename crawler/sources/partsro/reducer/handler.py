#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Reduce Partsro CSV parts into a single CSV file."""

from __future__ import annotations

import logging  # Structured logging.
import os  # Environment variables.
from typing import Iterable, List  # Type hints.

import boto3  # AWS SDK for S3.


logger = logging.getLogger(__name__)  # Module-level logger.

# Default S3 prefix for CSV parts.
DEFAULT_RESULT_PREFIX = "raw/partsro/parts"
# Default S3 prefix for final CSV.
DEFAULT_FINAL_PREFIX = "raw/partsro/final"
# Default log level.
DEFAULT_LOG_LEVEL = "INFO"
# Minimum size for each multipart upload part (5 MB).
MIN_PART_SIZE = 5 * 1024 * 1024
# UTF-8 BOM for Excel compatibility.
UTF8_BOM = "\ufeff"
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
    """Configure logging for Lambda.

    Sets the global logging format and level based on LOG_LEVEL.
    """
    level_name = os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()  # Read level.
    level = getattr(logging, level_name, logging.INFO)  # Fallback to INFO.
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
    keys: List[str] = []  # Accumulate keys.
    paginator = s3.get_paginator("list_objects_v2")  # Paginate list.
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):  # Iterate pages.
        for obj in page.get("Contents", []):  # Iterate objects.
            key = obj.get("Key")  # Object key.
            if not key:
                continue  # Skip empty keys.
            if key.endswith("/"):
                continue  # Skip folder markers.
            if not key.lower().endswith(".csv"):
                continue  # Skip non-CSV files.
            keys.append(key)  # Collect key.
    keys.sort()  # Stable order.
    return keys  # Return sorted list.


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
    create_resp = s3.create_multipart_upload(  # Start multipart upload.
        Bucket=bucket,
        Key=key,
        ContentType=content_type,
    )
    upload_id = create_resp["UploadId"]  # Upload id.
    parts = []  # Uploaded part metadata.
    part_number = 1  # S3 multipart part number.
    buffer = bytearray()  # Accumulation buffer.

    def flush_buffer() -> None:
        """Upload current buffer as a multipart part."""
        nonlocal part_number, buffer, parts
        if not buffer:
            return  # Nothing to upload.
        resp = s3.upload_part(  # Upload part.
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=bytes(buffer),
        )
        parts.append({"ETag": resp["ETag"], "PartNumber": part_number})  # Track part.
        part_number += 1  # Increment part number.
        buffer = bytearray()  # Reset buffer.

    try:
        # Write header once.
        buffer.extend(header_line.encode("utf-8"))

        for source_key in source_keys:  # Stream each source file.
            obj = s3.get_object(Bucket=bucket, Key=source_key)  # Read part.
            body = obj["Body"]  # Streaming body.
            for chunk in body.iter_chunks(chunk_size=1024 * 1024):  # Read in 1MB chunks.
                if not chunk:
                    continue  # Skip empty chunks.
                buffer.extend(chunk)  # Append chunk.
                if len(buffer) >= MIN_PART_SIZE:
                    flush_buffer()  # Upload when buffer is big enough.

        flush_buffer()  # Upload remaining bytes.

        if not parts:
            raise RuntimeError("No parts uploaded; nothing to complete")

        s3.complete_multipart_upload(  # Finalize upload.
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        logger.exception("Multipart upload failed, aborting")  # Log failure.
        s3.abort_multipart_upload(  # Abort to avoid orphaned uploads.
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
        )
        raise  # Re-raise error.


def handler(event, context):
    """AWS Lambda handler to reduce CSV parts into a single file.

    Args:
        event: Lambda event payload.
        context: Lambda runtime context.

    Returns:
        Metadata about the merged CSV file.
    """
    _configure_logging()  # Initialize logging.
    event = event or {}  # Normalize event.

    # Required inputs.
    run_id = event.get("run_id")  # Unique run id.
    if not run_id:
        raise ValueError("Missing run_id in event")

    bucket = event.get("bucket") or os.environ.get("RESULT_BUCKET")  # Target bucket.
    if not bucket:
        raise ValueError("Missing S3 bucket. Provide event.bucket or RESULT_BUCKET env.")

    # Prefixes for parts and final output.
    result_prefix = event.get("result_prefix") or os.environ.get(
        "RESULT_PREFIX", DEFAULT_RESULT_PREFIX
    )
    final_prefix = event.get("final_prefix") or os.environ.get(
        "FINAL_PREFIX", DEFAULT_FINAL_PREFIX
    )

    results_prefix = f"{result_prefix.rstrip('/')}/{run_id}/"  # Part folder.
    final_key = f"{final_prefix.rstrip('/')}/{run_id}/final.csv"  # Final CSV key.

    # List part files.
    s3 = boto3.client("s3")  # S3 client.
    keys = _list_result_keys(s3, bucket, results_prefix)  # Part keys.
    if not keys:
        raise RuntimeError("No result CSV parts found for run_id")

    logger.info("Reduce start: bucket=%s prefix=%s parts=%d", bucket, results_prefix, len(keys))
    header_line = UTF8_BOM + ",".join(FIELDNAMES) + "\n"  # Header with BOM.

    # Merge parts into a single CSV via multipart upload.
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
