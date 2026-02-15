#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json

import boto3


def upload_json_to_s3(bucket: str, key: str, payload: list[str]) -> None:
    """Upload JSON payload to S3.

    Args:
        bucket: S3 bucket name.
        key: S3 object key.
        payload: URL list to store.

    Raises:
        RuntimeError: If boto3 is not available.
    """
    s3 = boto3.client("s3")
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )
