from __future__ import annotations

import pendulum


LOCAL_TZ = pendulum.timezone("UTC")


def optional_int(value):
    if value in (None, "", "null", "None"):
        return None
    return int(value)


def resolve_effective_ds(context):
    """Return the effective partition date based on data interval end."""
    data_end = context["data_interval_end"].in_timezone(LOCAL_TZ)
    effective_date = data_end.date()
    return effective_date.isoformat(), effective_date.strftime("%Y%m%d"), data_end


def format_ts(context) -> str:
    """Human-friendly timestamp for notifications."""
    ts = context.get("ts")
    if not ts:
        return "unknown"
    return pendulum.parse(ts).in_timezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def now_utc_str() -> str:
    return pendulum.now("UTC").strftime("%Y-%m-%d %H:%M:%S UTC")


def norm_s3_prefix(prefix: str) -> str:
    return prefix.rstrip("/") if prefix else prefix


def split_s3_uri(uri: str) -> tuple[str, str]:
    """Parse s3://bucket/prefix or bucket/prefix into (bucket, prefix)."""
    normalized = uri.replace("s3://", "", 1)
    if "/" not in normalized:
        return normalized, ""
    bucket, prefix = normalized.split("/", 1)
    return bucket, prefix
