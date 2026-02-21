from __future__ import annotations

import csv
import io
import re
from typing import List

from sqlalchemy import create_engine

from lib.common import split_s3_uri
from lib.s3_metrics import list_keys

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def normalize_sqlalchemy_uri(uri: str) -> str:
    """Normalize Airflow connection URI to an explicit SQLAlchemy driver URI."""
    if uri.startswith("postgres://"):
        return uri.replace("postgres://", "postgresql+psycopg2://", 1)
    if uri.startswith("postgresql://") and "postgresql+psycopg2://" not in uri:
        return uri.replace("postgresql://", "postgresql+psycopg2://", 1)
    return uri


def load_summary_rows_from_s3(s3, summary_prefix: str, effective_ds: str) -> List[dict]:
    bucket, prefix = split_s3_uri(summary_prefix)
    rows = []
    for key in list_keys(s3, bucket, prefix):
        if not key.lower().endswith(".csv"):
            continue
        obj = s3.get_object(Bucket=bucket, Key=key)["Body"]
        reader = csv.DictReader(io.TextIOWrapper(obj, encoding="utf-8"))
        for row in reader:
            rows.append(
                {
                    "part_official_name": row.get("part_official_name") or row.get("name") or "",
                    "extracted_at": row.get("extracted_at") or effective_ds,
                    "min_price": int(row["min_price"]) if row.get("min_price") else None,
                    "max_price": int(row["max_price"]) if row.get("max_price") else None,
                    "car_type": row.get("car_type") or "",
                }
            )
    return rows


def _normalize_table_identifier(table: str) -> str:
    if table is None:
        raise ValueError("Invalid table name: None")

    normalized = table.strip()
    if not normalized:
        raise ValueError("Invalid table name: empty")

    parts = [part.strip() for part in normalized.split(".")]
    if len(parts) not in (1, 2):
        raise ValueError(f"Invalid table name: {table}")

    cleaned_parts = []
    for part in parts:
        # Allow optional quoting in Variable values, e.g. "test"."parts_master".
        if len(part) >= 2 and part.startswith('"') and part.endswith('"'):
            part = part[1:-1]
        if not _IDENTIFIER_RE.match(part):
            raise ValueError(f"Invalid table identifier: {table}")
        cleaned_parts.append(part)

    if len(cleaned_parts) == 2:
        return f'"{cleaned_parts[0]}"."{cleaned_parts[1]}"'
    return f'"{cleaned_parts[0]}"'


def _copy_rows_to_stage(cursor, stage_table: str, rows: List[dict]) -> None:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    for row in rows:
        writer.writerow(
            [
                row["part_official_name"],
                row["extracted_at"],
                row["min_price"],
                row["max_price"],
                row["car_type"],
            ]
        )
    buf.seek(0)
    cursor.copy_expert(
        (
            f"COPY {stage_table} "
            "(part_official_name, extracted_at, min_price, max_price, car_type) "
            "FROM STDIN WITH (FORMAT csv)"
        ),
        buf,
    )


def _dedupe_rows(rows: List[dict]) -> List[dict]:
    latest_by_key = {}
    for row in rows:
        key = (row["part_official_name"], row["car_type"], row["extracted_at"])
        latest_by_key[key] = row
    return list(latest_by_key.values())


def upsert_rows_to_rds(engine, table: str, effective_ds: str, rows: List[dict]) -> None:
    target_table = _normalize_table_identifier(table)
    deduped_rows = _dedupe_rows(rows)

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            cursor.execute(
                f"""
                DELETE FROM {target_table}
                WHERE extracted_at < (CAST(%s AS DATE) - INTERVAL '1 month')
                """,
                (effective_ds,),
            )

            cursor.execute(
                """
                CREATE TEMP TABLE _parts_master_stage (
                    part_official_name TEXT NOT NULL,
                    extracted_at DATE NOT NULL,
                    min_price INT,
                    max_price INT,
                    car_type TEXT NOT NULL
                ) ON COMMIT DROP
                """
            )
            _copy_rows_to_stage(cursor, "_parts_master_stage", deduped_rows)
            cursor.execute(
                f"""
                INSERT INTO {target_table}
                (part_official_name, extracted_at, min_price, max_price, car_type)
                SELECT
                    part_official_name,
                    extracted_at,
                    min_price,
                    max_price,
                    car_type
                FROM _parts_master_stage
                ON CONFLICT (part_official_name, car_type, extracted_at)
                DO UPDATE
                SET
                    min_price = EXCLUDED.min_price,
                    max_price = EXCLUDED.max_price
                """
            )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


def build_engine_from_airflow_uri(uri: str):
    return create_engine(normalize_sqlalchemy_uri(uri))
