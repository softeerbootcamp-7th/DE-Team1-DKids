from __future__ import annotations

import csv
import io
from typing import List

from sqlalchemy import create_engine, text

from lib.common import split_s3_uri
from lib.s3_metrics import list_keys


def normalize_mysql_uri(uri: str) -> str:
    if uri.startswith("mysql://") and "mysql+pymysql://" not in uri:
        return uri.replace("mysql://", "mysql+pymysql://", 1)
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


def upsert_rows_to_rds(engine, table: str, effective_ds: str, rows: List[dict]) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table} WHERE extracted_at < DATE_SUB(:dt, INTERVAL 30 DAY)"),
            {"dt": effective_ds},
        )
        conn.execute(
            text(
                f"""
                INSERT INTO {table}
                (part_official_name, extracted_at, min_price, max_price, car_type)
                VALUES (:part_official_name, :extracted_at, :min_price, :max_price, :car_type)
                """
            ),
            rows,
        )


def build_engine_from_airflow_uri(uri: str):
    return create_engine(normalize_mysql_uri(uri))
