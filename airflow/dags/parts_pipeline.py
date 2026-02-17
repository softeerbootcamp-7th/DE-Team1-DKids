import json
from datetime import timedelta
from typing import Dict, Iterable

import boto3
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.step_function import (
    StepFunctionStartExecutionOperator,
)
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

LOCAL_TZ = pendulum.timezone("UTC")


def _optional_int(value):
    if value in (None, "", "null", "None"):
        return None
    return int(value)


def _resolve_effective_ds(context):
    data_end = context["data_interval_end"].in_timezone(LOCAL_TZ)
    effective_date = data_end.date()
    return effective_date.isoformat(), effective_date.strftime("%Y%m%d"), data_end


def _now_utc_str() -> str:
    return pendulum.now("UTC").strftime("%Y-%m-%d %H:%M:%S UTC")


def _list_keys(s3, bucket: str, prefix: str) -> Iterable[str]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not key or key.endswith("/"):
                continue
            yield key


def _count_csv_rows(s3, bucket: str, prefix: str) -> int:
    total = 0
    for key in _list_keys(s3, bucket, prefix):
        if not key.lower().endswith(".csv"):
            continue
        body = s3.get_object(Bucket=bucket, Key=key)["Body"]
        for _ in body.iter_lines():
            total += 1
    return total


def _count_skipped_with_reasons(s3, bucket: str, prefix: str):
    total = 0
    reasons = {}
    for key in _list_keys(s3, bucket, prefix):
        if not key.lower().endswith(".json"):
            continue
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        for line in body.splitlines():
            if not line.strip():
                continue
            total += 1
            reason = "unknown"
            reasons[reason] = reasons.get(reason, 0) + 1
    return total, reasons


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="parts_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["crawler"],
) as dag:

    @task
    def build_master_input() -> Dict:
        context = get_current_context()
        effective_ds, effective_ds_nodash, _ = _resolve_effective_ds(context)
        ts_nodash = context["ts_nodash"]

        data_bucket = Variable.get("DATA_BUCKET")
        supplier_code = Variable.get("SUPPLIER_CODE", default_var="S0000000")
        count = int(Variable.get("CRAWL_COUNT", default_var="500"))
        max_pages = _optional_int(Variable.get("MAX_PAGES", default_var=None))

        run_id = f"{effective_ds_nodash}_{ts_nodash}"

        def source_input(source: str, list_url_var: str, category_urls_var: str):
            list_url = Variable.get(list_url_var, default_var=None)
            raw_category_urls = Variable.get(category_urls_var, default_var=None)
            category_urls = None
            if raw_category_urls:
                try:
                    category_urls = json.loads(raw_category_urls)
                except Exception:
                    category_urls = None

            base_prefix = f"raw/{source}"
            return {
                "bucket": data_bucket,
                "urls_prefix": f"{base_prefix}/urls/dt={effective_ds}",
                "result_prefix": f"{base_prefix}/parts/dt={effective_ds}",
                "skip_prefix": f"{base_prefix}/skipped/dt={effective_ds}",
                "final_prefix": f"{base_prefix}/final/dt={effective_ds}",
                "run_id": run_id,
                "list_url": list_url,
                "category_urls": category_urls,
                "max_pages": max_pages,
                "count": count,
                "supplier_code": supplier_code,
            }

        return {
            "partsro": source_input("partsro", "PARTSRO_LIST_URL", "PARTSRO_CATEGORY_URLS"),
            "hyunki_store": source_input(
                "hyunki_store", "HYUNKI_STORE_LIST_URL", "HYUNKI_STORE_CATEGORY_URLS"
            ),
            "hyunki_market": source_input(
                "hyunki_market", "HYUNKI_MARKET_LIST_URL", "HYUNKI_MARKET_CATEGORY_URLS"
            ),
        }

    @task
    def summarize_crawl() -> str:
        context = get_current_context()
        effective_ds, effective_ds_nodash, data_end = _resolve_effective_ds(context)
        ts_nodash = context["ts_nodash"]
        run_id = f"{effective_ds_nodash}_{ts_nodash}"

        data_bucket = Variable.get("DATA_BUCKET")
        s3 = boto3.client("s3")

        sent_at = _now_utc_str()
        lines = [f"Crawl summary (ds={effective_ds}, sent_at={sent_at})"]
        for source in ["partsro", "hyunki_store", "hyunki_market"]:
            parts_prefix = f"raw/{source}/parts/dt={effective_ds}/{run_id}/"
            skipped_prefix = f"raw/{source}/skipped/dt={effective_ds}/{run_id}/"
            success = _count_csv_rows(s3, data_bucket, parts_prefix)
            failed, reasons = _count_skipped_with_reasons(s3, data_bucket, skipped_prefix)
            total = success + failed
            line = f"- {source}: total={total:,} success={success:,} failed={failed:,}"
            if reasons:
                reason_text = ", ".join(f"{k}={v}" for k, v in sorted(reasons.items()))
                line = f"{line} ({reason_text})"
            lines.append(line)

        return "\n".join(lines)

    master_input = build_master_input()

    run_master = StepFunctionStartExecutionOperator(
        task_id="run_master_crawler",
        state_machine_arn=Variable.get("MASTER_SFN_ARN"),
        state_machine_input=master_input,
        name="parts-{{ ts_nodash }}",
    )

    wait_master = StepFunctionExecutionSensor(
        task_id="wait_master_crawler",
        execution_arn="{{ ti.xcom_pull(task_ids='run_master_crawler') }}",
    )

    crawl_summary = summarize_crawl()

    notify_crawl = SlackWebhookOperator(
        task_id="notify_crawl_summary",
        slack_webhook_conn_id=Variable.get(
            "SLACK_WEBHOOK_CONN_ID", default_var="slack_webhook_default"
        ),
        message="{{ ti.xcom_pull(task_ids='summarize_crawl') }}",
    )

    run_master >> wait_master >> crawl_summary >> notify_crawl
