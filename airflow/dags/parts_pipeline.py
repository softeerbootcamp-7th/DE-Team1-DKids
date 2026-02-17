from __future__ import annotations

import csv
import io
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Iterable

import boto3
import pendulum
from boto3.dynamodb.conditions import Attr, Key
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine, text

SOURCES = ["partsro", "hyunki_store", "hyunki_market"]
LOCAL_TZ = pendulum.timezone("UTC")


def _optional_int(value):
    if value in (None, "", "null", "None"):
        return None
    return int(value)


def _resolve_effective_ds(context):
    """Return the effective partition date based on the data interval end (UTC)."""
    data_end = context["data_interval_end"].in_timezone(LOCAL_TZ)
    effective_date = data_end.date()
    return effective_date.isoformat(), effective_date.strftime("%Y%m%d"), data_end


def _format_ts(context) -> str:
    """Human-friendly timestamp for notifications (UTC, to seconds)."""
    ts = context.get("ts")
    if not ts:
        return "unknown"
    return pendulum.parse(ts).in_timezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def _now_utc_str() -> str:
    """Return current UTC time string used in Slack messages."""
    return pendulum.now("UTC").strftime("%Y-%m-%d %H:%M:%S UTC")


def _norm_s3_prefix(prefix: str) -> str:
    """Normalize S3 prefixes to prevent double slashes."""
    return prefix.rstrip("/") if prefix else prefix


def _has_data_rows(s3, bucket: str, key: str, max_bytes: int = 1024 * 1024) -> bool:
    # Avoid downloading large objects; just check if there's at least one data row.
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read(max_bytes)
    lines = body.splitlines()
    return len(lines) > 1


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


def _query_failed_urls(table_name: str, source: str, ds: str, max_attempts: int) -> list[str]:
    ddb = boto3.resource("dynamodb").Table(table_name)
    pk = f"{source}#dt={ds}"
    filter_expr = Attr("status").eq("FAILED") & (
        Attr("attempt").lt(max_attempts) | Attr("attempt").not_exists()
    )
    items = []
    resp = ddb.query(KeyConditionExpression=Key("pk").eq(pk), FilterExpression=filter_expr)
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = ddb.query(
            KeyConditionExpression=Key("pk").eq(pk),
            FilterExpression=filter_expr,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))
    urls = []
    for item in items:
        url = item.get("url")
        if isinstance(url, str) and url:
            urls.append(url)
    return urls


def _write_retry_manifest(
    s3,
    bucket: str,
    key: str,
    urls: list[str],
) -> None:
    body = json.dumps(urls, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )


def _count_skipped_with_reasons(s3, bucket: str, prefix: str) -> tuple[int, Dict[str, int]]:
    total = 0
    reasons: Dict[str, int] = {}
    for key in _list_keys(s3, bucket, prefix):
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


def _slack_fail_callback(context):
    conn_id = Variable.get("SLACK_WEBHOOK_CONN_ID", default_var="slack_webhook_default")
    hook = SlackWebhookHook(slack_webhook_conn_id=conn_id)
    task_instance = context.get("task_instance")
    ts = _format_ts(context)
    msg = (
        f"Airflow failure ({ts}): dag={context.get('dag').dag_id} "
        f"task={task_instance.task_id if task_instance else 'unknown'} "
        f"run_id={context.get('run_id')} "
        f"log={task_instance.log_url if task_instance else 'n/a'}"
    )
    hook.send(text=msg)


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _slack_fail_callback,
}


with DAG(
    dag_id="parts_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["crawler", "emr", "rds"],
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
            "partsro": source_input(
                "partsro",
                list_url_var="PARTSRO_LIST_URL",
                category_urls_var="PARTSRO_CATEGORY_URLS_JSON",
            ),
            "hyunki_store": source_input(
                "hyunki_store",
                list_url_var="HYUNKI_STORE_LIST_URL",
                category_urls_var="HYUNKI_STORE_CATEGORY_URLS_JSON",
            ),
            "hyunki_market": source_input(
                "hyunki_market",
                list_url_var="HYUNKI_MARKET_LIST_URL",
                category_urls_var="HYUNKI_MARKET_CATEGORY_URLS_JSON",
            ),
        }

    @task
    def build_emr_steps() -> list:
        """Build EMR steps for preprocessing and summary generation."""
        context = get_current_context()
        effective_ds, _, _ = _resolve_effective_ds(context)

        code_prefix = _norm_s3_prefix(Variable.get("CODE_S3_PREFIX"))
        raw_prefix = _norm_s3_prefix(Variable.get("RAW_S3_PREFIX"))
        clean_prefix = _norm_s3_prefix(Variable.get("CLEAN_S3_PREFIX"))
        mart_prefix = _norm_s3_prefix(Variable.get("MART_S3_PREFIX"))

        return [
            {
                "Name": "partsro_clean",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        f"{code_prefix}/preprocess_partsro.py",
                        "--input",
                        f"{raw_prefix}/partsro/final/dt={effective_ds}/",
                        "--output",
                        f"{clean_prefix}/partsro/dt={effective_ds}/",
                        "--dt",
                        effective_ds,
                    ],
                },
            },
            {
                "Name": "hyunki_store_clean",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        f"{code_prefix}/preprocess_hyunki_store.py",
                        "--input",
                        f"{raw_prefix}/hyunki_store/final/dt={effective_ds}/",
                        "--output",
                        f"{clean_prefix}/hyunki_store/dt={effective_ds}/",
                        "--dt",
                        effective_ds,
                    ],
                },
            },
            {
                "Name": "hyunki_market_clean",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        f"{code_prefix}/preprocess_hyunki_market.py",
                        "--input",
                        f"{raw_prefix}/hyunki_market/final/dt={effective_ds}/",
                        "--output",
                        f"{clean_prefix}/hyunki_market/dt={effective_ds}/",
                        "--dt",
                        effective_ds,
                    ],
                },
            },
            {
                "Name": "normalize_car_type",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        f"{code_prefix}/normalize_car_type.py",
                        "--input",
                        (
                            f"{clean_prefix}/partsro/dt={effective_ds}/,"
                            f"{clean_prefix}/hyunki_store/dt={effective_ds}/,"
                            f"{clean_prefix}/hyunki_market/dt={effective_ds}/"
                        ),
                        "--format",
                        "parquet",
                        "--output",
                        f"{clean_prefix}/normalized/dt={effective_ds}/",
                        "--dt",
                        effective_ds,
                        "--output-format",
                        "parquet",
                    ],
                },
            },
            {
                "Name": "name_price_summary",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        f"{code_prefix}/build_name_price_summary.py",
                        "--input",
                        f"{clean_prefix}/normalized/dt={effective_ds}/",
                        "--format",
                        "parquet",
                        "--output",
                        f"{mart_prefix}/name_price_summary/dt={effective_ds}/",
                        "--dt",
                        effective_ds,
                        "--output-format",
                        "csv",
                        "--canonical-output",
                        f"{mart_prefix}/part_no_canonical_name/dt={effective_ds}/",
                    ],
                },
            },
        ]

    @task
    def summarize_crawl() -> str:
        """Summarize crawl successes and failures per source."""
        context = get_current_context()
        effective_ds, effective_ds_nodash, data_end = _resolve_effective_ds(context)
        ts = context["ts"]
        ts_nodash = context["ts_nodash"]
        run_id = f"{effective_ds_nodash}_{ts_nodash}"

        data_bucket = Variable.get("DATA_BUCKET")
        s3 = boto3.client("s3")

        sent_at = _now_utc_str()
        lines = [f"Crawl summary (ds={effective_ds}, sent_at={sent_at})"]
        for source in SOURCES:
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

    @task
    def build_retry_inputs() -> Dict[str, Dict]:
        """Build retry payloads from DynamoDB failure records."""
        context = get_current_context()
        effective_ds, effective_ds_nodash, data_end = _resolve_effective_ds(context)
        ts = context["ts"]
        ts_nodash = context["ts_nodash"]
        run_id = f"{effective_ds_nodash}_{ts_nodash}"

        table_name = Variable.get("DDB_TABLE", default_var="")
        if not table_name:
            sent_at = _now_utc_str()
            return {
                "_skip_reason": f"Retry skipped (ds={effective_ds}, sent_at={sent_at}): DDB disabled."
            }

        data_bucket = Variable.get("DATA_BUCKET")
        supplier_code = Variable.get("SUPPLIER_CODE", default_var="S0000000")
        count = int(Variable.get("CRAWL_COUNT", default_var="500"))
        max_pages = _optional_int(Variable.get("MAX_PAGES", default_var=None))
        max_attempts = int(Variable.get("DDB_RETRY_MAX_ATTEMPTS", default_var="2"))
        retry_attempt = int(Variable.get("DDB_RETRY_ATTEMPT", default_var="2"))

        s3 = boto3.client("s3")
        retry_inputs: Dict[str, Dict] = {}

        def make_input(source: str, urls_key: str, retry_run_id: str) -> Dict:
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

        for source in SOURCES:
            urls = _query_failed_urls(table_name, source, effective_ds, max_attempts)
            if not urls:
                continue
            retry_run_id = f"{run_id}-retry{retry_attempt}"
            key = f"raw/{source}/retry/dt={effective_ds}/run_id={retry_run_id}/urls.json"
            _write_retry_manifest(s3, data_bucket, key, urls)
            retry_inputs[source] = make_input(source, key, retry_run_id)

        if not retry_inputs:
            sent_at = _now_utc_str()
            return {
                "_skip_reason": f"Retry skipped (ds={effective_ds}, sent_at={sent_at}): no failed URLs."
            }

        return retry_inputs

    @task
    def run_retry_executions(retry_inputs: Dict[str, Dict]) -> str:
        """Run retry executions and return a summary string."""
        skip_reason = retry_inputs.pop("_skip_reason", None)
        if skip_reason:
            return skip_reason

        sfn = boto3.client("stepfunctions")
        arn_map = {
            "partsro": Variable.get("PARTSRO_SFN_ARN"),
            "hyunki_store": Variable.get("HYUNKI_STORE_SFN_ARN"),
            "hyunki_market": Variable.get("HYUNKI_MARKET_SFN_ARN"),
        }

        lines = ["Retry summary"]
        for source, payload in retry_inputs.items():
            if not payload:
                continue
            arn = arn_map.get(source)
            if not arn:
                raise ValueError(f"Missing state machine ARN for {source}")
            name = f"retry-{source}-{payload['run_id']}"[:80]
            resp = sfn.start_execution(
                stateMachineArn=arn,
                name=name,
                input=json.dumps(payload, ensure_ascii=False),
            )
            execution_arn = resp["executionArn"]
            while True:
                desc = sfn.describe_execution(executionArn=execution_arn)
                status = desc["status"]
                if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
                    break
                time.sleep(15)
            if status != "SUCCEEDED":
                raise RuntimeError(f"Retry failed for {source}: {status}")
            lines.append(f"- {source}: status={status}")

        return "\n".join(lines)

    @task
    def load_to_rds() -> None:
        """Load the summary CSV from S3 into MySQL."""
        context = get_current_context()
        effective_ds, _, _ = _resolve_effective_ds(context)

        data_bucket = Variable.get("DATA_BUCKET")
        mart_prefix = _norm_s3_prefix(Variable.get("MART_S3_PREFIX"))
        table = Variable.get("RDS_TABLE", default_var="parts_master")
        conn_id = Variable.get("RDS_CONN_ID", default_var="rds_default")

        s3 = boto3.client("s3")
        summary_prefix = f"{mart_prefix.replace('s3://', '')}/name_price_summary/dt={effective_ds}/"

        conn = BaseHook.get_connection(conn_id)
        uri = conn.get_uri()
        # SQLAlchemy expects an explicit driver name for MySQL connections.
        if uri.startswith("mysql://") and "mysql+pymysql://" not in uri:
            uri = uri.replace("mysql://", "mysql+pymysql://", 1)
        engine = create_engine(uri)

        rows = []
        bucket = summary_prefix.split("/", 1)[0]
        prefix = summary_prefix.split("/", 1)[1]

        for key in _list_keys(s3, bucket, prefix):
            if not key.lower().endswith(".csv"):
                continue
            obj = s3.get_object(Bucket=bucket, Key=key)["Body"]
            reader = csv.DictReader(io.TextIOWrapper(obj, encoding="utf-8"))
            for row in reader:
                rows.append(
                    {
                        "part_official_name": row.get("part_official_name")
                        or row.get("name")
                        or "",
                        "extracted_at": row.get("extracted_at") or effective_ds,
                        "min_price": int(row["min_price"]) if row.get("min_price") else None,
                        "max_price": int(row["max_price"]) if row.get("max_price") else None,
                        "car_type": row.get("car_type") or "",
                    }
                )

        print(
            f"load_to_rds: read {len(rows)} rows from s3://{bucket}/{prefix} for dt={effective_ds}"
        )
        if not rows:
            print("load_to_rds: no rows found, skipping insert.")
            return

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

    @task
    def cleanup_empty_mart_outputs() -> str:
        """Remove empty output CSVs to avoid false positives."""
        context = get_current_context()
        effective_ds, _, data_end = _resolve_effective_ds(context)
        sent_at = _now_utc_str()

        data_bucket = Variable.get("DATA_BUCKET")
        mart_prefix = _norm_s3_prefix(Variable.get("MART_S3_PREFIX"))
        s3 = boto3.client("s3")

        summary_prefix = f"{mart_prefix.replace('s3://', '')}/name_price_summary/dt={effective_ds}/"
        bucket = summary_prefix.split("/", 1)[0]
        prefix = summary_prefix.split("/", 1)[1]

        deleted = []
        has_data = False
        for key in _list_keys(s3, bucket, prefix):
            if not key.lower().endswith(".csv"):
                continue
            if _has_data_rows(s3, bucket, key):
                has_data = True
            else:
                s3.delete_object(Bucket=bucket, Key=key)
                deleted.append(key)

        # If there is no data CSV, remove _SUCCESS to avoid confusion.
        if not has_data:
            success_key = f"{prefix.rstrip('/')}/_SUCCESS"
            s3.delete_object(Bucket=bucket, Key=success_key)

        if not deleted and has_data:
            return f"Empty output cleanup skipped (ds={effective_ds}, sent_at={sent_at}): data present."

        return (
            f"Empty output cleanup (ds={effective_ds}, sent_at={sent_at}): "
            f"deleted={len(deleted)} has_data={has_data}"
        )

    master_input = build_master_input()

    run_master = StepFunctionStartExecutionOperator(
        task_id="run_master_crawler",
        state_machine_arn=Variable.get("MASTER_SFN_ARN"),
        state_machine_input=master_input,
        # Ensure unique execution names to avoid ExecutionAlreadyExists.
        name="parts-{{ ts_nodash }}-t{{ ti.try_number }}",
    )

    wait_master = StepFunctionExecutionSensor(
        task_id="wait_master_crawler",
        execution_arn="{{ ti.xcom_pull(task_ids='run_master_crawler') }}",
    )

    crawl_summary = summarize_crawl()
    retry_inputs = build_retry_inputs()
    retry_runs = run_retry_executions(retry_inputs)

    notify_crawl = SlackWebhookOperator(
        task_id="notify_crawl_summary",
        slack_webhook_conn_id=Variable.get(
            "SLACK_WEBHOOK_CONN_ID", default_var="slack_webhook_default"
        ),
        message="{{ ti.xcom_pull(task_ids='summarize_crawl') }}",
    )

    notify_retry = SlackWebhookOperator(
        task_id="notify_retry_summary",
        slack_webhook_conn_id=Variable.get(
            "SLACK_WEBHOOK_CONN_ID", default_var="slack_webhook_default"
        ),
        message="{{ ti.xcom_pull(task_ids='run_retry_executions') }}",
    )

    emr_steps = build_emr_steps()

    add_emr_steps = EmrAddStepsOperator(
        task_id="add_emr_steps",
        job_flow_id=Variable.get("EMR_CLUSTER_ID"),
        steps=emr_steps,
    )

    wait_emr = EmrStepSensor(
        task_id="wait_emr_summary",
        job_flow_id=Variable.get("EMR_CLUSTER_ID"),
        step_id="{{ ti.xcom_pull(task_ids='add_emr_steps') | last }}",
    )

    notify_transform = SlackWebhookOperator(
        task_id="notify_transform_success",
        slack_webhook_conn_id=Variable.get(
            "SLACK_WEBHOOK_CONN_ID", default_var="slack_webhook_default"
        ),
        message="Transform done ({{ ts }}).",
    )

    notify_all_done = SlackWebhookOperator(
        task_id="notify_pipeline_done",
        slack_webhook_conn_id=Variable.get(
            "SLACK_WEBHOOK_CONN_ID", default_var="slack_webhook_default"
        ),
        message="Pipeline done ({{ ts }}).",
    )

    load_rds = load_to_rds()
    cleanup_mart = cleanup_empty_mart_outputs()

    run_master >> wait_master >> crawl_summary
    crawl_summary >> notify_crawl
    crawl_summary >> retry_inputs >> retry_runs
    retry_runs >> notify_retry
    retry_runs >> add_emr_steps >> wait_emr >> cleanup_mart >> notify_transform >> load_rds >> notify_all_done

    notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id=Variable.get(
            "SLACK_WEBHOOK_CONN_ID", default_var="slack_webhook_default"
        ),
        message="Pipeline failed ({{ ts }}). Check task logs in Airflow.",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    [run_master, wait_master, add_emr_steps, wait_emr, load_rds, retry_runs] >> notify_failure
