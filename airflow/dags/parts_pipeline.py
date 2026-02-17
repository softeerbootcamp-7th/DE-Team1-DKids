import json
import time
from datetime import timedelta
from typing import Dict, Iterable

import boto3
import pendulum
from boto3.dynamodb.conditions import Attr, Key
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.step_function import (
    StepFunctionStartExecutionOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
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


def _norm_s3_prefix(prefix: str) -> str:
    return prefix.rstrip("/") if prefix else prefix


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


def _write_retry_manifest(s3, bucket: str, key: str, urls: list[str]) -> None:
    body = json.dumps(urls, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )


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
    tags=["crawler", "emr"],
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

    @task
    def build_retry_inputs() -> Dict[str, Dict]:
        context = get_current_context()
        effective_ds, effective_ds_nodash, data_end = _resolve_effective_ds(context)
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

        for source in ["partsro", "hyunki_store", "hyunki_market"]:
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
    def build_emr_steps() -> list:
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
                        f"{clean_prefix}/partsro/dt={effective_ds}/,"
                        f"{clean_prefix}/hyunki_store/dt={effective_ds}/,"
                        f"{clean_prefix}/hyunki_market/dt={effective_ds}/",
                        "--output",
                        f"{clean_prefix}/normalized/dt={effective_ds}/",
                        "--format",
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

    run_master >> wait_master >> crawl_summary
    crawl_summary >> notify_crawl
    crawl_summary >> retry_inputs >> retry_runs >> notify_retry
    retry_runs >> add_emr_steps >> wait_emr
