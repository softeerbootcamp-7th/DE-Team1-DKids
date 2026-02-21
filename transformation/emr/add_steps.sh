#!/usr/bin/env bash
set -euo pipefail

: "${REGION:?Need REGION}"
: "${CLUSTER_ID:?Need CLUSTER_ID}"
: "${DT:?Need DT (YYYY-MM-DD)}"
: "${RUN_ID:?Need RUN_ID (e.g. 20260219_114712)}"
: "${CODE_S3_PREFIX:?Need CODE_S3_PREFIX}"
: "${RAW_S3_PREFIX:?Need RAW_S3_PREFIX}"
: "${CLEAN_S3_PREFIX:?Need CLEAN_S3_PREFIX}"
: "${MART_S3_PREFIX:?Need MART_S3_PREFIX}"

aws emr add-steps \
  --region "$REGION" \
  --cluster-id "$CLUSTER_ID" \
  --steps "[
    {
      \"Name\":\"partsro_clean\",
      \"Type\":\"CUSTOM_JAR\",
      \"ActionOnFailure\":\"CONTINUE\",
      \"Jar\":\"command-runner.jar\",
      \"Args\":[\"spark-submit\",\"${CODE_S3_PREFIX}/preprocess_partsro.py\",
              \"--input\",\"${RAW_S3_PREFIX}/partsro/final/${RUN_ID}/final.csv\",
              \"--output\",\"${CLEAN_S3_PREFIX}/partsro/dt=${DT}/\",
              \"--dt\",\"${DT}\"]
    },
    {
      \"Name\":\"hyunki_store_clean\",
      \"Type\":\"CUSTOM_JAR\",
      \"ActionOnFailure\":\"CONTINUE\",
      \"Jar\":\"command-runner.jar\",
      \"Args\":[\"spark-submit\",\"${CODE_S3_PREFIX}/preprocess_hyunki_store.py\",
              \"--input\",\"${RAW_S3_PREFIX}/hyunki_store/final/${RUN_ID}/final.csv\",
              \"--output\",\"${CLEAN_S3_PREFIX}/hyunki_store/dt=${DT}/\",
              \"--dt\",\"${DT}\"]
    },
    {
      \"Name\":\"hyunki_market_clean\",
      \"Type\":\"CUSTOM_JAR\",
      \"ActionOnFailure\":\"CONTINUE\",
      \"Jar\":\"command-runner.jar\",
      \"Args\":[\"spark-submit\",\"${CODE_S3_PREFIX}/preprocess_hyunki_market.py\",
              \"--input\",\"${RAW_S3_PREFIX}/hyunki_market/final/${RUN_ID}/final.csv\",
              \"--output\",\"${CLEAN_S3_PREFIX}/hyunki_market/dt=${DT}/\",
              \"--dt\",\"${DT}\"]
    },
    {
      \"Name\":\"normalize_car_type\",
      \"Type\":\"CUSTOM_JAR\",
      \"ActionOnFailure\":\"CONTINUE\",
      \"Jar\":\"command-runner.jar\",
      \"Args\":[\"spark-submit\",\"${CODE_S3_PREFIX}/normalize_car_type.py\",
              \"--input\",
              \"${CLEAN_S3_PREFIX}/partsro/dt=${DT}/,${CLEAN_S3_PREFIX}/hyunki_store/dt=${DT}/,${CLEAN_S3_PREFIX}/hyunki_market/dt=${DT}/\",
              \"--format\",\"parquet\",
              \"--output\",\"${CLEAN_S3_PREFIX}/normalized/dt=${DT}/\",
              \"--dt\",\"${DT}\",
              \"--output-format\",\"parquet\"]
    },
    {
      \"Name\":\"name_price_summary\",
      \"Type\":\"CUSTOM_JAR\",
      \"ActionOnFailure\":\"CONTINUE\",
      \"Jar\":\"command-runner.jar\",
      \"Args\":[\"spark-submit\",\"${CODE_S3_PREFIX}/build_name_price_summary.py\",
              \"--input\",\"${CLEAN_S3_PREFIX}/normalized/dt=${DT}/\",
              \"--format\",\"parquet\",
              \"--output\",\"${MART_S3_PREFIX}/name_price_summary/dt=${DT}/\",
              \"--dt\",\"${DT}\",
              \"--output-format\",\"csv\",
              \"--canonical-output\",\"${MART_S3_PREFIX}/part_no_canonical_name/dt=${DT}/\"]
    }
  ]"
