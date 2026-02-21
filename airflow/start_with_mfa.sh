#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AWS_BASE_PROFILE="${AWS_BASE_PROFILE:-base}"
# Prefer explicit env var; fallback to profile's configured mfa_serial.
AWS_MFA_SERIAL="${AWS_MFA_SERIAL:-$(aws configure get mfa_serial --profile "$AWS_BASE_PROFILE" 2>/dev/null || true)}"
AWS_MFA_DURATION="${AWS_MFA_DURATION:-43200}"
AWS_REGION="${AWS_REGION:-ap-northeast-2}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"
AIRFLOW_WEB_PORT="${AIRFLOW_WEB_PORT:-8080}"

if [[ $# -ge 1 ]]; then
  MFA_TOKEN="$1"
else
  read -rsp "MFA token code (6 digits): " MFA_TOKEN
  echo
fi

if [[ -z "${AWS_MFA_SERIAL}" ]]; then
  read -rp "MFA device serial ARN: " AWS_MFA_SERIAL
fi

echo "Requesting temporary AWS session credentials..."
read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN < <(
  aws sts get-session-token \
    --profile "$AWS_BASE_PROFILE" \
    --serial-number "$AWS_MFA_SERIAL" \
    --token-code "$MFA_TOKEN" \
    --duration-seconds "$AWS_MFA_DURATION" \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
    --output text
)

export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN
export AWS_REGION
export AWS_DEFAULT_REGION
export AIRFLOW_WEB_PORT
unset AWS_PROFILE

echo "Verifying caller identity..."
aws sts get-caller-identity --query 'Arn' --output text

cd "$SCRIPT_DIR"
echo "Recreating Airflow containers with MFA session credentials..."
docker compose up -d --no-deps --force-recreate airflow-webserver airflow-scheduler

echo "Done. MFA session credentials are now injected into Airflow containers."
echo "Session duration: ${AWS_MFA_DURATION} seconds."
echo "Airflow UI: http://localhost:${AIRFLOW_WEB_PORT}"
