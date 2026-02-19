from __future__ import annotations

import json
import time
from typing import Dict


def run_retry_executions_blocking(sfn, arn_map: Dict[str, str], retry_inputs: Dict[str, Dict]) -> str:
    """Run retry state machines and wait until completion."""
    skip_reason = retry_inputs.get("_skip_reason")
    if skip_reason:
        return skip_reason

    lines = ["Retry summary"]
    for source, payload in retry_inputs.items():
        if source == "_skip_reason" or not payload:
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
