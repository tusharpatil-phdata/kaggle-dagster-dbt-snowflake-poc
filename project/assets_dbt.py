import os
import time
import requests
from dagster import asset, AssetExecutionContext

@asset(
    group_name="dbt",
    deps=["raw_netflix_titles"],
)
def dbt_poc_run(context: AssetExecutionContext):
    """
    Triggers the dbt Cloud job and waits for it to complete using the dbt Cloud REST API.
    """

    api_token = os.environ["DBT_CLOUD_API_TOKEN"]
    account_id = os.environ["DBT_CLOUD_ACCOUNT_ID"]
    job_id = os.environ["DBT_CLOUD_JOB_ID"]

    base_url = "https://ih492.us1.dbt.com/api/v2"

    headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json",
    }

    # 1) Trigger the job run
    trigger_url = f"{base_url}/accounts/{account_id}/jobs/{job_id}/run/"
    context.log.info(f"Triggering dbt Cloud job {job_id} at {trigger_url}")

    payload = {
        "cause": "Triggered from Dagster Cloud POC"
    }

    resp = requests.post(trigger_url, headers=headers, json=payload)

    if resp.status_code >= 400:
        context.log.error(
            f"dbt Cloud job trigger failed: status={resp.status_code}, body={resp.text}"
        )
        resp.raise_for_status()

    data = resp.json()
    run_id = data["data"]["id"]
    context.log.info(f"Triggered dbt Cloud run_id={run_id}")

    # 2) Poll the run until completion
    run_status_url = f"{base_url}/accounts/{account_id}/runs/{run_id}/"
    wait_seconds = 10
    max_wait = 30 * 60  # 30 minutes
    elapsed = 0

    final_status = None
    final_message = None

    while elapsed < max_wait:
        time.sleep(wait_seconds)
        elapsed += wait_seconds

        status_resp = requests.get(run_status_url, headers=headers)
        status_resp.raise_for_status()
        status_data = status_resp.json()["data"]

        status = status_data["status"]  # numeric
        status_humanized = status_data.get("status_humanized")
        final_message = status_data.get("status_message")

        context.log.info(
            f"dbt Cloud run_id={run_id} status={status} ({status_humanized}) "
            f"elapsed={elapsed}s"
        )

        # 10 = Success, 20 = Error, 30 = Cancelled (terminal states)
        if status in (10, 20, 30):
            final_status = status
            break

    if final_status != 10:
        raise Exception(
            f"dbt Cloud run_id={run_id} did not succeed. "
            f"Final status={final_status}, message={final_message}"
        )

    context.log.info(
        f"dbt Cloud run_id={run_id} completed successfully with status={final_status}"
    )

    context.add_output_metadata({"dbt_run_id": run_id})
