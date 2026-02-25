import os
import time
import requests
from dagster import asset, AssetExecutionContext

@asset(deps=["raw_netflix_titles"])
def dbt_poc_run(context: AssetExecutionContext):
    """
    Triggers the dbt Cloud job and waits for it to complete using the dbt Cloud REST API.
    """

    api_token = os.environ["DBT_CLOUD_API_TOKEN"]
    account_id = os.environ["DBT_CLOUD_ACCOUNT_ID"]
    job_id = os.environ["DBT_CLOUD_JOB_ID"]

    # Your dbt Cloud region is us1 (from your URL: ih492.us1.dbt.com)
    base_url = "https://ih492.us1.dbt.com/api/v2"

    headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json",
    }

    # 1) Trigger the job run
    trigger_url = f"{base_url}/accounts/{account_id}/jobs/{job_id}/run/"
    context.log.info(f"Triggering dbt Cloud job {job_id} at {trigger_url}")

    resp = requests.post(trigger_url, headers=headers, json={})

    # Log error body if dbt Cloud responds with 4xx/5xx
    if resp.status_code >= 400:
        context.log.error(
            f"dbt Cloud job trigger failed: status={resp.status_code}, body={resp.text}"
        )
        resp.raise_for_status()

    data = resp.json()

    # dbt Cloud API v2 wraps data under "data"
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

        # According to dbt Cloud docs: 10 = Success, 20 = Error, etc.
        if status in (10, 20, 30):  # terminal states
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

    # Optional: store run_id in metadata
    context.add_output_metadata({"dbt_run_id": run_id})
