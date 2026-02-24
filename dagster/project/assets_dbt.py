import os
from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCloudResource
from .resources import dbt_cloud

@asset(required_resource_keys={"dbt_cloud"}, deps=["raw_netflix_titles"])
def dbt_poc_run(context: AssetExecutionContext):
    """
    Triggers the dbt Cloud job that builds all models (staging → intermediate → marts).
    """
    job_id = int(os.environ["DBT_CLOUD_JOB_ID"])
    dbt: DbtCloudResource = context.resources.dbt_cloud

    run = dbt.run_job_and_wait_for_completion(job_id=job_id)
    context.log.info(f"dbt Cloud run finished with status: {run['status_humanized']}")
