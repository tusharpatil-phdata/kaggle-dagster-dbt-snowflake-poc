import os
import snowflake.connector
from dagster import resource
from dagster_dbt import DbtCloudResource

@resource
def snowflake_conn(_):
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )

dbt_cloud = DbtCloudResource(
    api_token=os.environ["DBT_CLOUD_API_TOKEN"],
    account_id=int(os.environ["DBT_CLOUD_ACCOUNT_ID"]),
)
