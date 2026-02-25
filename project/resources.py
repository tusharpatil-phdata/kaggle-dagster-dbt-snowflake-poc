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

# IMPORTANT: pass auth token and account id as POSITIONAL args
dbt_cloud = DbtCloudResource(
    os.environ["DBT_CLOUD_API_TOKEN"],          # auth_token
    int(os.environ["DBT_CLOUD_ACCOUNT_ID"]),    # account_id
)
