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

# Let DbtCloudResource read its config (token, account_id, etc.) from env/config
dbt_cloud = DbtCloudResource()
