
from dagster import Definitions
from .resources import snowflake_conn, dbt_cloud
from .assets_ingestion import (
    kaggle_download_netflix,
    snowflake_stage_netflix,
    raw_netflix_titles,
)
from .assets_dbt import dbt_poc_run
from .assets_insights import netflix_year_insights

defs = Definitions(
    assets=[
        kaggle_download_netflix,
        snowflake_stage_netflix,
        raw_netflix_titles,
        dbt_poc_run,
        netflix_year_insights,
    ],
    resources={
        "snowflake_conn": snowflake_conn,
        "dbt_cloud": dbt_cloud,
    },
)
