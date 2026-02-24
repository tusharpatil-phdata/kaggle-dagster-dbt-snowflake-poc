import pathlib
import snowflake.connector
from dagster import asset, AssetExecutionContext, MetadataValue
from .resources import snowflake_conn

@asset
def kaggle_download_netflix(context: AssetExecutionContext) -> str:
    """
    Downloads netflix_titles.csv from Kaggle into /tmp/kaggle/ in Dagster Cloud.
    """
    import kaggle

    dataset = "shivamb/netflix-shows"
    file_name = "netflix_titles.csv"

    download_dir = pathlib.Path("/tmp/kaggle")
    download_dir.mkdir(parents=True, exist_ok=True)
    local_path = download_dir / file_name

    kaggle.api.authenticate()  # uses KAGGLE_USERNAME/KAGGLE_KEY env vars
    kaggle.api.dataset_download_file(
        dataset,
        file_name,
        path=str(download_dir),
        unzip=True,
    )

    context.log.info(f"Downloaded {file_name} to {local_path}")
    context.add_output_metadata({"local_path": MetadataValue.path(str(local_path))})

    return str(local_path)

@asset(required_resource_keys={"snowflake_conn"}, deps=["kaggle_download_netflix"])
def snowflake_stage_netflix(
    context: AssetExecutionContext,
    kaggle_download_netflix: str,
):
    """
    PUT the CSV from Dagster container to Snowflake stage POC_DB.RAW.KAGGLE_STAGE.
    """
    conn: snowflake.connector.SnowflakeConnection = context.resources.snowflake_conn
    cur = conn.cursor()

    cur.execute("USE DATABASE POC_DB")
    cur.execute("USE SCHEMA RAW")

    file_path = kaggle_download_netflix.replace("\\", "/")
    put_sql = f"PUT file://{file_path} @KAGGLE_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    context.log.info(f"Running Snowflake PUT: {put_sql}")
    res = cur.execute(put_sql).fetchall()

    context.add_output_metadata({"put_result": MetadataValue.json([list(r) for r in res])})

@asset(required_resource_keys={"snowflake_conn"}, deps=["snowflake_stage_netflix"])
def raw_netflix_titles(context: AssetExecutionContext):
    """
    Creates RAW.NETFLIX_TITLES and loads data from @KAGGLE_STAGE via COPY INTO.
    """
    conn = context.resources.snowflake_conn
    cur = conn.cursor()

    cur.execute("USE DATABASE POC_DB")
    cur.execute("USE SCHEMA RAW")

    cur.execute("""
        CREATE OR REPLACE TABLE NETFLIX_TITLES (
          show_id STRING,
          type STRING,
          title STRING,
          director STRING,
          cast STRING,
          country STRING,
          date_added STRING,
          release_year INT,
          rating STRING,
          duration STRING,
          listed_in STRING,
          description STRING
        )
    """)

    copy_sql = """
        COPY INTO NETFLIX_TITLES
        FROM @KAGGLE_STAGE
        FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        ON_ERROR = 'CONTINUE'
        FORCE = TRUE;
    """
    context.log.info("Running COPY INTO NETFLIX_TITLES")
    res = cur.execute(copy_sql).fetchall()

    context.add_output_metadata({"copy_result": MetadataValue.json([list(r) for r in res])})
