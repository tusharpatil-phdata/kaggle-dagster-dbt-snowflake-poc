from dagster import (
    asset,
    AssetExecutionContext,
    StaticPartitionsDefinition,
    MetadataValue,
)
from datetime import datetime
from zoneinfo import ZoneInfo
from .resources import snowflake_conn

# Year-based static partitions
year_partitions = StaticPartitionsDefinition(
    [str(y) for y in range(1950, 2031)]
)

@asset(
    group_name="insights",
    partitions_def=year_partitions,
    required_resource_keys={"snowflake_conn"},
    deps=["dbt_poc_run"],
)
def netflix_year_insights(context: AssetExecutionContext):
    """
    One partition per release_year.

    For each partition (year), this asset:
    - Reads MARTS.MART_NETFLIX_COUNTRY_YEAR_STATS for that year.
    - Computes:
        - total_titles
        - top_countries (by titles)
    - Captures loaded_at_ist (current time in India timezone).
    - Writes an audit row into POC_DB.MARTS.NETFLIX_YEAR_INSIGHTS_AUDIT.
    - Attaches all of the above as Dagster metadata.
    """
    year = int(context.partition_key)

    # 1) Capture loaded time in India timezone
    loaded_at_ist = datetime.now(ZoneInfo("Asia/Kolkata"))

    conn = context.resources.snowflake_conn
    cur = conn.cursor()

    cur.execute("USE DATABASE POC_DB")
    cur.execute("USE SCHEMA MARTS")

    # 2) Read mart data for this year
    cur.execute(
        """
        SELECT
          country,
          title_count,
          movie_count,
          tvshow_count
        FROM MART_NETFLIX_COUNTRY_YEAR_STATS
        WHERE release_year = %s
        """,
        (year,),
    )

    rows = cur.fetchall()
    total_titles = sum(r[1] for r in rows)
    top_countries = sorted(
        [{"country": r[0], "title_count": int(r[1])} for r in rows],
        key=lambda x: x["title_count"],
        reverse=True,
    )[:5]

    # 3) Ensure audit table exists and insert an audit row
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS NETFLIX_YEAR_INSIGHTS_AUDIT (
          release_year INT,
          loaded_at_ist TIMESTAMP_TZ,
          run_id STRING
        )
        """
    )

    cur.execute(
        """
        INSERT INTO NETFLIX_YEAR_INSIGHTS_AUDIT (release_year, loaded_at_ist, run_id)
        VALUES (%s, %s, %s)
        """,
        (year, loaded_at_ist, context.run_id),
    )

    # 4) Attach metadata in Dagster
    context.add_output_metadata(
        {
            "year": MetadataValue.int(year),
            "total_titles": MetadataValue.int(total_titles),
            "top_countries": MetadataValue.json(top_countries),
            "loaded_at_ist": MetadataValue.text(
                loaded_at_ist.strftime("%Y-%m-%d %H:%M:%S %Z")
            ),
        }
    )

    return {
        "year": year,
        "total_titles": total_titles,
        "loaded_at_ist": loaded_at_ist.isoformat(),
    }
