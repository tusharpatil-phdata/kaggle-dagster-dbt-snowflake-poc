from dagster import (
    asset,
    AssetExecutionContext,
    StaticPartitionsDefinition,
    MetadataValue,
)
from .resources import snowflake_conn

# Year-based static partitions
year_partitions = StaticPartitionsDefinition(
    [str(y) for y in range(1950, 2031)]
)

@asset(
    partitions_def=year_partitions,
    required_resource_keys={"snowflake_conn"},
    deps=["dbt_poc_run"],
)
def netflix_year_insights(context: AssetExecutionContext):
    """
    One partition per release_year.
    Reads MARTS.MART_NETFLIX_COUNTRY_YEAR_STATS and attaches:
    - total_titles
    - top_countries (by titles)
    as Dagster metadata.
    """
    year = int(context.partition_key)
    conn = context.resources.snowflake_conn
    cur = conn.cursor()

    cur.execute("USE DATABASE POC_DB")
    cur.execute("USE SCHEMA MARTS")

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

    context.add_output_metadata(
        {
            "year": MetadataValue.int(year),
            "total_titles": MetadataValue.int(total_titles),
            "top_countries": MetadataValue.json(top_countries),
        }
    )

    return {"year": year, "total_titles": total_titles}
