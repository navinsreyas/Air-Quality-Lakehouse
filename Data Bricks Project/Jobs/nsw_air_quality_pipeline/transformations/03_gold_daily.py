from pyspark import pipelines as dp
from pyspark.sql import functions as F


POLLUTANTS = ["PM2.5", "PM10", "NO2", "OZONE"]


@dp.materialized_view(
    name="gold_daily_site_summary",
    comment="One row per station, date and pollutant with daily pollution statistics and hourly completeness.",
    table_properties={"quality": "gold"}
)
def gold_daily_site_summary():

    observations = (
        spark.read
        .table("silver_observations")
        .filter(
            F.col("parameter_code").isin(POLLUTANTS)
        )
    )

    stations = (
        spark.read
        .table("workspace.aq_silver.dim_station")
        .select(
            "site_id",
            "site_name",
            "region",
            "latitude",
            "longitude"
        )
    )

    return (
        observations
        .join(
            stations,
            on="site_id",
            how="inner"
        )
        .groupBy(
            "site_id",
            "site_name",
            "region",
            "latitude",
            "longitude",
            "obs_date",
            "parameter_code",
            "units"
        )
        .agg(
            F.avg("value_clean").alias("daily_avg"),
            F.max("value_clean").alias("daily_max"),
            F.min("value_clean").alias("daily_min"),
            F.count("value_clean").alias("valid_hours")
        )
        .withColumn(
            "completeness_pct",
            F.round(
                F.col("valid_hours") / F.lit(24.0) * 100.0,
                1
            )
        )
    )