from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


POLLUTANTS = ["PM2.5", "PM10", "NO2", "OZONE"]


@dp.materialized_view(
    name="gold_regional_trend",
    comment="Monthly regional pollutant trends with a 12-month rolling average and year-over-year comparison.",
    table_properties={"quality": "gold"}
)
def gold_regional_trend():

    stations = (
        spark.read
        .table("workspace.aq_silver.dim_station")
        .select(
            "site_id",
            "region"
        )
    )

    observations = (
        spark.read
        .table("silver_observations")
        .filter(
            F.col("parameter_code").isin(POLLUTANTS)
        )
    )

    monthly = (
        observations
        .join(
            stations,
            on="site_id",
            how="inner"
        )
        .withColumn(
            "month_start",
            F.trunc(F.col("obs_date"), "month")
        )
        .groupBy(
            "region",
            "parameter_code",
            "units",
            "month_start"
        )
        .agg(
            F.avg("value_clean").alias("monthly_avg"),
            F.countDistinct("site_id").alias("stations_reporting"),
            F.count("value_clean").alias("readings")
        )
    )

    ordering_window = (
        Window
        .partitionBy(
            "region",
            "parameter_code"
        )
        .orderBy("month_start")
    )

    rolling_window = (
        Window
        .partitionBy(
            "region",
            "parameter_code"
        )
        .orderBy("month_start")
        .rowsBetween(-11, 0)
    )

    result = (
        monthly
        .withColumn(
            "rolling_12m_avg",
            F.avg("monthly_avg").over(rolling_window)
        )
        .withColumn(
            "same_month_last_year",
            F.lag("monthly_avg", 12).over(ordering_window)
        )
        .withColumn(
            "yoy_pct_change",
            F.when(
                F.col("same_month_last_year").isNotNull()
                & (F.col("same_month_last_year") != 0),
                (
                    (
                        F.col("monthly_avg")
                        - F.col("same_month_last_year")
                    )
                    / F.col("same_month_last_year")
                ) * 100.0
            )
        )
    )

    return result