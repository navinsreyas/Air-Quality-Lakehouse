from pyspark import pipelines as dp
from pyspark.sql import functions as F


RAW_PATH = "/Volumes/workspace/aq_bronze/raw"
POLLUTANTS = ["PM2.5", "PM10", "NO2", "OZONE"]


@dp.table(
    name="bronze_observations",
    comment="Raw NSW air quality and weather observations loaded incrementally with Auto Loader."
)
def bronze_observations():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("multiLine", "true")
        .load(RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="silver_observations",
    comment="Cleaned and validated hourly NSW air quality and weather observations."
)
@dp.expect_or_drop(
    "valid_site",
    "site_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_date",
    "obs_date IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_hour",
    "hour_1based BETWEEN 1 AND 24"
)
@dp.expect_or_drop(
    "has_reading",
    "value IS NOT NULL"
)
@dp.expect(
    "above_detection_floor",
    """
    parameter_code NOT IN ('PM2.5', 'PM10', 'NO2', 'OZONE')
    OR value > -20
    """
)
def silver_observations():
    return (
        spark.readStream
        .table("bronze_observations")
        .select(
            F.col("Site_Id").alias("site_id"),
            F.col("Parameter.ParameterCode").alias("parameter_code"),
            F.col("Parameter.Units").alias("units"),
            F.col("Parameter.Frequency").alias("frequency"),
            F.to_date("Date").alias("obs_date"),
            F.col("Hour").alias("hour_1based"),
            F.col("Value").alias("value"),
            F.col("AirQualityCategory").alias("aqi_category"),
            F.col("_source_file"),
            F.col("_ingested_at"),
            F.col("_rescued_data")
        )
        .withColumn(
            "obs_hour",
            F.col("hour_1based") - 1
        )
        .withColumn(
            "obs_time",
            F.expr(
                "timestampadd(HOUR, obs_hour, CAST(obs_date AS TIMESTAMP))"
            )
        )
        .withColumn(
            "below_detection",
            F.when(
                F.col("parameter_code").isin(POLLUTANTS),
                F.col("value") < 0
            ).otherwise(
                F.lit(None).cast("boolean")
            )
        )
        .withColumn(
            "value_clean",
            F.when(
                F.col("parameter_code").isin(POLLUTANTS),
                F.greatest(
                    F.col("value"),
                    F.lit(0.0)
                )
            ).otherwise(
                F.col("value")
            )
        )
        .withColumn(
            "year",
            F.year("obs_date")
        )
        .withColumn(
            "month",
            F.month("obs_date")
        )
    )