# Databricks notebook source
results = []


def run_test(name, test_function):
    try:
        detail = test_function()

        results.append({
            "test": name,
            "passed": True,
            "detail": detail
        })

        print(f"PASS  {name}")

    except Exception as error:
        results.append({
            "test": name,
            "passed": False,
            "detail": str(error)
        })

        print(f"FAIL  {name}: {error}")


def test_hour_offset_corrected():
    result = spark.sql("""
        SELECT
            SUM(
                CASE
                    WHEN hour_1based = 1
                     AND HOUR(obs_time) <> 0
                    THEN 1
                    ELSE 0
                END
            ) AS incorrect_hour_1,
            MIN(obs_hour) AS min_obs_hour,
            MAX(obs_hour) AS max_obs_hour
        FROM workspace.aq_dlt.silver_observations
    """).collect()[0]

    assert result["incorrect_hour_1"] == 0, (
        f"{result['incorrect_hour_1']} Hour-1 observations "
        "do not map to midnight."
    )

    assert result["min_obs_hour"] == 0, (
        f"Minimum obs_hour is {result['min_obs_hour']}, expected 0."
    )

    assert result["max_obs_hour"] == 23, (
        f"Maximum obs_hour is {result['max_obs_hour']}, expected 23."
    )

    return "Hour 1 maps to midnight and obs_hour spans 0–23."


def test_pipeline_accounting():
    bronze = spark.table(
        "workspace.aq_dlt.bronze_observations"
    ).count()

    silver = spark.table(
        "workspace.aq_dlt.silver_observations"
    ).count()

    dropped = bronze - silver
    drop_rate = 100.0 * dropped / bronze

    assert bronze >= silver, (
        f"Silver ({silver:,}) exceeds Bronze ({bronze:,})."
    )

    assert dropped >= 0, (
        f"Calculated dropped count is {dropped:,}."
    )

    assert drop_rate <= 25.0, (
        f"Drop rate is {drop_rate:.2f}%, above the 25% threshold."
    )

    return (
        f"Bronze={bronze:,}; "
        f"Silver={silver:,}; "
        f"Dropped={dropped:,}; "
        f"Drop rate={drop_rate:.2f}%."
    )


def test_silver_grain_unique():
    duplicates = spark.sql("""
        SELECT COUNT(*) AS duplicate_keys
        FROM (
            SELECT
                site_id,
                parameter_code,
                obs_time
            FROM workspace.aq_dlt.silver_observations
            GROUP BY
                site_id,
                parameter_code,
                obs_time
            HAVING COUNT(*) > 1
        )
    """).collect()[0]["duplicate_keys"]

    assert duplicates == 0, (
        f"Found {duplicates:,} duplicate Silver grain keys."
    )

    return (
        "Silver grain is unique at "
        "site_id + parameter_code + obs_time."
    )


def test_value_clean_semantics():
    result = spark.sql("""
        SELECT
            SUM(
                CASE
                    WHEN parameter_code IN (
                        'PM2.5',
                        'PM10',
                        'NO2',
                        'OZONE'
                    )
                    AND value_clean < 0
                    THEN 1
                    ELSE 0
                END
            ) AS negative_pollutants,

            SUM(
                CASE
                    WHEN parameter_code IN (
                        'PM2.5',
                        'PM10',
                        'NO2',
                        'OZONE'
                    )
                    AND below_detection = TRUE
                    THEN 1
                    ELSE 0
                END
            ) AS below_detection_count,

            SUM(
                CASE
                    WHEN parameter_code = 'TEMP'
                     AND value < 0
                    THEN 1
                    ELSE 0
                END
            ) AS negative_temp,

            SUM(
                CASE
                    WHEN parameter_code = 'TEMP'
                     AND value < 0
                     AND value_clean <> value
                    THEN 1
                    ELSE 0
                END
            ) AS altered_negative_temp

        FROM workspace.aq_dlt.silver_observations
    """).collect()[0]

    assert result["negative_pollutants"] == 0, (
        f"{result['negative_pollutants']:,} cleaned pollutant "
        "readings are still negative."
    )

    assert result["below_detection_count"] > 0, (
        "No below-detection pollutant observations were found."
    )

    assert result["negative_temp"] > 0, (
        "No legitimate negative temperature observations were found."
    )

    assert result["altered_negative_temp"] == 0, (
        f"{result['altered_negative_temp']:,} negative temperature "
        "observations were altered by cleaning."
    )

    return (
        f"{result['negative_temp']:,} negative temperature readings "
        "are preserved while cleaned pollutant values remain non-negative."
    )


def test_hourly_wide_grain():
    result = spark.sql("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(
                DISTINCT struct(site_id, obs_time)
            ) AS distinct_keys
        FROM workspace.aq_gold.hourly_wide
    """).collect()[0]

    assert result["total_rows"] == result["distinct_keys"], (
        f"hourly_wide has {result['total_rows']:,} rows but only "
        f"{result['distinct_keys']:,} unique site/time keys."
    )

    return (
        f"{result['total_rows']:,} rows with one row per "
        "site_id + obs_time."
    )


def test_humidity_semantics():
    source = spark.sql("""
        SELECT
            COUNT(*) AS total_humidity,
            SUM(
                CASE
                    WHEN value_clean > 100
                    THEN 1
                    ELSE 0
                END
            ) AS above_100,
            MAX(value_clean) AS max_source_humidity
        FROM workspace.aq_dlt.silver_observations
        WHERE parameter_code = 'HUMID'
    """).collect()[0]

    analytical = spark.sql("""
        SELECT
            MIN(humidity) AS min_analytical_humidity,
            MAX(humidity) AS max_analytical_humidity
        FROM workspace.aq_gold.hourly_wide
        WHERE humidity IS NOT NULL
    """).collect()[0]

    assert source["above_100"] > 0, (
        "Silver no longer contains the known >100 humidity values."
    )

    assert source["max_source_humidity"] > 100, (
        "Silver source humidity maximum is no longer above 100."
    )

    assert analytical["min_analytical_humidity"] >= 0, (
        f"Analytical humidity minimum is "
        f"{analytical['min_analytical_humidity']}."
    )

    assert analytical["max_analytical_humidity"] <= 100, (
        f"Analytical humidity maximum is "
        f"{analytical['max_analytical_humidity']}."
    )

    return (
        f"{source['above_100']:,} >100% source readings preserved in "
        f"Silver; analytical humidity range is "
        f"{analytical['min_analytical_humidity']}–"
        f"{analytical['max_analytical_humidity']}."
    )


tests = [
    (
        "hour_offset_corrected",
        test_hour_offset_corrected
    ),
    (
        "pipeline_accounting",
        test_pipeline_accounting
    ),
    (
        "silver_grain_unique",
        test_silver_grain_unique
    ),
    (
        "value_clean_semantics",
        test_value_clean_semantics
    ),
    (
        "hourly_wide_grain",
        test_hourly_wide_grain
    ),
    (
        "humidity_semantics",
        test_humidity_semantics
    )
]


for name, function in tests:
    run_test(name, function)


results_df = spark.createDataFrame(results)

display(results_df)


failures = [
    result
    for result in results
    if not result["passed"]
]


if failures:
    raise Exception(
        f"{len(failures)} of {len(results)} tests failed: "
        f"{[failure['test'] for failure in failures]}"
    )


print()
print(f"All {len(results)} tests passed.")