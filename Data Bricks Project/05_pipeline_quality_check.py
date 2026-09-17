# Databricks notebook source
bronze = spark.table(
    "workspace.aq_dlt.bronze_observations"
).count()

silver = spark.table(
    "workspace.aq_dlt.silver_observations"
).count()

daily_gold = spark.table(
    "workspace.aq_dlt.gold_daily_site_summary"
).count()

regional_gold = spark.table(
    "workspace.aq_dlt.gold_regional_trend"
).count()

dropped = bronze - silver
drop_rate = 100.0 * dropped / bronze


silver_duplicates = spark.sql("""
    SELECT
        site_id,
        parameter_code,
        obs_time,
        COUNT(*) AS n
    FROM workspace.aq_dlt.silver_observations
    GROUP BY site_id, parameter_code, obs_time
    HAVING COUNT(*) > 1
""").count()


daily_duplicates = spark.sql("""
    SELECT
        site_id,
        obs_date,
        parameter_code,
        COUNT(*) AS n
    FROM workspace.aq_dlt.gold_daily_site_summary
    GROUP BY site_id, obs_date, parameter_code
    HAVING COUNT(*) > 1
""").count()


regional_duplicates = spark.sql("""
    SELECT
        region,
        parameter_code,
        month_start,
        COUNT(*) AS n
    FROM workspace.aq_dlt.gold_regional_trend
    GROUP BY region, parameter_code, month_start
    HAVING COUNT(*) > 1
""").count()


negative_clean_values = spark.sql("""
    SELECT COUNT(*) AS n
    FROM workspace.aq_dlt.silver_observations
    WHERE parameter_code IN ('PM2.5', 'PM10', 'NO2', 'OZONE')
      AND value_clean < 0
""").collect()[0]["n"]

max_completeness = spark.sql("""
    SELECT MAX(completeness_pct) AS max_pct
    FROM workspace.aq_dlt.gold_daily_site_summary
""").collect()[0]["max_pct"]


print(f"Bronze rows           : {bronze:,}")
print(f"Silver rows           : {silver:,}")
print(f"Dropped rows          : {dropped:,}")
print(f"Drop rate             : {drop_rate:.2f}%")
print(f"Daily Gold rows       : {daily_gold:,}")
print(f"Regional Gold rows    : {regional_gold:,}")
print(f"Silver duplicates     : {silver_duplicates:,}")
print(f"Daily Gold duplicates : {daily_duplicates:,}")
print(f"Regional duplicates   : {regional_duplicates:,}")
print(f"Negative pollutant clean values : {negative_clean_values:,}")
print(f"Max completeness      : {max_completeness}")


failures = []

if drop_rate > 25:
    failures.append(f"Drop rate {drop_rate:.2f}% exceeds 25%")

if silver_duplicates > 0:
    failures.append(f"{silver_duplicates:,} duplicate Silver grain keys")

if daily_duplicates > 0:
    failures.append(f"{daily_duplicates:,} duplicate Daily Gold grain keys")

if regional_duplicates > 0:
    failures.append(f"{regional_duplicates:,} duplicate Regional Gold grain keys")

if negative_clean_values > 0:
    failures.append(
        f"{negative_clean_values:,} negative pollutant value_clean records"
    )
if max_completeness > 100:
    failures.append(f"Completeness exceeds 100%: {max_completeness}")

if failures:
    raise Exception(" | ".join(failures))

print("All pipeline quality checks passed.")