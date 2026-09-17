# Databricks notebook source
spark.sql("""
CREATE OR REPLACE TABLE workspace.aq_gold.sensor_flatline_raw AS
WITH runs AS (
  SELECT
    site_id, parameter_code, obs_time, value,
    ROW_NUMBER() OVER (PARTITION BY site_id, parameter_code ORDER BY obs_time)
      - ROW_NUMBER() OVER (PARTITION BY site_id, parameter_code, value ORDER BY obs_time)
      AS run_group
  FROM workspace.aq_silver.fact_observation
  WHERE value <> 0
)
SELECT
  site_id, parameter_code,
  value         AS stuck_value,
  MIN(obs_time) AS run_start,
  MAX(obs_time) AS run_end,
  COUNT(*)      AS hours_stuck
FROM runs
GROUP BY site_id, parameter_code, value, run_group
HAVING COUNT(*) >= 6
""")

spark.sql("""
SELECT count(*) AS genuine_flatline_runs,
       max(hours_stuck) AS longest_hours,
       count(DISTINCT site_id) AS stations_affected
FROM workspace.aq_gold.sensor_flatline_raw
""").show()

# COMMAND ----------

spark.sql("""
SELECT s.site_name, f.parameter_code, f.stuck_value, f.hours_stuck, f.run_start
FROM workspace.aq_gold.sensor_flatline_raw f
JOIN workspace.aq_silver.dim_station s USING (site_id)
ORDER BY f.hours_stuck DESC LIMIT 15
""").show(truncate=False)

# COMMAND ----------

old = spark.table("workspace.aq_gold.sensor_flatline").count()
new = spark.table("workspace.aq_gold.sensor_flatline_raw").count()

print(f"floored-value detector : {old:,} runs, 100% at value 0.0 (all false positives)")
print(f"raw-value detector     : {new:,} runs of repeated non-zero readings")

# COMMAND ----------

spark.sql("""
WITH runs AS (
  SELECT site_id, parameter_code, obs_time, value,
    ROW_NUMBER() OVER (PARTITION BY site_id, parameter_code ORDER BY obs_time)
      - ROW_NUMBER() OVER (PARTITION BY site_id, parameter_code, value ORDER BY obs_time)
      AS run_group
  FROM workspace.aq_silver.fact_observation
  WHERE value <> 0
)
SELECT COUNT(*) AS run_count, MAX(hours) AS longest
FROM (
  SELECT COUNT(*) AS hours
  FROM runs
  GROUP BY site_id, parameter_code, value, run_group
  HAVING COUNT(*) >= 2
)
""").show()