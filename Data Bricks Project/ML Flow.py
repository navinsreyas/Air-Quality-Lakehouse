# Databricks notebook source
user = spark.sql("SELECT current_user()").collect()[0][0]
print(user)

# COMMAND ----------

import mlflow
mlflow.set_experiment(f"/Users/{user}/air_quality_anomaly_thresholds")
print("experiment set")

# COMMAND ----------

import mlflow

configs = [(z, w)
           for z in [2.5, 3.0, 3.5, 4.0, 5.0]
           for w in [72, 168, 336, 720]]     # 3, 7, 14, 30 days

for z_threshold, window in configs:
    with mlflow.start_run(run_name=f"z{z_threshold}_w{window}"):
        mlflow.log_params({
            "z_threshold":   z_threshold,
            "window_hours":  window,
            "pollutant":     "PM2.5",
            "baseline_type": "rolling",
        })

        row = spark.sql(f"""
        WITH scored AS (
          SELECT site_id, obs_time, obs_date, value_clean,
                 AVG(value_clean)    OVER w AS m,
                 STDDEV(value_clean) OVER w AS s
          FROM workspace.aq_silver.fact_observation
          WHERE parameter_code = 'PM2.5'
          WINDOW w AS (PARTITION BY site_id ORDER BY obs_time
                       ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING)
        ),
        flagged AS (
          SELECT *, ABS((value_clean - m) / NULLIF(s, 0)) > {z_threshold} AS is_anom
          FROM scored
          WHERE s IS NOT NULL AND s > 0
        )
        SELECT
          COUNT(*)                                   AS total,
          SUM(CASE WHEN is_anom THEN 1 ELSE 0 END)   AS anomalies,
          SUM(CASE WHEN is_anom
                    AND obs_date >= DATE'2020-01-01'
                    AND obs_date <  DATE'2020-02-01'
                   THEN 1 ELSE 0 END)                AS jan2020_anomalies
        FROM flagged
        """).collect()[0]

        total = row["total"]
        anoms = row["anomalies"] or 0
        jan   = row["jan2020_anomalies"] or 0

        mlflow.log_metrics({
            "anomaly_rate_pct":  round(100.0 * anoms / total, 4) if total else 0.0,
            "total_anomalies":   float(anoms),
            "jan2020_anomalies": float(jan),
            "jan2020_share_pct": round(100.0 * jan / anoms, 4) if anoms else 0.0,
        })

        print(f"z={z_threshold:<4} w={window:<4} "
              f"rate={100.0*anoms/total:6.3f}%  jan2020={jan}")

# COMMAND ----------

runs = mlflow.search_runs(order_by=["metrics.jan2020_share_pct DESC"])
cols = ["params.z_threshold", "params.window_hours",
        "metrics.anomaly_rate_pct", "metrics.jan2020_anomalies",
        "metrics.jan2020_share_pct"]
print(runs[cols].to_string(index=False))

# COMMAND ----------

import mlflow
runs = mlflow.search_runs(order_by=["params.window_hours ASC", "params.z_threshold ASC"])
cols = ["params.window_hours", "params.z_threshold",
        "metrics.anomaly_rate_pct", "metrics.jan2020_anomalies",
        "metrics.jan2020_share_pct"]
print(runs[cols].to_string(index=False))