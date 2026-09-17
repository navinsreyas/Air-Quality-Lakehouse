# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1. Which region is worst, and by how much
# MAGIC SELECT region, parameter_code,
# MAGIC        ROUND(AVG(daily_avg), 2) AS mean_daily,
# MAGIC        COUNT(DISTINCT site_id)  AS stations
# MAGIC FROM workspace.aq_gold.daily_site_summary
# MAGIC WHERE parameter_code IN ('PM2.5','PM10')
# MAGIC GROUP BY 1, 2 ORDER BY 2, 3 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Did air quality improve, 2020 to 2025
# MAGIC SELECT parameter_code, year(obs_date) AS yr,
# MAGIC        ROUND(AVG(daily_avg), 2) AS mean_daily
# MAGIC FROM workspace.aq_gold.daily_site_summary
# MAGIC GROUP BY 1, 2 ORDER BY 1, 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Exceedance days per year
# MAGIC SELECT year(obs_date) AS yr, parameter_code, count(*) AS days
# MAGIC FROM workspace.aq_gold.exceedance_events
# MAGIC GROUP BY 1, 2 ORDER BY 1, 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. The NO2 traffic signature
# MAGIC SELECT obs_hour, ROUND(AVG(mean_value), 3) AS mean_no2
# MAGIC FROM workspace.aq_gold.site_hourly_profile
# MAGIC WHERE parameter_code = 'NO2'
# MAGIC GROUP BY 1 ORDER BY 1;