# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — NSW Air Quality Ingestion
# MAGIC
# MAGIC Downloads hourly observations from the NSW Air Quality API and lands them
# MAGIC unchanged as JSON files, then reads those files into a Delta table.
# MAGIC
# MAGIC Scope: 24 Sydney monitoring stations, 4 pollutants, 2020-2025.
# MAGIC
# MAGIC Design rules:
# MAGIC - Raw JSON written to a Volume exactly as received. No cleaning at this stage.
# MAGIC - If downstream logic changes, tables are rebuilt from these files. The API is
# MAGIC   never re-queried.
# MAGIC - Every chunk recorded in a manifest table so nothing can be silently lost.

# COMMAND ----------

import os, json, time, requests
from datetime import datetime, UTC
from pyspark.sql import functions as F

BASE = "https://data.airquality.nsw.gov.au/api/Data"
RAW  = "/Volumes/workspace/aq_bronze/raw"
os.makedirs(RAW, exist_ok=True)

POLLUTANTS = ["PM2.5", "PM10", "NO2", "OZONE"]
YEARS      = [2020, 2021, 2022, 2023, 2024, 2025]

# COMMAND ----------

# MAGIC %md
# MAGIC RAW is a Volume — governed file storage inside Unity Catalog. Files here survive session restarts, unlike Python variables.

# COMMAND ----------

# MAGIC %md
# MAGIC Why the station filter exists
# MAGIC
# MAGIC The API returns 137 NSW sites, 26 of them tagged as a Sydney region. Two of those 26 are not stations at all:
# MAGIC
# MAGIC Site_Id: 120000000, SiteName: "Sydney north-west", Latitude: None, Longitude: None
# MAGIC Site_Id: 130000000, SiteName: "Sydney south-west", Latitude: None, Longitude: None
# MAGIC
# MAGIC A physical monitoring station has coordinates. These are regional aggregates — pre-averaged summaries the API publishes alongside real stations. Include them and every regional average counts that region twice.
# MAGIC
# MAGIC The honest filter is on null coordinates, not on the ID being suspiciously large.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Station selection
# MAGIC
# MAGIC Two of the 26 Sydney-region entries are regional aggregates, not physical
# MAGIC stations. Identified by null coordinates and a SiteName identical to the Region.
# MAGIC Excluded to prevent double-counting in regional averages.

# COMMAND ----------

sites = requests.get(f"{BASE}/get_SiteDetails").json()

stations = [s for s in sites
            if s["Region"].lower().startswith("sydney")
            and s["Latitude"] is not None
            and s["Longitude"] is not None]

print(f"{len(sites)} NSW sites -> {len(stations)} physical Sydney stations")

# COMMAND ----------

# MAGIC %md
# MAGIC Why the chunk is one station, one pollutant, one year
# MAGIC
# MAGIC You tested this rather than guessing:
# MAGIC
# MAGIC Request	Result
# MAGIC 6 pollutants × 1 station × 1 year	HTTP 502 — the upstream server crashed
# MAGIC 1 pollutant × 1 station × 1 year	8,760 records, fine
# MAGIC 1 pollutant × 1 station × 3 months	2,160 records, fine
# MAGIC
# MAGIC So the ceiling sits between those, and one pollutant-year is comfortably under it.
# MAGIC
# MAGIC Why EndDate is the following year
# MAGIC
# MAGIC Every test came back exactly 24 records short, no matter the window length:
# MAGIC
# MAGIC Asked for	Expected	Got
# MAGIC Jan 2024	744	720
# MAGIC Jan–Mar 2024	2,184	2,160
# MAGIC All 2024	8,784	8,760
# MAGIC
# MAGIC A constant shortfall of 24 — one day — means the end date is exclusive, not truncation. Confirmed by printing the returned date range: asking for 1–31 January returned 1–30 January.
# MAGIC
# MAGIC Left uncorrected, that would have quietly lost one day per station-pollutant-year: 1,296 missing days across the dataset, invisible unless someone counted.
# MAGIC
# MAGIC Cell 5 — %md
# MAGIC ## 2. Download function
# MAGIC
# MAGIC Chunk size is one station, one pollutant, one year. Larger requests cause the
# MAGIC upstream server to return HTTP 502.
# MAGIC
# MAGIC The API treats EndDate as EXCLUSIVE: a request ending 2024-01-31 returns data
# MAGIC through 2024-01-30. Requests therefore use the first day of the following year.
# MAGIC
# MAGIC Three retries with exponential backoff handle transient upstream failures.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Download function
# MAGIC
# MAGIC Chunk size is one station, one pollutant, one year. Larger requests cause the
# MAGIC upstream server to return HTTP 502.
# MAGIC
# MAGIC The API treats EndDate as EXCLUSIVE: a request ending 2024-01-31 returns data
# MAGIC through 2024-01-30. Requests therefore use the first day of the following year.
# MAGIC
# MAGIC Three retries with exponential backoff handle transient upstream failures.

# COMMAND ----------

def land_chunk(site_id, pollutant, year, retries=3):
    """Download one station-pollutant-year and save the raw JSON, unchanged."""
    body = {
        "Parameters": [pollutant],
        "Sites": [site_id],
        "StartDate": f"{year}-01-01",
        "EndDate":   f"{year + 1}-01-01",   # exclusive
        "Categories": ["Averages"],
        "SubCategories": ["Hourly"],
        "Frequency": ["Hourly average"],
    }

    safe = pollutant.replace(".", "_")
    path = f"{RAW}/site{site_id}_{safe}_{year}.json"

    for attempt in range(retries):
        try:
            r = requests.post(f"{BASE}/get_Observations", json=body, timeout=180)
            if r.status_code == 200:
                records = r.json()
                with open(path, "w") as f:
                    json.dump(records, f)
                return {"site_id": site_id, "pollutant": pollutant, "year": year,
                        "path": path, "records": len(records),
                        "status": "ok", "error": None,
                        "fetched_at": datetime.now(UTC).isoformat()}
            err = f"HTTP {r.status_code}: {r.text[:150]}"
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
        time.sleep(2 ** attempt)

    return {"site_id": site_id, "pollutant": pollutant, "year": year,
            "path": path, "records": 0, "status": "failed", "error": err,
            "fetched_at": datetime.now(UTC).isoformat()}

# COMMAND ----------

print(land_chunk(33, "PM2.5", 2024))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Backfill
# MAGIC
# MAGIC 576 chunks: 24 stations x 4 pollutants x 6 years. Roughly two hours.
# MAGIC
# MAGIC Chunks returning zero records are not failures. They indicate a station that does
# MAGIC not monitor that pollutant, or was not operating that year.

# COMMAND ----------

###ite_ids = [s["Site_Id"] for s in stations]
##jobs = [(s, p, y) for s in site_ids for p in POLLUTANTS for y in YEARS]
##print(f"{len(jobs)} chunks queued")

##results = []
#for i, (site, poll, year) in enumerate(jobs, 1):
    #res = land_chunk(site, poll, year)
    #results.append(res)
    #if i % 25 == 0 or res["status"] == "failed":
        #ok = sum(1 for r in results if r["status"] == "ok")
        #print(f"[{i}/{len(jobs)}] ok={ok} last={site}/{poll}/{year} "
#              f"{res['status']} {res['records']}")

#print("FINISHED  ok:", sum(1 for r in results if r["status"] == "ok"),
 #     " failed:", sum(1 for r in results if r["status"] == "failed"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Manifest
# MAGIC
# MAGIC Built by reading the landed files rather than trusting the loop's in-memory
# MAGIC results. Survives a session restart and records what is actually on disk.

# COMMAND ----------

def parse_name(fname):
    """site33_PM2_5_2024.json -> (33, 'PM2.5', 2024)"""
    stem  = fname.replace(".json", "")
    parts = stem.split("_")
    site  = int(parts[0].replace("site", ""))
    year  = int(parts[-1])
    pollutant = "_".join(parts[1:-1]).replace("_", ".")
    return site, pollutant, year

files = sorted(os.listdir(RAW))
manifest = []
for f in files:
    site, pollutant, year = parse_name(f)
    with open(f"{RAW}/{f}") as fh:
        records = json.load(fh)
    manifest.append({"site_id": site, "pollutant": pollutant, "year": year,
                     "path": f"{RAW}/{f}", "records": len(records), "status": "ok"})

(spark.createDataFrame(manifest)
    .withColumn("run_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("workspace.aq_bronze.ingest_manifest"))

spark.sql("""
SELECT count(*)                                     AS total_chunks,
       sum(CASE WHEN records = 0 THEN 1 ELSE 0 END) AS empty_chunks,
       sum(records)                                 AS total_records
FROM workspace.aq_bronze.ingest_manifest
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze table
# MAGIC
# MAGIC Reads every landed file into one Delta table with two audit columns added.
# MAGIC Nothing else is changed - types, names and nesting stay exactly as received.
# MAGIC
# MAGIC multiLine is required because each file contains a single JSON array rather than
# MAGIC one record per line.
# MAGIC
# MAGIC _metadata.file_path is a built-in Spark column recording the source file of every
# MAGIC row, making any bad row traceable to its origin.

# COMMAND ----------

bronze = (spark.read
    .option("multiLine", True)
    .json(f"{RAW}/*.json")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path")))

(bronze.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("workspace.aq_bronze.observations"))

spark.table("workspace.aq_bronze.observations").printSchema()

# COMMAND ----------

spark.sql("""
SELECT
  (SELECT count(*)     FROM workspace.aq_bronze.observations)    AS table_rows,
  (SELECT sum(records) FROM workspace.aq_bronze.ingest_manifest) AS files_on_disk
""").show()

# COMMAND ----------

spark.sql("""
SELECT
  (SELECT count(*)     FROM workspace.aq_bronze.observations)    AS table_rows,
  (SELECT sum(records) FROM workspace.aq_bronze.ingest_manifest) AS files_on_disk
""").show()

# COMMAND ----------

spark.sql("""
SELECT Parameter.ParameterCode AS pollutant,
       count(*)                AS total_rows,
       count(Value)            AS has_value,
       count(*) - count(Value) AS nulls,
       round(100.0 * (count(*) - count(Value)) / count(*), 1) AS pct_null,
       min(Date) AS first_date, max(Date) AS last_date
FROM workspace.aq_bronze.observations
GROUP BY 1 ORDER BY 1
""").show()

# COMMAND ----------

in_data = {r.Site_Id for r in spark.sql(
    "SELECT DISTINCT Site_Id FROM workspace.aq_bronze.observations").collect()}

missing = [s for s in stations if s["Site_Id"] not in in_data]
print(len(in_data), "stations with data;", len(missing), "with none")
for s in missing:
    print(" ", s["Site_Id"], s["SiteName"])

# COMMAND ----------

print(len(stations), "stations")
print(YEARS)
print(land_chunk)

# COMMAND ----------

WEATHER = ["TEMP", "HUMID", "WSP", "WDR", "RAIN"]

site_ids = [s["Site_Id"] for s in stations]

jobs = [
    (site, parameter, year)
    for site in site_ids
    for parameter in WEATHER
    for year in YEARS
]

print(f"{len(jobs)} chunks queued")

# COMMAND ----------

results = []

for i, (site, parameter, year) in enumerate(jobs, 1):
    res = land_chunk(site, parameter, year)
    results.append(res)

    if i % 25 == 0 or res["status"] == "failed":
        ok = sum(1 for r in results if r["status"] == "ok")
        empty = sum(1 for r in results if r["records"] == 0)

        print(
            f"[{i}/{len(jobs)}] "
            f"ok={ok} "
            f"empty={empty} "
            f"last={site}/{parameter}/{year} "
            f"{res['status']} "
            f"{res['records']}"
        )

print("FINISHED")
print("  ok    :", sum(1 for r in results if r["status"] == "ok"))
print("  failed:", sum(1 for r in results if r["status"] == "failed"))
print("  empty :", sum(1 for r in results if r["records"] == 0))

# COMMAND ----------

import os

files = sorted(os.listdir(RAW))

print(len(files), "files total")

weather = [
    f for f in files
    if any(
        parameter in f
        for parameter in ["TEMP", "HUMID", "WSP", "WDR", "RAIN"]
    )
]

print(len(weather), "weather files")