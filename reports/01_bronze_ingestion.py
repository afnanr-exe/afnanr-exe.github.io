# =========================================================
# BRONZE LAYER — Raw CSV ingestion into Delta tables
# Source: Kaggle - yogape/logistics-operations-database
# =========================================================

from pyspark.sql.functions import current_timestamp, col

volume_path = "/Volumes/workspace/default/logistics_ops_raw"
bronze_schema = "workspace.bronze"

# Create bronze schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")

tables = [
    "customers", "drivers", "trucks", "trailers", "facilities",
    "routes", "loads", "trips", "fuel_purchases", "maintenance_records",
    "delivery_events", "safety_incidents",
    "driver_monthly_metrics", "truck_utilization_metrics"
]

for table in tables:
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(f"{volume_path}/{table}.csv")
          .withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", col("_metadata.file_path")))

    df.write.mode("overwrite").format("delta").saveAsTable(f"{bronze_schema}.{table}")
    print(f"Loaded {table}: {df.count()} rows")
