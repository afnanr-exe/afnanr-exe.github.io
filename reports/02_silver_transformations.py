# =========================================================
# SILVER LAYER — Cleaned, joined, business-ready tables
# =========================================================

from pyspark.sql.functions import col, current_timestamp

silver_schema = "workspace.silver"
bronze_schema = "workspace.bronze"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")

# ---------------------------------------------------------
# trips_fact — central fact table (star schema)
# Grain: one row per trip
# Dimensions joined: loads, drivers, trucks, trailers
# ---------------------------------------------------------
trips = spark.table(f"{bronze_schema}.trips")
loads = spark.table(f"{bronze_schema}.loads")
drivers = spark.table(f"{bronze_schema}.drivers")
trucks = spark.table(f"{bronze_schema}.trucks")
trailers = spark.table(f"{bronze_schema}.trailers")

trips_fact = (trips
    .join(loads.select("load_id", "customer_id", "route_id", "revenue",
                        "fuel_surcharge", "load_type", "booking_type"),
          "load_id", "left")
    .join(drivers.select(col("driver_id"),
                          col("first_name").alias("driver_first_name"),
                          col("last_name").alias("driver_last_name")),
          "driver_id", "left")
    .join(trucks.select(col("truck_id"),
                         col("make").alias("truck_make"),
                         col("model_year").alias("truck_model_year")),
          "truck_id", "left")
    .join(trailers.select(col("trailer_id"), col("trailer_type")),
          "trailer_id", "left")
)

trips_fact.write.mode("overwrite").format("delta").saveAsTable(f"{silver_schema}.trips_fact")
print(f"trips_fact: {trips_fact.count()} rows, {len(trips_fact.columns)} columns")

# ---------------------------------------------------------
# fuel_purchases — promoted as-is (already trip-linked)
# ---------------------------------------------------------
fuel_purchases = spark.table(f"{bronze_schema}.fuel_purchases")
fuel_purchases.write.mode("overwrite").format("delta").saveAsTable(f"{silver_schema}.fuel_purchases")
print(f"fuel_purchases: {fuel_purchases.count()} rows")

# ---------------------------------------------------------
# delivery_events — add computed delay_minutes column
# ---------------------------------------------------------
delivery_events = spark.table(f"{bronze_schema}.delivery_events")
delivery_events_silver = delivery_events.withColumn(
    "delay_minutes",
    (col("actual_datetime").cast("long") - col("scheduled_datetime").cast("long")) / 60
)
delivery_events_silver.write.mode("overwrite").format("delta").saveAsTable(f"{silver_schema}.delivery_events")
print(f"delivery_events: {delivery_events_silver.count()} rows")
