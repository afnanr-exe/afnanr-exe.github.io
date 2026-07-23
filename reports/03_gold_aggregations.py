# =========================================================
# GOLD LAYER — Business-ready aggregate tables
# =========================================================

from pyspark.sql.functions import sum as _sum, avg, count, when, col

gold_schema = "workspace.gold"
silver_schema = "workspace.silver"
bronze_schema = "workspace.bronze"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

trips_fact = spark.table(f"{silver_schema}.trips_fact")
safety_incidents = spark.table(f"{bronze_schema}.safety_incidents")
routes = spark.table(f"{bronze_schema}.routes")
customers = spark.table(f"{bronze_schema}.customers")

# ---------------------------------------------------------
# 1. Driver performance
# Rate calculated as sum(revenue)/sum(miles) — NOT avg of ratios
# ---------------------------------------------------------
driver_perf = (trips_fact.groupBy("driver_id", "driver_first_name", "driver_last_name")
    .agg(
        count("trip_id").alias("total_trips"),
        _sum("actual_distance_miles").alias("total_miles"),
        avg("average_mpg").alias("avg_mpg"),
        _sum("revenue").alias("total_revenue"),
        (_sum("revenue") / _sum("actual_distance_miles")).alias("revenue_per_mile")
    ))
driver_perf.write.mode("overwrite").format("delta").saveAsTable(f"{gold_schema}.driver_performance")
print(f"driver_performance: {driver_perf.count()} rows")

# ---------------------------------------------------------
# 2. Route profitability
# ---------------------------------------------------------
route_profit = (trips_fact.groupBy("route_id")
    .agg(
        count("trip_id").alias("total_trips"),
        _sum("revenue").alias("total_revenue"),
        avg("actual_distance_miles").alias("avg_distance_miles"),
        avg("average_mpg").alias("avg_mpg")
    )
    .join(routes.select("route_id", "origin_city", "destination_city", "base_rate_per_mile"),
          "route_id", "left"))
route_profit.write.mode("overwrite").format("delta").saveAsTable(f"{gold_schema}.route_profitability")
print(f"route_profitability: {route_profit.count()} rows")

# ---------------------------------------------------------
# 3. Truck / fleet utilization
# ---------------------------------------------------------
truck_util = (trips_fact.groupBy("truck_id", "truck_make", "truck_model_year")
    .agg(
        count("trip_id").alias("total_trips"),
        _sum("actual_distance_miles").alias("total_miles"),
        _sum("revenue").alias("total_revenue")
    ))
truck_util.write.mode("overwrite").format("delta").saveAsTable(f"{gold_schema}.truck_utilization")
print(f"truck_utilization: {truck_util.count()} rows")

# ---------------------------------------------------------
# 4. Customer analysis
# ---------------------------------------------------------
customer_analysis = (trips_fact.groupBy("customer_id")
    .agg(
        count("trip_id").alias("total_shipments"),
        _sum("revenue").alias("total_revenue"),
        avg("revenue").alias("avg_revenue_per_shipment")
    )
    .join(customers.select("customer_id", "customer_name", "customer_type", "account_status"),
          "customer_id", "left"))
customer_analysis.write.mode("overwrite").format("delta").saveAsTable(f"{gold_schema}.customer_analysis")
print(f"customer_analysis: {customer_analysis.count()} rows")

# ---------------------------------------------------------
# 5. Safety metrics
# ---------------------------------------------------------
safety_metrics = (safety_incidents.groupBy("driver_id")
    .agg(
        count("incident_id").alias("total_incidents"),
        _sum(when(col("at_fault_flag") == True, 1).otherwise(0)).alias("at_fault_incidents"),
        _sum(when(col("preventable_flag") == True, 1).otherwise(0)).alias("preventable_incidents"),
        _sum("vehicle_damage_cost").alias("total_vehicle_damage"),
        _sum("claim_amount").alias("total_claims")
    ))
safety_metrics.write.mode("overwrite").format("delta").saveAsTable(f"{gold_schema}.safety_metrics")
print(f"safety_metrics: {safety_metrics.count()} rows")
