-- =========================================================
-- DASHBOARD QUERIES — Logistics Operations Performance Dashboard
-- Run against Gold layer tables (workspace.gold)
-- =========================================================

-- ---------------------------------------------------------
-- 1. Top 10 drivers by revenue per mile
-- Chart: Bar | X: driver_name | Y: revenue_per_mile
-- ---------------------------------------------------------
SELECT
    driver_first_name || ' ' || driver_last_name AS driver_name,
    total_trips,
    total_miles,
    ROUND(avg_mpg, 2) AS avg_mpg,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(revenue_per_mile, 2) AS revenue_per_mile
FROM workspace.gold.driver_performance
WHERE total_trips >= 10
ORDER BY revenue_per_mile DESC
LIMIT 10;


-- ---------------------------------------------------------
-- 2. Route profitability — top 15 lanes by revenue
-- Chart: Bar (stacked) | X: origin_city | Y: total_revenue | Color: destination_city
-- ---------------------------------------------------------
SELECT
    origin_city,
    destination_city,
    total_trips,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_distance_miles, 1) AS avg_distance_miles,
    ROUND(avg_mpg, 2) AS avg_mpg,
    base_rate_per_mile
FROM workspace.gold.route_profitability
ORDER BY total_revenue DESC
LIMIT 15;


-- ---------------------------------------------------------
-- 3. Fleet utilization — bottom 15 trucks by miles (underused assets)
-- Chart: Bar | X: truck_id | Y: total_miles
-- ---------------------------------------------------------
SELECT
    truck_id,
    truck_make,
    truck_model_year,
    total_trips,
    total_miles,
    ROUND(total_revenue, 2) AS total_revenue
FROM workspace.gold.truck_utilization
ORDER BY total_miles ASC
LIMIT 15;


-- ---------------------------------------------------------
-- 4. Customer revenue concentration — top 10 by revenue
-- Chart: Bar | X: customer_name | Y: total_revenue | Color: customer_type
-- ---------------------------------------------------------
SELECT
    customer_name,
    customer_type,
    account_status,
    total_shipments,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_revenue_per_shipment, 2) AS avg_revenue_per_shipment
FROM workspace.gold.customer_analysis
ORDER BY total_revenue DESC
LIMIT 10;


-- ---------------------------------------------------------
-- 5. Safety incidents — top 10 drivers, total vs at-fault
-- Chart: Bar (stacked) | X: driver_id | Y: total_incidents, at_fault_incidents
-- ---------------------------------------------------------
SELECT
    driver_id,
    total_incidents,
    at_fault_incidents,
    preventable_incidents,
    ROUND(total_vehicle_damage, 2) AS total_vehicle_damage,
    ROUND(total_claims, 2) AS total_claims
FROM workspace.gold.safety_metrics
ORDER BY total_incidents DESC
LIMIT 10;
