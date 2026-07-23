# Project 1 — Logistics Operations Database Pipeline

End-to-end data pipeline built on Databricks (PySpark + Delta Lake + SQL)
for a trucking/logistics operational database. Built as part of a
Data & Systems Analyst portfolio.

## Dataset
- Source: [Kaggle — logistics-operations-database](https://www.kaggle.com/datasets/yogape/logistics-operations-database)
- 14 CSVs: drivers, trucks, trailers, customers, facilities, routes,
  loads, trips, fuel_purchases, maintenance_records, delivery_events,
  safety_incidents, driver_monthly_metrics, truck_utilization_metrics
- ~85K trips, ~196K fuel purchases, ~171K delivery events

## Architecture — Medallion (Bronze → Silver → Gold)

**Bronze** (`workspace.bronze`) — 14 raw tables, 1:1 ingestion from CSV,
no transformation. Lineage columns (`_ingested_at`, `_source_file`) added.

**Silver** (`workspace.silver`) — cleaned/joined layer.
- `trips_fact`: central fact table (star schema) — trips joined to
  loads, drivers, trucks, trailers. Row count preserved exactly
  (85,410 in / 85,410 out), confirming a clean join.
- `fuel_purchases`, `delivery_events` (with computed `delay_minutes`).

**Gold** (`workspace.gold`) — 5 business-ready aggregate tables:
`driver_performance`, `route_profitability`, `truck_utilization`,
`customer_analysis`, `safety_metrics`.

## Files
| File | Purpose |
|---|---|
| `01_bronze_ingestion.py` | PySpark: raw CSV → Bronze Delta tables |
| `02_silver_transformations.py` | PySpark: joins + cleaning → Silver |
| `03_gold_aggregations.py` | PySpark: business aggregations → Gold |
| `04_dashboard_queries.sql` | SQL powering the 5 dashboard visuals |

## Dashboard
Built in Databricks Lakeview — 5 visuals:
1. Top 10 drivers by revenue/mile
2. Route profitability (top 15 lanes)
3. Fleet utilization (bottom 15 trucks by miles)
4. Customer revenue concentration by type
5. Safety incidents — total vs at-fault

## Key technical decisions
- Rate metrics use `sum(revenue) / sum(miles)`, not average of
  pre-computed ratios — avoids statistical distortion (Simpson's
  paradox-style error).
- Raw files ingested via Unity Catalog Volumes
  (`/Volumes/workspace/default/logistics_ops_raw`), not legacy DBFS.
- `_metadata.file_path` used instead of `input_file_name()`, which
  Unity Catalog does not support.

## Key findings
- **Miami → Seattle** is the longest route (3,229 mi) but has the
  lowest base rate per mile ($1.65) — a pricing review candidate.
- Fleet utilization is highly consistent across trucks (~1.2M miles
  each), suggesting balanced load distribution rather than underused
  assets.
- Driver DRV00058 has 9 total safety incidents with 2 at-fault — the
  highest incident count in the fleet, worth investigating root cause
  vs. at-fault ratio.
- Revenue per mile is tightly clustered across top drivers
  (~$2.18–2.21), indicating consistent performance rather than
  standout outliers.

## Known limitations
- No filtering applied on `trip_status` — all trips are included
  regardless of completion status.
- No explicit null/duplicate-key validation step was run against the
  source data.
- A second Kaggle dataset (global supply chain risk) was evaluated but
  excluded from the final pipeline — it shares no keys or grain with
  the logistics-ops data, so integrating it would have been forced
  rather than meaningful.
