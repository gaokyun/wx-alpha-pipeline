# wx-alpha-pipeline

End-to-end weather data pipeline ingesting ECMWF and GFS forecast models, orchestrated with Apache Airflow, transformed with dbt, and stored in **Oracle Autonomous Data Warehouse (ADW)**, **MySQL HeatWave**, and **PostgreSQL**, using **OCI Object Storage** (S3-compatible API) as the raw Delta Lake storage layer.

---

# WeatherOps Pipeline (Airflow + OCI Delta Lake + ADW + MySQL HeatWave + Postgres + dbt)

## Overview

This repository contains a **production-grade meteorological data pipeline** built with **Apache Airflow**, designed to ingest global numerical weather prediction datasets from:

- **NOAA GFS** (Global Forecast System)
- **ECMWF IFS** (Integrated Forecasting System)
- **ECMWF AIFS** (Artificial Intelligence Forecast System)
- **ECMWF ensemble spread products**

The pipeline automatically:
1. Detects forecast availability via sensors.
2. Downloads target forecast steps (192h, 240h, 288h, 360h).
3. Stores raw data in **OCI Object Storage** formatted as **Delta Lake**.
4. Synchronizes and bootstraps target data warehouses.
5. Executes **dbt staging and mart models** for multiple database engines.
6. Publishes unified, analytics-ready consensus forecast datasets (Gold Layer).
7. Runs governance verification checks on the output datasets.

---

# Architecture

```
                 Weather Models (NOAA NOMADS / ECMWF Open Data)
                                       ↓
                Availability Sensors (Reschedule Mode / Dynamic Poke)
                                       ↓
                 Dynamic Ingestion DAGs (weather_ops.extract.*)
                                       ↓
                OCI Object Storage Delta Lake (oci-s3-ykg-storage)
                                       ↓
         ┌─────────────────────────────┼─────────────────────────────┐
         ↓                             ↓                             ↓
    Postgres (PDH)             Oracle ADW (adw_prod)       MySQL HeatWave (DMH)
   (Docker Operator)         (External Tables Sync)         (Local Attach/Sync)
         ↓                             ↓                             ↓
    dbt (tag:pdh)                 dbt (ADW Gold)               dbt (MySQL Gold)
         └─────────────────────────────┼─────────────────────────────┘
                                       ↓
                             Unified Gold Forecasts
                                       ↓
                        Governance & Dataset Verification
```

---

# Supported Weather Models

| Model | Layer / Product Types | Forecast Steps Extracted |
| :--- | :--- | :--- |
| **GFS** | Upper-air, Surface | 192h, 240h, 288h, 360h |
| **ECMWF IFS** | Upper-air, Surface, Spread | 192h, 240h, 288h, 360h |
| **ECMWF AIFS** | Upper-air, Surface, Spread | 192h, 240h, 288h, 360h |

These represent **long-range forecast horizons** commonly used in medium-range signal analysis, weather-driven trading, and commodity forecasting.

---

# Key Pipeline Features

## 1. Cycle-Aware Scheduling & Buffering
The pipeline automatically aligns execution with model release cycles and provides configurable buffer offsets to account for publication delay:

- **GFS**: 00z / 06z / 12z / 18z (Buffer offset: `4.67 hours`)
- **ECMWF IFS**: 00z / 12z (Buffer offset: `7.57 / 7.67 hours`)
- **ECMWF AIFS**: 00z / 06z / 12z / 18z (Buffer offset: `6.93 / 7.57 hours`)

This ensures Airflow triggers tasks only when the source data has been fully compiled by meteorological centers.

## 2. Dynamic Ingestion DAG Generation
Ingestion DAGs are dynamically registered in Airflow using the format:
```
weather_ops.extract.<model>.<layer>
```
Each DAG handles its own availability check, downloading GRIB2/data batches, and writing them to the partition directories on OCI.

## 3. Centralized Master Dashboard
The pipeline features a master control DAG:
`weather_ops.standardized_master_control`

This dashboard coordinates:
- **Data Completeness check**: Validates prior-day dataset readiness.
- **Maintenance**: Automatically allocates future Postgres partitions.
- **Triggering Extractions**: Sequentially triggers and waits for the 8 dynamic extraction DAGs.
- **Triggering Transformations**: Triggers child database transformations (Oracle ADW, MySQL HeatWave, PostgreSQL).
- **Consensus Refreshes**: Refreshes final Gold consensus models for each data engine.
- **Governance**: Executes the final verification tasks.

## 4. Multi-Warehouse dbt Support
The dbt project `physical_meteor` is compiled for multiple database configurations:
- **Oracle ADW (`adw_prod`)**: Integrates natively with DBMS_CLOUD external tables mapping to OCI Parquet directories.
- **PostgreSQL (`dev_postgres` / `dev_duckdb_postgres`)**: Configures Foreign Data Wrappers (FDW) inside Postgres to expose DuckDB tables.
- **MySQL HeatWave (`dev_duckdb_mysql`)**: Attaches and syncs tables using DuckDB's local attach extensions.

## 5. Event-Coalesced Ingestion (Asset Scheduling)
Uses Airflow `Asset` subscriptions to orchestrate downstream transforms. Downstream transforms trigger on updates to raw Delta Lake assets with a **5-minute debounce window** to coalesce multiple asset updates into a single run.

## 6. Resource-Aware Pool Controls
To prevent resource exhaustion and I/O locking, critical transformations are restricted to single-slot Airflow pools:
- `dph_single_writer` (Postgres / DuckDB)
- `dmh_single_writer` (MySQL / DuckDB)
- `adw_dbt_pool` (Oracle ADW)

---

# Example Generated DAG List

### Master Orchestrator
- `weather_ops.standardized_master_control`

### Dynamic Extraction DAGs
- `weather_ops.extract.gfs.upper`
- `weather_ops.extract.gfs.surface`
- `weather_ops.extract.aifs.upper`
- `weather_ops.extract.aifs.surface`
- `weather_ops.extract.aifs.spread`
- `weather_ops.extract.ifs.upper`
- `weather_ops.extract.ifs.surface`
- `weather_ops.extract.ifs.spread`

### Transformation DAGs
- `weather_ops.transform.all_models_dbt_duckdb` (DuckDB event-driven transform)
- `weather_ops.transform.pdh_dbt_postgres` (Postgres FDW transform)
- `weather_ops.transform.unified_forecast_refresh_mysql` (MySQL HeatWave transform)
- `weather_ops.transform.unified_forecast_refresh_adw` (Oracle ADW transform)

---

# Environment Variables

The project loads configurations dynamically from `.env` files. Important environment variables include:

- `OCI_OBJECT_STORAGE_BUCKET`: Target OCI storage bucket (`oci-s3-ykg-storage`).
- `OCI_OBJECT_STORAGE_ACCESS_KEY` & `OCI_OBJECT_STORAGE_SECRET_KEY`: Credentials for accessing OCI Object Storage compat endpoint.
- `POSTGRES_USERNAME` & `POSTGRES_PASS`: PostgreSQL credentials.
- `ORACLE_USER` & `ORACLE_PASSWORD`: Credentials for the Autonomous Data Warehouse (ADW).
- `MYSQL_HOST` & `ORACLE_PASSWORD`: MySQL server configuration (the MySQL server uses ORACLE_PASSWORD for consistency).
- `DBT_PROJECT_PATH` & `HOST_PROJECT_PATH`: Path definitions for the dbt project and Docker mounts.

---

# Governance & Verification
At the end of every master execution cycle, the `verify_gold_datasets()` helper runs. This verifies the row counts, schema completeness, and partition consistency across the gold tables, raising warnings or failing the task if any consensus forecast datasets are empty or corrupt.

---

# Author
**WeatherOps Data Platform**  
Airflow + OCI Delta Lake + ADW + MySQL HeatWave + Postgres + dbt meteorological ingestion framework.
