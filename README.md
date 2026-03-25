# wx-alpha-pipeline
End-to-end weather data pipeline ingesting ECMWF and GFS forecast models, orchestrated with Airflow, transformed with dbt, and stored in Snowflake (production) / PostgreSQL (development), with S3 as intermediate object storage.


# WeatherOps Pipeline (Airflow + Delta Lake + Snowflake + dbt)

## Overview

This repository contains a **production-grade meteorological data pipeline** built with **Apache Airflow**, designed to ingest global numerical weather prediction datasets from:

-   NOAA **GFS**
    
-   ECMWF **IFS**
    
-   ECMWF **AIFS**
    
-   ECMWF ensemble **spread products**
    

The pipeline automatically:

1.  Detects forecast availability
    
2.  Downloads target forecast steps
    
3.  Stores raw data in **S3 Delta Lake**
    
4.  Refreshes **Snowflake external tables**
    
5.  Executes **dbt staging models**
    
6.  Publishes curated analytics-ready datasets
    

Architecture goal:

> Reliable + cost-efficient + cycle-aware weather model ingestion pipeline

----------

# Architecture

```
Weather Models
    ↓
Availability Sensors (NOMADS / ECMWF Open Data)
    ↓
Extraction DAGs
    ↓
S3 Delta Lake (Raw Layer)
    ↓
Snowflake External Tables
    ↓
dbt Transformations (Silver / Gold)
    ↓
Analytics / Forecast Modeling / Trading Signals
```

----------

# Supported Weather Models

Model

Layer Types

GFS

upper-air

ECMWF AIFS

upper / surface / spread

ECMWF IFS

upper / surface / spread

Forecast steps extracted:

```
192h
240h
288h
360h
```

These represent **long-range forecast horizons** commonly used in medium-range signal analysis and weather-driven trading.

----------

# Key Pipeline Features

## Cycle-aware scheduling

The pipeline automatically aligns execution with model release cycles:

Model

Cycles

GFS

00z / 06z / 12z / 18z

ECMWF IFS

00z / 12z

ECMWF AIFS

00z / 06z / 12z / 18z

Each dataset uses a configurable **buffer window** to ensure files are complete before ingestion.

Example:

```
gfs-upper buffer = 4.67 hours
```

This ensures Airflow sensors only trigger when forecasts are fully available.

----------

# Smart Availability Sensors

The pipeline validates dataset readiness before execution.

### NOAA GFS Sensor

Checks NOMADS directory listing:

```
https://nomads.ncep.noaa.gov/
```

Looks for sentinel file:

```
gfs.t{cycle}z.pgrb2.0p25.f360
```

If present:

```
Dataset ready
```

Otherwise:

```
Sensor waits (reschedule mode)
```

----------

### ECMWF Sensor

Checks ECMWF Open Data index:

```
https://data.ecmwf.int/
```

Validates availability of:

```
360h-oper-fc.grib2
```

or

```
360h-enfo-fc.grib2
```

depending on dataset type.

----------

# Extraction DAG Generator

Extraction DAGs are **dynamically generated**:

```
weather_ops.extract.<model>.<layer>
```

Examples:

```
weather_ops.extract.gfs.upper
weather_ops.extract.aifs.surface
weather_ops.extract.ifs.spread
```

Each DAG executes:

```
Sensor
   ↓
Download Task
   ↓
Snowflake Metadata Refresh
```

----------

# Forecast Cycle Translator Logic

Airflow runs slightly after model release.

Helper function:

```
get_cycle_and_date()
```

Automatically maps runtime back to:

```
actual forecast cycle origin
```

Example:

```
Airflow trigger = 10:40 UTC
buffer = 4.67 hours
→ maps to 06z cycle
```

This ensures correct dataset alignment.

----------

# Storage Layer (S3 Delta Lake)

Raw meteorological datasets are stored as:

```
s3://<bucket>/weather_data/delta_lake/
```

Partition strategy:

```
forecast_reference_time
forecast_cycle
```

Benefits:

-   ACID safety
    
-   incremental ingestion
    
-   reproducibility
    
-   time-travel debugging
    
-   schema evolution support
    

----------

# Snowflake External Table Sync Strategy

Pipeline uses **hybrid refresh mode** to reduce compute cost.

## Real-time mode (12z cycle)

Immediately refresh external tables:

```
ALTER EXTERNAL TABLE ... REFRESH
```

Purpose:

```
fast analytics availability
```

----------

## Batch mode (non-12z cycles)

Refresh deferred to transformation DAG:

```
cost optimized warehouse usage
```

Result:

```
lower Snowflake spend
```

----------

# Asset-driven DAG Dependencies

Extraction publishes assets:

```
s3://bucket/weather_data/delta_lake/...
```

Transformation DAGs subscribe to them.

Example:

```
schedule=[ASSETS['gfs-upper']]
```

This enables:

```
event-driven orchestration
```

instead of cron chaining.

----------

# Transformation Layer (dbt + Snowflake)

Two transformation pipelines:

## GFS Pipeline

```
weather_ops.transform.gfs_dbt_snowflake
```

Steps:

```
Batch refresh metadata
    ↓
Run dbt staging models
```

Executes:

```
models/staging/gfs+
```

----------

## ECMWF Pipeline

```
weather_ops.transform.ecmwf_dbt_snowflake
```

Triggered after:

```
AIFS upper
+
AIFS surface
```

available.

Executes:

```
models/staging/ecmwf+
```

----------

# Cost Optimization Strategy

Warehouse compute minimized using:

### selective refresh policy

Cycle

Behavior

12z

immediate refresh

others

deferred batch refresh

Impact:

```
fewer warehouse spin-ups
lower Snowflake cost
faster critical-cycle delivery
```

----------

# Airflow Sensor Mode

Sensors run in:

```
mode="reschedule"
```

Benefits:

-   frees worker slots
    
-   scalable scheduling
    
-   production-safe polling
    

----------

# Environment Variables

Required configuration:

```
AWS_S3_BUCKET
snowflake_default (Airflow connection)
```

Defaults:

```
amzn-s3-ykg-storage
```

----------

# Example Generated DAG List

Extraction DAGs:

```
weather_ops.extract.gfs.upper
weather_ops.extract.aifs.upper
weather_ops.extract.aifs.surface
weather_ops.extract.aifs.spread
weather_ops.extract.ifs.upper
weather_ops.extract.ifs.surface
weather_ops.extract.ifs.spread
```

Transformation DAGs:

```
weather_ops.transform.gfs_dbt_snowflake
weather_ops.transform.ecmwf_dbt_snowflake
```

----------

# Reliability Features

Pipeline guarantees:

✅ cycle alignment  
✅ dataset completeness validation  
✅ incremental ingestion  
✅ asset-based orchestration  
✅ cost-aware Snowflake refresh  
✅ automated dbt staging execution

----------

# Intended Use Cases

Designed for:

-   meteorological analytics platforms
    
-   energy trading signal pipelines
    
-   weather-driven commodity forecasting
    
-   climate data warehouses
    
-   research-grade forecast ingestion systems
    

----------

# Future Extensions

Recommended roadmap:

```
Add ensemble mean ingestion
Add probabilistic forecast tables
Add forecast verification metrics
Add anomaly detection monitoring
Add partition auto-compaction
```

----------

# Author

WeatherOps Data Platform  
Airflow + Delta Lake + Snowflake + dbt meteorological ingestion framework
