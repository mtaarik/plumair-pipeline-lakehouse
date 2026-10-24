# **Project Naming Conventions**

This document outlines the naming conventions used across the entire data platform, including infrastructure, streaming, storage, and transformation layers. Every example below is an object that actually exists in the project.

## **Table of Contents**

1. [General Principles](#general-principles)
2. [Infrastructure & Streaming](#infrastructure--streaming)
3. [Data Lakehouse Models (dbt & Iceberg)](#data-lakehouse-models-dbt--iceberg)
4. [Column & IoT Specific Conventions](#column--iot-specific-conventions)

---

## **1. General Principles**

* **Language**: English is mandatory for all code, tables, columns, and infrastructure components.
* **Case Sensitivity**:
    * **`snake_case`**: Exclusively for Data objects (Schemas, Tables, Columns, dbt models).
    * **`kebab-case`**: Exclusively for Infrastructure objects (Docker containers, Networks, Kafka topics).
* **Avoid Reserved Words**: Do not use standard SQL or Python reserved keywords (e.g., `select`, `date`, `time`, `value`).

---

## **2. Infrastructure & Streaming**

### **Docker Components**
* **Pattern**: `<project>-<service>`
* **Examples**: `plumair-kafka-broker`, `plumair-trino`, `plumair-nifi`, `plumair-ops-db`, `plumair-airflow-scheduler`
* **Network**: `ocp-data-net` (single bridge network, services reach each other by DNS name, e.g. `minio:9000`).

### **Kafka Topics**
* **Pattern**: `<domain>-<entity>-<status>`
* **Rules**:
    * `<domain>`: The business domain (e.g., `iot`).
    * `<entity>`: The object generating the event (e.g., `sensors`, `emissions`).
    * `<status>`: The state of the data (e.g., `raw`, `alert`).
    * Topics are created explicitly (service `kafka-init`) with **1 partition**: offsets are then a total order, which the incremental Bronze cursor relies on.
* **Examples**:
    * `iot-sensors-raw` (all hourly sensor telemetry)
    * `iot-emissions-alert` (real-time threshold breaches, PM2.5 > 155.1 µg/m³)

### **NiFi Processors**
* **Pattern**: `<Verb>_<Object>` in PascalCase words separated by underscores.
* **Examples**: `Read_IoT_Sensors`, `Split_Into_Chunks`, `Throttle_Sensor_Data`, `Add_Ingestion_Timestamp`, `Publish_Alerts`, `Store_Alert_PostgreSQL`

---

## **3. Data Lakehouse Models (dbt & Iceberg)**

The data architecture follows the Medallion architecture (Bronze, Silver, Gold), orchestrated via dbt. Each layer has its own physical schema in the Iceberg catalog (`iceberg.bronze`, `iceberg.silver`, `iceberg.gold`), enforced by the `generate_schema_name` macro.

### **Bronze Layer (Raw Data)**
* **Schema**: `bronze`
* **Pattern**: `bronze_<entity>_raw`
* **Rules**: Data is appended exactly as received from Kafka (JSON payload kept as text). No renaming or casting of business fields.
* **Examples**: `bronze_stations_raw`, `bronze_alerts_raw`

### **Silver Layer (Cleaned & Conformed)**
* **Schema**: `silver`
* **Pattern**: `silver_<entity>`
* **Rules**: Normalized (3NF) source of truth: typing, deduplication, quality flags. **No dimensional vocabulary**: `dim_` / `fact_` prefixes are forbidden here. Tables are either reference (master data) or transactional.
* **Examples**: `silver_station` (reference), `silver_telemetry` (transactional, grain station × hour)

### **Gold Layer (Aggregated & Business-Ready)**
* **Schema**: `gold`
* **Pattern**: `gold_<category>_<entity>`
* **Rules**: Star schema optimized for Power BI. The `dim_` and `fact_` prefixes are **reserved to this layer**.
* **Categories**:
    * `dim_`: Dimension tables (e.g., `gold_dim_station`, `gold_dim_date`, `gold_dim_aqi_category`).
    * `fact_`: Fact tables at the finest grain (e.g., `gold_fact_air_quality`, `gold_fact_alerts`, `gold_fact_compliance`).
    * `kpi_`: Aggregated monthly marts (e.g., `gold_kpi_compliance_rate_monthly`).
    * `ml_`: Denormalized views for data science (e.g., `gold_ml_features`).

---

## **4. Column & IoT Specific Conventions**

### **Time and Dates (Critical for IoT)**
* **Dates**: Must end with `_date` or `_id` when used as a dimension key (e.g., `date_id`).
* **Timestamps**: Must end with `_at` or `_ts` and are stored in UTC.
    * Examples: `triggered_at`, `kafka_ingestion_ts`, `meta_ingested_at`
    * Known exceptions kept for compatibility with the Power BI model: `measurement_time`, `ingestion_timestamp`.

### **Measures and Metrics**
* Always include the unit of measurement as a suffix for clarity.
* **Pattern**: `<metric>_<unit>`
* **Examples**: `pm25_ugm3`, `no2_ugm3`, `temperature_c`, `pressure_msl_hpa`, `wind_speed_kmh`

### **Booleans and Flags**
* Must be prefixed with `is_` / `has_`, or suffixed with `_is_<state>` for data quality flags.
* **Examples**: `is_current`, `is_weekend`, `temperature_is_outlier`, `pm25_is_outlier`

### **Primary, Surrogate, and Foreign Keys**
* **Primary Keys**: Must end with `_id` (e.g., `station_id`, `alert_id`).
* **Surrogate Keys**: Must end with `_sk` (e.g., `station_sk` in the SCD Type 2 dimension).
* **Foreign Keys**: Must match the primary key name of the target table.

### **Metadata Columns**
* Technical columns generated by the pipeline must be prefixed with `_meta_` and are hidden from business users.
* **Examples**:
    * `_meta_ingested_at` (processing time stamped by NiFi after the rate control).
    * `_meta_kafka_ingestion_ts` (time the message reached Kafka, used as incremental watermark).
    * `_loaded_at` (time the row was written in Gold).
