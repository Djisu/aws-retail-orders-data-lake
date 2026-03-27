# AWS Retail Orders Data Lake

A beginner-friendly data engineering project that simulates an end-to-end retail orders data pipeline using **Apache NiFi concepts**, **PySpark transformations**, and a **simple data lake folder structure**.

This project demonstrates how raw CSV order data can be ingested, validated, transformed, and written into curated and rejected zones — a foundational pattern used in modern data engineering pipelines.

---

## Project Overview

The purpose of this project is to practice core data engineering concepts by building a small local data lake pipeline.

### What this project demonstrates

- Ingesting retail order data files
- Organizing data into **raw**, **curated**, and **rejected** zones
- Transforming data with **PySpark**
- Applying basic **data quality validation**
- Handling invalid records separately for audit/troubleshooting
- Writing curated data in **Parquet** format
- Partitioning output data by `order_date`
- Using **Git & GitHub** to manage and document the project

---

## Architecture / Data Flow

**Source CSV files**  
⬇  
**Apache NiFi ingestion (concept / local simulation)**  
⬇  
**Raw zone (`data/raw/`)**  
⬇  
**PySpark transformation & validation**  
⬇  
**Curated zone (`data/curated/`)** + **Rejected zone (`data/rejected/`)**

---

## Tools & Technologies

- **Python 3**
- **PySpark**
- **Apache NiFi**
- **Git & GitHub**
- **macOS Terminal**

---

## Project Objectives

- Simulate a simple data engineering ingestion pipeline
- Practice local data lake organization
- Build a PySpark transformation script for retail orders
- Separate valid and invalid records
- Store curated output in a more analytics-friendly format (**Parquet**)
- Create a clean, documented GitHub portfolio project

---

## Data Lake Zones Used

This project uses a simple 3-zone structure:

- **Raw Zone (`data/raw/`)**
  - Stores incoming source CSV files before transformation

- **Curated Zone (`data/curated/`)**
  - Stores cleaned and validated output in **Parquet**
  - Partitioned by `order_date`

- **Rejected Zone (`data/rejected/`)**
  - Stores records that fail validation checks
  - Includes a rejection reason for troubleshooting and auditability

---

## PySpark Transformation Logic

The PySpark pipeline performs the following steps:

1. Reads CSV files from the **raw zone**
2. Trims whitespace from selected string columns
3. Validates records:
   - Rejects rows with null `order_id`
   - Rejects rows where `quantity <= 0`
   - Rejects rows where `unit_price <= 0`
4. Separates valid and rejected records
5. Deduplicates valid records by `order_id`
6. Creates a derived column:
   - `total_amount = quantity * unit_price`
7. Writes:
   - Valid records to **Parquet** in the curated zone
   - Rejected records to **CSV** in the rejected zone
8. Prints simple pipeline metrics

---

## Project Structure

```text
aws-retail-orders-data-lake/
├── data/
│   ├── raw/                     # Incoming raw CSV files
│   ├── curated/                 # Cleaned Parquet output (partitioned)
│   └── rejected/                # Invalid/rejected records with reasons
├── docs/                        # Project notes / documentation
├── nifi/                        # NiFi templates / flow-related assets
├── notes/                       # Personal implementation notes
├── sample-data/                 # Example input files for testing/demo
├── screenshots/                 # Screenshots for GitHub documentation
├── src/
│   └── transform/
│       └── transform_orders_pyspark.py
├── tests/                       # Future test scripts / validation checks
├── .gitignore
├── requirements.txt
└── README.md
