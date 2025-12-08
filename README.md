# Building_Data_Warehouse

This repository contains a **SQL-based ETL pipeline** that processes raw data into **clean, optimized, and analytics-ready datasets** using a **multi-layered (Bronze / Silver / Gold) architecture**.

- ✅ Handles **60,000+ records**
- ✅ Works across **6+ relational tables**
- ✅ Uses **Schemas, Stored Procedures, and CTEs**
- ✅ Designed for **scalable analytics and reporting**

---

## 🚀 Project Overview

The goal of this project is to:

1. **Ingest** raw data into the database (Bronze layer).  
2. **Clean, transform, and standardize** data (Silver layer).  
3. **Model optimized tables for reporting and analysis** (Gold layer).  

This architecture follows modern data engineering best practices, ensuring maintainability, traceability, and performance.

---

## 🏗️ Architecture: Bronze / Silver / Gold

The pipeline uses three logical schemas:

- **`bronze schema`** – Raw or minimally processed data.
- **`silver schema`** – Clean, standardized, relational data.
- **`gold schema`** – Business-ready analytical datasets.

## **📂 Schema & Table Design**
## 🥉 Bronze Schema (bronze)

Raw ingestion layer; preserves original structure.

Examples:

- **`bronze.customers_raw`**

- **`bronze.orders_ra`**

- **`bronze.products_raw`**

- **`bronze.payments_raw`**

- **`bronze.stores_raw`**

- **`bronze.shipments_raw`**


## Characteristics:

- Minimal transformations

- Load timestamps

- Source-of-truth for auditing

## **🥈 Silver Schema (silver)**

Cleaned, standardized, relational-modeled layer.

## Examples:

- **`silver.customers`**

- **`silver.orders`** 

- **`silver.products`**

- **`silver.payments`**

- **`silver.stores`**

- **`silver.shipments`**

## **Characteristics:**

- Deduplication

- Fix NULLs & data types

- Referential integrity

- Uniform date/time formats


