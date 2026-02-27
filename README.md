# Build_A_Data_Warehouse_ETL_Pipeline

This repository contains a **SQL-based ETL pipeline** that processes raw data into **clean, optimized, and analytics-ready datasets** using a **multi-layered (Bronze / Silver / Gold) architecture**.
- ✅ Handles **60,000+ records**
- ✅ Works across **6+ relational tables**
- ✅ Uses **Schemas, Stored Procedures, and CTEs**
- ✅ Designed for **scalable analytics and reporting**

## 🚀 Project Overview
ll
The goal of this project is to:

1. **Ingest** raw data into the database (Bronze layer).
2. **Clean, transform, and standardize** data (Silver layer).  
3. **Model optimized tables for reporting and analysis** (Gold Layer).

This architecture follows modern data engineering best practices, ensuring maintainability, traceability, and performance


## 🏗️ Architecture: Bronze / Silver / Gold

The pipeline uses three logical schemas:

- **`bronze schema`** – Raw or minimally processed data.
- **`silver schema`** – Clean, standardized, relational data.
- **`gold schema`** – Business-ready analytical datasets.

## **📂 Schema & Table Design**
## 🥉 Bronze Schema (bronze)

Raw ingestion layer; preserves original structure.
Examples:

- **`bronze.crm_cust_info`**

- **`bronze.crm_prd_info`**

- **`bronze.crm_sales_details`**

- **`bronze.erp_cust_az12`**

- **`bronze.erp_loc_a101`**

- **`bronze.erp_px_cat_g1v2`**


## Characteristics:
- Load all data into table format

- Minimal transformations

- Load timestamps.

- Source-of-truth for auditing

## **🥈 Silver Schema (silver)**

Cleaned, standardized, Optimizing, relational-modeled layer.

## Examples:

- **`silver.crm_cust_info_s`**

- **`silver.crm_prd_info`** 

- **`silver.crm_sales_info`**

- **`silver.erp_cust_az12`**

- **`silver.erp_loc_a101`**

- **`silver.erp_px_cat_g1v1`**

## **Characteristics:**

- Deduplication

- Fix NULLs & data types

- Referential integrity

- Uniform date/time formats

- Validating the numeric columns

## **🥇 Gold Schema (gold)**

Analytics-ready tables(store procedure) for BI and reporting for furthur analysis.

# Examples:

- **`gold.dim_customers`**

- **`gold.dim_products`**

- **`gold.fact_sales`**

## Characteristics:

- Fact/dimension modeling

- Denormalized for performance

- Optimized for dashboard queries


## **⚙️ ETL Logic (Stored Procedures + CTEs)**

This project uses Stored Procedures to orchestrate ETL and CTEs to prepare consistent transformations and VIEWs for retrive only the for data privacy.

## Example Transformation Procedure
####
CREATE OR ALTER PROCEDURE silver.sp_transform_orders
AS
BEGIN
    SET NOCOUNT ON

    WITH cleaned_orders AS (
        SELECT
            CAST(order_id AS INT) AS order_id,
            CAST(customer_id AS INT) AS customer_id,
            CAST(order_date AS DATE) AS order_date,
            CAST(product_id AS INT) AS product_id,
            CAST(quantity AS INT) AS quantity,
            CAST(total_amount AS DECIMAL(18,2)) AS total_amount,
            load_ts
        FROM bronze.orders_raw
        WHERE order_id IS NOT NULL
    ),
    deduped_orders AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY load_ts DESC) AS rn
        FROM cleaned_orders
    )

    INSERT INTO silver.orders (order_id, customer_id, order_date, product_id, quantity, total_amount, load_ts)
    SELECT order_id, customer_id, order_date, product_id, quantity, total_amount, load_ts
    FROM deduped_orders
    WHERE rn = 1
END;
GO

## 📁 Recommended Repository Structure

├── README.md
├── /sql
│   ├── 01_schemas.sql
│   ├── 02_bronze_tables.sql
│   ├── 03_silver_tables.sql
│   ├── 04_gold_tables.sql
│   ├── 05_stored_procedures.sql
│   ├── 06_views.sql
│   └── 07_seed_data.sql
└── /docs
    └── data_dictionary.md


## **🔄 ETL Workflow**
## 1️⃣ Create Schemas
####
- CREATE SCHEMA bronze;
- CREATE SCHEMA silver;
- CREATE SCHEMA gold;

## 2️⃣ Load Data → Bronze Layer
####
Raw CSV/external tables loaded directly into `bronze.*_raw`.

## 3️⃣ Transform → Silver Layer
####
EXEC silver.sp_transform_bronze_to_silver;

## 📊 Example Query (Gold Layer)
####
SELECT
    ds.store_name,
    dp.product_name,
    fs.sales_date,
    fs.quantity,
    fs.total_sales
FROM gold.fact_sales fs
JOIN gold.dim_store ds ON fs.store_key = ds.store_key
JOIN gold.dim_product dp ON fs.product_key = dp.product_key;


## **🧰 Technology Stack**

- SQL Server

- Stored Procedures

- CTE-based transformations

- Dimensional modeling (Star Schema)

## ⚡ Performance & Scalability

- Efficient transformations for 60,000+ records

- Indexed keys for optimized joins

- Use variables for more profesional and optimized dataset

- Layered architecture ensures reusability and low maintenance

- Modular Stored Procedures for incremental loads

## 🤝 Contributing

Contributions are welcome!

- **1.** Fork this repo

- **2.** Create a feature branch

- **3.** Commit your changes

- **4.** Submit a Pull Request

## **📄 License**

- No license is granted for this project.

- All rights to the source code and data are reserved by the author. 
You may view the contents of this repository, but you may not reuse, copy, modify, or distribute the code or data without explicit permission.

## **✨ Final Notes**

This ETL pipeline is built to demonstrate clean SQL engineering practices, and showcases:

- Multi-layer ETL architecture

- Readable & maintainable SQL transformations

- Professional project structure


# ** Contact Informations**
- Linkedin : https://www.linkedin.com/in/akash-m0ndal/
- email : heyakashmondal.analyst@gmail.com
- Contact No. : 9083666706















