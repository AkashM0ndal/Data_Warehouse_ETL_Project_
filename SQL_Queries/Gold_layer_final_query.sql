drop view gold.dim_customers;
-- create a view named gold.dim_customers for furthur analysis with clean and good quality data
CREATE VIEW gold.dim_customers AS

SELECT 
	ROW_NUMBER () OVER (ORDER BY ci.cst_id) AS customers_id,
	ci.cst_id  AS customer_id,
	ci.cst_key  AS customer_number,
	ci.cst_first_name AS first_name,
	ci.cst_last_name  AS last_name,
	CASE WHEN UPPER(TRIM(ci.cst_gndr)) = 'FEMALE' OR UPPER(TRIM(ci.cst_gndr)) = 'MALE'
		THEN ci.cst_gndr   -- CRM is the Master for gender info
	ELSE COALESCE(ca.GEN, 'N/A')
	END AS gender,
	la.CNTRY  AS country,
	ci.cst_material_status  AS marital_status,
	ci.cst_create_date AS create_date,	
	ca.BDATE  AS birthdate	
FROM silver.crm_cust_info_s AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON		ci.cst_key = ca.CID
LEFT JOIN  silver.erp_loc_a101 AS la
ON		ci.cst_key = la.CID


--  =====================================================================
--  =====================================================================
-- Drop the existing view for more dynamic

DROP VIEW gold.dim_products;

-- create a view named gold.dim_products for furthur analysis with neat and clean and good quality data
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY pn.pred_start_dt,pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.cat_id AS  category_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.pred_start_dt AS start_date,
	pc.CAT AS category,
	pc.SUBCAT sub_category,
	pc.MAINTAINANCE
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v1 AS pc
ON pn.cat_id = pc.ID
WHERE pn.prd_end_dt IS NULL  -- filter out all historical data




-- ==========================================================================
-- ==========================================================================

-- Drop the existing view for more dynamic

DROP VIEW gold.fact_sales;

-- create a view named gold.fact_details for furthur analysis with neat and clean and good quality data

CREATE VIEW gold.fact_sales AS 
SELECT 
	sd.sls_ord_num AS order_number,
	sd.sls_prd_key,
	pr.product_key,
	cu.customer_id,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price

FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id



	




