/*
SCRIPT PURPOSE: 
		- Fix the data quality.
		- Retriving and creating only the essential columns.
		- Data Cleaning and validating.
		- Create the new tables.
		- Insert into the new values after cleaing, find essential data for Business KPIs.
*/






-- creating a stored procedure with the below query for futhur need
CREATE OR ALTER PROCEDURE silver.load_silver AS 

BEGIN



	-- ** Clean the next table crm_cust_info and store to the silver.crm_cust_info **
	DROP TABLE silver.crm_cust_info_s;
	--create a table named silver.crm_cust_info for insert the cleaned values from the crm_cust_info
	CREATE TABLE silver.crm_cust_info_s(
		cst_id INT,
		cst_key VARCHAR(50),
		cst_create_date VARCHAR(50),
		cst_first_name VARCHAR(50),
		cst_last_name VARCHAR(50), 
		cst_gndr VARCHAR(10),
		cst_material_status VARCHAR(50)
	 
	) ;

	PRINT '>> Truncating the table..';
	TRUNCATE TABLE silver.crm_cust_info_s;
	-- retrieve the multiple non-updated values and kept the last updated values
	INSERT INTO silver.crm_cust_info_s(
		cst_id ,
		cst_key,
		cst_create_date,
		cst_first_name,
		cst_last_name,
		cst_gndr,
		cst_material_status )

	SELECT 
		cst_id,
		cst_key,
		cst_create_date,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE UPPER(TRIM(cst_gndr)) 
			 WHEN 'F' THEN 'Female'
			 WHEN 'M' THEN 'Male'
			 ELSE 'N/A'
		END cst_gndr,
		CASE UPPER(TRIM(cst_marital_status))
			WHEN 'M' THEN 'Married'
			WHEN 'S' THEN 'Single'
		END cst_material_status

	FROM 
	(
		SELECT 
		* ,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info_s
		WHERE cst_id IS NOT NULL
	)t
	WHERE flag_last = 1;


	


	-- ======================================================
	-- ======================================================

	-- ** Clean the next table crm_prd_info and store to the silver.crm_prd_info **
	DROP TABLE silver.crm_prd_info;

	CREATE TABLE silver.crm_prd_info(
		prd_id			INT,
		cat_id			VARCHAR(50),
		prd_key			VARCHAR(50),
		prd_nm			VARCHAR(50),
		prd_cost		INT ,
		prd_line		VARCHAR(40),
		pred_start_dt	DATE,
		prd_end_dt		DATE,
		dwh_create_date DATETIME DEFAULT GETDATE()
	);


	PRINT '>> Truncating the table..';
	TRUNCATE TABLE silver.crm_prd_info;
	-- insert the values from crm_prd_info to  silver.crm_prd_info
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		pred_start_dt,
		prd_end_dt
	)

	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		COALESCE(prd_cost,0) as prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
		END prd_line,
		CAST(prd_start_dt AS DATE) AS pred_start_dt,
		CAST (DATEADD(day,-1,(LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))) AS DATE) AS prd_end_dt
		
	FROM bronze.crm_prd_info;


	


	-- =============================================
	-- =============================================

	-- ** Cleaning the crm_sales_details **
	DROP TABLE silver.crm_sales_details;

	CREATE TABLE silver.crm_sales_details(

			sls_ord_num			VARCHAR(50),
			sls_prd_key			VARCHAR(50),
			sls_cust_id			VARCHAR(50),
			sls_order_dt		DATE,
			sls_ship_dt			DATE,
			sls_due_dt			DATE,
			sls_sales			INT,
			sls_quantity		INT,
			sls_price			INT,
			dwh_create_date		DATETIME DEFAULT GETDATE()
	)



	PRINT '>> Truncating the table..';
	TRUNCATE TABLE silver.crm_sales_details;
	-- inserting date into new table from cleaned data from crm_sales_details
 
	INSERT INTO silver.crm_sales_details
	(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)

	SELECT 
		 sls_ord_num,
		 sls_prd_key,
		 sls_cust_id,
		 CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE )
		 END sls_order_dt,
		 CASE 
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE )
		 END sls_ship_dt,
		 CASE 
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE )
		 END sls_due_dt,
	 
		 CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			  THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales 
			END sls_sales,
	 
		 ABS(sls_quantity) AS sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 
			THEN  ABS(sls_sales)/ NULLIF(ABS(sls_quantity), 0)
		ELSE sls_price
		END sls_price
	FROM bronze.crm_sales_details

	

	-- ================================================
	-- ================================================

	-- ** Cleaning the erp_cust_az12 table ** 

	DROP TABLE silver.erp_cust_az12;
	-- create a new table named silver.erp_cust_az12 and insert the values from erp_cust_az12
	CREATE TABLE silver.erp_cust_az12(
		CID		VARCHAR(100),
		BDATE	DATE,
		GEN		VARCHAR(50)

	)


	PRINT '>> Truncating the table..';
	TRUNCATE TABLE silver.erp_cust_az12;
	-- insert the cleaned values from erp_cust_az12 to silver.erp_cust_az12
	INSERT INTO silver.erp_cust_az12
	(	 CID,
		 BDATE,
		 GEN
	)

	SELECT 
		SUBSTRING(CID,4,LEN(CID)) AS CID,
		CASE 
			WHEN BDATE > GETDATE() THEN NULL
		ELSE BDATE
		END BDATE, -- set invalid birthdates into NULL
	
		CASE 
			WHEN UPPER(TRIM(GEN)) LIKE 'M%' THEN 'Male'
			WHEN UPPER(TRIM(GEN)) LIKE 'F%' THEN 'Female'
		ELSE 'N/A'
		END GEN  -- Normalize gender values and handle unknown cases
	FROM bronze.erp_cust_az12;

	
	-- =============================================
	-- =============================================

	-- **  clean and load the erp_loc_a101 table to the silver.erp_loc_a101  **
	 
	 DROP TABLE silver.erp_loc_a101;
	-- create a new table named silver.erp_loc_a101 for furthur analysis
	CREATE TABLE silver.erp_loc_a101
	(
		CID					VARCHAR(50),
		CNTRY				VARCHAR(50),
		dwh_create_date		DATETIME DEFAULT GETDATE()
	)


	PRINT '>> Truncating the table..';
	TRUNCATE TABLE silver.erp_loc_a101;
	-- insert cleaned data from erp_loc_a101 to silver.erp_loc_a101
	INSERT INTO silver.erp_loc_a101
	( 
		CID,
		CNTRY
	)

	SELECT 
		REPLACE(CID,'-','') AS CID,   --replace the value for the foreign key 
		CASE 
			WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(CNTRY) IN ('US', 'USA' ) THEN 'United States of America'
			WHEN TRIM(CNTRY) IS NULL OR TRIM(CNTRY) IN('') THEN 'N/A'
		ELSE TRIM(CNTRY)
		END CNTRY
	FROM bronze.erp_loc_a101;

	


	-- ============================================
	-- ============================================


	-- ** erp_px_cat_g1v1 ** 

	PRINT '>> Deleting the whole table....';
	DROP TABLE silver.erp_px_cat_g1v1;
	--	create a table named silver.erp_px_cat_g1v1 for insert the cleaned data from the erp_px_cat_g1v1
	PRINT 'Create a table named silver.erp_px_cat_g1v1';
	CREATE TABLE silver.erp_px_cat_g1v1
	(
		ID					VARCHAR(50),
		CAT					VARCHAR(50),
		SUBCAT				VARCHAR(50),
		MAINTAINANCE		VARCHAR(50)

	)


	PRINT '>> Truncating the table..';
	TRUNCATE TABLE silver.erp_px_cat_g1v1;
	-- insert cleaned data from erp_px_cat_g1v1 to silver.erp_px_cat_g1v1
	INSERT INTO silver.erp_px_cat_g1v1
	( 
		ID,
		CAT,
		SUBCAT,
		MAINTAINANCE
	)
	-- cleaning and normalize the erp_px_cat_g1v1 table
	SELECT

		TRIM(ID) AS ID,
		TRIM(CAT) AS CAT,
		SUBCAT,
		TRIM(MAINTAINANCE) AS MAINTAINANCE   --clean the column if there have any unwanted spaces
	FROM bronze.erp_px_cat_g1v2;


END



-- ts 26.39.54

