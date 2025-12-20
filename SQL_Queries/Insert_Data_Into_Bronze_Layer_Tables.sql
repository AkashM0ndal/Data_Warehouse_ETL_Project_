/*
--- ** QUICK SUMMARY ** ---

=============================================================================
CREATE A STORE PROCEDURE TO LOAD THE NEW DATA FOR THE ETL PROCESS
=============================================================================
-> All the datasets are in the '.csv' file. This is more efficient way to insert the data into the SQL Server.

Script Purpose: 
			- delete the existing data in the tables and load the new data.
			- create a store procedure to run the whole query very efficiently. 

*/



-- create a stored procedure for furthur help load data into Bronze layer Tables
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN


-- Truncate Table to work the query more dynamically
PRINT '==========================================';
PRINT '>> Truncating bronze.crm_cust_info_s Table';
PRINT '===========================================';
TRUNCATE TABLE bronze.crm_cust_info_s;
PRINT '================================';
PRINT '>>> Table Truncated Successfully';
PRINT '================================';
-- inserting date into bronze.crm_cust_info_s Table
PRINT '============================================================';
PRINT '>> Inserting Data inserted into bronze.crm_cust_info_s Table';
PRINT '============================================================';
BULK INSERT bronze.crm_cust_info_s
FROM 'C:\Users\am647\Desktop\Data With Baraa SQL Data Analysis Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
PRINT '================================================================';
PRINT '>>> Successfully Data inserted into bronze.crm_cust_info_s Table';
PRINT '================================================================';


-- ==================================================================================================
-- ==================================================================================================

PRINT '================================================';
PRINT 'Truncating bronze.crm_prd_info Table';
PRINT '================================================';
-- Truncateing the previous data for more dynamic
TRUNCATE TABLE bronze.crm_prd_info;
PRINT '================================================';
PRINT '>>> Successfully Table Truncated';
PRINT '================================================';
PRINT '>> Inserting Data into bronze.crm_prd_info Table';
PRINT '================================================';
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\am647\Desktop\Data With Baraa SQL Data Analysis Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK

);

PRINT '===========================================';
PRINT '>>> Successfully Data Inserted into Table..';
PRINT '===========================================';




-- ========================================================================================
-- ========================================================================================

PRINT '===============================================================';
PRINT '>> Truncating bronze.crm_sales_details Table';
TRUNCATE TABLE bronze.crm_sales_details;
PRINT '===============================================================';
PRINT '>>> Table Truncated';
PRINT '===============================================================';
PRINT '>> Insering Data into bronze.crm_sales_details..';
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\am647\Desktop\Data With Baraa SQL Data Analysis Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRST_ROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
PRINT '===============================';
PRINT '>>> Successfully Data Inserted';
PRINT '===============================';





-- ===========================================================================================
-- ===========================================================================================


PRINT '===========================================';
PRINT '>>> Truncating bronze.erp_cust_az12 Table..';
TRUNCATE TABLE bronze.erp_cust_az12;
PRINT '============================================';
PRINT '>> Table Trucnated.';
PRINT '===============================================';
PRINT 'Inserting Data into bronze.erp_cust_az12 Table.';
PRINT '===============================================';
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\am647\Desktop\Data With Baraa SQL Data Analysis Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
)
PRINT '===============================';
PRINT '>>> Successfully Data Inserted.';
PRINT '===============================';




-- ===========================================================================================
-- ===========================================================================================

PRINT '=============================================';
PRINT '>> Truncating bronze.loc_a101 Table';
PRINT '=============================================';
TRUNCATE TABLE bronze.erp_loc_a101;
PRINT '>>> Table Truncated';
PRINT '=============================================';
PRINT '>> Inserting Data Into bronze.loc_a101 Table';
PRINT '=============================================';
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\am647\Desktop\Data With Baraa SQL Data Analysis Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH
(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)
PRINT '============================================';
PRINT '>>> Successfully Data Inserted';
PRINT '============================================';





-- ===========================================================================================================
-- ===========================================================================================================






PRINT '===================================================';
PRINT '>> Truncating bronze.erp_px_cat_g1v2 Table';
PRINT '===================================================';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
PRINT '>>> Table Truncated';
PRINT '===================================================';
PRINT '>> Inserting Data into bronze.erp_px_cat_g1v2 Table';
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\am647\Desktop\Data With Baraa SQL Data Analysis Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH 
(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)
PRINT '=================================================';
PRINT '>>> Successfully Data Inserted.';
PRINT '=================================================';



END 












