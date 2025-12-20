/*
====================================================================
CREATE 'bronze' layer tables 
====================================================================
Script Purpose: 
			- Firstly drop the specified table and after that create a new table as the same name.
			- Create a store procedure to run the whole query more efficiently.

WARNINGS !! :
		- Run this query at first(before run the 'silver' & 'gold' layer queries.



*/






-- create a store procedure to create tables in Bronze layer
CREATE OR ALTER PROCEDURE create_bronze AS
BEGIN

-- Drop the table for more dynamic
DROP TABLE bronze.crm_cust_info_s;
PRINT '=======================================================================';
PRINT ' Deleted the 01 no. Table which was Created Previously for More Dynamic';
PRINT '=======================================================================';
-- create table namd bronze.crm_cust_info_s
CREATE TABLE bronze.crm_cust_info_s
(
	cst_id					BIGINT,
	cst_key					VARCHAR(50),
	cst_firstname			VARCHAR(50),
	cst_lastname			VARCHAR(50),
	cst_marital_status		VARCHAR(50),
	cst_gndr				VARCHAR(20),
	cst_create_date			VARCHAR(30)
	


)
PRINT '=======================================================================';
PRINT 'Successfully Created 01 Table out of 06 Tables..';
PRINT '=======================================================================';


-- drop the 02 no table cretaed previously
DROP TABLE bronze.crm_prd_info;
PRINT '=======================================================================';
PRINT ' Deleted the 02 no. Table which was Created Previously for More Dynamic';
PRINT '=======================================================================';
-- create table named bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info
(
	prd_id				BIGINT,
	prd_key				VARCHAR(80),
	prd_nm				VARCHAR(150),
	prd_cost			BIGINT,
	prd_line			VARCHAR(20),
	prd_start_dt		DATE,
	prd_end_dt			DATE
	

)
PRINT '=======================================================================';
PRINT 'Successfully Created 02 Table out of 06 Tables..';
PRINT '=======================================================================';



-- drop the below table for more dynamic
DROP TABLE bronze.crm_sales_details;
PRINT '=======================================================================';
PRINT ' Deleted the 03 no. Table which was Created Previously for More Dynamic';
PRINT '=======================================================================';
-- create table named bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details
(
	sls_ord_num				VARCHAR(50),
	sls_prd_key				VARCHAR(50),
	sls_cust_id				VARCHAR(50),
	sls_order_dt			BIGINT,
	sls_ship_dt				BIGINT,
	sls_due_dt				BIGINT,
	sls_sales				BIGINT,
	sls_quantity			BIGINT,
	sls_price				BIGINT
	


)
PRINT '=======================================================================';
PRINT 'Successfully Created 03 Table out of 06 Tables..';
PRINT '=======================================================================';



-- Drop the 05 no. Table which was Created Previously for More Dynamic
DROP TABLE bronze.erp_cust_az12;
PRINT '=======================================================================';
PRINT 'Deleted the 04 no. Table which was Created Previously for More Dynamic';
PRINT '=======================================================================';
-- create table named bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12
(
	CID			VARCHAR(150),
	BDATE		DATE ,
	GEN			VARCHAR(40)


)
PRINT '=======================================================================';
PRINT 'Successfully Creted 04 no. Table out of 06 Tables..';
PRINT '=======================================================================';




-- drop the 06 no. Table which was created previously for more dynamic
DROP TABLE bronze.erp_loc_a101;
PRINT '=======================================================================';
PRINT 'Deleted the 05 no. Table which was Created Previously for More Dynamic';
PRINT '=======================================================================';
-- create table named bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101
(
	CID					VARCHAR(50),
	CNTRY				VARCHAR(50)
	

)
PRINT '=======================================================================';
PRINT 'Successfully Created 05 Table out of 06 Tables..';
PRINT '=======================================================================';


-- drop the 07 no. table Created Previously
DROP TABLE bronze.erp_px_cat_g1v2;
PRINT '=======================================================================';
PRINT 'Deleted the 06 no. Table which was Created Previously for More Dynamic';
PRINT '=======================================================================';
-- create table named bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2
(
	ID					VARCHAR(50),
	CAT					VARCHAR(50),
	SUBCAT				VARCHAR(50),
	MAINTAINANCE		VARCHAR(50)


)

PRINT 'Successfully Create 06 Table out of 06 Tables..';
PRINT '================================================';

END









