/*
File: scripts/bronze/ddl_bronze.sql
Purpose: Define Bronze-layer table DDL for CRM and ERP sources.

Summary:
 - Drops and recreates raw landing tables in the `bronze` schema.
 - Tables are intended as the raw ingestion/landing zone for CSV
	 sources; they are intentionally permissive and match CSV columns.

Tables defined:
 - bronze.crm_cust_info
 - bronze.crm_prd_info
 - bronze.crm_sales_details
 - bronze.erp_cust_az12
 - bronze.erp_loc_a101
 - bronze.erp_px_cat_g1v2

Notes / Usage:
 - This script is destructive: existing tables are dropped before
	 being recreated. Do not run in production without review/backups.
 - Column data types are simple mappings; validate CSV headers and
	 types before loading production data.
 - If used together with the loader procedure, ensure the loader's
	 file paths are accessible to the SQL Server service account.

Last updated: 2026-01-02 (generated)
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') is Not null
	DROP TABLE bronze.crm_cust_info;
create table bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') is Not null
	DROP TABLE bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') is Not null
	DROP TABLE bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') is Not null
	DROP TABLE bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_loc_a101', 'U') is Not null
	DROP TABLE bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') is Not null
	DROP TABLE bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);

