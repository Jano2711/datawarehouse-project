/*
File: scripts/silver/ddl_silver.sql
Purpose: Define Silver-layer table DDL for cleansed and curated data.

Summary:
 - Drops and recreates curated tables in the `silver` schema.
 - Silver layer transforms and cleans raw bronze data, adding DWH metadata
   (dwh_create_date) and standardizing column definitions.
 - Tables serve as the source for Gold analytical views.

Tables defined:
 - silver.crm_cust_info
   Curated customer dimension from CRM.
   Columns: cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status,
            cst_gndr, cst_create_date, dwh_create_date.

 - silver.crm_prd_info
   Curated product dimension from CRM.
   Columns: prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line,
            prd_start_dt, prd_end_dt, dwh_create_date.
   Note: Includes cat_id for linking to category taxonomy.

 - silver.crm_sales_details
   Curated sales transactions from CRM.
   Columns: sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,
            sls_due_dt, sls_sales, sls_quantity, sls_price, dwh_create_date.

 - silver.erp_cust_az12
   Curated customer supplemental data from ERP.
   Columns: cid, bdate, gen, dwh_create_date.

 - silver.erp_loc_a101
   Curated location/country data from ERP.
   Columns: cid, cntry, dwh_create_date.

 - silver.erp_px_cat_g1v2
   Curated product category taxonomy from ERP.
   Columns: id, cat, subcat, maintenance, dwh_create_date.

Notes / Usage:
 - This script is destructive: existing tables are dropped before
   being recreated. Do not run in production without review/backups.
 - All tables include a dwh_create_date timestamp (default GETDATE()).
   Use this to track when records enter the DWH.
 - Silver schema serves as the curated zone; Gold views consume these
   tables and join them for analytics.
 - Ensure Bronze tables are populated before running transformation logic
   that populates Silver tables.

Last updated: 2026-01-16 (generated)
*/

IF OBJECT_ID('silver.crm_cust_info', 'U') is Not null
	DROP TABLE silver.crm_cust_info;
create table silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info', 'U') is Not null
	DROP TABLE silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details', 'U') is Not null
	DROP TABLE silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cust_az12', 'U') is Not null
	DROP TABLE silver.erp_cust_az12;
create table silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_loc_a101', 'U') is Not null
	DROP TABLE silver.erp_loc_a101;
create table silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') is Not null
	DROP TABLE silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

