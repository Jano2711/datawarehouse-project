# Bronze Layer — ddl_bronze.sql

Source: scripts/bronze/ddl_bronze.sql

## Purpose

Create and load Bronze-layer tables used as the raw landing zone for CRM and ERP CSV sources. The SQL defines table DDL for the `bronze` schema and a loader procedure that truncates and bulk-loads CSV files into these tables.

## Tables (created by the script)

- `bronze.crm_cust_info`
  - Columns: `cst_id`, `cst_key`, `cst_firstname`, `cst_lastname`, `cst_material_status`, `cst_gndr`, `cst_create_date`
- `bronze.crm_prd_info`
  - Columns: `prd_id`, `prd_key`, `prd_nm`, `prd_cost`, `prd_line`, `prd_start_dt`, `prd_end_dt`
- `bronze.crm_sales_details`
  - Columns: `sls_ord_num`, `sls_prd_key`, `sls_cust_id`, `sls_order_dt`, `sls_ship_dt`, `sls_due_dt`, `sls_sales`, `sls_quantity`, `sls_price`
- `bronze.erp_cust_az12`
  - Columns: `cid`, `bdate`, `gen`
- `bronze.erp_loc_a101`
  - Columns: `cid`, `cntry`
- `bronze.erp_px_cat_g1v2`
  - Columns: `id`, `cat`, `subcat`, `maintenance`

## Source CSV files

The loader (in the same script) expects these source files under the repository `datasets/` folder:

- datasets/source_crm/cust_info.csv -> `bronze.crm_cust_info`
- datasets/source_crm/prd_info.csv -> `bronze.crm_prd_info`
- datasets/source_crm/sales_details.csv -> `bronze.crm_sales_details`
- datasets/source_erp/CUST_AZ12.csv -> `bronze.erp_cust_az12`
- datasets/source_erp/LOC_A101.csv -> `bronze.erp_loc_a101`
- datasets/source_erp/PX_CAT_G1V2.csv -> `bronze.erp_px_cat_g1v2`

## Usage

Run the loader procedure (if present) from a SQL Server session where the service account can access the CSV paths:

```sql
EXEC bronze.load_bronze;
```

## Permissions & Notes

- Ensure the SQL Server service account has read access to the CSV files; the script uses absolute OneDrive paths by default which may not be accessible to the server.
- Consider updating the script to use a configurable path variable or use `BULK INSERT` with a network share accessible by the server.
- The DDL portion drops existing tables if present and recreates them (destructive). Use with caution in non-development environments.

## Changelog

- 2026-01-02: Initial documentation generated from `scripts/bronze/ddl_bronze.sql`.
