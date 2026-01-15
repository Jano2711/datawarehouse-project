INSERT INTO silver.crm_cust_info (
	cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
SELECT
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
from 
(
SELECT 
*, ROW_NUMBER() over (partition by cst_id order by cst_create_Date DESC) as flag_last 
FROM  bronze.crm_cust_info
where cst_id is not null
)t where flag_last = 1;
------------------------------------------------------------------------------------------------
INSERT INTO  silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)
select 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') As cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	CASE  UPPER(TRIM(prd_line)) 
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) as prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION by prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
---------------------------------------------------------------------------------------------------
INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price)
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 then NULL
	 ELSE cast(CAST(sls_order_dt AS VARCHAR) as DATE)
end as sls_order_dt,
CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 then NULL
	 ELSE cast(CAST(sls_ship_dt AS VARCHAR) as DATE)
end as sls_ship_dt,
CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 then NULL
	 ELSE cast(CAST(sls_due_dt AS VARCHAR) as DATE)
end as sls_due_dt,
CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales !=sls_quantity*ABS(sls_price) 
		THEN  sls_quantity*ABS(sls_price) 
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price is null or sls_price <=0
		 THEN sls_sales/NULLIF(sls_quantity,0)
	 ELSE sls_price
END AS sls_price,
sls_price
FROM
bronze.crm_sales_details


