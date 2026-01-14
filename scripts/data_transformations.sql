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

