/*
File: scripts/bronze/ddl_bronze.sql
Purpose: Loader procedure for the Bronze layer — truncates and bulk-loads
                 CSV datasets into corresponding `bronze` schema tables.
Author: Repository
Created: 2026-01-02

Description:
 - Defines/updates the stored procedure `bronze.load_bronze`.
 - For each source CSV it truncates the target bronze table then
     performs a `BULK INSERT` from the repository `datasets/` folder.

Usage:
 - Run in SQL Server where the files are accessible to the server process:
     `EXEC bronze.load_bronze;`

Dependencies:
 - datasets/source_crm/cust_info.csv
 - datasets/source_crm/prd_info.csv
 - datasets/source_crm/sales_details.csv
 - datasets/source_erp/CUST_AZ12.csv
 - datasets/source_erp/LOC_A101.csv
 - datasets/source_erp/PX_CAT_G1V2.csv

Notes:
 - File paths in the `BULK INSERT` statements are absolute and assume
     the current repository location; update paths if the SQL Server
     instance cannot access OneDrive paths.
 - Ensure the SQL Server service account has read access to the files.

Changelog:
 - 2026-01-02: Added initial documentation header.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_whole DATETIME, @end_time_whole DATETIME;
    BEGIN TRY

        PRINT '==============================================';
        PRINT 'Loading data into Bronze layer tables...';
        PRINT '==============================================';

        PRINT '----------------------------------------------';
        PRINT 'Loading CRM tables';
        PRINT '----------------------------------------------';

        SET @start_time = GETDATE();
        SET @start_time_whole = GETDATE();
        PRINT '>> Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        from '...\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Time taken to load bronze.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '----------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        from '...\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Time taken to load bronze.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '----------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        from '...\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Time taken to load bronze.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '----------------------------------------------';


        PRINT '----------------------------------------------';
        PRINT 'Loading ERP tables';
        PRINT '----------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        from '...\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Time taken to load bronze.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '----------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        from '...\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Time taken to load bronze.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '----------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        from '...\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        SET @end_time_whole = GETDATE();
        PRINT '>> Time taken to load bronze.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '----------------------------------------------';

        PRINT '==============================================';
        PRINT 'Total time taken to load all Bronze layer tables: ' + CAST(DATEDIFF(SECOND, @start_time_whole, @end_time_whole) AS VARCHAR) + ' seconds';
        PRINT '==============================================';

    END TRY
    BEGIN CATCH
        PRINT '======================================================================================';
        PRINT 'Error occurred while loading data into Bronze layer tables: ' + ERROR_MESSAGE();
        PRINT '======================================================================================';
    END CATCH;
END;