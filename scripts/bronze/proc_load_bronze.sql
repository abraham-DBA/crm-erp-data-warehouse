/*
==========================================================================================================================================
Bronze Layer Data Loader Stored Procedure
===========================================================================================================================================
Description: This stored procedure loads raw data from CSV files(source) into bronze layer tables using a full refresh strategy (truncate-and-load).

Data Flow:
  1. TRUNCATE target table (removes all existing data)
  2. BULK INSERT from CSV file (loads fresh data)
  3. Repeat for each table sequentially
  4. Implements full refresh pattern

Tables Loaded:
  1. CRM: crm_cust_info, crm_prd_info, crm_sales_details
  2. ERP: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2

Usage: EXEC bronze.load_bronze;
=============================================================================================================================================
*/

-- CREATING A STORED PROCEDURE FOR LOADING THE DATA INTO THE BRONZE SCHEMA
CREATE OR ALTER PROCEDURE bronze.load_bronze
    AS BEGIN
        DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
        BEGIN TRY
            -- Here we are doing a FULL LOAD we are truncating the table and inserting the data
            -- BULK INSERT FOR bronze.crm.cust_info TABLE
            SET @batch_start_time = GETDATE();
            PRINT('=====================================================');
            PRINT('Loading Bronze Layer');
            PRINT('=====================================================');

            PRINT('-----------------------------------------------------');
            PRINT('Loading CRM tables');
            PRINT('-----------------------------------------------------');

            SET @start_time = GETDATE();
            PRINT('>> Truncating Table: bronze.crm_cust_info');
            TRUNCATE TABLE bronze.crm_cust_info;

            PRINT('>> Inserting data into: bronze.crm_cust_info');
            BULK INSERT bronze.crm_cust_info
            FROM '/var/opt/mssql/data/datasets/source_crm/cust_info.csv'
            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds');
            PRINT('-----------------------------------------------------');

            SET @start_time = GETDATE();
            -- BULK INSERT FOR bronze.crm.prd_info TABLE
            PRINT('>> Truncating Table: bronze.crm_prd_info');
            TRUNCATE TABLE bronze.crm_prd_info;

            PRINT('>> Inserting into: bronze.crm_prd_info');
            BULK INSERT bronze.crm_prd_info
            FROM '/var/opt/mssql/data/datasets/source_crm/prd_info.csv'
            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds');
            PRINT('-----------------------------------------------------');

            SET @start_time = GETDATE();
            -- BULK INSERT FOR bronze.crm.sales_details TABLE
            PRINT('>> Truncating Table: bronze.crm_sales_details');
            TRUNCATE TABLE bronze.crm_sales_details;

            PRINT('>> Inserting into: bronze.crm_sales_details');
            BULK INSERT bronze.crm_sales_details
            FROM '/var/opt/mssql/data/datasets/source_crm/sales_details.csv'
            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds');
            PRINT('-----------------------------------------------------');

            -- BULK INSERT FOR bronze.erp.cust_az12 TABLE
            PRINT('-----------------------------------------------------');
            PRINT('Loading ERP tables');
            PRINT('-----------------------------------------------------');

            SET @start_time = GETDATE();
            PRINT('>> Truncating Table: bronze.erp_cust_az12');
            TRUNCATE TABLE bronze.erp_cust_az12;

            PRINT('>> Inserting into: bronze.erp_cust_az12');
            BULK INSERT bronze.erp_cust_az12
            FROM '/var/opt/mssql/data/datasets/source_erp/cust_az12.csv'
            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds');
            PRINT('-----------------------------------------------------');

            SET @start_time = GETDATE();
            -- BULK INSERT FOR bronze.erp.loc_a101 TABLE
            PRINT('>> Truncating Table: bronze.erp_loc_a101');
            TRUNCATE TABLE bronze.erp_loc_a101;

            PRINT('>> Inserting into: bronze.erp_loc_a101');
            BULK INSERT bronze.erp_loc_a101
            FROM '/var/opt/mssql/data/datasets/source_erp/loc_a101.csv'
            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds');
            PRINT('-----------------------------------------------------');

            SET @start_time = GETDATE();
            -- BULK INSERT FOR bronze.erp_px_cat_g1v2 TABLE
            PRINT('>> Truncating Table: bronze.erp_px_cat_g1v2');
            TRUNCATE TABLE bronze.erp_px_cat_g1v2;

            PRINT('>> Inserting into: bronze.erp_px_cat_g1v2');
            BULK INSERT bronze.erp_px_cat_g1v2
                FROM '/var/opt/mssql/data/datasets/source_erp/px_cat_g1v2.csv'
                WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds');
            PRINT('-----------------------------------------------------');
            SET @batch_end_time = GETDATE();
            PRINT('=====================================================');
            PRINT('Loading bronze layer completed')
            PRINT('>>Load Duration for the bronze layer: ' + CAST(DATEDIFF(second , @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds')
            PRINT('=====================================================');
        END TRY
        BEGIN CATCH
            PRINT('---------------------------------------------------------')
            PRINT('ERROR MESSAGE:' + ERROR_MESSAGE())
        END CATCH
END


