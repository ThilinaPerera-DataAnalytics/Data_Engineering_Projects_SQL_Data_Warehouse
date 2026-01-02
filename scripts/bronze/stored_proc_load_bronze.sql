/*
================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
================================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the COPY command to load data from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();

================================================================================
*/
-- PostgreSQL-compatible “LOAD_bronze” function that mimics MSSQL procedure.
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS
$$
DECLARE -- Assign variables
    b_start_time TIMESTAMP;
    b_end_time   TIMESTAMP;
    b_load_secs  INTEGER;

    start_time TIMESTAMP;
    end_time   TIMESTAMP;
    load_secs  INTEGER;

BEGIN
    b_start_time := clock_timestamp();
    RAISE NOTICE '>>> Loading Bronze Layer >>>';
    RAISE NOTICE '>>> Starting Load Process >>>';

    RAISE NOTICE '>> Truncating Tables';
	
-- Truncate all tables first
	    TRUNCATE bronze.crm_prd_info,
	             bronze.crm_sales_details,
	             bronze.erp_cust_az12,
	             bronze.erp_loc_a101,
	             bronze.erp_px_cat_g1v2,
	             bronze.crm_cust_info;
	
-- Load CRM data
	    RAISE NOTICE '>> Loading CRM tables';
	
	    start_time := clock_timestamp();
	    COPY bronze.crm_cust_info
	    FROM 'D:/1. Ongoing & Completed Projects/2025.12.12 - SQL Data Warehouse project/Data_Engineering_Projects_SQL_Data_Warehouse/datasets/source_crm/cust_info.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    load_secs := EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> LOAD DURATION: % seconds', load_secs;
	    RAISE NOTICE '>> -------------------';

	    start_time := clock_timestamp();
	    COPY bronze.crm_prd_info
	    FROM 'D:/1. Ongoing & Completed Projects/2025.12.12 - SQL Data Warehouse project/Data_Engineering_Projects_SQL_Data_Warehouse/datasets/source_crm/prd_info.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    load_secs := EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> LOAD DURATION: % seconds', load_secs;
	    RAISE NOTICE '>> -------------------';
	
	    start_time := clock_timestamp();
	    COPY bronze.crm_sales_details
	    FROM 'D:/1. Ongoing & Completed Projects/2025.12.12 - SQL Data Warehouse project/Data_Engineering_Projects_SQL_Data_Warehouse/datasets/source_crm/sales_details.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    load_secs := EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> LOAD DURATION: % seconds', load_secs;
	    RAISE NOTICE '>> -------------------';
	
	    RAISE NOTICE '>> Loading ERP tables';
	
-- Load ERP data
	    start_time := clock_timestamp();
	    COPY bronze.erp_cust_az12
	    FROM 'D:/1. Ongoing & Completed Projects/2025.12.12 - SQL Data Warehouse project/Data_Engineering_Projects_SQL_Data_Warehouse/datasets/source_erp/CUST_AZ12.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    load_secs := EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> LOAD DURATION: % seconds', load_secs;
	    RAISE NOTICE '>> -------------------';
	
	    start_time := clock_timestamp();
	    COPY bronze.erp_loc_a101
	    FROM 'D:/1. Ongoing & Completed Projects/2025.12.12 - SQL Data Warehouse project/Data_Engineering_Projects_SQL_Data_Warehouse/datasets/source_erp/LOC_A101.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    load_secs := EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> LOAD DURATION: % seconds', load_secs;
	    RAISE NOTICE '>> -------------------';
	
	    start_time := clock_timestamp();
	    COPY bronze.erp_px_cat_g1v2
	    FROM 'D:/1. Ongoing & Completed Projects/2025.12.12 - SQL Data Warehouse project/Data_Engineering_Projects_SQL_Data_Warehouse/datasets/source_erp/PX_CAT_G1V2.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    load_secs := EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> LOAD DURATION: % seconds', load_secs;
	    RAISE NOTICE '>> -------------------';
	
	    RAISE NOTICE '>>> Bronze load completed successfully >>>';
	
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Bronze load failed';
        RAISE WARNING 'Error message: %', SQLERRM;
        RAISE WARNING 'SQLSTATE: %', SQLSTATE;

        RAISE NOTICE '----------------------------'
	    b_end_time := clock_timestamp();
	    b_load_secs := EXTRACT(EPOCH FROM (b_end_time - b_start_time));
	    RAISE NOTICE '>> LOAD DURATION: % seconds', b_load_secs;
        RAISE NOTICE '============================'

END;
$$;
