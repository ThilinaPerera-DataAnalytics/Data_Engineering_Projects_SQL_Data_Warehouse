/*
==================================================
Create Database and Schema
==================================================
Script purpose:
    This script creates a new database named 'data_warehouse' after checking if it already exists.
    If the database exists, it will be dropped and recreated.
    Additionally, the script sets up three schemas within the database: 'bronze', 'silver' and 'gold'.
    
Warning:
    Running this script will drop the entire database if it exists.
    All data in the database will be permanently deleted.
    Proceed with caution and ensure you have properly backed up before running this script. 
*/

-- Connect to postgres database first (manually in DBeaver/psql)
-- In PostgreSQL, you need to connect to a database to run commands
-- So first connect to 'postgres' database, then run:

-- Drop and recreate the data_warehouse database
-- Terminate existing connections first
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity 
WHERE pg_stat_activity.datname = 'data_warehouse'
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS data_warehouse;

-- Create the data_warehouse database
CREATE DATABASE data_warehouse;

-- Note: In PostgreSQL, you now need to manually connect to 'data_warehouse' database
-- Then run the following:

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;