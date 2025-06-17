/*
============================
Create Database and Schemas
============================
This script creates a new database named 'DataWarehouse',
the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
*/


--create the "DataWarehouse" database
create database DataWarehouse;

use DataWarehouse;
--create schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
