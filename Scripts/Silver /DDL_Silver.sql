/*
===============================
create tables for Silver Layer
===============================
This script creates tables in 'Silver' Schema,Dropping Existing Tables if they already existed.
Run this script to re-define the DDL structure of 'Silver' Tables
*/

IF OBJECT_ID ('silver.CRM_cust_info','U') is not null
	DROP table silver.CRM_cust_info
CREATE TABLE silver.CRM_cust_info(
cst_id int,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(50),
cst_gndr varchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate()
)

IF OBJECT_ID ('silver.CRM_prd_info','U') is not null
	DROP table silver.CRM_prd_info
CREATE TABLE silver.CRM_prd_info(
prd_id int ,
cat_id varchar(50),
prd_key varchar(50) ,
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()
)


IF OBJECT_ID ('silver.CRM_sales_details','U') is not null
	DROP table silver.CRM_sales_details
CREATE TABLE silver.CRM_sales_details(
sls_ord_num varchar(50) ,
sls_prd_key varchar(50) ,
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()
)


IF OBJECT_ID ('silver.ERP_CUST_AZ12','U') is not null
	DROP table silver.ERP_CUST_AZ12
CREATE TABLE silver.ERP_CUST_AZ12(
CID varchar(50),
BDATE date,
GEN varchar(50),
dwh_create_date datetime2 default getdate()
)


IF OBJECT_ID ('silver.ERP_LOC_A101','U') is not null
	DROP table silver.ERP_LOC_A101
CREATE TABLE silver.ERP_LOC_A101(
CID varchar(50),
CNTRY varchar(50),
dwh_create_date datetime2 default getdate()
)

IF OBJECT_ID ('silver.ERP_PX_CAT_G1V2','U') is not null
	DROP table silver.ERP_PX_CAT_G1V2
CREATE TABLE silver.ERP_PX_CAT_G1V2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50),
dwh_create_date datetime2 default getdate()
)
