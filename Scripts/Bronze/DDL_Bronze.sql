--create tables for bronze stage

CREATE TABLE bronze.CRM_cust_info(
cst_id int,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(1),
cst_gndr varchar(1),
cst_create_date date
)
CREATE TABLE bronze.CRM_prd_info(
prd_id int ,
prd_key varchar(50) ,
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date
)

CREATE TABLE bronze.CRM_sales_details(
sls_ord_num varchar(50) ,
sls_prd_key varchar(50) ,
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
)


CREATE TABLE bronze.ERP_CUST_AZ12(
CID varchar(50),
BDATE date,
GEN varchar(6)

)

CREATE TABLE bronze.ERP_LOC_A101(
CID varchar(50),
CNTRY varchar(50)

)
CREATE TABLE bronze.ERP_PX_CAT_G1V2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50)

)
