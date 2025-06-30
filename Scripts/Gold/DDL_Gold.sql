/*
=========================
Create Views Gold Layers
=========================
This script creates views for the Gold layer in the data warehouse. 
The Gold layer represents the final dimension and fact tables 
we will use Star Schema

Each view performs transformations and combines data from the Silver layer 
to produce a clean, enriched, and business-ready dataset.

*/
CREATE VIEW gold.dim_customer as
(SELECT 
	  ROW_NUMBER() OVER(ORDER BY cst_info.cst_id) as Customer_Key --surrogate key to be our PK
	  ,cst_info.cst_id as Customer_Id
      ,cst_info.cst_key as customer_Number
      ,cst_info.cst_firstname as First_Name
      ,cst_info.cst_lastname as Last_Name
	  ,cst_loc.CNTRY as country
      ,cst_info.cst_marital_status as Marital_Status
      ,case 
	  when cst_info.cst_gndr ='unknowen' then isnull(cst_brth.GEN ,'unknowen')
	  when cst_info.cst_gndr !='unknowen' then cst_info.cst_gndr --   the priority is to CRM in Gender case
	  end as Gender
	  ,cst_brth.BDATE as Birth_Date
	  ,cst_info.cst_create_date as Create_Date
      
	  
  FROM silver.CRM_cust_info as cst_info 
  LEFT JOIN silver.ERP_CUST_AZ12 as cst_brth 
	on(cst_info.cst_key=cst_brth.CID) 
  LEFT JOIN silver.ERP_LOC_A101 as cst_loc
	on(cst_info.cst_key=cst_loc.CID)
	where cst_info.cst_id is not null
)

CREATE VIEW gold.dim_Product as
(SELECT
	ROW_NUMBER() OVER(ORDER BY prd_info.prd_start_dt,prd_info.prd_key) as Product_Key --surrogate key to be our PK
	,prd_info.prd_id as Product_Id
	,prd_info.prd_key as Product_Number
	,prd_info.prd_nm as Product_Name
	,prd_info.cat_id as Category_Id
	,prd_cat.CAT as Category
	,prd_cat.SUBCAT as SubCategory
	,prd_cat.Maintenance
	,prd_info.prd_cost as Cost 
	,prd_info.prd_line as Product_Line
	,prd_info.prd_start_dt as Start_Date
	
  FROM silver.CRM_prd_info as prd_info
  LEFT JOIN silver.ERP_PX_CAT_G1V2 as prd_cat
	on(prd_info.cat_id=prd_cat.ID)
  where prd_info.prd_end_dt is null	  --type 1 of slowly changing dimension thet contains the latest date of the product
  )
 

CREATE VIEW gold.fact_Sales as
  (SELECT sls_ord_num as Order_Number
      ,dp.Product_key
      ,dc.customer_key
      ,sls_order_dt as Order_Date
      ,sls_ship_dt as Shiping_Date
      ,sls_due_dt as Due_Date
	  ,sls_price as Price
      ,sls_quantity as Quantity
	  ,sls_sales as Sales_Amount
  FROM silver.CRM_sales_details as sd
  LEFT JOIN gold.dim_Product as dp
	on(dp.Product_number = sd.sls_prd_key)
  LEFT JOIN  gold.dim_customer as dc
	on(dc.customer_id=sd.sls_cust_id)
	)
