
/*
==============================
Silver Layer Data Quality Checks
==============================
This script validates silver layer tables
for data quality issues
including:
-duplicates        -nulls
-unwanted spaces   -invalid dates
-inconsistencies.
Results ensure data accuracy and standardization post-transformation.
*/
/*
Note:
==============================================================================
    - Run these checks after data loading the Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

 --check for NULLs and Dublicates in PK 
			select cst_id,count(*)
			from Silver.CRM_cust_info
			group by cst_id
			having count(*)>1 or cst_id is null
			-- Perfect, there are no Dublicates and null records in the PK 
   
-- check for unwanted spaces in all string columns

			select cst_firstname
			from Silver.CRM_cust_info
			where cst_firstname != TRIM(cst_firstname)
			-- Perfect,There are no unwanted spaces in cst_firstname

-- Data Standardization & Consistency
      select distinct cst_gndr
      from Silver.CRM_cust_info

--------------------------------------------------------------
		-- Check for Silver.CRM_prd_info
			--check for NULLs and Dublicates in PK 

				select prd_id,count(*)
				from Silver.CRM_prd_info
				group by prd_id
				having count(*)>1 or prd_id is null
					-- Perfect, there are no Dublicates in PK 

   -- check for unwanted spaces in all string columns
  	
  			select prd_line
  			from Silver.CRM_prd_info
  			where prd_line != TRIM(prd_line)
  				--  Perfect, there are no unwanted spaces in prd_line
        
			-- check for Nulls and Negative numbers

			select prd_cost
			from Silver.CRM_prd_info
			where prd_cost is null or prd_cost < 0
			--  Perfect, there are no columns that are Null, and all the values are positive


--Data consistency and Normalization check
			
			select distinct prd_line
			from Silver.CRM_prd_info

-- Check for Invalid Date Orders (Start Date > End Date)
      SELECT  * 
      FROM silver.crm_prd_info
      WHERE prd_end_dt < prd_start_dt;

-------------------------------------------------------------------------
-- check for Silver.CRM_sales_details
	--check for invalid dates
 select *
		from Silver.CRM_sales_details
		where sls_due_dt <0  --1 ->nullif
				or LEN(sls_order_dt)!=8 --2 ->nullif
				or sls_order_dt >20300101
				or sls_order_dt >19500101
				OR sls_order_dt is null
		-- Perfect, there are no invalid dates anymore, and the data type changed from int to date
    -- do this for sls_ship_dt and sls_due_dt

--check for the order of dates [sls_order_dt --> sls_ship_dt --> sls_due_dt]
		select * 
		from Silver.CRM_sales_details
		where sls_order_dt>sls_ship_dt 
			or sls_order_dt>sls_due_dt
			or sls_ship_dt> sls_due_dt
		--Prefect, the order of the dates is correct
-- check data consistincy :  sales = quantity*price  and sales not = 0 nor null nor negative value 
		select 
		sls_quantity,
		sls_price ,
		sls_sales 
		from Silver.CRM_sales_details
		where sls_sales<>sls_quantity*sls_price 
			or sls_price is null or sls_quantity is null or sls_sales is null
			or sls_price <=0 or sls_quantity <=0 or sls_sales <=0
		order by sls_sales
--------------------------------------------------------------------------------
-- check for Silver.ERP_CUST_AZ12
	--check for invalid dates
		  SELECT BDATE
		  FROM Silver.ERP_CUST_AZ12
		  where BDATE>getdate()

	-- data consistincy and normalization check
		 select distinct gen
		 FROM Silver.ERP_CUST_AZ12
-------------------------------------------------------------------

-- check for Silver.ERP_LOC_A101
		-- check for matching between cid and cst_key to join to CRM_cust_info
		 SELECT CiD ,CNTRY
		 FROM Silver.ERP_LOC_A101
		 where CID in (select cst_key from bronze.CRM_cust_info)
		
		-- data consistincy and normalization check  
		
		SELECT distinct  CNTRY
		FROM Silver.ERP_LOC_A101
--------------------------------------------------------------------------------
-- check for Silver.ERP_PX_CAT_G1V2		
		-- check for unwanted spaces
		SELECT *
		from Silver.ERP_PX_CAT_G1V2
		where trim(MAINTENANCE)!=MAINTENANCE or trim(CAT)!=CAT or trim(SUBCAT)!=SUBCAT

		-- data consistincy and normalization check  
		SELECT distinct MAINTENANCE from Silver.ERP_PX_CAT_G1V2
