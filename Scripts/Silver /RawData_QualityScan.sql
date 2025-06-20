/*
====================================
Data Quality Checks for Bronze Layer
====================================
This script validates bronze layer tables 
for data quality issues
including:
-duplicates        -nulls
-unwanted spaces   -invalid dates
-inconsistencies.
the Results guide transformations for the silver layer.
*/

    --check for NULLs and Dublicates in PK 
			select cst_id,count(*)
			from bronze.CRM_cust_info
			group by cst_id
			having count(*)>1 or cst_id is null
			-- there are Dublicates and null records in PK 

		-- check for unwanted spaces in all string columns

			select cst_firstname
			from bronze.CRM_cust_info
			where cst_firstname != TRIM(cst_firstname)
			-- there are unwanted spaces in cst_firstname

			select cst_lastname
			from bronze.CRM_cust_info
			where cst_lastname != TRIM(cst_lastname)
			-- there are unwanted spaces in cst_lastname
	
			select cst_marital_status
			from bronze.CRM_cust_info
			where cst_marital_status != TRIM(cst_marital_status)
			-- there are unwanted spaces in cst_marital_status
        
			select cst_gndr
			from bronze.CRM_cust_info
			where cst_gndr != TRIM(cst_gndr)
			-- there are no unwanted spaces in cst_gndr
		
    --Data consistency and Normalization check
			
			select distinct cst_gndr
			from bronze.CRM_cust_info
        -- it can be more readable than this and there are missing values in cst_gndr
			select distinct cst_marital_status
			from bronze.CRM_cust_info
        -- it can be more readable than this and there are missing values in cst_marital_status
--------------------------------------------------------------
		-- check for bronze.CRM_prd_info
			--check for NULLs and Dublicates in PK 

				select prd_id,count(*)
				from bronze.CRM_prd_info
				group by prd_id
				having count(*)>1 or prd_id is null
					-- there are no Dublicates in PK 
			
        -- check for unwanted spaces in all string columns
	
			select prd_line
			from bronze.CRM_prd_info
			where prd_line != TRIM(prd_line)
				-- there are no unwanted spaces in prd_line
        
			-- check for Nulls and Negative numbers

			select prd_cost
			from bronze.CRM_prd_info
			where prd_cost is null or prd_cost < 0
			-- there a few columns that is Null and all the values are positive

		--Data consistency and Normalization check
			
			select distinct prd_line
			from bronze.CRM_prd_info
			-- it can be more readable than this  
-------------------------------------------------------------------------
-- check for bronze.CRM_sales_details
	--check for invalid dates
		select *
		from bronze.CRM_sales_details
		where sls_due_dt <0  --1 ->nullif
				or LEN(sls_order_dt)!=8 --2 ->nullif
				or sls_order_dt >20300101
				or sls_order_dt >19500101
				OR sls_order_dt is null
		--and so on in with sls_ship_dt and sls_due_dt

	--check for the order of dates [sls_order_dt --> sls_ship_dt --> sls_due_dt]
		select * 
		from bronze.CRM_sales_details
		where sls_order_dt>sls_ship_dt 
			or sls_order_dt>sls_due_dt
			or sls_ship_dt> sls_due_dt
		--prefect the order is correct

	-- check data consistincy :  sales = quantity*price  and sales not = 0 nor null nor negative value 
		select 
		sls_quantity,
		sls_price ,
		sls_sales 
		from bronze.CRM_sales_details
		where sls_sales<>sls_quantity*sls_price 
			or sls_price is null or sls_quantity is null or sls_sales is null
			or sls_price <=0 or sls_quantity <=0 or sls_sales <=0
		order by sls_sales
--------------------------------------------------------------------------------
-- check for bronze.ERP_CUST_AZ12
	--check for invalid dates
		  SELECT BDATE
		  FROM bronze.ERP_CUST_AZ12
		  where BDATE>getdate()

	-- data consistincy and normalization check
		 select distinct gen
		 FROM bronze.ERP_CUST_AZ12
-------------------------------------------------------------------
-- check for bronze.ERP_LOC_A101
		-- check for matching between cid and cst_key to join to CRM_cust_info
		 SELECT CiD ,CNTRY
		 FROM bronze.ERP_LOC_A101
		 where CID in (select cst_key from bronze.CRM_cust_info)
		
		-- data consistincy and normalization check  
		
		SELECT distinct  CNTRY
		FROM bronze.ERP_LOC_A101
--------------------------------------------------------------------------------
-- check for bronze.ERP_PX_CAT_G1V2		
		-- check for unwanted spaces
		SELECT *
		from bronze.ERP_PX_CAT_G1V2
		where trim(MAINTENANCE)!=MAINTENANCE or trim(CAT)!=CAT or trim(SUBCAT)!=SUBCAT

		-- data consistincy and normalization check  
		SELECT distinct MAINTENANCE from bronze.ERP_PX_CAT_G1V2
