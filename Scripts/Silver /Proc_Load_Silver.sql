/*
=======================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=======================================================
Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to load data into the 'Silver' tables from 'Bronze' tables. 
    It performs the following actions:
	   - Truncates Silver tables.
	   - Inserts transformed and cleansed data from Bronze into Silver tables.
Parameters:
	   - This stored procedure does not accept any parameters or return any values.
Usage Example:
    	   - EXEC bronze.load_bronze;
==============================================================================
*/

create or alter procedure silver.load_silver as
BEGIN

	DECLARE @Global_start_time datetime,@Global_end_time datetime
	DECLARE @start_time datetime,@end_time datetime
	
	BEGIN TRY
		print '							======================================================================'
		print '							this script is insrting cleand and transformed data into silver layer'
		print '							======================================================================'
		set @Global_start_time=GETDATE()
		set @start_time=GETDATE()
		print '------------------------------------------'
		print '-> Trancating Table :silver.CRM_cust_info '
		print '------------------------------------------'
		truncate table silver.CRM_cust_info

		print '-------------------------------------------------'
		print '-> Inserting data into silver.CRM_cust_info Table'
		print '-------------------------------------------------'
		INSERT INTO silver.CRM_cust_info(
			cst_id ,
			cst_key ,
			cst_firstname ,
			cst_lastname ,
			cst_marital_status,
			cst_gndr ,
			cst_create_date 
		)
		select cst_id,cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname 
						--removing unwanted spaces
			,
			case  upper(TRIM(cst_marital_status))
				when'S' then 'Single'
				when'M' then 'Married'
				else 'unknowen' -- handling missing values 
			end as cst_marital_status
					--Normalizing cst_marital_status values to readable format
			,case upper(TRIM(cst_gndr))
				when 'F' then 'Female'
				when 'M' then 'Male'
				else 'unknowen' -- handling missing values 
			end as cst_gndr
					--Normalizing cst_gndr values to readable format
					,
			cst_create_date
		from (
		select *,
			ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as row_num
		from bronze.CRM_cust_info
		) as new_table	--remving Dublicate records

		where row_num =1 and cst_id is not null	 -- select only the most recent record per customer		
		set @end_time=GETDATE()
		print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
		print '--------------------------'
		set @Global_start_time=GETDATE()
		set @start_time=GETDATE()
		print '-----------------------------------------'
		print '-> Trancating Table :silver.CRM_prd_info '
		print '-----------------------------------------'
		truncate table silver.CRM_prd_info

		print '-------------------------------------------------'
		print '-> Inserting data into silver.CRM_prd_info Table'
		print '-------------------------------------------------'
		INSERT INTO silver.CRM_prd_info(
		prd_id ,
		cat_id,
		prd_key ,
		prd_nm ,
		prd_cost ,
		prd_line ,
		prd_start_dt,
		prd_end_dt )
		select prd_id ,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id, -- we have created a new column to join with ERP_PX_CAT_G1V2 table
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key, -- we have created a new column to join with CRM_sales_details table
		prd_nm ,
		ISNULL(prd_cost,0) as prd_cost ,
		case upper(TRIM(prd_line))
				when 'S' then 'Other Sales'
				when 'R' then 'Road'
				when 'T' then 'Touring'
				when'M' then 'Mountain'
				else 'unknowen' -- handling missing values 
			end as prd_line
					--Normalizing prd_line values to readable format 
					,
		prd_start_dt,
		dateadd(DAY,-1,LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt  -- select the prd_strat_date of the next customer as it's end date
		from bronze.CRM_prd_info -- calculating prd_end_date as one day before the next prd_start_date
		set @end_time=GETDATE()
		print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
		print '--------------------------'

		set @Global_start_time=GETDATE()
		set @start_time=GETDATE()
		print '----------------------------------------------'
		print '-> Trancating Table :silver.CRM_sales_details '
		print '----------------------------------------------'
		truncate table silver.CRM_sales_details

		print '-----------------------------------------------------'
		print '-> Inserting data into silver.CRM_sales_details Table'
		print '-----------------------------------------------------'
		insert into  silver.CRM_sales_details
		(
		sls_ord_num ,
		sls_prd_key  ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price 
		)
		SELECT sls_ord_num
			  ,sls_prd_key
			  ,sls_cust_id
			  ,case
			  when sls_order_dt=0 or len(sls_order_dt)!=8 then Null
			  else cast(cast(sls_order_dt as varchar) as date)
			  end as sls_order_dt				--converting int to date and handling out range values   
			  ,case
			  when sls_ship_dt=0 or len(sls_ship_dt)!=8 then Null
			  else cast(cast(sls_ship_dt as varchar) as date)
			  end as sls_ship_dt				--converting int to date and handling out range values 
			  ,case
			  when sls_due_dt=0 or len(sls_due_dt)!=8 then Null
			  else cast(cast(sls_due_dt as varchar) as date)
			  end as sls_due_dt					--converting int to date and handling out range values 
			  ,case when sls_sales <> sls_quantity*sls_price or sls_sales <=0 or sls_sales is null
					then sls_quantity*abs(sls_price)		 
					else sls_sales 
				end as sls_sales		-- handling wronge and missing values to be in form sls_sales = sls_quantity * sls_price			
				,sls_quantity
				,case when sls_price is null or sls_price <=0 
					then sls_sales/sls_quantity
					else sls_price
				end as sls_price		-- handling wronge and missing values to be in form sls_price = sls_sales/sls_quantity 
		  FROM bronze.CRM_sales_details
		  set @end_time=GETDATE()
		print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
		print '--------------------------'

		set @Global_start_time=GETDATE()
		set @start_time=GETDATE()
		print '----------------------------------------'
		print '-> Trancating Table :silver.ERP_CUST_AZ12 '
		print '------------------------------------------'
		truncate table silver.ERP_CUST_AZ12

		print '-------------------------------------------------'
		print '-> Inserting data into silver.ERP_CUST_AZ12 Table'
		print '-------------------------------------------------'
		insert into silver.ERP_CUST_AZ12(
			CID ,
			BDATE ,
			GEN 
		)
		 SELECT 
			  case
				  when CID like 'NAS%' then REPLACE(CID,'NAS','')  -- removing unneeded prefix value [NAS] 
				  else CID
			  end as CID
			  ,case 
				when BDATE> getdate() then NULL
				else BDATE
			  end as BDATE   -- handling invalid values
			  ,case 
				  when gen= '' or gen is null then 'unknowen' --  Normalizing whitespace and missing values to unknowen
				  when upper(trim(gen))='M' then 'Male'
				  when upper(trim(gen))='F' then 'Female'
				  else GEN			-- Normalizing Gender values to readable format  
			 end as GEN
		FROM bronze.ERP_CUST_AZ12
		set @end_time=GETDATE()
		print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
		print '--------------------------'

		set @Global_start_time=GETDATE()
		set @start_time=GETDATE()
		print '-----------------------------------------'
		print '-> Trancating Table :silver.ERP_LOC_A101 '
		print '-----------------------------------------'
		truncate table silver.ERP_LOC_A101

		print '------------------------------------------------'
		print '-> Inserting data into silver.ERP_LOC_A101 Table'
		print '------------------------------------------------'

		insert into silver.ERP_LOC_A101(CID,CNTRY)
		SELECT  replace(CID,'-','') as CID		-- removing invalid charcter to matching between cid and cst_key to join to CRM_cust_info
			,case
				when CNTRY ='' or CNTRY is null then 'unkownen'		----  Normalizing whitespace and  missing values to unknowen
				when upper(trim(CNTRY)) in('US','USA','UNITED STATES') then  'United States'
				when upper(trim(CNTRY))='DE' then  'Germany'
				else CNTRY		-- Normalizing CNTRY values to readable format  
			end as CNTRY
		FROM bronze.ERP_LOC_A101
		set @end_time=GETDATE()
		print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
		print '--------------------------'
		set @Global_start_time=GETDATE()
		set @start_time=GETDATE()
		print '--------------------------------------------'
		print '-> Trancating Table :silver.ERP_PX_CAT_G1V2 '
		print '--------------------------------------------'
		truncate table silver.ERP_PX_CAT_G1V2
		print '---------------------------------------------------'
		print '-> Inserting data into silver.ERP_PX_CAT_G1V2 Table'
		print '---------------------------------------------------'
		insert into silver.ERP_PX_CAT_G1V2(
			   ID
			  ,CAT
			  ,SUBCAT
			  ,MAINTENANCE
			  )
		SELECT  ID
			  ,CAT
			  ,SUBCAT
			  ,MAINTENANCE
		  FROM bronze.ERP_PX_CAT_G1V2
		set @end_time=GETDATE()
		print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
		print '--------------------------'

		set @Global_end_time=GETDATE()
		print '***************************************'
		print 'Loading silver Layer completed'
		print ' -Total Loading Time : '+cast(datediff(second,@global_start_time , @global_end_time) as varchar)+ 'seconds'
		print '***************************************'
	END TRY 
	BEGIN CATCH
			print '=========================================='
			print'ERROR OCCURED DURING LOADING BRONZE LAYER'
			print'ERROR MESSAGE'+ Error_message()
			print'ERROR NUMBER'+ cast(Error_NUMBER() as varchar)
			print'ERROR STATE'+ cast(Error_STATE() as varchar)
			print '=========================================='
		END CATCH	

END


