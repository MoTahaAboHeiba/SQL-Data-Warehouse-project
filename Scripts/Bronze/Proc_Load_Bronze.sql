/*
=======================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=======================================================
Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files(CRM and ERP files). 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
Parameters:
	  This stored procedure does not accept any parameters or return any values.
Usage Example:
    EXEC bronze.load_bronze;
==============================================================================
*/

create or alter procedure bronze.load_bronze as
	BEGIN
		DECLARE @Global_start_time datetime,@Global_end_time datetime
		DECLARE @start_time datetime,@end_time datetime
		BEGIN TRY
			print '																				================================='
			print '																					Loading The Bronze Layer'
			print '																				================================='
				print '==========================='
				print '	Loading CRM Tables'
				print '==========================='
					set @Global_start_time=GETDATE()
					set @start_time=GETDATE()
					print '---------------------------------------'
					print '-> Trancating Table :bronze.CRM_cust_info '
					print '---------------------------------------'
					truncate table bronze.CRM_cust_info
					print '-------------------------------------------------'
					print '-> Inserting data into bronze.CRM_cust_info Table'
					print '-------------------------------------------------'
					BULK INSERT bronze.CRM_cust_info
					from 'D:\Work Space\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
					with (
						FIRSTROW=2,
						FIELDTERMINATOR =',',	-- field delimiter type
						TABLOCK		-- just for optimization 
					)
					set @end_time=GETDATE()
					print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
					print '--------------------------'
					print '-----------------------------------------'
					print '-> Trancating Table :bronze.CRM_prd_info '
					print '-----------------------------------------'
					set @start_time=GETDATE()
					truncate table bronze.CRM_prd_info
					print '------------------------------------------------'
					print '-> Inserting data into bronze.CRM_prd_info Table'
					print '------------------------------------------------'
					BULK INSERT bronze.CRM_prd_info
					from 'D:\Work Space\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
					with (
						FIRSTROW=2,
						FIELDTERMINATOR =',',	
						TABLOCK		
					)
					set @end_time=GETDATE()
					print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
					print '--------------------------'
					print '----------------------------------------------'
					print '-> Trancating Table :bronze.CRM_sales_details '
					print '----------------------------------------------'
					set @start_time=GETDATE()
					truncate table bronze.CRM_sales_details
					print '-----------------------------------------------------'
					print '-> Inserting data into bronze.CRM_sales_details Table'
					print '-----------------------------------------------------'
					BULK INSERT bronze.CRM_sales_details
					from 'D:\Work Space\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
					with (
						FIRSTROW=2,
						FIELDTERMINATOR =',',	
						TABLOCK		 
					)
					set @end_time=GETDATE()
					print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
					print '--------------------------'

				print '==========================='
				print '	Loading ERP Tables'
				print '==========================='
					print '------------------------------------------'
					print '-> Trancating Table :bronze.ERP_CUST_AZ12 '
					print '------------------------------------------'
					set @start_time=GETDATE()
					truncate table bronze.ERP_CUST_AZ12
					print '-------------------------------------------------'
					print '-> Inserting data into bronze.ERP_CUST_AZ12 Table'
					print '-------------------------------------------------'
					BULK INSERT bronze.ERP_CUST_AZ12
					from 'D:\Work Space\Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
					with (
						FIRSTROW=2,
						FIELDTERMINATOR =',',	
						TABLOCK			
						) 
					set @end_time=GETDATE()
					print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
					print '--------------------------'
					print '-----------------------------------------'
					print '-> Trancating Table :bronze.ERP_LOC_A101 '
					print '-----------------------------------------'
					set @start_time=GETDATE()
					truncate table bronze.ERP_LOC_A101
					print '------------------------------------------------'
					print '-> Inserting data into bronze.ERP_LOC_A101 Table'
					print '------------------------------------------------'
					BULK INSERT bronze.ERP_LOC_A101
					from 'D:\Work Space\Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
					with (
						FIRSTROW=2,
						FIELDTERMINATOR =',',	
						TABLOCK			
					)
					set @end_time=GETDATE()
					print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
					print '--------------------------'
					print '-------------------------------------------'
					print '->Trancating Table :bronze.ERP_PX_CAT_G1V2 '
					print '-------------------------------------------'
					set @start_time=GETDATE()
					truncate table bronze.ERP_PX_CAT_G1V2
					print '---------------------------------------------------'
					print '-> Inserting data into bronze.ERP_PX_CAT_G1V2 Table'
					print '---------------------------------------------------'
					BULK INSERT bronze.ERP_PX_CAT_G1V2
					from 'D:\Work Space\Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
					with (
						FIRSTROW=2,
						FIELDTERMINATOR =',',	
						TABLOCK		 
					)
					set @end_time=GETDATE()
					print 'Loading Time : '+cast(datediff(second,@start_time , @end_time) as varchar)+ 'seconds'
					print '--------------------------'
					set @Global_end_time=GETDATE()
					print '***************************************'
					print 'Loading Bronze Layer completed'
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
