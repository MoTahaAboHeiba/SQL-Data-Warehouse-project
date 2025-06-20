/*
==============================
Gold Layer Data Quality Checks
==============================
This script performs quality checks to validate the integrity, consistency, and accuracy of the Gold Layer.
These checks make sure that:
  -Surrogate keys in dimension tables are unique.
  -Fact tables correctly link to dimension tables.
  -Data model relationships are correct for accurate analysis.
*/

-- Checking 'gold.dim_customers'

  --Check for duplicates in our surrogate key in gold.dim_customers
    SELECT  customer_key,COUNT(*) AS duplicate_count
    FROM gold.dim_customers
    GROUP BY customer_key
    HAVING COUNT(*) > 1;
    -- Perfect, there are no Dublicates records in the PK 
----------------------------------------------------------------------------------

-- Checking 'gold.product_key'
    
  --Check for duplicates in our surrogate key in gold.dim_products
    SELECT product_key,COUNT(*) AS duplicate_count
    FROM gold.dim_products
    GROUP BY product_key
    HAVING COUNT(*) > 1;
    -- Perfect, there are no Dublicates records in the PK 
----------------------------------------------------------------------------------
-- Checking 'gold.fact_sales'

-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
  ON(c.customer_key = f.customer_key)
LEFT JOIN gold.dim_products p
  ON(p.product_key = f.product_key)
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
