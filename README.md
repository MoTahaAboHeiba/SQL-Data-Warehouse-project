# Data Warehouse

Welcome to the **Data Warehouse** repository! 🚀

This is ***Mohamed Taha Abo Hieba***, a junior data engineer.
I built this project to apply what I’ve learned in data warehousing—from working with raw data to producing business-ready insights.
This project demonstrates an end-to-end data pipeline, including data ingestion, transformation, and modeling, following industry best practices.

I put a lot of effort into writing clean, well-organized code, with clear structure and comments throughout.
I hope you find the project helpful and easy to follow!
- Including:
  * ETL processes
  * Data modeling
---
## 🏗️ Data Architecture
The data architecture for this project follows **Medallion Architecture** Bronze, Silver, and Gold layers:

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.
---
## 📖 Project Overview
This project involves:
1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.
---
## 🛠️ Technologies Used
 1. **SQL Server** – For data storage, transformation, and processing
 2. **SQL** – For writing ETL pipelines and data modeling logic
 3. **Draw.io** – For designing and documenting the data architecture
---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---
