**1. Project Overview**
This project builds a Retail Lakehouse using Azure Databricks to integrate offline and online retail data, improve data quality, and generate reliable business reports.

**2. Business Problem**
Offline data is stored in Azure SQL Database.
Online orders are received as JSON files in ADLS Gen2.
Duplicate customers and inconsistent product information exist.
Reporting across both channels is difficult.

**3. Data Sources**
Azure SQL Server -- Customers, Products, Orders
ADLS Gen2 -- Online orders in JSON format

**4. Architecture**
Azure SQL Database ──┐
                     ├── Bronze ── Silver ── Gold ── Power BI
ADLS Gen2 JSON ──────┘

** 5. Solution Architecture**

                         DATA SOURCES
                              |
             +----------------+----------------+
             |                                 |
     Azure SQL Database                    ADLS Gen2
     Offline Retail Data                 Online JSON Files
             |                                 |
             +----------------+----------------+
                              |
                              v
                     AZURE DATABRICKS
                              |
             +----------------+----------------+
             |                                 |
     SQL Server Ingestion             JSON Streaming Ingestion
     Lakeflow Pipeline                PySpark Structured Streaming
             |                                 |
             +----------------+----------------+
                              |
                              v
                       BRONZE LAYER
                   Raw Ingested Delta Data
                              |
                              v
                       SILVER LAYER
             Cleansing, Standardization, Unification
                              |
                              v
                        GOLD LAYER
                Analytics-Ready Data Models
                              |
                              v
                   REPORTING AND ANALYTICS


  **  6. Databricks Asset Bundles**
  dab_retail_project/
│
├── databricks.yml
├── resources/
│   ├── jobs & pipelines/
│   └── variables/
├── src/
│   ├── bronze_layer/
│   ├── silver_layer/
│   └── gold_layer/




**7. CI/CD Implementation**



<img width="419" height="278" alt="image" src="https://github.com/user-attachments/assets/aebbe51f-d7d2-4cac-a5e3-40b7c2328d86" />





**8. Project Outcome**

*Integrated offline and online retail data.\
*Improved customer and product data quality.\
*Created unified dimensional models.\
*Enabled incremental data processing.\
*Automated Databricks deployment using Asset Bundles and CI/CD.\
*Prepared reliable datasets for Power BI reporting.

