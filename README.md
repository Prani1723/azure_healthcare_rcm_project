# Healthcare Revenue Cycle Management (RCM) - Azure Data Engineering Pipeline


Managing healthcare revenue efficiently is a critical challenge, especially with the increasing complexity of medical billing and insurance claims. **Revenue Cycle Management (RCM)** involves tracking patient visits, medical services, insurance claims, and payments to ensure healthcare providers receive timely reimbursements. This project builds an end-to-end data pipeline to automate and optimize RCM using **Azure Data Factory, Databricks, Delta Lake, and Synapse Analytics, with Power BI** providing actionable insights.

By leveraging the **Medallion Architecture (Bronze, Silver, Gold layers)**, this pipeline processes and transforms Electronic Medical Records (EMR), Insurance Claims, and Healthcare Codes (NPI, ICD, CPT) into a structured format. The system ensures data quality, **complete historical tracking (SCD Type 2) and standardized reporting (CDM)**, enabling hospitals to analyze financial trends, optimize cash flow, and improve decision-making.

## Datasets 

The pipeline processes multiple datasets essential for revenue cycle management. These datasets are categorized into different types based on their source and usage:

**Electronic Medical Records (EMR)**: Contains details about patient visits, services provided, billing information, and provider interactions. Stored in Azure SQL Database.

**Claims Data**: Includes insurance claims information, payment statuses, and claim approval/rejection details. These are received as flat files (CSV) and stored in Azure Data Lake Storage (ADLS Gen2).

**Health Codes**: Comprises standardized medical coding systems such as NPI (National Provider Identifier), ICD (International Classification of Diseases), and CPT (Current Procedural Terminology) codes. These are sourced via Public APIs and flat files.

**Providers Data**: Includes physician and hospital details, department affiliations, and NPI identifiers.

**Departments Data**: Contains department-level mapping of providers and their associated transactions.

**Transactions Data (Fact Table)**: Includes financial transaction details such as procedure codes, visit types, service dates, payment statuses, and claim-related information.

Each dataset is processed in stages, moving from Bronze (raw data) to Silver (cleaned, transformed data) to Gold (structured analytical data), ensuring that the data is refined and optimized for business insights.

## Project Architecture & Data Flow

**1. Data Ingestion & Storage (Bronze Layer)** 

**Sources**: Data is collected from multiple sources:

**Electronic Medical Records (EMR)** - Stored in Azure SQL Database

**Claims Data** - Monthly flat file uploads (CSV) stored in Azure Data Lake Storage (ADLS Gen2)

**Health Codes** - NPI, ICD, and CPT codes sourced via **Public API`s **

## Metadata-Driven Ingestion:

* The pipeline is designed to be metadata-driven, dynamically ingesting data based on configuration files.

* Lookup tables and configuration lists in Azure Data Factory (ADF) determine the data source, ingestion type (full/incremental), and target storage path.
* Azure Data Factory (ADF) orchestrates batch ingestion of EMR, Claims, and Healthcare Codes.
  
**Landing Zone**: Raw files and databases are stored in ADLS Gen2 in Parquet format.

**Audit Table**: Each ingestion is logged to track processing status and data integrity.

**Parameterization**: All linked services, datasets, and pipeline source/sinks in ADF are parameterized based on metadata configurations, ensuring dynamic and scalable data movement.
## Validation & Execution Flow:
* A lookup activity fetches configuration details from load_config.csv.
* A ForEach loop iterates over each entity to process.
* A conditional check validates whether is_active = 1, ensuring only active records are ingested.
* A copy activity moves data from SQL DB to ADLS Bronze Layer.
* If the file exists in Bronze, it is archived before processing new data.
* Based on load type (Full/Incremental), the respective query is executed.
* Audit logs are updated to track the process.

## 2. Data Processing & Transformation (Silver Layer)

Once the raw data is stored in the Bronze Layer, it undergoes cleaning, validation, and standardization to ensure consistency across healthcare providers.

#### Processing Engine:

* Azure Databricks (PySpark) is used to process large datasets efficiently.
* Data Quality Checks: Identify missing values, duplicate records, and inconsistencies.
* Common Data Model (CDM): Standardizes schema across all hospitals, making data uniform.
#### Automated Execution in ADF:

* The first Execute Pipeline triggers the ingestion process, moving data from SQL DB to the Bronze Layer, ensuring only records with is_active = 1 are processed.
* After the Bronze layer ingestion is complete, the second Execute Pipeline triggers Silver to Gold transformation notebooks in Databricks.
* This process automates data validation, transformation, and creation of Fact and Dimension tables in the Gold Layer.
* The entire workflow is orchestrated in ADF, ensuring seamless and efficient execution between the Bronze, Silver, and Gold layers.
  
#### Slowly Changing Dimension (SCD Type 2) Implementation:

* Maintains historical records for patients, providers, and claims.
* Tracks updates and modifications (e.g., address changes, new insurance policies).
* Ensures accurate longitudinal analysis for financial forecasting.

#### Silver Data Flow Execution:

* Data is validated and processed in Databricks notebooks.
* Invalid or missing records are flagged in quarantine tables.
* SCD-2 ensures historical tracking and record updates.
* The transformed data is stored as Delta Tables in Silver Layer.

#### Security with Azure Key Vault & Databricks Mount Points:

* All database credentials, API keys, and sensitive configurations are securely stored and managed using Azure Key Vault.
* Ensures secure access and compliance with data security standards.
* Databricks mount points are configured using key-backed secret scopes, ensuring secure access to Azure Data Lake Storage (ADLS Gen2) while preventing hardcoded credentials.
* This approach enhances data security, access control, and operational efficiency across the pipeline.

## 3. Data Aggregation & Analytics (Gold Layer)
The Gold Layer structures data into a Fact Table (Transactions) and supporting Dimension Tables, enabling faster analytics and reporting.

#### Fact Table:
Transactions: Includes financial transaction details such as service dates, procedure codes, payment amounts, claim statuses, and provider information.

#### Dimension Tables:
* Patients: Demographics, insurance details, visit history.
* Providers: Hospital and physician details.
* Departments: Mapping of providers to specific hospital units.
* ICD Codes: Standardized medical diagnosis codes.
* NPI Codes: Provider identification and associated organizational details.

Queries were executed in **Synapse Serverless to calculate Total Charge Amount per provider by department and Total Charge Amount per provider by department for each month in 2024**, providing financial insights into revenue generation at the provider and department level.


This end-to-end data pipeline provides a scalable, efficient, solution for healthcare revenue tracking. By implementing SCD-2, CDM, and Fact-Dimension modeling, this system ensures accuracy, consistency, and actionable insights for financial decision-making. If you're interested in Azure Data Engineering, Healthcare Analytics, or Cloud Solutions, let’s connect and discuss! 
