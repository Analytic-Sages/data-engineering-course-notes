Created README.md
Viewed README.md:1-31

# Module 03: Analytics Engineering with dbt

In this module, we introduce **dbt (Data Build Tool)**, the industry standard for managing SQL transformations, and apply it to our raw blockchain transaction data to build clean, model-driven datasets.

---

## Core Concepts Covered in this Module

### A. The ELT Framework
Instead of transforming data before loading it (ETL), we load raw blockchain logs directly into our PostgreSQL database first, then use the database engine's processing power to run all of our transformations.

### B. dbt (Data Build Tool)
We use dbt to write modular, version-controlled SQL queries. dbt automatically:
* Handles table dependencies (knowing which model must be created first).
* Translates SQL `SELECT` statements into raw tables, views in the database.
* Allows for automated data quality testing (such as checking for duplicates or null values).

### C. Reference Data & Dataset Enrichment (Seeds)
Raw blockchain data doesn't come with context (like token symbols or asset values in USD). We use **dbt seeds** to load reference CSV datasets (like token names, decimals, and price history) directly into the database. We then join this reference data with our raw transactional data to calculate actual transaction valuations.

### D. The Three-Tier Modeling Pipeline
We clean and enrich our data by organizing it into three logical layers:
1. **Staging Layer (`stg`)**: The raw interface. Here, we select from our raw source tables to clean up column names, standardize addresses to lowercase, cast timestamps, and filter out zero-value transactions.
2. **Intermediate Layer (`int`)**: The processing layer. Here, we join the cleaned transaction records with token metadata to normalize the raw blockchain integers into real token quantities (e.g., dividing by token decimals).
3. **Marts Layer (`fct`/`dim`)**: The analyst-facing layer. This is the finalized, dashboard-ready table containing enriched information (such as transaction amounts in USD) ready for business intelligence tools.