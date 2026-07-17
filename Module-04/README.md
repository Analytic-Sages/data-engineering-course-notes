# Module 04: Introduction to Data Orchestration

## 1. The Disjointed Pipeline Bottleneck

So far in this curriculum, we have successfully built the core layers of our blockchain data pipeline:
* **Ingestion**: Using the Envio indexer to extract raw `Transfer` event logs from the blockchain and load them into our PostgreSQL database.
* **Transformation**: Using dbt to stage, clean, and enrich our raw datasets.

However, our current workflow is completely **disjointed**. To get updated data, we must run the indexer, manually load reference CSV files (seeds), and run `dbt build` commands step-by-step. 

While the Envio indexer runs continuously in the background, constantly adding new raw blocks and transfer events to our database, our dbt transformations only run once when manually executed. To ensure our analytical models and USD valuations stay fresh, we need a reliable system to **orchestrate** this workflow independently.

---

## 2. Why Do We Need Orchestration?

In production data engineering, a transformation pipeline must run automatically at regular intervals (e.g., hourly or daily). This schedule ensures that incoming raw data is processed, validated, and made available for downstream consumption (like dashboards or reports) without manual intervention.

An orchestrator solves this by managing:
1. **Schedules**: Defining exactly when and how often jobs run.
2. **Dependencies**: Ensuring tasks run in the correct order (e.g., ensuring new price data is loaded before running dbt models).
3. **Failures**: Monitoring runs and alerting engineers if something breaks.

---

## 3. Abstracting Reference Data: From Seeds to Ingestion Scripts

In our transformation layer, we joined transactional transfer events with token metadata and token prices. In the previous module, we accomplished this using static reference CSVs via `dbt seed`.

While this works for static metadata (like token decimals and names), **token prices are highly dynamic**. Updating a CSV file manually every day is not scalable. 

To solve this, orchestration allows us to abstract this process:
* We can replace the static `dbt seed` reference files with a **dynamic ingestion script**.
* This script can be scheduled by our orchestrator to call a public market API (such as CoinGecko), fetch the latest price changes, and write them directly into a database table.
* Once the prices are updated, the orchestrator triggers the downstream dbt transformations.

Whether you use CSV files or dynamic ingestion scripts, data orchestration is the glue that tidies up every step of the data lifecycle.

---

## 4. Introducing Apache Airflow

In this module, we will explore **Apache Airflow**, the industry standard for workflow orchestration. 

With Airflow, we define our data pipelines as **DAGs** (Directed Acyclic Graphs) using Python. A DAG specifies the tasks in our pipeline and the dependencies between them, allowing us to:
* Schedule price ingestion scripts.
* Trigger dbt runs and data quality tests automatically.
* Monitor our pipeline health through a visual web interface.

By the end of this module, you will have a fully automated, production-ready blockchain data pipeline.
