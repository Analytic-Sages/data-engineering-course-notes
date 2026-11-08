# Assignment: GCS Ingestion + BigQuery External Table

**Goal:** Extract ERC-20 Transfer events from an Ethereum RPC, load partitioned Parquet into GCS, connect the data to BigQuery via an external table, and query it without copying data into BigQuery storage.

## Setup
Clone this assignment directory and use it as your working directory:

```bash
git clone <YOUR_REPO_URL>
cd Curriculum/03-reliable-pipline/Assignment
```

Install the Python dependencies for the pipeline:

```bash
uv sync
```

Create a new `.ipynb` notebook in this directory (next to `pipeline.py`).

**Notebook rules:**
- Run one command per cell - do not combine multiple commands in a single cell.
- Use the `!` prefix for shell commands.
- Leave the successful output in each cell - do not clear cell outputs before submitting.

Example cell:

```python
!uv run pipeline.py extract --bucket assignment --from-block 19000000 --to-block 19002000
```

## Terminal first (security)
Run these once in your terminal, not inside the notebook:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
gcloud services enable bigquery.googleapis.com storage.googleapis.com
```

## Steps (run all in the notebook)

**1. Create GCS bucket** (`assignment`, location `EU`)
```bash
!gcloud storage buckets create gs://assignment --location=EU
```

**2. Create BigQuery dataset** (same location as the bucket)
```bash
!bq mk --dataset --location=EU YOUR_PROJECT_ID:raw_assign
```

**3. Extract RPC to GCS**
```bash
!uv run pipeline.py extract --bucket assignment --from-block 19000000 --to-block 19002000
```

**4. Verify files landed in GCS**
```bash
!gcloud storage ls -r gs://assignment/raw/erc20_transfers/
```

**5. Create BigQuery external table**
```bash
!uv run pipeline.py external-table --bucket assignment --dataset raw_assign
```

**6. Query with a partition filter** (choose any `dt` that exists in your data)
```bash
!uv run pipeline.py query --bucket assignment --dataset raw_assign --dt 2024-01-01
```

**7. Prove the external table picks up new data automatically**
```bash
!uv run pipeline.py extract --bucket assignment --from-block 19002000 --to-block 19004000
```

```bash
!uv run pipeline.py query --bucket assignment --dataset raw_assign --dt 2024-01-02
```

## Deliverable
Upload the `.ipynb` notebook to GitHub with all cells and their outputs:
- Auth, bucket, dataset creation
- Data extracted to GCS
- External table created over the GCS prefix
- Query results for your chosen `dt`
- Answer: what happens if you run the query without `WHERE dt = ...`?