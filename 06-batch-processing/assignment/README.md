# Batch Processing with Apache Spark - Assignment

## Objective

In this assignment, you will practice a simple local batch-processing workflow with Apache Spark. You will generate your own dataset, store it as Parquet, process it with Spark transformations, and write the transformed output back to storage.

**Focus:** Local Spark only (`local[*]`). Do **not** use Google Cloud Storage or any cloud connector.

## Assignment Tasks

### 1. Generate or Extract Data

- Generate or extract any dataset of your choice.
- The dataset should be meaningful enough to transform (for example: transactions, logs, events, or user activity).
- Save the raw data in **Parquet** format in a local folder, such as `data/raw/`.

**Suggested minimum:**
- At least 20 rows
- At least 4 columns
- A mix of data types (e.g., string, number, date/timestamp)

### 2. Set Up Spark

- Create a local Spark session using PySpark.
- Example:

  ```python
  from pyspark.sql import SparkSession
  from pyspark.sql import functions as F

  spark = (
      SparkSession.builder
      .master("local[*]")
      .appName("batch-processing-assignment")
      .getOrCreate()
  )
  ```

### 3. Load the Data into Spark

- Load the raw Parquet data into a Spark DataFrame.
- Inspect the data:
  - Print the schema with `printSchema()`
  - Show sample rows with `show()`
  - Check the row count with `count()`

### 4. Transform the Data

Apply Spark transformations to the loaded DataFrame. Your transformations should include at least:

- A `filter` or `where`
- A `select` or `withColumn`
- One aggregation using `groupBy` and `agg` (or an equivalent Spark SQL transformation)

### 5. Write the Transformed Data Back to Storage

- Write the transformed DataFrame back to local storage in **Parquet** format.
- Use a clear output path, such as `data/processed/`.
- Use `mode("overwrite")` so the job can be rerun safely.

Example:

```python
transformed_df.write.mode("overwrite").parquet("data/processed/")
```

### 6. Upload Your Spark Script to GitHub

- Save your work as a Spark Python script (`.py`) or a notebook, as instructed.
- Upload the script/project to GitHub.
- Include a short `README.md` that explains:
  - What dataset you used/generated
  - Where the raw Parquet data is stored
  - What transformations you applied
  - Where the transformed output is written
  - How to run the script
