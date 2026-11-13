# Assignment 05: Orchestrate the Pipeline

## Goal

Build one or more Airflow DAG that runs your data pipeline on a schedule and tells you when it fails.

You already have a working extractor and dbt project. This assignment is only about **orchestrating** them: a schedule, a block range, retries, and an alert that reaches a human.

---

## Task

Create `Orchestration/dags/dex_pipeline.py` with this shape:

```
extract_swaps ──▶ dbt_run ──▶ dbt_test
```

- `extract_swaps`: Bash task. Runs `extract_swap.py` with `--from-block` and `--to-block`.
- `dbt_run`: Bash task.
- `dbt_test`: Bash task, separate from `dbt_run` so a data failure is its own red task.


### The five requirements

1. **Schedule**: a cron schedule, 
2. **State**: `params` expose `from_block` and `to_block`, 
3. **Retries**: `retries=2` and a `retry_delay` on `extract_swaps`.
4. **Alert**: an `on_failure_callback` that sends a Slack message containing `dag_id`, `task_id`, `run_id`, and the block range.

---

## Run it

```bash
docker compose up -d --build

Open <http://localhost:8080>, log in with `admin` / `admin`.

```
## Submit

```
Orchestration/dags/dex_pipeline.py 
Add the screenshot  of your Airflow Successfull runs to the Readme
```
