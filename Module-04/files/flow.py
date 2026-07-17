from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='ethereum_transfers_pipeline',
    default_args=default_args,
    description='An hourly pipeline to ingest token prices and build dbt transformations',
    # schedule='@hourly',             # Runs once every hour
    schedule=timedelta(minutes=10), # Run every 5 minutes
    start_date=datetime(2026, 7, 16),        # Date of the first block processed
    catchup=False,                            # Auto-backfills historical runs
    tags=['blockchain', 'dbt'],
)
def ethereum_transfers_pipeline():

    # Task 1: Ingest Token Prices (Using BashOperator to call our ingest script)
    # We pass the dynamic hourly timestamps directly using Airflow's Jinja templates
    ingest_prices = BashOperator(
        task_id='ingest_token_prices',
        bash_command=(
            'python3 /opt/airflow/dags/ingest_prices.py '
            '--start {{ data_interval_start.int_timestamp }} '
            '--end {{ data_interval_end.int_timestamp }}'
        )
    )

    # Task 2: Run and Test dbt Models
    # Runs 'dbt build' in the mounted warehouse folder
    dbt_transform = BashOperator(
        task_id='dbt_transformations',
        bash_command='cd /opt/airflow/dbt_warehouse && dbt build --profiles-dir .'
    )

    # Setting Task Dependencies
    # Prices must be populated in the DB before dbt joins them
    ingest_prices >> dbt_transform

# Instantiate the DAG
pipeline = ethereum_transfers_pipeline()
