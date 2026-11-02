
FROM apache/airflow:3.3.1-python3.12

USER root

# curl is used by the compose healthchecks
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-fab" \
    "web3>=6.0" \
    "psycopg2-binary>=2.9" \
    "psycopg[binary]>=3.1" \
    "python-dotenv>=1.0" \
    "dbt-postgres>=1.7"\
    "dbt-core>=1.7" 
