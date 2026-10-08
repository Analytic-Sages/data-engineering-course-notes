# Assignment: Ingest Any Crypto Data into Postgres with Docker Compose

## Task
Pick any crypto dataset (e.g. Bitcoin mempool API, CoinGecko CSV, Binance CSV). Build a pipeline that loads it into Postgres running in Docker.

## Must Submit this files:
1. `ingest.py`:`  reads your crypto data (API or CSV) with Pandas, cleans it, loads to Postgres with SQLAlchemy. Must use `os.getenv("DB_HOST", "localhost")`, not hardcoded `localhost`.
2. `Dockerfile`: builds your script (`FROM`, `WORKDIR`, `COPY`, `RUN`, `ENTRYPOINT`)
3. `docker-compose.yml`: 2 services: `db` (postgres + env + volume + ports) and `ingest` (`build: .` + `DB_HOST: "db"` + `depends_on: db`)

## Run + Prove
1. `docker compose up --build`
2. `docker ps` 
3. `SELECT * FROM your_table LIMIT 5;` from pgcli/DBeaver.
