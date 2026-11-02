# Assignment: Model Any Dataset with dbt

## Task

Pick **any** dataset you like (your own swap extraction, a CoinGecko API, a CSV, a public open-data table, anything), get it into Postgres, and build a dbt project that models it in three layers:

```
staging       →  clean, cast, rename only
intermediate  →  the logic and decisions
marts         →  the business table downstream tools read
```

The submission must be a **reproducible repo**: someone who clones it from GitHub can run the whole thing (extraction → Postgres → `dbt build`) without asking you a single question.

## Must Submit these files

1. `README.md` (repo root): a clean, complete guide for anyone trying to reproduce your work. What the dataset is, the prerequisites, and the exact commands from clone to `dbt build`. Nothing left for the reader to guess or ask you about.
2. `docker-compose.yml` + `.env.example`: Postgres in Docker (you know this from module 01). `.env.example` lists every variable needed; `.env` is gitignored; no hardcoded host/password anywhere.
3. `extract/`: the script that puts **your** data into Postgres (API, CSV, file, your choice). Must be **idempotent** (re-running the same input doesn't duplicate rows) and have its own short README with the exact command to run it. Reusing the lesson's Uniswap extractor? Copy it in and say so.
   - If your source is an API that can change, rate-limit, or expire: **commit a snapshot** (CSV/JSON) of the raw data you used, so the pipeline still runs when the API doesn't.
4. `dbt_project.yml` + `profiles.yml`: project initialized by you, Postgres target via env vars:

```yaml
analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', 'localhost') }}"
      port: "{{ env_var('DB_PORT', '5432') }}"
      user: "{{ env_var('DB_USER') }}"
      password: "{{ env_var('DB_PASS') }}"
      dbname: "{{ env_var('DB_NAME') }}"
      schema: public
```

5. `models/staging/sources.yml` + `stg_<your_table>.sql`: raw table declared as a **source**; `source()` is used only in this layer. Cast once, rename once, **no business logic**.
6. `models/intermediate/int_<something>.sql`: `ref()`s the staging model; this is where the decisions live (derived categories, direction, ratios, joins, renames for the business).
7. `models/marts/fct_*.sql` (or `dim_*.sql`): `materialized='table'`, `ref()`s intermediate only, at a stated business grain.
8. One `schema.yml` per layer: description for the model and every column, `not_null` on every column, and `accepted_values` on at least one column you derive.

## Run from scratch (this is what we will test)

Your repo's top-level README must document these exact steps, and they must work on a clean machine:

```bash
git clone <your-repo> && cd <your-repo>
cp .env.example .env              # fill DB_*, never commit .env
docker compose up -d              # Postgres
uv sync                           # dependencies
uv run <your extract command>     # raw data → Postgres
uv run dbt build                  # models + tests, all green
```



