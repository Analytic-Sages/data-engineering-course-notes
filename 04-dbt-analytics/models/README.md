# dbt Models

This directory contains the dbt models for the analytics project, organized in layers:

- `staging/` — Cleaned and renamed source data (e.g. `stg_dex_swap.sql`)
- `intermediate/` — Light transformations between staging and marts (e.g. `int_dex_swap.sql`)
- `marts/` — Business-level models consumed by downstream tools (e.g. `fct_dex_swap.sql`)

## Running the models

Run all models in this directory:

```bash
dbt run --select models/
```

Run a single model (with its upstream dependencies):

```bash
dbt run --select +stg_dex_swap.sql
```

Run only models in a specific layer:

```bash
dbt run --select staging
dbt run --select marts
```

Rebuild everything from scratch (full refresh):

```bash
dbt run --select models/ --full-refresh
```

## Testing the models

Run all tests defined in the `schema.yml` files (unique, not_null, relationships, etc.):

```bash
dbt test
```

Test only the models in this directory:

```bash
dbt test --select models/
```

Test a single model:

```bash
dbt test --select stg_dex_swap
```

## Building models and tests in one step

`dbt build` runs `dbt seed`, `dbt run`, and `dbt test` together in the correct order, and stops on the first failure:

```bash
dbt build
```

Build only the models in this directory with their tests:

```bash
dbt build --select models/
```

Build a single model, its tests, and its upstream dependencies:

```bash
dbt build --select +stg_dex_swap.sql
```

## Loading seed data

If the project uses seed files (CSVs in the `seeds/` directory), load them into the warehouse before running the models:

```bash
dbt seed
```


Use `--full-refresh` to reload a seed that already exists:

```bash
dbt seed --full-refresh
```
