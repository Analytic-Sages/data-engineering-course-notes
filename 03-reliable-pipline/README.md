# Google Cloud CLI Guide: GCS and BigQuery from the Terminal

A practical guide to doing from the command line what you first did in the GCP UI.
The Google Cloud SDK installs three tools at once:

| Tool | Purpose |
|---|---|
| `gcloud` | Auth, projects, config, IAM, and most GCP services (including `gcloud storage` for GCS) |
| `gsutil` | Older GCS tool (still works, but `gcloud storage` is now recommended) |
| `bq` | BigQuery: datasets, tables, loads, queries |

---

## 1. Install the Google Cloud SDK

Official install page: https://cloud.google.com/sdk/docs/install

### Windows
1. Download `GoogleCloudSDKInstaller.exe` from the install page.
2. Run it and keep the defaults.
3. Reopen PowerShell / Command Prompt so PATH updates.

### macOS
```bash
brew install --cask google-cloud-sdk
```

### Linux / WSL
```bash
curl https://sdk.cloud.google.com | bash
exec -l $SHELL        # restart the shell
```

### GitHub Codespaces / remote Linux
Same Linux command as above. Note: there is no local browser, so use the
`--no-launch-browser` login flow (see section 3).

---

## 2. Verify the installation

```bash
gcloud --version
gsutil --version
bq version
```

All three should print version information. If you get `command not found`:
- Close and reopen the terminal.
- Check that the SDK's `bin` folder is on your PATH.

> **Harmless warning:** you may see a `RequestsDependencyWarning` about
> urllib3/chardet from `bq`. It is a library version mismatch inside the SDK's
> bundled Python and does not affect functionality. Silence it with
> `export PYTHONWARNINGS=ignore`.

---

## 3. Initialize / log in

### Normal machine (has a browser)
```bash
gcloud init
```
This logs you in, lets you pick a project, and optionally sets a default region.

### Codespaces / headless machine (no browser)
```bash
gcloud auth login --no-launch-browser
```
It prints a URL. Open it in your own browser, sign in, approve access, and follow
the on-screen instructions to complete the login in the terminal.

### Pick your project
```bash
gcloud projects list                        # find the project ID
gcloud config set project YOUR_PROJECT_ID
```
Use the project **ID**, not the display name. They are often different.

---

## 4. Confirm the initialization worked

```bash
gcloud auth list          # which account is active
gcloud config list        # which project is set
```

What to look for:
- `gcloud auth list`: your account should appear with an asterisk (`*`) marking it active.
- `gcloud config list`: `account` and `project` should both be filled in.

Make this a habit: **always check `gcloud config list` before running commands**,
so you never create or delete resources in the wrong project.

---

## 5. Show existing buckets and datasets

### List GCS buckets
```bash
gcloud storage ls
```

### List the contents of a bucket
```bash
gcloud storage ls gs://YOUR_BUCKET
gcloud storage ls -r gs://YOUR_BUCKET        # recursive
gcloud storage ls -l gs://YOUR_BUCKET        # with sizes and dates
```

### List BigQuery datasets
```bash
bq ls
```

### List the tables inside a dataset
```bash
bq ls YOUR_DATASET
```

### Inspect a table's schema and metadata
```bash
bq show YOUR_DATASET.YOUR_TABLE
bq show --schema --format=prettyjson YOUR_DATASET.YOUR_TABLE
```

---

## 6. Create a new BigQuery dataset

```bash
bq mk --dataset --location=EU YOUR_PROJECT_ID:my_new_dataset
```

Useful options:
```bash
bq mk --dataset \
  --location=EU \
  --description="Raw ingested data" \
  YOUR_PROJECT_ID:raw_data
```

Rules and tips:
- Dataset names can contain letters, numbers, and underscores (no hyphens).
- **Location cannot be changed later.** Choose it deliberately (see Lesson 1).

---

## 7. Create a new GCS bucket

```bash
gcloud storage buckets create gs://my-unique-bucket-name --location=EU
```

Useful options:
```bash
gcloud storage buckets create gs://my-unique-bucket-name \
  --location=EU \
  --uniform-bucket-level-access
```

Rules and tips:
- Bucket names are **globally unique across all of GCP**. If the name is taken,
  add something unique (for example, your project ID as a suffix).
- Names must be lowercase letters, numbers, hyphens, and underscores.
- Location is fixed once the bucket is created.

---

## 8. Copy a file from local to GCS

### Single file
```bash
gcloud storage cp ./data.csv gs://my-unique-bucket-name/raw/data.csv
```

### Whole folder
```bash
gcloud storage cp -r ./my_folder gs://my-unique-bucket-name/raw/
```

### Many files by pattern
```bash
gcloud storage cp ./*.parquet gs://my-unique-bucket-name/raw/
```

### Sync a folder (copy only what changed)
```bash
gcloud storage rsync -r ./my_folder gs://my-unique-bucket-name/raw/
```

### Copy back from GCS to local
```bash
gcloud storage cp gs://my-unique-bucket-name/raw/data.csv ./data.csv
```

> Note: GCS has no real folders. `raw/` is just part of the object name
> (a prefix). The UI simply displays prefixes as folders.

---

## 9. Load data from GCS into BigQuery

```bash
bq load \
  --autodetect \
  --source_format=CSV \
  --skip_leading_rows=1 \
  YOUR_DATASET.YOUR_TABLE \
  gs://my-unique-bucket-name/raw/data.csv
```

For Parquet (recommended for larger data, because the schema comes with the file):
```bash
bq load \
  --source_format=PARQUET \
  YOUR_DATASET.YOUR_TABLE \
  gs://my-unique-bucket-name/raw/data.parquet
```

Append vs replace:
```bash
bq load --replace ...     # overwrite the table
bq load ...               # appends by default
```

---

## 10. Run queries from the CLI

```bash
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) AS rows FROM `YOUR_PROJECT_ID.YOUR_DATASET.YOUR_TABLE`'
```

**Check the cost before running** with a dry run:
```bash
bq query --use_legacy_sql=false --dry_run \
  'SELECT * FROM `YOUR_PROJECT_ID.YOUR_DATASET.YOUR_TABLE`'
```
It prints how many bytes the query would process, without running it.

---

## 11. Cleaning up (be careful, deletes are permanent)

```bash
# Delete one object
gcloud storage rm gs://my-unique-bucket-name/raw/data.csv

# Delete a bucket and everything in it
gcloud storage rm -r gs://my-unique-bucket-name

# Delete a BigQuery table
bq rm -t YOUR_DATASET.YOUR_TABLE

# Delete a dataset and all its tables
bq rm -r -d YOUR_DATASET
```

Always run `gcloud config list` first and double-check the name you are deleting.

---

## 12. Important lessons

### Lesson 1: Keep locations consistent
A BigQuery dataset and the GCS bucket you load from should be in the **same
location** (for example both `EU`, or both `US`). Mismatched locations cause load
errors, and neither can be moved after creation.

### Lesson 2: Always know which project you are in
```bash
gcloud config list
```
Most "I can't see my bucket" and "permission denied" problems come from being
pointed at the wrong project or account.

### Lesson 3: Enable the APIs you need
A fresh project may not have the services turned on. If you get an
"API has not been used" error:
```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
```

### Lesson 4: Your login vs service accounts
`gcloud auth login` is **you**. Scripts and pipelines should run as a
**service account**, not your personal login:
```bash
gcloud iam service-accounts create pipeline-sa

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

### Lesson 5: Least privilege
Give each service account only the roles it needs. For example,
`roles/storage.objectViewer` to read from GCS, `roles/bigquery.dataEditor`
to write tables, `roles/bigquery.jobUser` to run queries. Avoid `Owner` or
`Editor` for pipelines.

### Lesson 6: Application Default Credentials (ADC) for code
Python client libraries (and many tools) look for ADC automatically:
```bash
gcloud auth application-default login
# headless machines:
gcloud auth application-default login --no-launch-browser
```
Note that `gcloud auth login` and `gcloud auth application-default login` are
**two separate logins**. The first is for CLI commands, the second is for code.

### Lesson 7: Avoid downloading service account key files
JSON key files are a common source of leaked credentials. Prefer ADC locally and
attached service accounts in cloud environments. If you ever must use a key file,
never commit it to Git (add it to `.gitignore`).

### Lesson 8: Use named configurations for multiple projects
```bash
gcloud config configurations create dev
gcloud config configurations create prod
gcloud config configurations activate dev
gcloud config configurations list
```
Each configuration stores its own account and project, so switching is one command.

### Lesson 9: Prefer Parquet over CSV
Parquet keeps schema and types, is compressed, and loads faster and cheaper than
CSV. This matters a lot for large on-chain datasets.

### Lesson 10: BigQuery costs follow bytes scanned
- Never `SELECT *` on big tables. Select only the columns you need.
- Use `--dry_run` to estimate cost first.
- Use **partitioned** and **clustered** tables for large datasets so queries scan less.

### Lesson 11: Add `--help` to anything
```bash
gcloud storage --help
gcloud storage cp --help
bq load --help
```
The built-in help is accurate to your installed version and is the fastest way to
check a flag.

### Lesson 12: Update the SDK occasionally
```bash
gcloud components update
```
(If you installed through a package manager such as apt or Homebrew, update through
that instead.)

### Lesson 13: Turn commands into scripts
Once you have run a command by hand a few times, put it in a shell script and add
safety flags so it stops on the first error:
```bash
#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="your-project-id"
BUCKET="gs://my-unique-bucket-name"
DATASET="raw_data"

gcloud storage cp ./data.parquet "$BUCKET/raw/data.parquet"
bq load --source_format=PARQUET "$DATASET.swaps" "$BUCKET/raw/data.parquet"
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET.swaps\`"
```

---

## 13. Quick reference cheat sheet

| Task | Command |
|---|---|
| Check versions | `gcloud --version`, `bq version` |
| Log in | `gcloud auth login` (or `--no-launch-browser`) |
| Active account | `gcloud auth list` |
| Current config | `gcloud config list` |
| Set project | `gcloud config set project PROJECT_ID` |
| List buckets | `gcloud storage ls` |
| List datasets | `bq ls` |
| Create bucket | `gcloud storage buckets create gs://NAME --location=EU` |
| Create dataset | `bq mk --dataset --location=EU PROJECT:DATASET` |
| Upload file | `gcloud storage cp ./file gs://BUCKET/path/` |
| Load to BigQuery | `bq load --source_format=PARQUET DATASET.TABLE gs://BUCKET/file` |
| Run query | `bq query --use_legacy_sql=false 'SQL'` |
| Estimate cost | `bq query --use_legacy_sql=false --dry_run 'SQL'` |
| Delete object | `gcloud storage rm gs://BUCKET/path` |
| Delete table | `bq rm -t DATASET.TABLE` |

---

## 14. What to learn next

1. Write a shell script that does upload, load, and validate in one run.
2. Create a service account and use ADC.
3. Redo the same flow in Python with `google-cloud-storage` and `google-cloud-bigquery`.
4. Learn partitioned and clustered BigQuery tables.
5. Later: Terraform (infrastructure as code) and a scheduler or orchestrator (Cloud Scheduler, Airflow) to run it all automatically.