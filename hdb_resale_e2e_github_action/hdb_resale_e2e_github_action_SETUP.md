# DSAI - HDB Resale Price End to End Pipeline Setup 1 - Github Action

## HDB Resale Price End to End Pipeline - Setup Meltano

### Add an Extractor to Pull Data from Postgres (Supabase)

We will use the `tap-postgres` extractor to pull data from a Postgres database hosted on [Supabase](https://supabase.com). 

The Postgres database table with the HDB housing data of resale flat prices based on registration date from Jan-2017 onwards. It is the same data that we used in 2.6. We will be extracting data from a data ready postgres server, if you want to setup and upload your data please refer to the setup guide in exercise 2.6.

From Supabase, take note of your connection details from the Connection window, under Session Spooler:

```yaml
host: aws-0-us-east-2.pooler.supabase.com
port: 5432
database: postgres
user: postgres.ufkutyufdohbogiqgjel
pool_mode: session
```

We're going to add an extractor for Postgres to get our data. An extractor is responsible for pulling data out of any data source. We will use the `tap-postgress` extractor to pull data from the Supabase server. 

At the root folder, create a new Meltano project by running:

```bash
meltano init meltano_hdb_resale
```
```bash
cd meltano_hdb_resale
```

To add the extractor to our project, run:

```bash
meltano add tap-postgres
```

Next, configure the extractor by running:

```bash
meltano config set tap-postgres --interactive
```

Configure the following options:

- `database`: `postgres`
- `filter_schemas`: `['public']`
- `host`: `aws-0-ap-southeast-1.pooler.supabase.com` *(example)*
- `password`: *database password*
- `port` : `5432`
- `user`: *postgres.username*

Add the following to the meltano.yml file:


Test your configuration:
```bash
meltano config test tap-postgres
```
Use the following command to list all
```bash
meltano select tap-postgres --list --all
```

Next, we need to select the table that we need:
```bash
meltano select tap-postgres "public-resale_flat_prices_from_jan_2017"
```

Use the following command to check what we selected
```bash
meltano select tap-postgres --list
```


### Add an Loader to Load Data to BigQuery
We will now add a loader to load the data into BigQuery.
```bash
meltano add target-bigquery
```

```bash
meltano config set target-bigquery --interactive
```

Set the following options:

- `batch_size`: `104857600`
- `credentials_path`: _full path to the service account key file_
- `dataset`: `postgres_hdb_resale_raw`
- `denormalized`: `true`
- `flattening_enabled`: `true`
- `flattening_max_depth`: `1`
- `location`: `US`
- `method`: `batch_job`
- `overwrite`: `true`
- `project`: *your_gcp_project_id*

### Setting Overwrite

Comment out the original
```yml
# environments:
# - name: dev
# - name: staging
# - name: prod
```

Add the following 

```yml
# Environment-specific overrides
environments:
  - name: dev
    config:
      plugins:
        loaders:
          - name: target-bigquery
            config:
              dataset: dev_postgres_hdb_resale_raw
              # Path to your dev/shared service account key
              # credentials_path: /path/to/your/dev-service-account.json -- keep for future

  - name: prod
    config:
      plugins:
        loaders:
          - name: target-bigquery
            config:
              dataset: prod_postgres_hdb_resale_raw
              # Path to your prod service account key
              # credentials_path: /path/to/your/dev-service-account.json -- keep for future
```

Checking
```bash
# Check configuration for dev
meltano --environment=dev config list target-bigquery

# Check configuration for prod
meltano --environment=prod config list target-bigquery 
```

### Run Supabase (Postgres) to BigQuery

We can now run the full ingestion (extract-load) pipeline from Supabase to BigQuery.

```bash
meltano --environment=dev run tap-postgres target-bigquery
```

You will see the logs printed out in your console. Once the pipeline is completed, you can check the data in BigQuery.



## HDB Resale Price End to End Orchestration Setup dbt

Let's create a Dbt project to transform the data in BigQuery. 

To create a new dbt project. (make sure you exited the meltano folder)

```bash
dbt init dbt_hdb_resale
```

Fill in the required config details. 
- use service account
- add your path of the json key file
- dataset: `hdb_resale`
- project: your GCP project ID

Please note that the profiles is located at the hidden folder .dbt of your home folder. The `profiles.yml` that is located in the home folder includes multiple projects. Alternatively, you can create a separate `profiles.yml` for each project.

To create separate profiles for each project, create a new file called `profiles.yml` under `resale_flat` folder. Then copy the following to `profiles.yml`. Remember to change your key file location and your project ID.
```yaml
dbt_hdb_resale:
  outputs:
    dev:
      dataset: dev_hdb_resale
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: *<your_service_key_path>*
      location: US
      method: service-account
      priority: interactive
      project: *<gcp-project-id>*
      threads: 2
      type: bigquery
    prod:
      dataset: prod_hdb_resale
      job_execution_timeout_seconds: 300
      job_retries: 5
      keyfile: *<your_service_key_path>*
      location: US
      method: service-account
      priority: interactive
      project: *<gcp-project-id>*
      threads: 3
      type: bigquery
  target: dev
```

### Create source and models

We can start to create the source and models in the dbt project.

> 1. In `dbt_project.yml`, set the following:
```yml
models:
  dbt_hdb_resale:
    staging:
      +schema: stg    # Models in /models/staging/ go to 'staging'
      +materialized: view
    marts:
      +schema: marts      # Models in /models/marts/ go to 'marts'
      +materialized: table
```

> 2. Under `/models`, create 2 folder `staging` and `marts`

> 3. Create a `stg_hdb_resale.sql` model (materialized table) which selects all columns from the source table, perform some type conversion and calculating `remain_lease`.

```sql
{{ config(materialized='view') }}

WITH RAW AS (
    SELECT
        id,
        PARSE_DATE('%Y-%m', month) AS resale_month,
        town,
        flat_type,
        block,
        street_name,
        storey_range,
        CAST(floor_area_sqm AS FLOAT64) AS floor_area_sqm,
        flat_model,
        lease_commence_date,
        remaining_lease,
            -- Extract years, default to 0 if not found, multiply by 12
            COALESCE(CAST(REGEXP_EXTRACT(remaining_lease, r'(\d+) year') AS INT64), 0) * 12 +
            -- Extract months, default to 0 if not found
            COALESCE(CAST(REGEXP_EXTRACT(remaining_lease, r'(\d+) month') AS INT64), 0) 
        AS remaining_lease_months,
        CAST(resale_price AS FLOAT64) AS resale_price
    FROM {{ source('hdb_resale_source', 'public_resale_flat_prices_from_jan_2017') }}
)
SELECT
    *,
    resale_price / floor_area_sqm AS price_per_sqm
FROM raw
```

> 4. Under `/models/staging`, create  `stg_hdb_resale.yml` which contains the source and the schema.

```yml
version: 2

sources:
  - name: hdb_resale_source
    description: "Raw HDB resale data from Postgres source"
    schema: "{{ target.name }}_postgres_hdb_resale_raw"
    tables:
      - name: public_resale_flat_prices_from_jan_2017
        description: "Raw monthly HDB transaction records"

models:
  - name: stg_hdb_resale
    description: "Cleaned HDB resale data with standardized metrics."
    columns:
      - name: id
        tests:
          - unique
          - not_null

      - name: floor_area_sqm
        description: "The size of the flat in square meters."
        tests:
          - not_null
          # Ensures value is greater than 0
          - dbt_utils.expression_is_true:
              arguments:
                expression: "> 0"

      - name: resale_price
        description: "The total transaction price."
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              arguments:
                expression: "> 0"

      - name: price_per_sqm
        description: "Calculated unit price."
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              arguments:
                expression: "> 0"

      - name: storey_avg
        description: "The midpoint of the floor range (e.g., 06 TO 08 becomes 7)."

      - name: remaining_lease_months
        description: "Total remaining lease calculated as (Years * 12) + Months."
```

> 5. Under `/models/marts/`, create a `dim_prices_by_town_type_model.sql`, the model (materialized table) which selects the `town`, `flat_type` and `flat_model` columns from `prices`, group by them and calculate the average of `floor_area_sqm`, `resale_price` and `price_per_sqm`. Finally, sort by `town`, `flat_type` and `flat_model`.

```sql
{{ config(materialized='table') }}

SELECT
    town,
    flat_type,
    flat_model,
    AVG(floor_area_sqm) AS avg_floor_area_sqm,
    AVG(resale_price) AS avg_resale_price,
    AVG(price_per_sqm) AS avg_price_per_sqm
FROM {{ ref('stg_hdb_resale') }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
```

> 6. Under `/models/marts/`, create a `dim_prices_by_town_type_model.yml`, a schema with test.

```yml
version: 2

models:
  - name: dim_prices_by_town_type_model  # Ensure this matches your .sql filename
    description: "Aggregated HDB resale metrics grouped by town and flat characteristics."
    columns:
      - name: town
        description: "The HDB town name."
        tests:
          - not_null

      - name: avg_price_per_sqm
        description: "The mean price per square meter for this group."
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              arguments:
                expression: "> 0"

      - name: avg_resale_price
        description: "The mean transaction price for this group."
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              arguments:
                expression: "> 0"

      - name: flat_type
        description: "Type of flat (e.g., 3 ROOM, 4 ROOM, EXECUTVE)."
        tests:
          - not_null
```

### Run Dbt

Check dbt connection first

```bash
dbt debug
```

Optional: you can run `dbt clean` to clear any logs or run file in the dbt folders.

Run the following to install packages:
```bash
dbt deps
````

Run the dbt project to transform the data.

```bash
dbt run

# or

dbt run --full-refresh
```

You should see 2 new tables in the `resale_flat` dataset.

Tun the following to test:
```bash
dbt test
```

After testing is complete, run the following:
```bash
dbt build
```

### Generate Documents

Run the following to generate documents:
```bash
dbt docs generate
```

Run the following to review the automated documenation:
```bash
dbt docs serve
```

## Automation

1. Create a file at the root folder with `run.sh`

```bash
#!/bin/bash
set -e

# Start timer
START_TIME=$SECONDS

# Default to 'dev'
TARGET="dev"

# Check for the --prod flag
if [[ "$1" == "--prod" ]]; then
  TARGET="prod"
  echo "**********************************************************"
  echo "⚠️  WARNING: YOU ARE TARGETING PRODUCTION (MELTANO & DBT)"
  echo "*********************************************************"
else
  echo "🛠️  Running in DEVELOPMENT mode"
fi

# 1. Sync data with Meltano using Environment
echo "Step 1: Syncing data via Meltano ($TARGET)..."
cd meltano_hdb_resale
meltano --environment=$TARGET run tap-postgres target-bigquery
cd ..

# 2. Transform data with dbt
echo "Step 2: Installing dbt deps and building ($TARGET)..."
cd dbt_hdb_resale
dbt clean
dbt deps
dbt build --target $TARGET

# 3. Cleanup
echo "Step 3: Cleaning up..."
dbt clean

# Calculate duration
DURATION=$(( SECONDS - START_TIME ))
echo "--- ✅ Pipeline completed ($TARGET) in $((DURATION / 60))m $((DURATION % 60))s ---"
```

To run the script use the following for development:

```bash
./run.sh 

# or
./run.sh --dev
```

To run the script use the following for production:

```bash
./run.sh --prod
```
