#!/bin/bash
set -e

# Start timer
START_TIME=$SECONDS

# 1. Sync data with Meltano using Environment
echo "Step 1: Syncing data via Meltano ($TARGET)..."
cd meltano_hdb_resale
meltano run tap-postgres target-bigquery
cd ..

# 2. Transform data with dbt
echo "Step 2: Installing dbt deps and building ($TARGET)..."
cd dbt_hdb_resale
dbt clean
dbt deps
dbt build 

# 3. Cleanup
echo "Step 3: Cleaning up..."
dbt clean

# Calculate duration
DURATION=$(( SECONDS - START_TIME ))
echo "--- ✅ Pipeline completed ($TARGET) in $((DURATION / 60))m $((DURATION % 60))s ---"