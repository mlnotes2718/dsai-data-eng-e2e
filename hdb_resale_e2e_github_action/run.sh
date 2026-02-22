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
meltano config test tap-postgres
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