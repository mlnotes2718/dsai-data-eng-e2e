"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster_dbt import build_schedule_from_dbt_selection
from dagster import AssetSelection
from .assets import dbt_hdb_resale_dbt_assets, pipeline_meltano

full_pipeline_selection = AssetSelection.assets(pipeline_meltano) | AssetSelection.assets(dbt_hdb_resale_dbt_assets)

# schedules = [
# #     build_schedule_from_dbt_selection(
# #         [dbt_hdb_resale_dbt_assets],
# #         job_name="materialize_dbt_models",
# #         cron_schedule="0 0 * * *",
# #         dbt_select="fqn:*",
# #     ),
# ]

schedules = [
    # 2. Use the .to_job() method on the selection. 
    #    This is the most compatible way across Dagster versions.
    full_pipeline_selection.to_job(
        name="materialize_all_assets",
    ).to_schedule(
        name="daily_materialization_schedule",
        cron_schedule="0 0 * * *",
    ),
]