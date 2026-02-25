# definitions.py
from dagster import Definitions, AssetSelection, define_asset_job, ScheduleDefinition
from dagster_dbt import DbtCliResource
from .assets import dbt_hdb_resale_dbt_assets, pipeline_meltano
from .project import dbt_hdb_resale_project

all_assets_job = define_asset_job(
    name="all_assets_job",
    selection=AssetSelection.assets(pipeline_meltano) | AssetSelection.assets(dbt_hdb_resale_dbt_assets)
)

daily_schedule = ScheduleDefinition(
    name="daily_materialization_schedule",
    job=all_assets_job,
    cron_schedule="0 0 * * *",
)

defs = Definitions(
    assets=[pipeline_meltano, dbt_hdb_resale_dbt_assets],
    schedules=[daily_schedule],
    jobs=[all_assets_job],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_hdb_resale_project)
    },
)