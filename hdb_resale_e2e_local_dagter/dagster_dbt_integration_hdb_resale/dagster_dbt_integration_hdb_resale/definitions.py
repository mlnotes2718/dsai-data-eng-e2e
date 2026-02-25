# definitions.py
from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import dbt_hdb_resale_dbt_assets, pipeline_meltano
from .project import dbt_hdb_resale_project
from .schedules import schedules

defs = Definitions(
    assets=[pipeline_meltano, dbt_hdb_resale_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_hdb_resale_project)
    },
)