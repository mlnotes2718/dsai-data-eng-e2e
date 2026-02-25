from pathlib import Path

from dagster_dbt import DbtProject

dbt_hdb_resale_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "dbt_hdb_resale").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
dbt_hdb_resale_project.prepare_if_dev()