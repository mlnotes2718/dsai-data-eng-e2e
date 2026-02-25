# assets.py
import os
import subprocess
from dagster import AssetExecutionContext, asset, Config, EnvVar
from dagster_dbt import DbtCliResource, dbt_assets
from .project import dbt_hdb_resale_project

# --- 1. DEFINE THE CONFIG SCHEMA HERE ---
class PipelineConfig(Config):
    # This tells Dagster to look for "TARGET". 
    # If it's not found in the OS, it falls back to "dev".
    target_env: str = os.getenv("TARGET", "dev")

# --- 2. USE IT IN YOUR ASSETS ---

@asset(compute_kind="meltano")
def pipeline_meltano(config: PipelineConfig) -> None:
    # 1. Get the directory where THIS assets.py file lives
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 2. Go up to the project root and then into the meltano folder
    # This assumes meltano_hdb_resale is one level up from assets.py
    cwd = os.path.abspath(os.path.join(current_dir, "..", "..", "meltano_hdb_resale"))
    
    # Debugging: This will show up in Dagster logs so you can see where it's looking
    print(f"Looking for Meltano in: {cwd}")

    cmd = ["meltano", "--environment", config.target_env, "run", "tap-postgres", "target-bigquery"]
    
    try:
        # Check if the directory actually exists before running
        if not os.path.exists(cwd):
            raise FileNotFoundError(f"Could not find Meltano directory at {cwd}")
            
        output = subprocess.check_output(cmd, cwd=cwd, stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e:
        raise Exception(e.output.decode())

@dbt_assets(manifest=dbt_hdb_resale_project.manifest_path)
def dbt_hdb_resale_dbt_assets(
    context: AssetExecutionContext, 
    dbt: DbtCliResource, 
    config: PipelineConfig # Injecting the config here too
):
    # Pass the value to dbt --target
    yield from dbt.cli(["build", "--target", config.target_env], context=context).stream()
    