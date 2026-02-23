from setuptools import find_packages, setup

setup(
    name="dagster_dbt_integration_hdb_resale",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "dagster_dbt_integration_hdb_resale": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-core<1.11",
        "dbt-bigquery<1.11",
        "dbt-bigquery<1.11",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)