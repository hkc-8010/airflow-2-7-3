"""
# Large dbt Model Graph DAG

This DAG runs a dbt project with hundreds of models using the Cosmos provider.
The DAG will:
1. Load seed data
2. Run the entire dbt project with proper dependencies
3. Test the models

Each model represents a SQL transformation, and Cosmos automatically creates a task
for each model while preserving the dependencies defined within the dbt project.
"""

from pendulum import datetime
from cosmos import DbtDag, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Profile configuration for dbt
profile_config = ProfileConfig(
    profile_name="airflow_db",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",  # Connection ID to use for the profile
        profile_args={
            "database": "SANDBOX",
            "schema": "HEMKUMARCHHEDA",
            "warehouse": "HUMANS",
            "role": "HEMKUMARCHHEDA",
        },
    ),
)

# Project configuration for dbt
project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/dbt/jaffle_shop",
)

# Create the DAG using Cosmos
large_model_dag = DbtDag(
    dag_id="dbt_large_model_graph",
    project_config=project_config,
    profile_config=profile_config,
    schedule_interval=None,  # Set to None for manual triggering
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    dbt_args={
        "dbt_executable_path": "/usr/local/airflow/.local/bin/dbt",
    },
    execution_config={
        "dbt_seed": {"full_refresh": True},
        "dbt_run": {"full_refresh": False},
        "dbt_test": {"exclude": "source:*"},
    },
    operator_args={
        "tags": ["dbt", "large_model_graph"],
    },
    doc_md=__doc__,
)

# Make the DAG available to Airflow
dag = large_model_dag
