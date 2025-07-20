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
from cosmos import DbtDag, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Import constants from the constants.py file
from include.constants import jaffle_shop_project_config, dbt_executable

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

# Create a custom execution config
execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
)

# Create the DAG using Cosmos with constants from include/constants.py
large_model_dag = DbtDag(
    dag_id="dbt_large_model_graph",
    project_config=jaffle_shop_project_config,  # Using the project config from constants
    profile_config=profile_config,
    execution_config=execution_config,
    schedule_interval=None,  # Set to None for manual triggering
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    operator_args={
        "tags": ["dbt", "large_model_graph"],
        # Command-specific arguments in operator_args
        "env": {
            "DBT_FULL_REFRESH": "true",  # For seed operations
        },
        "vars": {
            "exclude_sources": "true",  # For test operations
        },
    },
    doc_md=__doc__,
)

# Make the DAG available to Airflow
dag = large_model_dag
