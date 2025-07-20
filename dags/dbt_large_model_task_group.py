"""
# Large dbt Model Graph DAG with TaskGroup

This DAG runs a dbt project with hundreds of models using the Cosmos TaskGroup.
The DAG will:
1. Load seed data
2. Run the entire dbt project with proper dependencies
3. Test the models

Each model represents a SQL transformation, and Cosmos automatically creates a task
for each model while preserving the dependencies defined within the dbt project.
"""

from pendulum import datetime
from airflow import DAG
from airflow.models.baseoperator import chain
from cosmos import DbtTaskGroup, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.operators import DbtSeedLocalOperator, DbtTestLocalOperator

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

# Create a standard Airflow DAG
with DAG(
    dag_id="dbt_large_model_task_group",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["dbt", "large_model_graph"],
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
) as dag:
    # Create a seed operation
    seed_op = DbtSeedLocalOperator(
        task_id="dbt_seed",
        project_dir=jaffle_shop_project_config.dbt_project_path,
        profile_config=profile_config,
        dbt_executable_path=str(dbt_executable),
        full_refresh=True,
    )

    # Create a dbt task group for models
    dbt_models = DbtTaskGroup(
        group_id="dbt_models",
        project_config=jaffle_shop_project_config,
        profile_config=profile_config,
        dbt_executable_path=str(dbt_executable),
    )

    # Create a test operation
    test_op = DbtTestLocalOperator(
        task_id="dbt_test",
        project_dir=jaffle_shop_project_config.dbt_project_path,
        profile_config=profile_config,
        dbt_executable_path=str(dbt_executable),
        select="test_type:singular",
    )

    # Set up the execution order
    chain(seed_op, dbt_models, test_op)

# Make the DAG available to Airflow
# dag is automatically exported from the with block
