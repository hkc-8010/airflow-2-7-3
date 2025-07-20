"""
# Large dbt Model Graph DAG

This DAG runs a dbt project with hundreds of models using the Cosmos provider.
The DAG will:
1. Load seed data with full refresh
2. Run the entire dbt project with proper dependencies
3. Test the models

Each model represents a SQL transformation, and Cosmos automatically creates a task
for each model while preserving the dependencies defined within the dbt project.
"""

from pendulum import datetime
from cosmos import DbtDag, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.models.baseoperator import chain

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

# Create the DAG using Cosmos with minimal configuration
# We'll manually set up the seed-run-test sequence
large_model_dag = DbtDag(
    # Airflow DAG parameters
    dag_id="dbt_large_model_graph",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["dbt", "large_model_graph"],
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    # Cosmos-specific parameters
    project_config=jaffle_shop_project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    # Enable all the operations we want to run
    # Each gets its own task group
    dbt_seed=True,
    dbt_run=True,
    dbt_test=True,
    # Custom args for seed operation
    # Note: This is the correct parameter name in Cosmos 1.5.0
    seed_vars={"full_refresh": True},
    # Filter tests to exclude source tests
    test_select="test_type:singular",
    doc_md=__doc__,
)

# Manually set the dependencies between task groups to ensure correct order
# This is the proper way to control execution order in Cosmos 1.5.0
chain(large_model_dag.seed_group, large_model_dag.run_group, large_model_dag.test_group)

# Make the DAG available to Airflow
dag = large_model_dag
