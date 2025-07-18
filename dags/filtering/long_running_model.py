"""
This DAG runs only the long-running model which is designed to execute
a SQL query for approximately 15 minutes on PostgreSQL.
"""

from datetime import datetime

from cosmos import DbtDag, ProfileConfig, RenderConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import LoadMode, InvocationMode

from include.constants import jaffle_shop_path, dbt_executable

# Create a profile config for PostgreSQL
postgres_profile = ProfileConfig(
    profile_name="postgres_profile",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",  # This should match an existing PostgreSQL connection
        profile_args={"schema": "public", "threads": 4},
    ),
)

# Create custom execution config for this specific DAG
long_running_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
    invocation_mode=InvocationMode.SUBPROCESS,
)

# Create a DAG that only runs the long-running model
long_running_model = DbtDag(
    project_config=ProjectConfig(jaffle_shop_path),
    profile_config=postgres_profile,
    execution_config=long_running_execution_config,
    # Render config with filtering and performance settings
    render_config=RenderConfig(
        select=["custom.long_running_query"], load_method=LoadMode.DBT_LS
    ),
    # normal dag parameters
    schedule_interval=None,  # Manual triggers only
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="long_running_model",
    default_args={
        "owner": "airflow",
        "retries": 0,  # No retries for this long-running task
        "execution_timeout": None,  # No timeout, let it run as long as needed
    },
    tags=["long_running", "postgres", "dbt"],
    doc_md=__doc__,
)
