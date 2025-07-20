from datetime import datetime

from cosmos import DbtDag, ProfileConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import LoadMode

from include.constants import jaffle_shop_project_config, venv_execution_config

dbt_profile_overrides = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=jaffle_shop_project_config,
    profile_config=ProfileConfig(
        # these map to dbt/jaffle_shop/profiles.yml
        profile_name="airflow_db",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_default",
            profile_args={
                "schema": "HEMKUMARCHHEDA",
                "database": "SANDBOX",
                "role": "HEMKUMARCHHEDA",
                "warehouse": "HUMANS",
                "account": "gp21411.us-east-1",
                "region": "us-east-1",
            },
        ),
    ),
    execution_config=venv_execution_config,
    render_config=RenderConfig(load_method=LoadMode.DBT_LS),
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_profile_overrides",
    tags=["profiles"],
)
