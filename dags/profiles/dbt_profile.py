from datetime import datetime

from cosmos import DbtDag, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode

from include.constants import (
    jaffle_shop_project_config,
    venv_execution_config,
    jaffle_shop_path,
)

dbt_profile_example = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=jaffle_shop_project_config,
    profile_config=ProfileConfig(
        # these map to dbt/jaffle_shop/profiles.yml (Snowflake config)
        profile_name="airflow_db",
        target_name="dev",
        profiles_yml_filepath=jaffle_shop_path / "profiles.yml",
    ),
    execution_config=venv_execution_config,
    render_config=RenderConfig(load_method=LoadMode.DBT_LS),
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_profile_example",
    tags=["profiles"],
)
