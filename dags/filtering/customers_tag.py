from datetime import datetime

from cosmos import DbtDag, RenderConfig
from cosmos.constants import LoadMode, InvocationMode

from include.profiles import airflow_db
from include.constants import jaffle_shop_project_config, venv_execution_config

customers_tag = DbtDag(
    project_config=jaffle_shop_project_config,
    profile_config=airflow_db,
    execution_config=venv_execution_config,
    # Render config with filtering and performance settings
    render_config=RenderConfig(
        select=["tag:customers"],
        load_method=LoadMode.DBT_LS,
        invocation_mode=InvocationMode.SUBPROCESS,
    ),
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="customers_tag",
    tags=["filtering"],
)
