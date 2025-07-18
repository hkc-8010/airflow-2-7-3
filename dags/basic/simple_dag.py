from datetime import datetime

from cosmos import DbtDag, RenderConfig
from cosmos.constants import LoadMode, InvocationMode

from include.profiles import airflow_db
from include.constants import jaffle_shop_project_config, venv_execution_config

simple_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=jaffle_shop_project_config,
    profile_config=airflow_db,
    execution_config=venv_execution_config,
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS, invocation_mode=InvocationMode.SUBPROCESS
    ),
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="simple_dag",
    tags=["simple"],
)
