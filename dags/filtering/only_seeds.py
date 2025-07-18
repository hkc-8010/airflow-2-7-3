from datetime import datetime

from cosmos import DbtDag, RenderConfig
from cosmos.constants import LoadMode

from include.profiles import airflow_db
from include.constants import jaffle_shop_project_config, venv_execution_config

only_seeds = DbtDag(
    project_config=jaffle_shop_project_config,
    profile_config=airflow_db,
    execution_config=venv_execution_config,
    # Render config with filtering and performance settings
    # Use select instead of models (to avoid deprecation warning)
    render_config=RenderConfig(select=["path:seeds"], load_method=LoadMode.DBT_LS),
    # normal dag parameters
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="only_seeds",
    tags=["filtering"],
)
