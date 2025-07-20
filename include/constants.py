"Contains constants used in the DAGs"

from pathlib import Path
from cosmos import ExecutionConfig, ProjectConfig
from cosmos.constants import InvocationMode

jaffle_shop_path = Path("/usr/local/airflow/dbt/jaffle_shop")
dbt_executable = Path("/usr/local/airflow/dbt_venv/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
    invocation_mode=InvocationMode.SUBPROCESS,
)

# Basic project config without parsing method
jaffle_shop_project_config = ProjectConfig(jaffle_shop_path)
