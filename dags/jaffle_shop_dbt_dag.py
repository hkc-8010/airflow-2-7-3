"""
Jaffle Shop DBT DAG

This DAG demonstrates the dynamic configuration approach for DBT projects.
It reads the dag_configuration.json file from the jaffle_shop DBT project
and creates a cosmos-based workflow.

This is similar to the pattern used in barren-vacuum-6397-airflow where
each DBT project has its own DAG file that references a configuration file.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig

# Import our custom utilities
import sys
sys.path.append('/usr/local/airflow/include')
from dynamic_dbt_utils import DbtConfigManager


# Path to the configuration file for this DBT project
CONFIG_FILE_PATH = "/usr/local/airflow/dbt/jaffle_shop/dag_configuration.json"

# Load configuration
config_manager = DbtConfigManager(CONFIG_FILE_PATH)
dag_config = config_manager.get_dag_config()
dbt_config = config_manager.get_dbt_config()
task_groups_config = config_manager.get_task_groups()
dependencies = config_manager.get_dependencies()

# Default arguments for the DAG
default_args = {
    'owner': dag_config['owner'],
    'depends_on_past': False,
    'start_date': dag_config['start_date'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    dag_id=dag_config['dag_id'],
    description=dag_config['description'],
    default_args=default_args,
    schedule_interval=dag_config['schedule_interval'],
    catchup=dag_config['catchup'],
    tags=dag_config['tags'],
    doc_md=f"""
    # Jaffle Shop DBT Project
    
    {dag_config['description']}
    
    This DAG processes the Jaffle Shop example data using DBT models.
    
    ## Project Structure
    - **Staging Models**: Clean and standardize raw data
    - **Mart Models**: Create business-ready customer and order analytics
    
    ## Configuration
    - **Project**: {dbt_config['project_name']}
    - **Profile**: {dbt_config['profile_name']}
    - **Target**: {dbt_config['target']}
    
    Configuration file: `{CONFIG_FILE_PATH}`
    """,
)

# Configure cosmos components
project_config = ProjectConfig(
    dbt_project_path=dbt_config['project_path'],
    project_name=dbt_config['project_name'],
)

profile_config = ProfileConfig(
    profile_name=dbt_config['profile_name'],
    target_name=dbt_config['target'],
    profiles_yml_filepath=f"{dbt_config['project_path']}/profiles.yml",
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

# Define tasks within the DAG context
with dag:
    # Start task
    start = DummyOperator(
        task_id='start',
        doc_md="Start of the Jaffle Shop DBT pipeline",
    )
    
    # Create task groups based on configuration
    if task_groups_config:
        task_groups = {}
        
        for group_name, group_config in task_groups_config.items():
            models = group_config.get('models', [])
            description = group_config.get('description', f'DBT {group_name} models')
            
            # Create render config to select specific models
            render_config = None
            if models:
                if 'staging' in models:
                    # For staging, select all models in the staging directory
                    render_config = RenderConfig(select=["staging"])
                else:
                    # For specific models, select them explicitly
                    render_config = RenderConfig(select=models)
            
            # Create DBT task group
            dbt_task_group = DbtTaskGroup(
                group_id=f"dbt_{group_name}",
                project_config=project_config,
                profile_config=profile_config,
                execution_config=execution_config,
                render_config=render_config,
                operator_args={
                    "vars": dbt_config['vars'],
                },
                default_args={
                    "retries": 2,
                    "retry_delay": timedelta(minutes=3),
                },
            )
            
            task_groups[group_name] = dbt_task_group
        
        # End task
        end = DummyOperator(
            task_id='end',
            doc_md="End of the Jaffle Shop DBT pipeline",
        )
        
        # Set up dependencies based on configuration
        # Start with groups that have no dependencies
        independent_groups = [
            task_groups[name] for name in task_groups.keys()
            if not dependencies.get(name)
        ]
        
        if independent_groups:
            start >> independent_groups
        
        # Set up inter-group dependencies
        for group_name, depends_on_list in dependencies.items():
            if group_name in task_groups:
                current_group = task_groups[group_name]
                for dependency in depends_on_list:
                    if dependency in task_groups:
                        task_groups[dependency] >> current_group
        
        # Connect final groups to end
        final_groups = []
        for name in task_groups.keys():
            # Check if this group is not a dependency of any other group
            is_final = True
            for other_deps in dependencies.values():
                if name in other_deps:
                    is_final = False
                    break
            if is_final:
                final_groups.append(task_groups[name])
        
        if final_groups:
            final_groups >> end
        else:
            # Fallback: connect all groups to end
            list(task_groups.values()) >> end
            
    else:
        # If no task groups are configured, create a single group for all models
        dbt_all = DbtTaskGroup(
            group_id="dbt_all",
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            operator_args={
                "vars": dbt_config['vars'],
            },
            default_args={
                "retries": 2,
                "retry_delay": timedelta(minutes=3),
            },
        )
        
        end = DummyOperator(
            task_id='end',
            doc_md="End of the Jaffle Shop DBT pipeline",
        )
        
        start >> dbt_all >> end

# Make the DAG available to Airflow
globals()[dag.dag_id] = dag 