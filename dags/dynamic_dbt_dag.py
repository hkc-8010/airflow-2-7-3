"""
Dynamic DBT DAG Template

This DAG template creates DBT workflows dynamically based on configuration files.
It reads dag_configuration.json files from DBT project directories and creates
cosmos-based task groups for each configured DBT project.

Similar to the pattern used in barren-vacuum-6397-airflow, this approach allows
for standardized DAG creation while maintaining flexibility through configuration.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Import our custom utilities
import sys
sys.path.append('/usr/local/airflow/include')
from dynamic_dbt_utils import DbtConfigManager, DbtProjectDiscovery


def create_dbt_dag_from_config(config_file_path: str) -> DAG:
    """
    Create a DBT DAG from a configuration file using cosmos.
    
    Args:
        config_file_path: Path to the dag_configuration.json file
        
    Returns:
        Configured DAG with DBT task groups
    """
    config_manager = DbtConfigManager(config_file_path)
    dag_config = config_manager.get_dag_config()
    dbt_config = config_manager.get_dbt_config()
    task_groups_config = config_manager.get_task_groups()
    dependencies = config_manager.get_dependencies()
    
    # Create the DAG
    dag = DAG(
        dag_id=dag_config['dag_id'],
        description=dag_config['description'],
        schedule_interval=dag_config['schedule_interval'],
        start_date=dag_config['start_date'],
        catchup=dag_config['catchup'],
        tags=dag_config['tags'],
        default_args={
            'owner': dag_config['owner'],
            'retries': 1,
            'retry_delay': datetime.timedelta(minutes=5)
        },
        doc_md=f"""
        # {dag_config['dag_id']}
        
        {dag_config['description']}
        
        ## DBT Project Configuration
        - **Project**: {dbt_config['project_name']}
        - **Profile**: {dbt_config['profile_name']}
        - **Target**: {dbt_config['target']}
        - **Connection**: {dbt_config['connection_id']}
        
        ## Task Groups
        {', '.join(task_groups_config.keys()) if task_groups_config else 'None configured'}
        
        This DAG was generated dynamically from configuration file: `{config_file_path}`
        """
    )
    
    # Configure cosmos components
    project_config = ProjectConfig(
        dbt_project_path=dbt_config['project_path'],
        project_name=dbt_config['project_name']
    )
    
    profile_config = ProfileConfig(
        profile_name=dbt_config['profile_name'],
        target_name=dbt_config['target'],
        profiles_yml_filepath=f"{dbt_config['project_path']}/profiles.yml",
    )
    
    execution_config = ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",  # Adjust path as needed
    )
    
    with dag:
        # Create start and end dummy operators
        start_task = DummyOperator(task_id="start")
        end_task = DummyOperator(task_id="end")
        
        task_groups = {}
        
        # Create task groups based on configuration
        if task_groups_config:
            for group_name, group_config in task_groups_config.items():
                models = group_config.get('models', [])
                
                # Create render config to select specific models
                render_config = None
                if models:
                    # If specific models are defined, select them
                    if models == ['staging']:
                        render_config = RenderConfig(select=["tag:staging"])
                    elif models == ['customers', 'orders']:
                        render_config = RenderConfig(select=["customers", "orders"])
                    else:
                        render_config = RenderConfig(select=models)
                
                # Create DBT task group using cosmos
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
                        "retry_delay": datetime.timedelta(minutes=5),
                    },
                )
                
                task_groups[group_name] = dbt_task_group
        else:
            # If no task groups defined, create a single group for the entire project
            dbt_task_group = DbtTaskGroup(
                group_id="dbt_all",
                project_config=project_config,
                profile_config=profile_config,
                execution_config=execution_config,
                operator_args={
                    "vars": dbt_config['vars'],
                },
                default_args={
                    "retries": 2,
                    "retry_delay": datetime.timedelta(minutes=5),
                },
            )
            task_groups['all'] = dbt_task_group
        
        # Set up dependencies
        if len(task_groups) == 1:
            # Single task group
            start_task >> list(task_groups.values())[0] >> end_task
        else:
            # Multiple task groups with dependencies
            start_task >> [tg for tg in task_groups.values() if not dependencies.get(name) for name, tg in task_groups.items()]
            
            # Set up inter-group dependencies
            for group_name, depends_on_list in dependencies.items():
                if group_name in task_groups:
                    current_group = task_groups[group_name]
                    for dependency in depends_on_list:
                        if dependency in task_groups:
                            task_groups[dependency] >> current_group
            
            # Connect final groups to end
            final_groups = [tg for name, tg in task_groups.items() 
                          if name not in [dep for deps in dependencies.values() for dep in deps]]
            if final_groups:
                final_groups >> end_task
            else:
                # If all groups have dependencies, connect the ones that aren't dependencies of others
                leaf_groups = [tg for name, tg in task_groups.items() 
                             if not any(name in dep_list for dep_list in dependencies.values())]
                if leaf_groups:
                    leaf_groups >> end_task
                else:
                    # Fallback: connect all to end
                    list(task_groups.values()) >> end_task
    
    return dag


# Auto-discover and create DAGs for all configured DBT projects
def create_all_dbt_dags():
    """
    Discover all DBT projects with configuration files and create DAGs for them.
    This function is called at module import time to register all DAGs with Airflow.
    """
    dbt_base_path = "/usr/local/airflow/dbt"
    
    if os.path.exists(dbt_base_path):
        config_files = DbtProjectDiscovery.find_dbt_projects(dbt_base_path)
        
        dags = {}
        for config_file in config_files:
            try:
                dag = create_dbt_dag_from_config(config_file)
                if dag:
                    # Add DAG to globals so Airflow can discover it
                    globals()[dag.dag_id] = dag
                    dags[dag.dag_id] = dag
            except Exception as e:
                print(f"Failed to create DAG from {config_file}: {e}")
        
        return dags
    else:
        print(f"DBT base path {dbt_base_path} does not exist")
        return {}


# Auto-discover and create all DBT DAGs
discovered_dags = create_all_dbt_dags()

# For development/testing, also expose the jaffle_shop DAG directly
jaffle_shop_config = "/usr/local/airflow/dbt/jaffle_shop/dag_configuration.json"
if os.path.exists(jaffle_shop_config):
    try:
        jaffle_shop_dbt = create_dbt_dag_from_config(jaffle_shop_config)
        # Make it available at module level for Airflow discovery
        globals()['jaffle_shop_dbt'] = jaffle_shop_dbt
    except Exception as e:
        print(f"Failed to create jaffle_shop DAG: {e}") 