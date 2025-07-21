#!/usr/bin/env python3
"""
DBT DAG Generator

This script helps generate configuration files and DAG templates for new DBT projects,
following the dynamic configuration pattern similar to barren-vacuum-6397-airflow.

Usage:
    python dbt_dag_generator.py create-config <dbt_project_path> [options]
    python dbt_dag_generator.py create-dag <config_file_path> [options]
    python dbt_dag_generator.py list-projects [base_path]
"""

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from dynamic_dbt_utils import DbtProjectDiscovery, DbtConfigManager


def create_default_profiles_yml(project_path: str) -> str:
    """
    Create a default profiles.yml file for a DBT project.
    
    Args:
        project_path: Path to the DBT project directory
        
    Returns:
        Content of the default profiles.yml file
    """
    project_name = Path(project_path).name
    
    return f"""airflow_db:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /tmp/{project_name}.duckdb
      threads: 4
    prod:
      type: snowflake
      account: your_account.region
      user: "{{{{ env_var('DBT_USER') }}}}"
      password: "{{{{ env_var('DBT_PASSWORD') }}}}"
      role: your_role
      database: your_database
      warehouse: your_warehouse
      schema: your_schema
      threads: 4
"""


def create_default_config(
    dbt_project_path: str,
    dag_id: str = None,
    schedule_interval: str = "@daily",
    owner: str = "data_team",
    tags: List[str] = None
) -> Dict[str, Any]:
    """
    Create a default configuration for a DBT project.
    
    Args:
        dbt_project_path: Path to the DBT project directory
        dag_id: DAG ID (defaults to project name with _dbt suffix)
        schedule_interval: Airflow schedule interval
        owner: DAG owner
        tags: List of tags for the DAG
        
    Returns:
        Dictionary containing the default configuration
    """
    project_path = Path(dbt_project_path)
    project_name = project_path.name
    
    if not dag_id:
        dag_id = f"{project_name}_dbt"
    
    if not tags:
        tags = ["dbt", project_name, "generated"]
    
    # Try to detect model structure
    models_dir = project_path / "models"
    task_groups = {}
    
    if models_dir.exists():
        subdirs = [d for d in models_dir.iterdir() if d.is_dir()]
        
        if subdirs:
            # Create task groups based on model subdirectories
            for subdir in subdirs:
                if subdir.name == "staging":
                    task_groups["staging"] = {
                        "models": ["staging"],
                        "description": "Load and clean raw data"
                    }
                elif subdir.name in ["marts", "mart", "datamart"]:
                    task_groups["marts"] = {
                        "models": [subdir.name],
                        "description": "Create business-ready data models",
                        "depends_on": ["staging"] if "staging" in task_groups else []
                    }
                else:
                    task_groups[subdir.name] = {
                        "models": [subdir.name],
                        "description": f"Process {subdir.name} models"
                    }
    
    return {
        "dag_id": dag_id,
        "description": f"DBT project for {project_name} - auto-generated configuration",
        "schedule_interval": schedule_interval,
        "start_date": datetime.now().strftime("%Y-%m-%d"),
        "catchup": False,
        "owner": owner,
        "tags": tags,
        "dbt_project_name": project_name,
        "dbt_project_path": str(project_path.absolute()),
        "dbt_profile_name": "airflow_db",
        "dbt_target": "dev",
        "connection_id": "airflow_default",
        "task_groups": task_groups,
        "test_after_each_model": True,
        "vars": {
            "start_date": "2023-01-01",
            "test_run": False
        }
    }


def create_dag_template(config_file_path: str, output_path: str = None) -> str:
    """
    Create a DAG template file for a given configuration.
    
    Args:
        config_file_path: Path to the dag_configuration.json file
        output_path: Output path for the DAG file (optional)
        
    Returns:
        Path to the created DAG file
    """
    config_manager = DbtConfigManager(config_file_path)
    dag_config = config_manager.get_dag_config()
    
    dag_id = dag_config['dag_id']
    
    if not output_path:
        # Default to the dags directory
        dags_dir = Path(__file__).parent.parent / "dags"
        output_path = dags_dir / f"{dag_id}.py"
    
    template = f'''"""
{dag_id.title().replace('_', ' ')} DAG

This DAG was auto-generated using the dynamic configuration approach.
It reads the dag_configuration.json file and creates a cosmos-based workflow.

Configuration file: {config_file_path}
Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
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
CONFIG_FILE_PATH = "{config_file_path}"

# Load configuration
config_manager = DbtConfigManager(CONFIG_FILE_PATH)
dag_config = config_manager.get_dag_config()
dbt_config = config_manager.get_dbt_config()
task_groups_config = config_manager.get_task_groups()
dependencies = config_manager.get_dependencies()

# Default arguments for the DAG
default_args = {{
    'owner': dag_config['owner'],
    'depends_on_past': False,
    'start_date': dag_config['start_date'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

# Create the DAG
dag = DAG(
    dag_id=dag_config['dag_id'],
    description=dag_config['description'],
    default_args=default_args,
    schedule_interval=dag_config['schedule_interval'],
    catchup=dag_config['catchup'],
    tags=dag_config['tags'],
    doc_md=f"""
    # {{dag_config['dag_id'].title().replace('_', ' ')}}
    
    {{dag_config['description']}}
    
    ## Configuration
    - **Project**: {{dbt_config['project_name']}}
    - **Profile**: {{dbt_config['profile_name']}}
    - **Target**: {{dbt_config['target']}}
    
    Configuration file: `{{CONFIG_FILE_PATH}}`
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
    profiles_yml_filepath=f"{{dbt_config['project_path']}}/profiles.yml",
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

# Define tasks within the DAG context
with dag:
    # Start task
    start = DummyOperator(
        task_id='start',
        doc_md="Start of the {{dag_config['dag_id']}} pipeline",
    )
    
    # Create task groups based on configuration
    if task_groups_config:
        task_groups = {{}}
        
        for group_name, group_config in task_groups_config.items():
            models = group_config.get('models', [])
            description = group_config.get('description', f'DBT {{group_name}} models')
            
            # Create render config to select specific models
            render_config = None
            if models:
                render_config = RenderConfig(select=models)
            
            # Create DBT task group
            dbt_task_group = DbtTaskGroup(
                group_id=f"dbt_{{group_name}}",
                project_config=project_config,
                profile_config=profile_config,
                execution_config=execution_config,
                render_config=render_config,
                operator_args={{
                    "vars": dbt_config['vars'],
                }},
                default_args={{
                    "retries": 2,
                    "retry_delay": timedelta(minutes=3),
                }},
            )
            
            task_groups[group_name] = dbt_task_group
        
        # End task
        end = DummyOperator(
            task_id='end',
            doc_md="End of the {{dag_config['dag_id']}} pipeline",
        )
        
        # Set up dependencies based on configuration
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
            list(task_groups.values()) >> end
            
    else:
        # Single task group for all models
        dbt_all = DbtTaskGroup(
            group_id="dbt_all",
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            operator_args={{
                "vars": dbt_config['vars'],
            }},
            default_args={{
                "retries": 2,
                "retry_delay": timedelta(minutes=3),
            }},
        )
        
        end = DummyOperator(
            task_id='end',
            doc_md="End of the {{dag_config['dag_id']}} pipeline",
        )
        
        start >> dbt_all >> end

# Make the DAG available to Airflow
globals()[dag.dag_id] = dag
'''
    
    # Write the template to the output file
    with open(output_path, 'w') as f:
        f.write(template)
    
    return str(output_path)


def list_dbt_projects(base_path: str = "/usr/local/airflow/dbt") -> List[Dict[str, Any]]:
    """
    List all DBT projects in the given base path.
    
    Args:
        base_path: Base directory to search for DBT projects
        
    Returns:
        List of project information dictionaries
    """
    projects = []
    base_path = Path(base_path)
    
    if not base_path.exists():
        return projects
    
    for item in base_path.iterdir():
        if item.is_dir() and DbtProjectDiscovery.validate_dbt_project(str(item)):
            config_file = item / "dag_configuration.json"
            
            project_info = {
                "name": item.name,
                "path": str(item),
                "has_config": config_file.exists(),
                "config_path": str(config_file) if config_file.exists() else None
            }
            
            if config_file.exists():
                try:
                    config_manager = DbtConfigManager(str(config_file))
                    dag_config = config_manager.get_dag_config()
                    project_info["dag_id"] = dag_config.get("dag_id")
                    project_info["schedule"] = dag_config.get("schedule_interval")
                except Exception as e:
                    project_info["config_error"] = str(e)
            
            projects.append(project_info)
    
    return projects


def main():
    parser = argparse.ArgumentParser(description="DBT DAG Generator")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Create config command
    config_parser = subparsers.add_parser("create-config", help="Create a dag_configuration.json file")
    config_parser.add_argument("dbt_project_path", help="Path to the DBT project directory")
    config_parser.add_argument("--dag-id", help="DAG ID (defaults to project name with _dbt suffix)")
    config_parser.add_argument("--schedule", default="@daily", help="Schedule interval (default: @daily)")
    config_parser.add_argument("--owner", default="data_team", help="DAG owner (default: data_team)")
    config_parser.add_argument("--tags", nargs="*", help="List of tags for the DAG")
    
    # Create DAG command
    dag_parser = subparsers.add_parser("create-dag", help="Create a DAG file from configuration")
    dag_parser.add_argument("config_file_path", help="Path to the dag_configuration.json file")
    dag_parser.add_argument("--output", help="Output path for the DAG file")
    
    # List projects command
    list_parser = subparsers.add_parser("list-projects", help="List all DBT projects")
    list_parser.add_argument("base_path", nargs="?", default="/usr/local/airflow/dbt", 
                           help="Base directory to search (default: /usr/local/airflow/dbt)")
    
    args = parser.parse_args()
    
    if args.command == "create-config":
        project_path = Path(args.dbt_project_path)
        
        config = create_default_config(
            args.dbt_project_path,
            dag_id=args.dag_id,
            schedule_interval=args.schedule,
            owner=args.owner,
            tags=args.tags
        )
        
        # Create configuration file
        config_file = project_path / "dag_configuration.json"
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=4)
        
        # Create profiles.yml if it doesn't exist
        profiles_file = project_path / "profiles.yml"
        if not profiles_file.exists():
            profiles_content = create_default_profiles_yml(args.dbt_project_path)
            with open(profiles_file, 'w') as f:
                f.write(profiles_content)
            print(f"Created profiles file: {profiles_file}")
        else:
            print(f"Profiles file already exists: {profiles_file}")
        
        print(f"Created configuration file: {config_file}")
        
    elif args.command == "create-dag":
        dag_file = create_dag_template(args.config_file_path, args.output)
        print(f"Created DAG file: {dag_file}")
        
    elif args.command == "list-projects":
        projects = list_dbt_projects(args.base_path)
        
        if not projects:
            print(f"No DBT projects found in {args.base_path}")
        else:
            print(f"Found {len(projects)} DBT project(s) in {args.base_path}:")
            print()
            
            for project in projects:
                print(f"📁 {project['name']}")
                print(f"   Path: {project['path']}")
                print(f"   Has Config: {project['has_config']}")
                
                if project['has_config']:
                    if 'config_error' in project:
                        print(f"   Config Error: {project['config_error']}")
                    else:
                        print(f"   DAG ID: {project.get('dag_id', 'Unknown')}")
                        print(f"   Schedule: {project.get('schedule', 'Unknown')}")
                
                print()
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 