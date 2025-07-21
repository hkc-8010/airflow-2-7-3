"""
Dynamic DBT Utilities for Airflow

This module provides utilities for creating dynamic DAGs from configuration files
for DBT projects, similar to the pattern used in the barren-vacuum-6397-airflow folder.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional


class DbtConfigManager:
    """Manages DBT project configurations for dynamic DAG creation."""
    
    def __init__(self, config_file_path: str):
        """
        Initialize the config manager with a configuration file.
        
        Args:
            config_file_path: Path to the dag_configuration.json file
        """
        self.config_file_path = config_file_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            with open(self.config_file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_file_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file {self.config_file_path}: {e}")
    
    def get_dag_config(self) -> Dict[str, Any]:
        """Get DAG-level configuration."""
        return {
            'dag_id': self.config.get('dag_id'),
            'description': self.config.get('description', ''),
            'schedule_interval': self.config.get('schedule_interval', '@daily'),
            'start_date': datetime.strptime(self.config.get('start_date', '2024-01-01'), '%Y-%m-%d'),
            'catchup': self.config.get('catchup', False),
            'owner': self.config.get('owner', 'airflow'),
            'tags': self.config.get('tags', [])
        }
    
    def get_dbt_config(self) -> Dict[str, Any]:
        """Get DBT-specific configuration."""
        return {
            'project_name': self.config.get('dbt_project_name'),
            'project_path': self.config.get('dbt_project_path'),
            'profile_name': self.config.get('dbt_profile_name'),
            'target': self.config.get('dbt_target', 'dev'),
            'connection_id': self.config.get('connection_id'),
            'vars': self.config.get('vars', {}),
            'test_after_each_model': self.config.get('test_after_each_model', True),
            'profiles_yml_filepath': self.config.get('profiles_yml_filepath')
        }
    
    def get_task_groups(self) -> Dict[str, Dict[str, Any]]:
        """Get task group configurations."""
        return self.config.get('task_groups', {})
    
    def get_dependencies(self) -> Dict[str, List[str]]:
        """Extract dependencies between task groups."""
        dependencies = {}
        task_groups = self.get_task_groups()
        
        for group_name, group_config in task_groups.items():
            depends_on = group_config.get('depends_on', [])
            dependencies[group_name] = depends_on
        
        return dependencies


class DbtProjectDiscovery:
    """Discovers DBT projects and their configuration files."""
    
    @staticmethod
    def find_dbt_projects(base_path: str) -> List[str]:
        """
        Find all DBT projects with configuration files in the given base path.
        
        Args:
            base_path: Base directory to search for DBT projects
            
        Returns:
            List of paths to dag_configuration.json files
        """
        config_files = []
        base_path = Path(base_path)
        
        # Search for dag_configuration.json files in dbt project directories
        for config_file in base_path.rglob('dag_configuration.json'):
            # Verify it's in a DBT project directory (contains dbt_project.yml)
            project_dir = config_file.parent
            if (project_dir / 'dbt_project.yml').exists():
                config_files.append(str(config_file))
        
        return config_files
    
    @staticmethod
    def validate_dbt_project(project_path: str) -> bool:
        """
        Validate that a directory contains a valid DBT project.
        
        Args:
            project_path: Path to the DBT project directory
            
        Returns:
            True if valid DBT project, False otherwise
        """
        project_path = Path(project_path)
        required_files = ['dbt_project.yml', 'profiles.yml']
        required_dirs = ['models']
        
        # Check for required files
        for file_name in required_files:
            if not (project_path / file_name).exists():
                return False
        
        # Check for required directories
        for dir_name in required_dirs:
            if not (project_path / dir_name).is_dir():
                return False
        
        return True


def create_dag_from_config(config_file_path: str):
    """
    Factory function to create a DAG from a configuration file.
    This function should be imported and used in individual DAG files.
    
    Args:
        config_file_path: Path to the dag_configuration.json file
        
    Returns:
        Configured DAG object
    """
    from airflow import DAG
    
    config_manager = DbtConfigManager(config_file_path)
    dag_config = config_manager.get_dag_config()
    
    # Create and return the DAG
    dag = DAG(
        dag_id=dag_config['dag_id'],
        description=dag_config['description'],
        schedule_interval=dag_config['schedule_interval'],
        start_date=dag_config['start_date'],
        catchup=dag_config['catchup'],
        tags=dag_config['tags'],
        default_args={
            'owner': dag_config['owner'],
            'retries': 1
        }
    )
    
    return dag 