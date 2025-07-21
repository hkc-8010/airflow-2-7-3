# Dynamic DBT DAG System

This implementation provides a dynamic DAG creation system for DBT projects in Airflow, similar to the approach used in the `barren-vacuum-6397-airflow` folder. It allows you to create standardized DBT workflows using configuration files while maintaining flexibility and reducing code duplication.

## Overview

The system consists of:

1. **Configuration Files** (`dag_configuration.json`) - Define DAG and DBT project settings
2. **Utility Module** (`include/dynamic_dbt_utils.py`) - Manages configuration loading and validation
3. **DAG Templates** - Generate Airflow DAGs from configurations using Cosmos
4. **Generator Script** (`include/dbt_dag_generator.py`) - Helps create new configurations and DAGs

## Quick Start

### 1. Existing DBT Project (Jaffle Shop)

The `jaffle_shop` DBT project is already configured and ready to use:

```bash
# Check the DAG in Airflow UI - it should appear as "jaffle_shop_dbt"
# The configuration is at: dbt/jaffle_shop/dag_configuration.json
# The DAG file is at: dags/jaffle_shop_dbt_dag.py
```

### 2. Creating a New DBT Project Configuration

```bash
cd include
python dbt_dag_generator.py create-config /path/to/your/dbt/project \
    --dag-id my_project_dbt \
    --schedule "@daily" \
    --owner "your_team" \
    --tags "dbt" "analytics" "my_project"
```

### 3. Creating a DAG from Configuration

```bash
python dbt_dag_generator.py create-dag /path/to/dag_configuration.json \
    --output ../dags/my_project_dbt_dag.py
```

### 4. Listing All DBT Projects

```bash
python dbt_dag_generator.py list-projects /usr/local/airflow/dbt
```

## Configuration File Structure

The `dag_configuration.json` file defines all aspects of your DBT DAG:

```json
{
    "dag_id": "my_project_dbt",
    "description": "Description of your DBT project",
    "schedule_interval": "@daily",
    "start_date": "2024-01-01",
    "catchup": false,
    "owner": "data_team",
    "tags": ["dbt", "my_project", "analytics"],
    
    "dbt_project_name": "my_project",
    "dbt_project_path": "/usr/local/airflow/dbt/my_project",
    "dbt_profile_name": "airflow_db",
    "dbt_target": "dev",
    "connection_id": "airflow_default",
    
    "task_groups": {
        "staging": {
            "models": ["staging"],
            "description": "Load and clean raw data"
        },
        "marts": {
            "models": ["customers", "orders"],
            "description": "Create business-ready data models",
            "depends_on": ["staging"]
        }
    },
    
    "test_after_each_model": true,
    "vars": {
        "start_date": "2023-01-01",
        "test_run": false
    }
}
```

## DBT Profiles Configuration

Each DBT project requires a `profiles.yml` file that defines database connections. The system automatically looks for this file in your DBT project directory.

### Example profiles.yml

```yaml
airflow_db:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /tmp/my_project.duckdb
      threads: 4
    prod:
      type: snowflake
      account: your_account.region
      user: "{{ env_var('DBT_USER') }}"
      password: "{{ env_var('DBT_PASSWORD') }}"
      role: your_role
      database: your_database
      warehouse: your_warehouse
      schema: your_schema
      threads: 4
```

The generator script will create a default `profiles.yml` if one doesn't exist when you run `create-config`.

## Configuration Options

### DAG Settings

| Field | Description | Required | Default |
|-------|-------------|----------|---------|
| `dag_id` | Unique DAG identifier | Yes | - |
| `description` | DAG description | No | "" |
| `schedule_interval` | Airflow schedule | No | "@daily" |
| `start_date` | DAG start date (YYYY-MM-DD) | No | "2024-01-01" |
| `catchup` | Enable catchup | No | false |
| `owner` | DAG owner | No | "airflow" |
| `tags` | List of DAG tags | No | [] |

### DBT Settings

| Field | Description | Required | Default |
|-------|-------------|----------|---------|
| `dbt_project_name` | DBT project name | Yes | - |
| `dbt_project_path` | Path to DBT project | Yes | - |
| `dbt_profile_name` | DBT profile name | Yes | - |
| `dbt_target` | DBT target environment | No | "dev" |
| `connection_id` | Airflow connection ID | No | "airflow_default" |
| `vars` | DBT variables | No | {} |
| `test_after_each_model` | Run tests after models | No | true |

### Task Groups

Task groups allow you to organize your DBT models into logical groups with dependencies:

```json
"task_groups": {
    "group_name": {
        "models": ["model1", "model2", "staging"],
        "description": "Description of this group",
        "depends_on": ["other_group"]
    }
}
```

- `models`: List of model names or directory names to include
- `description`: Human-readable description
- `depends_on`: List of other task groups this group depends on

## Directory Structure

```
airflow-2-7-3/
├── dags/
│   ├── dynamic_dbt_dag.py           # Auto-discovery DAG (creates all configured projects)
│   ├── jaffle_shop_dbt_dag.py       # Specific DAG for jaffle_shop project
│   └── [generated_dags].py          # Additional generated DAG files
├── dbt/
│   ├── jaffle_shop/
│   │   ├── dag_configuration.json   # Configuration for jaffle_shop
│   │   ├── dbt_project.yml
│   │   └── models/
│   └── [other_dbt_projects]/
└── include/
    ├── dynamic_dbt_utils.py         # Utility classes and functions
    └── dbt_dag_generator.py         # CLI tool for generating configs and DAGs
```

## Usage Patterns

### Pattern 1: Auto-Discovery (Recommended for Development)

Use `dynamic_dbt_dag.py` which automatically discovers all DBT projects with configuration files:

1. Place `dag_configuration.json` in your DBT project directory
2. The auto-discovery DAG will automatically create a DAG for your project
3. DAGs appear in Airflow UI without additional files

### Pattern 2: Individual DAG Files (Recommended for Production)

Create individual DAG files for each project:

1. Create configuration: `python dbt_dag_generator.py create-config ...`
2. Generate DAG file: `python dbt_dag_generator.py create-dag ...`
3. Each project has its own dedicated DAG file

## Advanced Configuration

### Model Selection

The `models` field in task groups supports various selection patterns:

```json
"models": ["staging"]              // All models in staging/ directory
"models": ["customers", "orders"]  // Specific models
"models": ["tag:critical"]         // Models with specific tag
"models": ["+customers"]           // customers and all upstream models
"models": ["customers+"]           // customers and all downstream models
```

### Environment-Specific Configurations

You can create different configurations for different environments:

```json
{
    "dag_id": "my_project_{{ var.value.environment }}_dbt",
    "dbt_target": "{{ var.value.environment }}",
    "vars": {
        "environment": "{{ var.value.environment }}",
        "project_id": "{{ var.value.project_id }}"
    }
}
```

### Custom Task Dependencies

Define complex dependencies between task groups:

```json
"task_groups": {
    "raw_data": {
        "models": ["staging"],
        "description": "Load raw data"
    },
    "transformations": {
        "models": ["intermediate"],
        "description": "Transform data",
        "depends_on": ["raw_data"]
    },
    "marts": {
        "models": ["customers", "orders"],
        "description": "Create marts",
        "depends_on": ["transformations"]
    },
    "reports": {
        "models": ["reports"],
        "description": "Generate reports",
        "depends_on": ["marts"]
    }
}
```

## Integration with Existing Systems

### Airflow Variables

The system integrates with Airflow Variables for environment-specific settings:

```python
# In your configuration or DAG files
"vars": {
    "project_id": "{{ var.value.gcp_project_id }}",
    "dataset": "{{ var.value.bq_dataset }}"
}
```

### Connection Management

Use Airflow Connections for database credentials:

```json
"connection_id": "snowflake_default"
```

### Notification and Monitoring

Add callbacks and monitoring to your configuration:

```python
# In the DAG generation, you can extend with:
default_args = {
    'on_failure_callback': your_failure_callback,
    'on_success_callback': your_success_callback,
    'sla': timedelta(hours=2)
}
```

## Troubleshooting

### Common Issues

1. **Configuration file not found**
   - Ensure `dag_configuration.json` exists in the DBT project directory
   - Check file permissions and paths

2. **DBT project not detected**
   - Verify `dbt_project.yml` exists
   - Ensure `models/` directory exists
   - Ensure `profiles.yml` exists with valid connection details

3. **DAG not appearing in Airflow**
   - Check Airflow logs for import errors
   - Verify Python paths in sys.path.append()
   - Ensure all required packages are installed

4. **Cosmos/DBT execution errors**
   - Verify DBT profiles are configured correctly
   - Check database connections
   - Ensure DBT executable path is correct

5. **CosmosValueError: Either profiles_yml_filepath or profile_mapping must be set**
   - This error occurs when Cosmos cannot find DBT connection configuration
   - Ensure `profiles.yml` exists in your DBT project directory
   - The system automatically looks for `profiles.yml` in the project path
   - Use the generator to create a default profiles.yml: `python dbt_dag_generator.py create-config`

6. **TypeError: ExecutionConfig.__init__() got an unexpected keyword argument 'env_vars'**
   - This error occurs when trying to pass `env_vars` to `ExecutionConfig`
   - DBT variables should be passed through `operator_args` with `vars` parameter
   - The system automatically handles this in the generated DAGs

7. **AttributeError: 'dict' object has no attribute 'project_path'**
   - This error occurs when `render_config` is passed as a dictionary instead of a `RenderConfig` object
   - The system now properly creates `RenderConfig` instances
   - Import `RenderConfig` from cosmos and use `RenderConfig(select=[...])` instead of `{"select": [...]}`

8. **CosmosLoadDbtException: dbt found two models with the same name**
   - This error occurs when there are duplicate model names across different directories
   - DBT requires unique model names across the entire project
   - Rename one of the conflicting models to resolve the issue
   - Example: `long_running_query.sql` in both `models/` and `models/custom/` directories

9. **Database Error: invalid identifier 'COLUMN_NAME'**
   - This error occurs when referencing columns that don't exist in the source models
   - Check the actual column names in the referenced models using `{{ ref('model_name') }}`
   - Common issue: assuming column names without checking the actual schema
   - Example: Using `lifetime_value` instead of `customer_lifetime_value`

10. **Function EXTRACT does not support NUMBER argument type**
    - This error occurs when using EXTRACT on date arithmetic results in Snowflake
    - In Snowflake, date subtraction returns a number, not an interval
    - Use `DATEDIFF('day', start_date, end_date)` instead of `EXTRACT(days FROM (end_date - start_date))`
    - The system now uses Snowflake-compatible date functions

### Debugging

Enable debug logging by adding to your DAG:

```python
import logging
logging.getLogger('cosmos').setLevel(logging.DEBUG)
```

View configuration loading:

```python
config_manager = DbtConfigManager(config_path)
print(f"Loaded config: {config_manager.config}")
```

## Examples

### Example 1: Simple Analytics Project

```json
{
    "dag_id": "analytics_dbt",
    "description": "Daily analytics refresh",
    "schedule_interval": "0 6 * * *",
    "dbt_project_name": "analytics",
    "dbt_project_path": "/usr/local/airflow/dbt/analytics",
    "dbt_profile_name": "warehouse",
    "task_groups": {
        "staging": {
            "models": ["staging"],
            "description": "Stage raw data"
        },
        "marts": {
            "models": ["customers", "products", "sales"],
            "description": "Build data marts",
            "depends_on": ["staging"]
        }
    }
}
```

### Example 2: Complex Multi-Stage Pipeline

```json
{
    "dag_id": "complex_pipeline_dbt",
    "description": "Multi-stage data pipeline",
    "schedule_interval": "@hourly",
    "task_groups": {
        "ingestion": {
            "models": ["raw"],
            "description": "Data ingestion layer"
        },
        "cleaning": {
            "models": ["staging"],
            "description": "Data cleaning and validation",
            "depends_on": ["ingestion"]
        },
        "features": {
            "models": ["features"],
            "description": "Feature engineering",
            "depends_on": ["cleaning"]
        },
        "aggregations": {
            "models": ["agg"],
            "description": "Data aggregations",
            "depends_on": ["features"]
        },
        "exports": {
            "models": ["exports"],
            "description": "Export to external systems",
            "depends_on": ["aggregations"]
        }
    }
}
```

## Migration from Existing DBT DAGs

To migrate existing DBT DAGs to this system:

1. **Extract configuration** from existing DAG files
2. **Create dag_configuration.json** with extracted settings
3. **Generate new DAG** using the template
4. **Test thoroughly** before replacing old DAGs
5. **Update any downstream dependencies**

## Best Practices

1. **Use descriptive DAG IDs** that clearly identify the project and purpose
2. **Organize task groups logically** based on data flow and dependencies
3. **Keep configurations in version control** alongside your DBT projects
4. **Use environment variables** for environment-specific settings
5. **Test configurations** in development before deploying to production
6. **Document custom configurations** and any project-specific requirements
7. **Monitor DAG performance** and adjust resource allocation as needed

## Contributing

When extending this system:

1. Follow the existing pattern of separating configuration from implementation
2. Add new configuration options to the schema documentation
3. Ensure backward compatibility with existing configurations
4. Add tests for new functionality
5. Update this documentation with new features 