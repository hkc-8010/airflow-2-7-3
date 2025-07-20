#!/bin/bash

# Script to generate hundreds of dbt models

echo "Starting the large dbt model generation process..."
echo "This script will create hundreds of dbt models in the jaffle_shop project."

# Change to the include directory
cd /usr/local/airflow/include

# Run the Python generator script
python generate_dbt_models.py

echo "Model generation complete!"
echo "You can now run the dbt_large_model_graph DAG in Airflow."
echo "Remember to set up the Snowflake connection as described in setup_connections.md"