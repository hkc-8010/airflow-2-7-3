#!/usr/bin/env python
"""
This script generates hundreds of dbt model files with a hierarchical structure.
The generated models will have dependencies between them to create a complex DAG.
"""

import os
import random

# Configuration
BASE_DIR = "../dbt/jaffle_shop/models"
LAYERS = ["raw", "staging", "intermediate", "mart"]
DOMAINS = ["sales", "marketing", "finance", "product", "customer"]
TOTAL_MODELS = 200  # Adjust as needed


def ensure_dir(directory):
    """Ensure directory exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)


def create_model_file(path, content):
    """Create a model file with the given content"""
    with open(path, "w") as f:
        f.write(content)


def generate_models():
    """Generate dbt models with dependencies"""
    # Track created models for dependency references
    created_models = []

    # Generate models for each layer and domain
    for i in range(TOTAL_MODELS):
        # Determine which layer this model belongs to
        if i < TOTAL_MODELS * 0.2:
            layer = LAYERS[0]  # raw layer (20% of models)
        elif i < TOTAL_MODELS * 0.5:
            layer = LAYERS[1]  # staging layer (30% of models)
        elif i < TOTAL_MODELS * 0.8:
            layer = LAYERS[2]  # intermediate layer (30% of models)
        else:
            layer = LAYERS[3]  # mart layer (20% of models)

        # Pick a domain randomly
        domain = random.choice(DOMAINS)

        # Create model name and file path
        model_name = f"{layer}_{domain}_{i:03d}"
        model_dir = os.path.join(BASE_DIR, layer, domain)
        ensure_dir(model_dir)
        model_path = os.path.join(model_dir, f"{model_name}.sql")

        # Generate SQL content based on layer
        if layer == "raw":
            # Raw models have no dependencies
            sql_content = f"""
-- {model_name}.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_{i % 5 + 1} as metric_{i % 3 + 1}
FROM raw_source_{domain}_{i % 10 + 1}
"""
        elif layer == "staging":
            # Staging models depend on raw models
            dependencies = []
            # Find suitable raw models to depend on
            for m in created_models:
                if "raw_" in m and domain in m:
                    dependencies.append(m)

            # If no suitable dependencies found, create without dependencies
            if not dependencies:
                sql_content = f"""
-- {model_name}.sql
-- Staging model

SELECT
    id,
    name,
    created_at,
    metric_1,
    metric_2
FROM {{{{ source('{domain}', 'source_table_{i % 5 + 1}') }}}}
"""
            else:
                # Select 1-3 random dependencies
                selected_deps = random.sample(
                    dependencies, min(random.randint(1, 3), len(dependencies))
                )
                sql_content = f"""
-- {model_name}.sql
-- Staging model

WITH source_data AS (
    {" UNION ALL ".join([f"SELECT id, name, created_at, metric_1, metric_2 FROM {{{{ ref('{dep}') }}}}" for dep in selected_deps])}
)

SELECT
    id,
    name,
    created_at,
    SUM(metric_1) as total_metric_1,
    AVG(metric_2) as avg_metric_2
FROM source_data
GROUP BY 1, 2, 3
"""
        elif layer == "intermediate":
            # Intermediate models depend on staging models
            dependencies = []
            # Find suitable staging models to depend on
            for m in created_models:
                if "staging_" in m and (
                    domain in m or random.random() < 0.3
                ):  # 30% chance to include cross-domain dependencies
                    dependencies.append(m)

            # If no suitable dependencies found, use staging models
            if not dependencies:
                sql_content = f"""
-- {model_name}.sql
-- Intermediate model (no specific dependencies found)

SELECT
    id,
    name,
    created_at,
    total_metric_1 * 1.5 as adjusted_metric_1,
    avg_metric_2 + 100 as normalized_metric_2
FROM {{{{ ref('staging_{domain}_{i % 50:03d}') }}}}
"""
            else:
                # Select 1-3 random dependencies
                selected_deps = random.sample(
                    dependencies, min(random.randint(1, 3), len(dependencies))
                )
                sql_content = f"""
-- {model_name}.sql
-- Intermediate model

WITH source_data AS (
    {" UNION ALL ".join([f"SELECT id, name, created_at, total_metric_1, avg_metric_2 FROM {{{{ ref('{dep}') }}}}" for dep in selected_deps])}
)

SELECT
    id,
    name,
    created_at,
    SUM(total_metric_1) as enhanced_metric_1,
    AVG(avg_metric_2) as enhanced_metric_2,
    COUNT(*) as record_count
FROM source_data
GROUP BY 1, 2, 3
"""
        else:  # mart layer
            # Mart models depend on intermediate models
            dependencies = []
            # Find suitable intermediate models to depend on
            for m in created_models:
                if "intermediate_" in m:
                    dependencies.append(m)

            # If no suitable dependencies found, use intermediate models
            if not dependencies:
                sql_content = f"""
-- {model_name}.sql
-- Mart model (no specific dependencies found)

SELECT
    id,
    name,
    created_at,
    enhanced_metric_1,
    enhanced_metric_2,
    record_count
FROM {{{{ ref('intermediate_{domain}_{i % 50:03d}') }}}}
"""
            else:
                # Select 2-4 random dependencies
                selected_deps = random.sample(
                    dependencies, min(random.randint(2, 4), len(dependencies))
                )
                sql_content = f"""
-- {model_name}.sql
-- Mart model

WITH source_data AS (
    {" UNION ALL ".join([f"SELECT id, name, created_at, enhanced_metric_1, enhanced_metric_2, record_count FROM {{{{ ref('{dep}') }}}}" for dep in selected_deps])}
)

SELECT
    name,
    MAX(created_at) as latest_update,
    SUM(enhanced_metric_1) as total_metric_1,
    AVG(enhanced_metric_2) as avg_metric_2,
    SUM(record_count) as total_records
FROM source_data
GROUP BY 1
"""

        # Create the file
        create_model_file(model_path, sql_content)
        created_models.append(model_name)
        print(f"Created model: {model_name}")

    return created_models


def create_source_yml():
    """Create a source YAML file for raw data sources"""
    source_content = """
version: 2

sources:
"""

    # Add sources for each domain
    for domain in DOMAINS:
        source_content += f"""
  - name: {domain}
    database: SANDBOX
    schema: HEMKUMARCHHEDA
    tables:
"""
        # Add 5 source tables per domain
        for i in range(1, 6):
            source_content += f"""
      - name: source_table_{i}
        description: Raw data source for {domain} domain, table {i}
        columns:
          - name: id
            description: Primary key
          - name: name
            description: Entity name
          - name: created_at
            description: Creation timestamp
          - name: value_1
            description: Metric 1
          - name: value_2
            description: Metric 2
          - name: value_3
            description: Metric 3
"""

    # Write sources.yml file
    source_path = os.path.join(BASE_DIR, "sources.yml")
    with open(source_path, "w") as f:
        f.write(source_content)

    print(f"Created sources.yml file at {source_path}")


def create_schema_yml(models):
    """Create schema.yml files for model documentation"""
    # Group models by layer and domain
    models_by_dir = {}

    for model in models:
        parts = model.split("_")
        layer = parts[0]
        domain = parts[1]
        dir_path = os.path.join(BASE_DIR, layer, domain)

        if dir_path not in models_by_dir:
            models_by_dir[dir_path] = []

        models_by_dir[dir_path].append(model)

    # Create schema.yml for each directory
    for dir_path, dir_models in models_by_dir.items():
        schema_content = """
version: 2

models:
"""

        # Add model documentation
        for model in dir_models:
            parts = model.split("_")
            layer = parts[0]
            domain = parts[1]

            schema_content += f"""
  - name: {model}
    description: "{layer.capitalize()} layer model for {domain} domain"
    columns:
"""

            # Add columns based on layer
            if layer == "raw":
                schema_content += """
      - name: id
        description: Primary key
      - name: name
        description: Entity name
      - name: created_at
        description: Creation timestamp
      - name: metric_1
        description: Metric 1
      - name: metric_2
        description: Metric 2
"""
            elif layer == "staging":
                schema_content += """
      - name: id
        description: Primary key
      - name: name
        description: Entity name
      - name: created_at
        description: Creation timestamp
      - name: total_metric_1
        description: Sum of metric 1
      - name: avg_metric_2
        description: Average of metric 2
"""
            elif layer == "intermediate":
                schema_content += """
      - name: id
        description: Primary key
      - name: name
        description: Entity name
      - name: created_at
        description: Creation timestamp
      - name: enhanced_metric_1
        description: Enhanced metric 1
      - name: enhanced_metric_2
        description: Enhanced metric 2
      - name: record_count
        description: Count of records
"""
            else:  # mart
                schema_content += """
      - name: name
        description: Entity name
      - name: latest_update
        description: Latest update timestamp
      - name: total_metric_1
        description: Total metric 1
      - name: avg_metric_2
        description: Average metric 2
      - name: total_records
        description: Total records count
"""

        # Write schema.yml file
        schema_path = os.path.join(dir_path, "schema.yml")
        with open(schema_path, "w") as f:
            f.write(schema_content)

        print(f"Created schema.yml file at {schema_path}")


def main():
    """Main function"""
    print("Starting dbt model generation...")

    # Ensure base directories exist
    for layer in LAYERS:
        for domain in DOMAINS:
            ensure_dir(os.path.join(BASE_DIR, layer, domain))

    # Generate models
    created_models = generate_models()

    # Create source definition
    create_source_yml()

    # Create schema files
    create_schema_yml(created_models)

    print(f"Successfully created {len(created_models)} dbt models!")


if __name__ == "__main__":
    main()
