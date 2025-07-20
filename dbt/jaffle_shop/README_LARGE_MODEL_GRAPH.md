# Large DBT Model Graph Project

This dbt project contains hundreds of models organized in a complex dependency graph. The models are structured in four layers:

1. **Raw Layer**: Initial data extraction with minimal transformations
2. **Staging Layer**: Basic cleaning and preparation of data
3. **Intermediate Layer**: Business logic and transformations across domains
4. **Mart Layer**: Final presentation layer for reporting and analysis

## Project Structure

```
models/
├── raw/
│   ├── sales/
│   ├── marketing/
│   ├── finance/
│   ├── product/
│   └── customer/
├── staging/
│   ├── sales/
│   ├── marketing/
│   ├── finance/
│   ├── product/
│   └── customer/
├── intermediate/
│   ├── sales/
│   ├── marketing/
│   ├── finance/
│   ├── product/
│   └── customer/
└── mart/
    ├── sales/
    ├── marketing/
    ├── finance/
    ├── product/
    └── customer/
```

## Model Domains

The models span across five business domains:

1. **Sales**: Order processing, revenue, and transactions
2. **Marketing**: Campaigns, attribution, and customer acquisition
3. **Finance**: Revenue recognition, costs, and profitability analysis
4. **Product**: Product usage, features, and performance metrics
5. **Customer**: Customer profiles, behavior, and segmentation

## Model Dependencies

The models have a complex dependency structure:

- **Raw models** have no dependencies but reference source tables
- **Staging models** depend on raw models within the same domain
- **Intermediate models** depend on staging models, potentially across domains
- **Mart models** depend on multiple intermediate models, often across domains

## Running with Cosmos

This project is designed to be run with Cosmos, which automatically converts the dbt project into an Airflow DAG with correct task dependencies.

To run the project:

1. Set up the required connection in Airflow (see `include/setup_connections.md`)
2. Trigger the `dbt_large_model_graph` DAG in Airflow
3. The DAG will execute all models in the correct dependency order

## Generating Models

The models in this project were generated using the script at `include/generate_dbt_models.py`. You can modify this script to generate more models or change the structure.

## Model Naming Convention

Models follow this naming convention:

```
{layer}_{domain}_{number}
```

For example:
- `raw_sales_001`
- `staging_marketing_042`
- `intermediate_finance_103`
- `mart_customer_187`