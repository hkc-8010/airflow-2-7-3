FROM quay.io/astronomer/astro-runtime:9.19.1

# install dbt into a virtual environment
# RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#     pip install --no-cache-dir dbt-postgres==1.8.2 && deactivate

RUN bash -c " \
    python -m venv dbt_venv; \
    source dbt_venv/bin/activate; \
    pip install --no-cache-dir dbt-bigquery==1.9.1; \
    pip install --no-cache-dir dbt-core==1.9.3; \
    pip install --no-cache-dir dbt-snowflake==1.9.4; \
    pip install --no-cache-dir 'snowflake-connector-python[pandas]==3.15.0'; \ 
    deactivate;"

# set a connection to the airflow metadata db to use for testing
ENV AIRFLOW_CONN_AIRFLOW_METADATA_DB=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres
