FROM quay.io/astronomer/astro-runtime:9.19.1

# # install dbt into a virtual environment
# RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#     pip install --no-cache-dir dbt-postgres==1.9.0 && deactivate

RUN python -m venv dbt_venv; \
    source dbt_venv/bin/activate; \
    pip install --no-cache-dir dbt-snowflake==1.9.4 'snowflake-connector-python[pandas]==3.15.0'; \ 
    deactivate;

# set connections to the airflow and application databases
ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled