FROM quay.io/astronomer/astro-runtime:9.19.1

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres==1.9.0 && deactivate

# RUN python -m venv dbt_venv; \
#     source dbt_venv/bin/activate; \
#     pip install --no-cache-dir dbt-bigquery==1.9.1 dbt-core==1.9.3 dbt-snowflake==1.9.4 dbt-postgres==1.9.3 'snowflake-connector-python[pandas]==3.15.0'; \ 
#     deactivate;"

# set connections to the airflow and application databases
ENV AIRFLOW_CONN_AIRFLOW_METADATA_DB=postgresql://frigid_relativity_1982_airflow:XKwtMF1Sf1k8GwVJlMyEg8xPwgi5soz5@frigid-relativity-1982-pgbouncer.frigid-relativity-1982.svc.cluster.local:6543/frigid-relativity-1982-pgbouncer-metadata?sslmode=disable
ENV AIRFLOW_CONN_AIRFLOW_DB=postgresql://frigid_relativity_1982_airflow:XKwtMF1Sf1k8GwVJlMyEg8xPwgi5soz5@frigid-relativity-1982-pgbouncer.frigid-relativity-1982.svc.cluster.local:6543/frigid-relativity-1982-pgbouncer-metadata?sslmode=disable