"Contains profile mappings used in the project"

from cosmos import ProfileConfig
from cosmos.profiles import (
    SnowflakeUserPasswordProfileMapping,
)


# Legacy PostgreSQL profile (commented out for reference)
# airflow_db = ProfileConfig(
#     profile_name="airflow_db",
#     target_name="dev",
#     profile_mapping=PostgresUserPasswordProfileMapping(
#         conn_id="airflow_metadata_db",
#         profile_args={
#             # Use airflow as the schema name instead of public
#             "schema": "airflow",
#         },
#     ),
# )

# Snowflake profile
airflow_db = ProfileConfig(
    profile_name="airflow_db",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "schema": "HEMKUMARCHHEDA",
            "database": "SANDBOX",
            "role": "HEMKUMARCHHEDA",
            "warehouse": "HUMANS",
            "account": "gp21411",
            "region": "us-east-1",
        },
    ),
)
