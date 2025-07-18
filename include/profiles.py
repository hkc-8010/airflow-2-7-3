"Contains profile mappings used in the project"

from cosmos import ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping


airflow_db = ProfileConfig(
    profile_name="airflow_db",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_metadata_db",
        profile_args={
            "schema": "public",
            # Add quotes around database name to handle hyphens properly
            "dbname": '"frigid-relativity-1982-pgbouncer-metadata"',
            # Set search_path to public to ensure tables are created in the correct schema
            "options": "-c search_path=public",
        },
    ),
)
