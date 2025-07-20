# Setting up Connections for the Large DBT Model Graph

To run the large dbt model graph DAG, you need to set up a Snowflake connection in Airflow.

## Snowflake Connection Setup

1. Go to Airflow UI > Admin > Connections
2. Click "Add a new record" button
3. Fill in the following details:
   - Connection Id: `snowflake_default`
   - Connection Type: `Snowflake`
   - Host: `gp21411.us-east-1` (your Snowflake account)
   - Schema: `HEMKUMARCHHEDA`
   - Login: `HEMKUMARCHHEDA` (your Snowflake username)
   - Password: Your password
   - Port: Leave empty
   - Extra: 
     ```
     {
       "warehouse": "HUMANS",
       "database": "SANDBOX",
       "role": "HEMKUMARCHHEDA",
       "region": "us-east-1"
     }
     ```

## Running the DAG

After setting up the connection:

1. Make sure the script `include/generate_dbt_models.py` has been run to generate the models
2. Trigger the DAG `dbt_large_model_graph` from the Airflow UI
3. The DAG will load seed data, run all models, and test the results

## Troubleshooting

If you encounter any issues:

1. Verify the Snowflake connection details
2. Check that the dbt project path is correct in the DAG file
3. Ensure that the dbt executable path is correct for your environment
4. Check Airflow logs for detailed error messages

## Notes

- The DAG is configured to use the Snowflake connection with ID `snowflake_default`
- The DAG will create a task for each dbt model (200+ tasks)
- You may need to adjust the Airflow worker resources to handle the large DAG