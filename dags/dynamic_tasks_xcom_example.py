"""
## Dynamic Task Creation with XCom Example

This DAG demonstrates how to create dynamic tasks based on data received from
upstream tasks via XCom. It shows multiple patterns for dynamic task mapping
using both simple lists and complex data structures.

The DAG includes:
1. An upstream task that generates a list of items to process
2. Dynamic task mapping that creates tasks for each item
3. A reduce task that collects all results
4. Examples with both simple and complex data structures

This pattern is useful for ETL pipelines where the number of processing tasks
depends on runtime data (e.g., number of files to process, list of databases
to query, or API endpoints to call).
"""

from airflow.decorators import dag, task
from pendulum import datetime
from typing import List, Dict, Any
import random


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger for demo purposes
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 1},
    tags=["example", "dynamic-tasks", "xcom"],
)
def dynamic_tasks_xcom_example():
    # =====================================
    # Example 1: Simple List Processing
    # =====================================

    @task
    def generate_simple_list() -> List[str]:
        """
        Generate a list of items to process. This could come from:
        - Database query results
        - API responses
        - File system scans
        - Configuration files
        """
        # Simulate dynamic data - in real scenarios this might come from:
        # - SELECT DISTINCT region FROM sales_data WHERE date = '{{ ds }}'
        # - API call to get list of active customers
        # - File listing from S3 bucket
        return ["region_a", "region_b", "region_c", "region_d"]

    @task
    def process_region(region: str) -> Dict[str, Any]:
        """
        Process each region. This represents any processing task that
        needs to be done for each item in the list.
        """
        # Simulate some processing work
        processed_count = random.randint(100, 1000)
        success_rate = round(random.uniform(0.85, 0.99), 3)

        print(f"Processing region: {region}")
        print(f"Processed {processed_count} records")
        print(f"Success rate: {success_rate}")

        return {
            "region": region,
            "processed_count": processed_count,
            "success_rate": success_rate,
            "status": "completed",
        }

    @task
    def summarize_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Collect and summarize all results from the mapped tasks.
        This is the 'reduce' part of the map-reduce pattern.
        """
        total_processed = sum(result["processed_count"] for result in results)
        avg_success_rate = sum(result["success_rate"] for result in results) / len(
            results
        )
        regions_processed = [result["region"] for result in results]

        summary = {
            "total_regions": len(results),
            "regions_processed": regions_processed,
            "total_records_processed": total_processed,
            "average_success_rate": round(avg_success_rate, 3),
            "all_successful": all(
                result["status"] == "completed" for result in results
            ),
        }

        print("Processing Summary:")
        for key, value in summary.items():
            print(f"  {key}: {value}")

        return summary

    # =====================================
    # Example 2: Complex Data Structures
    # =====================================

    @task
    def generate_complex_tasks() -> List[Dict[str, Any]]:
        """
        Generate a more complex list with different task configurations.
        Each item contains parameters for the downstream task.
        """
        return [
            {"database": "sales_db", "table": "daily_sales", "partition": "2024-01-01"},
            {"database": "sales_db", "table": "monthly_sales", "partition": "2024-01"},
            {
                "database": "customer_db",
                "table": "user_activity",
                "partition": "2024-01-01",
            },
            {
                "database": "inventory_db",
                "table": "stock_levels",
                "partition": "2024-01-01",
            },
        ]

    @task
    def process_database_table(task_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process each database table with its specific configuration.
        """
        database = task_config["database"]
        table = task_config["table"]
        partition = task_config["partition"]

        # Simulate database processing
        rows_processed = random.randint(1000, 10000)
        processing_time = round(random.uniform(1.5, 5.0), 2)

        print(f"Processing {database}.{table} for partition {partition}")
        print(f"Processed {rows_processed} rows in {processing_time} seconds")

        return {
            "database": database,
            "table": table,
            "partition": partition,
            "rows_processed": rows_processed,
            "processing_time_seconds": processing_time,
            "status": "success",
        }

    # =====================================
    # Example 3: Conditional Task Creation
    # =====================================

    @task
    def generate_conditional_list(**context) -> List[str]:
        """
        Generate a list based on some condition (e.g., day of week, data availability).
        This shows how the number and type of dynamic tasks can vary by run.
        """
        # Get the execution date
        execution_date = context["ds"]

        # Different processing based on conditions
        if execution_date.endswith(("01", "15")):  # 1st and 15th of month
            # Full processing on specific days
            return ["full_etl", "data_validation", "report_generation", "cleanup"]
        else:
            # Light processing on other days
            return ["incremental_etl", "basic_validation"]

    @task
    def execute_conditional_task(task_type: str, **context) -> str:
        """
        Execute different types of tasks based on the task_type parameter.
        """
        execution_date = context["ds"]

        task_actions = {
            "full_etl": "Running complete ETL pipeline with all transformations",
            "data_validation": "Performing comprehensive data quality checks",
            "report_generation": "Generating monthly business reports",
            "cleanup": "Cleaning up temporary files and old data",
            "incremental_etl": "Processing only new/changed records",
            "basic_validation": "Running essential data quality checks",
        }

        action = task_actions.get(task_type, "Unknown task type")
        print(f"[{execution_date}] {task_type}: {action}")

        return f"{task_type}_completed"

    # =====================================
    # Task Dependencies and Mapping
    # =====================================

    # Simple list processing flow
    simple_list = generate_simple_list()
    region_results = process_region.expand(region=simple_list)
    simple_summary = summarize_results(region_results)

    # Complex data structure processing flow
    complex_tasks = generate_complex_tasks()
    db_results = process_database_table.expand(task_config=complex_tasks)

    # Conditional task creation flow
    conditional_list = generate_conditional_list()
    conditional_results = execute_conditional_task.expand(task_type=conditional_list)

    # Set up dependencies to ensure proper execution order
    # The summarize_results task depends on all region processing being complete
    simple_list >> region_results >> simple_summary

    # Complex and conditional flows can run in parallel with simple flow
    complex_tasks >> db_results
    conditional_list >> conditional_results


# Instantiate the DAG
dynamic_tasks_xcom_example()
