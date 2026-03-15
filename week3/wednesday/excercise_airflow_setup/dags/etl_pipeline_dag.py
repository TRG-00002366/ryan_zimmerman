"""
Complete ETL Pipeline Exercise
==============================
Build an end-to-end ETL pipeline with:
- File sensor
- Data validation
- Transformation
- Loading
- Error handling and notifications

Complete the TODO sections.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
# Import FileSensor
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import json
import os
import csv


# ============================================================
# Configuration
# ============================================================

INPUT_PATH = "/opt/airflow/data/input/orders.csv"
OUTPUT_PATH = "/opt/airflow/data/output/processed_orders.json"
MIN_RECORDS = 5


# ============================================================
# Callbacks
# ============================================================

def on_failure(context):
    """
    Implement failure callback.
    
    Log the failure details:
    - DAG ID
    - Task ID
    - Exception message
    """
    task = context.get("task_instance")
    exception = context.get("exception")
    
    print("=" * 50)
    print("PIPELINE FAILURE!")
    # YOUR CODE HERE
    print(f"Dag ID: {task.dag_id}")
    print(f"Task ID: {task.task_id}")
    print(f"Exception: {str(exception)}")
    print("=" * 50)


# ============================================================
# Task Functions
# ============================================================

def extract_data(**context):
    """
    Extract data from the CSV file.
    
    Steps:
    1. Read the CSV file
    2. Parse into list of dictionaries
    3. Validate minimum row count
    4. Push data to XCom
    5. Return extraction metadata
    """
    print(f"Extracting from {INPUT_PATH}...")
    
    # YOUR CODE HERE
    data = []
    with open(INPUT_PATH, mode='r', newline='', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            data.append(row)
   
    if len(data) < 5:
        raise ValueError(f"Not enough data, need atleast 5 rows only got {len(data)}")
    
    ti = context["ti"]
    ti.xcom_push(key="data", value=data)
    return {"status": "extracted", "count": len(data)}
    

def validate_data(**context):
    """
    Validate the extracted data.
    
    Steps:
    1. Pull data from extract task via XCom
    2. Check required columns exist
    3. Check for null order_ids
    4. Raise ValueError if validation fails
    5. Return validation summary
    """
    ti = context["ti"]
    
    print("Validating data...")
    
    # YOUR CODE HERE
    data = ti.xcom_pull(task_ids="extract", key="data")
    
    keys = list(data[0].keys())
    keys.sort()
    if len(keys) != 4 or keys[3] != "order_id" or keys[0] != "amount" or keys[1] != "customer_id" or keys[2] != "order_date":
        raise ValueError("Data is not in the correct format!") 

    for row in data:
        if not row["order_id"]:
            raise ValueError("Data has null values in the order_id columns!") 
    
    print(f"Validated {len(data)} records")
    return {"status": "validated"}


def transform_data(**context):
    """
    Transform the data.
    
    Steps:
    1. Pull data from XCom
    2. Parse amounts as floats
    3. Add processing timestamp
    4. Calculate any aggregations
    5. Push transformed data to XCom
    """
    ti = context["ti"]
    
    print("Transforming data...")
    
    # YOUR CODE HERE
    data = ti.xcom_pull(task_ids="extract", key="data")
    total = 0
    for row in data:
        total += float(row["amount"])
        row["last_proccessed"] = str(datetime.now())

    ti.xcom_push(key="data", value=data)
    print(f"Total: {total}")
    return {"total": total}

def load_data(**context):
    """
    Load data to destination.
    
    Steps:
    1. Pull transformed data from XCom
    2. Create output structure with metadata
    3. Write to JSON file
    4. Return load summary
    """
    ti = context["ti"]

    print(f"Loading to {OUTPUT_PATH}...")
    
    data = ti.xcom_pull(task_ids="transform", key="data")

    # YOUR CODE HERE
    with open(OUTPUT_PATH, 'w') as json_file:
        json.dump(data, json_file, indent=1)
    
    return {"status": "loaded"}



def send_notification(**context):
    """
    Send success notification.
    
    Pull results from load task and print summary.
    In production, this would send email/Slack/etc.
    """
    ti = context["ti"]
    
    print("=" * 50)
    print("PIPELINE SUCCESS!")
    # YOUR CODE HERE
    data = ti.xcom_pull(task_ids="extract", key="data")
    print(f"Proccessed {len(data)} records")
    print("=" * 50)


# ============================================================
# DAG Definition
# ============================================================

# Configure default_args with retries and callbacks
default_args = {
    "owner": "data_team",
    # YOUR CODE HERE: Add retries, retry_delay, on_failure_callback
    "retries": 0,
    "retry_delay": 5,
    "on_failure_callback": on_failure
}

with DAG(
    dag_id="etl_exercise",
    description="Complete ETL pipeline exercise",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "etl"],
) as dag:
    
    # Create FileSensor to wait for input file
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INPUT_PATH,
        poke_interval=30,
        timeout=3600,
        mode="poke"
    )
        
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data
    )
    
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_data
    )
    
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data
    )
    
    load = PythonOperator(
        task_id="load",
        python_callable=load_data
    )
    
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=send_notification,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    # Dependencies
    wait_for_file >> extract >> validate >> transform >> load >> notify >> end