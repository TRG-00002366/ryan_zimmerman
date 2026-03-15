"""
Monitoring and Troubleshooting Exercise
========================================
This DAG contains INTENTIONAL BUGS for debugging practice.

Your tasks:
1. Configure SLAs and alerting
2. Deploy and observe failures
3. Debug using logs
4. Fix the bugs

Bugs to find:
- Import error
- Data error
- Logic error
- Type error

Complete the TODO sections AND fix the bugs!
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import time
import random

# ============================================================
# SLA Configuration
# ============================================================

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Implement SLA miss callback.
    
    Steps:
    1. Log the DAG ID
    2. Log the tasks that missed SLA
    3. Log the SLA details
    4. In production, send alert (Slack, email, etc.)
    """
    print("=" * 50)
    print("SLA MISS ALERT")
    # YOUR CODE HERE
    print(f"DAG ID: {dag.dag_id}")
    print(f"Tasks that missed SLA: {task_list}")
    print(f"SLA details: {slas}")

    print("=" * 50)


# ============================================================
# Failure Callback
# ============================================================

def on_failure_callback(context):
    """
    Implement failure callback.
    
    Extract from context:
    - task_instance
    - exception
    - dag_id
    - task_id
    
    Log all relevant information.
    """
    # YOUR CODE HERE
    print(f"Task {context.get('task_instance').task_id} failed.")
    print(f"Exception: {context.get('task_instance').exception}")
    print(f"Dag: {context.get('task_instance').dag_id}")


# ============================================================
# Task Functions (Contains Bugs)
# ============================================================

def extract_data(**context):
    """Extract data from source."""
    print("Extracting data...")
    time.sleep(1)
    
    data = [
        {"order_id": 1, "amount": 100}, 
        {"order_id": 2, "amount": 200},
        {"order_id": 3, "amount": 150},
    ]
    
    context["ti"].xcom_push(key="data", value=data)
    return {"status": "extracted", "count": len(data)}


def validate_data(**context):
    """Validate extracted data."""
    print("Validating data...")
    ti = context["ti"]
    
    data = ti.xcom_pull(task_ids="extract", key="data")
    
    if len(data) == 0:
        raise ValueError("No data to validate!")
    
    print(f"Validated {len(data)} records")
    return {"status": "validated"}


def transform_data(**context):
    """Transform the data."""
    print("Transforming data...")
    ti = context["ti"]
    
    data = ti.xcom_pull(task_ids="extract", key="data")
    
    total = 0
    for record in data:
        total += record["amount"]
    
    print(f"Total: {total}")
    return {"total": total}


def load_data(**context):
    """Load data to destination."""
    print("Loading data...")
    time.sleep(1)
    print("Load complete!")
    return {"status": "loaded"}


def notify_success(**context):
    """Send success notification."""
    print("Pipeline completed successfully!")


# ============================================================
# DAG Definition
# ============================================================

# Configure default_args with failure callback
default_args = {
    "owner": "training",
    "retries": 0,  # No retries so we see failures immediately
    # YOUR CODE HERE: Add on_failure_callback
    "on_failure_callback": on_failure_callback
}

with DAG(
    dag_id="monitoring_exercise",
    description="Debugging exercise with intentional bugs",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["exercise", "debugging"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # Add SLA to extract (5 minutes)
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        # YOUR CODE HERE: Add sla=timedelta(minutes=5)
        sla = timedelta(minutes=5)
    )
    
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_data
    )
    
    # Add SLA to transform (10 minutes)
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        # YOUR CODE HERE: Add sla=timedelta(minutes=10)
        sla=timedelta(minutes=10)
    )
    
    # Add SLA to load (3 minutes)
    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
        # YOUR CODE HERE: Add sla=timedelta(minutes=3)
        sla=timedelta(minutes=3)
    )
    
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    end = EmptyOperator(task_id="end")
    
    # Dependencies
    start >> extract >> validate >> transform >> load >> notify >> end
