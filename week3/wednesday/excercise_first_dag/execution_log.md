# Execution Log - My First DAG

## DAG Information

**DAG ID:** my_first_pipeline  
**Execution Date:**  
**Status:** 

---

## Screenshots

### 1. DAG Grid View (Successful Run)

[Paste your screenshot here or describe what you see]
Start, proccess, report, and end are all green. Graph shows they occur one after the other.

### 2. Task Logs - Process Task

[Paste the relevant log output showing your print statements]

[2026-03-12, 13:26:50 UTC] {logging_mixin.py:154} INFO - Processing data...
[2026-03-12, 13:26:50 UTC] {logging_mixin.py:154} INFO - Status: [SUCCESS], Records Processed: 100
[2026-03-12, 13:26:50 UTC] {python.py:194} INFO - Done. Returned value was: {'records_processed': 100, 'status': 'success'}

[2026-03-12, 13:26:51 UTC] {subprocess.py:86} INFO - Output:
[2026-03-12, 13:26:51 UTC] {subprocess.py:93} INFO - Generating report...

[2026-03-12, 13:26:52 UTC] {logging_mixin.py:154} INFO - Pipeline execution complete!
[2026-03-12, 13:26:52 UTC] {logging_mixin.py:154} INFO - 2026-03-12 13:26:52.392147
[2026-03-12, 13:26:52 UTC] {python.py:194} INFO - Done. Returned value was: Success

---

## Reflection Questions

### Question 1
What is the difference between an Operator and a Task?

**Your Answer:**
Operator is a template for a kind of work, a task is an instance of that operator

### Question 2
Why did we set `catchup=False`? What would happen if it was True?

**Your Answer:**
If it was true the scheduler would create dags for all intervals up to now. Since the start date was jan 1st 2026, that would make 70 or so Dags.

### Question 3
What does the `>>` operator do in Airflow?

**Your Answer:**
Defines task order.
Start >> proccess means start must finish before proccess can run

### Question 4
If the `process` task failed, what would happen to the `report` and `end` tasks?

**Your Answer:**
They would not run, since they are dependent on proccess.

---

## Task Execution Times

| Task | Duration | Status |
|------|----------|--------|
| start | | |
| process | | |
| report | | |
| end | | |

---

## Issues Encountered

Describe any problems you faced and how you solved them:
My DAG failed initially and I needed to debug the script.
The airflow UI showed it was an issue with the function calls.
I realized instead of calling them I jsut needed to pass the name

---

## Key Learnings

What are the 3 most important things you learned from this exercise?

1. Navigating the Airflow UI
2. Defining task order
3. Difference between using batch and python operators