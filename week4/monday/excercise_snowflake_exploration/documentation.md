TASK 2

What databases exist by default? 
Snowflake_learing_db and snowflake_sample_data

What is the name of the default virtual warehouse?
There are 3 already on my account, COMPUTE_WH. SNOWFLAKE_LEARNING_WH and SYSTEM$STREAMLIT_NOTEBOOK_WH

What role are you currently using?
ACCOUNTADMIN



TASK 3

Record the execution time for your COUNT(*) query.
Execution time: 1.1s




TASK 4

describe the three layers of Snowflake architecture
Cloud Service Layer: Coordinates activitys, manages metadata. Handles authentication and user accoutn control, sql parsing and optimization and infastructure managment.
Query Proccessing: Executes query plan from the cloud service layer, uses virtual warehouses for concurrent and isolated paralel query runs.
Database Storage: Fully managed storage. Data is stored in fault tolerant, immutable, compressed micro partitions. Uses cloud object storage like AWS S3 buckets.

Observation table
Virtual Warehouse: Independant compute clusters, isolated query execution
Database: Where the data is stored, Uses the cloud storage object of the cloud service provider being used.
Schema: List of columns and constraints, Defines the structure of the table
Table: Structured format where the data is stored, defines data relationships and format
Role: The permission level of the user, controls access

Document the AUTO_SUSPEND setting and explain why it matters.
A cost saving mesure, prevents querys from executing for a long time and wasting money/free credits.
