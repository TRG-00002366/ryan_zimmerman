TASK 5:
Regular run
 1 of 1 OK created sql incremental model DBT_RYAN.fct_events_incremental ........ [SUCCESS 3 in 3.39s]

 Full refresh
 1 of 1 OK created sql incremental model DBT_RYAN.fct_events_incremental ........ [SUCCESS 10 in 1.61s]

 Strangly the full refresh took less time despite proccessing more rows. 
 This is likley due to the low row count, if the table accrued millions of rows, incrementally proccessing 3 rows would be much faster than a full refresh.

 TASK 6:
 Append is best for very frequent inserts, such as logs
 Merge has a key, if the key exists it will replace the row instead of inserting. 
 Delete+ Insert is nearly identical, except it will delete existing rows and insert the new one rather than updating.
 