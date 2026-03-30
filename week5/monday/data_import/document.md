| Scenario | Recommended Mode | Justification |
| --- | --- | --- |
| Dashboard refreshed weekly, 50K rows | Import | Small dataset and infrequent refresh make import the fastest. |
| Real-time operational dashboard, 1M rows | DirectQuery | The dashboard needs near real-time data, so we cannot rely on refreshes. |
| CFO report with 5 years of history, 100M rows | Import | Since the data does not change, we can import it for better performance |
| Ad-hoc analysis on rapidly changing data | DirectQuery | Frequently changing data is better served by live queries  |
| Mobile dashboard with offline access needs | Import | Offline means direct query is not an option |