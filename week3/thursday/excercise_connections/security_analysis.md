1. Storing password information in plain text means it is trivial for someone to copy if the credentials are stored online or if someone can gain access.
Using connections in airflow stores credentials encrypted.

2. Could store them in an encrypted file format. If the credentials are only need to be stored locally a .env file should be fine.

3. DAG would fail to authenticate and the query tasks would fail. Connection would need to be remade.