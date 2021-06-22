# Smart Transfer Airflow Plugin

The SmartTransfer plugin is a easy and self contained way to transfer tables between two databases, with minimal configuration and efforts. 

It's a very good solution for simple cases when you need to copy a table from a database to another.

Main features are:
- Very simple to use
- Don't need extra libraries or packages (simple insert commands)
- Creates the table in the destination database automatically 
- Checks for changes in the source table structure and recreates the destination table if necessary
- Can handle tables up to millions of lines 

## Example:

```python
from airflow import DAG
from smart_transfer.smart_transfer import SmartTransfer

with DAG("copy_staging", schedule_interval='0 * * * *') as dag:

    task = SmartTransfer(
            task_id="extract_table",
            source_conn_id="production_db",
            source_table="users",
            destination_conn_id="datawarehouse",
            destination_table="staging_area.users",
            updated_at_filter=False,
            skip_columns=[],
            commit_every=1000
        )
```

# Limitations

- It was developed for Postgresql and Oracle only, but it's easy to be adapted to other databases
