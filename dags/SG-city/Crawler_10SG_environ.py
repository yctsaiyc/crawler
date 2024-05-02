from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timezone, timedelta, datetime


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 2, 0, 0, 0, tzinfo=timezone(timedelta(hours=8))),
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

with DAG(
    "Crawler_10SG_environ.py",
    default_args=default_args,
    schedule_interval="* * * * *",
    max_active_runs=1,
    tags=["SG-city"],
) as Crawler_10SG_environ:
    path = "/opt/airflow/playground/script/crawler/SG-city"
    backup_task = BashOperator(
        task_id="Crawler_10SG_environ",
        bash_command=f" cd {path} ;python3 SG_environ.py",
        dag=Crawler_10SG_environ,
    )
