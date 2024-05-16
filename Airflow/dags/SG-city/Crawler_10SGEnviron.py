from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timezone, timedelta, datetime


def create_dag(
    name="", start_date=(2024, 5, 2, 0, 0, 0), schedule_interval="* * * * *"
):
    y, mon, d, h, m, s = start_date

    default_args = {
        "owner": "airflow",
        "start_date": datetime(y, mon, d, h, m, s, tzinfo=timezone(timedelta(hours=8))),
        "retry_delay": timedelta(minutes=1),
        "catchup": False,
    }

    with DAG(
        f"Crawler_10SGEnviron_{name}.py",
        default_args=default_args,
        schedule_interval=schedule_interval,
        max_active_runs=1,
        tags=["SG-city"],
    ) as dag:
        path = "/opt/airflow/playground/script/crawler/SG-city"
        backup_task = BashOperator(
            task_id=f"Crawler_10SG_{name}",
            bash_command=f" cd {path} ;"
            + "python3 SG_environ.py"
            + f" Environ_{name}/Environ_{name}.json",
            dag=dag,
        )

    return dag
