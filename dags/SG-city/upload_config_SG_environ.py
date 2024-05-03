from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timezone, timedelta, datetime

def create_dag(name, start_date, schedule_interval):

    y, mon, d, h, m, s = start_date

    default_args = {
        "owner": "airflow",
        "start_date": datetime(y, mon, d, h, m, s, tzinfo=timezone(timedelta(hours=8))),
        "retry_delay": timedelta(minutes=1),
        "catchup": False,
    }

    with DAG(
        f"upload_config_{name}",
        default_args=default_args,
        schedule_interval=schedule_interval,
        max_active_runs=1,
        tags=['SG-city'],
    ) as dag:
        path = "/opt/airflow/playground/script/uploader"
        backup_task = BashOperator(
            task_id=f"upload_config_{name}",
            bash_command=f" cd {path} ;"
                + "python3 dp_upload_csv_PROD.py"
                + f" config/SG-city-environment/upload_config_{name}.json"
                + " config/SG-city-environment/dp_general_config.json",
            dag=dag,
        )

    return dag
