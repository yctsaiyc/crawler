from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# from smtp_agent import send_email
from datetime import timezone, timedelta, datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 2, 16, 40, 0, tzinfo=timezone(timedelta(hours=8))),
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    # 'on_failure_callback': send_email
    # 'tzinfo':timezone(timedelta(hours=8))
}


with DAG(
    "upload_config_Air_Temperature_across_Singapore",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    max_active_runs=1,
    tags=['SG-city'],
) as upload_config_Air_Temperature_across_Singapore:
    path = "/opt/airflow/playground/script/uploader"
    backup_task = BashOperator(
        task_id="upload_config_Air_Temperature_across_Singapore",
        bash_command=f" cd {path} ;python3 dp_upload_csv_PROD.py config/SG-city-environment/upload_config_Air-Temperature-across-Singapore.json config/SG-city-environment/dp_general_config.json",
        dag=upload_config_Air_Temperature_across_Singapore,
    )
    # clean_rawdata_task= BashOperator(task_id="clean_rawdata_task",bash_command=f'python3 {path}/clean_rawdata.py ',dag=daily_dag)upload_config_hum
