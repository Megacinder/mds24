# aws s3 cp ahremenko_ma_dag_2.py s3://gsb2024airflow/ahremenko_ma_dag_0.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from pendulum import duration

@dag(
    dag_id="ahremenko_ma_write_to_log",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="Mihail Ahremenko spam to log",
    tags=["dag_00"],
    dagrun_timeout=duration(minutes=10)
)
def my_dag():

    start = EmptyOperator(task_id='start_task')

    log_task = BashOperator(
        task_id="console_log_task",
        bash_command='echo "Hello, Airflow"'
    )

    end = EmptyOperator(task_id='end_task')

    start >> log_task >> end


my_dag()
