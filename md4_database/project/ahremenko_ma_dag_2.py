# aws s3 cp ahremenko_ma_dag_2.py s3://gsb2024airflow/ahremenko_ma_dag_0.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net


from datetime import datetime, timedelta
# from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task

S3_BUCKET = 'gsbdwhdata'
S3_LOCATION = 'ahremenko_ma/flights/new_flights.csv'
PG_CONN_ID = "con_dwh_2024_s004"

SQL_LIST = [
    "CREATE SCHEMA IF NOT EXISTS ods",
    "DROP TABLE IF EXISTS ods.flights",
    """
    create table ods.flights (
            year INTEGER,
            month INTEGER,
            flight_dt TEXT,
            carrier_code TEXT DEFAULT '',
            tail_num TEXT DEFAULT '',
            carrier_flight_num TEXT DEFAULT '',
            origin_code TEXT,
            origin_city_name TEXT,
            dest_code TEXT,
            dest_city_name TEXT,
            scheduled_dep_tm TEXT DEFAULT '',
            actual_dep_tm TEXT DEFAULT '',
            dep_delay_min float DEFAULT 0,
            dep_delay_group_num float DEFAULT 0,
            wheels_off_tm TEXT DEFAULT '',
            wheels_on_tm TEXT DEFAULT '',
            scheduled_arr_tm TEXT DEFAULT '',
            actual_arr_tm TEXT DEFAULT '',
            arr_delay_min float DEFAULT 0,
            arr_delay_group_num float DEFAULT 0,
            cancelled_flg float DEFAULT 0,
            cancellation_code TEXT DEFAULT '',
            flights_cnt float DEFAULT 0,
            distance float DEFAULT 0,
            distance_group_num float DEFAULT 0,
            carrier_delay_min float DEFAULT 0,
            weather_delay_min float DEFAULT 0,
            nas_delay_min float DEFAULT 0,
            security_delay_min float DEFAULT 0,
            late_aircraft_delay_min float DEFAULT 0
        )
    """,
]


@dag(
    dag_id="ahremenko_ma_flights_copy_2",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Ahremenko MA Копирование файла перелетов в промежуточную таблицу",
    tags=["dag_02"],
)
def flights_copy_dag():
    s3_sensor = S3KeySensor(
        task_id='s3_file_check',
        poke_interval=60,
        timeout=180,
        soft_fail=False,
        retries=2,
        bucket_key=S3_LOCATION,
        bucket_name=S3_BUCKET,
        aws_conn_id='object_storage_yc',
    )


    task_create_table = SQLExecuteQueryOperator(
        task_id='sql_src_table',
        sql=SQL_LIST,
        autocommit=True,
        conn_id=PG_CONN_ID,
        parameters=None,
        split_statements=False,
    )


    @task()
    def write_file_to_db() -> None:
        s3_hook = S3Hook(aws_conn_id='object_storage_yc')
        file_name = s3_hook.download_file(
            key=S3_LOCATION,
            bucket_name=S3_BUCKET,
        )

        if file_name:
            postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            with open(file_name, "r") as file:
                cur.copy_expert(
                    "COPY ods.flights FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    file,
                )
            conn.commit()

    s3_sensor >> task_create_table >> write_file_to_db()


instance = flights_copy_dag()
