# copy file from s3 to a local machine
# aws s3 cp s3://gsbdwhdata/ahremenko_ma/flights/new_flights.csv C:\Download\ --endpoint-url=https://storage.yandexcloud.net --profile=dbdwh


from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

@dag(
    dag_id="ahremenko_ma_flights_copy",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Ахременко МА. Копирование файла перелетов за указанный месяц",
    tags=["dag_01"]
)
def load_flights_dag():

    @task()
    def copy_flight_file():
        # Параметры
        cur_month = Variable.get("ahremenko_ma_cur_month")
        your_name = 'ahremenko_ma'

        source_bucket = "db01-content"
        source_key = f"flights/T_ONTIME_REPORTING-{cur_month}.csv"

        destination_bucket = "gsbdwhdata"
        destination_key = f"{your_name}/flights/new_flights.csv"

        print(f"Файл {source_bucket}/{source_key}")

        # Получаем S3 Hook с конфигурацией из подключения object_storage_yc
        s3_hook = S3Hook(aws_conn_id="object_storage_yc")

        print(f"Файл connection")

        # Считываем файл 
        file_data = s3_hook.read_key(key=source_key, bucket_name=source_bucket)

        print(f"Файл read")

        # Перезаписываем файл в целевой бакет
        s3_hook.load_string(
            string_data=file_data,
            key=destination_key,
            bucket_name=destination_bucket,
            replace=True,
        )

        print("Файл flights загружен")

    copy_flight_file()


load_flights_dag_instance = load_flights_dag()
