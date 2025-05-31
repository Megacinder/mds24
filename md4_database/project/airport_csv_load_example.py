from airflow.decorators import dag, task
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


from csv import writer as csv_writer
from datetime import datetime
from gzip import open as gzip_open
from logging import getLogger, Filter, WARNING
from os import path, unlink, environ
from tempfile import NamedTemporaryFile

# скопировать файлы с погодой в S3 можно так:
# aws s3 cp c:\download\KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz s3://gsbdwhdata/superteam/
# только поменять "superteam" на название вашей папки

# Верхним регистром обозначаются константные переменные - которые задаются человеком руками
ETL_PARAM = dict(
    root_file_path="superteam",  # это папка в AWS, где наши файлы с погодой - например, тут указано s3://gsbdwhdata/superteam/
    dag_id="ahremenko_ma_team_3_etl_dag_csv",
    dag_tag="team_3",
    pg_conn_id="con_dwh_2024_s004",
    pg_db_name="dwh_2024_s004",
)

# тут указываем все файлы в погодой, которые вы скачали из инета, и которые уже скопированы в S3
WEATHER_CSV_GZ_FILE_LIST = {  # т
    "KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KFSM.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KNYL.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KXNA.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
}
WEATHER_CSV_GZ_FILE_DICT = {
    i[:4]: f"{ETL_PARAM["root_file_path"]}/{i}"
    for i
    in WEATHER_CSV_GZ_FILE_LIST
}


# Этот метод парсит gz_csv (это сжатый бинарник csv), приводит строки в читаемый вид и отдаёт эти строки в виде массива
def get_lines_from_gzip(gz_filename: str, icao_code: str, start_line: int = 6, stop_line: int = None) -> list:
    o_list = []
    with gzip_open(gz_filename, 'r') as gzfile:
        for line in gzfile:
            decoded_line = (
               line
               .decode("utf-8")
               .strip()
               .replace('"', '')
               .replace("'", '_')
           )[:-1]
            o_list.append([icao_code] + decoded_line.split(';'))
    # o_list = list(set(o_list))  # deduplication
    if stop_line and start_line > stop_line:
        stop_line = None
    o_list = o_list[start_line:stop_line]
    return o_list


@dag(
    dag_id=f"{ETL_PARAM["dag_id"]}",
    schedule=None,
    start_date=datetime(2025, 5, 1),
    catchup=False,
    description=f"supadupadag",
    tags=[f"{ETL_PARAM["dag_tag"]}"],
    max_active_tasks=5,
)
def dag1() -> None:
    start = EmptyOperator(task_id="start")
    stop = EmptyOperator(task_id="stop")

    # создаём таблицу - куда мы будет складывать данные о погоде
    @task.python(do_xcom_push=False)
    def create_ods_tables() -> (XComArg | None):
        sql = """
        create schema if not exists ods
        ;

        --drop table if exists ods.weather
        --;
        create table if not exists ods.weather (
             icao_code   text
            ,local_time  text
            ,raw_t       text
            ,raw_p0      text
            ,raw_p       text
            ,raw_u       text
            ,raw_dd      text
            ,raw_ff      text
            ,raw_ff10    text
            ,raw_ww      text
            ,raw_w_w_    text
            ,raw_c       text
            ,raw_vv      text
            ,raw_td      text
        )
        ;
        """

        with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()

    # складываем в таблицу данные из gzip
    @task.python(do_xcom_push=True)
    def copy_from_gzip_to_ods_weather() -> (XComArg | None):
        for icao_code, csv_gz_file in WEATHER_CSV_GZ_FILE_DICT.items():
            s3_hook = S3Hook(aws_conn_id="object_storage_yc")
            file_path = s3_hook.download_file(
                key=csv_gz_file,
                bucket_name="gsbdwhdata",
            )

            # получаем нам массив строк
            rows = get_lines_from_gzip(file_path, icao_code, start_line=7)

            # хотя тут всё же мы складываем данные в CSV (а я говорил, что нет:)), но временный CSV
            # это потребовалось из-за ограничения Postgres на количество коннектов:
            #  складывать сразу массив строк - это складывать по одной строку в таблицу,
            #  а каждая строка - это новый коннект - попытка подключения - к Постгресу
            #  и база ругается "не могу соединиться"
            with NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as tmp:
                csv_writer_ex = csv_writer(tmp)
                csv_writer_ex.writerows(rows)
                tmp_path = tmp.name

            # кладём данные из нашего временного CSV в таблицу в базе
            with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
                with open(tmp_path, 'r') as file:
                    cur = conn.cursor()
                    ods_weather_table = "ods.weather"
                    cur.execute(f"truncate table {ods_weather_table}")
                    sql_pg_copy_csv = f"copy {ods_weather_table} from stdin with csv delimiter as ',' quote '\"'"
                    cur.copy_expert(sql_pg_copy_csv, file)
                    conn.commit()

            # удаляем временный CSV-файл (самое главное - удаляем руками и не автоматом, потому что автоматически
            #  файл мог удалиться ещё до завершения загрузки данных в таблицу)
            if tmp_path and path.exists(tmp_path):
                unlink(tmp_path)


    (  # а это сам даг - его можно писать в таком стиле (в столбик, но выделив скобками в начале и конце)
        start
        >> create_ods_tables()
        >> copy_from_gzip_to_ods_weather()
        >> stop
    )

# вызов дага - что в Airflow он был именно дагом, а не просто питонячим файлом
dag = dag1()


# эта штука иногда полезна, если хочется запустить текущий файл руками, и тогда будет выполняться только код ниже -
#  даги, таски и всё, что выше, выполняться не будут)
if __name__ == "__main__":
    for k, v in WEATHER_CSV_GZ_FILE_DICT.items():
        print(k, ':', v)
