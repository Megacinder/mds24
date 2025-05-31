from csv import writer as csv_writer
from datetime import datetime
from gzip import open as gzip_open

# скопировать файлы с погодой в S3 можно так:
# aws s3 cp c:\download\KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz s3://gsbdwhdata/superteam/
# только поменять "superteam" на название вашей папки

# Верхним регистром обозначаются константные переменные - которые задаются человеком руками
ETL_PARAM = dict(
    root_file_path="c:/download",  # это папка в AWS, где наши файлы с погодой - например, тут указано s3://gsbdwhdata/superteam/
    dag_id="ahremenko_ma_team_3_etl_dag_csv",
    dag_tag="team_3",
    pg_conn_id="con_dwh_2024_s004",
    pg_db_name="dwh_2024_s004",
)

# тут указываем все файлы в погодой, которые вы скачали из инета, и которые уже скопированы в S3
WEATHER_CSV_GZ_FILE_LIST = {  # т
    # "KGFK.01.01.2024.01.07.2024.1.0.0.en.utf8.00000000.csv.gz",
    "KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    # "KFSM.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    # "KNYL.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    # "KXNA.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
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


rows = []
for icao_code, csv_gz_file in WEATHER_CSV_GZ_FILE_DICT.items():
    file_path = csv_gz_file
    # получаем нам массив строк
    rows = get_lines_from_gzip(file_path, icao_code, start_line=7)



if __name__ == "__main__":
    with open("c:/download/b.csv", mode='w+') as csv_file:
        csv_writer = csv_writer(csv_file)
        csv_writer.writerows(rows)
