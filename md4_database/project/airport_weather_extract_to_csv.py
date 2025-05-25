from gzip import open as gzip_open
from csv import writer as csv_writer


CSV_GZ_FILE_LIST = {
    "KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    # "KFSM.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    # "KNYL.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    # "KXNA.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
}
ROOT_PATH = "airport_weather"
CSV_GZ_FILE_DICT = {
    i[:4]: f"{ROOT_PATH}/{i}"
    for i
    in CSV_GZ_FILE_LIST
}


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
    if stop_line and start_line > stop_line:
        stop_line = None
    o_list = o_list[start_line:stop_line]
    return o_list


for icao_code, csv_gz_file in CSV_GZ_FILE_DICT.items():
    file_path = csv_gz_file

    rows = get_lines_from_gzip(file_path, icao_code, start_line=6)

    print(len(rows))
    # for row in rows:
    #     print(row)
