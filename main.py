import gzip
import csv


CSV_GZ_FILE_LIST = (
    "c:/Users/guppi/PycharmProjects/mds24/md4_database/project/airport_weather/KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "c:/Users/guppi/PycharmProjects/mds24/md4_database/project/airport_weather/KFSM.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "c:/Users/guppi/PycharmProjects/mds24/md4_database/project/airport_weather/KNYL.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "c:/Users/guppi/PycharmProjects/mds24/md4_database/project/airport_weather/KXNA.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
)


CSV_FILE = (
    "c:/Users/guppi/PycharmProjects/mds24/md4_database/project/airport_weather/KFLG.csv",
    "c:/Users/guppi/PycharmProjects/mds24/md4_database/project/airport_weather/KFSM.csv",
    "c:/Users/guppi/PycharmProjects/mds24/md4_database/project/airport_weather/KNYL.csv",
    "c:/Users/guppi/PycharmProjects/mds24/md4_database/project/airport_weather/KXNA.csv",
)



def extract_gz_to_csv(gz_filename: str, output_filename: str = None):
    with gzip.open(gz_filename, 'r') as gzfile:
        gzfile_lines = gzfile.readlines()

    with open(output_filename, 'w', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        for line in gzfile_lines[6:9]:
            decoded_line = (
                line
                .decode("utf-8")
                .strip()
                .replace('"', '')
            )[:-1]
            line_list = decoded_line.split(';')
            writer.writerow(line_list)


extract_gz_to_csv(CSV_GZ_FILE_LIST[0], CSV_FILE[0])
