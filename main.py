import os
from tech_data_generator import main as gen_main
import psycopg2


def print_compress_ratio(records):
    for record in records:
        print(record[0])
        print(f"Size before compression: {record[1]}")
        print(f"Size after compression: {record[2]}")
        if record[1].split(" ")[1] == "kB":
            origin_size_in_kb = int(record[1].split(" ")[0])
        elif record[1].split(" ")[1] == "MB":
            origin_size_in_kb = int(record[1].split(" ")[0]) * 1024
        elif record[1].split(" ")[1] == "GB":
            origin_size_in_kb = int(record[1].split(" ")[0]) * 1024 * 1024
        
        if record[2].split(" ")[1] == "kB":
            compressed_size_in_kb = int(record[2].split(" ")[0])
        elif record[2].split(" ")[1] == "MB":
            compressed_size_in_kb = int(record[2].split(" ")[0]) * 1024
        elif record[2].split(" ")[1] == "GB":
            compressed_size_in_kb = int(record[2].split(" ")[0]) * 1024 * 1024
        
        print(f"Compression ratio: {origin_size_in_kb / compressed_size_in_kb}")
        print()


def main():
    os.system('clear' if os.name == 'nt' else 'cls')
    while True:
        print("""
            Главное меню:
            1. Генерация данных.
            2. Создать базу данных, таблицы и включить сжатие таблиц.
            3. Загрузить данные в TimescaleDB.
            4. Сжать таблицу с загруженными данными.
            5. Показать результаты.
            6. Выход.
        """)
        answer = input()
        
        if answer == "1":
            count = input("Введите количество сигналов для генерации:")
            gen_main(int(count))
            continue
        elif answer == "2":
            os.system("psql -U postgres < create_db.sql")
            os.system("psql -h localhost -U postgres -d tech_data < create_tables.sql")
            continue
        elif answer == "3":
            os.system("upload_datas.bat")
            continue
        elif answer == "4":
            os.system("psql -h localhost -U postgres -d tech_data < compress_datas.sql")
            continue
        elif answer == "5":
            conn = psycopg2.connect(dbname='tech_data', user='postgres',
                        password='password', host='localhost')
            cursor = conn.cursor()
            query = 'SELECT hypertable_name, uncompressed_total_bytes, compressed_total_bytes FROM "timescaledb_information"."compressed_hypertable_stats";'
            cursor.execute(query)
            records = cursor.fetchall()
            cursor.close()
            conn.close()
            print_compress_ratio(records)
            exit()
        elif answer == "6":
            exit()


if __name__ == "__main__":
    main()
