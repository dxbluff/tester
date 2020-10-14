import json
import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from tech_data_generator import main as gen_main

PARENT_DIR = Path(__file__).parent.absolute()
with (PARENT_DIR / 'timescale_config.json').open(mode='r', encoding='utf-8') as fp:
    CONFIG = json.load(fp)


@contextmanager
def connect(host, user, password, port=5432, database=None):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()


def execute(credentials, path=None, query=None):
    if path is not None:
        with (PARENT_DIR / path).open(mode='r', encoding='utf-8') as fp:
            query = fp.read()
    with connect(**credentials) as cursor:
        cursor.execute(query)
        if cursor.description is not None:
            return cursor.fetchall()


def print_compress_ratio(records):
    for record in records:
        print(record[0])
        print(f"Размер до сжатия: {record[1]}")
        print(f"Размер после сжатия: {record[2]}")
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
        
        print(f"Коэффициент сжатия: {origin_size_in_kb / compressed_size_in_kb}")
        print()


def main():
    while True:
        
        menu = """
            TimescaleDB compression ratio tester
            Главное меню:
            1. Генерация данных.
            2. Создать базу данных, таблицы и включить сжатие таблиц.
            3. Загрузить данные в TimescaleDB.
            4. Сжать таблицу с загруженными данными.
            5. Показать результаты.
            6. Выход.
        """
        
        print(menu)
        
        answer = input()
        
        if answer == "1":
            count = input("Введите количество сигналов для генерации:")
            gen_main(int(count))
            continue
        elif answer == "2":
            try:
                execute(dict(CONFIG, database=None), path='create_db.sql')
            except:
                pass
            
            execute(CONFIG, path='create_tables.sql')
            
            continue
        elif answer == "3":
            with connect(**CONFIG) as cursor:
                with (PARENT_DIR / 'data' / 'events_log.csv').open(mode='r', encoding='utf-8') as fp:
                    cursor.copy_from(fp, 'events', sep=',', null='NULL')
                
                for table_name in ('bool', 'float', 'int', 'string'):
                    with (PARENT_DIR / 'data' / f'{table_name}_data.csv').open(mode='r', encoding='utf-8') as fp:
                        cursor.copy_from(fp, f'{table_name}_values', sep=',')
            
            continue
        elif answer == "4":
            execute(CONFIG, path='compress_datas.sql')
            continue
        elif answer == "5":
            query = 'SELECT hypertable_name, uncompressed_total_bytes, compressed_total_bytes ' \
                    'FROM "timescaledb_information"."compressed_hypertable_stats";'
            records = execute(CONFIG, query=query)
            print_compress_ratio(records)
            exit()
        elif answer == "6":
            exit()


if __name__ == "__main__":
    main()
