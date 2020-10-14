import os

import psycopg2

conn = psycopg2.connect(dbname='tech_data', user='postgres',
                        password='password', host='localhost')
cursor = conn.cursor()


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
            Choose option:
            1. Generate data.
            2. Create database, tables and enable compression.
            3. Upload datas to TimescaleDB.
            4. Compress tables with uploaded data.
            5. Show results.
            6. Exit.
        """)
        answer = input()
        
        if answer == "1":
            count = input("Input quantity of signals to generate:")
            os.system(f"python tech_data_generator.py {count}")
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
            query = 'SELECT hypertable_name, uncompressed_total_bytes, compressed_total_bytes FROM "timescaledb_information"."compressed_hypertable_stats";'
            cursor.execute(query)
            records = cursor.fetchall()
            print_compress_ratio(records)
            exit()
        elif answer == "6":
            cursor.close()
            conn.close()
            exit()


if __name__ == "__main__":
    main()
