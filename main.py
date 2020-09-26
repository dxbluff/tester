import os

os.environ["postgres"]="password"


def main():
    
    print("""
    Choose option:
        1. Generate data.
        2. Create database, tables and enable compression.
        3. Upload datas to TimescaleDB.
        4. Compress tables with uploaded data.
        5. Show results.
    """)
    
    answer = input()

    if answer == "1":
        os.system("python tech_data_generator.py")
    elif answer== "2":
        os.system("psql -U postgres < create_db.sql")
        os.system("psql -h localhost -U postgres -d tech_data < create_tables.sql")
    elif answer == "3":
        os.system("upload_datas.bat")
    elif answer == "4":
        os.system("psql -h localhost -U postgres -d tech_data < compress_datas.sql")
    elif answer == "5":
        os.system('psql -h localhost -U postgres -d tech_data -c "SELECT hypertable_name, uncompressed_total_bytes, compressed_total_bytes FROM "timescaledb_information"."compressed_hypertable_stats"";')

if __name__ == "__main__":
	main()
