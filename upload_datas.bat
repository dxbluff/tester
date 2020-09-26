set PGPASSWORD=password
psql -h localhost -d tech_data -U postgres -c "\COPY events FROM data\events_log.csv CSV ENCODING 'UTF8' NULL AS 'NULL'"
psql -h localhost -d tech_data -U postgres -c "\COPY boolean_values FROM data\bool_data.csv CSV ENCODING 'UTF8'"
psql -h localhost -d tech_data -U postgres -c "\COPY float_values FROM data\float_data.csv CSV ENCODING 'UTF8'"
psql -h localhost -d tech_data -U postgres -c "\COPY int_values FROM data\int_data.csv CSV ENCODING 'UTF8'"
psql -h localhost -d tech_data -U postgres -c "\COPY string_values FROM data\string_data.csv CSV ENCODING 'UTF8'"
