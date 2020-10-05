FROM postgres:12

COPY sql_setup.sh /docker-entrypoint-initdb.d/
RUN apt-get update && apt-get install postgresql-12-wal2json
