#!/bin/bash
set -ev

cd "$(dirname "$0")"

psql -U postgres < setup.sql

sudo cp pg_hba.conf $(psql -U postgres -c "SHOW hba_file" -At)

DATA_DIR=$(psql -U postgres -c "SHOW data_directory" -At)
PG_PID=$(sudo head -n1 $DATA_DIR/postmaster.pid)
sudo kill -SIGHUP $PG_PID
