#!/bin/bash
set -e

cd "$(dirname "$0")"

psql -U postgres < setup.sql

sudo cp pg_hba.conf $(psql -U postgres -c "SHOW hba_file" -At)

DATA_DIR=$(psql -U postgres -c "SHOW data_directory" -At)
CONFIG_FILE=$(psql -U postgres -c "SHOW config_file" -At)
sudo install -m 0600 -o postgres server.crt $DATA_DIR
sudo install -m 0600 -o postgres server.key $DATA_DIR
sudo bash -c "echo ssl_cert_file = \\'server.crt\\' >> $CONFIG_FILE"
sudo bash -c "echo ssl_key_file = \\'server.key\\' >> $CONFIG_FILE"

sudo service postgresql stop
sudo service postgresql start 9.4
