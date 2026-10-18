#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=cdc_password="$CDC_PASSWORD" <<'SQL'
CREATE USER cdc_user REPLICATION LOGIN PASSWORD :'cdc_password';
GRANT CONNECT ON DATABASE finance_db TO cdc_user;
GRANT USAGE ON SCHEMA operations TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA operations TO cdc_user;
CREATE PUBLICATION cdc_publication FOR ALL TABLES;
SQL
