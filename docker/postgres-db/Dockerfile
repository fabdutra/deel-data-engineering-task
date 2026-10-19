FROM postgres:15-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-15-cron \
    && rm -rf /var/lib/apt/lists/*
COPY --chmod=0644 db-scripts/initialize_db_ddl.sql /docker-entrypoint-initdb.d/10-source.sql
COPY --chmod=0755 db-scripts/20-cdc.sh /docker-entrypoint-initdb.d/20-cdc.sh
