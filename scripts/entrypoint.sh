#!/bin/bash

# Exit on subcommand errors
set -Eeuo pipefail

# List of extensions to possibly install (if a version variable is set)
declare -A extensions=(
    [pg_bm25]=${PG_BM25_VERSION:-}
    [pgml]=${PGML_VERSION:-}
    [vector]=${PGVECTOR_VERSION:-}
    [pg_search]=${PG_SEARCH_VERSION:-}
    [pgnodemx]=${PGNODEMX_VERSION:-}
    [pg_cron]=${PG_CRON_VERSION:-}
    [pg_net]=${PG_NET_VERSION:-}
    [pg_ivm]=${PG_IVM_VERSION:-}
    [pg_graphql]=${PG_GRAPHQL_VERSION:-}
    [pg_hashids]=${PG_HASHIDS_VERSION:-}
    [pg_jsonschema]=${PG_JSONSCHEMA_VERSION:-}
    [pg_repack]=${PG_REPACK_VERSION:-}
    [pg_stat_monitor]=${PG_STAT_MONITOR_VERSION:-}
    [pg_hint_plan]=${PG_HINT_PLAN_VERSION:-}
    [pgfaceting]=${PGFACETING_VERSION:-}
    [pgtap]=${PGTAP_VERSION:-}
    [pgaudit]=${PGAUDIT_VERSION:-}
    [postgis]=${POSTGIS_VERSION:-}
    [pgrouting]=${PGROUTING_VERSION:-}
    [roaringbitmap]=${PG_ROARINGBITMAP_VERSION:-}
    [http]=${PGSQL_HTTP_VERSION:-}
    [hypopg]=${HYPOPG_VERSION:-}
    [rum]=${RUM_VERSION:-}
    [age]=${AGE_VERSION:-}
)

# List of extensions that must be added to shared_preload_libraries
declare -A preload_names=(
    [pgml]=pgml
    [pgnodemx]=pgnodemx
    [pg_cron]=pg_cron
    [pg_net]=pg_net
    [pgaudit]=pgaudit
)

# Build the shared_preload_libraries list, only including extensions that are installed
# and have a preload name specified
for extension in "${!extensions[@]}"; do
    version=${extensions[$extension]}
    if [ -n "$version" ] && [[ -n "${preload_names[$extension]:-}" ]]; then
        preload_name=${preload_names[$extension]}
        shared_preload_list+="${preload_name},"
    fi
done
# Remove the trailing comma
shared_preload_list=${shared_preload_list%,}

# Update the PostgreSQL configuration
echo "pg_net.database_name = '$POSTGRES_DB'" >> "${PGDATA}/postgresql.conf"
echo "cron.database_name = '$POSTGRES_DB'" >> "${PGDATA}/postgresql.conf"
sed -i "s/^#shared_preload_libraries = .*/shared_preload_libraries = '$shared_preload_list'  # (change requires restart)/" "${PGDATA}/postgresql.conf"

# Setup users
ROOT_ROLE_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_roles WHERE rolname='root'")
POSTGRES_ROLE_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_roles WHERE rolname='postgres'")

if [ -z "$ROOT_ROLE_EXISTS" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER root;
	CREATE DATABASE root;
	GRANT ALL PRIVILEGES ON DATABASE root TO root;
EOSQL
fi

if [ -z "$POSTGRES_ROLE_EXISTS" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE ROLE postgres WITH SUPERUSER CREATEDB CREATEROLE LOGIN;
EOSQL
fi

# We need to restart the server for the changes above
# to be reflected
pg_ctl restart

echo "PostgreSQL is up - installing extensions..."

# Preinstall extensions for which a version is specified
for extension in "${!extensions[@]}"; do
    version=${extensions[$extension]}
    if [ -n "$version" ]; then
        PGPASSWORD=$POSTGRES_PASSWORD psql -c "CREATE EXTENSION IF NOT EXISTS $extension CASCADE" -d "$POSTGRES_DB" -U "$POSTGRES_USER" || echo "Failed to install extension $extension"
    fi
done

echo "PostgreSQL extensions installed - initialization completed!"
echo "ParadeDB is ready for connections!"
