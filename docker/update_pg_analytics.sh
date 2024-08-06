#!/bin/bash

# Upgrades ParadeDB pg_analytics to the specified version. Assumes the new version of the
# extension is already downloaded and installed on the system.

# Exit on subcommand errors
set -Eeuo pipefail

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

PARADEDB_VERSION="${PARADEDB_VERSION%%+*}"

# Update ParadeDB pg_analytics into both template_database and $POSTGRES_DB
for DB in template_paradedb "$POSTGRES_DB" "${@}"; do
  echo "Updating ParadeDB pg_analytics '$DB' to $PARADEDB_VERSION"
  psql -d "$DB" -c "
    -- Upgrade ParadeDB extensions
    -- pg_analytics
    CREATE EXTENSION IF NOT EXISTS pg_analytics VERSION '$PARADEDB_VERSION';
    ALTER EXTENSION pg_analytics UPDATE TO '$PARADEDB_VERSION';
  "
done
