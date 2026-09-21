#!/bin/sh
set -eu

container="${ORCA_DB_CONTAINER:-orca-db-test}"
source_db="${ORCA_DEMO_SOURCE_DB:-orca_test}"
demo_db="${ORCA_DEMO_DB:-orca_demo}"

if [ "$demo_db" = "$source_db" ]; then
  printf '%s\n' "Refusing to replace source database '$source_db'." >&2
  exit 1
fi

docker exec "$container" dropdb --username postgres --if-exists "$demo_db"
docker exec "$container" createdb --username postgres "$demo_db"
docker exec "$container" pg_dump \
  --username postgres \
  --dbname "$source_db" \
  --schema-only \
  --no-owner \
  --no-privileges \
  | docker exec --interactive "$container" psql \
      --username postgres \
      --dbname "$demo_db" \
      --set ON_ERROR_STOP=1

docker exec "$container" psql \
  --username postgres \
  --dbname "$demo_db" \
  --set ON_ERROR_STOP=1 \
  --command='CREATE EXTENSION IF NOT EXISTS unaccent;' \
  --command='CREATE EXTENSION IF NOT EXISTS pg_trgm;'
