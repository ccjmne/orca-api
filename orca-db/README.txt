Current DB version: 16 (pg_dump must be at least that recent)

Get the *schema* dump with:

    pg_dump \
        --host orca-november-2023.c43s9fxu21fi.eu-west-1.rds.amazonaws.com --port 5432 \
        --username orcadb_root --dbname peidf \
        --no-owner --clean --if-exists --schema-only \
        --file peidf-schema-$(date -I).sql

Restore with:

    psql \
        --host orca-november-2023.c43s9fxu21fi.eu-west-1.rds.amazonaws.com --port 5432 \
        --username orcadb_root --dbname peidf \
        --file peidf-schema-$(date -I).sql

Apply patch with:

    psql \
        --host orca-november-2023.c43s9fxu21fi.eu-west-1.rds.amazonaws.com --port 5432 \
        --username orcadb_root --dbname peidf \
        --file patches/x.x.x.sql

vim: textwidth=80 expandtab shiftwidth=4 smarttab
