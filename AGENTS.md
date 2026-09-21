# Local Development Notes

## Communication

- KISS: explain things simply and briefly. Lead with the result or next action.
- Prefer an ELI5/TL;DR answer. Add detail only when asked or when needed for
  safety.
- Do not narrate routine steps or repeat information.

## Safety

- Treat database dumps as sensitive local files. Do not commit them.
- Do not use `orca.conf` or `rsp-server-config.json` to launch a local server.
  They may contain stale production connection details or cloud credentials.
- Keep demo mode disabled when using a restored database. Enabling `-Ddemo=true`
  causes the application to replace database contents and may perform S3
  operations.
- Port `5432` may be occupied by an SSH tunnel. The local PostgreSQL container
  therefore binds to `127.0.0.1:5433`.
- Bind services to `127.0.0.1`, not all interfaces.
- Use one Maven build thread (`-T 1`) to avoid consuming excessive resources.

## Local Services

The established local environment uses:

| Service | Container | Host address | Docker network |
| --- | --- | --- | --- |
| PostgreSQL 16 | `orca-db-test` | `127.0.0.1:5433` | `orca-test-network` |
| Tomcat 9 / API | `orca-api-test` | `127.0.0.1:8080` | `orca-test-network` |

Local database connection:

```text
Database: orca_test
Username: postgres
Password: postgres
```

These are local test credentials only.

## Routine Start And Stop

Build the WAR and repository-owned Tomcat image:

```bash
mvn -T 1 -DskipTests package
docker compose --profile normal build
```

Start the existing database, then run the API against restored `orca_test` data:

```bash
docker start orca-db-test
docker compose --profile normal up --detach api
```

Stop the API without stopping PostgreSQL:

```bash
docker compose --profile normal down
```

Follow server logs:

```bash
docker compose --profile normal logs --follow api
```

Connect to PostgreSQL from the host:

```bash
PGPASSWORD=postgres psql \
  --host 127.0.0.1 \
  --port 5433 \
  --username postgres \
  --dbname orca_test
```

The API base URL is:

```text
http://127.0.0.1:8080/api/
```

Safe smoke checks:

```bash
curl --silent --show-error --output /dev/null \
  --write-out 'client endpoint: HTTP %{http_code}\n' \
  http://127.0.0.1:8080/api/client/

curl --silent --show-error --output /dev/null \
  --write-out 'auth endpoint: HTTP %{http_code}\n' \
  --request POST http://127.0.0.1:8080/api/auth/
```

Expected statuses are `200` for the client endpoint and `401` for an
unauthenticated authentication request.

## Isolated Demo Mode

Never enable demo mode against `orca_test`: startup truncates and replaces all
database contents. Use a separate disposable `orca_demo` database. Local demo
resets skip S3 cleanup by default; set `-Ddemo-s3-cleanup=true` only when cloud
cleanup is explicitly intended and correctly configured.

Initialize a fresh disposable demo database from the restored database's schema,
including PostgreSQL's required search extensions:

```bash
./docker/init-demo-db.sh
```

Then run the API in demo mode:

```bash
docker compose --profile demo up --detach --build api-demo
```

Stop it with:

```bash
docker compose --profile demo down
```

The built-in demo administrator credentials are:

```text
Username: root
Password: pwd
```

The WAR configures permissive CORS responses itself. Tomcat deployment does
not need a separate global CORS filter.

## One-Time Database Setup

The current local dump is a PostgreSQL custom-format archive created by
`pg_dump` 18 from PostgreSQL 16. It is named:

```text
dump-peidf-202609211410.sql
```

Despite the `.sql` extension, restore it with `pg_restore`, not directly with
`psql`.

Create an isolated PostgreSQL container and persistent volume:

```bash
docker pull postgres:16
docker volume create orca-db-test-data
docker run --detach \
  --name orca-db-test \
  --publish 127.0.0.1:5433:5432 \
  --env POSTGRES_USER=postgres \
  --env POSTGRES_PASSWORD=postgres \
  --env POSTGRES_DB=orca_test \
  --volume orca-db-test-data:/var/lib/postgresql/data \
  postgres:16
```

Wait until it is ready:

```bash
until docker exec orca-db-test \
  pg_isready --username postgres --dbname orca_test >/dev/null 2>&1
do
  sleep 1
done
```

`pg_dump` 18 emits `SET transaction_timeout`, which PostgreSQL 16 does not
recognize. Restore by rendering the archive as SQL and removing only that
setting:

```bash
set -o pipefail
pg_restore --no-owner --no-privileges --file=- \
  dump-peidf-202609211410.sql \
  | perl -ne 'print unless /^SET transaction_timeout = 0;$/' \
  | PGPASSWORD=postgres psql \
      --host 127.0.0.1 \
      --port 5433 \
      --username postgres \
      --dbname orca_test \
      --set ON_ERROR_STOP=1
```

To replace an existing local database before restoring:

```bash
docker exec orca-db-test dropdb --username postgres --if-exists orca_test
docker exec orca-db-test createdb --username postgres orca_test
```

Then run the restore pipeline above.

Basic post-restore checks:

```bash
PGPASSWORD=postgres psql \
  --host 127.0.0.1 \
  --port 5433 \
  --username postgres \
  --dbname orca_test \
  --no-psqlrc \
  --command="
    SELECT count(*) AS public_tables
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';

    SELECT count(*) AS type_definitions FROM trainingtypes_defs;
    SELECT count(*) AS unmapped_certificate_links
    FROM trainingtypes_certificates
    WHERE ttce_ttdf_fk IS NULL;
  "
```

## jOOQ Code Generation

`orca-api` depends on:

```text
org.ccjmne.orca:orca-jooq-codegen:2.7.0
```

That artifact may not be available from the configured Maven repositories. Its
sibling source repository is expected at:

```text
../orca-jooq-codegen
```

The generator's current `pom.xml` hardcodes port `5432`. Do not run it unchanged
while that port points through an SSH tunnel. Generate against the local
database by making `db_port` configurable in an isolated worktree:

```xml
<properties>
  ...
  <db_name>orca</db_name>
  <db_port>5432</db_port>
</properties>
```

and change its JDBC URL to:

```xml
<url>jdbc:postgresql://localhost:${db_port}/${db_name}</url>
```

Then install the generated artifact locally:

```bash
mvn -T 1 -Ddb_name=orca_test -Ddb_port=5433 install
```

Do not commit that port adjustment unless intentionally improving the sibling
project. The generated artifact must match the schema being used by the API.

## Build The API

After installing the codegen artifact:

```bash
mvn -T 1 -DskipTests package
```

The output is:

```text
target/api.war
```

## One-Time API Setup

The Compose services use the existing private Docker network. Create it and
attach PostgreSQL if that was not already done:

```bash
docker network create orca-test-network
docker network connect orca-test-network orca-db-test
```

Inside the Docker network, PostgreSQL listens on its normal port `5432`; port
`5433` is only the host-side mapping.

## Redeploy After A Build

Rebuild the WAR and recreate whichever API service is active:

```bash
mvn -T 1 -DskipTests package
docker compose --profile demo up --detach --build --force-recreate api-demo
```

## Cleanup

Remove the API container:

```bash
docker rm --force orca-api-test
```

Remove the database container while retaining its data:

```bash
docker rm --force orca-db-test
```

Permanently remove the restored local database:

```bash
docker volume rm orca-db-test-data
```

Remove the private network after its containers have been removed:

```bash
docker network rm orca-test-network
```
