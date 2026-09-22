# V2 Core Migration Plan

## Goal

Revive the smarter search/filtering API from `develop` and port the newer
training-type functionality from `master` onto it.

Work only on:

```text
integrate/v2-core-master-features
```

This branch starts at `develop` commit `58aa76c`. Treat `master` as a read-only
reference, not as the implementation target.

## Current State

- The main checkout is on `integrate/v2-core-master-features`.
- The branch builds against `org.ccjmne.orca:orca-jooq-codegen:2.7.0` and the
  definition-based schema.
- The smarter v2 `QueryParams`, `RecordsCollator`, filtering, sorting,
  pagination, and quick search remain in place.
- PostgreSQL is running locally as `orca-db-test` on `127.0.0.1:5433`.
- The normal API service is running against restored `orca_test` data at
  `127.0.0.1:8080` with demo mode disabled.
- The disposable `orca_demo` database can be recreated with
  `./docker/init-demo-db.sh` and run through the Compose `demo` profile.
- `orca_test` and `orca_demo` contain the required `unaccent` and `pg_trgm`
  extensions.
- See `AGENTS.md` for database restore, build, Docker, and safety instructions.

## Agreed Scope

Implement the coherent core migration first:

1. Effective-dated training-type definitions.
2. Presence-only session behavior.
3. Certificate validity extension.
4. Certified training definitions.
5. Definition-aware session validation.
6. Historically correct certificate statistics.
7. Definition-aware session-type reads and quick search.
8. Deterministic demo data and focused manual verification.

Do not mix in secondary features yet:

- S3 dual-stack
- General dependency modernization

## Migration Checkpoint

### Completed

- The bootstrap milestone is complete.
- Session creation, type/date updates, completion, and deprecated bulk import
  now resolve the definition effective on the session date.
- Presence-only definitions reject `FLUNKED` outcomes.
- Type/date updates revalidate existing trainee outcomes.
- Deprecated bulk import now uses generated session IDs for related records.
- Definition create/update/delete routes validate flags, certificates, dates,
  and protect definitions selected by existing sessions.
- Session-type history reads return ordered definitions with certificates.
- PostgreSQL infinite definition dates serialize as `-infinity` or `infinity`.
- Site and site-group employee counts now use the update snapshot relevant to
  the requested date instead of summing historical assignments.
- Quick session search accepts frontend-provided month/year ranges and filters
  session dates accordingly.
- Quick session search selects certificate text from the definition effective
  on each session's own date.
- Demo data includes a dated renewal definition and deterministic sessions
  immediately before, on, and after its inclusive transition date.
- Authentication decodes Base64 credentials as UTF-8.
- Annual reports reuse the historical global site-group resource and expose
  year-end snapshot suggestions.
- Employee resources expose birth name and birthplace fields through the full
  generated employee record projection.

Relevant commits, oldest first:

```text
2333630 Bootstrap API against definition schema
261ce95 Make session writes definition-aware
7389a86 Add effective-dated definition administration
b8496cf Scope site employee counts to the requested date
1598522 Filter quick session search by date range
c012316 Search sessions with their historical definitions
1791859 Add deterministic demo definition transition
```

The companion frontend parses full dates, years, numeric months, and French
month names into session-search ranges in `orca-ui-2` commit `3ad650b`.

### Verified Manually

- The WAR packages successfully with `mvn -T 1 -DskipTests package`.
- Normal mode starts against `orca_test`; client, authenticated resources, and
  quick-search requests return successful responses.
- Presence-only completion rejects `FLUNKED` and accepts `MISSING`.
- Changing a completed session to a presence-only definition rejects and rolls
  back incompatible existing outcomes.
- Definition history, CRUD, baseline retention, and historical-use protection
  work against disposable demo data.
- Historical quick search uses the certificate set effective on each session's
  date.
- Site and global site-group employee counts match the relevant update snapshot.
- Demo transition sessions resolve the baseline before the transition and the
  new definition on and after the transition date.
- Validity-extension statistics work end to end against isolated demo data:
  renewal before expiry extends from the prior expiry, renewal after expiry
  restarts from the renewal date, duration `0` remains indefinitely valid, and
  voiding caps expiry and changes status on the voiding date.
- `statistics-over-time` preserves the computed expiry and the existing
  `status`, `expiry`, and `void_since` response fields across those validity
  transitions.
- `statistics-over-time` selects each session's historical definition across an
  inclusive transition: the session before the transition retains its old
  certificate and duration, sessions on and after it use the new set, and a
  later unused definition does not appear retroactively. The `status`, `expiry`,
  and `void_since` response shape remains unchanged.
- Demo data includes isolated presence-only and certificate-less definitions.
  Their deterministic completed sessions use `MISSING` and `VALIDATED`
  respectively, both definitions remain visible with empty certificate arrays,
  and both sessions are searchable by type name.
- The companion frontend exposes ordered definition history and definition
  create/update/delete controls from session-type administration, while keeping
  type-name editing separate from effective-dated flags and certificates.
- ASCII and non-ASCII credentials authenticate successfully through the normal
  API service.
- Annual reports return current and historical population/statistics snapshots,
  suggested dates, and reject accounts without global site access.
- Employee birth name, birth city, and birth country match the restored database
  values in resource responses.

### Remaining Work

No core migration work remains in the agreed scope. Frontend changes still need
their normal build and browser verification before release.

There is no existing Java test suite, and adding one is not part of the current
scope. Use disposable demo data plus repeatable SQL/API checks for verification.

## Data Model

The target schema is already present in the restored database:

```text
trainingtypes
  trty_pk
  trty_name
  trty_order

trainingtypes_defs
  ttdf_pk
  ttdf_trty_fk
  ttdf_effective_from
  ttdf_presenceonly
  ttdf_extendvalidity
  ttdf_certified

trainingtypes_certificates
  ttce_ttdf_fk
  ttce_cert_fk
  ttce_duration
```

The effective definition is the newest definition for a type where:

```text
ttdf_effective_from <= relevant date
```

Historical sessions must use the definition effective on their own
`trng_date`, not the current definition or report date.

## Implementation Order

### 1. Generate Compatible jOOQ Classes

Use sibling repository:

```text
../orca-jooq-codegen
```

Its `master` branch is version `2.7.0` and matches the restored schema. Make its
hardcoded database port configurable in an isolated worktree as documented in
`AGENTS.md`, then run:

```bash
mvn -T 1 -Ddb_name=orca_test -Ddb_port=5433 install
```

Update this branch's `pom.xml` from codegen `2.4.0` to `2.7.0`. Do not copy
unrelated dependency versions from `master`.

Expected generated fields include:

```text
Tables.TRAININGTYPES_DEFS
TRAININGTYPES_DEFS.TTDF_*
TRAININGTYPES_CERTIFICATES.TTCE_TTDF_FK
```

The old `TTCE_TRTY_FK` field will no longer exist, exposing all required source
changes at compile time.

### 2. Add Effective-Definition Query Helpers

Add shared helpers in:

```text
src/main/java/org/ccjmne/orca/api/utils/Fields.java
```

Support:

- Selecting one effective definition for `(training type, date)`.
- Selecting one effective definition per type for an arbitrary date.
- An inclusive transition date.
- A controlled error when no definition exists.

Avoid duplicating correlated definition-selection SQL across endpoints.

### 3. Port Session-Type Reads

Update:

```text
src/main/java/org/ccjmne/orca/api/rest/fetch/SubResourcesEndpoint.java
```

Keep the v2 contract:

```text
GET /sub-resources/session-types
```

It should return each type with the definition effective on `QueryParams.DATE`
(today by default), including definition fields and certificates.

Add explicit history:

```text
GET /sub-resources/session-types/{session-type}/definitions
```

Return an ordered array rather than a date-keyed map. Keep certificate-less
definitions visible. Certificate admin hints should use only definitions
effective on the requested date.

### 4. Port Session-Type Administration

Update:

```text
src/main/java/org/ccjmne/orca/api/rest/admin/CertificatesEndpoint.java
```

Preserve the v2 route family under `/certificates/session-types`.

Creating a type must atomically create its baseline definition and certificate
links. Add explicit definition create/update/delete routes.

Validate:

- Definition belongs to the route's type.
- Effective dates are unique per type.
- Certificate IDs exist.
- Durations are non-negative.
- A definition cannot be both presence-only and certified.
- Deleting a definition must not silently change historical session behavior.

Do not copy `master`'s broad update that converts every historical `FLUNKED`
outcome for a type.

### 5. Make Session Writes Definition-Aware

Update:

```text
src/main/java/org/ccjmne/orca/api/rest/edit/SessionsEndpoint.java
```

Apply effective-definition validation to:

- Session creation
- Type/date updates
- Session completion
- Deprecated bulk import

Completed normal sessions allow:

```text
VALIDATED, MISSING, FLUNKED
```

Completed presence-only sessions allow:

```text
VALIDATED, MISSING
```

Changing a session's type or date must revalidate existing outcomes.

### 6. Port Historical Expiry Statistics

Update:

```text
src/main/java/org/ccjmne/orca/api/inject/core/StatisticsSelection.java
```

For each session:

1. Select the definition effective on `TRAININGS.TRNG_DATE`.
2. Join certificates through `TTCE_TTDF_FK`.
3. Keep report cutoff date separate from definition-selection date.
4. Replace `max(session date + duration)` with the database `expiryAgg`
   aggregate ordered by session date.

Preserve existing output fields so these consumers remain compatible:

```text
src/main/java/org/ccjmne/orca/api/rest/fetch/ResourcesEndpoint.java
src/main/java/org/ccjmne/orca/api/rest/fetch/StatisticsOverTimeEndpoint.java
```

### 7. Fix Session Quick Search

Update:

```text
src/main/java/org/ccjmne/orca/api/rest/utils/QuickSearchEndpoint.java
```

Certificate text used to search a session must come from the definition
effective on that session's date. The `session-date` query parameter is only a
proximity reference and must not choose the definition.

Keep types with no certificates searchable by type name.

### 8. Update Demo Data

Update:

```text
src/main/java/org/ccjmne/orca/api/demo/DemoCommonResources.java
src/main/java/org/ccjmne/orca/api/demo/DemoDataTrainings.java
```

Create baseline `-infinity` definitions before certificate links. Add at least
one second definition and sessions before, on, and after its transition date.

Cover presence-only, validity extension, duration `0`, and a certificate-less
definition. Do not run demo reset against the restored database because it can
replace data and perform S3 operations.

### 9. Handle Definition Date Serialization

Update `CustomObjectMapper` only as needed so definition dates serialize as:

```text
infinity
-infinity
YYYY-MM-DD
```

This is required because baseline definitions use `-infinity`.

## Files With Direct Old-Schema References

Search for all remaining old links:

```bash
rg 'TTCE_TRTY_FK|TRTY_PRESENCEONLY|TRTY_EXTENDVALIDITY' src/main/java
```

Known affected files include:

```text
src/main/java/org/ccjmne/orca/api/rest/admin/CertificatesEndpoint.java
src/main/java/org/ccjmne/orca/api/inject/core/StatisticsSelection.java
src/main/java/org/ccjmne/orca/api/rest/fetch/SubResourcesEndpoint.java
src/main/java/org/ccjmne/orca/api/rest/utils/QuickSearchEndpoint.java
src/main/java/org/ccjmne/orca/api/demo/DemoCommonResources.java
```

Indirectly affected:

```text
src/main/java/org/ccjmne/orca/api/rest/edit/SessionsEndpoint.java
src/main/java/org/ccjmne/orca/api/rest/fetch/ResourcesEndpoint.java
src/main/java/org/ccjmne/orca/api/rest/fetch/StatisticsOverTimeEndpoint.java
src/main/java/org/ccjmne/orca/api/utils/Fields.java
src/main/java/org/ccjmne/orca/api/utils/CustomObjectMapper.java
```

## Verification

Build conservatively:

```bash
mvn -T 1 -DskipTests package
```

Minimum behavior checks:

- Before a transition uses the old definition.
- The transition day uses the new definition.
- Future definitions do not affect today.
- Historical sessions retain historical certificate sets and durations.
- Presence-only sessions reject `FLUNKED`.
- Duration `0` yields infinite validity.
- Renewing before expiry extends validity when configured.
- Renewing after expiry starts from the new session date.
- Voiding caps expiry.
- Statistics-over-time remains historically correct.
- Quick search uses each session's historical certificates.
- Existing v2 response shapes remain compatible.

After the build succeeds, recreate `orca-api-test` using `AGENTS.md`, then smoke
test:

```bash
curl --silent --show-error --output /dev/null \
  --write-out 'client endpoint: HTTP %{http_code}\n' \
  http://127.0.0.1:8080/api/client/
```

Only test smart employee filtering after the v2 API is confirmed running.

## Suggested Commits

```text
1. Update generated schema dependency
2. Add effective-definition query helpers
3. Port session-type reads and history
4. Port definition administration
5. Enforce definition-aware session outcomes
6. Port historical certificate statistics
7. Make session quick search definition-aware
8. Update demo data and verification scenarios
```

Do not commit the database dump, production configuration, generated temporary
worktrees, or local credentials.
