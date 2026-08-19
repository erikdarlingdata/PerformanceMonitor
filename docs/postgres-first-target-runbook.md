# Runbook: point Darling at a PostgreSQL target

End to end, with a **proof point at every step** — what to look at, and what it should say. The
[README's PostgreSQL Targets section](../Darling/README.md#postgresql-targets) is the reference for *what*
each collector does; this is the ordered procedure for getting one collecting and knowing that it is.

> **Status.** This is the procedure derived from the code, not a transcript of a completed run.
>
> What IS proven: every collector's generated SQL and every MCP reader's SQL has been executed against live
> Aurora PostgreSQL 16.11 and 17.7, confirming they run, return the expected shape, carry no timezone on a
> naive column, and compute their windowed differences correctly.
>
> What is NOT: **the service has never been pointed at a PostgreSQL target.** Nothing has done connect probe
> → collector dispatch → COPY into a store → read back out. Log text below is quoted from the source that
> emits it, not from a session. The first person to follow this runbook is validating that layer, and should
> expect to find things — the last defect found this way was one where every collector's SQL was correct and
> the feature still could not work. Correct this file from what you actually see.

---

## 0. What you need first

| | | Check |
|---|---|---|
| A target | Aurora PostgreSQL, or self-managed PostgreSQL 13+ (`pg_replication_slots.wal_status`) | `SELECT version();` |
| A login on it | `pg_monitor`, password auth | step 1 |
| A store | Managed (bundled) or your own PostgreSQL | README [The Store](../Darling/README.md#the-store) |
| The service | This branch's build | `dotnet build` |
| Network | The service host must reach the target's port 5432 | `nc -z <host> 5432` |

On Aurora, "the target" is one **instance**, not the cluster. A writer and a reader are two entries with
two identities, because they have two sets of statistics — see step 3 for what changes between them.

## 1. Grant the login on the target

Read-only. Nothing is created and no setting changes, unlike a SQL Server target with its Extended Events
sessions.

```sql
CREATE ROLE darling_monitor WITH LOGIN PASSWORD '<password>';
GRANT pg_monitor TO darling_monitor;
```

On Aurora/RDS run the `GRANT` as an `rds_superuser`. No superuser is needed for the monitoring login.

**Proof:** connect as the login and confirm it can see *other* backends, which is the whole point of
`pg_monitor` — without it the statistics views quietly narrow to the connecting user's own sessions and
fleet monitoring becomes self-monitoring with no error anywhere.

```sql
SELECT count(*) AS visible_backends,
       count(*) FILTER (WHERE query = '<insufficient privilege>') AS hidden
FROM pg_stat_activity;
```

`hidden` must be 0. If it equals `visible_backends - 1`, the grant did not take.

**Optional, for `pg_statement_stats` only** (the per-query-shape collector, Aurora-gated):

```sql
SELECT count(*) FROM pg_extension WHERE extname = 'pg_stat_statements';
```

If that returns 0 you need `pg_stat_statements` in `shared_preload_libraries` — a parameter-group change
plus a reboot on Aurora/RDS — and then `CREATE EXTENSION pg_stat_statements;` in the database Darling
connects to. One installation covers the whole instance; the extension is keyed by `dbid` and tracks all
databases. Skipping this is fine: the collector records a non-fatal skip that says exactly this (step 9),
and the other six are unaffected.

## 2. Register the target

**Which path you use depends on whether this store has ever been seeded**, and getting this wrong is the
most likely way to conclude the feature is broken when it is not. `darling.json` seeds
`config.config_monitored_servers` **once**, when that table is empty. After that the registry is
authoritative and a darling.json edit adds nothing.

**Fresh store — edit `darling.json`:**

```json
{
  "name": "aurora-orders-writer",
  "engine": "postgres",
  "host": "orders.cluster-abc123.us-east-1.rds.amazonaws.com",
  "auth": "sql",
  "username": "darling_monitor",
  "encryptedPassword": "<output of --encrypt-password>",
  "trustServerCertificate": true
}
```

**Existing install — use `add_servers` through MCP**, which is the designed path and takes effect within one
collection sweep, no restart:

```json
[{"host": "orders.cluster-abc123.us-east-1.rds.amazonaws.com",
  "engine": "postgres", "auth": "SQL",
  "username": "darling_monitor", "password": "...",
  "trust_server_certificate": true}]
```

It probes before saving, so a bad credential or an unreachable host comes back as `connection_failed`
without leaving a broken row behind — and an added PostgreSQL target's `detail` reports the same
writer/reader, Aurora, and collectors-apply facts step 3 describes.

Check which situation you are in before assuming:

```sql
SELECT count(*) AS registered FROM config.config_monitored_servers;
```

Three things go wrong here and each has a specific symptom:

- **`auth` must be `"sql"`.** There is no integrated-auth path for PostgreSQL. An entry asking for it
  fails config validation with a message saying so, rather than failing later at connect.
- **`trustServerCertificate: true` is usually needed on Aurora.** The default is full certificate
  verification (`SslMode=VerifyFull`) and Aurora presents an RDS CA a stock trust store does not know.
  Without it, connect fails on certificate validation — which reads like a network problem and is not.
- **A target with TLS switched off entirely needs `"encryptMode": "optional"`.** `trustServerCertificate`
  relaxes *verification*, not the requirement — so a stock self-hosted PostgreSQL running `ssl = off`, which is
  the normal shape for a local or lab instance, is **unreachable** until `encryptMode` is `optional`
  (`SslMode=Prefer`). Found the hard way on the first real run of this runbook: fail-closed TLS is correct, and
  the failure gives no hint that the fix is a different setting than the one you already tried.
- **`name` is the storage identity.** It derives `server_id`, so renaming an existing entry orphans its
  history under the old id rather than moving it.
- **`engine` typos behave differently by path, on purpose.** In `darling.json` an unrecognized engine
  resolves to SQL Server rather than throwing, so one bad line cannot stop the service from monitoring
  everything else. `add_servers` refuses it — onboarding is one deliberate act, and `"postgress"` silently
  becoming a SQL Server target produces a connection failure against 5432 with nothing naming the cause.

The password slot takes `env:NAME` and `file:/path` references as well as a DPAPI blob; on non-Windows
hosts use those, since DPAPI is Windows-only.

## 3. Pre-flight — the step that tells you what to expect

```
PerformanceMonitor.Darling.Service.exe --test-connection
```

**Proof:** a `[PASS]` line that reports PostgreSQL facts, ending in how many collectors will actually run.

```
  [PASS] aurora-orders-writer: PostgreSQL 17 (server_version_num 170007), writer, Aurora — all 8 PostgreSQL collectors apply
```

**Read the count.** It is computed by asking the same gate the collector runner asks, so it is the real
answer, and it is the difference between "this is configured" and "this will collect":

| Target | Applies | Skipped, and why |
|---|---|---|
| Aurora writer | 7 of 7 | — |
| Aurora reader | 6 of 7 | `pg_autovacuum_stats` — `pg_stat_user_tables` reports all zeros on a standby |
| Self-managed 16+ writer | 5 of 7 | `pg_wait_stats`, `pg_statement_stats` — both read Aurora-only functions |
| Self-managed 15 reader | 3 of 7 | the above, plus `pg_io_stats` (needs `pg_stat_io`, PostgreSQL 16+) |

If the count is lower than the table says it should be, the probe disagrees with you about the target —
check `writer`/`reader` and `Aurora`/`not Aurora` in the same line before touching anything else.

A `[FAIL]` carries the driver's own error. The three common ones:

| Error | Cause |
|---|---|
| `28P01: password authentication failed` | wrong password, or the secret reference did not resolve |
| certificate / SSL validation failure | `trustServerCertificate` not set on Aurora (step 2) |
| `Timeout ... connection attempt` | security group, VPN route, or wrong endpoint — not credentials |

The verb exits 0 only when the config is valid **and** every server is reachable, so it works as a
deployment gate. One caveat inherited from the SQL Server path: it connects as *you*, the console user.
For a PostgreSQL target that is the same credential the service will use (it comes from the config, not
the ambient identity), so the caveat that matters for integrated auth does not apply here.

## 4. Know what the first start does to the store

Adding the first PostgreSQL target does not need a store change of its own, but **starting this build
against an existing store migrates it** — the PostgreSQL collector tables arrive as migration rungs
V61–V67, and the registry's engine/port columns as V68, all applied automatically on start. Forward-only; there is no down-migration.

**Before starting, if your store is unmanaged and has TimescaleDB**, re-derive the background-worker
settings. Every collector table becomes a hypertable, so seven new tables move the required numbers, and
undersizing does not error — it silently stops compression and retention from running. See
[Background workers](../Darling/README.md#background-workers-sizing-an-unmanaged-store-and-what-happens-if-you-dont);
today the numbers are 51 and 62, and both need a server restart. Managed mode does this itself.

## 5. First start, in console mode

```
Darling\PerformanceMonitor.Darling.Service\bin\Release\net10.0\PerformanceMonitor.Darling.Service.exe
```

There is no `--console` flag — the bare executable **is** console mode, and the Windows-service lifetime is a
no-op when there is no service host. Worth stating because looking for the flag and not finding it reads like
a missing feature.

**Proof — three lines, in order.** The store migrated (the applied count is however many rungs this store
was behind — on a fresh store it is all of them):

```
Postgres store ready (schema v68, 8 migration(s) applied)
```

The target was probed as PostgreSQL:

```
Connected to PostgreSQL target 'aurora-orders-writer': major 17 (server_version_num 170007), writer, Aurora: True — PostgreSQL 17.7 ...
```

If this line says `writer` for something you believe is a reader, or `Aurora: False` for an Aurora
instance, stop — every gate downstream keys off these and the collection you get will not be what you
expect.

Then per-collector lines with row counts:

```
  [aurora-orders-writer] pg_wait_stats => 47 rows (sql:31ms, pg:9ms)
  [aurora-orders-writer] pg_xmin_horizon => 4 rows (sql:12ms, pg:4ms)
```

The per-database collector names its database, one line per database per cycle:

```
  [aurora-orders-writer] pg_autovacuum_stats [orders] => 19 rows (sql:88ms, pg:6ms)
```

## 6. Minute 2 — prove rows are landing

Against the **store**, not the target:

```sql
SELECT collector_name,
       count(*)                                   AS runs,
       max(collection_time)                        AS latest,
       sum(rows_collected)                         AS rows_total,
       count(*) FILTER (WHERE status <> 'SUCCESS') AS non_success,
       max(error_message)                          AS last_error
FROM collect.collection_log
WHERE server_id = (SELECT server_id FROM collect.servers WHERE server_name = 'aurora-orders-writer')
  AND collector_name LIKE 'pg\_%'
GROUP BY collector_name
ORDER BY collector_name;
```

Every collector the pre-flight count promised should appear with `non_success = 0`. `status` is one of
exactly five values — `SUCCESS`, `PERMISSIONS`, `ERROR`, `SESSION_MISSING`, `YIELDED` — and step 9 covers
what a non-`SUCCESS` one means.

A collector that is **absent entirely** was gated off, not failing: cross-check it against the skipped
list from step 3.

## 7. When each collector's data becomes useful

Two different waits, and conflating them is the most likely way to mistake a working install for a broken
one. A row exists after the first cycle. A **reader** — the MCP tool — needs two samples before it can
difference a cumulative counter, so the first read after startup legitimately shows zero activity.

| Collector | Cadence | First row | First meaningful read |
|---|---|---|---|
| `pg_wait_stats` | 1 min | 1 min | 2 min |
| `pg_statement_stats` | 1 min | 1 min | 2 min |
| `pg_xmin_horizon` | 1 min | 1 min | 1 min (levels, not counters) |
| `pg_replication_slots` | 1 min | 1 min | 2 min (growth needs two) |
| `pg_io_stats` | 1 min | 1 min | 2 min |
| `pg_wraparound_stats` | 5 min | 5 min | 5 min (levels) |
| `pg_autovacuum_stats` | 60 min | **60 min** | 2 h (growing/flat needs two) |

`pg_autovacuum_stats` is the one that surprises people: an hour before the first row, two before
"dead tuples growing" can be answered. That cadence is deliberate — a PostgreSQL connection is bound to
one database for life, so per-database collection costs one connection per database per cycle.

## 8. Read it

Through MCP, one tool per collector:

| Tool | Answers |
|---|---|
| `get_pg_wait_stats` | what the instance waits on (Aurora only) |
| `get_pg_top_queries` | top query shapes by total time (Aurora only) |
| `get_pg_wraparound_risk` | XID and MultiXact freeze headroom — how close to a write outage |
| `get_pg_xmin_horizon` | *why* vacuum is reclaiming nothing, attributed to the specific holder |
| `get_pg_replication_slots` | slot health, and whether retained WAL is still growing |
| `get_pg_autovacuum_health` | tables ranked by how far past their **own** trigger threshold |
| `get_pg_io_stats` | I/O by (backend type, object, context) — who, what, and why |

**Proof, and the trap:** on a healthy target most of these are *supposed* to be boring. Do not read
"nothing alarming" as "not collecting" — check `collection_log` (step 6) for that. Distinguish:

- **Rows, all classified `ok`** — collecting, and the target is healthy. Success.
- **No rows, collector ran with `rows_collected = 0`** — the target genuinely has none of that thing. No
  replication slots is the common one, and it is good news.
- **No rows, collector never ran** — gated off (step 3) or failing (step 9).

Two results that look like bugs and are not:

- `get_pg_io_stats` on Aurora reports **write counters not tracked**. Correct: Aurora backends do not
  write data files, the storage layer does, so those columns are NULL — which is why the tool reports
  trackedness instead of letting a NULL read as a zero.
- `get_pg_replication_slots` empty **on a reader** is per-instance, not a cluster all-clear. Slots live on
  the writer. Same for autovacuum state.

## 9. Alerting

The three outage predictors alert; the other four collectors are read-only signals.

- Evaluated on the **30-second** alert sweep, after the shared SQL Server sweep, gated on the probed
  engine.
- Reads only data collected in the last **2 hours**. A stale target alerts on nothing — which is what the
  separate collection-stopped self-alert is for.
- Delivered through the same deliverer, history and mute rules as every SQL Server alert.
- Thresholds derive from the target's own settings — wraparound grades against *that cluster's*
  `autovacuum_freeze_max_age`, not a constant — and are **not yet configurable**. See
  [`postgres-alerting-design-note.md`](postgres-alerting-design-note.md).

**Proof on a healthy target is silence**, which is unfalsifiable, so verify the path rather than the
outcome: confirm the collectors backing it have fresh rows (step 6 — `pg_wraparound_stats`,
`pg_xmin_horizon`, `pg_replication_slots`), and that a *SQL Server* alert has delivered through the same
deliverer at some point. Nothing here fires on a healthy cluster, and that is the design: a predictor
that cries wolf gets muted, and a muted outage predictor is worse than none.

## 10. Failure modes

A collector that fails is classified rather than logged as `ERROR` forever. The store's non-fatal bucket
is named `PERMISSIONS`, and most things landing in it are **not** missing grants — the `error_message`
says which kind it is.

| `status` | `error_message` says | Actually means | Fix |
|---|---|---|---|
| `PERMISSIONS` | a missing grant | it is one | `GRANT pg_monitor` (step 1) |
| `PERMISSIONS` | "NOT a missing grant", view/function absent | `pg_stat_statements` never created | step 1's optional half |
| `PERMISSIONS` | "NOT a missing grant", not implemented | reading something this engine lacks | nothing — expected off Aurora |
| `PERMISSIONS` | "NOT a missing grant", feature disabled | switched off in the parameter group | enable it, or accept the gap |
| `ERROR` | a statement timeout | the query was too slow **once** | usually transient; deliberately does *not* drop the connection, so a slow query cannot cause a reconnect storm |
| `ERROR` | anything else | unclassified | read `error_message`; this is the bucket that wants a bug report |
| `YIELDED` | lock contention | the collector stepped aside | none today — no PostgreSQL collector opts into the lock-timeout yield |

Connection-level failures (the `08` class, `57P0x`) force a reconnect and a re-probe, so a failover
re-probes the target — which is how a promoted reader stops being gated as a standby.

**Symptoms with no error anywhere**, the ones that cost the most time:

| Symptom | Cause |
|---|---|
| The connect line says SQL Server, or the error mentions `SqlException` / a TDS handshake against 5432 | the target lost its `engine` on the way through the registry. Requires store schema **v68+**: `SELECT name, engine, port FROM config.config_monitored_servers;` — if `engine` is not a column, this build predates the fix and no darling.json edit will help |
| Autovacuum health reports everything fine on a cluster you know is behind | you are reading a **reader**. It reports all zeros, not an error. Measured: writer 13,654,458 dead tuples, reader 0, same cluster and tables |
| Added a target to darling.json and nothing happened | the store was already seeded; use `add_servers` (step 2) |
| `pg_wait_stats` and `pg_top_queries` empty, everything else fine | not Aurora. Core PostgreSQL has no cumulative wait counters at all |
| Only some databases in `pg_autovacuum_stats` | by design: `datallowconn` and non-template only, minus your `excludedDatabases` |
| Store stopped compressing after adding targets | background workers (step 4). Silent — check the postmaster log for "out of background workers" |

## 11. Removing a target

`remove_server` through MCP, or the Viewer's Manage Servers dialog — **not** by deleting the darling.json
entry, which the registry ignores (step 2). Collection stops within a sweep; the collected history stays
under its `server_id` and ages out on the normal retention horizons. Nothing was ever created on the
monitored instance, so there is nothing to clean up there — dropping `darling_monitor` is optional and
unrelated to Darling's state.

## 12. What this does not cover, because it does not exist yet

Do not go looking for these:

- **Plan capture.** No PostgreSQL equivalent in the store.
- **Blocking chains.** Designed, not built —
  [`postgres-blocking-design-note.md`](postgres-blocking-design-note.md).
- **Scheduled analysis.** Still SQL-Server-shaped; a PostgreSQL target produces no analysis findings.
- **Configurable alert thresholds.** Constants today (step 9).
- **The Viewer.** Collection lands in the shared store and reads through MCP; the WPF surfaces are still
  SQL-Server-shaped.
