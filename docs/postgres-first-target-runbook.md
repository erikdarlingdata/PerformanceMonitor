# Runbook: point Darling at a PostgreSQL target

End to end, with a **proof point at every step** — what to look at, and what it should say. The
[README's PostgreSQL Targets section](../Darling/README.md#postgresql-targets) is the reference for *what*
each collector does; this is the ordered procedure for getting one collecting and knowing that it is.

> **Status.** This started as the procedure derived from the code, written before the service had ever
> been pointed at a PostgreSQL target. That sentence is now false in the direction you want: the service
> has since run PostgreSQL targets end to end — connect probe → collector dispatch → COPY into a store →
> read back out — on live fleets, self-hosted and Aurora/RDS both, and several steps below carry
> corrections only a real run could have found: the TLS trap in step 2 came off the first real run of
> this runbook, the statement-stats growth cap came off a 50-cluster fleet's first 36 hours
> (`PgStatementStatsCollector`'s own remarks tell that story), and the `pgstattuple` failure mode step 7
> describes was observed on the `postgres` maintenance database of the first production target.
>
> Log text below is still quoted from the source that emits it, and every count is re-derived from
> `CollectorCatalog` and the store's migration ladder rather than remembered. The instruction stands:
> if what you see disagrees with this file, trust the target, and correct this file from what you
> actually see.

---

## 0. What you need first

| | | Check |
|---|---|---|
| A target | Aurora PostgreSQL, or self-managed PostgreSQL 13+ (`pg_replication_slots.wal_status`) | `SELECT version();` |
| A login on it | `pg_monitor`, password auth | step 1 |
| A store | Managed (bundled) or your own PostgreSQL | README [The Store](../Darling/README.md#the-store) |
| The service | A current build | `dotnet build` |
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

**Optional, for the five extension-backed collectors.** The biggest one first: `pg_statement_stats`,
the per-query-shape collector, reads `pg_stat_statements` — on Aurora it reads
`aurora_stat_statements()`, the same view plus Aurora-only columns, and on everything else the vanilla
view with those columns honestly NULL (#2625). Check for it:

```sql
SELECT count(*) FROM pg_extension WHERE extname = 'pg_stat_statements';
```

If that returns 0 you need `pg_stat_statements` in `shared_preload_libraries` — a parameter-group change
plus a reboot on Aurora/RDS — and then `CREATE EXTENSION pg_stat_statements;` in the database Darling
connects to. One installation covers the whole instance; the extension is keyed by `dbid` and tracks all
databases. The other four, from the collectors' own declared dependencies: `pg_buffer_usage`
(`pg_buffercache` — a plain `CREATE EXTENSION`, no restart), `pg_kernel_stats` (`pg_stat_kcache`, which
sits on top of `pg_stat_statements`), `pg_predicate_stats` (`pg_qualstats` — preloaded, and additionally
`CREATE EXTENSION` per database), and `pg_wait_sampling` (the `pg_wait_sampling` module — preload-only,
so check it with `SHOW shared_preload_libraries;`, not `pg_extension`). Skipping any of them is fine:
the collector records a non-fatal skip naming exactly which install is missing (step 10), and the other
26 collectors are unaffected.

### IAM, for the two collectors that read the server log (plan capture, deadlocks)

This is a **different axis from the grant above** — it authorizes the **monitoring host's AWS identity**,
not the PostgreSQL login. On Aurora/RDS there is no local log directory a SQL session can read with
`pg_read_file()`; plan capture and deadlock detection instead pull the log tail through the RDS control
plane. Attach this to the instance role/profile the Darling service actually runs as:

```json
{
  "Effect": "Allow",
  "Action": [
    "rds:DescribeDBClusters",
    "rds:DescribeDBLogFiles",
    "rds:DownloadDBLogFilePortion"
  ],
  "Resource": [
    "arn:aws:rds:<region>:<account-id>:cluster:*",
    "arn:aws:rds:<region>:<account-id>:db:*"
  ]
}
```

`DescribeDBClusters` resolves an Aurora cluster to its current writer instance; the other two list and read
the log file itself. Self-managed PostgreSQL doesn't need this at all — it reads `pg_read_file()` directly,
and the DB-level grant above is the only one it wants. Skipping this on Aurora/RDS is not silent: both
collectors log the missing action by name (see step 10).

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
  [PASS] aurora-orders-writer: PostgreSQL 17 (server_version_num 170007), writer, Aurora — all 27 PostgreSQL collectors apply
```

**Read the count.** It is computed by asking the same gate the collector runner asks, so it is the real
answer, and it is the difference between "this is configured" and "this will collect". A gated-off
collector is named in the line itself — `23 of 27 PostgreSQL collectors apply (skipped: ...)` — and the
reasons come from the collectors' own gates in `CollectorCatalog`:

| Target | Applies | Skipped, and why |
|---|---|---|
| Aurora writer, PG 16+ | 27 of 27 | — |
| Aurora reader | 23 of 27 | `pg_autovacuum_stats`, `pg_index_usage_stats`, `pg_table_bloat_stats`, `pg_index_bloat` — all four are writer-only: a standby's per-table statistics are either zeros or its own, and both readings are wrong for the cluster |
| Self-managed 16+ writer | 25 of 27 | `pg_wait_stats`, `pg_cpu_utilization` — one reads `aurora_stat_system_waits()`, the other AWS Performance Insights; both gate on Aurora detection |
| Self-managed 15 reader | 20 of 27 | all of the above, plus `pg_io_stats` (needs `pg_stat_io`, PostgreSQL 16+) |

A PostgreSQL 13 target additionally skips `pg_write_stats`, whose `pg_stat_wal` source is 14+.

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
against an existing store migrates it** — applied automatically on start, forward-only, no
down-migration. The first PostgreSQL collector tables arrived as rungs V63–V69 and the registry's
engine/port columns as V70, and the ladder has kept climbing with the collectors since (V71 blocking
edges, V83–V95, plan capture at v99, ...): a current build carries any older store to **v114**.
`StorageVersion.SchemaVersion` is the source of truth, and step 5's proof line quotes whatever it says
at your build.

**Before starting, if your store is unmanaged and has TimescaleDB**, re-derive the background-worker
settings. Every collector table becomes a hypertable — all 69 of them, 27 PostgreSQL — so the required
numbers move whenever collectors are added, and undersizing does not error — it silently stops
compression and retention from running. See
[Background workers](../Darling/README.md#background-workers-sizing-an-unmanaged-store-and-what-happens-if-you-dont);
today the numbers are 72 and 83 for 70 hypertables, and both need a server restart. Managed mode does this itself.

## 5. First start, in console mode

```
Darling\PerformanceMonitor.Darling.Service\bin\Release\net10.0\PerformanceMonitor.Darling.Service.exe
```

There is no `--console` flag — the bare executable **is** console mode, and the Windows-service lifetime is a
no-op when there is no service host. Worth stating because looking for the flag and not finding it reads like
a missing feature.

**Proof — two lines, in order.** The store migrated (the applied count is however many rungs this store
was behind — on a fresh store it is all of them):

```
Postgres store ready (schema v114, 9 migration(s) applied)
```

The target was probed as PostgreSQL:

```
Connected to PostgreSQL target 'aurora-orders-writer': major 17 (server_version_num 170007), writer, Aurora: True — PostgreSQL 17.7 ...
```

If this line says `writer` for something you believe is a reader, or `Aurora: False` for an Aurora
instance, stop — every gate downstream keys off these and the collection you get will not be what you
expect.

The per-collector row-count lines exist too, but they are **`Debug`-level since #3102**, so a default
console shows the two lines above and not these. Raise the service's own namespace
(`Logging__LogLevel__PerformanceMonitor.Darling.Service=Debug`, documented in the README) to see them —
or skip straight to step 6, which proves the same thing from `collection_log` at any log level:

```
  [aurora-orders-writer] pg_wait_stats => 47 rows (sql:31ms, pg:9ms)
  [aurora-orders-writer] pg_xmin_horizon => 4 rows (sql:12ms, pg:4ms)
```

The per-database collectors name their database, one line per database per cycle:

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

Within a couple of minutes, every **per-minute** collector the pre-flight count promised should appear
with `non_success = 0`. The hourly and daily ones arrive on their own cadences — step 7's table says
when each is due, so an absence there is a schedule, not a failure, until its first interval has passed.
`status` is one of exactly six values — `SUCCESS`, `PERMISSIONS`, `EXTENSION_MISSING`, `ERROR`,
`SESSION_MISSING`, `YIELDED` — and step 10 covers what a non-`SUCCESS` one means.

A collector that is **absent entirely** was gated off, not failing: cross-check it against the skipped
list from step 3.

## 7. When each collector's data becomes useful

Two different waits, and conflating them is the most likely way to mistake a working install for a broken
one. A row exists after the first cycle. A **reader** — the MCP tool — needs two samples before it can
difference a cumulative counter, so the first read after startup legitimately shows zero activity.

All 27, from `CollectorScheduleDefaults` — the shared table both SKUs schedule by:

| Collector | Cadence | First row | First meaningful read |
|---|---|---|---|
| `pg_wait_stats` | 1 min | 1 min | 2 min (counters need two samples) |
| `pg_statement_stats` | 1 min | 1 min | 2 min |
| `pg_xmin_horizon` | 1 min | 1 min | 1 min (levels, not counters) |
| `pg_replication_slots` | 1 min | 1 min | 2 min (growth needs two) |
| `pg_replication_stats` | 1 min | 1 min | 1 min (a sample, not a counter) |
| `pg_io_stats` | 1 min | 1 min | 2 min |
| `pg_write_stats` | 1 min | 1 min | 2 min |
| `pg_database_stats` | 1 min | 1 min | 2 min |
| `pg_blocking` | 1 min | 1 min | 1 min (a sample, not a counter) |
| `pg_session_states` | 1 min | 1 min | 1 min (a sample, not a counter) |
| `pg_lock_stats` | 1 min | 1 min | 1 min (a sample, not a counter) |
| `pg_wraparound_stats` | 5 min | 5 min | 5 min (levels) |
| `pg_deadlocks` | 5 min | 5 min | the first deadlock reported — an event log, not a counter |
| `pg_cpu_utilization` | 5 min | 5 min | 5 min (Performance Insights backfills the 1-minute points) |
| `pg_autovacuum_stats` | 60 min | **60 min** | 2 h (growing/flat needs two) |
| `pg_table_bloat_stats` | 60 min | **60 min** | 2 h (growing/flat needs two) |
| `pg_server_config` | 60 min | 60 min | 2 h for the *changes* read (a change needs two snapshots) |
| `pg_plan_capture_readiness` | 60 min | 60 min | 60 min (facets are levels) |
| `pg_plan_capture` | 60 min | **60 min** | the first plan `auto_explain` logs past its threshold |
| `pg_wait_sampling` | 60 min | 60 min | 2 h (counters) |
| `pg_kernel_stats` | 60 min | 60 min | 2 h (counters) |
| `pg_predicate_stats` | 60 min | 60 min | 2 h (counters) |
| `pg_buffer_usage` | 60 min | 60 min | 60 min (residency is a level) |
| `pg_index_usage_stats` | 24 h | **24 h** | 48 h (a scan count is a difference) |
| `pg_index_bloat` | 24 h | **24 h** | 24 h, complete (see below) |
| `pg_column_stats` | 24 h | **24 h** | 24 h (levels; "it moved" needs two) |
| `pg_extension_availability` | 24 h | **24 h** | 24 h (levels) |

The four extension-backed hourly ones (`pg_wait_sampling`, `pg_kernel_stats`, `pg_predicate_stats`,
`pg_plan_capture`) are hourly for the fleet's sake rather than the data's: on a managed target they can
only ever record a non-fatal skip, and a five-minute cadence would be ~900 skip rows per target per day
saying the same thing. Their counters lose nothing at the longer interval — with one exception the
schedule's own remarks spell out: `pg_plan_capture`'s self-hosted route is a fixed 4 MB log tail with
no resume marker, so a server logging faster than that window covers silently loses plans between reads,
and a self-hosted operator lowers the cadence per server rather than relying on the default.

The per-database collectors are the ones that surprise people — seven of them at the time of writing,
which is every collector whose `RunsPerDatabase` returns true, so count them from `CollectorCatalog`
rather than from this sentence. `pg_autovacuum_stats` and `pg_table_bloat_stats` take an hour before the
first row and two before "growing or flat" can be answered; `pg_index_usage_stats` takes a **day**, and
two before a windowed scan count exists at all.

`pg_index_bloat` used to be the one whose first-answer column needed reading carefully, and since
#3234 it is not. It **estimates from catalog statistics and reads no index page**, so one cycle covers
every index rather than a rotating slice: a day gets you the whole census, not a fraction of it. What
it needs instead is the `pg_read_all_data` grant below — `pg_stats` supplies the column widths the
model rests on, and without them every row reports the widths-not-visible reason rather than a number.
It no longer needs the `pgstattuple` extension at all; that is now the ON-REQUEST route to an exact
figure, and every row carries the `pgstatindex` call for its own index.

**Read the reason on a row that has no estimate, because two of them are permanent.** A never-analyzed
parent needs an `ANALYZE`; invisible column widths need the grant. But a **partial** index cannot be
modelled — the only row count available is the parent table's, and the index holds just the rows
matching its predicate — and a **deduplicated** one cannot either, because PostgreSQL 13+ stores
duplicate keys once in a posting list, so real storage is denser than per-tuple arithmetic allows and a
correct model still over-predicts. Those two are what the exact command is for, at any grant and any
statistics freshness.

It remains the **complete btree census with no size floor**, while `pg_index_usage_stats` floors at
64 kB, which is why the two report row counts differing by roughly 65% on the same target: the gap is
entirely indexes too small for usage statistics to be worth recording.

Those cadences are deliberate — a PostgreSQL connection is bound to one database for life, so per-database collection
costs one connection per database per cycle, and index usage is a structural question that an hourly
sample would re-record 24 times a day for nothing.

`pg_table_bloat_stats` shares `pg_autovacuum_stats`' hour on purpose rather than by copying: it measures
the DAMAGE whose CAUSE that one measures, and "vacuum fell behind at 14:00 and bloat grew" is only a
sentence the data can support if both are sampled on the same grain.

## 8. Read it

Through MCP — a read per collector, plus the trend, detail and config-diff readers that sit on top of
them; 33 `get_pg_*` tools in all, registered by the same service:

| Tool | Answers |
|---|---|
| `get_pg_wait_stats` | what the instance waits on (Aurora only — `get_pg_wait_sampling` covers everything else) |
| `get_pg_wait_sampling` | the sampled wait profile from the `pg_wait_sampling` extension — the stock-PostgreSQL counterpart of the above, with a waiting-vs-working split |
| `get_pg_top_queries` | top query shapes by total time, wherever `pg_stat_statements` is installed; Aurora adds the storage-vs-cache I/O split and per-statement peak memory |
| `get_pg_plans` | captured execution plans from `auto_explain`, grouped by plan shape, literals redacted before storage |
| `get_pg_plan_capture_readiness` | whether the target can capture plans at all, facet by facet in causal order, with the remedy for each unmet step |
| `get_pg_wraparound_risk` | XID and MultiXact freeze headroom — how close to a write outage |
| `get_pg_xmin_horizon` | *why* vacuum is reclaiming nothing, attributed to the specific holder |
| `get_pg_replication_slots` | slot health, and whether retained WAL is still growing |
| `get_pg_replication_stats` | the CONNECTED replicas: send/replay lag, with the window's worst beside the latest |
| `get_pg_autovacuum_health` | tables ranked by how far past their **own** trigger threshold |
| `get_pg_io_stats` | I/O by (backend type, object, context) — who, what, and why (PostgreSQL 16+) |
| `get_pg_io_trend` | one (backend type, context) pair's rates and hit ratio over time |
| `get_pg_cpu_utilization` | instance CPU from AWS Performance Insights (Aurora/RDS only) |
| `get_pg_kernel_stats` | OS-level CPU and device I/O per query shape (`pg_stat_kcache`) — the burning-vs-waiting split beside `get_pg_top_queries` |
| `get_pg_predicate_stats` | which columns queries filter on and how selectively (`pg_qualstats`) — the evidence behind an index idea |
| `get_pg_blocking` | blocking chains that were SAMPLED, with the root attributed |
| `get_pg_lock_stats` | contended lock modes and relations over time, sampled from `pg_locks` |
| `get_pg_deadlocks` | deadlocks parsed from the server log, one row per distinct deadlock |
| `get_pg_deadlock_detail` | one deadlock in full: the complete wait graph and every participant's SQL |
| `get_pg_database_stats` | temp-file spills, cache hit ratio, deadlocks, commit/rollback split |
| `get_pg_database_trend` | one database's spills, hit ratio, deadlocks and rollback share, interval by interval |
| `get_pg_index_usage` | which indexes nothing scans — **and whether each one can actually be dropped** |
| `get_pg_index_bloat` | how much of each index is reclaimable, **estimated** from statistics with its accuracy stated — read the reason on a row with no estimate, and `exact_measurement_command` when you need certainty on one index |
| `get_pg_table_bloat` | how much space the vacuum lag above has cost, as an **estimate** with its own error stated |
| `get_pg_column_stats` | the planner's own per-column inputs — read `coverage` before acting on the ranking |
| `get_pg_buffer_usage` | what is resident in shared buffers, per relation (`pg_buffercache`) |
| `get_pg_extensions` | which monitoring-relevant extensions are installed, outdated, merely available, or absent — per database |
| `get_pg_write_stats` | checkpoints (timed vs REQUESTED), background writer, WAL volume across the window |
| `get_pg_server_config` | the settings snapshot, non-defaults first, `pending_restart` reported loudly |
| `get_pg_server_config_changes` | what changed and when, old and new value side by side |
| `get_pg_session_states` | who is holding a transaction open — **and whether they actually pin the xmin horizon** |
| `get_pg_wait_trend` | one wait event as a time series |
| `get_pg_query_duration_trend` | one statement's per-execution cost over time — the regression read |

**Proof, and the trap:** on a healthy target most of these are *supposed* to be boring. Do not read
"nothing alarming" as "not collecting" — check `collection_log` (step 6) for that. Distinguish:

- **Rows, all classified `ok`** — collecting, and the target is healthy. Success.
- **No rows, collector ran with `rows_collected = 0`** — the target genuinely has none of that thing. No
  replication slots is the common one, and it is good news.
- **No rows, collector never ran** — gated off (step 3), not yet due (step 7), or failing (step 10).

Five results that look like bugs and are not:

- `get_pg_io_stats` on Aurora reports **write counters not tracked**. Correct: Aurora backends do not
  write data files, the storage layer does, so those columns are NULL — which is why the tool reports
  trackedness instead of letting a NULL read as a zero.
- `get_pg_replication_slots` empty **on a reader** is per-instance, not a cluster all-clear. Slots live on
  the writer. Same for autovacuum state, index usage and both bloat surfaces — all four are writer-only
  collectors.
- `get_pg_table_bloat` **or `get_pg_index_bloat`** reporting most of its rows with a **suppressed**
  estimate is almost always a permissions gap rather than a missing ANALYZE, and it is a step in
  this runbook that `GRANT pg_monitor` alone does not satisfy. See the note below. Index bloat joined
  this list in #3234, when it stopped walking pages and started modelling from `pg_stats`.
- `get_pg_session_states` reporting a session **idle in transaction for an hour with `peak_horizon_age`
  of `-1`** is not a contradiction and not a rounding artefact. It means the session pins nothing: a
  READ COMMITTED transaction releases its snapshot at the end of each statement, and one whose write
  matched no rows never got a transaction id to hold. Both were measured on a live PostgreSQL 16.15
  instance. Terminating such a session reclaims not one dead row, which is exactly why the tool says
  so instead of letting the duration imply otherwise.
- `get_pg_database_stats` reporting `stats_reset_count` above zero is the tool working, not a fault. The
  counters it reads are cumulative since the last `pg_stat_reset()`, so a reset zeroes them; the window
  totals become LOWER BOUNDS and the tool says so rather than letting the reset surface as a negative
  rate or a spike. A crash restart shows the same way.

### The one grant `pg_monitor` does not cover

`pg_monitor` is enough for every collector here except the THREE that read `pg_stats` — table bloat,
**index bloat since #3234**, and per-column statistics — and the way it fails is worth knowing because
it does not look like a failure. `pg_stats` is filtered by `has_column_privilege(..., 'select')`, and `pg_monitor` confers
**no** SELECT on user tables — so the monitoring role sees **zero**
rows in `pg_stats` and the estimator, fed nothing, returns confident large numbers. Measured against a
`pg_monitor`-only role on a live PostgreSQL 16 target: 88.59% reported for a table whose true bloat is
0.50%, 95.03% for one that is really 74.82%, 22.57% for one that is really 0.46%.

Darling does not publish those numbers — `estimate_unavailable` is set on every such row and the read
suppresses the figure rather than captioning it — but the result is a bloat surface that reports nothing
useful.

The `pg_column_stats` collector has the same dependency and fails more quietly still: it reads `pg_stats`
directly, so without this grant it returns **zero rows** and logs `SUCCESS`. Measured on a live Aurora
target carrying 361 tables over the collector's size floor, 107 of them analyzed: the collector ran, succeeded,
and collected nothing. An empty per-column statistics panel on a busy database means this grant is missing,
not that the planner has no statistics.

**You no longer have to know that from here.** Until #3154 that sentence was the only place the distinction
was written down, and nobody reads a runbook while looking at an empty grid: the panel and
`get_pg_column_stats` both listed the size floor and the privilege filter and selected neither. They now name
which one applies, and report it on a POPULATED result too — a login that can read four tables of twenty
produces a ranking that looks complete. `get_pg_column_stats` returns the arm as its own `coverage` field, and
every value it can take means something different to you:

| `coverage` | what it means | your move |
|---|---|---|
| `StatisticsNotVisible` | tables clear the floor, none of their statistics are readable by the monitoring login | **this grant** |
| `PartialVisibility` | some are readable, the rest are not — the ranking you are looking at is a subset | **this grant**; treat the result as partial until then |
| `BelowSizeFloor` | nothing on the server is large enough for this collector to read | nothing; there is nothing to collect |
| `FullyMeasured` | every table above the floor is represented | nothing |
| `CollectionFault` | tables clear the floor AND are readable, and nothing was stored anyway | ours — neither cause explains it, so raise it |
| `EvidenceStale` | rows exist, yet no table clears the floor in the evidence window | nothing on the grant; the rows describe tables that may have since shrunk, been truncated or been dropped |
| `Undetermined` | the two counts could not be established | nothing; see below |

The two counts behind every verdict come from `pg_table_bloat_stats`, which collects on **writers only** and
hourly, so on a read replica — where `pg_column_stats` does run — the answer is honestly `Undetermined`
rather than a guess. They are measured over a fixed 24-hour lookback while the row count is measured over
whatever window you asked for, which is why `EvidenceStale` exists and why the census states the span beside
each figure: do not draw a ratio between two numbers spanning different intervals.

The fix is one grant:

```sql
-- PostgreSQL 14 and newer
GRANT pg_read_all_data TO darling_monitor;

-- PostgreSQL 13: the role does not exist, so grant it per schema
GRANT USAGE ON SCHEMA public TO darling_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO darling_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO darling_monitor;
```

Verified on the live rig: with `pg_read_all_data` added, a `pg_monitor` role's estimates became
byte-identical to a superuser's. This is a genuine widening of what the monitoring role can read, so it is
a decision to take deliberately rather than a step to run — every other collector works without it, and a
fleet that does not want it simply gets measured sizes and dead-tuple counts from this surface instead of
an estimate, and an empty per-column statistics panel.

### The other way to do it: a helper function

`pg_read_all_data` is not the only route, and it is worth knowing the alternative exists before deciding,
because the two fail in different directions.

**Datadog takes the other route.** Its PostgreSQL setup has you create a `datadog` schema in **each**
monitored database holding a `SECURITY DEFINER` function, and grant the monitoring role EXECUTE on that
rather than SELECT on the data. A `SECURITY DEFINER` function runs with its OWNER's privileges, so a
low-privilege login can obtain one specific privileged answer without being able to read anything else.

Applied here, that would be a function returning `pg_stats` rows for a table — the monitoring role gets
column statistics and still cannot `SELECT` a single row of customer data.

| | `GRANT pg_read_all_data` | helper function |
|---|---|---|
| what it widens | SELECT on **all data**, cluster-wide | EXECUTE on one function |
| objects created in the customer's database | **none** | one schema + one function, **per database** |
| install | one statement, once | DDL in every database, repeated for every database added later |
| upgrade | nothing to upgrade | the function is versioned code that ships with the product |
| removal | `REVOKE` | `DROP`, and something has to remember it is there |
| PostgreSQL 13 | role does not exist — explicit `GRANT SELECT` per schema | works |
| who must run it | someone who can grant a role | someone who can create objects and own the definer |

**The honest summary of the trade.** The grant is one statement and no footprint, and it hands over more
than the collector needs. The helper hands over exactly what the collector needs and puts product-owned
code inside the customer's database forever — which is a support obligation, not just an install step:
it has to be versioned, upgraded in place, and removable, and a database created after onboarding silently
has no helper until something notices.

Which is why a monitoring vendor might reasonably choose either. A product that cannot ask for
`pg_read_all_data` — because its customers will not grant it, or because it must work on PostgreSQL 13
where the role does not exist — has the helper as its only route to the same data.

**Darling ships neither today.** The grant is documented above and the helper is not implemented. If a
fleet will not widen the role, the current behaviour is the honest one: `pg_column_stats` collects nothing,
the bloat estimate reports `estimate_unavailable`, and every other collector is unaffected.

## 9. Alerting

The three outage predictors alert; the other 24 collectors are read-only signals.

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
is named `PERMISSIONS`, and things landing in it are not always missing grants — the `error_message`
says which kind it is. A missing extension a collector declares gets its own `EXTENSION_MISSING` status
(#3240) rather than that bucket, so an uninstalled optional module never reads as a grant problem.

| `status` | `error_message` says | Actually means | Fix |
|---|---|---|---|
| `PERMISSIONS` | a missing grant | it is one | `GRANT pg_monitor` (step 1) |
| `EXTENSION_MISSING` | names the extension, "NOT a missing grant" | that extension never created where the collector connects | `CREATE EXTENSION` per the message (step 1's optional half), or leave it uninstalled and accept the gap |
| `PERMISSIONS` | "NOT a missing grant", not implemented | reading something this engine lacks | nothing — expected off Aurora |
| `PERMISSIONS` | "NOT a missing grant", feature disabled | switched off in the parameter group | enable it, or accept the gap |
| `PERMISSIONS` | `is not authorized to perform: rds:Describe...`/`rds:Download...`, names an IAM role ARN | the **monitoring host's IAM role** lacks the AWS-level grant plan capture/deadlocks need on Aurora/RDS | attach the IAM policy in step 1's IAM subsection — a DB-side grant cannot fix this, it's a different identity entirely |
| `ERROR` | a statement timeout | the query was too slow **once** | usually transient; deliberately does *not* drop the connection, so a slow query cannot cause a reconnect storm |
| `ERROR` | anything else | unclassified | read `error_message`; this is the bucket that wants a bug report |
| `YIELDED` | lock contention | the collector stepped aside | none today — no PostgreSQL collector opts into the lock-timeout yield |

Connection-level failures (the `08` class, `57P0x`) force a reconnect and a re-probe, so a failover
re-probes the target — which is how a promoted reader stops being gated as a standby.

**Symptoms with no error anywhere**, the ones that cost the most time:

| Symptom | Cause |
|---|---|
| The connect line says SQL Server, or the error mentions `SqlException` / a TDS handshake against 5432 | the target lost its `engine` on the way through the registry. Requires store schema **v70+**: `SELECT name, engine, port FROM config.config_monitored_servers;` — if `engine` is not a column, this build predates the fix and no darling.json edit will help |
| Autovacuum health reports everything fine on a cluster you know is behind | you are reading a **reader**. It reports all zeros, not an error. Measured: writer 13,654,458 dead tuples, reader 0, same cluster and tables |
| Added a target to darling.json and nothing happened | the store was already seeded; use `add_servers` (step 2) |
| `pg_wait_stats` empty, everything else fine | not Aurora. Core PostgreSQL has no cumulative wait counters at all, and the gap message says so — `pg_wait_sampling` answers the same question from its extension |
| `get_pg_top_queries` empty on self-hosted | `pg_stat_statements` not installed (step 1's optional half) — since #2625 the collector reads the vanilla view anywhere the extension exists |
| Only some databases in `pg_autovacuum_stats` | by design: `datallowconn` and non-template only, minus `rdsadmin` on a managed instance (it rejects every customer principal) and your `excludedDatabases` |
| Store stopped compressing after adding targets | background workers (step 4). Silent — check the postmaster log for "out of background workers" |

## 11. Removing a target

`remove_server` through MCP, or the Viewer's Manage Servers dialog — **not** by deleting the darling.json
entry, which the registry ignores (step 2). Collection stops within a sweep; the collected history stays
under its `server_id` and ages out on the normal retention horizons. Nothing was ever created on the
monitored instance, so there is nothing to clean up there — dropping `darling_monitor` is optional and
unrelated to Darling's state. If you enabled `auto_explain` for plan capture, that parameter is yours to
keep or revert; Darling only ever read the log it produced.

## 12. What this does not cover, because it does not exist yet

This list used to be longer, and everything struck from it is covered in the steps above: plan capture
shipped (`pg_plan_capture` — #2566 self-hosted via the server log, #2538/#2692 on Aurora/RDS via the
log API, with `pg_plan_capture_readiness` naming any missing precondition), blocking chains shipped
(`pg_blocking` and `get_pg_blocking`), and the Viewer shipped its PostgreSQL surfaces (#2530 — a
PostgreSQL target gets seven inner tabs in place of the nineteen SQL Server ones). What genuinely
remains:

- **Scheduled analysis findings.** The analysis pipeline is still SQL-Server-shaped and a PostgreSQL
  target produces no findings — but it now says so instead of sitting blank: `analysis_state` records
  that scheduled analysis does not apply to a PostgreSQL target and routes you to the `get_pg_*` reads
  and the three outage-predictor alerts. Do not read that message as "still collecting".
- **Configurable alert thresholds.** Still derived from the target's own settings and not
  operator-tunable (step 9) — [`postgres-alerting-design-note.md`](postgres-alerting-design-note.md).
- **The `pg_stats` helper-function route.** Step 8 documents `pg_read_all_data` and the
  `SECURITY DEFINER` alternative; only the grant is implemented. A fleet that will not widen the role
  gets measured sizes and suppressed estimates, exactly as step 8 describes.
- **Charts on the PostgreSQL tabs.** Both UIs render tables over the window — no correlated timeline,
  and no drill-down from a blocking root to the sessions behind it.
