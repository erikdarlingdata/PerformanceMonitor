# PostgreSQL blocking chains — design note

Status: **not started.** Decisions below are the ones worth making before writing code; none needs Erik
unless flagged.

The SQL Server side has three blocking surfaces (`dmv_blocking_snapshots`, `blocked_process_reports`,
`get_blocking`). PostgreSQL has none of it yet, and blocking is the condition people actually call about.

## What the source looks like

`pg_stat_activity` plus `pg_blocking_pids(pid)`. Unlike SQL Server there is no ring-buffer equivalent and no
server-side threshold that materialises a report — nothing is recorded unless someone looks. So this is a
**sampling** collector: it captures who is blocked, by whom, on what, at the moment it runs. That has a
consequence worth stating in the collector's own docs, because it will otherwise be mistaken for a
blocked-process-report equivalent: **blocking shorter than the cadence is invisible.** The SQL Server
collector catches a 6-second block via the ring buffer; a 1-minute PostgreSQL sample will not.

## Decisions

**1. `pg_blocking_pids()` is not free — call it selectively.** It takes ShareLock on the lock manager
partitions per call. Calling it for every row in `pg_stat_activity` on a 5,000-connection instance is
exactly the kind of monitoring query that becomes the incident. Call it only for backends that are already
waiting on a lock: `WHERE wait_event_type = 'Lock'`. That is the population that can have blockers, so the
filter loses nothing and bounds the cost to the actually-blocked set.

**2. Store the edge list, not a rendered tree.** `pg_blocking_pids()` returns an array; unnest it to one row
per (blocked_pid, blocking_pid) pair. Rendering a chain at collection time bakes in one view of it, and the
interesting questions (root blocker, chain depth, fan-out) are all cheap over an edge list and expensive to
recover from a string. The reader assembles the tree — same division as the existing readers computing
ratios the collector deliberately does not.

**3. Capture the blocker's own state, not just its pid.** A chain whose root is `idle in transaction` is a
different problem from one whose root is a long-running query, and the pid alone does not say which. Both
sides of each edge need `state`, `wait_event_type`/`wait_event`, `xact_start`, `query_start` and the query
text. This is the single most common gap in homegrown PostgreSQL blocking monitoring — you get a pid and
have to go find out what it was doing, by which time it is gone.

**4. Query text is a truncation decision, not an afterthought.** `pg_stat_activity.query` is capped by
`track_activity_query_size` (1 KB default). Store what the server gives and do NOT try to join out to
`pg_stat_statements` for the full text — the queryid is not exposed on `pg_stat_activity` before PG14, and
even after, correlating a live backend to a normalised entry is a different claim than "this is what it
ran". Record the truncation instead so a reader knows the text may be clipped.

**5. Applies to: any PostgreSQL target, INCLUDING standbys.** Recovery conflicts are real blocking and a
standby is where they happen. Do not inherit the autovacuum collector's `IsInRecovery` gate — that gate
exists because `pg_stat_user_tables` reports zeros on a replica, which does not apply here.
`pg_stat_activity` on a standby reports that standby's own backends, which is what you want.

**6. Cadence: 1 minute, retention 30 days, and do NOT set a lock timeout.** The SQL Server snapshot
collector sets a 1-second `LOCK_TIMEOUT` and yields rather than joining a blocking chain
(`YieldsOnLockTimeout`). That guard exists because it reads DMVs that can themselves block. Reading
`pg_stat_activity` does not take table locks, so there is nothing to yield on and declaring the flag would
add a branch that can never fire. (If a future version reads something heavier, revisit — the classifier
already maps 55P03 to a yield, and that branch is currently unreachable for PostgreSQL.)

**7. Permissions.** `pg_monitor` is enough to see other backends' `query` text. Without it
`pg_stat_activity.query` shows `<insufficient privilege>` for backends the login does not own — worth a
note in the collector, since it degrades to a useless capture rather than an error.

## Shape

Payload roughly: `blocked_pid`, `blocking_pid`, then for each side `state`, `wait_event_type`,
`wait_event`, `query`, `xact_duration_ms`, `query_duration_ms`, `application_name`, `client_addr`,
`database_name`, `username`. Plus `blocked_pid_count` per blocker so the reader can rank fan-out without a
self-join. Timestamps come from `pg_stat_activity` as timestamptz — **use `AT TIME ZONE 'UTC'`**, per the
trap that already bit twice on this branch, or prefer storing durations computed server-side.

## Read surface

`get_pg_blocking` — root blockers first, ranked by how many backends they are blocking and for how long,
with each root's own state spelled out (the `idle in transaction` case named explicitly, since the remedy is
"fix the application", not "tune the query"). Chain depth per edge. Follow the existing pattern: severity
classified in the tool, a distinct explanation per blocker state, and no claim the collector cannot support.

## Verification, per the established habit

Ladder-generator diff for the rung; `probe_collector_sql_live.py` against stage 16.11 and 17.7 (this one
genuinely needs the live run — `pg_blocking_pids()` behaviour and the `<insufficient privilege>` degradation
are both things to see rather than assume); `probe_validate_reader_sql.py` with a synthetic edge list that
includes a two-level chain, a fan-out root, and an `idle in transaction` root.

---

## What building it changed — 2026-08-12

Shipped as `pg_blocking` → `collect.pg_blocking_edges`, rung **V71**, `SchemaVersion` 71. Seven of the eight
decisions above survived contact unchanged. What the live runs changed is recorded here rather than edited
into the decisions above, because the corrections are the useful part.

**Three defects the C# suite could not have caught, all found by running the real strings on real Aurora.**

1. **`current_setting('track_activity_query_size')::int` throws.** `current_setting()` renders a memory GUC
   *with its unit* — the value comes back `'8kB'` on stage Aurora 17.7 and `'4kB'` on 16.11 (both instances
   are set well above the 1 kB default). The cast raises an invalid-input-syntax error, which fails the
   **whole collection**, every cycle, not just the one column. Fixed with
   `pg_size_bytes(current_setting(...))`, which parses every unit form. Pinned by
   `ReadsTheQuerySizeGucThroughPgSizeBytes_NotAnIntCast`.

2. **The reader's chain query needed `WITH RECURSIVE`, not `WITH`.** PostgreSQL scopes `RECURSIVE` to the
   entire `WITH` clause, not to the CTE that self-references, so the query failed with
   `relation "chain" does not exist` — a forward reference it will not resolve. A runtime error on the first
   `get_pg_blocking` call and invisible at build time.

3. **Three output columns had no alias** and came back all named `coalesce`. Harmless to the positional C#
   reader, and exactly the kind of thing that makes a query nobody can debug in psql. Every output column of
   both reads is now aliased.

**The eighth decision, which the design note did not anticipate: a sampled cycle was invisible.**
`chains` identifies a root by absence — a backend that blocks something and is not itself blocked. In a lock
cycle every participant is blocked, so there is no root, so the entire cyclic component was silently dropped:
**0 rows from a capture that recorded real blocking.** For a collector whose whole premise is that an empty
answer must never be mistaken for "nothing happened", that was the one place the read did precisely that.

Cycles are rare but genuinely reachable — PostgreSQL's deadlock detector resolves them, but only after
`deadlock_timeout` (1 s by default), and a capture can land inside that window. When it does, the stored
edges are the only evidence that will ever exist, because the engine kills a participant a moment later.

Fixed with a second read, `PgBlockingCyclesSql`, which finds participants by **reachability from
themselves** rather than by "this capture has no root" — the latter would miss a cycle sharing a capture with
an ordinary chain. `get_pg_blocking` reports `cycles` alongside `chains`, always present so its absence can
never read as "not checked", and has a dedicated `cycles_only` status for a window that captured nothing else.

**Verification actually run** (all PASS on stage Aurora **16.11** and **17.7**):

- `laddercheck` — new harness, and it diffs **all eight** PostgreSQL rungs against `PgSchemaGenerator`, not
  just the new one. Nobody had checked this before: the suite asserted the generator emits every table and
  that each rung is well-formed, never that the two said the *same thing*. Now also a real test,
  `EveryPostgresRung_IsIdenticalToTheGeneratedSchema`.
- `probe_blocking_collector_live.py` — runs the **shipped** collector string against a **real two-deep
  blocking chain**, built with **advisory locks**. That is what keeps the probe strictly read-only while
  still producing genuine lock-manager blocking: `pg_blocking_pids()` reports advisory waiters and their
  `wait_event_type` is `Lock`, identical to a row or table lock, but nothing is written and every lock is
  released on disconnect. A syntax-only check would have exercised none of the parts that can be wrong,
  because a healthy instance returns zero rows. 17 checks, including that the synthetic backend id is
  *recomputable* from `backend_start` + pid rather than merely present.
- `probe_blocking_reader_sql.py` — the reader text over synthetic edge sets with known answers: a 3-deep
  chain (depth 3, 1 direct victim, 3 total), a wide fan-out (depth 1, 3 direct), recurrence across two
  captures for one backend id, and the cycle. 21 checks, including that neither acyclic scenario is
  *mis*-reported as a cycle — a false deadlock claim is worse than a missed one, because it prescribes lock
  ordering as the fix when that is not the fix.

### The invariant that replaced scenario-hunting — 2026-08-13

Two review rounds each found the same CATEGORY of defect: **a captured blocking relationship that no read
reports.** First pure cycles (no root exists, so `chains` drops the whole component); then the "lollipop" —
X blocked by A where A/B/C cycle — which `chains` cannot reach *and* the cycle walk cannot close on, so X
appeared nowhere at all.

Fixing the first instance did nothing to prevent the second, because both fixes were scenario-shaped and the
probe's scenarios were all **isolated**: one situation per capture, where a real capture snapshots the whole
instance. Enumerating scenarios finds instances; it does not close a category.

`probe_blocking_reader_sql.py` now asserts the invariant directly: **every `blocked_pid` in the stored edge
set must be accounted for by some read** — in a root's chain, as a cycle participant, or in the set queued
behind a cycle. It is built by SPLICING the shipped queries' own CTE blocks (cut at the final top-level
`SELECT`, which is the one at column 0 after the raw-string literal dedents) rather than reimplementing them,
so it cannot keep passing while the real logic drifts.

Proven non-vacuous rather than assumed: with the `behind` arm removed — the pre-fix state — it reports
exactly `(9, 910)` and `(9, 911)`, the two lollipop victims. With the arm present, zero orphans on both
stage majors.

**Any future read added here should be added to that reconciliation.** A new read that covers nothing new
still has to leave the orphan set empty, and a new *collector* case that nothing covers will show up as an
orphan instead of as a silent absence months later.
