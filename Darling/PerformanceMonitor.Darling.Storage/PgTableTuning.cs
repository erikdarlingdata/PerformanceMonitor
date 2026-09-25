/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Composer + analyze_*_plan performance tuning — RUNTIME setup, deliberately NOT a versioned migration. These
/// are RESULTS-INVARIANT covering indexes plus a per-table autovacuum-insert override: the Custom Views composer
/// and the analyze_*_plan reads return identical rows with or without them, just slower, so they must NOT bump
/// <see cref="StorageVersion"/> — that would gate the Viewer's connect-time schema check on perf indexes it does
/// not need (the same reasoning that keeps role GUCs / GRANTs in role provisioning rather than a migration). The
/// service applies them once at startup AFTER migrations and the TimescaleDB block, BEFORE collectors: so a fresh
/// store's collector tables are already hypertables when indexed (CREATE INDEX / ALTER TABLE SET propagate to all
/// existing and future chunks), a plain-PostgreSQL store gets them too, and a first build on a large adopted store
/// never contends with live inserts. Idempotent (CREATE INDEX IF NOT EXISTS / ALTER TABLE SET), so every restart
/// re-converges and a store that already has them (a field box hand-indexed, or a prior start) no-ops.
///
/// <para>EXPLAIN-backed field fix (2026-07-22): the composer filtered by <c>object_name</c> / <c>query_hash</c>
/// fell to a Seq Scan and timed out at the 15 s statement_timeout; a PLAIN index still paid a Bitmap Heap Scan
/// re-fetching the aggregate columns, so the three composer indexes are COVERING (INCLUDE exactly the
/// measure-catalog columns the Procedures / Queries / Query Store measures SUM/AVG) for an Index Only Scan. The
/// three <c>_server_..._time</c> indexes back the single-row analyze_*_plan lookups (ORDER BY collection_time DESC
/// LIMIT 1 — one heap fetch, no INCLUDE needed). The per-table
/// <c>autovacuum_vacuum_insert_scale_factor = 0.02</c> override keeps the visibility map current on the pure-insert
/// hypertable chunks (the Postgres default 0.2 leaves the day's hot chunk stale before the daily TimescaleDB
/// rollover, degrading the Index Only Scan back to heap fetches). Verified: the two panels that timed out at 15 s
/// then ran in 139 ms / 514 ms with 0 heap fetches on vacuumed chunks. DarlingStoredPlanReader and ComposeCompiler
/// are unchanged — correct as-is, they just needed these indexes + the vacuum state to perform.</para>
///
/// <para><b>The alerting pass joined the composer here (#3573)</b>, for the same reason and with the same
/// EXPLAIN-backed shape: <c>DarlingAlertReadAdapter.ForcePlanFailuresSql</c> takes the fourth covering index
/// below, an Index Only Scan replacing a plan that streamed the whole fleet's two-hour slice of
/// <c>query_store_stats</c> to keep one server's rows. Its derivation is on the statement itself — including
/// why it is here and not a ladder rung, and why it is a plain <c>CREATE INDEX</c> and not the per-chunk form.</para>
///
/// <para><b>#4247: five of what were originally eight indexes here are DROPS, not CREATEs.</b> Every row
/// inserted into these three hypertables lands on an arbitrary leaf of a day-wide chunk index; with data
/// checksums on and 5-minute checkpoints, the first touch of each leaf after a checkpoint writes a full 8 KB
/// page image to WAL, so a random-key index bigger than one checkpoint's worth of inserts costs close to a
/// page image PER INSERTED ROW. Production measurement (the issue): these five cost ~24-28% of two stores'
/// WAL and were scanned at most 13 times across several retained days, against hundreds of thousands of scans
/// for the three composer/alerting indexes that stay. Ruling: drop an index only if its slowest reader,
/// WITHOUT it, stays under HALF that reader's deadline. Measured on a rig seeded at one busy server's rate
/// (11 servers, 5 days, ~60k rows/day/table/server) with EXPLAIN (ANALYZE, BUFFERS); the two handle readers
/// timed at a handle seen in the last hour and one last seen on day 1 of the range. All five passed by wide
/// margins (13x-75x headroom, one skip-scan case ~3780x) — the numbers are on each DROP statement below.</para>
///
/// <para>A drop is <c>DROP INDEX IF EXISTS</c> here, never a migration (same RESULTS-INVARIANT / runtime-setup
/// reasoning as a CREATE), wrapped in its own <c>BEGIN; SET LOCAL lock_timeout; DROP; COMMIT;</c>
/// (<see cref="GuardedDrop"/>) so a build stuck behind a live reader gives up after
/// <see cref="DropLockTimeoutSeconds"/> instead of holding ACCESS EXCLUSIVE on the hypertable — and every
/// chunk — for up to <see cref="SetupTimeoutSeconds"/>; the next start's idempotent re-run retries it. Proven
/// live on the rig: a blocked guarded drop raises <c>55P03 lock_not_available</c> right at the timeout, NOT at
/// <see cref="SetupTimeoutSeconds"/>. That failure leaves the connection inside the statement's own aborted
/// implicit transaction — Npgsql's simple-query protocol stops at the first error and never reaches the
/// trailing <c>COMMIT</c> text in the same command, so the session stays "in failed transaction" for whatever
/// runs on it next. <see cref="ApplyAsync"/> therefore issues a best-effort <c>ROLLBACK</c> after EVERY caught
/// failure, not just a drop's, so one blocked or invalid statement can never take the rest of the sweep down
/// with it — proven live by seeding a block, forcing the failure, and asserting the statement after it still
/// applies.</para>
/// </summary>
public static class PgTableTuning
{
    /* A first CREATE INDEX on a large adopted store can take a while (it runs pre-collection, so it blocks no
       live inserts, but the build itself is real work) — the same generous budget the TimescaleDB first
       conversion uses. */
    private const int SetupTimeoutSeconds = 300;

    /// <summary>
    /// #4247: the lock_timeout every guarded <c>DROP INDEX</c> statement below runs under. A DROP on a
    /// hypertable takes ACCESS EXCLUSIVE on it and every chunk; without a short timeout, a drop queued behind
    /// a live reader would hold up every LATER reader of the table for up to <see cref="SetupTimeoutSeconds"/>.
    /// With it, the drop gives up (proven live: raises <c>55P03 lock_not_available</c> right at this bound) and
    /// the next idempotent start retries it. <c>SET LOCAL</c> so it cannot outlive the statement's own
    /// transaction either way.
    /// </summary>
    private const string DropLockTimeoutSeconds = "5s";

    /// <summary>
    /// Builds a guarded <c>DROP INDEX IF EXISTS</c> statement: its own transaction, a short
    /// <see cref="DropLockTimeoutSeconds"/> lock_timeout, reset by the transaction boundary either way. NOT
    /// <c>CONCURRENTLY</c> — hypertables refuse it, same as every CREATE in this file.
    ///
    /// <para>A lock-timeout failure aborts THIS statement's own implicit transaction and never reaches the
    /// trailing <c>COMMIT</c> text (Npgsql's simple-query protocol stops at the first error in a multi-statement
    /// command), so the connection is left "in failed transaction" for whatever runs on it next —
    /// <see cref="ApplyAsync"/>'s best-effort post-failure <c>ROLLBACK</c> is what keeps that from taking the
    /// rest of the sweep down with it.</para>
    /// </summary>
    private static string GuardedDrop(string indexName) =>
        "BEGIN; SET LOCAL lock_timeout = '" + DropLockTimeoutSeconds + "'; DROP INDEX IF EXISTS collect."
        + indexName + "; COMMIT;";

    /// <summary>
    /// The #3573 covering index's name, and the columns it carries in key-then-INCLUDE order. Exposed so the
    /// pin (<c>ForcePlanFailuresAccessPathTests</c>) can hold the read's column references against THIS list
    /// rather than against a second copy of the statement text, and so the live test can find the index by
    /// name in <c>pg_indexes</c>. The first two are the key; the rest are INCLUDE. Every column the read
    /// references must appear here or the Index Only Scan silently degrades to the heap plan it replaced.
    /// </summary>
    public const string ForcePlanFailuresIndexName = "idx_query_store_stats_server_time_forcing";

    public static IReadOnlyList<string> ForcePlanFailuresIndexColumns { get; } = new[]
    {
        "server_id", "collection_time",
        "database_name", "query_id", "plan_id", "force_failure_count", "is_forced_plan", "plan_forcing_type", "last_force_failure_reason",
    };

    /// <summary>
    /// The idempotent tuning statements, applied in order, each on its own command (failure-isolated). #4247
    /// DROPPED the three composer covering indexes and two of the three single-row lookup indexes (measured:
    /// near-zero reads, ~25% of two production stores' WAL). What remains under CREATE: the lookup index that
    /// IS read constantly (<c>server_id, query_hash, collection_time DESC</c>), the QS db+query+plan lookup,
    /// the #3573 covering index for the alerting pass (INCLUDE exactly that read's columns, for an Index Only
    /// Scan), and the #3934 store-metrics skip-scan index — then the per-table autovacuum-insert override on
    /// exactly the four growing tables. Bare collect-qualified names; every identifier is a compile-time
    /// constant, never user input, so interpolation is not a concern.
    /// </summary>
    public static IReadOnlyList<string> Statements { get; } = new[]
    {
        /* #4247 DROP (was CREATE): idx_procedure_stats_object_name (object_name, collection_time) INCLUDE
           (database_name, delta_worker_time, delta_elapsed_time, delta_execution_count) — 0-2 scans/store,
           1.1-1.3% of WAL. Reader: the composer's procedure_stats "object_name" dimension (Procedures
           measures), 15 s statement_timeout deadline. Rig-measured (11 servers x 5 days x ~60k rows/day,
           WHERE object_name = <one of 50 procs> AND a 24 h collection_time window): 6.762 ms with the index
           (Index Only Scan) vs. 140.747 ms without it (Parallel Seq Scan, no usable fallback index on this
           table) — 53x under the 7.5 s half-deadline. ProcedureStatsFallbackByObjectSql (oversized-plan
           backlog) also filters object_name but keys server_id/database_name/schema_name first; it is a rare
           fallback read, not measured separately. */
        GuardedDrop("idx_procedure_stats_object_name"),
        /* #4247 DROP (was CREATE): idx_procedure_stats_server_handle_time (server_id, sql_handle,
           collection_time DESC) — 1-6 scans/store, 5.9-8.3% of WAL. Reader:
           DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql (server_id, sql_handle) -> ORDER BY
           collection_time DESC LIMIT 1, McpCommandDeadlines.ReadSeconds = 20 s deadline. Rig-measured, a
           handle seen in the last hour and one last seen on day 1 of 5: 0.137 ms / 0.190 ms with the index vs.
           40.089 ms / 396.139 ms without it (falls back to idx_procedure_stats_time, an Index Scan Backward
           filtered by sql_handle) — worst case 25x under the 10 s half-deadline. */
        GuardedDrop("idx_procedure_stats_server_handle_time"),
        /* #4247 DROP (was CREATE): idx_query_stats_server_handle_time (server_id, sql_handle,
           collection_time DESC) — the #1981 ProcStats comparison's representative-statement LATERAL twin of
           the procedure_stats handle index above. 0-13 scans/store, 8.7-11.3% of WAL. Reader: the LEFT JOIN
           LATERAL in ViewerDataService.ProcedureStats.GetProcedureStatsComparisonAsync (qs.server_id,
           qs.sql_handle) -> ORDER BY collection_time DESC LIMIT 1, ViewerCommandDeadlines base
           InteractiveReadSeconds = 15 s deadline. Rig-measured, fresh/stale handle: 3.951 ms / 6.462 ms with
           the index vs. 68.200 ms / 565.808 ms without it (idx_query_stats_time, Index Scan Backward filtered
           by sql_handle) — worst case 13x under the 7.5 s half-deadline. */
        GuardedDrop("idx_query_stats_server_handle_time"),
        /* #4247 DROP (was CREATE): idx_query_stats_query_hash (query_hash, collection_time) INCLUDE
           (database_name, delta_worker_time, delta_elapsed_time, delta_execution_count) — 0-1 scans/store,
           2.5-3.2% of WAL. Reader: the composer's query_stats "query_hash" dimension (Queries measures), 15 s
           statement_timeout deadline. Rig-measured (WHERE query_hash = <1 of 3000> AND a 24 h window):
           0.403 ms with the index vs. 1.983 ms without it — PG18 picks a native Index Skip Scan on the KEPT
           idx_query_stats_server_hash_time sibling below (Bitmap Index Scan, 13 index searches, one per
           server_id), ~3780x under the 7.5 s half-deadline. DarlingStoredPlanReader.QueryStatsPlanXmlByHashSql
           also keys on query_hash, but WITH server_id too, so it was already served by that same KEPT sibling
           and was never at risk. */
        GuardedDrop("idx_query_stats_query_hash"),
        "CREATE INDEX IF NOT EXISTS idx_query_stats_server_hash_time ON collect.query_stats (server_id, query_hash, collection_time DESC)",
        /* #4247 DROP (was CREATE): idx_query_store_stats_query_hash (query_hash, collection_time) INCLUDE
           (database_name, module_name, execution_count, avg_duration_us, max_duration_us, avg_cpu_time_us,
           max_cpu_time_us) — 0 scans/store (no fallback sibling keyed on query_hash exists for this table), 5.2-8.4%
           of WAL. Reader: the composer's query_store_stats "query_hash" dimension (Query Store measures), 15 s
           statement_timeout deadline. Rig-measured: 0.398 ms with the index (Index Only Scan) vs. 99.968 ms
           without it (Parallel Index Scan on idx_query_store_stats_time filtered by query_hash) — 75x under
           the 7.5 s half-deadline. */
        GuardedDrop("idx_query_store_stats_query_hash"),
        "CREATE INDEX IF NOT EXISTS idx_query_store_stats_server_db_query_plan_time ON collect.query_store_stats (server_id, database_name, query_id, plan_id, collection_time DESC)",
        /* #3573: the alerting pass's forced-plan-failures read (DarlingAlertReadAdapter.ForcePlanFailuresSql,
           WHERE server_id = $1 AND collection_time > $2, a two-hour window, per server, every 30 s pass) outgrew
           its 10 s deadline on the largest production store: 1,744.9 ms cold when the deadline was derived over
           ~6 GB of query_store_stats, 10.3 s excursions at 23 GB. The live plan named the mechanism —

               Index Scan Backward using _hyper_.._chunk_query_store_stats_collection_time_idx
                 Index Cond: (collection_time > now() - '02:00:00')
                 Filter: (server_id = ...)   Rows Removed by Filter: 691,058   actual rows: 37,878
                 Buffers: shared hit=54,664 read=2,643

           — the read walks the ENTIRE fleet's two-hour slice through the TimescaleDB default time index and
           discards 95% of it to keep one server. Forty-three servers deep, that is the whole slice re-read
           forty-three times per pass cycle; warm it is 422 ms, and the excursions are the cold tail whenever
           cache pressure evicts a slice ~4x the size it had on measurement day.

           THE INDEX THAT READ WANTED ALREADY EXISTED. V1's generated idx_query_store_stats_time is exactly
           (server_id, collection_time), it is on the hypertable and on the very chunk in that plan, and the
           planner declined it. Read from the production catalog: server_id's physical correlation is 0.022
           (43 servers interleaved by collection pass) while collection_time's is 0.99999, so the cost model
           prices the composite's heap fetches as one random page per tuple — ~50K pages at random_page_cost
           4 — and the perfectly-correlated time index's 54,664-page stream wins on paper at 58,677. It loses
           in fact by 11x: forced under random_page_cost = 1.1 the SAME statement took the existing composite
           (Bitmap Index Scan, Index Cond on both columns) and touched 5,063 buffers (Heap Blocks: exact=5001)
           instead of 57,307, because a server's rows land in one contiguous run per collection pass (~13
           rows a page) that the planner's single correlation statistic cannot see. A second plain composite,
           however ordered, would be priced identically and ignored identically — which is why this is not
           the (server_id, collection_time DESC) rung the issue first proposed.

           COVERING, so the choice stops depending on the cost model. With every column the read touches in
           the key or INCLUDE, the plan is an Index Only Scan whose cost is the index pages for ONE server's
           two hours and nothing else — no heap component to misprice, at any random_page_cost and at any
           share of the fleet the busiest server grows into. Both uncompressed production chunks read
           relallvisible = 100% of relpages (the insert-autovacuum override below is what keeps them there),
           so heap fetches for visibility are the newest pass's pages at most. Measured on a PG18 /
           TimescaleDB 2.28.1 rig seeded in the production's write pattern: the shipped statement went from
           the identical time-index-plus-Filter plan at 1,514 buffers to Index Only Scan, Heap Fetches: 0,
           50 buffers. The INCLUDE list IS the read's column list, deliberately and exactly — a column added
           to the read and not to this list silently degrades it back to the heap plan, so
           ForcePlanFailuresAccessPathTests pins the two against each other.

           THE COST, stated rather than implied: INCLUDE disables btree deduplication, so this index is
           ~86 bytes/row on the rig (V1's deduplicated composite is ~7) — roughly 0.7-0.9 GB per day-chunk on
           the largest store's 8-16 M rows/day, against a 4.7-9.4 GB heap per chunk. It is self-limiting:
           on 2.28.1 a compressed chunk's uncompressed relation is an empty shell, and CREATE INDEX on the
           hypertable builds an 8 KB page for each one (measured: 4 compressed chunks at 8192 bytes each,
           the 2 live chunks at 46 MB and 27 MB), so the footprint is the one or two uncompressed chunks and
           the compression policy erases the rest a day later. That is also why the owner's "index only the
           uncompressed/new chunks" needs no mechanism: it is what the engine does. New chunks inherit the
           index at creation; a decompressed chunk fills it and recompression empties it (both measured).

           PLAIN CREATE INDEX, ONE TRANSACTION, deliberately. The build takes a ShareLock on the hypertable
           root for its duration — reads proceed, every INSERT into any chunk queues behind it — which is
           why this list runs BEFORE collectors start and why this statement belongs here rather than in the
           ladder (whose applier wraps each rung in a transaction block, the one place the per-chunk form
           below is refused outright). A cancel at SetupTimeoutSeconds rolls the whole build back and the next
           start retries with nothing left behind. The per-chunk form, WITH (timescaledb.transaction_per_chunk),
           was measured and rejected: it takes the same root ShareLock first, buying no write concurrency here,
           and a cancel MID-build — exactly what the command timeout is — commits the chunks built so far,
           leaves the parent index indisvalid = false and the live chunk unindexed, after which this very
           idempotent statement reports "already exists, skipping" on every start forever. CREATE INDEX
           CONCURRENTLY is refused on hypertables ("hypertables do not support concurrent index creation").
           A collision with the compression job on yesterday's chunk makes one of them wait for the other;
           if that is this build past its budget, the cancel-and-retry above is the outcome.

           NOT random_page_cost, though it flipped the plan: at 1.1 the composite won by 53,095 to 58,677 for
           a server holding ~8% of the fleet's rows, and the ratio scales with that share, so the next-busiest
           store or the same one a month on flips back. A store-wide planner setting is also an owner's
           ruling for every read at once, not a lane's fix for one. NOT a partial index WHERE is_forced_plan,
           which would be a few MB: the read aggregates unforced rows on purpose (a plan's previous sighting
           may be its unforced one), so that index needs the statement reshaped and its first-sighting
           semantics changed — the cheaper long-run shape, deferred rather than smuggled in. */
        "CREATE INDEX IF NOT EXISTS " + ForcePlanFailuresIndexName + " ON collect.query_store_stats (server_id, collection_time DESC) INCLUDE (database_name, query_id, plan_id, force_failure_count, is_forced_plan, plan_forcing_type, last_force_failure_reason)",
        "ALTER TABLE collect.procedure_stats SET (" + InsertTuningOptions + ")",
        "ALTER TABLE collect.query_stats SET (" + InsertTuningOptions + ")",
        "ALTER TABLE collect.query_store_stats SET (" + InsertTuningOptions + ")",
        /* pg_statement_stats is query_stats' per-minute PostgreSQL twin — same shape, same cadence, same
           pure-insert hypertable chunks — so the identical reasoning applies: the stock 0.2 scale factor
           leaves the day's hot chunk stale before the TimescaleDB rollover and the Index Only Scan degrades
           to heap fetches. It was simply missed when the PostgreSQL collectors landed, since this list is
           hand-maintained rather than derived from the catalog. */
        "ALTER TABLE collect.pg_statement_stats SET (" + InsertTuningOptions + ")",
        /* #2402: query_plan_dim needs the OTHER knob, and needs it for the opposite reason. Every override
           above tunes INSERT vacuuming so the visibility map stays current on pure-insert hypertable chunks.
           This table is not a hypertable and is not insert-only: retention DELETEs from it, so its churn is
           DEAD TUPLES, which autovacuum_vacuum_insert_* does not govern at all.

           Left at the stock 0.2 it needs 20% of the table dead before autovacuum will look at it — on the
           dogfood store that is ~2.4 M rows, and it showed: 1,223,777 dead tuples with the last autovacuum
           two days earlier, while its own TOAST table (which has its own thresholds against 57 M chunks) had
           been vacuumed 152 times. The parent is the half that matters here, because the dead rows a purge
           leaves behind are precisely the idx_query_plan_dim_last_seen entries the NEXT purge has to visit
           and test for visibility before discarding. Untuned, each purge makes the following one slower and
           then waits days for cleanup.

           0.02 + 10000 sizes the trigger to roughly one purge's worth of deletions rather than to the
           table's total size, so cleanup follows the work that created it. */
        "ALTER TABLE collect.query_plan_dim SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 10000)",
        /* #3934: the store-metrics latest read (DarlingStoreMetricsReader.StoreMetricsLatestSql) took each
           object's newest row with DISTINCT ON (object_kind, object_name) ... ORDER BY object_kind,
           object_name, metric_time DESC over the WHOLE table — the only index was idx_store_metrics_time
           (metric_time) alone, so every call sorted every retained row. The table keeps 400 days of hourly
           sweeps at ~250 objects/sweep, so the sort grows linearly for over a year after a store is created:
           7,772 ms and an external merge sort spilling ~180 MB at full retention on a CI-sized rig (2.4 M
           rows), 225 ms on DARLING01 today (83,355 rows). A RESULTS-INVARIANT covering-shape index, same
           reasoning as every other statement in this list — Erik's ruling on the issue was that this needed
           no migration rung, since the composer's Tuning stage already creates exactly this kind of index
           idempotently at every start and hourly (#3817, #3913). Paired with the skip-scan rewrite of
           StoreMetricsLatestSql below, the latest read went from 7,772 ms to 15 ms on the same seed with
           identical rows (251). */
        "CREATE INDEX IF NOT EXISTS idx_store_metrics_kind_name_time ON collect.store_metrics (object_kind, object_name, metric_time DESC)",
    };

    /// <summary>
    /// Applies every <see cref="Statements"/> statement, each on its own command and FAILURE-ISOLATED: a single
    /// failure (e.g. a column absent on an odd store) warns and the sweep continues — the store keeps working,
    /// just without that one index. Returns the count that applied (or no-op'd) cleanly. Idempotent, so a re-run
    /// on an already-tuned store no-ops.
    /// </summary>
    public static async Task<int> ApplyAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var applied = 0;
        foreach (var statement in Statements)
        {
            try
            {
                using var command = new NpgsqlCommand(statement, connection) { CommandTimeout = SetupTimeoutSeconds };
                await command.ExecuteNonQueryAsync(cancellationToken);
                applied++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Composer performance-tuning statement failed — the store keeps working without it ({Statement}): {Message}",
                    statement, ex.Message);

                /* #4247: a guarded DROP that hits its lock_timeout fails INSIDE its own explicit BEGIN, and
                   Npgsql's simple-query protocol stops at that first error — the trailing COMMIT text in the
                   same command never runs, so the connection is left "in failed transaction" for whatever this
                   loop sends next. A plain autocommit statement never does this (no explicit BEGIN of its own
                   to leave open), so this is a no-op for every statement above except a failed guarded drop —
                   but it must run unconditionally, since a future statement could open a transaction the same
                   way. ROLLBACK outside a transaction is a harmless no-op (a WARNING, not an error), so this is
                   always safe; if it too fails the connection is unusable and every later statement's own
                   failure will say so. */
                try
                {
                    using var rollback = new NpgsqlCommand("ROLLBACK", connection);
                    await rollback.ExecuteNonQueryAsync(cancellationToken);
                }
                catch (Exception rollbackEx) when (rollbackEx is not OperationCanceledException)
                {
                    logger?.LogWarning(
                        "Post-failure ROLLBACK also failed — the connection may be unusable for the rest of this sweep: {Message}",
                        rollbackEx.Message);
                }
            }
        }

        logger?.LogInformation(
            "Composer performance tuning applied ({Applied}/{Total} covering-index / autovacuum statements)",
            applied, Statements.Count);

        applied += await ApplyHypertableInsertTuningAsync(connection, logger, cancellationToken);
        return applied;
    }

    /// <summary>The reloptions every pure-insert hypertable gets. Shared with the literal statements above so
    /// the two spellings cannot drift apart.</summary>
    public const string InsertTuningOptions =
        "autovacuum_vacuum_insert_scale_factor = 0.02, autovacuum_vacuum_insert_threshold = 10000";

    /// <summary>
    /// Every hypertable that does not already carry the insert tuning, resolved from the TimescaleDB catalog.
    /// The name is built with <c>format('%I.%I')</c> so it comes back correctly quoted.
    /// </summary>
    public const string UntunedHypertablesSql = @"
SELECT format('%I.%I', h.hypertable_schema, h.hypertable_name)
FROM timescaledb_information.hypertables h
JOIN pg_class c ON c.relname = h.hypertable_name
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = h.hypertable_schema
WHERE COALESCE(array_to_string(c.reloptions, ','), '') NOT LIKE '%insert_scale_factor%'";

    /// <summary>
    /// Applies the insert tuning to every hypertable still missing it, DERIVED from the catalog rather than
    /// from the hand-written list above (#2405).
    ///
    /// <para>The literal statements exist because a plain-PostgreSQL store has no TimescaleDB catalog to read,
    /// and those four tables are the ones whose read paths were measured; this sweep is what makes the property
    /// hold for the rest. It matters because "pure-insert append-only fact table" describes EVERY collector
    /// target by construction, while the literal list described whichever four had been the subject of an
    /// EXPLAIN investigation — 4 of 51 hypertables on the dogfood store, with untuned tables (perfmon_stats at
    /// 5.3 GB, spinlock_stats at 3.7 GB, wait_stats at 2.8 GB) larger than tuned ones. A list that has to be
    /// remembered when a collector lands is a list that gets missed, and this one already had been: the file's
    /// own comment records pg_statement_stats being added late for exactly that reason.</para>
    ///
    /// <para>Deliberately scoped to HYPERTABLES, not to everything in <c>collect</c>. Insert-driven autovacuum
    /// governs the visibility map and freezing on append-only data; a plain table whose churn is DELETEs needs
    /// <c>autovacuum_vacuum_scale_factor</c> instead, which is a different knob for a different failure and is
    /// set explicitly where it applies. Sweeping every table here would silently apply the inert one.</para>
    ///
    /// <para>The 10,000-row threshold is what keeps this safe on the many small hypertables: the scale factor is
    /// not consulted until that many inserts have accumulated, so a low-rate table is not pushed into
    /// constant autovacuum. Per-table failure isolation matches <see cref="ApplyAsync"/>, and a store without
    /// TimescaleDB simply finds no catalog and no-ops.</para>
    /// </summary>
    public static async Task<int> ApplyHypertableInsertTuningAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var targets = new List<string>();
        try
        {
            using var query = new NpgsqlCommand(UntunedHypertablesSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            await using var reader = await query.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                targets.Add(reader.GetString(0));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* No TimescaleDB (a plain-PostgreSQL store) or an unreadable catalog — the literal statements above
               already ran, so this is a no-op rather than a failure. */
            logger?.LogDebug("Hypertable insert-tuning sweep skipped: {Message}", ex.Message);
            return 0;
        }

        if (targets.Count == 0)
        {
            return 0;
        }

        var applied = 0;
        foreach (var target in targets)
        {
            /* Identifier comes from the catalog already quoted by format('%I.%I') — never user input. */
            var statement = $"ALTER TABLE {target} SET ({InsertTuningOptions})";
            try
            {
                using var command = new NpgsqlCommand(statement, connection) { CommandTimeout = SetupTimeoutSeconds };
                await command.ExecuteNonQueryAsync(cancellationToken);
                applied++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Insert-autovacuum tuning failed for {Table} — the store keeps working without it: {Message}",
                    target, ex.Message);
            }
        }

        logger?.LogInformation(
            "Insert-autovacuum tuning applied to {Applied}/{Total} previously-untuned hypertable(s)",
            applied, targets.Count);
        return applied;
    }
}
