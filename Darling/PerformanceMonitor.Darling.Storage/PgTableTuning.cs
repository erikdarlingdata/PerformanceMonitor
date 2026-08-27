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
/// </summary>
public static class PgTableTuning
{
    /* A first CREATE INDEX on a large adopted store can take a while (it runs pre-collection, so it blocks no
       live inserts, but the build itself is real work) — the same generous budget the TimescaleDB first
       conversion uses. */
    private const int SetupTimeoutSeconds = 300;

    /// <summary>
    /// The idempotent tuning statements, applied in order, each on its own command (failure-isolated). Three
    /// COVERING composer indexes (INCLUDE the aggregate columns the Procedures / Queries / Query Store measures
    /// SUM/AVG, for an Index Only Scan), three <c>(server_id, handle/hash/id, collection_time DESC)</c> lookup
    /// indexes for the single-row analyze_*_plan reads (no INCLUDE — one heap fetch is cheap), then the per-table
    /// autovacuum-insert override on exactly the four growing tables. Bare collect-qualified names; every
    /// identifier is a compile-time constant, never user input, so interpolation is not a concern.
    /// </summary>
    public static IReadOnlyList<string> Statements { get; } = new[]
    {
        "CREATE INDEX IF NOT EXISTS idx_procedure_stats_object_name ON collect.procedure_stats (object_name, collection_time) INCLUDE (database_name, delta_worker_time, delta_elapsed_time, delta_execution_count)",
        "CREATE INDEX IF NOT EXISTS idx_procedure_stats_server_handle_time ON collect.procedure_stats (server_id, sql_handle, collection_time DESC)",
        /* #1981: the ProcStats comparison's representative-statement LATERAL probes query_stats by
           (server_id, sql_handle) newest-first — the exact twin of the procedure_stats handle index
           above. Bounded by raw retention (4 days of chunks), so the build is cheap on any store. */
        "CREATE INDEX IF NOT EXISTS idx_query_stats_server_handle_time ON collect.query_stats (server_id, sql_handle, collection_time DESC)",
        "CREATE INDEX IF NOT EXISTS idx_query_stats_query_hash ON collect.query_stats (query_hash, collection_time) INCLUDE (database_name, delta_worker_time, delta_elapsed_time, delta_execution_count)",
        "CREATE INDEX IF NOT EXISTS idx_query_stats_server_hash_time ON collect.query_stats (server_id, query_hash, collection_time DESC)",
        "CREATE INDEX IF NOT EXISTS idx_query_store_stats_query_hash ON collect.query_store_stats (query_hash, collection_time) INCLUDE (database_name, module_name, execution_count, avg_duration_us, max_duration_us, avg_cpu_time_us, max_cpu_time_us)",
        "CREATE INDEX IF NOT EXISTS idx_query_store_stats_server_db_query_plan_time ON collect.query_store_stats (server_id, database_name, query_id, plan_id, collection_time DESC)",
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
