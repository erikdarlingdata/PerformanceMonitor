/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Darling's delta calculator: the shared <see cref="CollectorDeltaCalculator"/> core (baseline /
/// counter-reset / gap-policy semantics live there, identical across SKUs) plus the Postgres
/// seeding that lets the first collection after a service restart produce accurate deltas instead
/// of returning 0 for everything — the twin of Lite's DuckDB-seeding DeltaCalculator, with the
/// seed queries, delta-group names, and key formats mirrored verbatim so the two hosts can never
/// restore different baselines. Seeds EVERY delta family the service monitors, keys and pass
/// window both (#3540 A4), so the restart contract in <see cref="CollectorDeltaCalculator"/>'s
/// remarks holds here; the two PostgreSQL families exist on this host only.
/// </summary>
public sealed class DarlingDeltaCalculator : CollectorDeltaCalculator
{
    /* The seed queries are verbatim from Lite's DeltaCalculator — deliberately written in the
       PG-shared dialect (the (server_id, collection_time) row-value latest-row form, DISTINCT ON for
       the latest-row-per-key form, and the reused $1 positional placeholder, all of which run on
       either engine as-is). Pinned by DarlingDeltaSeederTests, mirrored in Lite by
       LiteDeltaSeederTests, and read from both hosts' sources by Lite.Tests'
       DeltaFamilySeedingCensusTests, which proves every family has a seeder on each host.

       $1 is CollectorDeltaCalculator.SeedCutoff(), and it is bound on BOTH the outer read and the
       inner MAX() — either one left unbounded scans every chunk of the hypertable. That is what
       #1772 was: on a 276 GB field store the unbounded form could not finish inside the 30-second
       command timeout, so restart continuity silently degraded to first-cycle-zero deltas every
       time the service came up. The bound is free rather than a trade, because the delta gap policy
       throws away anything older than it anyway — see CollectorDeltaCalculator.SeedLookback.

       Two shapes, chosen per family by how the collector WRITES (#3540 A4):

       - Latest collection per server (the original four, latch_stats, spinlock_stats): these
         collectors write every key they read on every pass, so the newest collection holds every
         key's current counter and the row-value probe is the cheapest exact read.
       - Latest row per key (procedure_stats, query_stats, pg_wait_stats, pg_statement_stats): these
         collectors do NOT write every key every pass — procedure_stats and query_stats are TOP (n)
         reads that churn, and the two PostgreSQL collectors skip idle rows at the write — so the newest
         collection is missing keys whose counters are nevertheless unchanged, and a key seeded from
         nothing takes the first-sighting path. DISTINCT ON (server_id, key) ... ORDER BY
         collection_time DESC over the
         cutoff window returns each key's newest row instead. Its bound is the single
         collection_time >= $1 on its only table read: there is no inner aggregate to bind a second
         time. On the hypertable that bound is what keeps the read to the window's chunk(s) through
         the (server_id, collection_time) index — the same chunk exclusion the #1772 pin proves for
         the row-value shape — and the window's rows are the whole working set: one sort over fifteen
         minutes of one family, which on the dogfood fleet's largest family is tens of thousands of
         rows, not a hypertable. A key seeded from an older row inside the window carries that row's
         timestamp, so its first delta spans a longer interval than the in-memory cache would have
         measured; the value is exact (the counter was idle in between, which is why no newer row
         exists) and the gap policy still bounds the span.

       query_stats joined the per-key shape with Darling V128 / Lite v61 (#3540). Its delta key is
       sql_handle:statement_start_offset:statement_end_offset:plan_handle, and until V128 the store
       persisted neither offset, so no row could reproduce the key the collector presents and only the
       family's PASS WINDOW could be seeded (the #2235 series-age rescue's input). The offsets are
       stored now, raw (-1 = "to the end of the batch", byte offsets into the nvarchar batch text) and
       the seed rebuilds the key from them with the collector's own interpolation. Two rules, both in
       the seeder rather than the SQL: a row whose offsets are NULL — every row written before V128 —
       seeds NO key, because a key built from a fabricated 0/-1 would be one nothing ever presents and
       the baseline under it would sit unread until it aged out; and EVERY row, NULL offsets or not,
       still feeds the pass window, so the first restart after the upgrade (when the whole window is
       pre-V128 rows) keeps the rescue armed exactly as #3614 left it. One read serves both, which is
       why the offset filter is not in the WHERE. */

    public const string WaitStatsSeedSql = @"
SELECT server_id, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, collection_time
FROM wait_stats
WHERE collection_time >= $1
AND   (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time) FROM wait_stats WHERE collection_time >= $1 GROUP BY server_id
)";

    public const string FileIoStatsSeedSql = @"
SELECT server_id, database_name, file_name,
       num_of_reads, num_of_writes, read_bytes, write_bytes,
       io_stall_read_ms, io_stall_write_ms,
       io_stall_queued_read_ms, io_stall_queued_write_ms,
       collection_time
FROM file_io_stats
WHERE collection_time >= $1
AND   (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time) FROM file_io_stats WHERE collection_time >= $1 GROUP BY server_id
)";

    public const string PerfmonStatsSeedSql = @"
SELECT server_id, object_name, counter_name, instance_name, cntr_value, collection_time
FROM perfmon_stats
WHERE collection_time >= $1
AND   (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time) FROM perfmon_stats WHERE collection_time >= $1 GROUP BY server_id
)";

    /* This one also SELECTs collection_time, which the other three always did. Without it the
       memory-grant baselines seeded with a null timestamp, which disarms the gap policy for exactly
       the two counters where a stale baseline shows as a fabricated spike (grant timeouts and forced
       grants are monotonic). Inside the bounded window the row is fresh anyway; carrying the
       timestamp is what makes that a guarantee instead of an assumption. */
    public const string MemoryGrantStatsSeedSql = @"
SELECT server_id, pool_id, resource_semaphore_id, timeout_error_count, forced_grant_count, collection_time
FROM memory_grant_stats
WHERE collection_time >= $1
AND   (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time) FROM memory_grant_stats WHERE collection_time >= $1 GROUP BY server_id
)";

    /* #3540 A4: the six families below were never seeded. Column lists and key formats mirror each
       collector's own CalculateDelta* call exactly — a key that differs by one character seeds a
       baseline nothing will ever read. LatchStatsCollector / SpinlockStatsCollector key on the class
       / spinlock name and write every row every pass, so they take the latest-collection shape. */

    public const string LatchStatsSeedSql = @"
SELECT server_id, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms, collection_time
FROM latch_stats
WHERE collection_time >= $1
AND   (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time) FROM latch_stats WHERE collection_time >= $1 GROUP BY server_id
)";

    public const string SpinlockStatsSeedSql = @"
SELECT server_id, spinlock_name, collisions, spins, sleep_time, backoffs, collection_time
FROM spinlock_stats
WHERE collection_time >= $1
AND   (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time) FROM spinlock_stats WHERE collection_time >= $1 GROUP BY server_id
)";

    /* ProcedureStatsCollector keys on plan_handle, falling back to database.schema.object when the
       handle is null; the SQL builds the SAME string (a null part formats as empty, as C# string
       interpolation does) so DISTINCT ON partitions by the key the collector will present. Latest row
       per key, because a TOP (150) drops and readmits plans between passes. */
    public const string ProcedureStatsSeedSql = @"
SELECT DISTINCT ON (server_id, delta_key)
       server_id, delta_key,
       execution_count, total_worker_time, total_elapsed_time,
       total_logical_reads, total_logical_writes, total_physical_reads, total_spills,
       collection_time
FROM (
    SELECT server_id,
           COALESCE(plan_handle, COALESCE(database_name, '') || '.' || COALESCE(schema_name, '') || '.' || COALESCE(object_name, '')) AS delta_key,
           execution_count, total_worker_time, total_elapsed_time,
           total_logical_reads, total_logical_writes, total_physical_reads, total_spills,
           collection_time
    FROM procedure_stats
    WHERE collection_time >= $1
) AS recent
ORDER BY server_id, delta_key, collection_time DESC";

    /* QueryStatsCollector keys on the dm_exec_query_stats row identity — sql_handle, both statement
       offsets, plan_handle — and its TOP (n) churns like procedure_stats', so: latest row per that
       identity. DISTINCT ON treats NULL offsets as one group, which is harmless: those are pre-V128 rows
       the seeder reads for the pass window only (see the header). The eight counters are the ones the
       collector's eight CalculateDeltaWithSeriesAge calls difference. */
    public const string QueryStatsSeedSql = @"
SELECT DISTINCT ON (server_id, sql_handle, statement_start_offset, statement_end_offset, plan_handle)
       server_id, sql_handle, statement_start_offset, statement_end_offset, plan_handle,
       execution_count, total_worker_time, total_elapsed_time,
       total_logical_reads, total_logical_writes, total_physical_reads, total_rows, total_spills,
       collection_time
FROM query_stats
WHERE collection_time >= $1
ORDER BY server_id, sql_handle, statement_start_offset, statement_end_offset, plan_handle, collection_time DESC";

    /* PgWaitStatsCollector keys on the numeric wait_event_id (never the name — it changes case across
       Aurora majors) and skips idle rows at the write, so: latest row per (server, event id). */
    public const string PgWaitStatsSeedSql = @"
SELECT DISTINCT ON (server_id, wait_event_id)
       server_id, wait_event_id, waits, wait_time_us, collection_time
FROM pg_wait_stats
WHERE collection_time >= $1
ORDER BY server_id, wait_event_id, collection_time DESC";

    /* PgStatementStatsCollector keys on the full pg_stat_statements identity
       (queryid, database_id, user_id, toplevel) and skips idle rows at the write, so: latest row per
       that identity. database_id is the datid OID the collector writes today (#3540 A11c names the
       DROP/CREATE reuse trap in that choice); the seed reproduces the key as written, it does not
       redesign it. total_exec_time_ms is a double in the store and the collector deltas it as a
       truncated long — the reader below truncates the same way. */
    public const string PgStatementStatsSeedSql = @"
SELECT DISTINCT ON (server_id, queryid, database_id, user_id, toplevel)
       server_id, queryid, database_id, user_id, toplevel,
       calls, total_exec_time_ms, rows_returned, collection_time
FROM pg_statement_stats
WHERE collection_time >= $1
ORDER BY server_id, queryid, database_id, user_id, toplevel, collection_time DESC";

    /* The delta GROUP names each family's collector passes — the pass window is keyed by these, not
       by the collector name, so a seeder that seeded "query_stats" would arm nothing. Spelled once per
       family here and read by the seeders below; the census test compares them against the
       collectors' own call sites. */
    internal static readonly string[] WaitStatsGroups = { "wait_stats_tasks", "wait_stats_time", "wait_stats_signal" };
    internal static readonly string[] FileIoStatsGroups =
    {
        "file_io_reads", "file_io_writes", "file_io_read_bytes", "file_io_write_bytes",
        "file_io_stall_read", "file_io_stall_write", "file_io_stall_queued_read", "file_io_stall_queued_write",
    };
    internal static readonly string[] PerfmonStatsGroups = { "perfmon" };
    internal static readonly string[] MemoryGrantStatsGroups = { "memory_grants_timeouts", "memory_grants_forced" };
    internal static readonly string[] LatchStatsGroups = { "latch_stats_waiting_requests", "latch_stats_wait_time", "latch_stats_max_wait" };
    internal static readonly string[] SpinlockStatsGroups = { "spinlock_stats_collisions", "spinlock_stats_spins", "spinlock_stats_sleep_time", "spinlock_stats_backoffs" };
    internal static readonly string[] ProcedureStatsGroups =
    {
        "proc_stats_exec", "proc_stats_worker", "proc_stats_elapsed", "proc_stats_reads",
        "proc_stats_writes", "proc_stats_phys_reads", "proc_stats_spills",
    };
    internal static readonly string[] QueryStatsGroups =
    {
        "query_stats_exec", "query_stats_worker", "query_stats_elapsed", "query_stats_reads",
        "query_stats_writes", "query_stats_phys_reads", "query_stats_rows", "query_stats_spills",
    };
    internal static readonly string[] PgWaitStatsGroups = { "pg_wait_stats_waits", "pg_wait_stats_time" };
    internal static readonly string[] PgStatementStatsGroups = { "pg_statement_stats_calls", "pg_statement_stats_time", "pg_statement_stats_rows" };

    /// <summary>
    /// Seeds the delta cache from the Postgres store so that the first collection after a service
    /// restart can produce accurate deltas instead of returning 0 for everything. A seed failure
    /// logs a warning and the service proceeds with first-cycle-zero deltas — restart continuity
    /// must never block collection.
    ///
    /// <para>Each family seeds under its own guard. Before #3540 one connection-level try wrapped four
    /// reads in series, so the first family to throw cost every later family its continuity, silently;
    /// at eleven reads that posture is worse, and a family whose read is slow on one store is exactly
    /// the case where the other ten still have their rows to give. A family that fails logs a warning
    /// naming itself and the seed moves on. Every command carries the bootstrap deadline: these run
    /// once, awaited, ahead of the collection loop, and the startup deadline census pins each one.</para>
    /// </summary>
    public async Task SeedFromStoreAsync(NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

            /* One cutoff for all reads, so they describe the same instant. */
            var cutoff = SeedCutoff();

            await SeedFamilyAsync("wait_stats", () => SeedWaitStatsAsync(connection, cutoff, logger, cancellationToken), logger);
            await SeedFamilyAsync("file_io_stats", () => SeedFileIoStatsAsync(connection, cutoff, logger, cancellationToken), logger);
            await SeedFamilyAsync("perfmon_stats", () => SeedPerfmonStatsAsync(connection, cutoff, logger, cancellationToken), logger);
            await SeedFamilyAsync("memory_grant_stats", () => SeedMemoryGrantStatsAsync(connection, cutoff, logger, cancellationToken), logger);
            await SeedFamilyAsync("latch_stats", () => SeedLatchStatsAsync(connection, cutoff, logger, cancellationToken), logger);
            await SeedFamilyAsync("spinlock_stats", () => SeedSpinlockStatsAsync(connection, cutoff, logger, cancellationToken), logger);
            await SeedFamilyAsync("procedure_stats", () => SeedProcedureStatsAsync(connection, cutoff, logger, cancellationToken), logger);
            await SeedFamilyAsync("query_stats", () => SeedQueryStatsAsync(connection, cutoff, logger, cancellationToken), logger);
            await SeedFamilyAsync("pg_wait_stats", () => SeedPgWaitStatsAsync(connection, cutoff, logger, cancellationToken), logger);
            await SeedFamilyAsync("pg_statement_stats", () => SeedPgStatementStatsAsync(connection, cutoff, logger, cancellationToken), logger);

            logger?.LogInformation(
                "Delta calculator seeded from Postgres store (baselines from the last {Minutes} minutes)",
                (int)SeedLookback.TotalMinutes);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "Failed to seed delta calculator from Postgres store, first collection will return 0 deltas");
        }
    }

    private static async Task SeedFamilyAsync(string family, Func<Task> seed, ILogger? logger)
    {
        try
        {
            await seed();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "Failed to seed {Family} delta baselines from Postgres store, its first collection will return 0 deltas", family);
        }
    }

    private async Task SeedWaitStatsAsync(NpgsqlConnection connection, DateTime cutoff, ILogger? logger, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(WaitStatsSeedSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        cmd.Parameters.AddWithValue(cutoff);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            var waitType = reader.GetString(1);
            /* Naive-UTC storage by convention across the product — read as-is, never convert. */
            var ts = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
            Seed(serverId, "wait_stats_tasks", waitType, reader.GetInt64(2), ts);
            Seed(serverId, "wait_stats_time", waitType, reader.GetInt64(3), ts);
            Seed(serverId, "wait_stats_signal", waitType, reader.GetInt64(4), ts);
            passes.Observe(serverId, ts);
            count++;
        }
        SeedPasses(passes, WaitStatsGroups);
        if (count > 0) logger?.LogDebug("Seeded {Count} wait_stats baseline rows", count);
    }

    private async Task SeedFileIoStatsAsync(NpgsqlConnection connection, DateTime cutoff, ILogger? logger, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(FileIoStatsSeedSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        cmd.Parameters.AddWithValue(cutoff);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            var dbName = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var fileName = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var deltaKey = $"{dbName}|{fileName}";
            var ts = reader.IsDBNull(11) ? (DateTime?)null : reader.GetDateTime(11);
            Seed(serverId, "file_io_reads", deltaKey, reader.IsDBNull(3) ? 0 : reader.GetInt64(3), ts);
            Seed(serverId, "file_io_writes", deltaKey, reader.IsDBNull(4) ? 0 : reader.GetInt64(4), ts);
            Seed(serverId, "file_io_read_bytes", deltaKey, reader.IsDBNull(5) ? 0 : reader.GetInt64(5), ts);
            Seed(serverId, "file_io_write_bytes", deltaKey, reader.IsDBNull(6) ? 0 : reader.GetInt64(6), ts);
            Seed(serverId, "file_io_stall_read", deltaKey, reader.IsDBNull(7) ? 0 : reader.GetInt64(7), ts);
            Seed(serverId, "file_io_stall_write", deltaKey, reader.IsDBNull(8) ? 0 : reader.GetInt64(8), ts);
            Seed(serverId, "file_io_stall_queued_read", deltaKey, reader.IsDBNull(9) ? 0 : reader.GetInt64(9), ts);
            Seed(serverId, "file_io_stall_queued_write", deltaKey, reader.IsDBNull(10) ? 0 : reader.GetInt64(10), ts);
            passes.Observe(serverId, ts);
            count++;
        }
        SeedPasses(passes, FileIoStatsGroups);
        if (count > 0) logger?.LogDebug("Seeded {Count} file_io_stats baseline rows", count);
    }

    private async Task SeedPerfmonStatsAsync(NpgsqlConnection connection, DateTime cutoff, ILogger? logger, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(PerfmonStatsSeedSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        cmd.Parameters.AddWithValue(cutoff);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            var objectName = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var counter = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var instance = reader.IsDBNull(3) ? "" : reader.GetString(3);
            var ts = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
            Seed(serverId, "perfmon", $"{objectName}|{counter}|{instance}", reader.GetInt64(4), ts);
            passes.Observe(serverId, ts);
            count++;
        }
        SeedPasses(passes, PerfmonStatsGroups);
        if (count > 0) logger?.LogDebug("Seeded {Count} perfmon_stats baseline rows", count);
    }

    private async Task SeedMemoryGrantStatsAsync(NpgsqlConnection connection, DateTime cutoff, ILogger? logger, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(MemoryGrantStatsSeedSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        cmd.Parameters.AddWithValue(cutoff);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            var poolId = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            var semaphoreId = reader.IsDBNull(2) ? (short)0 : reader.GetInt16(2);
            var deltaKey = $"{poolId}_{semaphoreId}";
            var ts = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
            Seed(serverId, "memory_grants_timeouts", deltaKey, reader.IsDBNull(3) ? 0 : reader.GetInt64(3), ts);
            Seed(serverId, "memory_grants_forced", deltaKey, reader.IsDBNull(4) ? 0 : reader.GetInt64(4), ts);
            passes.Observe(serverId, ts);
            count++;
        }
        SeedPasses(passes, MemoryGrantStatsGroups);
        if (count > 0) logger?.LogDebug("Seeded {Count} memory_grant_stats baseline rows", count);
    }

    private async Task SeedLatchStatsAsync(NpgsqlConnection connection, DateTime cutoff, ILogger? logger, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(LatchStatsSeedSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        cmd.Parameters.AddWithValue(cutoff);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            var latchClass = reader.GetString(1);
            var ts = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
            Seed(serverId, "latch_stats_waiting_requests", latchClass, reader.IsDBNull(2) ? 0 : reader.GetInt64(2), ts);
            Seed(serverId, "latch_stats_wait_time", latchClass, reader.IsDBNull(3) ? 0 : reader.GetInt64(3), ts);
            Seed(serverId, "latch_stats_max_wait", latchClass, reader.IsDBNull(4) ? 0 : reader.GetInt64(4), ts);
            passes.Observe(serverId, ts);
            count++;
        }
        SeedPasses(passes, LatchStatsGroups);
        if (count > 0) logger?.LogDebug("Seeded {Count} latch_stats baseline rows", count);
    }

    private async Task SeedSpinlockStatsAsync(NpgsqlConnection connection, DateTime cutoff, ILogger? logger, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(SpinlockStatsSeedSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        cmd.Parameters.AddWithValue(cutoff);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            var spinlockName = reader.GetString(1);
            var ts = reader.IsDBNull(6) ? (DateTime?)null : reader.GetDateTime(6);
            Seed(serverId, "spinlock_stats_collisions", spinlockName, reader.IsDBNull(2) ? 0 : reader.GetInt64(2), ts);
            Seed(serverId, "spinlock_stats_spins", spinlockName, reader.IsDBNull(3) ? 0 : reader.GetInt64(3), ts);
            Seed(serverId, "spinlock_stats_sleep_time", spinlockName, reader.IsDBNull(4) ? 0 : reader.GetInt64(4), ts);
            Seed(serverId, "spinlock_stats_backoffs", spinlockName, reader.IsDBNull(5) ? 0 : reader.GetInt64(5), ts);
            passes.Observe(serverId, ts);
            count++;
        }
        SeedPasses(passes, SpinlockStatsGroups);
        if (count > 0) logger?.LogDebug("Seeded {Count} spinlock_stats baseline rows", count);
    }

    private async Task SeedProcedureStatsAsync(NpgsqlConnection connection, DateTime cutoff, ILogger? logger, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(ProcedureStatsSeedSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        cmd.Parameters.AddWithValue(cutoff);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            /* The key exactly as ProcedureStatsCollector.WritePayload builds it — computed in the SQL so
               DISTINCT ON partitions by it; read back verbatim. */
            var deltaKey = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var ts = reader.IsDBNull(9) ? (DateTime?)null : reader.GetDateTime(9);
            Seed(serverId, "proc_stats_exec", deltaKey, reader.IsDBNull(2) ? 0 : reader.GetInt64(2), ts);
            Seed(serverId, "proc_stats_worker", deltaKey, reader.IsDBNull(3) ? 0 : reader.GetInt64(3), ts);
            Seed(serverId, "proc_stats_elapsed", deltaKey, reader.IsDBNull(4) ? 0 : reader.GetInt64(4), ts);
            Seed(serverId, "proc_stats_reads", deltaKey, reader.IsDBNull(5) ? 0 : reader.GetInt64(5), ts);
            Seed(serverId, "proc_stats_writes", deltaKey, reader.IsDBNull(6) ? 0 : reader.GetInt64(6), ts);
            Seed(serverId, "proc_stats_phys_reads", deltaKey, reader.IsDBNull(7) ? 0 : reader.GetInt64(7), ts);
            Seed(serverId, "proc_stats_spills", deltaKey, reader.IsDBNull(8) ? 0 : reader.GetInt64(8), ts);
            passes.Observe(serverId, ts);
            count++;
        }
        SeedPasses(passes, ProcedureStatsGroups);
        if (count > 0) logger?.LogDebug("Seeded {Count} procedure_stats baseline rows", count);
    }

    private async Task SeedQueryStatsAsync(NpgsqlConnection connection, DateTime cutoff, ILogger? logger, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(QueryStatsSeedSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        cmd.Parameters.AddWithValue(cutoff);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var count = 0;
        var preV128 = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            var ts = reader.IsDBNull(13) ? (DateTime?)null : reader.GetDateTime(13);

            /* The pass window takes EVERY row, offsets or not — see the header. */
            passes.Observe(serverId, ts);

            /* A pre-V128 row never recorded its offsets. Its key cannot be rebuilt, and a key built from a
               guessed pair would be a baseline nothing ever reads — so it seeds nothing. */
            if (reader.IsDBNull(2) || reader.IsDBNull(3))
            {
                preV128++;
                continue;
            }

            /* The key exactly as QueryStatsCollector.WritePayload spells it: the same interpolation over
               the same raw parts, so a null handle formats as empty and the offsets — -1 included — are
               spelled by the same int formatting on both sides. Never normalized. */
            var sqlHandle = reader.IsDBNull(1) ? null : reader.GetString(1);
            var planHandle = reader.IsDBNull(4) ? null : reader.GetString(4);
            var deltaKey = $"{sqlHandle}:{reader.GetInt32(2)}:{reader.GetInt32(3)}:{planHandle}";

            Seed(serverId, "query_stats_exec", deltaKey, reader.IsDBNull(5) ? 0 : reader.GetInt64(5), ts);
            Seed(serverId, "query_stats_worker", deltaKey, reader.IsDBNull(6) ? 0 : reader.GetInt64(6), ts);
            Seed(serverId, "query_stats_elapsed", deltaKey, reader.IsDBNull(7) ? 0 : reader.GetInt64(7), ts);
            Seed(serverId, "query_stats_reads", deltaKey, reader.IsDBNull(8) ? 0 : reader.GetInt64(8), ts);
            Seed(serverId, "query_stats_writes", deltaKey, reader.IsDBNull(9) ? 0 : reader.GetInt64(9), ts);
            Seed(serverId, "query_stats_phys_reads", deltaKey, reader.IsDBNull(10) ? 0 : reader.GetInt64(10), ts);
            Seed(serverId, "query_stats_rows", deltaKey, reader.IsDBNull(11) ? 0 : reader.GetInt64(11), ts);
            Seed(serverId, "query_stats_spills", deltaKey, reader.IsDBNull(12) ? 0 : reader.GetInt64(12), ts);
            count++;
        }
        SeedPasses(passes, QueryStatsGroups);
        if (count > 0) logger?.LogDebug("Seeded {Count} query_stats baseline rows", count);
        if (preV128 > 0) logger?.LogDebug("Skipped {Count} query_stats rows with no stored statement offsets (pre-V128); their collection times still seeded the pass window", preV128);
    }

    private async Task SeedPgWaitStatsAsync(NpgsqlConnection connection, DateTime cutoff, ILogger? logger, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(PgWaitStatsSeedSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        cmd.Parameters.AddWithValue(cutoff);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            /* The key exactly as PgWaitStatsCollector builds it: the event id, invariant-formatted. */
            var key = reader.GetInt64(1).ToString(CultureInfo.InvariantCulture);
            var ts = reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4);
            Seed(serverId, "pg_wait_stats_waits", key, reader.IsDBNull(2) ? 0 : reader.GetInt64(2), ts);
            Seed(serverId, "pg_wait_stats_time", key, reader.IsDBNull(3) ? 0 : reader.GetInt64(3), ts);
            passes.Observe(serverId, ts);
            count++;
        }
        SeedPasses(passes, PgWaitStatsGroups);
        if (count > 0) logger?.LogDebug("Seeded {Count} pg_wait_stats baseline rows", count);
    }

    private async Task SeedPgStatementStatsAsync(NpgsqlConnection connection, DateTime cutoff, ILogger? logger, CancellationToken cancellationToken)
    {
        using var cmd = new NpgsqlCommand(PgStatementStatsSeedSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        cmd.Parameters.AddWithValue(cutoff);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            /* The key exactly as PgStatementStatsCollector.ReadAsync builds it. */
            var key = string.Create(CultureInfo.InvariantCulture,
                $"{reader.GetInt64(1)}|{reader.GetInt64(2)}|{reader.GetInt64(3)}|{(!reader.IsDBNull(4) && reader.GetBoolean(4) ? 1 : 0)}");
            var ts = reader.IsDBNull(8) ? (DateTime?)null : reader.GetDateTime(8);
            Seed(serverId, "pg_statement_stats_calls", key, reader.IsDBNull(5) ? 0 : reader.GetInt64(5), ts);
            /* (long) of the stored double — the collector's own truncation. */
            Seed(serverId, "pg_statement_stats_time", key, reader.IsDBNull(6) ? 0 : (long)reader.GetDouble(6), ts);
            Seed(serverId, "pg_statement_stats_rows", key, reader.IsDBNull(7) ? 0 : reader.GetInt64(7), ts);
            passes.Observe(serverId, ts);
            count++;
        }
        SeedPasses(passes, PgStatementStatsGroups);
        if (count > 0) logger?.LogDebug("Seeded {Count} pg_statement_stats baseline rows", count);
    }
}
