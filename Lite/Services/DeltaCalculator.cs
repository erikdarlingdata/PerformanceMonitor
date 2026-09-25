/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Lite's delta calculator: the shared <see cref="CollectorDeltaCalculator"/> core (baseline /
/// counter-reset / gap-policy semantics live there, identical across SKUs) plus the DuckDB
/// seeding that lets the first collection after an app restart produce accurate deltas instead
/// of returning 0 for everything — for EVERY delta family Lite monitors, keys and pass window both
/// (#3540 A4), so the restart contract in <see cref="CollectorDeltaCalculator"/>'s remarks holds here.
/// </summary>
public class DeltaCalculator : CollectorDeltaCalculator
{
    /* The seed queries, mirrored verbatim in Darling's DarlingDeltaCalculator — deliberately written
       in the shared dialect (the (server_id, collection_time) row-value latest-row form, DISTINCT ON
       for the latest-row-per-key form, and the reused $1 positional placeholder, all of which run on
       either engine as-is). Held as constants so LiteDeltaSeederTests can pin the same shape
       DarlingDeltaSeederTests pins on the other side, and so Lite.Tests' DeltaFamilySeedingCensusTests
       can read both hosts' sources and prove every family has a seeder on each.

       $1 is CollectorDeltaCalculator.SeedCutoff(), bound on BOTH the outer read and the inner MAX():
       either one left unbounded reads the whole table. #1772 was the Postgres half of that on a
       276 GB store, where the unbounded form timed out and restart continuity silently degraded to
       first-cycle-zero deltas; Lite carries the bound too because the seed is one shared design and a
       long-lived local store grows the same shape of scan.

       Two shapes, chosen per family by how the collector WRITES (#3540 A4):

       - Latest collection per server (the original four, latch_stats, spinlock_stats): these
         collectors write every key they read on every pass, so the newest collection holds every
         key's current counter and the row-value probe is the cheapest exact read.
       - Latest row per key (procedure_stats and query_stats here; the PostgreSQL pair on Darling):
         these collectors do NOT write every key every pass — procedure_stats and query_stats are
         TOP (n) reads that churn, and the PostgreSQL collectors skip idle rows at the write — so the
         newest collection is missing keys whose counters are nevertheless unchanged, and a key seeded
         from nothing takes the first-sighting path. DISTINCT ON (server_id, key) ... ORDER BY
         collection_time DESC over the
         cutoff window returns each key's newest row instead. Its bound is the single
         collection_time >= $1 on its only table read: there is no inner aggregate to bind a second
         time, and the window's rows are the whole working set (one sort over fifteen minutes of one
         family). A key seeded from an older row inside the window carries that row's timestamp, so
         its first delta spans a longer interval than the in-memory cache would have measured; the
         value is exact (the counter was idle in between, which is why no newer row exists) and the
         gap policy still bounds the span.

       query_stats joined the per-key shape with Lite v61 / Darling V128 (#3540). Its delta key is
       sql_handle:statement_start_offset:statement_end_offset:plan_handle, and until v61 the store
       persisted neither offset, so no row could reproduce the key the collector presents and only the
       family's PASS WINDOW could be seeded (the #2235 series-age rescue's input). The offsets are
       stored now, raw (-1 = "to the end of the batch", byte offsets into the nvarchar batch text) and
       the seed rebuilds the key from them with the collector's own interpolation. Two rules, both in
       the seeder rather than the SQL: a row whose offsets are NULL — every row written before v61 —
       seeds NO key, because a key built from a fabricated 0/-1 would be one nothing ever presents and
       the baseline under it would sit unread until it aged out; and EVERY row, NULL offsets or not,
       still feeds the pass window, so the first restart after the upgrade (when the whole window is
       pre-v61 rows) keeps the rescue armed exactly as #3614 left it. One read serves both, which is
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

    /* Also SELECTs collection_time, which the other three always did. Without it the memory-grant
       baselines seeded with a null timestamp, which disarms the gap policy for exactly the two
       counters where a stale baseline shows as a fabricated spike (grant timeouts and forced grants
       are monotonic). Inside the bounded window the row is fresh anyway; carrying the timestamp is
       what makes that a guarantee instead of an assumption. */
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
       identity. DISTINCT ON treats NULL offsets as one group, which is harmless: those are pre-v61 rows
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

    private readonly ILogger? _logger;

    public DeltaCalculator(ILogger? logger = null)
    {
        _logger = logger;
    }

    /// <summary>
    /// Seeds the delta cache from DuckDB so that the first collection after restart
    /// can produce accurate deltas instead of returning 0 for everything.
    ///
    /// <para>Each family seeds under its own guard. Before #3540 one connection-level try wrapped four
    /// reads in series, so the first family to throw cost every later family its continuity, silently;
    /// at nine reads that posture is worse, and a family whose table is missing or slow is exactly the
    /// case where the other eight still have their rows to give. A family that fails logs a warning
    /// naming itself and the seed moves on.</para>
    /// </summary>
    public async Task SeedFromDatabaseAsync(DuckDbInitializer duckDb)
    {
        try
        {
            /* Read lock (#4343): every family below is a plain SELECT (DuckDbInitializer's own rule — an
               ordinary read only needs "the file must not be reorganized under me"), but with the sentinel
               live a connection opened with no lock at all can attach to an instance ResetDatabaseAsync is
               tearing down mid-reset, rather than merely reading stale data as before #4262. */
            using var readLock = duckDb.AcquireReadLock();
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            /* One cutoff for all reads, so they describe the same instant. */
            var cutoff = SeedCutoff();

            await SeedFamilyAsync("wait_stats", () => SeedWaitStatsAsync(connection, cutoff));
            await SeedFamilyAsync("file_io_stats", () => SeedFileIoStatsAsync(connection, cutoff));
            await SeedFamilyAsync("perfmon_stats", () => SeedPerfmonStatsAsync(connection, cutoff));
            await SeedFamilyAsync("memory_grant_stats", () => SeedMemoryGrantStatsAsync(connection, cutoff));
            await SeedFamilyAsync("latch_stats", () => SeedLatchStatsAsync(connection, cutoff));
            await SeedFamilyAsync("spinlock_stats", () => SeedSpinlockStatsAsync(connection, cutoff));
            await SeedFamilyAsync("procedure_stats", () => SeedProcedureStatsAsync(connection, cutoff));
            await SeedFamilyAsync("query_stats", () => SeedQueryStatsAsync(connection, cutoff));

            _logger?.LogInformation(
                "Delta calculator seeded from database (baselines from the last {Minutes} minutes)",
                (int)SeedLookback.TotalMinutes);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to seed delta calculator from database, first collection will return 0 deltas");
        }
    }

    private async Task SeedFamilyAsync(string family, Func<Task> seed)
    {
        try
        {
            await seed();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to seed {Family} delta baselines from database, its first collection will return 0 deltas", family);
        }
    }

    private async Task SeedWaitStatsAsync(DuckDBConnection connection, DateTime cutoff)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = WaitStatsSeedSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync())
        {
            var serverId = reader.GetInt32(0);
            var waitType = reader.GetString(1);
            var ts = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
            Seed(serverId, "wait_stats_tasks", waitType, reader.GetInt64(2), ts);
            Seed(serverId, "wait_stats_time", waitType, reader.GetInt64(3), ts);
            Seed(serverId, "wait_stats_signal", waitType, reader.GetInt64(4), ts);
            passes.Observe(serverId, ts);
            count++;
        }
        SeedPasses(passes, WaitStatsGroups);
        if (count > 0) _logger?.LogDebug("Seeded {Count} wait_stats baseline rows", count);
    }

    private async Task SeedFileIoStatsAsync(DuckDBConnection connection, DateTime cutoff)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = FileIoStatsSeedSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync())
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
        if (count > 0) _logger?.LogDebug("Seeded {Count} file_io_stats baseline rows", count);
    }

    private async Task SeedPerfmonStatsAsync(DuckDBConnection connection, DateTime cutoff)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = PerfmonStatsSeedSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync())
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
        if (count > 0) _logger?.LogDebug("Seeded {Count} perfmon_stats baseline rows", count);
    }

    private async Task SeedMemoryGrantStatsAsync(DuckDBConnection connection, DateTime cutoff)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = MemoryGrantStatsSeedSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync())
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
        if (count > 0) _logger?.LogDebug("Seeded {Count} memory_grant_stats baseline rows", count);
    }

    private async Task SeedLatchStatsAsync(DuckDBConnection connection, DateTime cutoff)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = LatchStatsSeedSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync())
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
        if (count > 0) _logger?.LogDebug("Seeded {Count} latch_stats baseline rows", count);
    }

    private async Task SeedSpinlockStatsAsync(DuckDBConnection connection, DateTime cutoff)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = SpinlockStatsSeedSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync())
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
        if (count > 0) _logger?.LogDebug("Seeded {Count} spinlock_stats baseline rows", count);
    }

    private async Task SeedProcedureStatsAsync(DuckDBConnection connection, DateTime cutoff)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = ProcedureStatsSeedSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync())
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
        if (count > 0) _logger?.LogDebug("Seeded {Count} procedure_stats baseline rows", count);
    }

    private async Task SeedQueryStatsAsync(DuckDBConnection connection, DateTime cutoff)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = QueryStatsSeedSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        var preV61 = 0;
        var passes = new SeedPassTracker();
        while (await reader.ReadAsync())
        {
            var serverId = reader.GetInt32(0);
            var ts = reader.IsDBNull(13) ? (DateTime?)null : reader.GetDateTime(13);

            /* The pass window takes EVERY row, offsets or not — see the header. */
            passes.Observe(serverId, ts);

            /* A pre-v61 row never recorded its offsets. Its key cannot be rebuilt, and a key built from a
               guessed pair would be a baseline nothing ever reads — so it seeds nothing. */
            if (reader.IsDBNull(2) || reader.IsDBNull(3))
            {
                preV61++;
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
        if (count > 0) _logger?.LogDebug("Seeded {Count} query_stats baseline rows", count);
        if (preV61 > 0) _logger?.LogDebug("Skipped {Count} query_stats rows with no stored statement offsets (pre-v61); their collection times still seeded the pass window", preV61);
    }
}
