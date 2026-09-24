/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 (A8, option B): the silent-detector guard. Every anomaly detector wraps each family in its own
/// <c>catch (Exception ex) when (...) { _logger?.LogError(...); }</c> — deliberate isolation (one metric's
/// failure costs the pass that metric, never the whole run) that has a dark side: a SQL error or a missing
/// parameter bind makes a family SILENTLY dead. No fact, no thrown exception, no failed assertion — the
/// only trace is a log line nothing reads unless something goes looking. During #3653 B this happened FOUR
/// times (three unbound <c>$4..$6</c>, one <c>42803 subquery uses ungrouped column</c>), and each was found
/// only by a hand-run live class. This guard runs every family over minimally seeded tables — one row each,
/// enough for every <c>*WindowSql</c>/<c>*TileWindowSql</c> const's FROM clause to actually execute — and
/// fails if the capturing logger recorded ANY <see cref="Microsoft.Extensions.Logging.LogLevel.Error"/> (or
/// above) line. A binding or compile error surfaces over an EMPTY window; it needs no anomalous data, only
/// data enough for the SQL to run.
///
/// <para>Runs both engines side by side: <see cref="PgAnomalyDetector"/> (the SQL Server store) and
/// <see cref="PgTargetAnomalyDetector"/> (the PostgreSQL-target pass) — a shared logger, a shared assertion.
/// The detector PRs landing after this one (#4170, #4171, #4172, #4177, #4178) rebase onto dev and so run
/// under this guard in CI once merged; a family with an unbound parameter or a bad column name in NEW tiled
/// SQL now fails this class instead of silently emitting nothing.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class AnomalyDetectorErrorGuardLiveTests
{
    /// <summary>Distinctive fake ids — a real server_id is a storage-name hash, never in this range.</summary>
    private const int SqlServerId = -365900;
    private const string SqlServerName = "a8-guard-sqlserver";
    private const int PgTargetServerId = -365901;
    private const string PgTargetServerName = "a8-guard-pgtarget";

    /* G1-4 (#3653 B): SampledWaitContribWindowSql only runs when pg_wait_stats has NO rows for the server
       (the "only pg_wait_stats" exclusivity rule in DetectSampledWaitProfileAnomalies's summary) — a distinct
       fake server id carries pg_wait_sampling rows and nothing else. */
    private const int PgTargetSampledOnlyServerId = -365902;
    private const string PgTargetSampledOnlyServerName = "a8-guard-pgtarget-sampled";

    [Fact]
    public async Task NoAnomalyDetectorLogsAnErrorOverASeededWindow_SqlServerStore()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the detector error guard.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSqlServerRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, SqlServerId, SqlServerName, ct);
            await SeedSqlServerFamilyTablesAsync(connection, ct);
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var logger = new CapturingTestLogger();
            var provider = new PgBaselineProvider(postgres);
            var detector = new PgAnomalyDetector(postgres, provider, logger);

            var (_, commands) = await CommandCapture.CaptureAsync(
                () => RunBothWindowsAsync(detector, SqlServerId, SqlServerName));

            Assert.True(!logger.Joined.Contains("Error:", StringComparison.Ordinal),
                $"a SQL Server anomaly detector logged an error over a seeded window: {logger.Joined}");

            AssertEveryWindowSqlRan(typeof(PgAnomalyDetector), commands);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteSqlServerRowsAsync(cleanup, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task NoAnomalyDetectorLogsAnErrorOverASeededWindow_PgTarget()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the detector error guard.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeletePgTargetRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, PgTargetServerId, PgTargetServerName, "aurora-postgres", 17, ct);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, PgTargetSampledOnlyServerId, PgTargetSampledOnlyServerName, "postgres", 18, ct);
            await SeedPgTargetFamilyTablesAsync(connection, ct);

            var logger = new CapturingTestLogger();
            var provider = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, provider, logger);

            var (_, commands) = await CommandCapture.CaptureAsync(async () =>
            {
                await RunBothWindowsAsync(detector, PgTargetServerId, PgTargetServerName);
                /* SampledWaitContribWindowSql only runs for a server with pg_wait_sampling rows and NO
                   pg_wait_stats rows (class summary of DetectSampledWaitProfileAnomalies) — the sampled-only
                   fake server above, run in the SAME capture so its command text joins the main assertion. */
                await RunBothWindowsAsync(detector, PgTargetSampledOnlyServerId, PgTargetSampledOnlyServerName);
                return true;
            });

            Assert.True(!logger.Joined.Contains("Error:", StringComparison.Ordinal),
                $"a PostgreSQL-target anomaly detector logged an error over a seeded window: {logger.Joined}");

            AssertEveryWindowSqlRan(typeof(PgTargetAnomalyDetector), commands);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeletePgTargetRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>
    /// Non-vacuous by construction: every <c>public const string</c> field on <paramref name="detectorType"/>
    /// whose name ends in <c>WindowSql</c> (and, since detectors are declared across partial-class files, on
    /// EVERY type sharing that name — reflection over one <c>Type</c> handle already covers every partial
    /// piece, since they compile to one type) must appear, VERBATIM, among the SQL text this pass's Npgsql
    /// activities captured. A const that never ran means its detector arm returned before reaching its
    /// window read — the exact silent-family failure mode this guard exists to catch (baseline gated on
    /// <c>SampleCount == 0</c>, or <c>HasBaselineDataAsync</c> gating the whole pass). Excluded:
    /// <c>PgAnomalyDetector.HasBaselineDataSql</c> and its PgTarget analog (the baseline-gate canary, not a
    /// window read — checked separately by the pass's very ability to reach ANY window SQL at all) — neither
    /// const's name ends in <c>WindowSql</c>, so the name filter already excludes them; documented here so a
    /// reviewer does not go looking for a missing exclusion list.
    /// </summary>
    private static void AssertEveryWindowSqlRan(Type detectorType, List<string> commands)
    {
        /* #4171 (the tiled-window recipe, #3653 A8 option B lane L3a) gave PgTargetAnomalyDetector's TPS,
           session, CPU and wait-rate arms tile twins (*TileWindowSql) that the detector now actually reads;
           the plain *WindowSql consts stay on the type ONLY so PgTargetAnomalyTests can keep pinning their
           shape (DatabaseCounterWindowSql still runs directly, for the deadlock-rate arm — not excluded).
           SCOPED to PgTargetAnomalyDetector only: PgAnomalyDetector's own SessionWindowSql/CpuWindowSql/
           WaitRateWindowSql (same names, SQL Server family) still run directly and must stay covered there.
           A NAMED exclusion, not a name-pattern one: each entry is the const superseded by its own tile twin. */
        var supersededByTile = detectorType == typeof(PgTargetAnomalyDetector)
            ? new HashSet<string> { "SessionWindowSql", "CpuWindowSql", "WaitRateWindowSql" }
            : new HashSet<string>();

        var windowSqlFields = detectorType
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.EndsWith("WindowSql", StringComparison.Ordinal))
            .Where(f => !supersededByTile.Contains(f.Name))
            .ToList();

        Assert.NotEmpty(windowSqlFields);

        var neverRan = new List<string>();
        foreach (var field in windowSqlFields)
        {
            var sql = (string)field.GetRawConstantValue()!;
            if (!commands.Any(c => c.Contains(sql, StringComparison.Ordinal)))
            {
                neverRan.Add(field.Name);
            }
        }

        Assert.True(neverRan.Count == 0,
            $"{detectorType.Name}: these *WindowSql consts never ran over the seeded window (their detector arm " +
            $"returned before reaching its window read): {string.Join(", ", neverRan)}");
    }

    /// <summary>
    /// Red-first, per lane-3653b-G1.md: temporarily corrupt <see cref="PgAnomalyDetector.CpuWindowSql"/> with a
    /// bad column name and confirm the guard names the failing family. Left as a comment rather than a permanent
    /// test (a standing corruption would break every other CPU-arm test): the transcript below is the recorded
    /// proof, taken during development of this class and reverted before commit.
    ///
    /// <para>Corruption applied: <c>SELECT MAX(sqlserver_cpu_utilization)</c> → <c>SELECT MAX(sqlserver_cpu_utilization_typo)</c>
    /// in <see cref="PgAnomalyDetector.CpuWindowSql"/>. Result: <c>NoAnomalyDetectorLogsAnErrorOverASeededWindow_SqlServerStore</c>
    /// FAILED with the logger's joined text containing
    /// <c>Error: [PgAnomalyDetector] CPU anomaly detection failed: 42703: column "sqlserver_cpu_utilization_typo" does not exist</c>
    /// — the guard bit on the corrupted family and named it in the assertion message, exactly as designed. The column
    /// name was then reverted and the test re-run green before commit.</para>
    /// </summary>
    private static void RedFirstProof_DocumentedNotAsserted()
    {
    }

    private static async Task<bool> RunBothWindowsAsync(IAnomalyDetector detector, int serverId, string serverName)
    {
        var now = DateTime.SpecifyKind(
            new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)),
            DateTimeKind.Unspecified);

        foreach (var window in new[] { TimeSpan.FromHours(4), TimeSpan.FromHours(24) })
        {
            var context = new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = now.Subtract(window),
                TimeRangeEnd = now,
                ServerUtcOffset = TimeSpan.Zero
            };
            /* DetectDeadlockRateAnomalies (and everything downstream of it) divides by ObservedDurationMs,
               which reads Coverage?.ObservedMs — null (0) until a fact collector stamps it. Without this the
               deadlock-rate arm returns before its window read ever executes, and DatabaseCounterWindowSql
               reads as "never ran" no matter how the tables are seeded. Stamp full coverage: this guard is
               about detector SQL reaching the store, not about the coverage machinery itself. */
            context.Coverage = new WindowCoverage { NominalMs = window.TotalMilliseconds, ObservedMs = window.TotalMilliseconds, SampleCount = 1 };
            await detector.DetectAnomaliesAsync(context);
        }

        return true;
    }

    /// <summary>
    /// One row in every table a SQL Server–store family's <c>*WindowSql</c> const reads (grepped from the
    /// consts' FROM clauses), plus the baseline gate's canary (wait_stats / cpu_utilization_stats — already
    /// covered below) and the <see cref="PgAnomalyDetector.HasBaselineDataAsync"/> 30-day window.
    /// </summary>
    /// <summary>History rows for the SQL Server baseline gate: 12 same-hour/same-day-of-week rows, one
    /// per week going back 12 weeks, plus the current-window row for each table a
    /// <c>*WindowSql</c> const reads. 12 clears <see cref="BaselineMath.CollapseThreshold"/> (10) for the
    /// Full (hour+dow) tier and <see cref="PgAnomalyDetector.HasBaselineDataAsync"/>'s 30-day canary —
    /// WITHOUT that density every detector's <c>baseline.SampleCount == 0</c> early-return fires BEFORE its
    /// window SQL ever runs, which is exactly how PR #4179's guard shipped vacuous.</summary>
    private static async Task SeedSqlServerFamilyTablesAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var now = DateTime.SpecifyKind(
            new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)),
            DateTimeKind.Unspecified);

        // Inside every 24h window this class runs.
        var t = now.AddHours(-1);
        // History rows: same hour-of-day/day-of-week as t, one per week for 12 weeks (all well inside the
        // 30-day gate is NOT required here — SelectBucket's Full tier has no recency requirement, only
        // sample density at the (hour, dow) key; HasBaselineDataAsync's 30-day canary is covered separately below).
        var historyTimes = Enumerable.Range(1, 12).Select(w => t.AddDays(-7 * w)).ToArray();

        foreach (var at in historyTimes.Append(t))
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                CollectionIdGenerator.Next(), at, SqlServerId, SqlServerName, "PAGEIOLATCH_SH", 1L, 100L, 10L, 1L, 100L, 10L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                CollectionIdGenerator.Next(), at, SqlServerId, SqlServerName, at, 10, 5);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count, total_cpu_time_ms, total_reads, total_writes, total_logical_reads) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                CollectionIdGenerator.Next(), at, SqlServerId, SqlServerName, "app", 5L, 3, 2, 0, 1000L, 100L, 10L, 500L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count, delta_elapsed_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                CollectionIdGenerator.Next(), at, SqlServerId, SqlServerName, "testdb", "hash1", 1L, 1000L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_server_memory_mb, target_server_memory_mb) VALUES ($1, $2, $3, $4, $5, $6)",
                CollectionIdGenerator.Next(), at, SqlServerId, SqlServerName, 5000m, 8000m);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                CollectionIdGenerator.Next(), at, SqlServerId, SqlServerName, "SQLServer:SQL Statistics", "Batch Requests/sec", "", 100L, 100L, 60);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb, num_of_reads, num_of_writes, read_bytes, write_bytes, io_stall_read_ms, io_stall_write_ms, io_stall_queued_read_ms, io_stall_queued_write_ms, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, delta_stall_queued_read_ms, delta_stall_queued_write_ms) " +
                "VALUES ($1, $2, $3, $4, 'testdb', 'testdb.mdf', 'ROWS', 'C:\\testdb.mdf', 100, 10, 5, 8192, 4096, 50, 25, 0, 0, 1, 1, 8192, 4096, 5, 5, 0, 0)",
                CollectionIdGenerator.Next(), at, SqlServerId, SqlServerName);
        }

        /* An extra current-window wait_stats row, one minute after t, with a spiked delta_wait_time_ms so the
           wait-profile detector's fallback bar (WaitProfileFallbackMsPerSec, 250 ms/sec) fires and
           WaitContribWindowSql actually runs — the flat per-week series above never exceeds it and leaves that
           const "never ran". */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), t.AddMinutes(1), SqlServerId, SqlServerName, "PAGEIOLATCH_SH", 1L, 30000L, 3000L, 1L, 30000L, 3000L);

        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_time_ms, wait_resource, lock_mode, blocked_status, blocked_isolation_level, blocked_log_used, blocked_transaction_count, blocked_client_app, blocked_host_name, blocked_login_name, blocked_sql_text, blocking_status, blocking_isolation_level, blocking_client_app, blocking_host_name, blocking_login_name, blocking_sql_text, blocked_transaction_name, blocking_transaction_name, blocked_last_tran_started, blocking_last_tran_started, blocked_last_batch_started, blocking_last_batch_started, blocked_last_batch_completed, blocking_last_batch_completed, blocked_priority, blocking_priority, blocked_process_report_xml, object_id, database_id, contentious_object, monitor_loop, blocked_query_plan_xml, blocking_query_plan_xml) " +
            "VALUES ($1, $2, $3, $4, $5, 'testdb', 1, 0, 2, 0, 100, 'KEY', 'X', 'suspended', 'READ COMMITTED', 0, 1, 'app', 'host', 'login', 'select 1', 'suspended', 'READ COMMITTED', 'app', 'host', 'login', 'select 2', NULL, NULL, $5, $5, $5, $5, $5, $5, 0, 0, '<x/>', 1, 1, 'obj', 1, NULL, NULL)",
            (long)(SqlServerId * -1), t, SqlServerId, SqlServerName, t);

        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name, blocked_spid, blocked_ecid, blocked_last_tran_started, blocking_spid, blocking_ecid, blocking_last_tran_started, wait_time_ms, lock_mode, blocking_status, contentious_object, blocked_sql_text, blocking_sql_text, blocked_login_name, blocked_host_name, blocked_client_app, blocking_login_name, blocking_host_name, blocking_client_app) " +
            "VALUES ($1, $2, $3, $4, 1, $2, 'testdb', 1, 0, $2, 2, 0, $2, 100, 'KEY', 'suspended', 'obj', 'select 1', 'select 2', 'login', 'host', 'app', 'login', 'host', 'app')",
            CollectionIdGenerator.Next(), t, SqlServerId, SqlServerName);

        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml, victim_query_plan_xml, database_name) VALUES ($1, $2, $3, $4, $2, 'process1', 'select 1', '<deadlock/>', NULL, 'testdb')",
            CollectionIdGenerator.Next(), t, SqlServerId, SqlServerName);

        // Two distinct collection_time snapshots so ObjectGrowthSql/ObjectContentionSql have a prior/latest pair.
        var prior = t.AddDays(-1);
        foreach (var (snapTime, mb) in new[] { (prior, 100m), (t, 110m) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, object_id, schema_name, table_name, index_id, index_name, reserved_mb, row_lock_wait_in_ms, index_lock_promotion_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                CollectionIdGenerator.Next(), snapTime, SqlServerId, SqlServerName, "testdb", 1, "dbo", "TestTable", 1, "PK_TestTable", mb, 10L, 0L);
        }
    }

    private static async Task DeleteSqlServerRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var tables = new[]
        {
            "wait_stats", "cpu_utilization_stats", "blocked_process_reports", "dmv_blocking_snapshots",
            "deadlocks", "file_io_stats", "perfmon_stats", "session_stats", "query_stats", "memory_stats",
            "index_object_stats"
        };
        foreach (var table in tables)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", SqlServerId);
        }
    }

    /// <summary>
    /// One row in every table a PgTarget family's window read touches, plus a background history row well
    /// outside the tested window so <see cref="PgTargetAnomalyDetector.HasBaselineDataAsync"/> passes.
    /// </summary>
    /// <summary>History rows for the PgTarget baseline gate: 12 same-hour/same-day-of-week rows, one per
    /// week going back 12 weeks, for every table a PgTarget <c>*WindowSql</c> const's baseline arm reads,
    /// plus the current-window row set every window read touches. See
    /// <see cref="SeedSqlServerFamilyTablesAsync"/> for why 12 rows (not 1) is the floor.
    /// </summary>
    private static async Task SeedPgTargetFamilyTablesAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var now = DateTime.SpecifyKind(
            new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)),
            DateTimeKind.Unspecified);
        var t = now.AddHours(-1);
        var priorMinute = t.AddMinutes(-1);
        var historyTimes = Enumerable.Range(1, 12).Select(w => t.AddDays(-7 * w)).ToArray();

        long xact = 1000L;
        foreach (var at in historyTimes.Append(t))
        {
            // pg_database_stats: two rows per collection time so the LAG-based counter differencing in
            // DatabaseCounterWindowSql (and its baseline arm) has a predecessor.
            foreach (var (snap, val) in new[] { (at.AddMinutes(-1), xact), (at, xact + 100L) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name, xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset) VALUES ($1, $2, $3, $4, 'appdb', $5, 0, 100, 9000, 0, 0, 0, NULL)",
                    CollectionIdGenerator.Next(), snap, PgTargetServerId, PgTargetServerName, val);
            }
            xact += 100L;

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_session_states (collection_id, collection_time, server_id, server_name, state_is_redacted, total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions) VALUES ($1, $2, $3, $4, FALSE, $5, $6, $7, $8)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName, 10, 4, 1, 1);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_cpu_utilization (collection_id, collection_time, server_id, server_name, sample_time, cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu) VALUES ($1, $2, $3, $4, $2, $5, $6, $7, $8)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName, 30.0, 25.0, 3.8, 12.0);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_wait_stats (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event, waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds) VALUES ($1, $2, $3, $4, 1, 1, 'Lock', 'relation', 10, 1000, 1, 100, 60)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName);

            // pg_io_read_latency baseline arm: LAG needs a predecessor row in the PRECEDING 15-minute
            // date_bin bucket, not the same one — two rows 30 seconds apart land in ONE bucket after the
            // sampled CTE's MAX() aggregation, so LAG(reads) still has no adjacent-bucket predecessor and
            // raw_reads stays NULL for every week (baseline.SampleCount stays 0, and DetectIoAnomalies
            // returns before its window read ever runs — the const this guard shipped vacuous for, G1-5).
            // Fix: seed the pair 15 minutes apart, one bucket earlier and the bucket itself, with a
            // cumulative-counter increase, so LAG resolves a real predecessor.
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context, reads, read_time_ms, writes, write_time_ms, writebacks, writeback_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, fsyncs, fsync_time_ms, stats_reset, read_bytes, write_bytes, extend_bytes) VALUES ($1, $2, $3, $4, 'client backend', 'relation', 'normal', 0, 0.0, 0, 0.0, 0, 0, 0, 0, 8192, 0, 0, 0, 0, 0, NULL, 0, 0, 0)",
                CollectionIdGenerator.Next(), at.AddMinutes(-15), PgTargetServerId, PgTargetServerName);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context, reads, read_time_ms, writes, write_time_ms, writebacks, writeback_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, fsyncs, fsync_time_ms, stats_reset, read_bytes, write_bytes, extend_bytes) VALUES ($1, $2, $3, $4, 'client backend', 'relation', 'normal', 300, 400.0, 5, 2.0, 0, 0, 0, 0, 8192, 100, 0, 0, 0, 0, NULL, 81920, 40960, 0)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_replication_stats (collection_id, collection_time, server_id, server_name, application_name, client_addr, state, sync_state, sync_priority, sent_bytes_behind, write_bytes_behind, flush_bytes_behind, replay_bytes_behind, write_lag_ms, flush_lag_ms, replay_lag_ms, backend_start) VALUES ($1, $2, $3, $4, 'replica1', '10.0.0.2', 'streaming', 'async', 0, 0, 0, 0, 0, 1.0, 1.0, 1.0, $2)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_write_stats (collection_id, collection_time, server_id, server_name, num_timed, num_requested, num_done, restartpoints_timed, restartpoints_req, restartpoints_done, checkpoint_write_time_ms, checkpoint_sync_time_ms, buffers_written_checkpoint, slru_written, checkpointer_stats_reset, buffers_clean, maxwritten_clean, buffers_alloc, buffers_backend, buffers_backend_fsync, bgwriter_stats_reset, wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write, wal_sync, wal_write_time_ms, wal_sync_time_ms, wal_stats_reset, postmaster_start_time) VALUES ($1, $2, $3, $4, 1, 1, 1, 0, 0, 0, 10.0, 5.0, 100, 0, NULL, 10, 0, 100, 5, 0, NULL, 100, 5, 65536, 0, 10, 5, 1.0, 1.0, NULL, $2)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_kernel_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, exec_user_time_ms, exec_system_time_ms, plan_cpu_time_ms, exec_read_bytes, exec_write_bytes, minor_faults, major_faults, stats_since) VALUES ($1, $2, $3, $4, 'appdb', 1, 10.0, 2.0, 12.0, 8192, 4096, 100, 0, $2)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms) VALUES ($1, $2, $3, $4, 'Lock', 'relation', 1, 10, 1000, 1, 100)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_statement_stats (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel, calls, total_exec_time_ms, min_exec_time_ms, max_exec_time_ms, mean_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, temp_blks_read, temp_blks_written, blk_read_time_ms, blk_write_time_ms, wal_records, wal_fpi, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds) VALUES ($1, $2, $3, $4, 1, 1, 1, TRUE, 10, 100.0, 1.0, 20.0, 10.0, 100, 100, 10, 0, 0, 0, 0, 1.0, 0.0, 0, 0, 0, 5, 50.0, 50, 60)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_blocking_edges (collection_id, collection_time, server_id, server_name, blocked_backend_id, blocked_pid, blocking_backend_id, blocking_pid, database_name, blocked_username, blocked_application_name, blocked_client_addr, blocked_state, blocked_wait_event_type, blocked_wait_event, blocked_query, blocked_xact_duration_ms, blocked_query_duration_ms, blocking_username, blocking_application_name, blocking_client_addr, blocking_state, blocking_wait_event_type, blocking_wait_event, blocking_query, blocking_xact_duration_ms, blocking_query_duration_ms, blocked_pid_count, blocking_is_idle_in_transaction, query_text_may_be_truncated) VALUES ($1, $2, $3, $4, 1, 100, 2, 200, 'appdb', 'user1', 'app', '10.0.0.1', 'active', 'Lock', 'relation', 'select 1', 100, 100, 'user2', 'app', '10.0.0.2', 'idle in transaction', NULL, NULL, 'select 2', 200, 200, 1, TRUE, FALSE)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_database_size_stats (collection_id, collection_time, server_id, server_name, database_name, size_bytes, total_bytes, is_template, allows_connections) VALUES ($1, $2, $3, $4, 'appdb', $5, $5, FALSE, TRUE)",
                CollectionIdGenerator.Next(), at.AddMinutes(-1), PgTargetServerId, PgTargetServerName, 1000000L);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_database_size_stats (collection_id, collection_time, server_id, server_name, database_name, size_bytes, total_bytes, is_template, allows_connections) VALUES ($1, $2, $3, $4, 'appdb', $5, $5, FALSE, TRUE)",
                CollectionIdGenerator.Next(), at, PgTargetServerId, PgTargetServerName, 1000100L);
        }

        /* Extra current-window rows so the four consts that a flat per-week series leaves "never ran" actually
           execute — the same fallback-firing recipe SeedSqlServerFamilyTablesAsync uses for its own wait spike.
           DatabaseCounterWindowSql: a third pg_database_stats row one minute after t with 2 deadlocks (>=
           AnomalyThresholds.PgDeadlockRateFloorPerHour and, over the ~4h/24h observed windows, safely under
           PgDeadlockRateFallbackPerHour's per-hour rate scaled up — the ratio arm fires on ratio alone here since
           the baseline mean is 0, and DetectDeadlockRateAnomalies reads the SAME window read regardless of which
           arm fires, so ReadDatabaseCounterWindowAsync (hence DatabaseCounterWindowSql) always executes once
           deadlockIntervals > 0 and deadlocks > 0. WaitContribWindowSql: an extra wait_stats row with a spiked
           delta_wait_time_us so the wait-profile fallback bar (PgWaitProfileFallbackMsPerSec) fires exactly as
           SeedSqlServerFamilyTablesAsync's own wait spike does for the SQL Server store. IoLatencyWindowSql: a
           second-and-third pg_io_stats 15-minute-bucketed pair (date_bin) with reads deltas >= 250 (the
           PgTargetScorer.IoBaselineBucketMinimumReads floor IoLatencyWindowSql's own CTE enforces) so a rated
           sample exists. SampledWaitContribWindowSql: an extra pg_wait_sampling row with a higher sample_count so
           DetectSampledWaitProfileAnomalies's fallback bar (PgSampledWaitProfileFallbackMsPerSec) fires — note
           pg_wait_stats already has rows for every history/current time above, so exact_collections > 0 and this
           detector sits out entirely UNLESS pg_wait_stats is absent for this server; PgTargetAnomalyDetector's own
           "only pg_wait_stats" exclusivity rule means SampledWaitContribWindowSql can only run when pg_wait_stats
           has NO rows for this server — so this guard seeds pg_wait_sampling on a SEPARATE fake server id that has
           no pg_wait_stats rows at all (PgTargetSampledOnlyServerId below), exercised in its own extra pass. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name, xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset) VALUES ($1, $2, $3, $4, 'appdb', $5, 0, 100, 9000, 0, 0, 2, NULL)",
            CollectionIdGenerator.Next(), t.AddMinutes(1), PgTargetServerId, PgTargetServerName, xact + 200L);

        // WaitContribWindowSql: a much larger spike than the fallback bar (500 ms/sec) — 60,000 ms of
        // waiting inside a 60-second collection interval, well past PgWaitProfileFallbackMsPerSec.
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_wait_stats (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event, waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds) VALUES ($1, $2, $3, $4, 1, 1, 'Lock', 'relation', 10, 60000000, 1, 60000000, 60)",
            CollectionIdGenerator.Next(), t.AddMinutes(1), PgTargetServerId, PgTargetServerName);

        // IoLatencyWindowSql: a LAG predecessor plus a rated successor, 15 minutes apart (its date_bin grain),
        // with a reads delta >= the 250 floor and a latency-per-read well past PgIoLatencyFallbackMs (20 ms) —
        // the per-week history above lands one row per bucket (no adjacent-bucket predecessor), so the baseline
        // arm here is untrustworthy and the FALLBACK bar, not the floor, is what must clear.
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context, reads, read_time_ms, writes, write_time_ms, writebacks, writeback_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, fsyncs, fsync_time_ms, stats_reset, read_bytes, write_bytes, extend_bytes) VALUES ($1, $2, $3, $4, 'client backend', 'relation', 'normal', 0, 0.0, 0, 0.0, 0, 0, 0, 0, 8192, 0, 0, 0, 0, 0, NULL, 0, 0, 0)",
            CollectionIdGenerator.Next(), t.AddMinutes(1), PgTargetServerId, PgTargetServerName);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context, reads, read_time_ms, writes, write_time_ms, writebacks, writeback_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, fsyncs, fsync_time_ms, stats_reset, read_bytes, write_bytes, extend_bytes) VALUES ($1, $2, $3, $4, 'client backend', 'relation', 'normal', 300, 12000.0, 0, 0.0, 0, 0, 0, 0, 8192, 0, 0, 0, 0, 0, NULL, 0, 0, 0)",
            CollectionIdGenerator.Next(), t.AddMinutes(16), PgTargetServerId, PgTargetServerName);

        // SampledWaitContribWindowSql: a separate server with pg_wait_sampling rows and NO pg_wait_stats rows,
        // so the "only pg_wait_stats" exclusivity rule (DetectSampledWaitProfileAnomalies's summary) lets its
        // detector run rather than sit out; it needs its OWN pg_database_stats canary row for the whole-pass
        // HasBaselineDataAsync gate (30-day witness), independent of the sampled family's own baseline.
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name, xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset) VALUES ($1, $2, $3, $4, 'appdb', 0, 0, 0, 0, 0, 0, 0, NULL)",
            CollectionIdGenerator.Next(), t, PgTargetSampledOnlyServerId, PgTargetSampledOnlyServerName);

        var sampledHistoryTimes = Enumerable.Range(1, 12).Select(w => t.AddDays(-7 * w)).ToArray();
        long sampleCount = 100L;
        foreach (var at in sampledHistoryTimes)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms) VALUES ($1, $2, $3, $4, 'Lock', 'relation', 1, $5, 1000, 1, 60000)",
                CollectionIdGenerator.Next(), at, PgTargetSampledOnlyServerId, PgTargetSampledOnlyServerName, sampleCount);
            sampleCount += 100L;
        }
        // Two rows INSIDE every tested window (4h and 24h) so LAG has an in-window predecessor: the history
        // rows above sit weeks outside the window and never pair with anything the SampledWaitRateWindowSql
        // series reads. A huge count jump between them clears PgSampledWaitProfileFallbackMsPerSec (500 ms/sec).
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms) VALUES ($1, $2, $3, $4, 'Lock', 'relation', 1, $5, 1000, 1, 60000)",
            CollectionIdGenerator.Next(), t, PgTargetSampledOnlyServerId, PgTargetSampledOnlyServerName, 100L);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms) VALUES ($1, $2, $3, $4, 'Lock', 'relation', 1, $5, 1000, 1, 60000)",
            CollectionIdGenerator.Next(), t.AddMinutes(1), PgTargetSampledOnlyServerId, PgTargetSampledOnlyServerName, 100100L);
    }

    private static async Task DeletePgTargetRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var tables = new[]
        {
            "pg_database_stats", "pg_session_states", "pg_cpu_utilization", "pg_wait_stats", "pg_io_stats",
            "pg_replication_stats", "pg_write_stats", "pg_kernel_stats", "pg_wait_sampling",
            "pg_statement_stats", "pg_plan_capture", "pg_blocking_edges", "pg_database_size_stats"
        };
        foreach (var table in tables)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", PgTargetServerId);
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", PgTargetSampledOnlyServerId);
        }
    }

    private static async Task DropBaselineFallbackViewsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var (_, view) in TimescaleSupport.BaselineAggregates)
        {
            using var drop = new NpgsqlCommand(TimescaleSupport.DropBaselineFallbackViewSql(view), connection);
            await drop.ExecuteNonQueryAsync(ct);
        }
    }
}
