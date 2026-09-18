/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Postgres alert-read adapter (Phase-5 slice B). Ungated: every feed query is
/// dialect-safe PG — no bare <c>now()</c> anywhere (timestamptz vs the naive-UTC timestamp
/// columns; the long-running-query read binds a parameterized naive-UTC $4 instead, per the
/// Phase-5 review constraint), no <c>N''</c> literals, raw collector tables (never Lite's
/// <c>v_</c> views), and Lite's window/cap/filter shape preserved per feed. Gated on
/// DARLING_TEST_PG: all seven feeds end-to-end against a dev Postgres — including the XE→DMV
/// blocking fallback: a DMV snapshot row appears exactly when no blocked-process report covers
/// the same SPID pair in the same minute.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class DarlingAlertReadAdapterTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -717171;
    private static readonly string TestServerKey = TestServerId.ToString(CultureInfo.InvariantCulture);
    private const string TestServerName = "alert-adapter-e2e";

    /// <summary>
    /// Regression guard: the sp_server_diagnostics exclusion must match on BOTH the wait type and the query text.
    /// A field session (multi39, 2026-07-22) fired a false Long-Running Query alert because sp_server_diagnostics
    /// was captured in a PREEMPTIVE_XE_GETTARGETSTATE wait (it also does Extended Events work), which the
    /// wait-type-only match missed. The query-text match (case-insensitive, NULL-safe) catches it regardless of
    /// the wait it is in at capture time. Ungated (a pure SQL-fragment pin).
    /// </summary>
    [Fact]
    public void SpServerDiagnosticsFilter_AlsoMatchesQueryText_NotJustWaitType_AndIsNullSafe()
    {
        var filter = DarlingAlertReadAdapter.SpServerDiagnosticsFilter;
        Assert.Contains("wait_type NOT LIKE '%SP_SERVER_DIAGNOSTICS%'", filter, StringComparison.Ordinal);
        Assert.Contains("query_text NOT ILIKE '%sp_server_diagnostics%'", filter, StringComparison.Ordinal);
        /* NULL-safe: a legitimate long-running query with a NULL query_text must NOT be dropped by this filter. */
        Assert.Contains("r.query_text IS NULL OR", filter, StringComparison.Ordinal);
    }

    private static readonly string[] AllFeedSql =
    {
        DarlingAlertReadAdapter.BlockedProcessReportsSql,
        DarlingAlertReadAdapter.DmvBlockingSnapshotsSql,
        DarlingAlertReadAdapter.DeadlocksSql,
        DarlingAlertReadAdapter.PoisonWaitsSql,
        DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate,
        DarlingAlertReadAdapter.SpServerDiagnosticsFilter,
        DarlingAlertReadAdapter.WaitForFilter,
        DarlingAlertReadAdapter.BackupsFilter,
        DarlingAlertReadAdapter.MiscWaitsFilter,
        DarlingAlertReadAdapter.CdcFilter,
        DarlingAlertReadAdapter.VolumeFreeSpaceSql,
        DarlingAlertReadAdapter.TempDbSpaceSql,
        DarlingAlertReadAdapter.AnomalousJobsSql
    };

    /* ---------------- ungated dialect pins ---------------- */

    [Fact]
    public void NoFeedQuery_UsesBareNow_OrNPrefixedLiterals()
    {
        foreach (var sql in AllFeedSql)
        {
            /* Bare now() is timestamptz — the naive-UTC columns would compare in the server's
               time zone. Every "now" must arrive as a bound Kind-Unspecified parameter. */
            Assert.DoesNotContain("now(", sql.ToLowerInvariant());
            /* Postgres has no N'' literal syntax (that's the one DuckDB/T-SQL-ism in Lite's LRQ read). */
            Assert.DoesNotContain("N'", sql);
        }
    }

    [Fact]
    public void FeedQueries_TargetRawCollectorTables_NeverLiteViews()
    {
        foreach (var sql in AllFeedSql)
        {
            /* Lite's reads go FROM v_<table>; the PG twin must hit the raw tables (the "v_" in
               dmv_blocking_snapshots is the table's own name, so match the FROM/JOIN usage). */
            Assert.DoesNotContain("FROM v_", sql);
            Assert.DoesNotContain("JOIN v_", sql);
        }

        Assert.Contains("FROM blocked_process_reports", DarlingAlertReadAdapter.BlockedProcessReportsSql);
        Assert.Contains("FROM dmv_blocking_snapshots", DarlingAlertReadAdapter.DmvBlockingSnapshotsSql);
        Assert.Contains("FROM deadlocks", DarlingAlertReadAdapter.DeadlocksSql);
        Assert.Contains("FROM wait_stats", DarlingAlertReadAdapter.PoisonWaitsSql);
        Assert.Contains("FROM query_snapshots", DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate);
        Assert.Contains("FROM database_size_stats", DarlingAlertReadAdapter.VolumeFreeSpaceSql);
        Assert.Contains("FROM tempdb_stats", DarlingAlertReadAdapter.TempDbSpaceSql);
        Assert.Contains("FROM running_jobs", DarlingAlertReadAdapter.AnomalousJobsSql);
    }

    [Fact]
    public void FeedQueries_PreserveLitesWindowsCapsAndFilters()
    {
        /* Blocking: newest-first by event time, both sources capped at 200 (the shared merge
           re-caps after appending the DMV fallback). */
        Assert.Contains("ORDER BY event_time DESC", DarlingAlertReadAdapter.BlockedProcessReportsSql);
        Assert.Contains("LIMIT 200", DarlingAlertReadAdapter.BlockedProcessReportsSql);
        Assert.Contains("ORDER BY event_time DESC", DarlingAlertReadAdapter.DmvBlockingSnapshotsSql);
        Assert.Contains("LIMIT 200", DarlingAlertReadAdapter.DmvBlockingSnapshotsSql);

        /* Deadlocks: newest-first by deadlock time, capped at 50. */
        Assert.Contains("ORDER BY deadlock_time DESC", DarlingAlertReadAdapter.DeadlocksSql);
        Assert.Contains("LIMIT 50", DarlingAlertReadAdapter.DeadlocksSql);

        /* Poison waits (#3539 A4): the same wait-type list, a parameterized window floor, and an
           ACCUMULATION per wait type — see PoisonWaitsSql_IsAWindowAccumulation_NotTheNewestDeltas. */
        Assert.Contains("'THREADPOOL', 'RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE'",
            DarlingAlertReadAdapter.PoisonWaitsSql);
        Assert.Contains("collection_time >= $2", DarlingAlertReadAdapter.PoisonWaitsSql);

        /* Long-running queries: latest snapshot only, parameterized staleness floor ($4 — never
           now()), user sessions, parameterized cap, filter splice point. */
        Assert.Contains("r.collection_time >= $4", DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate);
        Assert.Contains("r.session_id > 50", DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate);
        Assert.Contains("LIMIT $3", DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate);
        Assert.Contains("{0}", DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate);
        Assert.Contains("ORDER BY r.total_elapsed_time_ms DESC", DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate);

        /* Volumes: latest collection only, no-mount-point rows excluded, worst ratio first. */
        Assert.Contains("volume_mount_point IS NOT NULL", DarlingAlertReadAdapter.VolumeFreeSpaceSql);
        Assert.Contains("ORDER BY MIN(volume_free_mb) / MAX(volume_total_mb)", DarlingAlertReadAdapter.VolumeFreeSpaceSql);

        /* Anomalous jobs: latest snapshot, 60-second noise floor, threshold + cap. */
        Assert.Contains("avg_duration_seconds >= 60", DarlingAlertReadAdapter.AnomalousJobsSql);
        Assert.Contains("percent_of_average >= $2", DarlingAlertReadAdapter.AnomalousJobsSql);
        Assert.Contains("LIMIT 5", DarlingAlertReadAdapter.AnomalousJobsSql);
    }

    /// <summary>
    /// #3539 A4: the poison read SUMs every row in the window per wait type. Three things the retired text
    /// had must be ABSENT — the <c>delta_waiting_tasks &gt; 0</c> filter (a task waiting across the interval
    /// boundary accrues time with zero completed tasks, and the measured fleet holds such rows), the
    /// <c>LIMIT 3</c> (a limit on a sum is an undercount) and any threshold — and the shape must be the sums,
    /// the row count and the newest collection_time, grouped by wait type. The Lite twin's DuckDB text is
    /// pinned to the same clauses in Lite.Tests so the two SKUs cannot drift.
    /// </summary>
    [Fact]
    public void PoisonWaitsSql_IsAWindowAccumulation_NotTheNewestDeltas()
    {
        var sql = DarlingAlertReadAdapter.PoisonWaitsSql;

        Assert.DoesNotContain("delta_waiting_tasks > 0", sql);
        Assert.DoesNotContain("LIMIT", sql);
        Assert.DoesNotContain("avg_ms_per_wait", sql);

        Assert.Contains("SUM(delta_wait_time_ms)::bigint AS accumulated_wait_ms", sql);
        Assert.Contains("SUM(delta_waiting_tasks)::bigint AS accumulated_waits", sql);
        Assert.Contains("COUNT(*)::bigint AS observed_intervals", sql);
        Assert.Contains("MAX(collection_time) AS newest_collection_time", sql);
        Assert.Contains("GROUP BY wait_type", sql);
        /* No interval handling: the V128 lane owns sample_interval_seconds and rebases onto this text. */
        Assert.DoesNotContain("sample_interval", sql);

        /* The read-side wait-type list IS the evaluator's, spelled once each and equal. */
        foreach (var waitType in PoisonWaitEvaluator.SqlServerWaitTypes)
        {
            Assert.Contains($"'{waitType}'", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Adapter_ImplementsTheSharedReadSeam()
    {
        Assert.True(typeof(IAlertReadAdapter).IsAssignableFrom(typeof(DarlingAlertReadAdapter)));
    }

    /* ---------------- gated live E2E ---------------- */

    private const string DeadlockGraphXml = @"<deadlock><victim-list><victimProcess id=""process1""/></victim-list><process-list><process id=""process1"" spid=""55"" currentdbname=""StackOverflow""><inputbuf>UPDATE Users SET Reputation = 1</inputbuf></process><process id=""process2"" spid=""60"" currentdbname=""StackOverflow""><inputbuf>UPDATE Badges SET Name = 'x'</inputbuf></process></process-list><resource-list><keylock objectname=""StackOverflow.dbo.Users""><owner id=""process2"" mode=""X""/><waiter id=""process1"" mode=""U""/></keylock></resource-list></deadlock>";

    [Fact]
    public async Task EndToEnd_AllSevenFeeds_IncludingDmvBlockingFallback_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live alert-read test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(postgres);

        var bodySucceeded = false;
        try
        {
            /* All timestamps Kind-Unspecified — naive-UTC storage, see PgCollectorRowWriter.
               Event times floored to a minute so the merge's same-minute dedup is deterministic. */
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var minuteFloor = new DateTime(utcNow.Year, utcNow.Month, utcNow.Day, utcNow.Hour, utcNow.Minute, 0);
            var collectionTime = utcNow.AddMinutes(-1);

            /* --- blocking: one BPR + one same-minute DMV duplicate + one distinct DMV row --- */
            var bprEventTime = minuteFloor.AddMinutes(-5).AddSeconds(10);
            var dmvOverlapTime = minuteFloor.AddMinutes(-5).AddSeconds(40); /* same minute + SPIDs as the BPR */
            var dmvFallbackTime = minuteFloor.AddMinutes(-3).AddSeconds(20);

            await InsertAsync(connection,
                "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_process_report_xml, contentious_object) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                1L, collectionTime, TestServerId, TestServerName, bprEventTime, "StackOverflow",
                55, 66, 12000L, "X", "UPDATE Users SET x = 1", "BEGIN TRAN UPDATE Users",
                "<blocked-process-report/>", "StackOverflow.dbo.Users");

            await InsertAsync(connection,
                "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, contentious_object) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                1L, collectionTime, TestServerId, TestServerName, dmvOverlapTime, "StackOverflow",
                55, 66, 9000L, "X", "UPDATE Users SET x = 1", "BEGIN TRAN UPDATE Users", "StackOverflow.dbo.Users");

            await InsertAsync(connection,
                "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, contentious_object) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                2L, collectionTime, TestServerId, TestServerName, dmvFallbackTime, "StackOverflow",
                77, 88, 4000L, "U", "DELETE FROM Badges", "UPDATE Badges SET x = 2", "StackOverflow.dbo.Badges");

            /* --- deadlocks --- */
            await InsertAsync(connection,
                "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                1L, collectionTime, TestServerId, TestServerName, utcNow.AddMinutes(-4),
                "process1", "UPDATE Users SET Reputation = 1", DeadlockGraphXml);

            /* --- poison waits (#3539 A4): four THREADPOOL rows inside the window that the retired read
                   judged wrongly or not at all — a 703-task 8 ms storm row, a time-with-no-completed-task
                   row (the old tasks > 0 filter dropped it), a (0, 0) calculator marker, and one more storm
                   row; plus one RESOURCE_SEMAPHORE row, and a THREADPOOL row OUTSIDE the window that must
                   not be summed. Expected THREADPOOL: 5,779 + 304 + 0 + 594,000 = 600,083 ms across
                   703 + 0 + 0 + 29,700 waits over 4 observed intervals. --- */
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                1L, collectionTime.AddMinutes(-3), TestServerId, TestServerName, "THREADPOOL", 703L, 5779L);
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                2L, collectionTime.AddMinutes(-2), TestServerId, TestServerName, "THREADPOOL", 0L, 304L);
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                3L, collectionTime.AddMinutes(-1), TestServerId, TestServerName, "THREADPOOL", 0L, 0L);
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                4L, collectionTime, TestServerId, TestServerName, "THREADPOOL", 29700L, 594000L);
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                4L, collectionTime, TestServerId, TestServerName, "RESOURCE_SEMAPHORE", 8L, 3154L);
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                0L, collectionTime.AddMinutes(-30), TestServerId, TestServerName, "THREADPOOL", 1000L, 999999L);

            /* --- long-running queries: one 10-minute query + one in an excluded database --- */
            await InsertAsync(connection,
                "INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, total_elapsed_time_ms, cpu_time_ms, reads, writes, wait_type, blocking_session_id, query_hash, program_name) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                1L, collectionTime, TestServerId, TestServerName, 71, "StackOverflow",
                "SELECT COUNT(*) FROM Users", 600000L, 1234L, 10L, 2L, "CXPACKET", 0, "0x9AAF0129E4E9AD07", "HammerDB");

            await InsertAsync(connection,
                "INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, total_elapsed_time_ms, cpu_time_ms, reads, writes, wait_type, blocking_session_id, query_hash, program_name) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                2L, collectionTime, TestServerId, TestServerName, 72, "ExcludedDb",
                "SELECT 1", 720000L, 1L, 1L, 0L, "CXPACKET", 0, "0x1111111111111111", "HammerDB");

            /* --- volumes: two files on C:\ (MAX total / MIN free), one on healthy D:\ --- */
            await InsertAsync(connection,
                "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, volume_mount_point, volume_total_mb, volume_free_mb) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                1L, collectionTime, TestServerId, TestServerName, "StackOverflow", "C:\\", 102400m, 8192m);
            await InsertAsync(connection,
                "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, volume_mount_point, volume_total_mb, volume_free_mb) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                2L, collectionTime, TestServerId, TestServerName, "msdb", "C:\\", 102400m, 9000m);
            await InsertAsync(connection,
                "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, volume_mount_point, volume_total_mb, volume_free_mb) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                3L, collectionTime, TestServerId, TestServerName, "tempdb", "D:\\", 204800m, 102400m);

            /* --- tempdb: 800 reserved / 200 unallocated -> 80% used --- */
            await InsertAsync(connection,
                "INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb, total_reserved_mb, unallocated_mb, top_session_id, top_session_tempdb_mb) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                1L, collectionTime, TestServerId, TestServerName, 500m, 250m, 50m, 800m, 200m, 55, 123.4m);

            /* --- the server's UTC offset, so the anomalous-jobs read can state its server-local
                   start_time in UTC (#3421). Non-zero on purpose: at 0 the conversion asserted below
                   would pass whether or not the offset was read at all. --- */
            await InsertAsync(connection,
                "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) VALUES ($1, $2, $3, $4, $5)",
                1L, collectionTime, TestServerId, TestServerName, -240);

            /* --- running jobs: one anomalous, one under the 60-second average noise floor --- */
            await InsertAsync(connection,
                "INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, percent_of_average) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                collectionTime, TestServerId, TestServerName, "Nightly ETL", "job-guid-1",
                utcNow.AddHours(-1), 3661L, 90L, 120L, 350.0m);
            await InsertAsync(connection,
                "INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, percent_of_average) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                collectionTime, TestServerId, TestServerName, "Fast Job", "job-guid-2",
                utcNow.AddMinutes(-2), 90L, 30L, 40L, 999.0m);

            /* --- blocking: BPR preferred, same-minute DMV duplicate dropped, distinct DMV appended --- */
            var blocking = await adapter.GetRecentBlockedProcessReportsAsync(TestServerKey, hoursBack: 1, ct);
            Assert.Equal(2, blocking.Count);
            /* Newest first: the 3-minutes-ago DMV fallback row, then the 5-minutes-ago BPR row. */
            Assert.Equal(BlockedProcessAlertRow.DmvSnapshotSource, blocking[0].Source);
            Assert.Equal(77, blocking[0].BlockedSpid);
            Assert.False(blocking[0].HasReportXml);
            Assert.Equal(BlockedProcessAlertRow.XeReportSource, blocking[1].Source);
            Assert.Equal(55, blocking[1].BlockedSpid);
            Assert.True(blocking[1].HasReportXml);
            Assert.Equal("StackOverflow.dbo.Users", blocking[1].ContentiousObject);
            /* The overlapping DMV row (same SPID pair, same minute as the BPR) must NOT appear. */
            Assert.DoesNotContain(blocking, b =>
                b.Source == BlockedProcessAlertRow.DmvSnapshotSource && b.BlockedSpid == 55);

            /* --- deadlocks: shape + the shared XML process-summary parse --- */
            var deadlocks = await adapter.GetRecentDeadlocksAsync(TestServerKey, hoursBack: 1, ct);
            var deadlock = Assert.Single(deadlocks);
            Assert.True(deadlock.HasDeadlockXml);
            Assert.Equal("UPDATE Users SET Reputation = 1", deadlock.VictimSqlText);
            Assert.Equal("SPID 55 (victim) vs SPID 60", deadlock.ProcessSummary);

            /* --- poison waits: the window sum per type, no threshold, worst first; the out-of-window row
                   is excluded and the sub-bar RESOURCE_SEMAPHORE row comes back too (observed-and-quiet is
                   an answer the engine needs) --- */
            var poison = await adapter.GetPoisonWaitAccumulationAsync(TestServerKey, PoisonWaitEvaluator.WindowMinutes, ct);
            Assert.Equal(2, poison.Count);
            Assert.Equal("THREADPOOL", poison[0].WaitType);
            Assert.Equal(600_083L, poison[0].AccumulatedWaitMs);
            Assert.Equal(30_403L, poison[0].AccumulatedWaits);
            Assert.Equal(4L, poison[0].ObservedIntervals);
            Assert.Equal(collectionTime, poison[0].NewestCollectionTime);
            Assert.Equal("RESOURCE_SEMAPHORE", poison[1].WaitType);
            Assert.Equal(3_154L, poison[1].AccumulatedWaitMs);
            Assert.Equal(1L, poison[1].ObservedIntervals);
            /* And the evaluator reads that window as the storm it is: Warning, where the retired shape saw
               a 20 ms average on the biggest row and slept. */
            var graded = PoisonWaitEvaluator.EvaluateSqlServer(poison);
            var storm = Assert.Single(graded);
            Assert.Equal("THREADPOOL", storm.WaitType);
            Assert.Equal(AlertSeverityLevel.Warning, storm.Severity);

            /* --- long-running queries: threshold + excluded-database drop --- */
            var lrq = await adapter.GetLongRunningQueriesAsync(
                TestServerKey, thresholdMinutes: 5, maxResults: 5,
                excludeSpServerDiagnostics: true, excludeWaitFor: true, excludeBackups: true,
                excludeMiscWaits: true, excludeCdc: true,
                excludedDatabases: new List<string> { "excludeddb" }, ct);
            var query = Assert.Single(lrq);
            Assert.Equal(71, query.SessionId);
            Assert.Equal(600L, query.ElapsedSeconds);
            Assert.Equal("HammerDB", query.ProgramName);
            Assert.Equal("0x9AAF0129E4E9AD07", query.QueryHash);

            /* --- volumes: per-volume rollup, worst free-ratio first --- */
            var volumes = await adapter.GetVolumeFreeSpaceAsync(TestServerKey, ct);
            Assert.Equal(2, volumes.Count);
            Assert.Equal("C:\\", volumes[0].MountPoint);
            Assert.Equal(102400d, volumes[0].TotalMb, precision: 3);
            Assert.Equal(8192d, volumes[0].FreeMb, precision: 3); /* MIN of the two C:\ rows */
            Assert.Equal("D:\\", volumes[1].MountPoint);

            /* --- tempdb --- */
            var tempDb = await adapter.GetTempDbSpaceAsync(TestServerKey, ct);
            Assert.NotNull(tempDb);
            Assert.Equal(80d, tempDb!.ReservedPercent, precision: 3);
            Assert.Equal(55, tempDb.TopConsumerSessionId);

            /* --- anomalous jobs: threshold + the 60-second average noise floor --- */
            var jobs = await adapter.GetAnomalousJobsAsync(TestServerKey, multiplier: 3, ct);
            Assert.True(jobs.SnapshotIsFresh);
            var job = Assert.Single(jobs.Jobs);
            Assert.Equal("Nightly ETL", job.JobName);
            Assert.Equal(3661L, job.CurrentDurationSeconds);
            Assert.Equal(350.0m, job.PercentOfAverage);

            /* #3421: start_time is the monitored server's own clock, so this read carries the server's
               collected offset beside it and the row states the same instant in UTC. Asserted as
               INSTANTS, not as formatted strings — a pinned string passes under a sign error as readily
               as under the right sign. The offset assertion is what proves the projection resolves
               against a live store at all rather than compiling.

               The round trip is asserted to POSTGRES's resolution, not .NET's: a timestamp column is
               microsecond-precision and a planted DateTime carries 100 ns ticks, so the value comes back
               truncated by up to 9 ticks. Comparing the converted value against the locally-computed
               plant instead of against what the store returned asserts that truncation, which fails on a
               plant whose sub-microsecond digits happen to be non-zero and passes on one where they are
               not - measured, at 4 ticks. */
            Assert.Equal(-240, job.UtcOffsetMinutes);
            Assert.True(
                (utcNow.AddHours(-1) - job.StartTime).Duration() < TimeSpan.FromMicroseconds(1),
                $"start_time round-tripped as {job.StartTime:O}, further than a microsecond from the planted {utcNow.AddHours(-1):O}");
            Assert.Equal(job.StartTime.AddMinutes(240), job.StartTimeUtc);

            /* #1812: age the SAME snapshot past the freshness bound (default 2-minute cadence → 10
               minutes) — the read becomes no evidence: not fresh, rows skipped, exactly the state that
               used to re-alert a historical run every cooldown forever.

               Aged by DELETE + re-INSERT at the old timestamp, NOT by UPDATE: collection_time is the
               hypertable's partition key, and TimescaleDB never re-routes a row on UPDATE — moving the
               key across a chunk boundary violates the chunk's slice CHECK constraint (23514). now-2h
               crosses the UTC-midnight boundary between 00:00 and 02:00 UTC, so the UPDATE form is a
               nightly two-hour time bomb; a fresh INSERT routes to the right chunk at any hour. */
            var agedTime = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-2), DateTimeKind.Unspecified);
            await InsertAsync(connection, $"DELETE FROM running_jobs WHERE server_id = {TestServerId}");
            await InsertAsync(connection,
                "INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, percent_of_average) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                agedTime, TestServerId, TestServerName, "Nightly ETL", "job-guid-1",
                utcNow.AddHours(-1), 3661L, 90L, 120L, 350.0m);
            await InsertAsync(connection,
                "INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, percent_of_average) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                agedTime, TestServerId, TestServerName, "Fast Job", "job-guid-2",
                utcNow.AddMinutes(-2), 90L, 30L, 40L, 999.0m);
            var stale = await adapter.GetAnomalousJobsAsync(TestServerKey, multiplier: 3, ct);
            Assert.False(stale.SnapshotIsFresh);
            Assert.Empty(stale.Jobs);

            /* And the cadence hook genuinely widens the bound: a relaxed profile's 60-minute cadence
               makes the same 2-hour-old snapshot CURRENT (bound 180 minutes). */
            var relaxed = new DarlingAlertReadAdapter(postgres, _ => 60);
            var slowProfile = await relaxed.GetAnomalousJobsAsync(TestServerKey, multiplier: 3, ct);
            Assert.True(slowProfile.SnapshotIsFresh);
            Assert.Single(slowProfile.Jobs);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertAsync(NpgsqlConnection connection, string sql, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
        {
            command.Parameters.AddWithValue(value);
        }
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {TestServerId};" +
            $"DELETE FROM dmv_blocking_snapshots WHERE server_id = {TestServerId};" +
            $"DELETE FROM deadlocks WHERE server_id = {TestServerId};" +
            $"DELETE FROM wait_stats WHERE server_id = {TestServerId};" +
            $"DELETE FROM query_snapshots WHERE server_id = {TestServerId};" +
            $"DELETE FROM database_size_stats WHERE server_id = {TestServerId};" +
            $"DELETE FROM tempdb_stats WHERE server_id = {TestServerId};" +
            $"DELETE FROM running_jobs WHERE server_id = {TestServerId};" +
            $"DELETE FROM server_properties WHERE server_id = {TestServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
