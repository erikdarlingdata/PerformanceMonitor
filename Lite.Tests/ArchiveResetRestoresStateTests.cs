using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The 512 MB archive-and-reset exports every archivable table to Parquet and recreates the live
/// database. These pins run the REAL <see cref="ArchiveService.ArchiveAllAndResetAsync"/> on a seeded
/// DuckDB file and then read what the collectors read next cycle: the watermark reads must still see
/// the pre-reset maximum (so already-collected events are not stored again), the "has collected before"
/// read must still see the old SUCCESS rows, and the alert and collector state tables must survive.
/// </summary>
/* ArchiveAllAndResetAsync touches CollectionResetGate and ArchiveService's static archive lock, both
   process-wide, so this class joins the serialized collection the other reset tests use. */
[Collection("CollectionResetGate")]
public sealed class ArchiveResetRestoresStateTests : IDisposable
{
    private static readonly DateTime T1 = new(2026, 5, 1, 10, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T2 = new(2026, 5, 1, 10, 5, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T3 = new(2026, 5, 1, 10, 10, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T4 = new(2026, 5, 1, 10, 7, 0, DateTimeKind.Unspecified);

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;

    public ArchiveResetRestoresStateTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        ArchiveService.AfterPreservedTableRestoredForTests = null;
        ArchiveService.BetweenPreserveCopyAndResetForTests = null;
        ArchiveService.BeforeDatabaseFileResetForTests = null;
        ArchiveService.BeforePreservedTableRestoreForTests = null;
        DuckDbInitializer.AfterDatabaseFilesDeletedForTests = null;
        CollectionResetGate.ResetForTests();
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    /// <summary>Exposes the runner's protected reads; only the initializer is exercised.</summary>
    private sealed class Reads(DuckDbInitializer duckDb)
        : RemoteCollectorService(duckDb, serverManager: null!, scheduleManager: null!)
    {
        public Task<DateTime?> TimeAsync(int serverId, string table, string column) =>
            GetLastCollectedTimeAsync(serverId, table, column, CancellationToken.None);

        public Task<(DateTime? Value, bool FromUtcColumn)> FrameAsync(int serverId, string table, string column, string utcColumn) =>
            GetLastCollectedTimeWithFrameAsync(serverId, table, column, utcColumn, CancellationToken.None);

        public Task<DateTime?> DatabaseTimeAsync(int serverId, string table, string column, string databaseName, DateTime? since = null) =>
            GetLastCollectedTimeForDatabaseAsync(serverId, table, column, "database_name", databaseName, CancellationToken.None, since);

        public Task<long?> InstanceIdAsync(int serverId, string table, string column) =>
            GetLastCollectedInstanceIdAsync(serverId, table, column, CancellationToken.None);

        public Task<bool> PriorSuccessAsync(int serverId, string collector) =>
            HasPriorCollectorSuccessAsync(serverId, collector, CancellationToken.None);
    }

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private async Task SeedAsync(params string[] statements)
    {
        await _duckDb.InitializeAsync();
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private Task ResetAsync() => new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync();

    private async Task<long> CountAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private static string Deadlock(long id, int server, DateTime t) =>
        $"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time) VALUES ({id}, {Ts(T3)}, {server}, 'S{server}', {Ts(t)})";

    private static string Bpr(long id, int server, DateTime t) =>
        $"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time) VALUES ({id}, {Ts(T3)}, {server}, 'S{server}', {Ts(t)})";

    [Fact]
    public async Task DeadlockWatermark_AfterRealReset_IsThePreResetMaxPerServer()
    {
        await SeedAsync(
            Deadlock(1, 1, T1), Deadlock(2, 1, T2), Deadlock(3, 1, T3), Deadlock(4, 2, T4));
        await ResetAsync();

        var reads = new Reads(_duckDb);
        Assert.Equal(T3, await reads.TimeAsync(1, "deadlocks", "deadlock_time"));
        Assert.Equal(T4, await reads.TimeAsync(2, "deadlocks", "deadlock_time"));
    }

    [Fact]
    public async Task BlockedProcessReportWatermark_AfterRealReset_IsThePreResetMaxPerServer()
    {
        await SeedAsync(
            Bpr(1, 1, T1), Bpr(2, 1, T2), Bpr(3, 1, T3), Bpr(4, 1, T2), Bpr(5, 2, T4));
        await ResetAsync();

        var reads = new Reads(_duckDb);
        Assert.Equal(T3, await reads.TimeAsync(1, "blocked_process_reports", "event_time"));
        Assert.Equal(T4, await reads.TimeAsync(2, "blocked_process_reports", "event_time"));
    }

    [Theory]
    [InlineData("long_query_completions", "event_time")]
    [InlineData("system_health_events", "event_time")]
    [InlineData("default_trace_events", "event_time")]
    public async Task EventTimeWatermark_AfterRealReset_IsThePreResetMax(string table, string column)
    {
        var idColumn = table switch
        {
            "long_query_completions" => "long_query_completion_id",
            "system_health_events" => "system_health_event_id",
            _ => "default_trace_event_id",
        };
        await SeedAsync(
            $"INSERT INTO {table} ({idColumn}, collection_time, server_id, server_name, {column}) VALUES (1, {Ts(T3)}, 1, 'S1', {Ts(T1)})",
            $"INSERT INTO {table} ({idColumn}, collection_time, server_id, server_name, {column}) VALUES (2, {Ts(T3)}, 1, 'S1', {Ts(T3)})");
        await ResetAsync();

        Assert.Equal(T3, await new Reads(_duckDb).TimeAsync(1, table, column));
    }

    [Fact]
    public async Task MemoryPressureWatermark_AfterRealReset_IsThePreResetMax()
    {
        await SeedAsync(
            $"INSERT INTO memory_pressure_events (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system) VALUES (1, {Ts(T3)}, 1, 'S1', {Ts(T1)}, 'RESOURCE_MEMPHYSICAL_LOW', 1, 1)",
            $"INSERT INTO memory_pressure_events (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system) VALUES (2, {Ts(T3)}, 1, 'S1', {Ts(T3)}, 'RESOURCE_MEMPHYSICAL_LOW', 1, 1)");
        await ResetAsync();

        Assert.Equal(T3, await new Reads(_duckDb).TimeAsync(1, "memory_pressure_events", "sample_time"));
    }

    [Fact]
    public async Task CpuWatermarkFrame_AfterRealReset_IsThePreResetUtcMax()
    {
        var utc1 = T1.AddHours(7);
        var utc3 = T3.AddHours(7);
        await SeedAsync(
            $"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc) VALUES (1, {Ts(T3)}, 1, 'S1', {Ts(T1)}, {Ts(utc1)})",
            $"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc) VALUES (2, {Ts(T3)}, 1, 'S1', {Ts(T3)}, {Ts(utc3)})");
        await ResetAsync();

        var (value, fromUtc) = await new Reads(_duckDb).FrameAsync(1, "cpu_utilization_stats", "sample_time", "sample_time_utc");
        Assert.Equal(utc3, value);
        Assert.True(fromUtc);
    }

    [Fact]
    public async Task QueryStoreWatermark_AfterRealReset_IsThePreResetMaxPerDatabase_WithAndWithoutFloor()
    {
        await SeedAsync(
            $"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, last_execution_time) VALUES (1, {Ts(T2)}, 1, 'S1', 'DbA', {Ts(T1)})",
            $"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, last_execution_time) VALUES (2, {Ts(T3)}, 1, 'S1', 'DbA', {Ts(T3)})",
            $"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, last_execution_time) VALUES (3, {Ts(T3)}, 1, 'S1', 'DbB', {Ts(T2)})");
        await ResetAsync();

        var reads = new Reads(_duckDb);
        Assert.Equal(T3, await reads.DatabaseTimeAsync(1, "query_store_stats", "last_execution_time", "DbA"));
        Assert.Equal(T2, await reads.DatabaseTimeAsync(1, "query_store_stats", "last_execution_time", "DbB"));
        /* A floor older than the newest rows' collection_time still sees them. */
        Assert.Equal(T3, await reads.DatabaseTimeAsync(1, "query_store_stats", "last_execution_time", "DbA", since: T1));
    }

    [Fact]
    public async Task JobHistoryInstanceId_AfterRealReset_IsTheNewestBatchMax()
    {
        const string cols = "(job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, run_status, run_duration_seconds, retries_attempted, run_datetime)";
        await SeedAsync(
            /* The older batch holds the higher id; the newest batch's own max (7) is the watermark. */
            $"INSERT INTO job_history {cols} VALUES (1, {Ts(T1)}, 1, 'S1', 900, 'J', 'Job', true, 0, 1, 1, 0, {Ts(T1)})",
            $"INSERT INTO job_history {cols} VALUES (2, {Ts(T3)}, 1, 'S1', 5, 'J', 'Job', true, 0, 1, 1, 0, {Ts(T2)})",
            $"INSERT INTO job_history {cols} VALUES (3, {Ts(T3)}, 1, 'S1', 7, 'J', 'Job', true, 0, 1, 1, 0, {Ts(T3)})");
        await ResetAsync();

        Assert.Equal(7L, await new Reads(_duckDb).InstanceIdAsync(1, "job_history", "instance_id"));
    }

    [Fact]
    public async Task HasPriorCollectorSuccess_AfterRealReset_StillSeesTheOldSuccessRow()
    {
        await SeedAsync(
            $"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES (1, 1, 'S1', 'default_trace_events', {Ts(T1)}, 'SUCCESS')",
            $"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES (2, 1, 'S1', 'job_history', {Ts(T1)}, 'ERROR')");
        await ResetAsync();

        var reads = new Reads(_duckDb);
        Assert.True(await reads.PriorSuccessAsync(1, "default_trace_events"));
        Assert.False(await reads.PriorSuccessAsync(1, "job_history"));
        Assert.False(await reads.PriorSuccessAsync(2, "default_trace_events"));
    }

    [Fact]
    public async Task AlertAndCollectorState_SurvivesRealReset_Unchanged()
    {
        await SeedAsync(
            $"INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at) VALUES (1, 'Deadlocks', 3, NULL, {Ts(T3)})",
            $"INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at) VALUES (1, 'Failed Job', 0, {Ts(T2)}, {Ts(T3)})",
            $"INSERT INTO config_incident_occurrences (server_id, metric_name, dedup_key, total_occurrences, observed_window_count, incident_started_at, last_observed_at) VALUES (1, 'Blocking', 'k1', 9, 2, {Ts(T1)}, {Ts(T3)})",
            $"INSERT INTO config_alert_persistence_state (server_id, metric_name, consecutive_breaches, consecutive_clears, firing, last_observed_sample_at, updated_at) VALUES (1, 'High CPU', 4, 0, true, {Ts(T2)}, {Ts(T3)})",
            $"INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at) VALUES (1, 'DbA', 'ONLINE', true, {Ts(T3)})",
            $"INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at) VALUES (1, 'default_trace_events', 'last_file', 'log_42.trc', {Ts(T3)})");
        await ResetAsync();

        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_edge_trigger_watermarks WHERE server_id = 1 AND metric_name = 'Deadlocks' AND watermark = 3 AND watermark_time IS NULL"));
        Assert.Equal(1, await CountAsync($"SELECT COUNT(*) FROM config_edge_trigger_watermarks WHERE server_id = 1 AND metric_name = 'Failed Job' AND watermark_time = {Ts(T2)}"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_incident_occurrences WHERE server_id = 1 AND metric_name = 'Blocking' AND dedup_key = 'k1' AND total_occurrences = 9 AND observed_window_count = 2"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_alert_persistence_state WHERE server_id = 1 AND metric_name = 'High CPU' AND consecutive_breaches = 4 AND firing"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_database_state_expected WHERE server_id = 1 AND database_name = 'DbA' AND expected_state = 'ONLINE' AND is_user_override"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM collector_state WHERE server_id = 1 AND collector_name = 'default_trace_events' AND state_key = 'last_file' AND state_value = 'log_42.trc'"));
    }

    [Fact]
    public async Task EdgeTriggerWatermarkLoads_AfterRealReset_ReturnTheSeededRows()
    {
        await SeedAsync(
            $"INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at) VALUES (1, 'Deadlocks', 3, NULL, {Ts(T3)})",
            $"INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at) VALUES (1, 'Failed Agent Job', 0, {Ts(T2)}, {Ts(T3)})");
        await ResetAsync();

        var store = new DuckDbAlertHistoryStore(_duckDb);
        var counts = await store.LoadEdgeTriggerWatermarksAsync();
        var failedJobs = await store.LoadFailedJobWatermarksAsync();

        Assert.Contains(counts, c => c.ServerId == 1 && c.MetricName == "Deadlocks" && c.Watermark == 3);
        Assert.Contains(failedJobs, f => f.ServerId == 1 && f.Watermark == T2);
    }

    // ---- A crash in the reset's restore window is recovered at the next start --------------------

    private const string RestoreMarkerName = PreservedTableRestore.RestoreMarkerFileName;

    private static readonly string[] PreservedTables =
    [
        "config_mute_rules", "dismissed_archive_alerts", "config_edge_trigger_watermarks",
        "config_incident_occurrences", "config_alert_persistence_state", "config_database_state_expected",
        "collector_state", "analysis_muted", "server_tags", "server_tag_map"
    ];

    /// <summary>Rows seeded per preserved table; two in the two tables the pins also count by value.</summary>
    private static readonly Dictionary<string, long> SeededCounts = new()
    {
        ["config_mute_rules"] = 2,
        ["dismissed_archive_alerts"] = 2,
        ["config_edge_trigger_watermarks"] = 2,
        ["config_incident_occurrences"] = 1,
        ["config_alert_persistence_state"] = 1,
        ["config_database_state_expected"] = 1,
        ["collector_state"] = 1,
        ["analysis_muted"] = 1,
        ["server_tags"] = 1,
        ["server_tag_map"] = 1
    };

    private Task SeedAllPreservedTablesAsync() => SeedAsync(
        $"INSERT INTO config_mute_rules (id, enabled, created_at_utc, reason, metric_name) VALUES ('m1', true, {Ts(T1)}, 'first', 'High CPU')",
        $"INSERT INTO config_mute_rules (id, enabled, created_at_utc, reason, server_name) VALUES ('m2', false, {Ts(T2)}, 'second', 'S9')",
        $"INSERT INTO dismissed_archive_alerts (alert_time, server_id, metric_name, dismissed_at) VALUES ({Ts(T1)}, 1, 'Deadlocks', {Ts(T3)})",
        $"INSERT INTO dismissed_archive_alerts (alert_time, server_id, metric_name, dismissed_at) VALUES ({Ts(T2)}, 2, 'Blocking', {Ts(T3)})",
        $"INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at) VALUES (1, 'Deadlocks', 3, NULL, {Ts(T3)})",
        $"INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at) VALUES (1, 'Failed Agent Job', 0, {Ts(T2)}, {Ts(T3)})",
        $"INSERT INTO config_incident_occurrences (server_id, metric_name, dedup_key, total_occurrences, observed_window_count, incident_started_at, last_observed_at) VALUES (1, 'Blocking', 'k1', 9, 2, {Ts(T1)}, {Ts(T3)})",
        $"INSERT INTO config_alert_persistence_state (server_id, metric_name, consecutive_breaches, consecutive_clears, firing, last_observed_sample_at, updated_at) VALUES (1, 'High CPU', 4, 0, true, {Ts(T2)}, {Ts(T3)})",
        $"INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at) VALUES (1, 'DbA', 'ONLINE', true, {Ts(T3)})",
        $"INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at) VALUES (1, 'default_trace_events', 'last_file', 'log_42.trc', {Ts(T3)})",
        "INSERT INTO analysis_muted (mute_id, server_id, database_name, story_path_hash, story_path, reason) VALUES (41, 1, 'DbA', 'hash41', 'a>b>c', 'noise')",
        "INSERT INTO server_tags (id, name, parent_id, sort_order, colour) VALUES (7, 'Prod', NULL, 3, '#ff0000')",
        "INSERT INTO server_tag_map (server_id, tag_id) VALUES (1, 7)");

    private async Task AssertAllPreservedRowsBackAsync()
    {
        foreach (var table in PreservedTables)
        {
            Assert.Equal(SeededCounts[table], await CountAsync($"SELECT COUNT(*) FROM {table}"));
        }

        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_mute_rules WHERE id = 'm1' AND enabled AND reason = 'first' AND metric_name = 'High CPU'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_mute_rules WHERE id = 'm2' AND NOT enabled AND server_name = 'S9'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM dismissed_archive_alerts WHERE server_id = 2 AND metric_name = 'Blocking'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_edge_trigger_watermarks WHERE server_id = 1 AND metric_name = 'Deadlocks' AND watermark = 3"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_incident_occurrences WHERE dedup_key = 'k1' AND total_occurrences = 9"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_alert_persistence_state WHERE metric_name = 'High CPU' AND consecutive_breaches = 4 AND firing"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_database_state_expected WHERE database_name = 'DbA' AND expected_state = 'ONLINE' AND is_user_override"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM collector_state WHERE state_key = 'last_file' AND state_value = 'log_42.trc'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM analysis_muted WHERE mute_id = 41 AND story_path_hash = 'hash41' AND reason = 'noise'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM server_tags WHERE id = 7 AND name = 'Prod' AND sort_order = 3 AND colour = '#ff0000'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM server_tag_map WHERE server_id = 1 AND tag_id = 7"));
    }

    private void AssertNoRestoreMarkerOrPreserveDirectory()
    {
        Assert.False(File.Exists(Path.Combine(_archiveDir, RestoreMarkerName)), "the restore marker must be gone");
        Assert.Empty(Directory.GetDirectories(_archiveDir, "pm_preserve_*"));
    }

    /// <summary>Runs the real reset with a seam that stands in for a process kill, then starts up again.</summary>
    private async Task ResetKilledAsync(Action<ArchiveService> arm)
    {
        var service = new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance);
        arm(service);
        try
        {
            await service.ArchiveAllAndResetAsync();
        }
        catch (ArchiveService.SimulatedKillException)
        {
            /* A real kill ends the process here; the reset may let the exception out or log it. */
        }
    }

    [Fact]
    public async Task KillAfterTheReset_IsRecoveredAtTheNextInitializeAsync()
    {
        await SeedAllPreservedTablesAsync();

        await ResetKilledAsync(s => s.AfterDatabaseResetForTests = () => throw new ArchiveService.SimulatedKillException());
        await _duckDb.InitializeAsync();

        await AssertAllPreservedRowsBackAsync();
        AssertNoRestoreMarkerOrPreserveDirectory();
    }

    [Theory]
    [InlineData("config_mute_rules")]
    [InlineData("dismissed_archive_alerts")]
    public async Task KillMidRestoreLoop_IsFinishedWithoutDuplicates(string killAfterTable)
    {
        await SeedAllPreservedTablesAsync();

        ArchiveService.AfterPreservedTableRestoredForTests = t =>
        {
            if (t == killAfterTable)
            {
                throw new ArchiveService.SimulatedKillException();
            }
        };
        await ResetKilledAsync(_ => { });
        ArchiveService.AfterPreservedTableRestoredForTests = null;
        await _duckDb.InitializeAsync();

        await AssertAllPreservedRowsBackAsync();
        AssertNoRestoreMarkerOrPreserveDirectory();
    }

    [Fact]
    public async Task NormalReset_HoldsTheMarkerAndDirectoryInTheArchiveFolderOnlyDuringTheRestore()
    {
        await SeedAllPreservedTablesAsync();

        bool? markerDuring = null;
        int? directoriesDuring = null;
        await ResetKilledAsync(s => s.AfterDatabaseResetForTests = () =>
        {
            markerDuring = File.Exists(Path.Combine(_archiveDir, RestoreMarkerName));
            directoriesDuring = Directory.GetDirectories(_archiveDir, "pm_preserve_*").Length;
        });

        Assert.True(markerDuring, "the restore marker must exist while the tables are empty");
        Assert.Equal(1, directoriesDuring);
        AssertNoRestoreMarkerOrPreserveDirectory();
        await AssertAllPreservedRowsBackAsync();
    }

    [Fact]
    public async Task ResetThatThrowsBeforeTheDatabaseIsReset_LeavesNoMarkerAndNoDirectory()
    {
        await SeedAllPreservedTablesAsync();

        await ResetKilledAsync(s => s.BeforeDatabaseResetForTests = () => throw new InvalidOperationException("before the reset"));

        AssertNoRestoreMarkerOrPreserveDirectory();
        await AssertAllPreservedRowsBackAsync();
    }

    [Fact]
    public async Task AlertWatermarkLoads_AfterAKillAndTheNextInitializeAsync_ReturnTheSeededRows()
    {
        await SeedAllPreservedTablesAsync();
        await ResetKilledAsync(s => s.AfterDatabaseResetForTests = () => throw new ArchiveService.SimulatedKillException());
        await _duckDb.InitializeAsync();

        var store = new DuckDbAlertHistoryStore(_duckDb);
        var counts = await store.LoadEdgeTriggerWatermarksAsync();
        var failedJobs = await store.LoadFailedJobWatermarksAsync();

        Assert.Contains(counts, c => c.ServerId == 1 && c.MetricName == "Deadlocks" && c.Watermark == 3);
        Assert.Contains(failedJobs, f => f.ServerId == 1 && f.Watermark == T2);
    }

    [Fact]
    public void InitializeAsync_RecoversThePendingRestore_AfterTheSchemaAndBeforeTheSentinelOpens()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(RepoPath("Lite", "Database", "DuckDbInitializer.cs")));
        var body = MethodBody(source, "public async Task InitializeAsync()");

        var core = body.IndexOf("InitializeCoreAsync(", StringComparison.Ordinal);
        var recover = body.IndexOf("RecoverPendingPreservedRestoreCoreAsync(", StringComparison.Ordinal);
        var reopen = body.IndexOf("ReopenSentinel(", StringComparison.Ordinal);

        Assert.True(core >= 0, "InitializeAsync creates the schema");
        Assert.True(recover > core, "the pending restore is recovered after the schema exists");
        Assert.True(reopen > recover, "the sentinel opens only after the pending restore was recovered");
    }

    [Fact]
    public void MainWindowLoaded_InitializesTheDatabaseBeforeAnythingThatReadsOrWritesIt()
    {
        /* Guards the start-up order the recovery relies on; it holds today. */
        var source = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(RepoPath("Lite", "MainWindow.xaml.cs")));
        var body = MethodBody(source, "private async void MainWindow_Loaded(");

        var init = body.IndexOf("_databaseInitializer.InitializeAsync(", StringComparison.Ordinal);
        Assert.True(init >= 0, "MainWindow_Loaded initializes the database");
        foreach (var later in new[] { "new CollectionBackgroundService(", "new AlertEngine(", "_muteRuleService.LoadAsync(", "StartMcpServerAsync(" })
        {
            var at = body.IndexOf(later, StringComparison.Ordinal);
            Assert.True(at > init, $"{later} must come after the database is initialized");
        }
    }

    [Fact]
    public async Task BothMarkersOnDisk_MeansTheResetNeverStarted_TheRestoreIsDiscardedAndTheExportMarkerStays()
    {
        await _duckDb.InitializeAsync();

        const string preserveName = "pm_preserve_x";
        var preserveDir = Path.Combine(_archiveDir, preserveName);
        Directory.CreateDirectory(preserveDir);
        var parquet = Path.Combine(preserveDir, "config_mute_rules.parquet");
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"COPY (SELECT 'm1' AS id, true AS enabled, {Ts(T1)} AS created_at_utc, 'stale' AS reason) TO '{DuckDbInitializer.EscapeSqlPath(parquet)}' (FORMAT PARQUET)";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
        Assert.True(File.Exists(parquet));

        var exportMarker = Path.Combine(_archiveDir, PreservedTableRestore.ResetExportMarkerFileName);
        File.WriteAllText(exportMarker, "20260501_1000_deadlocks.parquet" + Environment.NewLine);
        var restoreMarker = Path.Combine(_archiveDir, RestoreMarkerName);
        File.WriteAllText(restoreMarker, preserveName + Environment.NewLine + "config_mute_rules" + Environment.NewLine);

        await _duckDb.InitializeAsync();

        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM config_mute_rules"));
        Assert.False(File.Exists(restoreMarker), "the restore marker is discarded");
        Assert.False(Directory.Exists(preserveDir), "the preserve directory is discarded");
        Assert.True(File.Exists(exportMarker), "the export marker is left for the first archival run");
    }

    private async Task WriteRestoreMarkerWithParquetCopiesAsync(string preserveName)
    {
        var preserveDir = Path.Combine(_archiveDir, preserveName);
        Directory.CreateDirectory(preserveDir);
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            foreach (var table in PreservedTables)
            {
                var parquet = Path.Combine(preserveDir, table + ".parquet");
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $"COPY (SELECT * FROM {table}) TO '{DuckDbInitializer.EscapeSqlPath(parquet)}' (FORMAT PARQUET)";
                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        }

        File.WriteAllLines(Path.Combine(_archiveDir, RestoreMarkerName), [preserveName, .. PreservedTables]);
    }

    [Fact]
    public async Task WritesBetweenTheResetsTwoLocks_SurviveTheReset()
    {
        await SeedAsync(
            $"INSERT INTO config_mute_rules (id, enabled, created_at_utc, reason) VALUES ('M1', true, {Ts(T1)}, 'doomed')",
            $"INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at) VALUES (1, 'Deadlocks', 5, NULL, {Ts(T3)})");

        ArchiveService.BetweenPreserveCopyAndResetForTests = async () =>
        {
            using var writeLock = _duckDb.AcquireWriteLock();
            using var connection = new DuckDBConnection($"Data Source={_dbPath}");
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            foreach (var sql in new[]
            {
                "DELETE FROM config_mute_rules WHERE id = 'M1'",
                "UPDATE config_edge_trigger_watermarks SET watermark = 9 WHERE server_id = 1 AND metric_name = 'Deadlocks'"
            })
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = sql;
                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        };

        await ResetAsync();

        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM config_mute_rules WHERE id = 'M1'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_edge_trigger_watermarks WHERE metric_name = 'Deadlocks' AND watermark = 9"));
    }

    [Fact]
    public async Task OrphanPreserveDirectoryWithNoRestoreMarker_IsSweptAtInitialize()
    {
        await _duckDb.InitializeAsync();
        var orphan = Path.Combine(_archiveDir, "pm_preserve_orphan");
        Directory.CreateDirectory(orphan);
        File.WriteAllText(Path.Combine(orphan, "config_mute_rules.parquet"), "not read");

        await _duckDb.InitializeAsync();

        Assert.False(Directory.Exists(orphan), "a preserve directory no marker names is removed");
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM config_mute_rules"));
    }

    /// <summary>
    /// A hand-written marker has no identity line, so it is the legacy format, which restores and keeps the
    /// files whatever the database holds. One Theory covers both seeding modes: a database that was never
    /// reset (the legacy form of the crash after the export marker was deleted and before the database
    /// was), and a database a real reset just restored whose process died before deleting the marker (C5).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RestoreMarkerWithTheDatabaseAlreadyFull_InsertsNothingAndCleansUp(bool afterRealReset)
    {
        await SeedAllPreservedTablesAsync();
        if (afterRealReset)
        {
            var last = ArchiveService.PreservedConfigTables[^1];
            await ResetKilledAsync(s => ArchiveService.AfterPreservedTableRestoredForTests =
                table => { if (table == last) throw new ArchiveService.SimulatedKillException(); });
            ArchiveService.AfterPreservedTableRestoredForTests = null;
            Assert.True(File.Exists(Path.Combine(_archiveDir, RestoreMarkerName)), "the kill left the restore marker");
        }
        else
        {
            await WriteRestoreMarkerWithParquetCopiesAsync("pm_preserve_x");
        }

        await _duckDb.InitializeAsync();

        await AssertAllPreservedRowsBackAsync();
        AssertNoRestoreMarkerOrPreserveDirectory();
    }

    // ---- The reset's C2 point, the store identity, an unreadable or pending marker, and tags ------

    private const string PendingMarkerName = PreservedTableRestore.RestoreMarkerFileName;

    private async Task ExecAsync(params string[] statements)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private async Task<string> ScalarTextAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToString(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken)) ?? "";
    }

    private sealed class CapturingLogger : ILogger<DuckDbInitializer>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Add((logLevel, formatter(state, exception)));
    }

    [Fact]
    public async Task KillBetweenExportMarkerDeleteAndDatabaseReset_DoesNotDoubleCount()
    {
        await SeedAllPreservedTablesAsync();
        await ExecAsync(Deadlock(1, 1, T1), Deadlock(2, 1, T2), Deadlock(3, 1, T3), Deadlock(4, 2, T4));

        await ResetKilledAsync(s => ArchiveService.BeforeDatabaseFileResetForTests = () => throw new ArchiveService.SimulatedKillException());
        ArchiveService.BeforeDatabaseFileResetForTests = null;
        await _duckDb.InitializeAsync();

        Assert.Equal(4, await CountAsync("SELECT COUNT(*) FROM v_deadlocks"));
        Assert.Equal(4, await CountAsync("SELECT COUNT(*) FROM deadlocks"));
        Assert.Empty(Directory.GetFiles(_archiveDir, "*deadlocks*.parquet"));
        await AssertAllPreservedRowsBackAsync();
        AssertNoRestoreMarkerOrPreserveDirectory();
    }

    /* Guards the other branch of the same decision: with the database files already deleted, the exported files are
       the only copy, so they stay and the next start restores from them. Passes on the base too. */
    [Fact]
    public async Task KillAfterDatabaseFilesDeleted_RestoresAndKeepsTheFiles()
    {
        await SeedAllPreservedTablesAsync();
        await ExecAsync(Deadlock(1, 1, T1), Deadlock(2, 1, T2), Deadlock(3, 1, T3), Deadlock(4, 2, T4));

        DuckDbInitializer.AfterDatabaseFilesDeletedForTests = () => throw new ArchiveService.SimulatedKillException();
        await ResetKilledAsync(_ => { });
        DuckDbInitializer.AfterDatabaseFilesDeletedForTests = null;
        await _duckDb.InitializeAsync();

        Assert.Equal(4, await CountAsync("SELECT COUNT(*) FROM v_deadlocks"));
        Assert.NotEmpty(Directory.GetFiles(_archiveDir, "*deadlocks*.parquet"));
        await AssertAllPreservedRowsBackAsync();
        AssertNoRestoreMarkerOrPreserveDirectory();
    }

    /* Fails on the base because store_identity does not exist yet (the read throws). */
    [Fact]
    public async Task StoreIdentity_ChangesAcrossAReset()
    {
        await _duckDb.InitializeAsync();
        var before = await ScalarTextAsync("SELECT id FROM store_identity");

        await ResetAsync();
        var after = await ScalarTextAsync("SELECT id FROM store_identity");

        Assert.False(string.IsNullOrEmpty(before));
        Assert.False(string.IsNullOrEmpty(after));
        Assert.NotEqual(before, after);
    }

    private string WriteStrandedPreserveDirectory(string preserveName)
    {
        var dir = Path.Combine(_archiveDir, preserveName);
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "config_mute_rules.parquet"), "the only copy");
        return dir;
    }

    [Theory]
    [InlineData("")]
    [InlineData("../evil")]
    public async Task UnreadableRestoreMarker_KeepsTheCopy_AndLogsAnError(string markerContent)
    {
        var dir = WriteStrandedPreserveDirectory("pm_preserve_x");
        var marker = Path.Combine(_archiveDir, PendingMarkerName);
        File.WriteAllText(marker, markerContent);
        var log = new CapturingLogger();

        await new DuckDbInitializer(_dbPath, log).InitializeAsync();

        Assert.True(Directory.Exists(dir), "an unreadable marker must not make startup delete the preserved copy");
        Assert.True(File.Exists(Path.Combine(dir, "config_mute_rules.parquet")));
        Assert.True(File.Exists(marker), "the marker stays so the problem is not silent");
        Assert.Contains(log.Entries, e => e.Level >= LogLevel.Error);
    }

    [Fact]
    public async Task PendingRestoreMarker_DefersTheNextReset_AndTheCopySurvives()
    {
        await SeedAllPreservedTablesAsync();
        ArchiveService.BeforePreservedTableRestoreForTests = t =>
        {
            if (t == "config_mute_rules") throw new InvalidOperationException("restore failed");
        };
        await ResetAsync();
        ArchiveService.BeforePreservedTableRestoreForTests = null;

        var marker = Path.Combine(_archiveDir, PendingMarkerName);
        Assert.True(File.Exists(marker), "the failed restore left the marker");
        var markerText = File.ReadAllText(marker);
        var directories = Directory.GetDirectories(_archiveDir, "pm_preserve_*");
        Assert.Single(directories);

        /* Written after the first reset, so a second reset that runs would export and clear it. */
        await ExecAsync(Deadlock(9, 1, T1));
        await ResetAsync();

        Assert.True(File.Exists(marker), "the pending marker is still there");
        Assert.Equal(markerText, File.ReadAllText(marker));
        Assert.Equal(directories, Directory.GetDirectories(_archiveDir, "pm_preserve_*"));
        Assert.True(File.Exists(Path.Combine(directories[0], "config_mute_rules.parquet")), "the preserved copy survives");
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM deadlocks WHERE deadlock_id = 9"));
    }

    [Fact]
    public async Task RestoreMarkerListingAMissingParquet_IsAFailure_AndKeepsTheMarker()
    {
        await _duckDb.InitializeAsync();
        var dir = Path.Combine(_archiveDir, "pm_preserve_x");
        Directory.CreateDirectory(dir);
        var marker = Path.Combine(_archiveDir, PendingMarkerName);
        File.WriteAllLines(marker, ["pm_preserve_x", "config_mute_rules"]);

        await _duckDb.InitializeAsync();

        Assert.True(File.Exists(marker), "a listed table with no parquet is a failed restore, so the marker stays");
        Assert.True(Directory.Exists(dir), "and so does the directory");
    }

    [Fact]
    public async Task FailedTagRestore_RefusesTagWrites_ThenRestartRestoresTheRightMeaning()
    {
        await SeedAsync(
            "INSERT INTO server_tags (id, name, parent_id, sort_order, colour) VALUES (1, 'Prod', NULL, 3, '#ff0000')",
            "INSERT INTO server_tag_map (server_id, tag_id) VALUES (1, 1)");
        ArchiveService.BeforePreservedTableRestoreForTests = t =>
        {
            if (t == "server_tags") throw new InvalidOperationException("restore failed");
        };
        await ResetAsync();
        ArchiveService.BeforePreservedTableRestoreForTests = null;
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM server_tags"));

        var data = new LocalDataService(_duckDb);
        await Assert.ThrowsAsync<InvalidOperationException>(() => data.CreateServerTagAsync("Mine", null));
        await Assert.ThrowsAsync<InvalidOperationException>(() => data.RenameServerTagAsync(1, "Mine"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => data.AssignServerTagAsync([2], 1));
        await Assert.ThrowsAsync<InvalidOperationException>(() => data.UnassignServerTagAsync([1], 1));
        await Assert.ThrowsAsync<InvalidOperationException>(() => data.ClearServerTagsForServerAsync(1));

        await _duckDb.InitializeAsync();

        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM server_tags WHERE id = 1 AND name = 'Prod'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM server_tag_map WHERE server_id = 1 AND tag_id = 1"));

        var id = await data.CreateServerTagAsync("Mine", null);
        Assert.NotEqual(1, id);
        Assert.Equal(1, await CountAsync($"SELECT COUNT(*) FROM server_tags WHERE id = {id} AND name = 'Mine'"));
        Assert.Equal(0, await CountAsync($"SELECT COUNT(*) FROM server_tag_map WHERE tag_id = {id}"));
    }

    [Fact]
    public async Task FailedTagMapRestore_RefusesTagDelete()
    {
        await SeedAsync(
            "INSERT INTO server_tags (id, name, parent_id, sort_order, colour) VALUES (1, 'Prod', NULL, 3, '#ff0000')",
            "INSERT INTO server_tag_map (server_id, tag_id) VALUES (1, 1)");
        ArchiveService.BeforePreservedTableRestoreForTests = t =>
        {
            if (t == "server_tag_map") throw new InvalidOperationException("restore failed");
        };
        await ResetAsync();
        ArchiveService.BeforePreservedTableRestoreForTests = null;

        var data = new LocalDataService(_duckDb);
        await Assert.ThrowsAsync<InvalidOperationException>(() => data.DeleteServerTagAsync(1));

        await _duckDb.InitializeAsync();

        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM server_tags WHERE id = 1 AND name = 'Prod'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM server_tag_map WHERE server_id = 1 AND tag_id = 1"));
    }

    [Fact]
    public async Task FailedMuteRuleRestore_RefusesMuteRuleDelete_ThenRestartKeepsTheRule()
    {
        await SeedAsync(
            $"INSERT INTO config_mute_rules (id, enabled, created_at_utc, reason, metric_name) VALUES ('m1', true, {Ts(T1)}, 'first', 'High CPU')");
        ArchiveService.BeforePreservedTableRestoreForTests = t =>
        {
            if (t == "config_mute_rules") throw new InvalidOperationException("restore failed");
        };
        await ResetAsync();
        ArchiveService.BeforePreservedTableRestoreForTests = null;
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM config_mute_rules"));

        var markerPath = Path.Combine(_archiveDir, PendingMarkerName);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => new DuckDbMuteRuleStore(_duckDb).DeleteAsync("m1"));
        Assert.Contains(markerPath, ex.Message);

        await _duckDb.InitializeAsync();

        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM config_mute_rules WHERE id = 'm1' AND enabled AND reason = 'first'"));
    }

    private static string MethodBody(string strippedSource, string signature)
    {
        var at = strippedSource.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{signature} must exist");
        var open = strippedSource.IndexOf('{', at);
        return CSharpSourceWalker.BraceBalanced(strippedSource, open);
    }

    private static string RepoPath(params string[] parts) => Path.Combine([RepoRoot(), .. parts]);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
