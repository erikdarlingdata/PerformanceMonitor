using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
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
}
