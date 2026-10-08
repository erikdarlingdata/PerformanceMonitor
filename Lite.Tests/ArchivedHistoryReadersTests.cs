using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The 512 MB reset (<see cref="ArchiveService.ArchiveAllAndResetAsync"/>) moves every collected row to Parquet and
/// empties the tables. A reader that asks about a window or about history must still see those rows, through the
/// archive view. Each test seeds one reader's rows, runs the real reset, checks that the table holds nothing for the
/// server, and asks the reader.
/// </summary>
/* ArchiveAllAndResetAsync takes CollectionResetGate and ArchiveService's process-wide s_archiveLock, so this
   class joins the serialized collection CollectionResetGateTests defines. */
[Collection("CollectionResetGate")]
public sealed class ArchivedHistoryReadersTests : IDisposable
{
    private readonly List<DuckDbInitializer> _initializers = [];

    private const int ServerId = TestDataSeeder.TestServerId;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchivedHistoryReadersTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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

    private async Task ExecAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<long> HotRowsAsync(string table)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table} WHERE server_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Writes <paramref name="table"/>'s rows with <paramref name="seed"/>, runs the real reset, and checks that the
    /// table then holds no row for the server, so whatever the reader returns came from the archive.
    /// </summary>
    private async Task<LocalDataService> WriteThenResetAsync(string table, Func<DuckDbInitializer, Task> seed)
    {
        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeAsync();
        await seed(initializer);
        Assert.True(await HotRowsAsync(table) > 0, $"the seed wrote no {table} row");

        await new ArchiveService(initializer, _archiveDir).ArchiveAllAndResetAsync();

        Assert.Equal(0, await HotRowsAsync(table));
        return new LocalDataService(initializer);
    }

    private Task<LocalDataService> SeedThenResetAsync(string table, Func<TestDataSeeder, Task> seed) =>
        WriteThenResetAsync(table, async initializer =>
        {
            using var seeder = new TestDataSeeder(initializer);
            await seed(seeder);
        });

    private static Task<List<RecommendationRow>> RecommendationsAsync(LocalDataService dataService) =>
        dataService.GetRecommendationsAsync(ServerId, "", "", 10000m);

    [Fact]
    public async Task ReservedCapacity_ReadsTheArchivedCpuSamples()
    {
        var dataService = await SeedThenResetAsync("cpu_utilization_stats", s => s.SeedStableCpuForReservedCapacityAsync());

        var recs = await RecommendationsAsync(dataService);

        Assert.Contains(recs, r => r.Category == "Cloud" && r.Finding.Contains("reserved", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task LongRunningJobs_ReadTheArchivedRuns()
    {
        var dataService = await SeedThenResetAsync("running_jobs", s => s.SeedLongRunningJobsAsync());

        var recs = await RecommendationsAsync(dataService);

        Assert.Contains(recs, r => r.Category == "Maintenance");
    }

    [Fact]
    public async Task StorageLatency_ReadsTheArchivedFileStats()
    {
        var dataService = await SeedThenResetAsync("file_io_stats", s => s.SeedLowIoLatencyAsync());

        var recs = await RecommendationsAsync(dataService);

        Assert.Contains(recs, r => r.Category == "Storage");
    }

    [Fact]
    public async Task HighImpactQueries_ReadTheArchivedQueryStats()
    {
        var dataService = await SeedThenResetAsync("query_stats", s => s.SeedHighImpactQuerySkewAsync());
        /* Back from now to the seeded period's start, at any hour the suite runs (as FinOpsTests does). */
        var hoursBack = (int)Math.Ceiling((DateTime.UtcNow - TestDataSeeder.TestPeriodStart).TotalHours) + 1;

        var results = await dataService.GetHighImpactQueriesAsync(ServerId, hoursBack);

        Assert.NotEmpty(results);
        /* The query text comes from two more reads of the same rows; each must find them too. */
        Assert.StartsWith("SELECT /* ", results[0].SampleQueryText, StringComparison.Ordinal);
        Assert.StartsWith("SELECT /* ", results[0].FullQueryText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AgentStatus_ReadsTheArchivedRows()
    {
        const string columns =
            "(collection_id, collection_time, server_id, server_name, agent_running, agent_status_desc, agent_startup_desc, next_scheduled_run)";
        var now = DateTime.UtcNow;
        var dataService = await WriteThenResetAsync("agent_status", _ => ExecAsync(
            $"INSERT INTO agent_status {columns} VALUES "
            + $"(1, TIMESTAMP '{now.AddDays(-10):yyyy-MM-dd HH:mm:ss}', {ServerId}, 'ARCHIVED-HISTORY', true, 'Running', 'Automatic', NULL), "
            + $"(2, TIMESTAMP '{now.AddMinutes(-3):yyyy-MM-dd HH:mm:ss}', {ServerId}, 'ARCHIVED-HISTORY', false, 'Stopped', 'Automatic', NULL)"));

        var row = Assert.Single(await dataService.GetAgentStatusAsync(ServerId));

        Assert.False(row.AgentRunning);
        Assert.True(row.EverSeenRunning);
    }

    [Fact]
    public async Task NeverRan_SeesDatabaseStatesRowsInTheArchive()
    {
        var now = DateTime.UtcNow;
        var dataService = await WriteThenResetAsync("database_states", _ => ExecAsync(
            "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES "
            + $"(1, TIMESTAMP '{now.AddDays(-2):yyyy-MM-dd HH:mm:ss}', {ServerId}, 'ARCHIVED-HISTORY', 'app', 5, 'ONLINE', false)"));

        /* Another collector has logged since the reset, over more than an hour, and database_states never has. */
        await ExecAsync(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES "
            + $"(1, {ServerId}, 'ARCHIVED-HISTORY', 'wait_stats', TIMESTAMP '{now.AddMinutes(-90):yyyy-MM-dd HH:mm:ss}', 'SUCCESS'), "
            + $"(2, {ServerId}, 'ARCHIVED-HISTORY', 'wait_stats', TIMESTAMP '{now.AddMinutes(-1):yyyy-MM-dd HH:mm:ss}', 'SUCCESS')");

        var runs = await dataService.GetCollectorRunHistoryAsync(ServerId);

        Assert.True(runs.ServerHasAnyLogRow);
        Assert.DoesNotContain("database_states", runs.LoggedCollectors);
        Assert.False(runs.NeverRan("database_states"));
    }
}
