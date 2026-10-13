using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5643: the collection loop writes a <c>collection_cycle</c> / <c>SKIPPED</c> row to <c>collection_log</c> when it skips
/// slots. The row is a note about the loop, not a collector run, so every reader that counts collectors, runs, "last
/// collected" or "any log row" must leave it out. One test per reader that would otherwise miscount; the readers that key on
/// a collector name or on a status other than SKIPPED need no test (they never see the row), and the raw-log readers show it
/// by design. Each test seeds a real collector row plus a NEWER skipped row, so a reader that counts the skipped row reads a
/// different answer.
/// </summary>
/* Reads the process-wide ServerTimeHelper.UtcOffsetMinutes to build its windows, like CollectionHealthWindowTests. */
[Collection("server-time-helper")]
public sealed class CollectionCycleRowReaderTests : IDisposable
{
    private const int ServerId = 5643;
    private const int OtherServerId = 5644;

    private readonly string _dir = Directory.CreateTempSubdirectory("pm-lite-cycle-row-tests-").FullName;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public CollectionCycleRowReaderTests()
    {
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "pm.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private async Task SeedAsync(int serverId, string collector, DateTime timeUtc, string status, int? rows = 5, int? durationMs = 100)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ($1, $2, 'TestSrv', $3, $4, $5, $6, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = timeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = rows ?? (object)DBNull.Value });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime Floor(DateTime t) => new(t.Ticks - t.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc);

    /// <summary>A real collector run 30 minutes ago and a skipped-slots row one minute ago, on <see cref="ServerId"/>.</summary>
    private async Task<(DateTime Real, DateTime Skipped)> SeedRealAndSkippedAsync()
    {
        var real = Floor(DateTime.UtcNow.AddMinutes(-30));
        var skipped = Floor(DateTime.UtcNow.AddMinutes(-1));
        await SeedAsync(ServerId, "wait_stats", real, "SUCCESS");
        await SeedAsync(ServerId, CollectionCycleSkip.CollectorName, skipped, CollectionCycleSkip.Status, rows: 0, durationMs: 312_000);
        return (real, skipped);
    }

    [Fact]
    public async Task CollectionHealth_ListsNoCollectionCycleCollector_AndItsSkippedStatusIsNoSuccess()
    {
        var (real, _) = await SeedRealAndSkippedAsync();

        var rows = await new LocalDataService(_duckDb).GetCollectionHealthAsync(ServerId);

        var only = Assert.Single(rows);
        Assert.Equal("wait_stats", only.CollectorName);
        Assert.Equal(real, only.LastSuccessTime);
        Assert.Equal(1, only.TotalRuns);
    }

    [Fact]
    public async Task HasAnyCollectionLog_IsFalseForAServerWhoseOnlyRowIsTheSkippedCycle_OnOneServerAndTheFleet()
    {
        await SeedAsync(OtherServerId, CollectionCycleSkip.CollectorName, Floor(DateTime.UtcNow), CollectionCycleSkip.Status, rows: 0);
        await SeedAsync(ServerId, "wait_stats", Floor(DateTime.UtcNow.AddMinutes(-5)), "SUCCESS");
        var service = new LocalDataService(_duckDb);

        Assert.False(await service.HasAnyCollectionLogAsync(OtherServerId));
        Assert.False(await service.HasAnyCollectionLogFleetAsync([OtherServerId]));
        Assert.True(await service.HasAnyCollectionLogAsync(ServerId));
        Assert.True(await service.HasAnyCollectionLogFleetAsync([OtherServerId, ServerId]));
    }

    [Fact]
    public async Task CollectorRunHistory_LeavesTheSkippedCycleOutOfTheLoggedCollectorsAndTheServersLastCollected()
    {
        var (real, _) = await SeedRealAndSkippedAsync();
        await SeedAsync(OtherServerId, CollectionCycleSkip.CollectorName, Floor(DateTime.UtcNow), CollectionCycleSkip.Status, rows: 0);
        var service = new LocalDataService(_duckDb);

        var history = await service.GetCollectorRunHistoryAsync(ServerId);
        var onlySkipped = await service.GetCollectorRunHistoryAsync(OtherServerId);

        Assert.Equal(["wait_stats"], history.LoggedCollectors.ToArray());
        Assert.Equal(real, history.ServerLastCollectedUtc);
        Assert.False(onlySkipped.ServerHasAnyLogRow);
        Assert.Empty(onlySkipped.LoggedCollectors);
        Assert.Null(onlySkipped.ServerLastCollectedUtc);
    }

    [Fact]
    public async Task CollectorLastRun_ReportsTheServersLastAndFirstCollectedWithoutTheSkippedCycle()
    {
        var (real, _) = await SeedRealAndSkippedAsync();

        var (collectorLast, serverLast, serverFirst) = await new LocalDataService(_duckDb).GetCollectorLastRunAsync(ServerId, "wait_stats");

        Assert.Equal(real, collectorLast);
        Assert.Equal(real, serverLast);
        Assert.Equal(real, serverFirst);
    }

    [Fact]
    public async Task ServerOverview_LastCollectionTime_IsTheLastCollectorRun_NotTheSkippedCycle()
    {
        var (real, _) = await SeedRealAndSkippedAsync();

        var summary = await new LocalDataService(_duckDb).GetServerSummaryAsync(ServerId, "TestSrv", registeredAtUtc: null);

        Assert.NotNull(summary);
        Assert.Equal(real, summary!.LastCollectionTime);
    }

    [Fact]
    public async Task DailySummary_CollectionRuns_DoesNotCountTheSkippedCycle()
    {
        var day = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);
        await SeedAsync(ServerId, "wait_stats", day, "SUCCESS");
        await SeedAsync(ServerId, "wait_stats", day.AddMinutes(1), "ERROR");
        await SeedAsync(ServerId, CollectionCycleSkip.CollectorName, day.AddMinutes(2), CollectionCycleSkip.Status, rows: 0);

        var rows = await new LocalDataService(_duckDb).GetDailySummaryRangeAsync(ServerId, day.Date, day.Date.AddDays(1));

        var summary = Assert.Single(rows);
        Assert.Equal(2, summary.CollectionRuns);
        Assert.Equal(1, summary.CollectionErrors);
    }

    [Fact]
    public async Task ConfigSnapshotCoverage_IsNotProvedByTheSkippedCycleAlone()
    {
        var windowStart = Floor(DateTime.UtcNow.AddHours(-2));
        var windowEnd = Floor(DateTime.UtcNow);
        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES (1, $1, $2, 'TestSrv', 'max degree of parallelism', 0, 0, true, true)";
            cmd.Parameters.Add(new DuckDBParameter { Value = windowStart.AddDays(-2) });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var service = new LocalDataService(_duckDb);
        await SeedAsync(ServerId, CollectionCycleSkip.CollectorName, windowEnd.AddMinutes(-10), CollectionCycleSkip.Status, rows: 0);

        /* The snapshot before the window proves coverage only if the server was monitored in the window; a skipped-slots
           row is the loop talking about itself, not a collection. */
        Assert.Null(await service.GetConfigSnapshotCoverageFloorAsync(QueryWindowRelation.ServerConfig, ServerId, windowStart, windowEnd));

        await SeedAsync(ServerId, "wait_stats", windowEnd.AddMinutes(-20), "SUCCESS");
        Assert.Equal(windowStart, await service.GetConfigSnapshotCoverageFloorAsync(QueryWindowRelation.ServerConfig, ServerId, windowStart, windowEnd));
    }
}
