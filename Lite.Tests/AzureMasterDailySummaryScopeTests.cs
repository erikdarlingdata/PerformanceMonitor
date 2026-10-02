using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The daily summary (Performance Calendar and the MCP daily-summary reads) for an Azure SQL Database master
/// counts only its own blocking and deadlocks per day, so its band follows; the databases monitored as their own
/// targets carry their own days. A SQL Server target and a master with no siblings count what they did.
/// </summary>
[Collection(SeparatelyMonitoredProviderCollection.Name)]
public class AzureMasterDailySummaryScopeTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4927_05;
    private static readonly IReadOnlyList<string> Siblings = new[] { "GP", "HS" };
    private static readonly DateTime Day = DateTime.UtcNow.Date.AddDays(-3);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public AzureMasterDailySummaryScopeTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose()
    {
        AnalysisService.SeparatelyMonitoredDatabasesProvider = null;
        _conn?.Dispose();
    }

    private async Task ExecAsync(string sql, params object?[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _conn ??= _duckDb.CreateConnection();
        if (_conn.State != System.Data.ConnectionState.Open) await _conn.OpenAsync();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
            cmd.Parameters.Add(new DuckDBParameter { Value = arg ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedBprAsync(int count, string database)
    {
        for (var i = 0; i < count; i++)
            await ExecAsync(
                "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, database_name, wait_time_ms) VALUES ($1,$2,$2,$3,'TestServer',$4,1000)",
                _nextId--, Day.AddHours(1).AddMinutes(i), ServerId, database);
    }

    private async Task SeedDmvAsync(int count, string database)
    {
        for (var i = 0; i < count; i++)
            await ExecAsync(
                "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, event_time, server_id, server_name, database_name, monitor_loop, wait_time_ms) VALUES ($1,$2,$2,$3,'TestServer',$4,1,1000)",
                _nextId--, Day.AddHours(1).AddMinutes(i), ServerId, database);
    }

    private static string Graph(params string[] databases) =>
        "<deadlock><victim-list/><process-list>"
        + string.Concat(databases.Select((d, i) => $"<process id=\"p{i}\" currentdbname=\"{d}\"/>"))
        + "</process-list></deadlock>";

    private async Task SeedDeadlocksAsync(int count, params string[] databases)
    {
        for (var i = 0; i < count; i++)
            await ExecAsync(
                "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1,$2,$3,'TestServer',$2,$4,NULL)",
                _nextId--, Day.AddHours(2).AddMinutes(i), ServerId, Graph(databases));
    }

    private async Task<DailySummaryRow> DayAsync()
    {
        var rows = await new LocalDataService(_duckDb).GetDailySummaryRangeAsync(ServerId, Day, Day.AddDays(1));
        return Assert.Single(rows);
    }

    [Fact]
    public async Task MasterWithSiblings_DayCountsOnlyItsOwnEvents()
    {
        await SeedBprAsync(40, "GP");
        await SeedBprAsync(2, "HS");
        await SeedBprAsync(3, "master");
        await SeedDeadlocksAsync(30, "GP");
        await SeedDeadlocksAsync(2, "master");
        await SeedDeadlocksAsync(1, "GP", "master");
        AnalysisService.SeparatelyMonitoredDatabasesProvider = id => id == ServerId ? Siblings : null;

        var day = await DayAsync();

        Assert.Equal(3, day.BlockingEvents);
        Assert.Equal(3, day.DeadlockCount);
    }

    [Fact]
    public async Task MasterWhoseDayIsAllSiblings_BandsHealthy()
    {
        await SeedBprAsync(60, "GP");
        await SeedDeadlocksAsync(40, "HS");
        var unscoped = await DayWithProviderAsync(_ => null);
        var scoped = await DayWithProviderAsync(id => id == ServerId ? Siblings : null);

        Assert.True(unscoped.BlockingEvents > 0 && unscoped.DeadlockCount > 0);
        Assert.Equal(0, scoped.BlockingEvents);
        Assert.Equal(0, scoped.DeadlockCount);
        Assert.True(scoped.HealthBand <= unscoped.HealthBand);
    }

    private async Task<DailySummaryRow> DayWithProviderAsync(Func<int, IReadOnlyList<string>?> provider)
    {
        AnalysisService.SeparatelyMonitoredDatabasesProvider = provider;
        return await DayAsync();
    }

    [Fact]
    public async Task MasterWithSiblings_DmvArmCountsOnlyMasterRows()
    {
        await SeedDmvAsync(5, "GP");
        await SeedDmvAsync(2, "master");
        AnalysisService.SeparatelyMonitoredDatabasesProvider = id => id == ServerId ? Siblings : null;

        Assert.Equal(2, (await DayAsync()).BlockingEvents);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SqlServerTarget_AndSiblinglessMaster_CountEverything(bool emptyList)
    {
        await SeedBprAsync(4, "GP");
        await SeedBprAsync(3, "master");
        await SeedDeadlocksAsync(5, "GP");
        await SeedDeadlocksAsync(3, "master");
        AnalysisService.SeparatelyMonitoredDatabasesProvider = _ => emptyList ? Array.Empty<string>() : null;

        var day = await DayAsync();

        Assert.Equal(7, day.BlockingEvents);
        Assert.Equal(8, day.DeadlockCount);
    }
}
