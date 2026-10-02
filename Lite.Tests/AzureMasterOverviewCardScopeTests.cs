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
/// The overview card's last-hour blocking and deadlock counts for an Azure SQL Database master registration
/// skip events of databases monitored as their own targets (the list analysis uses). A SQL Server target and a
/// master with no siblings count exactly what they did.
/// </summary>
[Collection(SeparatelyMonitoredProviderCollection.Name)]
public class AzureMasterOverviewCardScopeTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4924_03;
    private static readonly IReadOnlyList<string> Siblings = new[] { "GP", "HS" };

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public AzureMasterOverviewCardScopeTests(SharedDuckDbFixture fixture)
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

    private async Task SeedBprAsync(int count, string? database)
    {
        for (var i = 0; i < count; i++)
            await ExecAsync(
                "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, database_name, wait_time_ms) VALUES ($1,$2,$2,$3,'TestServer',$4,1000)",
                _nextId--, DateTime.UtcNow.AddMinutes(-10 - i), ServerId, database);
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
                _nextId--, DateTime.UtcNow.AddMinutes(-10 - i), ServerId, Graph(databases));
    }

    private async Task SeedDmvAsync(int count, string? database)
    {
        for (var i = 0; i < count; i++)
            await ExecAsync(
                "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, event_time, server_id, server_name, database_name, monitor_loop, wait_time_ms) VALUES ($1,$2,$2,$3,'TestServer',$4,1,1000)",
                _nextId--, DateTime.UtcNow.AddMinutes(-10 - i), ServerId, database);
    }

    private async Task SeedDeadlockRowAsync(string rowDatabase, params string[] graphDatabases) =>
        await ExecAsync(
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1,$2,$3,'TestServer',$2,$4,$5)",
            _nextId--, DateTime.UtcNow.AddMinutes(-10), ServerId, Graph(graphDatabases), rowDatabase);

    private async Task<ServerSummaryItem> CardAsync() =>
        (await new LocalDataService(_duckDb).GetServerSummaryAsync(ServerId, "TestServer", registeredAtUtc: null))!;

    [Fact]
    public async Task MasterWithSiblings_CountsOnlyItsOwnBlockingAndDeadlocks()
    {
        await SeedBprAsync(4, "GP");
        await SeedBprAsync(2, "hs");
        await SeedBprAsync(3, "master");
        await SeedBprAsync(1, null);
        await SeedDeadlocksAsync(5, "GP");
        await SeedDeadlocksAsync(2, "GP", "HS");
        await SeedDeadlocksAsync(3, "master");
        await SeedDeadlocksAsync(1, "GP", "master");
        AnalysisService.SeparatelyMonitoredDatabasesProvider = id => id == ServerId ? Siblings : null;

        var card = await CardAsync();

        Assert.Equal(4, card.BlockingCount);
        Assert.Equal(4, card.DeadlockCount);
    }

    [Fact]
    public async Task MasterWhoseEventsAreAllSiblings_ShowsNoAlertAndAHealthyBand()
    {
        /* Every event belongs to GP or HS, which have their own cards: master's card (the unit the status
           word, the border, the tooltip, the alert sweep and any total are built from) must read clean. */
        await SeedBprAsync(25, "GP");
        await SeedBprAsync(24, "HS");
        await SeedDeadlocksAsync(23, "GP");
        await SeedDeadlocksAsync(24, "HS");
        AnalysisService.SeparatelyMonitoredDatabasesProvider = id => id == ServerId ? Siblings : null;

        var card = await CardAsync();

        Assert.Equal(0, card.BlockingCount);
        Assert.Equal(0, card.DeadlockCount);
        Assert.False(card.HasAlerts);
        Assert.DoesNotContain("eadlock", card.StatusReason);
        Assert.DoesNotContain("locking", card.StatusReason);
    }

    [Fact]
    public async Task SqlServerTarget_CountsEverything()
    {
        await SeedBprAsync(4, "GP");
        await SeedBprAsync(3, "master");
        await SeedDeadlocksAsync(5, "GP");
        await SeedDeadlocksAsync(3, "master");
        AnalysisService.SeparatelyMonitoredDatabasesProvider = _ => null;

        var card = await CardAsync();

        Assert.Equal(7, card.BlockingCount);
        Assert.Equal(8, card.DeadlockCount);
    }

    [Fact]
    public async Task MasterWithoutSiblings_CountsEverything()
    {
        await SeedBprAsync(4, "GP");
        await SeedBprAsync(3, "master");
        await SeedDeadlocksAsync(5, "GP");
        await SeedDeadlocksAsync(3, "master");
        AnalysisService.SeparatelyMonitoredDatabasesProvider = _ => Array.Empty<string>();

        var card = await CardAsync();

        Assert.Equal(7, card.BlockingCount);
        Assert.Equal(8, card.DeadlockCount);
    }

    [Fact]
    public async Task Deadlock_WithRowDatabaseGp_AndAGpOnlyGraph_IsSkipped()
    {
        await SeedDeadlockRowAsync("GP", "GP");
        AnalysisService.SeparatelyMonitoredDatabasesProvider = id => id == ServerId ? Siblings : null;

        Assert.Equal(0, (await CardAsync()).DeadlockCount);
    }

    [Fact]
    public async Task Deadlock_WithRowDatabaseMaster_AndAGpAndMasterGraph_IsCounted()
    {
        await SeedDeadlockRowAsync("master", "GP", "master");
        AnalysisService.SeparatelyMonitoredDatabasesProvider = id => id == ServerId ? Siblings : null;

        Assert.Equal(1, (await CardAsync()).DeadlockCount);
    }

    [Fact]
    public async Task DmvSnapshotFallback_CountsOnlyMasterRows()
    {
        await SeedDmvAsync(4, "GP");
        await SeedDmvAsync(3, "master");
        AnalysisService.SeparatelyMonitoredDatabasesProvider = id => id == ServerId ? Siblings : null;

        Assert.Equal(3, (await CardAsync()).BlockingCount);
    }
}
