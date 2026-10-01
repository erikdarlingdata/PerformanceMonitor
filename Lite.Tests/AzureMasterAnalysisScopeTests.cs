using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// On an Azure SQL Database <c>master</c> target, the blocking and deadlock findings skip events that belong
/// to databases monitored as their own targets (<see cref="AnalysisContext.SeparatelyMonitoredDatabases"/>).
/// Pins the BLOCKING_EVENTS and DEADLOCKS facts and both anomaly spike detectors, on the blocked-process arm
/// and the DMV-snapshot arm, and the every-process rule for deadlock graphs. A null or empty list changes nothing.
/// </summary>
public class AzureMasterAnalysisScopeTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4894_07;
    private static readonly DateTime WindowStart = new(2026, 3, 4, 10, 0, 0);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(4);
    private static readonly IReadOnlyList<string> Gp = new[] { "GP" };

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public AzureMasterAnalysisScopeTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _conn?.Dispose();

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

    private Task SeedHistoryAsync() => ExecAsync(
        @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time,
            sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, 'TestServer', $2, 10, 2)",
        _nextId--, WindowEnd.AddDays(-1), ServerId);

    /* Wait-stats deltas give the fact collector its observed time; four hours, one collection every 15 minutes. */
    private async Task SeedObservedWindowAsync()
    {
        for (var i = 0; i <= 16; i++)
            await ExecAsync(
                @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type,
                    wait_time_ms, signal_wait_time_ms, waiting_tasks_count,
                    delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks)
                  VALUES ($1, $2, $3, 'TestServer', 'CXPACKET', 1000, 0, 1, $4, 0, $4)",
                _nextId--, WindowStart.AddMinutes(15 * i), ServerId, i == 0 ? 0L : 1000L);
    }

    private async Task SeedBprAsync(int count, string? database, int offsetMinutes = 0)
    {
        for (var i = 0; i < count; i++)
            await ExecAsync(
                "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, database_name, wait_time_ms) VALUES ($1,$2,$3,'TestServer',$4,1000)",
                _nextId--, WindowStart.AddMinutes(30 + offsetMinutes + (i * 7)), ServerId, database);
    }

    private async Task SeedDmvAsync(int count, string? database)
    {
        for (var i = 0; i < count; i++)
            await ExecAsync(
                "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, database_name, monitor_loop) VALUES ($1,$2,$3,'TestServer',$4,1)",
                _nextId--, WindowStart.AddMinutes(30 + (i * 7)), ServerId, database);
    }

    private static string Graph(params string[] databases) =>
        "<deadlock><victim-list/><process-list>"
        + string.Concat(databases.Select((d, i) => $"<process id=\"p{i}\" currentdbname=\"{d}\"/>"))
        + "</process-list></deadlock>";

    private async Task SeedDeadlocksAsync(int count, params string[] databases)
    {
        for (var i = 0; i < count; i++)
            await ExecAsync(
                "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml) VALUES ($1,$2,$3,'TestServer',$2,$4)",
                _nextId--, WindowStart.AddMinutes(30 + (i * 11)), ServerId, Graph(databases));
    }

    private AnalysisContext Context(IReadOnlyList<string>? scope)
    {
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "TestServer",
            TimeRangeStart = WindowStart,
            TimeRangeEnd = WindowEnd,
            SeparatelyMonitoredDatabases = scope,
        };
        return context;
    }

    private async Task<List<Fact>> FactsAsync(IReadOnlyList<string>? scope)
    {
        var context = Context(scope);
        return await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);
    }

    private async Task<List<Fact>> AnomaliesAsync(IReadOnlyList<string>? scope) =>
        await new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb)).DetectAnomaliesAsync(Context(scope));

    /* ───────────────────────── facts ───────────────────────── */

    [Fact]
    public async Task BlockingEventsFact_SkipsTheSeparatelyMonitoredDatabase_AndKeepsNullDatabaseRows()
    {
        await SeedObservedWindowAsync();
        await SeedBprAsync(6, "GP");
        await SeedBprAsync(2, "gp", offsetMinutes: 3);
        await SeedBprAsync(3, "HS");
        await SeedBprAsync(1, null, offsetMinutes: 5);

        var facts = await FactsAsync(Gp);

        Assert.Equal(4.0, facts.First(f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
    }

    [Fact]
    public async Task DeadlocksFact_SkipsDeadlocksWhoseEveryProcessIsInTheSeparatelyMonitoredDatabase()
    {
        await SeedObservedWindowAsync();
        await SeedDeadlocksAsync(5, "GP", "GP");
        await SeedDeadlocksAsync(2, "GP", "HS");
        await SeedDeadlocksAsync(1, "HS");

        var facts = await FactsAsync(Gp);

        Assert.Equal(3.0, facts.First(f => f.Key == "DEADLOCKS").Metadata["deadlock_count"]);
    }

    [Fact]
    public async Task DeadlocksFact_WhenEveryDeadlockIsTheSeparatelyMonitoredDatabases_EmitsNoFact()
    {
        await SeedObservedWindowAsync();
        await SeedDeadlocksAsync(4, "GP");

        var facts = await FactsAsync(Gp);

        Assert.DoesNotContain(facts, f => f.Key == "DEADLOCKS");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NullOrEmptyList_LeavesEveryFactCountUnchanged(bool emptyList)
    {
        await SeedObservedWindowAsync();
        await SeedBprAsync(6, "GP");
        await SeedBprAsync(3, "HS");
        await SeedDeadlocksAsync(5, "GP");
        await SeedDeadlocksAsync(2, "GP", "HS");

        var facts = await FactsAsync(emptyList ? Array.Empty<string>() : null);

        Assert.Equal(9.0, facts.First(f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
        Assert.Equal(7.0, facts.First(f => f.Key == "DEADLOCKS").Metadata["deadlock_count"]);
    }

    /* ───────────────────────── anomaly spikes ───────────────────────── */

    [Fact]
    public async Task BlockingSpike_OnTheBlockedProcessArm_CountsOnlyOtherDatabases()
    {
        await SeedHistoryAsync();
        await SeedBprAsync(8, "GP");
        await SeedBprAsync(1, "HS");

        Assert.Contains(await AnomaliesAsync(null), f => f.Key == "ANOMALY_BLOCKING_SPIKE");
        Assert.DoesNotContain(await AnomaliesAsync(Gp), f => f.Key == "ANOMALY_BLOCKING_SPIKE");
    }

    [Fact]
    public async Task BlockingSpike_OnTheBlockedProcessArm_StillFiresOnOtherDatabasesAndNullRows()
    {
        await SeedHistoryAsync();
        await SeedBprAsync(8, "GP");
        await SeedBprAsync(3, "HS");
        await SeedBprAsync(2, null, offsetMinutes: 4);

        var spike = (await AnomaliesAsync(Gp)).Single(f => f.Key == "ANOMALY_BLOCKING_SPIKE");

        Assert.Equal(5.0, spike.Value);
    }

    [Fact]
    public async Task BlockingSpike_OnTheDmvSnapshotArm_CountsOnlyOtherDatabases()
    {
        await SeedHistoryAsync();
        await SeedDmvAsync(8, "GP");
        await SeedDmvAsync(1, "HS");

        Assert.Contains(await AnomaliesAsync(null), f => f.Key == "ANOMALY_BLOCKING_SPIKE");
        Assert.DoesNotContain(await AnomaliesAsync(Gp), f => f.Key == "ANOMALY_BLOCKING_SPIKE");
    }

    [Fact]
    public async Task BlockingSpike_OnTheDmvSnapshotArm_StillFiresOnOtherDatabasesAndNullRows()
    {
        await SeedHistoryAsync();
        await SeedDmvAsync(8, "GP");
        await SeedDmvAsync(4, "HS");
        await SeedDmvAsync(1, null);

        var spike = (await AnomaliesAsync(Gp)).Single(f => f.Key == "ANOMALY_BLOCKING_SPIKE");

        Assert.Equal(5.0, spike.Value);
    }

    [Fact]
    public async Task DeadlockSpike_CountsOnlyDeadlocksNotWhollyInTheSeparatelyMonitoredDatabase()
    {
        await SeedHistoryAsync();
        await SeedDeadlocksAsync(6, "GP");
        await SeedDeadlocksAsync(1, "HS");

        Assert.Contains(await AnomaliesAsync(null), f => f.Key == "ANOMALY_DEADLOCK_SPIKE");
        Assert.DoesNotContain(await AnomaliesAsync(Gp), f => f.Key == "ANOMALY_DEADLOCK_SPIKE");
    }

    [Fact]
    public async Task DeadlockSpike_AMixedGraphStillCounts()
    {
        await SeedHistoryAsync();
        await SeedDeadlocksAsync(6, "GP");
        await SeedDeadlocksAsync(3, "GP", "HS");

        var spike = (await AnomaliesAsync(Gp)).Single(f => f.Key == "ANOMALY_DEADLOCK_SPIKE");

        Assert.Equal(3.0, spike.Value);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NullOrEmptyList_LeavesBothSpikesUnchanged(bool emptyList)
    {
        await SeedHistoryAsync();
        await SeedBprAsync(8, "GP");
        await SeedDeadlocksAsync(6, "GP");

        var anomalies = await AnomaliesAsync(emptyList ? Array.Empty<string>() : null);

        Assert.Equal(8.0, anomalies.Single(f => f.Key == "ANOMALY_BLOCKING_SPIKE").Value);
        Assert.Equal(6.0, anomalies.Single(f => f.Key == "ANOMALY_DEADLOCK_SPIKE").Value);
    }
}
