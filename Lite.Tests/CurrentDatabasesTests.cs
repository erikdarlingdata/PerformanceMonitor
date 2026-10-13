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
/// #5633 item 4: <see cref="LocalDataService.GetCurrentDatabasesAsync"/> names the databases of the server's NEWEST
/// <c>database_states</c> snapshot, the set get_top_procedures_by_cpu ranks when no database is named. No snapshot, or one
/// older than the six-hour horizon, is unknown (null), so nothing is filtered.
/// </summary>
public sealed class CurrentDatabasesTests : IDisposable
{
    private const int ServerId = 5633;
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public CurrentDatabasesTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
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

    private async Task SnapshotAsync(TimeSpan age, params string[] databases)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        var at = DateTime.UtcNow - age;
        foreach (var database in databases)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ({_nextId++}, TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'S', '{database}', 5, 'ONLINE', false)";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task NoSnapshot_IsUnknown_TheNewestSnapshotIsTheSet_AndAStaleOneIsUnknown()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var service = new LocalDataService(_duckDb);

        Assert.Null(await service.GetCurrentDatabasesAsync(ServerId));

        /* The older snapshot still names GoneDb; the newest does not. */
        await SnapshotAsync(TimeSpan.FromHours(2), "GoneDb", "LiveDb");
        await SnapshotAsync(TimeSpan.FromMinutes(10), "LiveDb");
        var current = await service.GetCurrentDatabasesAsync(ServerId);
        Assert.Equal(new[] { "LiveDb" }, current!.ToArray());

        /* Seen from a clock past the horizon, the same newest snapshot is stale: unknown. */
        Assert.Null(await service.GetCurrentDatabasesAsync(ServerId, DateTime.UtcNow + LocalDataService.CurrentDatabasesHorizon + TimeSpan.FromHours(1)));
    }

    /// <summary>Review M2: with an as_of the set is the newest snapshot at or before it, horizon measured from it, so a database dropped
    /// since the window still counts for the window.</summary>
    [Fact]
    public async Task AsOf_UsesTheSnapshotAtOrBeforeIt()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var service = new LocalDataService(_duckDb);

        await SnapshotAsync(TimeSpan.FromHours(73), "GoneDb", "LiveDb");
        await SnapshotAsync(TimeSpan.FromMinutes(5), "LiveDb");

        var asOf = DateTime.UtcNow - TimeSpan.FromHours(70);
        var then = await service.GetCurrentDatabasesAsync(ServerId, asOfUtc: asOf);
        Assert.Equal(new[] { "GoneDb", "LiveDb" }, then!.OrderBy(n => n, StringComparer.Ordinal).ToArray());

        /* Past the horizon measured from the anchor, that snapshot is stale: unknown. */
        Assert.Null(await service.GetCurrentDatabasesAsync(ServerId, asOfUtc: asOf + LocalDataService.CurrentDatabasesHorizon));
        /* No anchor: the newest snapshot, as before. */
        Assert.Equal(new[] { "LiveDb" }, (await service.GetCurrentDatabasesAsync(ServerId))!.ToArray());
    }

    private async Task ProcedureAsync(string database, string objectName, long cpuUs)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        var at = DateTime.UtcNow - TimeSpan.FromHours(1);
        cmd.CommandText = $"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time) VALUES ({_nextId++}, TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'S', '{database}', 'dbo', '{objectName}', 'SQL_STORED_PROCEDURE', TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', 10, {cpuUs}, {cpuUs})";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>Review L2 leftover: dropped = listed by an earlier snapshot and absent from the newest. A name no snapshot lists is never in
    /// it; no snapshot or a stale newest one is unknown.</summary>
    [Fact]
    public async Task Dropped_IsWhatAnEarlierSnapshotListedAndTheNewestDoesNot()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var service = new LocalDataService(_duckDb);

        Assert.Null(await service.GetDroppedDatabasesAsync(ServerId));

        await SnapshotAsync(TimeSpan.FromHours(2), "GoneDb", "LiveDb");
        await SnapshotAsync(TimeSpan.FromMinutes(10), "LiveDb");
        Assert.Equal(new[] { "GoneDb" }, (await service.GetDroppedDatabasesAsync(ServerId))!.ToArray());

        /* Stale newest snapshot: unknown, not "everything older is dropped". */
        Assert.Null(await service.GetDroppedDatabasesAsync(ServerId, DateTime.UtcNow + LocalDataService.CurrentDatabasesHorizon + TimeSpan.FromHours(1)));

        /* A window reaching back past the 6-hour horizon still finds a drop from before it. */
        await SnapshotAsync(TimeSpan.FromHours(20), "OlderGoneDb", "LiveDb");
        Assert.Equal(new[] { "GoneDb" }, (await service.GetDroppedDatabasesAsync(ServerId))!.ToArray());
        Assert.Equal(new[] { "GoneDb", "OlderGoneDb" },
            (await service.GetDroppedDatabasesAsync(ServerId, windowStartUtc: DateTime.UtcNow - TimeSpan.FromHours(24)))!.ToArray());
    }

    /// <summary>Review L2 leftover: the ranking leaves out a dropped database and still ranks a database created after the newest snapshot
    /// (no snapshot names it).</summary>
    [Fact]
    public async Task TopProcedures_LeaveOutADroppedDatabase_AndRankADatabaseNoSnapshotNames()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var service = new LocalDataService(_duckDb);
        await SnapshotAsync(TimeSpan.FromHours(2), "GoneDb", "LiveDb");
        await SnapshotAsync(TimeSpan.FromMinutes(10), "LiveDb");
        await ProcedureAsync("GoneDb", "usp_Gone", 900_000);
        await ProcedureAsync("NewDb", "usp_New", 800_000);
        await ProcedureAsync("LiveDb", "usp_Live", 100_000);

        var dropped = await service.GetDroppedDatabasesAsync(ServerId);
        var page = await service.GetTopProceduresByCpuAsync(ServerId, hoursBack: 24, top: 10, excludeDatabaseNames: dropped);
        Assert.Equal(new[] { "usp_New", "usp_Live" }, page.Select(r => r.ObjectName));

        var top1 = await service.GetTopProceduresByCpuAsync(ServerId, hoursBack: 24, top: 1, excludeDatabaseNames: dropped);
        Assert.Equal("usp_New", Assert.Single(top1).ObjectName);

        /* No exclusion: every database, as before. */
        Assert.Equal(3, (await service.GetTopProceduresByCpuAsync(ServerId, hoursBack: 24, top: 10)).Count);
    }
}
