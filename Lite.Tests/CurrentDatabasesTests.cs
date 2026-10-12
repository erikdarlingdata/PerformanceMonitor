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
}
