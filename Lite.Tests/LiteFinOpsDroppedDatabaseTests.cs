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
/// #7725: the Lite FinOps grids that keep a dropped database's row mark it, as the Viewer's and the web page's do. The Database Resources,
/// Top Databases (by total CPU and by average CPU) and High Impact reads set <c>IsDropped</c> on a row whose database an earlier
/// <c>database_states</c> snapshot listed and the newest does not, and the grid's cell (<c>DatabaseDisplay</c>) reads "name (dropped)".
/// The plain <c>DatabaseName</c> stays the sort and filter key. With no snapshot nothing is marked.
/// </summary>
public sealed class LiteFinOpsDroppedDatabaseTests : IDisposable
{
    private const int ServerId = 7725;
    private const string Gone = "GoneDb";
    private const string Live = "LiveDb";

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public LiteFinOpsDroppedDatabaseTests()
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

    private async Task ExecAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private Task PlantQueryAsync(string database, string hash, long cpuMicros) =>
        ExecAsync($@"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                         delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
                     VALUES ({_nextId++}, TIMESTAMP '{DateTime.UtcNow.AddHours(-1):yyyy-MM-dd HH:mm:ss}', {ServerId}, 'S', '{database}', '{hash}', 10, {cpuMicros}, {cpuMicros}, 100)");

    private async Task SnapshotAsync(TimeSpan age, params string[] databases)
    {
        var id = 1;
        foreach (var database in databases)
        {
            await ExecAsync($"INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ({_nextId++}, TIMESTAMP '{DateTime.UtcNow - age:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'S', '{database}', {id++}, 'ONLINE', false)");
        }
    }

    [Fact]
    public async Task DatabaseResources_TopDatabases_AndHighImpact_MarkTheDroppedDatabase_AndKeepThePlainNameAsTheKey()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var service = new LocalDataService(_duckDb);

        /* The dropped database is the heavier one. */
        await PlantQueryAsync(Gone, "0xGONE", 900_000);
        await PlantQueryAsync(Live, "0xLIVE", 100_000);

        /* No snapshot: the set is unknown, so nothing is marked and the cell is the plain name. */
        var unknownUsage = await service.GetDatabaseResourceUsageAsync(ServerId);
        Assert.NotEmpty(unknownUsage);
        Assert.DoesNotContain(unknownUsage, r => r.IsDropped);
        Assert.Equal(new[] { Gone, Live }, unknownUsage.Select(r => r.DatabaseDisplay).ToArray());

        /* An earlier snapshot named both; the newest names LiveDb only, so GoneDb was dropped in between. */
        await SnapshotAsync(TimeSpan.FromHours(2), Gone, Live);
        await SnapshotAsync(TimeSpan.FromMinutes(10), Live);

        var usage = await service.GetDatabaseResourceUsageAsync(ServerId);
        Assert.Equal(new[] { Gone, Live }, usage.Select(r => r.DatabaseName).ToArray());
        Assert.Equal(new[] { true, false }, usage.Select(r => r.IsDropped).ToArray());
        Assert.Equal(new[] { Gone + " (dropped)", Live }, usage.Select(r => r.DatabaseDisplay).ToArray());

        var (byTotal, byAvg) = await service.GetTopResourceConsumersAsync(ServerId);
        Assert.NotEmpty(byTotal);
        Assert.NotEmpty(byAvg);
        Assert.Equal(new[] { Gone + " (dropped)", Live }, byTotal.OrderByDescending(r => r.CpuTimeMs).Select(r => r.DatabaseDisplay).ToArray());
        Assert.Equal(new[] { Gone + " (dropped)", Live }, byAvg.OrderByDescending(r => r.CpuTimeMs).Select(r => r.DatabaseDisplay).ToArray());
        Assert.Equal(new[] { Gone, Live }, byTotal.OrderByDescending(r => r.CpuTimeMs).Select(r => r.DatabaseName).ToArray());

        var impact = await service.GetHighImpactQueriesAsync(ServerId);
        Assert.NotEmpty(impact);
        Assert.Equal(Gone + " (dropped)", impact.Single(r => r.QueryHash == "0xGONE").DatabaseDisplay);
        Assert.Equal(Live, impact.Single(r => r.QueryHash == "0xLIVE").DatabaseDisplay);
        Assert.Equal(Gone, impact.Single(r => r.QueryHash == "0xGONE").DatabaseName);
    }

    [Fact]
    public void HighImpactCell_PutsTheDroppedMarkOnTheName_BeforeThePlusMore()
    {
        var row = new HighImpactQueryRow { DatabaseName = Gone, DatabaseCount = 3 };
        Assert.Equal(Gone + " + 2 more", row.DatabaseDisplay);

        row.MarkDropped(true);
        Assert.Equal(Gone + " (dropped) + 2 more", row.DatabaseDisplay);
    }
}
