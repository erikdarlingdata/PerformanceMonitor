using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9195: the unfiltered Query Store top read over a store whose history sits mostly in the Parquet archive. After its ranking it
/// looks up each candidate's newest row, and that lookup read every archived row of the server whatever the window was. It now
/// carries a floor every candidate's newest row is at or after, which the Parquet scan can use to skip files and row groups.
/// Each test archives a seeded history through the real archive passes, so about 90 percent of the rows are read from Parquet.
/// </summary>
/* ArchiveOldDataAsync takes ArchiveService's process-wide s_archiveLock, so this class joins the serialized collection the
   other archive tests use. */
[Collection("CollectionResetGate")]
public sealed class QueryStoreArchiveReadTests : IDisposable
{
    private const int ServerId = 1;

    private readonly string _tempDir;
    private readonly List<DuckDbInitializer> _initializers = [];
    private DateTime _now;

    public QueryStoreArchiveReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));
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

    /// <summary>Six days of history for two servers: everything older than a day moves to Parquet in passes that write one file per
    /// pass, the way the archiver does, so the files cover different spans of time.</summary>
    private async Task<LocalDataService> SeedAndArchiveAsync()
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();
        _now = DateTime.UtcNow;

        using (var connection = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "test.duckdb")}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await QueryStoreArchiveFixture.SeedAsync(connection, _now, 6, [60, 15], 4, 600, TestContext.Current.CancellationToken);
        }

        var archive = new ArchiveService(initializer, Path.Combine(_tempDir, "archive"));
        foreach (var days in new[] { 4, 2, 1 })
        {
            archive.TimestampForTests = _now.AddDays(-days).ToString("yyyyMMdd_HHmm");
            await archive.ArchiveOldDataAsync(hotDataDays: days, cancellationToken: TestContext.Current.CancellationToken);
        }

        return new LocalDataService(initializer);
    }

    private static async Task<long> ScalarAsync(LockedConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(24)]
    [InlineData(168)]
    public async Task TheNewestRowLookup_FindsTheSameRowsWithTheFloorAsWithout(int hours)
    {
        var data = await SeedAndArchiveAsync();
        var candidates = await data.GetQueryStoreTopQueriesAsync(ServerId, hours, 300);
        var keys = candidates.Select(r => (r.DatabaseName, r.QueryId)).Distinct().ToList();
        Assert.True(keys.Count > 20, "the comparison needs a spread of candidates");

        using var connection = await data.OpenConnectionAsync();
        var unbounded = await LocalDataService.ReadQueryStoreLatestRowsAsync(connection, ServerId, keys);
        var floored = await LocalDataService.ReadQueryStoreLatestRowsAsync(connection, ServerId, keys, "v_query_store_stats", _now.AddHours(-hours).AddDays(-1));

        Assert.Equal(keys.Count, unbounded.Count);
        Assert.Equal(unbounded.OrderBy(kv => kv.Key).ToList(), floored.OrderBy(kv => kv.Key).ToList());
    }

    [Fact]
    public async Task TheFloor_IsPushedIntoTheParquetScan_SoItReadsFewerRowsThanTheUnboundedLookup()
    {
        var data = await SeedAndArchiveAsync();
        var keys = (await data.GetQueryStoreTopQueriesAsync(ServerId, 24, 300)).Select(r => (r.DatabaseName, r.QueryId)).Distinct().ToList();
        var floor = _now.AddHours(-24).AddDays(-1);

        using var connection = await data.OpenConnectionAsync();
        var archiveRows = await ScalarAsync(connection, $"SELECT count(*) FROM v_query_store_stats WHERE server_id = {ServerId}") - await ScalarAsync(connection, $"SELECT count(*) FROM query_store_stats WHERE server_id = {ServerId}");
        var archiveRowsFromFloor = await ScalarAsync(connection, $"SELECT count(*) FROM v_query_store_stats WHERE server_id = {ServerId} AND collection_time >= TIMESTAMP '{floor:yyyy-MM-dd HH:mm:ss}'") - await ScalarAsync(connection, $"SELECT count(*) FROM query_store_stats WHERE server_id = {ServerId} AND collection_time >= TIMESTAMP '{floor:yyyy-MM-dd HH:mm:ss}'");

        var unbounded = await ParquetRowsScannedAsync(connection, () => LocalDataService.ReadQueryStoreLatestRowsAsync(connection, ServerId, keys));
        var floored = await ParquetRowsScannedAsync(connection, () => LocalDataService.ReadQueryStoreLatestRowsAsync(connection, ServerId, keys, "v_query_store_stats", floor));

        /* The scan is the check: a filter applied after it would leave its row count at the whole archive's. */
        Assert.True(archiveRows > 3 * archiveRowsFromFloor, "the seeded archive must hold far more rows than the floor leaves");
        Assert.True(unbounded > 3 * floored, $"the unbounded lookup scans the whole archive's rows of the server, the floored one a part ({unbounded} against {floored}, of {archiveRows})");
        Assert.True(floored <= archiveRowsFromFloor, $"the floored lookup scans only the rows at or after the floor ({floored} of {archiveRowsFromFloor})");
    }

    [Theory]
    [InlineData(24)]
    [InlineData(168)]
    public async Task TheTopRead_ReturnsEachRowWithTheTextOfItsNewestRow(int hours)
    {
        var data = await SeedAndArchiveAsync();

        var rows = await data.GetQueryStoreTopQueriesAsync(ServerId, hours, 50);

        Assert.Equal(50, rows.Count);
        using var connection = await data.OpenConnectionAsync();
        foreach (var row in rows)
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT query_text FROM v_query_store_stats WHERE server_id = $1 AND database_name = $2 AND query_id = $3 AND query_text IS NOT NULL ORDER BY collection_time DESC, collection_id DESC LIMIT 1";
            command.Parameters.Add(new DuckDBParameter { Value = ServerId });
            command.Parameters.Add(new DuckDBParameter { Value = row.DatabaseName });
            command.Parameters.Add(new DuckDBParameter { Value = row.QueryId });
            var expected = (string?)await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
            Assert.False(string.IsNullOrEmpty(expected));
            Assert.Equal(expected, row.QueryText);
        }

        var cost = rows.Select(r => r.TotalExecutions * r.AvgDurationMs).ToList();
        Assert.Equal(cost.OrderByDescending(c => c).ToList(), cost);
    }

    /// <summary>The rows the Parquet scans of the one statement <paramref name="read"/> runs output, from DuckDB's profile of it.</summary>
    private async Task<long> ParquetRowsScannedAsync<T>(LockedConnection connection, Func<Task<T>> read)
    {
        var profilePath = Path.Combine(_tempDir, "profile_" + Guid.NewGuid().ToString("N")[..6] + ".json").Replace('\\', '/');
        await ExecuteAsync(connection, "PRAGMA enable_profiling = 'json'");
        await ExecuteAsync(connection, $"PRAGMA profiling_output = '{profilePath}'");
        await read();
        await ExecuteAsync(connection, "PRAGMA disable_profiling");

        // A real async file read can resume on another thread while the connection holds the thread-affine read lock (#2463).
        using var profile = JsonDocument.Parse(File.ReadAllText(profilePath));
        long rows = 0;
        var scans = 0;
        Walk(profile.RootElement);
        Assert.True(scans > 0, "the statement must scan the archive");
        return rows;

        void Walk(JsonElement node)
        {
            if (node.TryGetProperty("extra_info", out var info) && info.ValueKind == JsonValueKind.Object
                && info.TryGetProperty("Function", out var function) && function.ToString() == "READ_PARQUET"
                && node.TryGetProperty("operator_cardinality", out var cardinality))
            {
                scans++;
                rows += cardinality.GetInt64();
            }

            if (node.TryGetProperty("children", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Walk(child);
                }
            }
        }
    }

    private static async Task ExecuteAsync(LockedConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
