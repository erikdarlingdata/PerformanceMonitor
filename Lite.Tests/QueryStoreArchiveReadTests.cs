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

    /// <summary>
    /// The fixture puts the same text on every row, so a lookup that picks the wrong row of a candidate still reads the right text.
    /// This candidate has three archived rows with three texts: the newest and a middle one collected a day to a day and a half before
    /// the window start, and an older one before the lookup's floor. Their runtime intervals start inside the window, which is what
    /// makes the ranking take the candidate (it windows on the interval start), so the newest row is the one the floor must keep.
    /// A floor at the window start drops it and returns the middle row's text, and a lookup that takes the oldest row returns another.
    /// </summary>
    [Fact]
    public async Task TheTopRead_ReturnsTheTextOfTheNewestRow_WhenThatRowWasCollectedJustBeforeTheWindowStart()
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();
        _now = DateTime.UtcNow;
        var windowStart = _now.AddHours(-24);

        using (var connection = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "test.duckdb")}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await QueryStoreArchiveFixture.SeedAsync(connection, _now, 6, [60, 15], 4, 600, TestContext.Current.CancellationToken);
            var rows = new (long Id, DateTime Collected, DateTime IntervalStart, string Text)[]
            {
                (9_000_000_000_001, windowStart.AddHours(-30), windowStart.AddHours(-30), "/* before the floor */ SELECT 1"),
                (9_000_000_000_002, windowStart.AddHours(-12), windowStart.AddHours(2), "/* middle */ SELECT 2"),
                (9_000_000_000_003, windowStart.AddHours(-6), windowStart.AddHours(3), "/* newest */ SELECT 3"),
            };
            foreach (var (id, collected, intervalStart, text) in rows)
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     first_execution_time, last_execution_time, query_text, query_hash, execution_count,
     avg_duration_us, min_duration_us, max_duration_us, avg_cpu_time_us, min_cpu_time_us, max_cpu_time_us,
     avg_logical_io_reads, min_logical_io_reads, max_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     avg_rowcount, min_dop, max_dop, plan_type, is_forced_plan, force_failure_count, query_plan_hash,
     runtime_stats_interval_id, interval_start_time_utc, interval_end_time_utc)
VALUES
    ({id}, TIMESTAMP '{collected:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'SRV{ServerId}', 'DbFloor', 990001, 990001, 'Regular',
     TIMESTAMP '{intervalStart:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{intervalStart:yyyy-MM-dd HH:mm:ss}', '{text}', '0xAB', 1000000,
     500000, 500000, 500000, 400000, 400000, 400000, 100, 100, 100, 10, 10, 5, 1, 1, 'Regular', FALSE, 0, '0xCD',
     {id % 1_000_000}, TIMESTAMP '{intervalStart:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{intervalStart.AddHours(1):yyyy-MM-dd HH:mm:ss}')";
                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        }

        var archive = new ArchiveService(initializer, Path.Combine(_tempDir, "archive"));
        foreach (var days in new[] { 4, 2, 1 })
        {
            archive.TimestampForTests = _now.AddDays(-days).ToString("yyyyMMdd_HHmm");
            await archive.ArchiveOldDataAsync(hotDataDays: days, cancellationToken: TestContext.Current.CancellationToken);
        }

        var data = new LocalDataService(initializer);
        using (var connection = await data.OpenConnectionAsync())
        {
            Assert.Equal(0, await ScalarAsync(connection, "SELECT count(*) FROM query_store_stats WHERE query_id = 990001"));
            Assert.Equal(3, await ScalarAsync(connection, "SELECT count(*) FROM v_query_store_stats WHERE query_id = 990001"));
        }

        var rowsRead = await data.GetQueryStoreTopQueriesAsync(ServerId, 24, 50);

        var candidate = Assert.Single(rowsRead, r => r.QueryId == 990001);
        Assert.Equal("/* newest */ SELECT 3", candidate.QueryText);
    }

    /// <summary>
    /// #9195: the newest row (and its text) of a candidate is read from the hot table when that row is newer than everything archived,
    /// and from the view otherwise. Three candidates with a different text on every row: A has an archived row and a newer hot row (its
    /// answer is the hot row's text), B has an archived row and an OLDER hot row, as a backfilled slice leaves until the next archive pass
    /// (its answer is the archived row's text), and C has archived rows only. Every answer must be the newest row's text in the view.
    /// </summary>
    [Fact]
    public async Task TheTopRead_ReturnsTheTextOfTheNewestRow_WhetherItIsHotOrArchived()
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();
        _now = DateTime.UtcNow;

        async Task InsertAsync(DuckDbInitializer target, long id, DateTime collected, long queryId, string text)
        {
            var intervalStart = collected.AddHours(-1);
            using var readLock = target.AcquireReadLock();
            using var connection = target.CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     first_execution_time, last_execution_time, query_text, query_hash, execution_count,
     avg_duration_us, min_duration_us, max_duration_us, avg_cpu_time_us, min_cpu_time_us, max_cpu_time_us,
     avg_logical_io_reads, min_logical_io_reads, max_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     avg_rowcount, min_dop, max_dop, plan_type, is_forced_plan, force_failure_count, query_plan_hash,
     runtime_stats_interval_id, interval_start_time_utc, interval_end_time_utc)
VALUES
    ({id}, TIMESTAMP '{collected:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'SRV{ServerId}', 'DbHotArchive', {queryId}, {queryId}, 'Regular',
     TIMESTAMP '{intervalStart:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{intervalStart:yyyy-MM-dd HH:mm:ss}', '{text}', '0xAB', 1000000,
     500000, 500000, 500000, 400000, 400000, 400000, 100, 100, 100, 10, 10, 5, 1, 1, 'Regular', FALSE, 0, '0xCD',
     {id % 1_000_000}, TIMESTAMP '{intervalStart:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{intervalStart.AddHours(1):yyyy-MM-dd HH:mm:ss}')";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        using (var connection = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "test.duckdb")}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await QueryStoreArchiveFixture.SeedAsync(connection, _now, 6, [60, 15], 4, 600, TestContext.Current.CancellationToken);
        }

        /* The archived rows: A's older row, B's newest row (a day and a half back) and C's two. */
        await InsertAsync(initializer, 9_000_000_000_011, _now.AddHours(-70), 990201, "/* A old */ SELECT 1");
        await InsertAsync(initializer, 9_000_000_000_021, _now.AddHours(-36), 990202, "/* B archived newest */ SELECT 2");
        await InsertAsync(initializer, 9_000_000_000_031, _now.AddHours(-60), 990203, "/* C older */ SELECT 3");
        await InsertAsync(initializer, 9_000_000_000_032, _now.AddHours(-40), 990203, "/* C newest */ SELECT 4");

        var archive = new ArchiveService(initializer, Path.Combine(_tempDir, "archive"));
        foreach (var days in new[] { 4, 2, 1 })
        {
            archive.TimestampForTests = _now.AddDays(-days).ToString("yyyyMMdd_HHmm");
            await archive.ArchiveOldDataAsync(hotDataDays: days, cancellationToken: TestContext.Current.CancellationToken);
        }

        /* After the archive passes: A's newer row, and B's older row, collected before B's archived one. Both stay in the hot table. */
        await InsertAsync(initializer, 9_000_000_000_012, _now.AddHours(-3), 990201, "/* A hot newest */ SELECT 5");
        await InsertAsync(initializer, 9_000_000_000_022, _now.AddHours(-50), 990202, "/* B hot older */ SELECT 6");

        var data = new LocalDataService(initializer);
        using (var connection = await data.OpenConnectionAsync())
        {
            Assert.Equal(1, await ScalarAsync(connection, "SELECT count(*) FROM query_store_stats WHERE query_id = 990201"));
            Assert.Equal(1, await ScalarAsync(connection, "SELECT count(*) FROM query_store_stats WHERE query_id = 990202"));
            Assert.Equal(0, await ScalarAsync(connection, "SELECT count(*) FROM query_store_stats WHERE query_id = 990203"));
        }

        var rows = await data.GetQueryStoreTopQueriesAsync(ServerId, 168, 50);

        Assert.Equal("/* A hot newest */ SELECT 5", Assert.Single(rows, r => r.QueryId == 990201).QueryText);
        Assert.Equal("/* B archived newest */ SELECT 2", Assert.Single(rows, r => r.QueryId == 990202).QueryText);
        Assert.Equal("/* C newest */ SELECT 4", Assert.Single(rows, r => r.QueryId == 990203).QueryText);
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
