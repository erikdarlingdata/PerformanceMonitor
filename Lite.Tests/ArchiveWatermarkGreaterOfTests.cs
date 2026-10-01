using System;
using System.Collections;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// After the archive-and-reset, a live table can hold a row that is OLDER than what the archive holds
/// (a hole fill, a late row). Every high-water-mark read must return the GREATER of the live and the
/// archived value, the backfill's stored-floor read must see archived history, and the archive-side cache
/// must stay bounded. These pins run the REAL <see cref="ArchiveService.ArchiveAllAndResetAsync"/> on a
/// seeded DuckDB file and then call the collector's real reads.
/// </summary>
/* ArchiveAllAndResetAsync touches CollectionResetGate and ArchiveService's static archive lock, both
   process-wide, so this class joins the serialized collection the other reset tests use. */
[Collection("CollectionResetGate")]
public sealed class ArchiveWatermarkGreaterOfTests : IDisposable
{
    private static readonly DateTime T0 = new(2026, 5, 1, 9, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T1 = new(2026, 5, 1, 10, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T2 = new(2026, 5, 1, 10, 5, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T3 = new(2026, 5, 1, 10, 10, 0, DateTimeKind.Unspecified);

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;

    public ArchiveWatermarkGreaterOfTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        CollectionResetGate.ResetForTests();
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

    /// <summary>Exposes the runner's reads; only the initializer is exercised.</summary>
    private sealed class Reads(DuckDbInitializer duckDb)
        : RemoteCollectorService(duckDb, serverManager: null!, scheduleManager: null!)
    {
        public Task<DateTime?> TimeAsync(int serverId, string table, string column) =>
            GetLastCollectedTimeAsync(serverId, table, column, CancellationToken.None);

        public Task<(DateTime? Value, bool FromUtcColumn)> FrameAsync(int serverId, string table, string column, string utcColumn) =>
            GetLastCollectedTimeWithFrameAsync(serverId, table, column, utcColumn, CancellationToken.None);

        public Task<DateTime?> DatabaseTimeAsync(int serverId, string table, string column, string databaseName, DateTime? since = null) =>
            GetLastCollectedTimeForDatabaseAsync(serverId, table, column, "database_name", databaseName, CancellationToken.None, since);

        public Task<long?> InstanceIdAsync(int serverId, string table, string column) =>
            GetLastCollectedInstanceIdAsync(serverId, table, column, CancellationToken.None);

        public Task<DateTime?> FloorAsync(int serverId, string databaseName, DateTime floorLimit) =>
            GetMinCollectedTimeForDatabaseAsync(
                serverId, "query_store_stats", "last_execution_time", "database_name", databaseName, floorLimit, CancellationToken.None);

        /// <summary>The number of entries the archive-side cache holds, read by reflection.</summary>
        public int CacheEntryCount()
        {
            var cacheField = typeof(RemoteCollectorService)
                .GetFields(BindingFlags.Instance | BindingFlags.NonPublic)
                .Single(f => f.FieldType == typeof(ArchiveWatermarkCache));
            var cache = cacheField.GetValue(this)!;
            var entries = (ICollection)typeof(ArchiveWatermarkCache)
                .GetField("_entries", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(cache)!;
            return entries.Count;
        }
    }

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private async Task ExecuteAsync(params string[] statements)
    {
        await _duckDb.InitializeAsync();
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private Task ResetAsync() => new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync();

    private static string Deadlock(long id, int server, DateTime collected, DateTime t) =>
        $"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time) VALUES ({id}, {Ts(collected)}, {server}, 'S{server}', {Ts(t)})";

    private static string QueryStore(long id, DateTime collected, string db, DateTime lastExecution) =>
        $"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, last_execution_time) VALUES ({id}, {Ts(collected)}, 1, 'S1', '{db}', {Ts(lastExecution)})";

    private const string JobCols = "(job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, run_status, run_duration_seconds, retries_attempted, run_datetime)";

    private static string Job(long id, DateTime collected, long instanceId) =>
        $"INSERT INTO job_history {JobCols} VALUES ({id}, {Ts(collected)}, 1, 'S1', {instanceId}, 'J', 'Job', true, 0, 1, 1, 0, {Ts(collected)})";

    [Fact]
    public async Task Watermark_LiveRowOlderThanTheArchive_ReturnsTheArchiveMax()
    {
        await ExecuteAsync(Deadlock(1, 1, T3, T1), Deadlock(2, 1, T3, T2), Deadlock(3, 1, T3, T3));
        await ResetAsync();
        await ExecuteAsync(Deadlock(10, 1, T3, T0));

        Assert.Equal(T3, await new Reads(_duckDb).TimeAsync(1, "deadlocks", "deadlock_time"));
    }

    [Fact]
    public async Task Watermark_LiveRowNewerThanTheArchive_ReturnsTheLiveMax()
    {
        await ExecuteAsync(Deadlock(1, 1, T3, T1), Deadlock(2, 1, T3, T2));
        await ResetAsync();
        await ExecuteAsync(Deadlock(10, 1, T3, T3));

        Assert.Equal(T3, await new Reads(_duckDb).TimeAsync(1, "deadlocks", "deadlock_time"));
    }

    [Fact]
    public async Task DatabaseWatermark_LiveRowOlderThanTheArchive_ReturnsTheArchiveMax()
    {
        await ExecuteAsync(QueryStore(1, T2, "DbA", T1), QueryStore(2, T3, "DbA", T3));
        await ResetAsync();
        await ExecuteAsync(QueryStore(10, T3, "DbA", T0));

        var reads = new Reads(_duckDb);
        Assert.Equal(T3, await reads.DatabaseTimeAsync(1, "query_store_stats", "last_execution_time", "DbA", since: T0));
        Assert.Equal(T3, await reads.DatabaseTimeAsync(1, "query_store_stats", "last_execution_time", "DbA"));
    }

    [Fact]
    public async Task CpuFrame_LiveUtcOlderThanTheArchiveUtc_ReturnsTheArchiveUtc()
    {
        var utc1 = T1.AddHours(7);
        var utc3 = T3.AddHours(7);
        const string cols = "(collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc)";
        await ExecuteAsync(
            $"INSERT INTO cpu_utilization_stats {cols} VALUES (1, {Ts(T3)}, 1, 'S1', {Ts(T1)}, {Ts(utc1)})",
            $"INSERT INTO cpu_utilization_stats {cols} VALUES (2, {Ts(T3)}, 1, 'S1', {Ts(T3)}, {Ts(utc3)})");
        await ResetAsync();
        await ExecuteAsync($"INSERT INTO cpu_utilization_stats {cols} VALUES (10, {Ts(T3)}, 1, 'S1', {Ts(T0)}, {Ts(T0.AddHours(7))})");

        var (value, fromUtc) = await new Reads(_duckDb).FrameAsync(1, "cpu_utilization_stats", "sample_time", "sample_time_utc");
        Assert.Equal(utc3, value);
        Assert.True(fromUtc);
    }

    [Fact]
    public async Task JobHistoryInstanceId_OlderLiveBatch_ReturnsTheArchivedNewestBatchMax()
    {
        await ExecuteAsync(Job(1, T3, 5), Job(2, T3, 7));
        await ResetAsync();
        await ExecuteAsync(Job(10, T1, 900));

        Assert.Equal(7L, await new Reads(_duckDb).InstanceIdAsync(1, "job_history", "instance_id"));
    }

    [Fact]
    public async Task JobHistoryInstanceId_NewerLiveBatch_ReturnsTheLiveBatchMax()
    {
        await ExecuteAsync(Job(1, T2, 900), Job(2, T2, 7));
        await ResetAsync();
        await ExecuteAsync(Job(10, T3, 3));

        Assert.Equal(3L, await new Reads(_duckDb).InstanceIdAsync(1, "job_history", "instance_id"));
    }

    [Fact]
    public async Task BackfillFloor_PostResetLiveRow_StillReturnsTheArchivedOldestExecution()
    {
        var floorLimit = T0;
        var e1 = T1;
        var e9 = T3.AddHours(1);
        await ExecuteAsync(QueryStore(1, T2, "DbA", e1), QueryStore(2, T3, "DbA", T3));
        await ResetAsync();
        await ExecuteAsync(QueryStore(10, T3, "DbA", e9));

        Assert.Equal(e1, await new Reads(_duckDb).FloorAsync(1, "DbA", floorLimit));
    }

    [Fact]
    public async Task BackfillFloor_ArchivedRowAtOrBeforeTheLimit_ReportsTheLimit()
    {
        var floorLimit = T2;
        await ExecuteAsync(QueryStore(1, T1, "DbA", T1));
        await ResetAsync();
        await ExecuteAsync(QueryStore(10, T3, "DbA", T3));

        Assert.Equal(floorLimit, await new Reads(_duckDb).FloorAsync(1, "DbA", floorLimit));
    }

    [Fact]
    public async Task DatabaseWatermarkCache_ManyFloorsForAnArchivedOnlyKey_StaysBounded()
    {
        await ExecuteAsync(QueryStore(1, T2, "DbA", T1), QueryStore(2, T3, "DbA", T3));
        await ResetAsync();

        var reads = new Reads(_duckDb);
        for (var i = 0; i < 50; i++)
        {
            var value = await reads.DatabaseTimeAsync(
                1, "query_store_stats", "last_execution_time", "DbA", since: T0.AddMinutes(i));
            Assert.Equal(T3, value);
        }

        Assert.True(reads.CacheEntryCount() <= 3, $"cache holds {reads.CacheEntryCount()} entries");
    }

    [Fact]
    public async Task DatabaseWatermark_IdleDatabaseOlderThanTheFloor_ReadsNoWatermark()
    {
        /* An idle Query Store: every row, archived by a real reset and one live, is 6 h before "now". The
           floor (now - 3 h) is a semantic bound (#2344: no recent rows means NULL), so the callers see no
           stored watermark and neither warn about a clamp nor record a backfill hole. */
        var now = new DateTime(2026, 5, 1, 18, 0, 0, DateTimeKind.Unspecified);
        var old = now.AddHours(-6);
        await ExecuteAsync(QueryStore(1, old, "DbA", old), QueryStore(2, old, "DbA", old));
        await ResetAsync();
        await ExecuteAsync(QueryStore(10, old, "DbA", old));

        Assert.Null(await new Reads(_duckDb).DatabaseTimeAsync(
            1, "query_store_stats", "last_execution_time", "DbA", since: now.AddHours(-3)));
    }

    [Fact]
    public async Task DatabaseWatermark_ArchivedRowAboveTheFloor_ReturnsThatRow()
    {
        var now = new DateTime(2026, 5, 1, 18, 0, 0, DateTimeKind.Unspecified);
        var recent = now.AddHours(-1);
        await ExecuteAsync(QueryStore(1, now.AddHours(-6), "DbA", now.AddHours(-6)), QueryStore(2, recent, "DbA", recent));
        await ResetAsync();

        Assert.Equal(recent, await new Reads(_duckDb).DatabaseTimeAsync(
            1, "query_store_stats", "last_execution_time", "DbA", since: now.AddHours(-3)));
    }

    [Fact]
    public async Task DatabaseWatermark_IdleArchiveAndARecentLiveRow_ReturnsTheLiveRow()
    {
        var now = new DateTime(2026, 5, 1, 18, 0, 0, DateTimeKind.Unspecified);
        var old = now.AddHours(-6);
        var recent = now.AddHours(-1);
        await ExecuteAsync(QueryStore(1, old, "DbA", old));
        await ResetAsync();
        await ExecuteAsync(QueryStore(10, recent, "DbA", recent));

        Assert.Equal(recent, await new Reads(_duckDb).DatabaseTimeAsync(
            1, "query_store_stats", "last_execution_time", "DbA", since: now.AddHours(-3)));
    }
}
