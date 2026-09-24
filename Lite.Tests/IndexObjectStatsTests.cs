using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests for the #1103 index/object-stats read layer (DuckDB dialect: date_diff,
/// GREATEST, LIMIT, unused-index classification) and the delta-based growth/contention
/// anomaly detection. Seeds two daily snapshots and exercises both paths.
/// </summary>
public class IndexObjectStatsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly AnomalyDetector _detector;
    private DuckDBConnection? _seedConn;

    private const int ServerId = -777;
    private static readonly DateTime _latest = DateTime.UtcNow;
    private static readonly DateTime _prior = _latest.AddDays(-1);
    private static readonly DateTime _startTime = _latest.AddDays(-10); // instance start (no reset)
    private long _nextId = -1;

    public IndexObjectStatsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        var baselineProvider = new BaselineProvider(_duckDb);
        _detector = new AnomalyDetector(_duckDb, baselineProvider);
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// One connection reused for every seeded row — opening a fresh connection per
    /// single-row INSERT measured ~90ms/row and dominated this class's runtime.
    /// </summary>
    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    // ── helpers ──

    private async Task SeedScenarioAsync()
    {
        // BigTable (object 100): 200 MB -> 600 MB  => +400 MB / 200% growth
        await InsertObjectStat(_prior, "AppDb", 100, 1, "dbo", "BigTable", "PK_BigTable", 200m, 1_000_000, 0, 0, 0, 0, 0, 0);
        await InsertObjectStat(_latest, "AppDb", 100, 1, "dbo", "BigTable", "PK_BigTable", 600m, 3_000_000, 5000, 100, 10, 50, 0, 0);

        // IX_unused (object 200 index 2): 0 reads, 500 updates in latest => Write-only
        await InsertObjectStat(_prior, "AppDb", 200, 2, "dbo", "T2", "IX_unused", 50m, 500_000, 0, 0, 0, 0, 0, 0);
        await InsertObjectStat(_latest, "AppDb", 200, 2, "dbo", "T2", "IX_unused", 50m, 500_000, 0, 0, 0, 500, 0, 0);

        // HotTable (object 300 index 1): 10000ms -> 100000ms lock wait => +90000ms contention
        await InsertObjectStat(_prior, "AppDb", 300, 1, "dbo", "HotTable", "PK_HotTable", 80m, 250_000, 100, 5, 0, 200, 10_000, 0);
        await InsertObjectStat(_latest, "AppDb", 300, 1, "dbo", "HotTable", "PK_HotTable", 80m, 250_000, 200, 8, 0, 400, 100_000, 3);
    }

    private async Task InsertObjectStat(
        DateTime time, string db, int objectId, int indexId, string schema, string table, string indexName,
        decimal reservedMb, long rows, long seeks, long scans, long lookups, long updates, long lockWaitMs, long escalations)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO index_object_stats
            (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
             schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
             user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_in_ms, index_lock_promotion_count)
            VALUES ($1,$2,$3,'TestServer',$4,$5,7,$6,$7,$8,$9,$10,'NONCLUSTERED',$11,$11,$12,$13,$14,$15,$16,$17,$18)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--); P(time); P(ServerId); P(_startTime); P(db); P(schema); P(objectId); P(table);
        P(indexId); P(indexName); P(reservedMb); P(rows); P(seeks); P(scans); P(lookups); P(updates); P(lockWaitMs); P(escalations);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedCpuAsync(DateTime time, int cpu)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
            VALUES ($1,$2,$3,'TestServer',$2,$4,2)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpu });
        await cmd.ExecuteNonQueryAsync();
    }

    // ── read layer ──

    [Fact]
    public async Task ObjectSizeGrowth_ComputesDelta()
    {
        await SeedScenarioAsync();
        var rows = await _dataService.GetObjectSizeGrowthAsync(ServerId);

        var big = rows.FirstOrDefault(r => r.TableName == "BigTable");
        Assert.NotNull(big);
        Assert.Equal(600m, big!.CurrentReservedMb);

        /* #3541 A12: one day of history. The 400 MB / 200% this used to assert as Growth30dMb was growth over
           ONE day labelled thirty — the store has no 30-day (or 7-day) snapshot, so those figures are null,
           and the same delta carries its own name and its real span. */
        Assert.Null(big.Snapshot7dTime);
        Assert.Null(big.Snapshot30dTime);
        Assert.Null(big.Growth7dMb);
        Assert.Null(big.Growth30dMb);
        Assert.Null(big.GrowthPct30d);
        Assert.Equal(1, big.DaysOfData);
        Assert.Equal(400m, big.GrowthOverAvailableHistoryMb);
        Assert.Equal(200m, big.GrowthOverAvailableHistoryPct);
        Assert.Equal(400m, big.DailyGrowthRateMb);
    }

    [Fact]
    public async Task IndexUsage_FlagsWriteOnly()
    {
        await SeedScenarioAsync();
        var rows = await _dataService.GetIndexUsageAsync(ServerId);

        var unused = rows.FirstOrDefault(r => r.IndexName == "IX_unused");
        Assert.NotNull(unused);
        Assert.Equal("Write-only", unused!.Classification);
        Assert.Equal(0, unused.TotalReads);
        Assert.Equal(500, unused.UserUpdates);
    }

    /// <summary>
    /// #4134: a capped page has to come back the SAME rows on every call when every sort key the query had
    /// (unused-first, then <c>reserved_mb DESC</c>) ties across candidates. Without a further tiebreaker,
    /// DuckDB is free to return any subset, and which rows can change call to call.
    /// </summary>
    [Fact]
    public async Task IndexUsage_TiedRows_PageIsStableAcrossRepeatedCalls()
    {
        // Three Unused indexes, same reserved_mb, all zero reads/updates: nothing but
        // (database, schema, table, index) breaks the tie.
        await InsertObjectStat(_latest, "AppDb", 500, 2, "dbo", "T500", "IX_Tie1", 10m, 1_000, 0, 0, 0, 0, 0, 0);
        await InsertObjectStat(_latest, "AppDb", 500, 3, "dbo", "T500", "IX_Tie2", 10m, 1_000, 0, 0, 0, 0, 0, 0);
        await InsertObjectStat(_latest, "AppDb", 600, 2, "dbo", "T600", "IX_Tie1", 10m, 1_000, 0, 0, 0, 0, 0, 0);

        for (var attempt = 0; attempt < 3; attempt++)
        {
            var rows = await _dataService.GetIndexUsageAsync(ServerId);
            var tied = rows
                .Where(r => r.TableName == "T500" || r.TableName == "T600")
                .Select(r => (r.TableName, r.IndexName))
                .ToArray();

            Assert.Equal(
                new[] { ("T500", "IX_Tie1"), ("T500", "IX_Tie2"), ("T600", "IX_Tie1") },
                tied);
        }
    }

    [Fact]
    public async Task IndexLocking_ReturnsContendedObjects()
    {
        await SeedScenarioAsync();
        var rows = await _dataService.GetIndexLockingAsync(ServerId);

        var hot = rows.FirstOrDefault(r => r.TableName == "HotTable");
        Assert.NotNull(hot);
        Assert.Equal(100_000, hot!.RowLockWaitInMs);
    }

    // ── #1138 object-growth heatmap producer ──

    [Fact]
    public async Task ObjectGrowthHeatmap_RanksByGrowth_AndReturnsDailySeries()
    {
        await SeedScenarioAsync();
        var (objects, samples) = await _dataService.GetObjectGrowthHeatmapDataAsync(ServerId, "AppDb", daysBack: 30, topN: 20);

        Assert.NotEmpty(objects);
        // BigTable grew 200 -> 600 MB; the others are flat, so BigTable ranks first by growth.
        Assert.Equal("BigTable", objects[0].TableName);
        Assert.Equal(400m, objects[0].Growth30dMb);

        // The daily series for BigTable carries both snapshots (prior + latest), keyed schema.table.
        var bigSamples = samples.Where(s => s.ObjectKey == "dbo.BigTable").ToList();
        Assert.Equal(2, bigSamples.Count);
        Assert.Contains(bigSamples, s => s.ReservedMb == 600);
        Assert.Contains(bigSamples, s => s.ReservedMb == 200);
    }

    [Fact]
    public async Task ObjectIndexDetail_ReturnsPerIndexRowsForObject()
    {
        await SeedScenarioAsync();
        var rows = await _dataService.GetObjectIndexDetailAsync(ServerId, "AppDb", "dbo", "BigTable");

        Assert.Single(rows);
        Assert.Equal("PK_BigTable", rows[0].IndexName);
        Assert.Equal(600m, rows[0].ReservedMb);
    }

    [Fact]
    public async Task IndexLocking_DatabaseSelector_ListsContendedDatabases()
    {
        await SeedScenarioAsync();
        var dbs = await _dataService.GetIndexLockingDatabasesAsync(ServerId);

        // HotTable has lock waits in AppDb, so AppDb is offered in the selector.
        Assert.Contains("AppDb", dbs);
    }

    [Fact]
    public async Task IndexLocking_FilteredByDatabase_ReturnsThatDbOnly()
    {
        await SeedScenarioAsync();
        var rows = await _dataService.GetIndexLockingAsync(ServerId, 200, "AppDb");

        Assert.NotEmpty(rows);
        Assert.All(rows, r => Assert.Equal("AppDb", r.DatabaseName));
        Assert.Contains(rows, r => r.TableName == "HotTable" && r.RowLockWaitInMs == 100_000);
    }

    /// <summary>
    /// #3876, the reporter's repro: a database renamed between captures. The old name's rows exist only in
    /// an OLDER capture; the new name's rows only in the newest. "Latest" used to be resolved PER NAME
    /// (MAX(collection_time) GROUP BY database_name), which made the dead name its own immortal group — the
    /// grid showed it a month later and the DB selector still offered it. Anchored on the SERVER's latest
    /// capture, the grid and the selector both see only what the newest pass collected — the same answer
    /// Database sizes and Storage growth always gave — while the old name's rows stay in the store untouched
    /// as capture-time history.
    /// </summary>
    [Fact]
    public async Task IndexLocking_AfterADatabaseRename_ShowsOnlyTheCurrentName_InGridAndSelector()
    {
        // Pre-rename capture: contended rows under the OLD name only.
        await InsertObjectStat(_prior, "OldName", 400, 1, "dbo", "RenamedHot", "PK_RenamedHot", 90m, 100_000, 50, 2, 0, 100, 40_000, 1);
        // Newest capture: the same workload under the NEW name; the old name is absent from this pass.
        await InsertObjectStat(_latest, "NewName", 400, 1, "dbo", "RenamedHot", "PK_RenamedHot", 95m, 110_000, 80, 3, 0, 150, 70_000, 2);

        var rows = await _dataService.GetIndexLockingAsync(ServerId);
        Assert.Contains(rows, r => r.DatabaseName == "NewName" && r.RowLockWaitInMs == 70_000);
        Assert.DoesNotContain(rows, r => r.DatabaseName == "OldName");

        var dbs = await _dataService.GetIndexLockingDatabasesAsync(ServerId);
        Assert.Contains("NewName", dbs);
        Assert.DoesNotContain("OldName", dbs);

        /* The dead name must not be resurrectable through the filter arm either: scoping the grid to the
           old name finds nothing at the current capture, rather than the pre-rename rows. */
        var oldScoped = await _dataService.GetIndexLockingAsync(ServerId, 200, "OldName");
        Assert.Empty(oldScoped);
    }

    // ── anomaly detection ──

    [Fact]
    public async Task DetectObjectStatsAnomalies_FiresGrowthAndContention()
    {
        await SeedScenarioAsync();
        // HasBaselineDataAsync canary: needs cpu/wait data in the last 30 days
        for (int d = 1; d <= 5; d++)
            await SeedCpuAsync(_latest.AddDays(-d), 10);

        var context = new AnalysisContext { ServerId = ServerId, ServerName = "TestServer", TimeRangeStart = _latest.AddHours(-4), TimeRangeEnd = _latest };
        var anomalies = await _detector.DetectAnomaliesAsync(context);

        var growth = anomalies.FirstOrDefault(f => f.Key == "ANOMALY_OBJECT_GROWTH");
        Assert.NotNull(growth);
        Assert.Equal("AppDb", growth!.DatabaseName);
        Assert.True(growth.Metadata["growth_mb"] >= 399);
        Assert.True(growth.Metadata["growth_ratio"] >= 1.0);

        var contention = anomalies.FirstOrDefault(f => f.Key == "ANOMALY_OBJECT_CONTENTION");
        Assert.NotNull(contention);
        Assert.True(contention!.Metadata["lock_wait_ms_delta"] >= 89_000);
    }

    [Fact]
    public async Task DetectObjectStatsAnomalies_SingleSnapshot_NoFalsePositive()
    {
        // Only one snapshot — growth/contention need two distinct snapshots
        await InsertObjectStat(_latest, "AppDb", 100, 1, "dbo", "BigTable", "PK_BigTable", 600m, 3_000_000, 5000, 100, 10, 50, 100_000, 3);
        for (int d = 1; d <= 5; d++)
            await SeedCpuAsync(_latest.AddDays(-d), 10);

        var context = new AnalysisContext { ServerId = ServerId, ServerName = "TestServer", TimeRangeStart = _latest.AddHours(-4), TimeRangeEnd = _latest };
        var anomalies = await _detector.DetectAnomaliesAsync(context);

        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_OBJECT_GROWTH");
        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_OBJECT_CONTENTION");
    }
}
