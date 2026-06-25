using System;
using System.IO;
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
public class IndexObjectStatsTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly AnomalyDetector _detector;

    private const int ServerId = -777;
    private static readonly DateTime _latest = DateTime.UtcNow;
    private static readonly DateTime _prior = _latest.AddDays(-1);
    private static readonly DateTime _startTime = _latest.AddDays(-10); // instance start (no reset)
    private long _nextId = -1;

    public IndexObjectStatsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "IdxObjTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(dbPath);
        _dataService = new LocalDataService(_duckDb);
        var baselineProvider = new BaselineProvider(_duckDb);
        _detector = new AnomalyDetector(_duckDb, baselineProvider);
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { }
    }

    // ── helpers ──

    private async Task SeedScenarioAsync()
    {
        await _duckDb.InitializeAsync();

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
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
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
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
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
        Assert.Equal(400m, big.Growth30dMb);
        Assert.True(big.GrowthPct30d >= 199 && big.GrowthPct30d <= 201);
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

    [Fact]
    public async Task IndexLocking_ReturnsContendedObjects()
    {
        await SeedScenarioAsync();
        var rows = await _dataService.GetIndexLockingAsync(ServerId);

        var hot = rows.FirstOrDefault(r => r.TableName == "HotTable");
        Assert.NotNull(hot);
        Assert.Equal(100_000, hot!.RowLockWaitInMs);
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
        await _duckDb.InitializeAsync();
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
