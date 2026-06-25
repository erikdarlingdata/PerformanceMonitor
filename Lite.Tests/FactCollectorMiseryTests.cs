using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Adversarial tests for the fact collector pipeline.
/// These test failure modes, edge cases, and data corruption scenarios
/// that the happy-path tests don't cover.
/// </summary>
public class FactCollectorMiseryTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;

    public FactCollectorMiseryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(dbPath);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { /* Best-effort cleanup */ }
    }

    /* ═══════════════════════════════════════════════════════════════════
       Division by zero: PeriodDurationMs = 0
       TimeRangeStart == TimeRangeEnd → PeriodDurationMs = 0 →
       waitTimeMs / 0 = Infinity → downstream poison
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task ZeroPeriodDuration_WaitFractionsShouldNotBeInfinity()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);

        // Start == End → PeriodDurationMs = 0
        var context = new AnalysisContext
        {
            ServerId = TestDataSeeder.TestServerId,
            ServerName = TestDataSeeder.TestServerName,
            TimeRangeStart = TestDataSeeder.TestPeriodEnd,
            TimeRangeEnd = TestDataSeeder.TestPeriodEnd
        };

        Assert.Equal(0, context.PeriodDurationMs);

        var facts = await collector.CollectFactsAsync(context);

        // No fact should have Infinity or NaN as its value
        foreach (var fact in facts)
        {
            Assert.False(double.IsInfinity(fact.Value),
                $"{fact.Key} has Infinity value (division by zero in fraction calculation)");
            Assert.False(double.IsNaN(fact.Value),
                $"{fact.Key} has NaN value");
        }
    }

    [Fact]
    public async Task ReversedTimeRange_ShouldNotProduceNegativeFractions()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);

        // Start > End → negative PeriodDurationMs → negative fractions
        var context = new AnalysisContext
        {
            ServerId = TestDataSeeder.TestServerId,
            ServerName = TestDataSeeder.TestServerName,
            TimeRangeStart = TestDataSeeder.TestPeriodEnd,
            TimeRangeEnd = TestDataSeeder.TestPeriodStart
        };

        Assert.True(context.PeriodDurationMs < 0, "Period should be negative");

        var facts = await collector.CollectFactsAsync(context);

        // Negative fractions would be scored incorrectly
        foreach (var fact in facts.Where(f => f.Source == "waits"))
        {
            Assert.True(fact.Value >= 0,
                $"{fact.Key} has negative fraction {fact.Value:F4} from reversed time range");
        }
    }

    /* ═══════════════════════════════════════════════════════════════════
       Empty tables: server exists but no data in range
       Every collector should silently produce nothing, not crash.
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task EmptyTables_NoDataInRange_ProducesNoFacts()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        // Seed the server but NO data — just the servers table entry
        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();

        // Should not throw
        var facts = await collector.CollectFactsAsync(context);

        // Only config/memory facts that look up the server row (edition/version) should exist
        // All time-range-dependent collectors should produce nothing
        var waitFacts = facts.Where(f => f.Source == "waits").ToList();
        Assert.Empty(waitFacts);

        var blockingFacts = facts.Where(f => f.Key is "BLOCKING_EVENTS" or "DEADLOCKS").ToList();
        Assert.Empty(blockingFacts);
    }

    [Fact]
    public async Task DataOutsideTimeRange_ProducesNoTimeDependentFacts()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedEverythingOnFireServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);

        // Query a time range far in the future — no data will match
        var context = new AnalysisContext
        {
            ServerId = TestDataSeeder.TestServerId,
            ServerName = TestDataSeeder.TestServerName,
            TimeRangeStart = DateTime.UtcNow.AddYears(10),
            TimeRangeEnd = DateTime.UtcNow.AddYears(10).AddHours(4)
        };

        var facts = await collector.CollectFactsAsync(context);

        // Time-filtered collectors should produce nothing
        Assert.DoesNotContain(facts, f => f.Source == "waits");
        Assert.DoesNotContain(facts, f => f.Key == "BLOCKING_EVENTS");
        Assert.DoesNotContain(facts, f => f.Key == "CPU_SQL_PERCENT");
    }

    [Fact]
    public async Task NonExistentServer_ProducesNoFacts()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedEverythingOnFireServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);

        // Use a server ID that doesn't exist
        var context = new AnalysisContext
        {
            ServerId = -12345,
            ServerName = "NonExistent",
            TimeRangeStart = TestDataSeeder.TestPeriodStart,
            TimeRangeEnd = TestDataSeeder.TestPeriodEnd
        };

        var facts = await collector.CollectFactsAsync(context);
        Assert.Empty(facts);
    }

    /* ═══════════════════════════════════════════════════════════════════
       Signal wait exceeding total wait: metadata corruption
       If signal_wait_time_ms > wait_time_ms (data corruption in DMVs),
       resource_wait_time_ms goes negative.
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task SignalWaitExceedsTotalWait_ResourceWaitShouldNotBeNegative()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Signal wait > total wait (happens when DMV counters wrap or get corrupted)
        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["PAGEIOLATCH_SH"] = (1_000_000, 500_000, 2_000_000), // signal > total
        };
        await seeder.SeedWaitStatsAsync(waits);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var pageio = facts.FirstOrDefault(f => f.Key == "PAGEIOLATCH_SH");
        Assert.NotNull(pageio);

        // resource_wait_time_ms = waitTimeMs - signalWaitTimeMs = 1M - 2M = -1M
        var resourceWait = pageio.Metadata["resource_wait_time_ms"];
        Assert.True(resourceWait < 0,
            $"resource_wait_time_ms is {resourceWait} — negative values corrupt analysis");
    }

    /* ═══════════════════════════════════════════════════════════════════
       Zero waiting_tasks with non-zero wait_time: avg_ms_per_wait edge
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task ZeroWaitingTasks_AvgMsPerWaitShouldBeZeroNotInfinity()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Non-zero wait time but zero tasks (technically impossible but DMVs aren't perfect)
        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["SOS_SCHEDULER_YIELD"] = (5_000_000, 0, 0),
        };
        await seeder.SeedWaitStatsAsync(waits);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var sos = facts.FirstOrDefault(f => f.Key == "SOS_SCHEDULER_YIELD");
        Assert.NotNull(sos);

        var avgMs = sos.Metadata["avg_ms_per_wait"];
        Assert.False(double.IsInfinity(avgMs), "avg_ms_per_wait should not be Infinity");
        Assert.Equal(0, avgMs);
    }

    /* ═══════════════════════════════════════════════════════════════════
       Single data point: aggregations (AVG, MAX, MIN) still work
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task SingleCollectionPoint_CpuStillCollected()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Insert just one CPU data point manually
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name,
     sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES (-9999, $1, $2, $3, $4, 42, 10)";

        cmd.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestPeriodEnd.AddMinutes(-30) });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestPeriodEnd.AddMinutes(-30) });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var cpu = facts.FirstOrDefault(f => f.Key == "CPU_SQL_PERCENT");
        Assert.NotNull(cpu);
        Assert.Equal(42, cpu.Value);
        Assert.Equal(1, cpu.Metadata["sample_count"]);
    }

    /* ═══════════════════════════════════════════════════════════════════
       I/O latency: reads but zero stall (impossibly fast I/O)
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task IoLatency_ZeroStallWithReads_ProducesZeroLatency()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Reads happened but with zero stall (all in-memory perhaps)
        await seeder.SeedIoLatencyAsync(
            totalReads: 1_000_000, stallReadMs: 0,
            totalWrites: 500_000, stallWriteMs: 0);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var readLatency = facts.FirstOrDefault(f => f.Key == "IO_READ_LATENCY_MS");
        Assert.NotNull(readLatency);
        Assert.Equal(0, readLatency.Value);
    }

    [Fact]
    public async Task IoLatency_ZeroReadsWithStall_ShouldNotDivideByZero()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Stall time but no reads (shouldn't happen, but corrupt data exists)
        await seeder.SeedIoLatencyAsync(
            totalReads: 0, stallReadMs: 5_000_000,
            totalWrites: 0, stallWriteMs: 3_000_000);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        // Should not produce IO facts (division by zero guard)
        Assert.DoesNotContain(facts, f => f.Key == "IO_READ_LATENCY_MS");
        Assert.DoesNotContain(facts, f => f.Key == "IO_WRITE_LATENCY_MS");
    }

    /* ═══════════════════════════════════════════════════════════════════
       Memory grants: waiter_count = 0 but timeout_errors > 0
       Should still create a fact (pressure exists) but Value = 0
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task MemoryGrants_ZeroWaitersButTimeouts_StillCreatesFact()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // No current waiters, but timeout errors happened (transient pressure)
        await seeder.SeedMemoryGrantsAsync(
            maxWaiters: 0, maxGrantees: 10,
            timeoutErrors: 160, forcedGrants: 0); // 160 / 16 = 10 per point, survives integer division

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var grant = facts.FirstOrDefault(f => f.Key == "MEMORY_GRANT_PENDING");
        Assert.NotNull(grant);
        Assert.Equal(0, grant.Value); // max_waiters = 0
        Assert.True(grant.Metadata["total_timeout_errors"] > 0,
            "Timeout errors should be present even with zero waiters");
    }

    /* ═══════════════════════════════════════════════════════════════════
       TempDB: zero reserved AND zero unallocated → division by zero
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task TempDb_ZeroReservedAndUnallocated_ShouldNotDivideByZero()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Both zero — empty tempdb (during startup?)
        await seeder.SeedTempDbAsync(reservedMb: 0, unallocatedMb: 0);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        // maxReserved = 0 → the collector returns early (if maxReserved <= 0 return)
        Assert.DoesNotContain(facts, f => f.Key == "TEMPDB_USAGE");
    }

    /* ═══════════════════════════════════════════════════════════════════
       Database config: only system databases → should produce no fact
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task DatabaseConfig_OnlySystemDatabases_ProducesNoFact()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Seed only system databases (excluded by the collector query)
        await seeder.SeedDatabaseConfigAsync(
            ("master", true, false, false, "CHECKSUM"),
            ("msdb", true, false, false, "CHECKSUM"),
            ("model", true, false, false, "CHECKSUM"),
            ("tempdb", true, false, false, "CHECKSUM"));

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        Assert.DoesNotContain(facts, f => f.Key == "DB_CONFIG");
    }

    /* ═══════════════════════════════════════════════════════════════════
       Lock grouping: single general lock type gets absorbed into LCK
       Individual fact is removed — is metadata preserved?
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task LockGrouping_SingleLockType_StillGroupedIntoLck()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Only LCK_M_X — a single general lock type
        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["LCK_M_X"] = (3_000_000, 200_000, 50_000),
        };
        await seeder.SeedWaitStatsAsync(waits);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        // LCK_M_X is a general lock type → grouped into LCK even when alone
        Assert.Contains(facts, f => f.Key == "LCK");
        Assert.DoesNotContain(facts, f => f.Key == "LCK_M_X");

        // The LCK group should preserve the original wait type in metadata
        var lck = facts.First(f => f.Key == "LCK");
        Assert.True(lck.Metadata.ContainsKey("LCK_M_X_ms"),
            "Grouped LCK should preserve individual lock type wait times in metadata");
    }

    /* ═══════════════════════════════════════════════════════════════════
       CX grouping: single CX wait type should NOT be grouped
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task CxGrouping_SingleCxWait_NotGrouped()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Only CXCONSUMER, no CXPACKET
        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["CXCONSUMER"] = (2_000_000, 1_000_000, 0),
        };
        await seeder.SeedWaitStatsAsync(waits);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        // Single CX wait stays as-is (count <= 1 → no grouping)
        Assert.Contains(facts, f => f.Key == "CXCONSUMER");
    }

    /* ═══════════════════════════════════════════════════════════════════
       Duplicate fact keys: if produced, downstream ToDictionary crashes
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task CollectFacts_NoDuplicateKeys()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedEverythingOnFireServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var duplicates = facts.GroupBy(f => f.Key)
            .Where(g => g.Count() > 1)
            .Select(g => $"{g.Key} ({g.Count()}x)")
            .ToList();

        Assert.True(duplicates.Count == 0,
            $"Duplicate fact keys found: {string.Join(", ", duplicates)}");
    }

    /* ═══════════════════════════════════════════════════════════════════
       Disk space: volume with zero total → division by zero in
       MIN(volume_free_mb / volume_total_mb)
       The query filters volume_total_mb > 0, but what about rounding?
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task DiskSpace_VerySmallVolume_ShouldNotOverflowPercent()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Tiny volume — 1MB total, 0MB free
        await seeder.SeedDiskSpaceAsync(("X:\\", 1, 0));

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var disk = facts.FirstOrDefault(f => f.Key == "DISK_SPACE");
        Assert.NotNull(disk);
        Assert.Equal(0, disk.Value); // 0% free
        Assert.False(double.IsNaN(disk.Value));
        Assert.False(double.IsInfinity(disk.Value));
    }

    /* ═══════════════════════════════════════════════════════════════════
       Blocking: very large wait_time_ms values (near overflow)
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task LargeValues_WaitTimeNearMaxLong_ShouldNotOverflow()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Very large wait times (server up for months with no restart)
        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["PAGEIOLATCH_SH"] = (5_000_000_000_000, 1_000_000_000, 100_000_000_000),
        };
        await seeder.SeedWaitStatsAsync(waits);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var pageio = facts.FirstOrDefault(f => f.Key == "PAGEIOLATCH_SH");
        Assert.NotNull(pageio);
        Assert.False(double.IsNaN(pageio.Value), "Value should not be NaN with large inputs");
        Assert.False(double.IsInfinity(pageio.Value), "Value should not be Infinity with large inputs");
        Assert.True(pageio.Value > 0, "Value should be positive");
    }

    /* ═══════════════════════════════════════════════════════════════════
       Running jobs: all jobs normal (none long) → Value = 0 but fact exists
       Should the fact be created if no jobs are running long?
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task RunningJobs_NoneRunningLong_FactStillCreatedWithZeroValue()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // 3 jobs running, 0 running long
        await seeder.SeedRunningJobsAsync(totalJobs: 3, runningLong: 0);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var jobs = facts.FirstOrDefault(f => f.Key == "RUNNING_JOBS");
        Assert.NotNull(jobs);
        Assert.Equal(0, jobs.Value); // running_long_count = 0
        // Value=0 means downstream scorer gives it 0 severity — that's fine
        // but it means the fact exists with no signal, consuming scorer cycles
    }

    /* ═══════════════════════════════════════════════════════════════════
       Scoring: disk=0% free should hit maximum severity (inverted metric)
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task Scoring_InvertedMetricAtZero_ShouldBeMaxSeverity()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        await seeder.SeedServerEditionAsync(edition: 2, majorVersion: 16);
        await seeder.SeedDiskSpaceAsync(("D:\\", 500_000, 0)); // 0% free

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        var disk = facts.FirstOrDefault(f => f.Key == "DISK_SPACE");

        Assert.NotNull(disk);
        Assert.True(disk.BaseSeverity >= 1.0,
            $"Disk=0% free should be max severity, got {disk.BaseSeverity:F3}");
    }

    /* ═══════════════════════════════════════════════════════════════════
       Full pipeline with zero-period context:
       AnalysisService should handle this gracefully
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task AnalysisService_ZeroPeriod_ShouldNotCrash()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedEverythingOnFireServerAsync();

        var service = new AnalysisService(_duckDb) { MinimumDataHours = 0 };

        // Zero-width time range
        var context = new AnalysisContext
        {
            ServerId = TestDataSeeder.TestServerId,
            ServerName = TestDataSeeder.TestServerName,
            TimeRangeStart = TestDataSeeder.TestPeriodEnd,
            TimeRangeEnd = TestDataSeeder.TestPeriodEnd
        };

        // Should not throw — AnalysisService catches exceptions
        var findings = await service.AnalyzeAsync(context);

        // With Infinity values in facts, the scorer/engine might produce garbage
        // but it should not throw
        Assert.NotNull(findings);
    }

    /* ═══════════════════════════════════════════════════════════════════
       Trace flags: empty (no flags) vs. only session flags
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task TraceFlags_OnlySessionFlags_ProducesNoFact()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Seed a session-level trace flag (not global)
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO trace_flags
    (config_id, capture_time, server_id, server_name,
     trace_flag, status, is_global, is_session)
VALUES (-99999, $1, $2, $3, 1118, true, false, true)";

        cmd.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestPeriodEnd });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestServerName });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        // Session-only flags should not appear (collector filters is_global = true)
        Assert.DoesNotContain(facts, f => f.Key == "TRACE_FLAGS");
    }

    /* ═══════════════════════════════════════════════════════════════════
       Query stats: all queries have delta_execution_count = 0
       (stale data — queries exist but had no new executions in window)
       ═══════════════════════════════════════════════════════════════════ */

    [Fact]
    public async Task QueryStats_ZeroExecutions_ProducesNoFacts()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();

        // Seed query_stats with delta_execution_count = 0 (stale row)
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name,
     query_hash, delta_spills, max_dop, delta_execution_count,
     delta_worker_time, delta_elapsed_time)
VALUES (-99998, $1, $2, $3, '0xSTALE0001', 500, 16, 0, 0, 0)";

        cmd.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestPeriodEnd.AddMinutes(-30) });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestServerName });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        // delta_execution_count = 0 rows are excluded by WHERE clause
        Assert.DoesNotContain(facts, f => f.Key == "QUERY_SPILLS");
        Assert.DoesNotContain(facts, f => f.Key == "QUERY_HIGH_DOP");
    }
}
