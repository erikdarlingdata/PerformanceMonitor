using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests the DuckDbFactCollector against seeded test data.
/// Verifies that facts are collected with correct values and metadata.
/// </summary>
public class FactCollectorTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _duckDb;

    public FactCollectorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(_dbPath);
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

    /// <summary>
    /// Seeds a scenario, collects facts, and returns them keyed by fact key.
    /// </summary>
    private async Task<Dictionary<string, Fact>> SeedAndCollectAsync(
        Func<TestDataSeeder, Task> seedScenario)
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seedScenario(seeder);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);
        return facts.ToDictionary(f => f.Key, f => f);
    }

    [Fact]
    public async Task CollectFacts_MemoryStarvedServer_ReturnsWaitFacts()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        Assert.NotEmpty(facts);
        Assert.Contains(facts, f => f.Source == "waits");
    }

    [Fact]
    public async Task CollectFacts_MemoryStarvedServer_PageioLatchHasCorrectFraction()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var pageioFact = facts.First(f => f.Key == "PAGEIOLATCH_SH");

        /* 10,000,000 ms / 14,400,000 ms ≈ 0.694 */
        Assert.InRange(pageioFact.Value, 0.68, 0.71);
        Assert.Equal(TestDataSeeder.TestServerId, pageioFact.ServerId);
    }

    [Fact]
    public async Task CollectFacts_MemoryStarvedServer_MetadataContainsRawValues()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var pageioFact = facts.First(f => f.Key == "PAGEIOLATCH_SH");

        Assert.True(pageioFact.Metadata.ContainsKey("wait_time_ms"));
        Assert.True(pageioFact.Metadata.ContainsKey("waiting_tasks_count"));
        Assert.True(pageioFact.Metadata.ContainsKey("signal_wait_time_ms"));
        Assert.True(pageioFact.Metadata.ContainsKey("avg_ms_per_wait"));

        /* Raw wait_time_ms should be close to 10,000,000 (integer division may lose some) */
        Assert.InRange(pageioFact.Metadata["wait_time_ms"], 9_900_000, 10_100_000);
    }

    [Fact]
    public async Task CollectFacts_MemoryStarvedServer_WaitsOrderedByValue()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        /* PAGEIOLATCH_SH should be the highest wait */
        var waitFacts = facts.Where(f => f.Source == "waits").ToList();
        Assert.Equal("PAGEIOLATCH_SH", waitFacts[0].Key);
    }

    [Fact]
    public async Task CollectFacts_CleanServer_ReturnsLowFractions()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        /* All waits should be well below 5% of the period */
        var waitFacts = facts.Where(f => f.Source == "waits").ToList();
        Assert.All(waitFacts, f => Assert.True(f.Value < 0.05,
            $"{f.Key} fraction {f.Value:P1} should be < 5%"));
    }

    [Fact]
    public async Task CollectFacts_BadParallelism_CxPacketDominates()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedBadParallelismServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var cxFact = facts.First(f => f.Key == "CXPACKET");
        var sosFact = facts.First(f => f.Key == "SOS_SCHEDULER_YIELD");

        /* CXPACKET should have highest fraction among wait facts (CXPACKET + CXCONSUMER combined) */
        var highest = facts.Where(f => f.Source == "waits").OrderByDescending(f => f.Value).First();
        Assert.Equal("CXPACKET", highest.Key);

        /* (8,000,000 + 2,000,000) / 14,400,000 ≈ 0.694 */
        Assert.InRange(cxFact.Value, 0.68, 0.71);
    }

    /* ── New Collector Tests ── */

    [Fact]
    public async Task CollectFacts_CpuUtilization_ReturnsAvgPercent()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("CPU_SQL_PERCENT"), "CPU_SQL_PERCENT should be collected");
        var cpu = facts["CPU_SQL_PERCENT"];
        Assert.Equal("cpu", cpu.Source);
        Assert.Equal(95, cpu.Value, precision: 0);
        Assert.Equal(95, cpu.Metadata["avg_sql_cpu"], precision: 0);
        Assert.Equal(10, cpu.Metadata["avg_other_cpu"], precision: 0);
    }

    [Fact]
    public async Task CollectFacts_IoLatency_ReturnsReadAndWriteLatency()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("IO_READ_LATENCY_MS"), "IO_READ_LATENCY_MS should be collected");
        Assert.True(facts.ContainsKey("IO_WRITE_LATENCY_MS"), "IO_WRITE_LATENCY_MS should be collected");

        // 100,000,000 stall / 2,000,000 reads = 50ms avg
        Assert.InRange(facts["IO_READ_LATENCY_MS"].Value, 45, 55);
        // 15,000,000 stall / 500,000 writes = 30ms avg
        Assert.InRange(facts["IO_WRITE_LATENCY_MS"].Value, 25, 35);
    }

    [Fact]
    public async Task CollectFacts_TempDb_ReturnsUsageFraction()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("TEMPDB_USAGE"), "TEMPDB_USAGE should be collected");
        var tempdb = facts["TEMPDB_USAGE"];
        Assert.Equal("tempdb", tempdb.Source);
        // 9000 / (9000 + 1000) = 0.9
        Assert.InRange(tempdb.Value, 0.85, 0.95);
    }

    [Fact]
    public async Task CollectFacts_MemoryGrants_ReturnsMaxWaiters()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("MEMORY_GRANT_PENDING"), "MEMORY_GRANT_PENDING should be collected");
        var grant = facts["MEMORY_GRANT_PENDING"];
        Assert.Equal("memory", grant.Source);
        Assert.Equal(8, grant.Value); // max_waiters = 8
        Assert.True(grant.Metadata.ContainsKey("max_waiters"));
    }

    [Fact]
    public async Task CollectFacts_QueryStats_ReturnsSpillsAndHighDop()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("QUERY_SPILLS"), "QUERY_SPILLS should be collected");
        Assert.True(facts.ContainsKey("QUERY_HIGH_DOP"), "QUERY_HIGH_DOP should be collected");

        Assert.True(facts["QUERY_SPILLS"].Value >= 4_000); // ~5000 total spills
        Assert.Equal(20, facts["QUERY_HIGH_DOP"].Value); // 20 high-DOP queries
    }

    [Fact]
    public async Task CollectFacts_Perfmon_ReturnsRateCounters()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("PERFMON_BATCH_REQ_SEC"), "PERFMON_BATCH_REQ_SEC should be collected");
    }

    [Fact]
    public async Task CollectFacts_MemoryClerks_ReturnsTopClerks()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("MEMORY_CLERKS"), "MEMORY_CLERKS should be collected");
        var clerks = facts["MEMORY_CLERKS"];
        Assert.True(clerks.Metadata.ContainsKey("MEMORYCLERK_SQLBUFFERPOOL"));
        Assert.Equal(50_000, clerks.Metadata["MEMORYCLERK_SQLBUFFERPOOL"]);
    }

    [Fact]
    public async Task CollectFacts_DatabaseConfig_ReturnsAggregatedCounts()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("DB_CONFIG"), "DB_CONFIG should be collected");
        var dbConfig = facts["DB_CONFIG"];
        Assert.Equal("database_config", dbConfig.Source);
        Assert.Equal(3, dbConfig.Value); // 3 databases
        Assert.Equal(1, dbConfig.Metadata["auto_shrink_on_count"]); // AppDB1
        Assert.Equal(1, dbConfig.Metadata["auto_close_on_count"]); // AppDB2
        Assert.Equal(2, dbConfig.Metadata["rcsi_off_count"]); // AppDB1 + AppDB2
        Assert.Equal(1, dbConfig.Metadata["page_verify_not_checksum_count"]); // AppDB1 = NONE
    }

    [Fact]
    public async Task CollectFacts_ProcedureStats_ReturnsAggregate()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("PROCEDURE_STATS"), "PROCEDURE_STATS should be collected");
        var procs = facts["PROCEDURE_STATS"];
        Assert.Equal("queries", procs.Source);
        Assert.Equal(25, procs.Metadata["distinct_procedures"]);
        Assert.True(procs.Metadata["total_executions"] > 0);
    }

    [Fact]
    public async Task CollectFacts_ActiveQueries_ReturnsLongRunningCount()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("ACTIVE_QUERIES"), "ACTIVE_QUERIES should be collected");
        var aq = facts["ACTIVE_QUERIES"];
        Assert.Equal("queries", aq.Source);
        Assert.Equal(8, aq.Value); // 8 long-running queries
        Assert.Equal(5, aq.Metadata["blocked_count"]);
    }

    [Fact]
    public async Task CollectFacts_RunningJobs_ReturnsLongRunningCount()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("RUNNING_JOBS"), "RUNNING_JOBS should be collected");
        var jobs = facts["RUNNING_JOBS"];
        Assert.Equal("jobs", jobs.Source);
        Assert.Equal(3, jobs.Value); // 3 running long
    }

    [Fact]
    public async Task CollectFacts_SessionStats_ReturnsTotalConnections()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("SESSION_STATS"), "SESSION_STATS should be collected");
        var sessions = facts["SESSION_STATS"];
        Assert.Equal("sessions", sessions.Source);
        Assert.Equal(260, sessions.Value); // 200 + 50 + 10
        Assert.Equal(3, sessions.Metadata["distinct_applications"]);
        Assert.Equal(200, sessions.Metadata["max_app_connections"]); // WebApp has most
    }

    [Fact]
    public async Task CollectFacts_TraceFlags_ReturnsFlagCount()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("TRACE_FLAGS"), "TRACE_FLAGS should be collected");
        var tf = facts["TRACE_FLAGS"];
        Assert.Equal("config", tf.Source);
        Assert.Equal(3, tf.Value); // 1118, 3226, 2371
        Assert.Equal(1, tf.Metadata["TF_1118"]);
        Assert.Equal(1, tf.Metadata["TF_3226"]);
    }

    [Fact]
    public async Task CollectFacts_ServerProperties_ReturnsCpuCount()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("SERVER_HARDWARE"), "SERVER_HARDWARE should be collected");
        var hw = facts["SERVER_HARDWARE"];
        Assert.Equal("config", hw.Source);
        Assert.Equal(16, hw.Value); // cpu_count
        Assert.Equal(65_536, hw.Metadata["physical_memory_mb"]);
    }

    [Fact]
    public async Task CollectFacts_DiskSpace_ReturnsMinFreePercent()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        Assert.True(facts.ContainsKey("DISK_SPACE"), "DISK_SPACE should be collected");
        var disk = facts["DISK_SPACE"];
        Assert.Equal("disk", disk.Source);
        // C: = 35000/500000 = 7%, D: = 140000/2000000 = 7% → min = 7%
        Assert.InRange(disk.Value, 0.06, 0.08);
        Assert.Equal(2, disk.Metadata["volume_count"]);
    }

    [Fact]
    public async Task CollectFacts_CleanServer_CpuIsLow()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedCleanServerAsync());

        Assert.True(facts.ContainsKey("CPU_SQL_PERCENT"));
        Assert.Equal(5, facts["CPU_SQL_PERCENT"].Value, precision: 0);
    }

    [Fact]
    public async Task CollectFacts_CleanServer_IoLatencyIsLow()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedCleanServerAsync());

        Assert.True(facts.ContainsKey("IO_READ_LATENCY_MS"));
        // 500,000 stall / 500,000 reads = 1ms
        Assert.InRange(facts["IO_READ_LATENCY_MS"].Value, 0.8, 1.2);
    }

    [Fact]
    public async Task CollectFacts_CleanServer_DiskSpaceIsHealthy()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedCleanServerAsync());

        Assert.True(facts.ContainsKey("DISK_SPACE"));
        // 900000/2000000 = 45%
        Assert.InRange(facts["DISK_SPACE"].Value, 0.40, 0.50);
    }

    [Fact]
    public async Task CollectFacts_MemoryStarved_HasCorroboratingContext()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedMemoryStarvedServerAsync());

        // Memory-starved server should have corroborating evidence
        Assert.True(facts.ContainsKey("CPU_SQL_PERCENT"));
        Assert.True(facts.ContainsKey("IO_READ_LATENCY_MS"));
        Assert.True(facts.ContainsKey("MEMORY_CLERKS"));

        // CPU should be high (85%)
        Assert.True(facts["CPU_SQL_PERCENT"].Value > 80);
        // Read latency should be high (35ms)
        Assert.True(facts["IO_READ_LATENCY_MS"].Value > 30);
    }

    [Fact]
    public async Task CollectFacts_BadParallelism_HasHighDopQueries()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedBadParallelismServerAsync());

        Assert.True(facts.ContainsKey("CPU_SQL_PERCENT"));
        Assert.True(facts.ContainsKey("QUERY_HIGH_DOP"));
        Assert.True(facts.ContainsKey("SERVER_HARDWARE"));

        Assert.Equal(90, facts["CPU_SQL_PERCENT"].Value, precision: 0);
        Assert.Equal(15, facts["QUERY_HIGH_DOP"].Value);
        Assert.Equal(32, facts["SERVER_HARDWARE"].Value); // 32 CPUs
    }

    [Fact]
    public async Task CollectFacts_ResourceSemaphoreCascade_HasGrantWaiters()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedResourceSemaphoreCascadeServerAsync());

        Assert.True(facts.ContainsKey("MEMORY_GRANT_PENDING"));
        Assert.True(facts.ContainsKey("QUERY_SPILLS"));

        Assert.Equal(5, facts["MEMORY_GRANT_PENDING"].Value);
        Assert.True(facts["QUERY_SPILLS"].Value >= 1_500); // ~2000 spills
    }

    [Fact]
    public async Task CollectFacts_ReaderWriterBlocking_HasRcsiOffDatabases()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedReaderWriterBlockingServerAsync());

        Assert.True(facts.ContainsKey("DB_CONFIG"), "DB_CONFIG should be collected");
        Assert.Equal(3, facts["DB_CONFIG"].Metadata["rcsi_off_count"]); // All 3 dbs have RCSI off
    }

    [Fact]
    public async Task CollectFacts_EverythingOnFire_AllNewCollectorsProduceFacts()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedEverythingOnFireServerAsync());

        var output = TestContext.Current.TestOutputHelper!;
        output.WriteLine($"=== EVERYTHING ON FIRE: {facts.Count} total facts ===");

        var expectedKeys = new[]
        {
            "CPU_SQL_PERCENT", "IO_READ_LATENCY_MS", "IO_WRITE_LATENCY_MS",
            "TEMPDB_USAGE", "MEMORY_GRANT_PENDING", "QUERY_SPILLS", "QUERY_HIGH_DOP",
            "PERFMON_BATCH_REQ_SEC", "MEMORY_CLERKS", "DB_CONFIG",
            "PROCEDURE_STATS", "ACTIVE_QUERIES", "RUNNING_JOBS", "SESSION_STATS",
            "TRACE_FLAGS", "SERVER_HARDWARE", "DISK_SPACE"
        };

        foreach (var key in expectedKeys)
        {
            Assert.True(facts.ContainsKey(key), $"Missing expected fact: {key}");
            var f = facts[key];
            output.WriteLine($"  {key}: value={f.Value:F2} source={f.Source} metadata_keys={string.Join(",", f.Metadata.Keys)}");
        }
    }

    /* ── WS3: percent-autogrowth-on-large-files collector smoke test ── */

    // The Lite collector emits FILE_AUTOGROWTH_PERCENT ONLY for large (>= 10 GB) percent-growth
    // files in non-system databases. Small files, fixed-MB-growth files, and system databases
    // are excluded. This is the fact-emission smoke test that would have caught a dead fact.
    [Fact]
    public async Task CollectFacts_PercentAutogrowthOnLargeFiles_FiresForQualifyingFilesOnly()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedPercentAutogrowthFilesAsync(
            ("AppDb",   "AppDb_log",  "LOG",  200000, true,  10),  // 200 GB %-growth  -> qualifies
            ("AppDb",   "AppDb_data", "ROWS",  60000, true,  10),  // 60 GB  %-growth  -> qualifies
            ("ReportDb", "Rep_data",  "ROWS",   5000, true,  25),  // 5 GB   %-growth  -> too small
            ("AppDb",   "AppDb_fix",  "ROWS",  50000, false, 0),   // 50 GB  fixed     -> excluded
            ("master",  "master",     "ROWS", 100000, true,  10))); // system DB       -> excluded

        Assert.True(facts.ContainsKey("FILE_AUTOGROWTH_PERCENT"),
            "FILE_AUTOGROWTH_PERCENT should be collected when a large percent-growth file exists");
        var fact = facts["FILE_AUTOGROWTH_PERCENT"];
        Assert.Equal("config", fact.Source);
        Assert.Equal(2, fact.Value);                              // the two qualifying AppDb files
        Assert.Equal(2, fact.Metadata["file_count"]);
        Assert.Equal(1, fact.Metadata["database_count"]);          // both qualifying files are in AppDb
    }

    // No qualifying file -> the fact is not emitted at all (collector returns before Add).
    [Fact]
    public async Task CollectFacts_PercentAutogrowthOnLargeFiles_AbsentWhenNoQualifyingFile()
    {
        var facts = await SeedAndCollectAsync(s => s.SeedPercentAutogrowthFilesAsync(
            ("AppDb", "AppDb_data", "ROWS", 5000, true,  10),   // too small
            ("AppDb", "AppDb_fix",  "ROWS", 80000, false, 0)));  // fixed growth

        Assert.False(facts.ContainsKey("FILE_AUTOGROWTH_PERCENT"),
            "FILE_AUTOGROWTH_PERCENT should not be emitted when no large percent-growth file exists");
    }
}
