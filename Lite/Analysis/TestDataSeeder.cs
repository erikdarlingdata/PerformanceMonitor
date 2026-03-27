using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Seeds DuckDB with synthetic data for controlled analysis testing.
/// Each scenario method clears test data and inserts known values
/// so engine output is deterministic and verifiable.
/// Only available when analysis is enabled.
/// </summary>
public class TestDataSeeder
{
    private readonly DuckDbInitializer _duckDb;

    /// <summary>
    /// Negative server_id to avoid collisions with real servers (hash-based positive IDs).
    /// </summary>
    public const int TestServerId = -999;
    public const string TestServerName = "TestServer-ErikAI";

    /// <summary>
    /// Test scenarios use a 4-hour window ending "now" so the data
    /// falls within any reasonable time range query.
    /// Captured once so all references use identical boundaries.
    /// </summary>
    private static readonly DateTime _periodEnd = DateTime.UtcNow;
    public static DateTime TestPeriodEnd => _periodEnd;
    public static DateTime TestPeriodStart => _periodEnd.AddHours(-4);
    public static double TestPeriodDurationMs => (TestPeriodEnd - TestPeriodStart).TotalMilliseconds;

    /// <summary>
    /// Baseline period for anomaly detection: 24 hours before the analysis window.
    /// </summary>
    public static DateTime BaselineStart => TestPeriodStart.AddHours(-24);
    public static DateTime BaselineEnd => TestPeriodStart;

    private long _nextId = -1_000_000;

    public TestDataSeeder(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
    }

    /// <summary>
    /// Builds an AnalysisContext matching the test data time range.
    /// </summary>
    public static AnalysisContext CreateTestContext()
    {
        return new AnalysisContext
        {
            ServerId = TestServerId,
            ServerName = TestServerName,
            TimeRangeStart = TestPeriodStart,
            TimeRangeEnd = TestPeriodEnd
        };
    }

    /// <summary>
    /// Memory-starved server: high PAGEIOLATCH, moderate SOS, some CXPACKET.
    /// Buffer pool undersized, max memory misconfigured.
    ///
    /// Expected stories:
    ///   PAGEIOLATCH_SH → buffer_pool → max_memory → physical_memory
    ///
    /// Wait fractions (of 4-hour period = 14,400,000 ms):
    ///   PAGEIOLATCH_SH:        10,000,000 ms = 69.4%
    ///   SOS_SCHEDULER_YIELD:    3,000,000 ms = 20.8%
    ///   CXPACKET:               1,500,000 ms = 10.4%
    ///   WRITELOG:                  200,000 ms =  1.4%
    /// </summary>
    public async Task SeedMemoryStarvedServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["PAGEIOLATCH_SH"]      = (10_000_000, 5_000_000, 100_000),
            ["PAGEIOLATCH_EX"]      = (   500_000,   200_000,  10_000),
            ["SOS_SCHEDULER_YIELD"] = ( 3_000_000, 8_000_000,       0),
            ["CXPACKET"]            = ( 1_500_000, 2_000_000,       0),
            ["WRITELOG"]            = (   200_000,   100_000,  20_000),
        };

        await SeedWaitStatsAsync(waits);
        await SeedServerConfigAsync(ctfp: 50, maxdop: 8, maxMemoryMb: 57344);
        await SeedMemoryStatsAsync(totalPhysicalMb: 65_536, bufferPoolMb: 56_000, targetMb: 57_344);
        await SeedFileSizeAsync(totalDataSizeMb: 512_000); // 500GB data on 64GB RAM
        await SeedServerEditionAsync(edition: 2, majorVersion: 16); // Standard 2022

        // Corroborating context from new collectors
        await SeedCpuUtilizationAsync(85, 5);
        await SeedIoLatencyAsync(totalReads: 1_000_000, stallReadMs: 35_000_000, // 35ms avg read
                                  totalWrites: 200_000, stallWriteMs: 2_000_000); // 10ms avg write
        await SeedPerfmonAsync(ple: 120); // Low PLE — buffer pool under pressure
        await SeedMemoryClerksAsync(new Dictionary<string, double>
        {
            ["MEMORYCLERK_SQLBUFFERPOOL"] = 54_000,
            ["MEMORYCLERK_SQLQUERYPLAN"] = 1_500,
        });
        await SeedTempDbAsync(reservedMb: 600, unallocatedMb: 400); // 60% — moderate
        await SeedMemoryGrantsAsync(maxWaiters: 3);
        await SeedServerPropertiesAsync(cpuCount: 16, htRatio: 2, physicalMemMb: 65_536);
        await SeedDiskSpaceAsync(("D:\\", 1_000_000, 150_000)); // 15% free
    }

    /// <summary>
    /// Bad parallelism config: CTFP=5, MAXDOP=0, high CX and SOS waits.
    ///
    /// Expected stories:
    ///   CXPACKET → parallelism_config → CTFP(5), MAXDOP(0)
    ///
    /// Wait fractions (of 4-hour period):
    ///   CXPACKET:               8,000,000 ms = 55.6%
    ///   SOS_SCHEDULER_YIELD:    6,000,000 ms = 41.7%
    ///   CXCONSUMER:             2,000,000 ms = 13.9%
    /// </summary>
    public async Task SeedBadParallelismServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["CXPACKET"]            = (8_000_000, 4_000_000,       0),
            ["SOS_SCHEDULER_YIELD"] = (6_000_000, 12_000_000,      0),
            ["CXCONSUMER"]          = (2_000_000, 1_000_000,       0),
            ["THREADPOOL"]          = (   50_000,       20,        0),
        };

        await SeedWaitStatsAsync(waits);
        await SeedServerConfigAsync(ctfp: 5, maxdop: 0); // Bad defaults
        await SeedMemoryStatsAsync(totalPhysicalMb: 131_072, bufferPoolMb: 122_880, targetMb: 122_880);
        await SeedFileSizeAsync(totalDataSizeMb: 204_800); // 200GB
        await SeedServerEditionAsync(edition: 3, majorVersion: 16); // Enterprise 2022

        // Corroborating context: high CPU, high DOP queries
        await SeedCpuUtilizationAsync(90, 5);
        await SeedQueryStatsAsync(totalSpills: 500, highDopQueryCount: 15);
        await SeedServerPropertiesAsync(cpuCount: 32, htRatio: 2, physicalMemMb: 131_072,
            edition: "Enterprise Edition");
        await SeedPerfmonAsync(ple: 800);
    }

    /// <summary>
    /// Clean server: low waits across the board. Should produce only absolution.
    ///
    /// Wait fractions (of 4-hour period):
    ///   SOS_SCHEDULER_YIELD:      100,000 ms = 0.7%
    ///   WRITELOG:                   50,000 ms = 0.3%
    ///   PAGEIOLATCH_SH:            30,000 ms = 0.2%
    /// </summary>
    public async Task SeedCleanServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["SOS_SCHEDULER_YIELD"] = (100_000, 500_000,     0),
            ["WRITELOG"]            = ( 50_000,  30_000, 5_000),
            ["PAGEIOLATCH_SH"]      = ( 30_000,  15_000, 1_000),
        };

        await SeedWaitStatsAsync(waits);
        await SeedServerConfigAsync(ctfp: 50, maxdop: 8, maxMemoryMb: 122_880);
        await SeedMemoryStatsAsync(totalPhysicalMb: 131_072, bufferPoolMb: 100_000, targetMb: 122_880);
        await SeedFileSizeAsync(totalDataSizeMb: 102_400); // 100GB
        await SeedServerEditionAsync(edition: 3, majorVersion: 16); // Enterprise 2022

        // Clean server context — all healthy values (very low to keep severities near zero)
        await SeedCpuUtilizationAsync(5, 3);
        await SeedIoLatencyAsync(totalReads: 500_000, stallReadMs: 500_000, // 1ms avg read
                                  totalWrites: 200_000, stallWriteMs: 100_000); // 0.5ms avg write
        await SeedTempDbAsync(reservedMb: 100, unallocatedMb: 900); // 10% — healthy
        await SeedPerfmonAsync(ple: 5_000); // Excellent PLE
        await SeedDatabaseConfigAsync(
            ("AppDB1", true, false, false, "CHECKSUM"),
            ("AppDB2", true, false, false, "CHECKSUM"));
        await SeedServerPropertiesAsync(cpuCount: 16, htRatio: 2, physicalMemMb: 131_072,
            edition: "Enterprise Edition");
        await SeedDiskSpaceAsync(("D:\\", 2_000_000, 900_000)); // 45% free — healthy
    }

    /// <summary>
    /// Thread exhaustion: THREADPOOL dominant with CXPACKET as root cause.
    /// The "emergency — connect via DAC" scenario. Parallel queries consumed
    /// the entire worker thread pool.
    ///
    /// Expected stories:
    ///   THREADPOOL → CXPACKET → SOS_SCHEDULER_YIELD
    ///
    /// Wait fractions (of 4-hour period):
    ///   THREADPOOL:              5,400,000 ms = 37.5%  (avg 270s/wait — severe)
    ///   CXPACKET:                5,000,000 ms = 34.7%
    ///   SOS_SCHEDULER_YIELD:     4,000,000 ms = 27.8%
    ///   CXCONSUMER:              1,000,000 ms =  6.9%
    /// </summary>
    public async Task SeedThreadExhaustionServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["THREADPOOL"]          = (5_400_000,     4_000,       0), // avg 1350ms/wait, >1h and >1s floors
            ["CXPACKET"]            = (5_000_000, 3_000_000,       0),
            ["SOS_SCHEDULER_YIELD"] = (4_000_000, 9_000_000,       0),
            ["CXCONSUMER"]          = (1_000_000,   500_000,       0),
        };

        await SeedWaitStatsAsync(waits);
    }

    /// <summary>
    /// Blocking-driven thread exhaustion: THREADPOOL caused by heavy lock contention.
    /// Stuck queries holding exclusive locks, consuming all available worker threads.
    /// Unlike the parallelism scenario, this is caused by blocking, not DOP.
    ///
    /// Expected stories:
    ///   THREADPOOL → LCK (blocking holding threads)
    ///
    /// Wait fractions (of 4-hour period):
    ///   THREADPOOL:              5,400,000 ms = 37.5%  (avg 270s/wait — severe)
    ///   LCK_M_X:                 4,000,000 ms = 27.8%
    ///   LCK_M_U:                 2,000,000 ms = 13.9%
    ///   LCK_M_IX:                  800,000 ms =  5.6%
    ///   SOS_SCHEDULER_YIELD:       500,000 ms =  3.5%
    /// </summary>
    public async Task SeedBlockingThreadExhaustionServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["THREADPOOL"]          = (5_400_000,     4_000,       0), // avg 1350ms/wait, >1h and >1s floors
            ["LCK_M_X"]            = (4_000_000,   300_000,  50_000),
            ["LCK_M_U"]            = (2_000_000,   150_000,  25_000),
            ["LCK_M_IX"]           = (  800_000,   400_000,  10_000),
            ["SOS_SCHEDULER_YIELD"] = (  500_000, 2_000_000,       0),
        };

        await SeedWaitStatsAsync(waits);
        // 200 blocking events (~50/hr) — heavy, at critical threshold
        await SeedBlockingEventsAsync(200, avgWaitTimeMs: 60_000, sleepingBlockerCount: 40, distinctBlockers: 8);
        // 15 deadlocks (~3.75/hr) — escalating
        await SeedDeadlocksAsync(15);
    }

    /// <summary>
    /// Heavy lock contention: LCK_M_X and LCK_M_U dominant.
    /// Writers blocking writers — classic OLTP contention pattern.
    ///
    /// Expected stories:
    ///   LCK_M_X (exclusive lock waits, highest)
    ///   LCK_M_U (update lock waits)
    ///
    /// Wait fractions (of 4-hour period):
    ///   LCK_M_X:                 3,000,000 ms = 20.8%
    ///   LCK_M_U:                 1,500,000 ms = 10.4%
    ///   LCK_M_IX:                  800,000 ms =  5.6%
    ///   SOS_SCHEDULER_YIELD:       500,000 ms =  3.5%
    ///   WRITELOG:                   400,000 ms =  2.8%
    /// </summary>
    public async Task SeedLockContentionServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["LCK_M_X"]            = (3_000_000,   200_000,  50_000),
            ["LCK_M_U"]            = (1_500_000,   100_000,  25_000),
            ["LCK_M_IX"]           = (  800_000,   300_000,  10_000),
            ["SOS_SCHEDULER_YIELD"] = (  500_000, 2_000_000,       0),
            ["WRITELOG"]            = (  400_000,   200_000,  30_000),
        };

        await SeedWaitStatsAsync(waits);
        // 60 blocking events (~15/hr) — confirmed write-write blocking
        await SeedBlockingEventsAsync(60, avgWaitTimeMs: 30_000, sleepingBlockerCount: 5, distinctBlockers: 4);
    }

    /// <summary>
    /// Reader/writer blocking: LCK_M_S and LCK_M_IS dominant.
    /// Readers blocked by writers — the "enable RCSI" scenario.
    ///
    /// Expected stories:
    ///   LCK_M_S → recommendation to enable RCSI
    ///   LCK_M_IS
    ///
    /// Wait fractions (of 4-hour period):
    ///   LCK_M_S:                 4,000,000 ms = 27.8%
    ///   LCK_M_IS:                2,000,000 ms = 13.9%
    ///   LCK_M_X:                   500,000 ms =  3.5%
    ///   WRITELOG:                   300,000 ms =  2.1%
    /// </summary>
    public async Task SeedReaderWriterBlockingServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["LCK_M_S"]  = (4_000_000,   800_000,  40_000),
            ["LCK_M_IS"] = (2_000_000,   500_000,  20_000),
            ["LCK_M_X"]  = (  500_000,    30_000,   5_000),
            ["WRITELOG"]  = (  300_000,   150_000,  25_000),
        };

        await SeedWaitStatsAsync(waits);
        // 40 blocking events (~10/hr) — reader/writer blocking confirmed
        await SeedBlockingEventsAsync(40, avgWaitTimeMs: 20_000, sleepingBlockerCount: 3, distinctBlockers: 6);
        // 8 deadlocks (~2/hr) — reader/writer deadlocks (RCSI would eliminate)
        await SeedDeadlocksAsync(8);

        // RCSI off on multiple databases — the key recommendation
        await SeedDatabaseConfigAsync(
            ("AppDB1", false, false, false, "CHECKSUM"),
            ("AppDB2", false, false, false, "CHECKSUM"),
            ("ReportDB", false, false, false, "CHECKSUM"));
    }

    /// <summary>
    /// Serializable isolation abuse: range lock modes present.
    /// Someone has SERIALIZABLE on a high-traffic table — unnecessary and destructive.
    ///
    /// Expected stories:
    ///   LCK_M_RIn_X → "SERIALIZABLE or REPEATABLE READ isolation"
    ///   LCK_M_RS_S
    ///
    /// Wait fractions (of 4-hour period):
    ///   LCK_M_RIn_X:              800,000 ms =  5.6%
    ///   LCK_M_RS_S:               600,000 ms =  4.2%
    ///   LCK_M_RIn_S:              400,000 ms =  2.8%
    ///   LCK_M_S:                   200,000 ms =  1.4%
    ///   LCK_M_X:                   100,000 ms =  0.7%
    /// </summary>
    public async Task SeedSerializableAbuseServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["LCK_M_RIn_X"] = (800_000,  50_000,  5_000),
            ["LCK_M_RS_S"]  = (600_000,  40_000,  3_000),
            ["LCK_M_RIn_S"] = (400_000,  30_000,  2_000),
            ["LCK_M_S"]     = (200_000,  60_000,  1_000),
            ["LCK_M_X"]     = (100_000,  10_000,    500),
        };

        await SeedWaitStatsAsync(waits);
        // 25 deadlocks (~6.25/hr) — serializable often causes deadlocks
        await SeedDeadlocksAsync(25);
    }

    /// <summary>
    /// Log write pressure: WRITELOG dominant with some lock contention.
    /// Storage can't keep up with transaction log writes — shared storage
    /// or undersized log disks.
    ///
    /// Expected stories:
    ///   WRITELOG → log write latency
    ///
    /// Wait fractions (of 4-hour period):
    ///   WRITELOG:                 5,000,000 ms = 34.7%
    ///   LCK_M_X:                   600,000 ms =  4.2%
    ///   SOS_SCHEDULER_YIELD:       400,000 ms =  2.8%
    /// </summary>
    public async Task SeedLogWritePressureServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["WRITELOG"]            = (5_000_000, 2_000_000, 500_000),
            ["LCK_M_X"]            = (  600_000,    40_000,   5_000),
            ["SOS_SCHEDULER_YIELD"] = (  400_000, 1_500_000,       0),
        };

        await SeedWaitStatsAsync(waits);
    }

    /// <summary>
    /// Resource semaphore cascade: memory grant waits causing buffer pool
    /// starvation and downstream PAGEIOLATCH. Queries requesting too much memory.
    ///
    /// Expected stories:
    ///   RESOURCE_SEMAPHORE → PAGEIOLATCH_SH (cascade)
    ///
    /// Wait fractions (of 4-hour period):
    ///   RESOURCE_SEMAPHORE:      1,500,000 ms = 10.4%
    ///   PAGEIOLATCH_SH:          6,000,000 ms = 41.7%
    ///   PAGEIOLATCH_EX:            500_000 ms =  3.5%
    ///   SOS_SCHEDULER_YIELD:       800,000 ms =  5.6%
    /// </summary>
    public async Task SeedResourceSemaphoreCascadeServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["RESOURCE_SEMAPHORE"]  = (1_500_000,     5_000,       0), // avg 300s/wait — severe
            ["PAGEIOLATCH_SH"]      = (6_000_000, 3_000_000,  50_000),
            ["PAGEIOLATCH_EX"]      = (  500_000,   200_000,  10_000),
            ["SOS_SCHEDULER_YIELD"] = (  800_000, 3_000_000,       0),
        };

        await SeedWaitStatsAsync(waits);
        await SeedServerConfigAsync(ctfp: 50, maxdop: 8, maxMemoryMb: 57_344);
        await SeedMemoryStatsAsync(totalPhysicalMb: 65_536, bufferPoolMb: 40_000, targetMb: 57_344);
        await SeedFileSizeAsync(totalDataSizeMb: 307_200); // 300GB
        await SeedServerEditionAsync(edition: 2, majorVersion: 16); // Standard 2022

        // Cascade evidence: grant waiters + spills + I/O + low PLE
        await SeedMemoryGrantsAsync(maxWaiters: 5, timeoutErrors: 3);
        await SeedQueryStatsAsync(totalSpills: 2_000, highDopQueryCount: 5);
        await SeedIoLatencyAsync(totalReads: 800_000, stallReadMs: 28_000_000, // 35ms avg read
                                  totalWrites: 200_000, stallWriteMs: 3_000_000);
        await SeedPerfmonAsync(ple: 200);
        await SeedServerPropertiesAsync(cpuCount: 16, htRatio: 2, physicalMemMb: 65_536);
    }

    /// <summary>
    /// Everything on fire: multiple high-severity categories competing.
    /// Memory pressure, CPU pressure, parallelism, lock contention, log writes.
    /// Tests that the engine produces multiple stories in priority order.
    ///
    /// Expected stories (multiple, ordered by severity):
    ///   1. PAGEIOLATCH_SH (memory pressure, amplified by SOS)
    ///   2. CXPACKET (parallelism, amplified by SOS + THREADPOOL)
    ///   3. LCK_M_X (lock contention)
    ///   4. WRITELOG (log writes)
    ///
    /// Wait fractions (of 4-hour period):
    ///   PAGEIOLATCH_SH:          8,000,000 ms = 55.6%
    ///   CXPACKET:                6,000,000 ms = 41.7%
    ///   SOS_SCHEDULER_YIELD:     5,000,000 ms = 34.7%
    ///   LCK_M_X:                 2,000,000 ms = 13.9%
    ///   THREADPOOL:              4,000,000 ms = 27.8%
    ///   WRITELOG:                 1,500,000 ms = 10.4%
    ///   RESOURCE_SEMAPHORE:        300,000 ms =  2.1%
    /// </summary>
    public async Task SeedEverythingOnFireServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["PAGEIOLATCH_SH"]      = (8_000_000, 4_000_000, 100_000),
            ["CXPACKET"]            = (6_000_000, 3_000_000,       0),
            ["SOS_SCHEDULER_YIELD"] = (5_000_000, 10_000_000,      0),
            ["LCK_M_X"]            = (2_000_000,   150_000,  20_000),
            ["THREADPOOL"]          = (4_000_000,     3_000,       0), // avg 1333ms/wait, >1h and >1s floors
            ["WRITELOG"]            = (1_500_000,   700_000, 150_000),
            ["RESOURCE_SEMAPHORE"]  = (  300_000,     1_000,       0), // avg 300s/wait
        };

        await SeedWaitStatsAsync(waits);
        // 100 blocking events (~25/hr) — systemic blocking
        await SeedBlockingEventsAsync(100, avgWaitTimeMs: 40_000, sleepingBlockerCount: 15, distinctBlockers: 10);
        // 30 deadlocks (~7.5/hr) — escalating
        await SeedDeadlocksAsync(30);
        await SeedServerConfigAsync(ctfp: 5, maxdop: 0, maxMemoryMb: 2_147_483_647); // All defaults
        await SeedMemoryStatsAsync(totalPhysicalMb: 65_536, bufferPoolMb: 58_000, targetMb: 65_536);
        await SeedFileSizeAsync(totalDataSizeMb: 1_024_000); // 1TB
        await SeedServerEditionAsync(edition: 2, majorVersion: 15); // Standard 2019

        // New collectors — full coverage
        await SeedCpuUtilizationAsync(95, 10); // 95% SQL + 10% other = pegged
        await SeedIoLatencyAsync(totalReads: 2_000_000, stallReadMs: 100_000_000, // 50ms avg read
                                  totalWrites: 500_000, stallWriteMs: 15_000_000); // 30ms avg write
        await SeedTempDbAsync(reservedMb: 9_000, unallocatedMb: 1_000); // 90% full
        await SeedMemoryGrantsAsync(maxWaiters: 8, maxGrantees: 5, timeoutErrors: 10, forcedGrants: 5);
        await SeedQueryStatsAsync(totalSpills: 5_000, highDopQueryCount: 20);
        await SeedPerfmonAsync(ple: 45); // Critically low PLE
        await SeedMemoryClerksAsync(new Dictionary<string, double>
        {
            ["MEMORYCLERK_SQLBUFFERPOOL"] = 50_000,
            ["MEMORYCLERK_SQLQUERYPLAN"] = 4_000,
            ["MEMORYCLERK_SQLOPTIMIZER"] = 1_500,
            ["CACHESTORE_OBJCP"] = 2_000,
            ["CACHESTORE_SQLCP"] = 3_500,
        });
        await SeedDatabaseConfigAsync(
            ("AppDB1", false, true, false, "NONE"),       // RCSI off, auto_shrink, bad page_verify
            ("AppDB2", false, false, true, "CHECKSUM"),    // RCSI off, auto_close
            ("AppDB3", true, false, false, "CHECKSUM"));   // OK
        await SeedProcedureStatsAsync(distinctProcs: 25, totalExecs: 500_000, totalCpuUs: 50_000_000_000);
        await SeedActiveQueriesAsync(longRunning: 8, blocked: 5, parallel: 6, maxElapsedMs: 300_000, maxDop: 16);
        await SeedRunningJobsAsync(totalJobs: 5, runningLong: 3, maxPctAvg: 400, maxDurationSeconds: 10_800);
        await SeedSessionStatsAsync(
            ("WebApp", 200, 15, 180),
            ("ReportingService", 50, 8, 40),
            ("SQLAgent", 10, 3, 7));
        await SeedTraceFlagsAsync(1118, 3226, 2371);
        await SeedServerPropertiesAsync(cpuCount: 16, htRatio: 2, physicalMemMb: 65_536);
        await SeedDiskSpaceAsync(
            ("C:\\", 500_000, 35_000),   // 7% free — critical
            ("D:\\", 2_000_000, 140_000)); // 7% free — critical
    }

    /// <summary>
    /// CPU spike anomaly: server normally runs at 10% CPU, then spikes to 95%.
    /// Baseline: 24h of steady ~10% CPU.
    /// Analysis window: 4h with 95% peak CPU.
    ///
    /// Expected: ANOMALY_CPU_SPIKE with high deviation (~10σ+).
    /// </summary>
    public async Task SeedCpuSpikeAnomalyAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // Baseline: 24h of steady low CPU (10% avg, small variance)
        await SeedCpuUtilizationInRangeAsync(BaselineStart, BaselineEnd, avgCpu: 10, variance: 3, samples: 96);

        // Analysis window: spike to 95%
        await SeedCpuUtilizationInRangeAsync(TestPeriodStart, TestPeriodEnd, avgCpu: 95, variance: 5, samples: 16);

        // Need basic config for the analysis to run
        await SeedServerConfigAsync(ctfp: 50, maxdop: 8, maxMemoryMb: 122_880);
        await SeedMemoryStatsAsync(totalPhysicalMb: 131_072, bufferPoolMb: 100_000, targetMb: 122_880);
        await SeedFileSizeAsync(totalDataSizeMb: 102_400);
        await SeedServerEditionAsync(edition: 3, majorVersion: 16);
        await SeedServerPropertiesAsync(cpuCount: 8, htRatio: 1, physicalMemMb: 131_072);
    }

    /// <summary>
    /// Blocking spike anomaly: normally no blocking, then sudden burst.
    /// Baseline: 24h with 0 blocking events.
    /// Analysis window: 4h with 50 blocking events and 10 deadlocks.
    ///
    /// Expected: ANOMALY_BLOCKING_SPIKE, ANOMALY_DEADLOCK_SPIKE.
    /// </summary>
    public async Task SeedBlockingSpikeAnomalyAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // Baseline: some normal wait activity (no blocking/deadlocks)
        await SeedWaitStatsInRangeAsync(BaselineStart, BaselineEnd,
            new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
            {
                ["SOS_SCHEDULER_YIELD"] = (100_000, 500_000, 0),
            }, samples: 24);

        // Analysis window: sudden blocking burst
        await SeedBlockingEventsAsync(50, avgWaitTimeMs: 15_000, sleepingBlockerCount: 5);
        await SeedDeadlocksAsync(10);

        // Some lock waits to corroborate
        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["LCK_M_X"]  = (5_000_000, 200_000, 50_000),
            ["LCK_M_S"]  = (1_000_000, 100_000, 10_000),
        };
        await SeedWaitStatsAsync(waits);

        await SeedServerConfigAsync(ctfp: 50, maxdop: 8, maxMemoryMb: 122_880);
        await SeedMemoryStatsAsync(totalPhysicalMb: 131_072, bufferPoolMb: 100_000, targetMb: 122_880);
        await SeedFileSizeAsync(totalDataSizeMb: 102_400);
        await SeedServerEditionAsync(edition: 3, majorVersion: 16);
        await SeedServerPropertiesAsync(cpuCount: 8, htRatio: 1, physicalMemMb: 131_072);
        await SeedDatabaseConfigAsync(
            ("AppDB1", false, false, false, "CHECKSUM"),
            ("AppDB2", false, false, false, "CHECKSUM"));
    }

    /// <summary>
    /// Wait spike anomaly: normally low waits, then sudden PAGEIOLATCH flood.
    /// Baseline: 24h with minimal PAGEIOLATCH.
    /// Analysis window: 4h with massive PAGEIOLATCH.
    ///
    /// Expected: ANOMALY_WAIT_PAGEIOLATCH_SH with high ratio.
    /// </summary>
    public async Task SeedWaitSpikeAnomalyAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // Baseline: 24h with minimal PAGEIOLATCH
        await SeedWaitStatsInRangeAsync(BaselineStart, BaselineEnd,
            new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
            {
                ["PAGEIOLATCH_SH"] = (50_000, 25_000, 1_000),  // 50 seconds over 24h = noise
                ["SOS_SCHEDULER_YIELD"] = (100_000, 500_000, 0),
            }, samples: 24);

        // Analysis window: massive PAGEIOLATCH spike
        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["PAGEIOLATCH_SH"] = (8_000_000, 4_000_000, 100_000),  // 8 million ms in 4h
            ["SOS_SCHEDULER_YIELD"] = (100_000, 500_000, 0),        // Normal
        };
        await SeedWaitStatsAsync(waits);

        await SeedServerConfigAsync(ctfp: 50, maxdop: 8, maxMemoryMb: 122_880);
        await SeedMemoryStatsAsync(totalPhysicalMb: 131_072, bufferPoolMb: 100_000, targetMb: 122_880);
        await SeedFileSizeAsync(totalDataSizeMb: 102_400);
        await SeedServerEditionAsync(edition: 3, majorVersion: 16);
        await SeedServerPropertiesAsync(cpuCount: 8, htRatio: 1, physicalMemMb: 131_072);
    }

    /// <summary>
    /// Seeds CPU utilization data in a specific time range with variance.
    /// Used for baseline + spike anomaly scenarios.
    /// </summary>
    internal async Task SeedCpuUtilizationInRangeAsync(DateTime start, DateTime end,
        int avgCpu, int variance, int samples)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var interval = (end - start).TotalMinutes / samples;
        var rng = new Random(42); // Deterministic for reproducibility

        for (var i = 0; i < samples; i++)
        {
            var t = start.AddMinutes(i * interval);
            var cpu = Math.Clamp(avgCpu + rng.Next(-variance, variance + 1), 0, 100);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name,
     sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6, $7)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = cpu });
            cmd.Parameters.Add(new DuckDBParameter { Value = 2 }); // other CPU

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds wait stats in a specific time range. Used for baseline periods
    /// in anomaly detection scenarios.
    /// </summary>
    internal async Task SeedWaitStatsInRangeAsync(DateTime start, DateTime end,
        Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)> waits, int samples)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var interval = (end - start).TotalMinutes / samples;

        foreach (var (waitType, (totalWaitTimeMs, totalWaitingTasks, totalSignalMs)) in waits)
        {
            var perSampleWaitMs = totalWaitTimeMs / samples;
            var perSampleTasks = totalWaitingTasks / samples;
            var perSampleSignal = totalSignalMs / samples;

            for (var i = 0; i < samples; i++)
            {
                var t = start.AddMinutes(i * interval);

                using var cmd = connection.CreateCommand();
                cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name,
     wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";

                cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
                cmd.Parameters.Add(new DuckDBParameter { Value = t });
                cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
                cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
                cmd.Parameters.Add(new DuckDBParameter { Value = totalWaitingTasks }); // cumulative
                cmd.Parameters.Add(new DuckDBParameter { Value = totalWaitTimeMs });   // cumulative
                cmd.Parameters.Add(new DuckDBParameter { Value = totalSignalMs });     // cumulative
                cmd.Parameters.Add(new DuckDBParameter { Value = perSampleTasks });    // delta
                cmd.Parameters.Add(new DuckDBParameter { Value = perSampleWaitMs });   // delta
                cmd.Parameters.Add(new DuckDBParameter { Value = perSampleSignal });   // delta

                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    /// <summary>
    /// Removes all test data across all tables.
    /// </summary>
    internal async Task ClearTestDataAsync()
    {
        var tables = new[]
        {
            "wait_stats", "memory_stats", "server_config", "database_config",
            "cpu_utilization_stats", "file_io_stats", "memory_clerks",
            "query_stats", "procedure_stats", "query_store_stats",
            "query_snapshots", "tempdb_stats", "perfmon_stats",
            "blocked_process_reports", "deadlocks", "memory_grant_stats",
            "waiting_tasks", "servers", "running_jobs", "session_stats",
            "trace_flags", "server_properties", "database_size_stats"
        };

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        foreach (var table in tables)
        {
            try
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $"DELETE FROM {table} WHERE server_id = {TestServerId}";
                await cmd.ExecuteNonQueryAsync();
            }
            catch
            {
                /* Table may not exist yet — that's fine */
            }
        }
    }

    /// <summary>
    /// Registers the test server in the servers table.
    /// </summary>
    internal async Task SeedTestServerAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO servers (server_id, server_name, display_name, use_windows_auth, is_enabled)
VALUES ($1, $2, $3, true, true)";
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ErikAI Test Server" });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Seeds blocked_process_reports with synthetic blocking events.
    /// </summary>
    internal async Task SeedBlockingEventsAsync(int count, long avgWaitTimeMs,
        int sleepingBlockerCount = 0, int distinctBlockers = 3)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var intervalMinutes = 240.0 / count; // Spread across 4-hour window

        for (var i = 0; i < count; i++)
        {
            var eventTime = TestPeriodStart.AddMinutes(i * intervalMinutes);
            var id = _nextId--;
            var isSleeping = i < sleepingBlockerCount;
            var blockerSpid = 50 + (i % distinctBlockers);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name,
     event_time, blocked_spid, blocking_spid, wait_time_ms,
     lock_mode, blocked_status, blocking_status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";

            cmd.Parameters.Add(new DuckDBParameter { Value = id });
            cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = 100 + i }); // blocked spid
            cmd.Parameters.Add(new DuckDBParameter { Value = blockerSpid });
            cmd.Parameters.Add(new DuckDBParameter { Value = avgWaitTimeMs });
            cmd.Parameters.Add(new DuckDBParameter { Value = "X" });
            cmd.Parameters.Add(new DuckDBParameter { Value = "suspended" });
            cmd.Parameters.Add(new DuckDBParameter { Value = isSleeping ? "sleeping" : "running" });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds deadlocks table with synthetic deadlock events.
    /// </summary>
    internal async Task SeedDeadlocksAsync(int count)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var intervalMinutes = 240.0 / count;

        for (var i = 0; i < count; i++)
        {
            var eventTime = TestPeriodStart.AddMinutes(i * intervalMinutes);
            var id = _nextId--;

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time)
VALUES ($1, $2, $3, $4, $5)";

            cmd.Parameters.Add(new DuckDBParameter { Value = id });
            cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds wait_stats with the given wait type values.
    /// Distributes data across 16 collection points (every 15 minutes)
    /// so the data looks realistic in trend queries.
    /// </summary>
    internal async Task SeedWaitStatsAsync(
        Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)> waits)
    {
        const int collectionPoints = 16;

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        foreach (var (waitType, totals) in waits)
        {
            var deltaWaitPerPoint = totals.waitTimeMs / collectionPoints;
            var deltaTasksPerPoint = totals.waitingTasks / collectionPoints;
            var deltaSignalPerPoint = totals.signalMs / collectionPoints;

            long cumulativeWait = 0;
            long cumulativeTasks = 0;
            long cumulativeSignal = 0;

            for (var i = 0; i < collectionPoints; i++)
            {
                cumulativeWait += deltaWaitPerPoint;
                cumulativeTasks += deltaTasksPerPoint;
                cumulativeSignal += deltaSignalPerPoint;

                var collectionTime = TestPeriodStart.AddMinutes(i * 15);
                var id = _nextId--;

                using var cmd = connection.CreateCommand();
                cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";

                cmd.Parameters.Add(new DuckDBParameter { Value = id });
                cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
                cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
                cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
                cmd.Parameters.Add(new DuckDBParameter { Value = cumulativeTasks });
                cmd.Parameters.Add(new DuckDBParameter { Value = cumulativeWait });
                cmd.Parameters.Add(new DuckDBParameter { Value = cumulativeSignal });
                cmd.Parameters.Add(new DuckDBParameter { Value = deltaTasksPerPoint });
                cmd.Parameters.Add(new DuckDBParameter { Value = deltaWaitPerPoint });
                cmd.Parameters.Add(new DuckDBParameter { Value = deltaSignalPerPoint });

                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    /// <summary>
    /// Seeds memory_stats with physical memory, buffer pool, and target memory values.
    /// </summary>
    internal async Task SeedMemoryStatsAsync(double totalPhysicalMb, double bufferPoolMb, double targetMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";

        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalPhysicalMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalPhysicalMb - bufferPoolMb }); // available = total - used
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = bufferPoolMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = bufferPoolMb });

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Seeds file_io_stats with a total database size entry.
    /// Creates a single "data" file entry representing the total data footprint.
    /// </summary>
    internal async Task SeedFileSizeAsync(double totalDataSizeMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, file_name, file_type, size_mb,
     num_of_reads, num_of_writes, read_bytes, write_bytes,
     io_stall_read_ms, io_stall_write_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 0, 0, 0, 0, 0)";

        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "AllDatabases" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "aggregate_data" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ROWS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalDataSizeMb });

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Updates the test server's edition and major version in the servers table.
    /// </summary>
    internal async Task SeedServerEditionAsync(int edition, int majorVersion)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
UPDATE servers
SET sql_engine_edition = $1,
    sql_major_version = $2
WHERE server_id = $3";

        cmd.Parameters.Add(new DuckDBParameter { Value = edition });
        cmd.Parameters.Add(new DuckDBParameter { Value = majorVersion });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Seeds server_config with specific CTFP and MAXDOP values for testing.
    /// </summary>
    internal async Task SeedServerConfigAsync(int ctfp = 50, int maxdop = 8, int maxMemoryMb = 57344)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var configs = new (string name, int value)[]
        {
            ("cost threshold for parallelism", ctfp),
            ("max degree of parallelism", maxdop),
            ("max server memory (MB)", maxMemoryMb),
            ("max worker threads", 0)
        };

        foreach (var (name, value) in configs)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name,
     value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = name });
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
            cmd.Parameters.Add(new DuckDBParameter { Value = true });
            cmd.Parameters.Add(new DuckDBParameter { Value = true });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds cpu_utilization_stats across 16 collection points.
    /// </summary>
    internal async Task SeedCpuUtilizationAsync(int avgSqlCpu, int avgOtherCpu)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        for (var i = 0; i < 16; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name,
     sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6, $7)";

            var t = TestPeriodStart.AddMinutes(i * 15);
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = avgSqlCpu });
            cmd.Parameters.Add(new DuckDBParameter { Value = avgOtherCpu });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds file_io_stats with I/O latency delta data across 16 collection points.
    /// totalReads/totalWrites are the total I/O count over the period;
    /// stallReadMs/stallWriteMs are total stall times.
    /// Average latency = stallMs / ioCount.
    /// </summary>
    internal async Task SeedIoLatencyAsync(long totalReads, long stallReadMs,
        long totalWrites, long stallWriteMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var deltaReads = totalReads / 16;
        var deltaStallRead = stallReadMs / 16;
        var deltaWrites = totalWrites / 16;
        var deltaStallWrite = stallWriteMs / 16;

        for (var i = 0; i < 16; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, file_name, file_type, size_mb,
     num_of_reads, num_of_writes, read_bytes, write_bytes,
     io_stall_read_ms, io_stall_write_ms,
     delta_reads, delta_writes, delta_stall_read_ms, delta_stall_write_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, 0,
        $8, $9, 0, 0, $10, $11, $12, $13, $14, $15)";

            var t = TestPeriodStart.AddMinutes(i * 15);
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = "UserDB" });
            cmd.Parameters.Add(new DuckDBParameter { Value = "UserDB.mdf" });
            cmd.Parameters.Add(new DuckDBParameter { Value = "ROWS" });
            cmd.Parameters.Add(new DuckDBParameter { Value = (long)(deltaReads * (i + 1)) }); // cumulative
            cmd.Parameters.Add(new DuckDBParameter { Value = (long)(deltaWrites * (i + 1)) });
            cmd.Parameters.Add(new DuckDBParameter { Value = (long)(deltaStallRead * (i + 1)) });
            cmd.Parameters.Add(new DuckDBParameter { Value = (long)(deltaStallWrite * (i + 1)) });
            cmd.Parameters.Add(new DuckDBParameter { Value = deltaReads });
            cmd.Parameters.Add(new DuckDBParameter { Value = deltaWrites });
            cmd.Parameters.Add(new DuckDBParameter { Value = deltaStallRead });
            cmd.Parameters.Add(new DuckDBParameter { Value = deltaStallWrite });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds tempdb_stats across 16 collection points.
    /// </summary>
    internal async Task SeedTempDbAsync(double reservedMb, double unallocatedMb,
        double userObjectMb = 0, double internalObjectMb = 0, double versionStoreMb = 0)
    {
        if (userObjectMb == 0) userObjectMb = reservedMb * 0.6;
        if (internalObjectMb == 0) internalObjectMb = reservedMb * 0.3;
        if (versionStoreMb == 0) versionStoreMb = reservedMb * 0.1;

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        for (var i = 0; i < 16; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO tempdb_stats
    (collection_id, collection_time, server_id, server_name,
     user_object_reserved_mb, internal_object_reserved_mb,
     version_store_reserved_mb, total_reserved_mb, unallocated_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";

            var t = TestPeriodStart.AddMinutes(i * 15);
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = userObjectMb });
            cmd.Parameters.Add(new DuckDBParameter { Value = internalObjectMb });
            cmd.Parameters.Add(new DuckDBParameter { Value = versionStoreMb });
            cmd.Parameters.Add(new DuckDBParameter { Value = reservedMb });
            cmd.Parameters.Add(new DuckDBParameter { Value = unallocatedMb });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds memory_grant_stats across 16 collection points.
    /// </summary>
    internal async Task SeedMemoryGrantsAsync(int maxWaiters, int maxGrantees = 10,
        long timeoutErrors = 0, long forcedGrants = 0)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var timeoutDeltaPerPoint = timeoutErrors / 16;
        var forcedDeltaPerPoint = forcedGrants / 16;

        for (var i = 0; i < 16; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name,
     resource_semaphore_id, waiter_count, grantee_count,
     timeout_error_count_delta, forced_grant_count_delta)
VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8)";

            var t = TestPeriodStart.AddMinutes(i * 15);
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = maxWaiters });
            cmd.Parameters.Add(new DuckDBParameter { Value = maxGrantees });
            cmd.Parameters.Add(new DuckDBParameter { Value = timeoutDeltaPerPoint });
            cmd.Parameters.Add(new DuckDBParameter { Value = forcedDeltaPerPoint });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds query_stats with aggregate spill and DOP data.
    /// Creates individual query entries that the collector aggregates.
    /// </summary>
    internal async Task SeedQueryStatsAsync(long totalSpills, int highDopQueryCount,
        long totalExecutions = 10_000)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        // Spilling queries
        var spillingQueries = Math.Max(1, (int)(totalSpills / 100)); // ~100 spills per query
        var spillsPerQuery = totalSpills / spillingQueries;
        var execsPerQuery = totalExecutions / (spillingQueries + highDopQueryCount + 5);

        var totalQueries = spillingQueries + highDopQueryCount;
        var intervalMinutes = 240.0 / Math.Max(totalQueries, 1);

        for (var i = 0; i < spillingQueries; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name,
     query_hash, delta_spills, max_dop, delta_execution_count,
     delta_worker_time, delta_elapsed_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

            var t = TestPeriodStart.AddMinutes(i * intervalMinutes);
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"0xSPILL{i:D4}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = spillsPerQuery });
            cmd.Parameters.Add(new DuckDBParameter { Value = 4 }); // normal DOP
            cmd.Parameters.Add(new DuckDBParameter { Value = execsPerQuery });
            cmd.Parameters.Add(new DuckDBParameter { Value = execsPerQuery * 50_000L }); // 50ms avg CPU
            cmd.Parameters.Add(new DuckDBParameter { Value = execsPerQuery * 100_000L });

            await cmd.ExecuteNonQueryAsync();
        }

        // High-DOP queries
        for (var i = 0; i < highDopQueryCount; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name,
     query_hash, delta_spills, max_dop, delta_execution_count,
     delta_worker_time, delta_elapsed_time)
VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, $9)";

            var t = TestPeriodStart.AddMinutes((spillingQueries + i) * intervalMinutes);
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"0xHDOP{i:D4}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = 16 + (i % 16) }); // DOP > 8
            cmd.Parameters.Add(new DuckDBParameter { Value = execsPerQuery });
            cmd.Parameters.Add(new DuckDBParameter { Value = execsPerQuery * 200_000L });
            cmd.Parameters.Add(new DuckDBParameter { Value = execsPerQuery * 50_000L });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds perfmon_stats with key counters. PLE uses cntr_value (absolute);
    /// rate counters use delta_cntr_value.
    /// </summary>
    internal async Task SeedPerfmonAsync(long ple, long batchReqSec = 500,
        long compilationsSec = 50, long recompilationsSec = 5)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var counters = new (string name, long cntrValue, long deltaValue)[]
        {
            ("Page life expectancy", ple, 0),
            ("Batch Requests/sec", batchReqSec * 60, batchReqSec), // cntr = cumulative, delta = rate
            ("SQL Compilations/sec", compilationsSec * 60, compilationsSec),
            ("SQL Re-Compilations/sec", recompilationsSec * 60, recompilationsSec)
        };

        foreach (var (name, cntr, delta) in counters)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name,
     object_name, counter_name, cntr_value, delta_cntr_value, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 60)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = "SQLServer:Buffer Manager" });
            cmd.Parameters.Add(new DuckDBParameter { Value = name });
            cmd.Parameters.Add(new DuckDBParameter { Value = cntr });
            cmd.Parameters.Add(new DuckDBParameter { Value = delta });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds memory_clerks with clerk type → MB mappings.
    /// </summary>
    internal async Task SeedMemoryClerksAsync(Dictionary<string, double> clerks)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        foreach (var (clerkType, memoryMb) in clerks)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO memory_clerks
    (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES ($1, $2, $3, $4, $5, $6)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = clerkType });
            cmd.Parameters.Add(new DuckDBParameter { Value = memoryMb });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds database_config with per-database configuration flags.
    /// </summary>
    internal async Task SeedDatabaseConfigAsync(
        params (string dbName, bool rcsiOn, bool autoShrink, bool autoClose, string pageVerify)[] databases)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        foreach (var (dbName, rcsiOn, autoShrink, autoClose, pageVerify) in databases)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name,
     recovery_model, is_auto_shrink_on, is_auto_close_on,
     is_read_committed_snapshot_on, is_auto_create_stats_on,
     is_auto_update_stats_on, page_verify_option, is_query_store_on)
VALUES ($1, $2, $3, $4, $5, 'FULL', $6, $7, $8, true, true, $9, false)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = dbName });
            cmd.Parameters.Add(new DuckDBParameter { Value = autoShrink });
            cmd.Parameters.Add(new DuckDBParameter { Value = autoClose });
            cmd.Parameters.Add(new DuckDBParameter { Value = rcsiOn });
            cmd.Parameters.Add(new DuckDBParameter { Value = pageVerify });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds procedure_stats with aggregate execution data.
    /// </summary>
    internal async Task SeedProcedureStatsAsync(int distinctProcs, long totalExecs, long totalCpuUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var execsPerProc = totalExecs / distinctProcs;
        var cpuPerProc = totalCpuUs / distinctProcs;

        for (var i = 0; i < distinctProcs; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, schema_name, object_name,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $9, $10)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd.AddMinutes(-i) });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = "UserDB" });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"usp_TestProc_{i}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = execsPerProc });
            cmd.Parameters.Add(new DuckDBParameter { Value = cpuPerProc });
            cmd.Parameters.Add(new DuckDBParameter { Value = cpuPerProc * 2 }); // elapsed ~2x CPU
            cmd.Parameters.Add(new DuckDBParameter { Value = execsPerProc * 1000L }); // 1000 reads/exec

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds query_snapshots (active queries) with snapshot data.
    /// </summary>
    internal async Task SeedActiveQueriesAsync(int longRunning, int blocked,
        int parallel, long maxElapsedMs = 120_000, int maxDop = 8)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var total = longRunning + blocked + parallel + 5; // +5 short normal queries

        for (var i = 0; i < total; i++)
        {
            var isLongRunning = i < longRunning;
            var isBlocked = i >= longRunning && i < longRunning + blocked;
            var isParallel = i >= longRunning + blocked && i < longRunning + blocked + parallel;

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name,
     session_id, total_elapsed_time_ms, blocking_session_id,
     dop, status, cpu_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

            var t = TestPeriodStart.AddMinutes(i * 5);
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = 50 + i });
            cmd.Parameters.Add(new DuckDBParameter { Value = isLongRunning ? maxElapsedMs : 5_000L });
            cmd.Parameters.Add(new DuckDBParameter { Value = isBlocked ? 51 : 0 });
            cmd.Parameters.Add(new DuckDBParameter { Value = isParallel ? maxDop : 1 });
            cmd.Parameters.Add(new DuckDBParameter { Value = isBlocked ? "suspended" : "running" });
            cmd.Parameters.Add(new DuckDBParameter { Value = isLongRunning ? maxElapsedMs / 2 : 2_000L });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds running_jobs with job execution data.
    /// </summary>
    internal async Task SeedRunningJobsAsync(int totalJobs, int runningLong,
        double maxPctAvg = 300, long maxDurationSeconds = 7200)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        for (var i = 0; i < totalJobs; i++)
        {
            var isLong = i < runningLong;

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO running_jobs
    (collection_time, server_id, server_name, job_name, job_id,
     job_enabled, start_time, current_duration_seconds,
     avg_duration_seconds, p95_duration_seconds, successful_run_count,
     is_running_long, percent_of_average)
VALUES ($1, $2, $3, $4, $5, true, $6, $7, $8, $9, 100, $10, $11)";

            var t = TestPeriodEnd.AddMinutes(-10);
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"Test Job {i}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = Guid.NewGuid().ToString() });
            cmd.Parameters.Add(new DuckDBParameter { Value = t.AddSeconds(-(isLong ? maxDurationSeconds : 300)) });
            cmd.Parameters.Add(new DuckDBParameter { Value = isLong ? maxDurationSeconds : 300L });
            cmd.Parameters.Add(new DuckDBParameter { Value = isLong ? maxDurationSeconds / 3 : 300L }); // avg
            cmd.Parameters.Add(new DuckDBParameter { Value = isLong ? maxDurationSeconds / 2 : 400L }); // p95
            cmd.Parameters.Add(new DuckDBParameter { Value = isLong });
            cmd.Parameters.Add(new DuckDBParameter { Value = isLong ? maxPctAvg : 100.0 });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds session_stats with per-application connection data.
    /// </summary>
    internal async Task SeedSessionStatsAsync(
        params (string appName, int connections, int running, int sleeping)[] apps)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        foreach (var (appName, conns, running, sleeping) in apps)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO session_stats
    (collection_id, collection_time, server_id, server_name,
     program_name, connection_count, running_count, sleeping_count, dormant_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = appName });
            cmd.Parameters.Add(new DuckDBParameter { Value = conns });
            cmd.Parameters.Add(new DuckDBParameter { Value = running });
            cmd.Parameters.Add(new DuckDBParameter { Value = sleeping });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds trace_flags with active global flags.
    /// </summary>
    internal async Task SeedTraceFlagsAsync(params int[] flags)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        foreach (var flag in flags)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO trace_flags
    (config_id, capture_time, server_id, server_name,
     trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, $4, $5, true, true, false)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = flag });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds server_properties with hardware/edition info.
    /// </summary>
    internal async Task SeedServerPropertiesAsync(int cpuCount, int htRatio,
        long physicalMemMb, int socketCount = 2, int coresPerSocket = 0,
        bool hadrEnabled = false, string edition = "Standard Edition",
        int engineEdition = 2, string? serviceObjective = null, int? vcoreCount = null)
    {
        if (coresPerSocket == 0) coresPerSocket = cpuCount / (socketCount * 2); // assume HT

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name,
     edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb,
     socket_count, cores_per_socket, is_hadr_enabled,
     service_objective, vcore_count)
VALUES ($1, $2, $3, $4, $5, '16.0.4150.1', 'RTM', $12,
        $6, $7, $8, $9, $10, $11, $13, $14)";

        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = edition });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuCount });
        cmd.Parameters.Add(new DuckDBParameter { Value = htRatio });
        cmd.Parameters.Add(new DuckDBParameter { Value = physicalMemMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = socketCount });
        cmd.Parameters.Add(new DuckDBParameter { Value = coresPerSocket });
        cmd.Parameters.Add(new DuckDBParameter { Value = hadrEnabled });
        cmd.Parameters.Add(new DuckDBParameter { Value = engineEdition });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)serviceObjective ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)vcoreCount ?? DBNull.Value });

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Seeds database_size_stats with volume-level disk space data.
    /// </summary>
    internal async Task SeedDiskSpaceAsync(
        params (string mountPoint, double totalMb, double freeMb)[] volumes)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        foreach (var (mountPoint, totalMb, freeMb) in volumes)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, database_id, file_id, file_type_desc, file_name, physical_name,
     total_size_mb, used_size_mb,
     volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1, $2, $3, $4, 'UserDB', 5, 1, 'ROWS', 'UserDB', 'D:\Data\UserDB.mdf',
        1000, 800, $5, $6, $7)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = mountPoint });
            cmd.Parameters.Add(new DuckDBParameter { Value = totalMb });
            cmd.Parameters.Add(new DuckDBParameter { Value = freeMb });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    // ============================================
    // FinOps Test Scenarios
    // ============================================

    /// <summary>
    /// Scenario 1: Over-provisioned Enterprise server.
    /// 32 cores, 256GB RAM, but avg CPU 8%, buffer pool only 40GB of 256GB.
    ///
    /// Expected recommendations:
    ///   - CPU right-sizing (P95 &lt; 30%, many cores)
    ///   - Memory right-sizing (buffer pool &lt; 50% of physical RAM)
    /// </summary>
    public async Task SeedOverProvisionedEnterpriseAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // 32 cores, 256GB RAM, but avg CPU 8%, buffer pool only 40GB of 256GB
        await SeedCpuUtilizationAsync(8, 2);
        await SeedMemoryStatsAsync(totalPhysicalMb: 262_144, bufferPoolMb: 40_960, targetMb: 245_760);
        await SeedServerPropertiesAsync(cpuCount: 32, htRatio: 2, physicalMemMb: 262_144,
            edition: "Enterprise Edition");
        await SeedFileSizeAsync(totalDataSizeMb: 51_200); // 50GB — tiny for 256GB RAM
    }

    /// <summary>
    /// Scenario 2: Idle databases with cost impact.
    /// 3 databases seeded — only 1 has query activity, the other 2 are idle.
    ///
    /// Expected recommendations:
    ///   - Dormant database detection (2 idle databases)
    /// </summary>
    public async Task SeedIdleDatabasesAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // Seed database sizes for 3 databases + query activity for only 1
        await SeedDatabaseSizesForIdleTestAsync();
        await SeedQueryStatsForDatabaseAsync("ActiveDB", executions: 5000, cpuMs: 100_000);
    }

    /// <summary>
    /// Scenario 3: High impact query skew — one query consuming 80%+ of CPU.
    ///
    /// Expected: HighImpactScorer.Score() returns query "AAAA" with dominant CpuShare.
    /// </summary>
    public async Task SeedHighImpactQuerySkewAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // 5 queries: one uses 80% CPU, rest split the remaining 20%
        await SeedQueryStatsForHighImpactAsync();
    }

    /// <summary>
    /// Scenario 4: Dev/test databases on a production server.
    /// Seeds database_size_stats with databases named "staging_app", "dev_analytics", "test_warehouse".
    ///
    /// NOTE: The recommendation engine detects dev/test databases via a LIVE SQL query
    /// (sys.databases WHERE name LIKE '%dev%'). This won't fire against DuckDB test data.
    /// The scenario documents the expected behavior but the live check will silently fail.
    /// Use this scenario to test the idle-database detection path instead.
    /// </summary>
    public async Task SeedDevTestDatabasesAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        await SeedDatabaseSizesWithNamesAsync("staging_app", "dev_analytics", "test_warehouse", "ProductionDB");
    }

    /// <summary>
    /// Scenario 5: Long-running maintenance jobs.
    /// Seeds running_jobs with a job that ran long 5+ times in 7 days.
    ///
    /// Expected recommendations:
    ///   - Maintenance window efficiency warning
    /// </summary>
    public async Task SeedLongRunningJobsAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        await SeedRunningJobsForMaintenanceTestAsync();
    }

    /// <summary>
    /// Scenario 6: Clean FinOps server — no recommendations expected.
    /// Healthy CPU (50%), good buffer pool ratio (75%), no idle databases.
    ///
    /// Expected: empty or minimal recommendation list.
    /// </summary>
    public async Task SeedCleanFinOpsServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // Healthy: 50% CPU, 75% buffer pool ratio, no idle databases
        await SeedCpuUtilizationAsync(50, 5);
        await SeedMemoryStatsAsync(totalPhysicalMb: 65_536, bufferPoolMb: 49_152, targetMb: 57_344);
        await SeedServerPropertiesAsync(cpuCount: 8, htRatio: 2, physicalMemMb: 65_536,
            edition: "Developer Edition");
        await SeedFileSizeAsync(totalDataSizeMb: 204_800); // 200GB
    }

    // ============================================
    // FinOps Test Runner Methods
    // ============================================

    /// <summary>
    /// Runs the FinOps recommendation engine against test data.
    /// Pass empty strings for connectionString/utilityConnectionString to skip live SQL checks.
    /// </summary>
    public async Task<List<PerformanceMonitorLite.Services.RecommendationRow>> RunFinOpsRecommendationsAsync(
        PerformanceMonitorLite.Services.LocalDataService dataService, decimal monthlyCost = 10000m)
    {
        return await dataService.GetRecommendationsAsync(TestServerId, "", "", monthlyCost);
    }

    /// <summary>
    /// Runs the High Impact scorer against test data.
    /// </summary>
    public async Task<List<PerformanceMonitorLite.Services.HighImpactQueryRow>> RunHighImpactAnalysisAsync(
        PerformanceMonitorLite.Services.LocalDataService dataService, int hoursBack = 24)
    {
        return await dataService.GetHighImpactQueriesAsync(TestServerId, hoursBack);
    }

    // ============================================
    // FinOps Seed Helpers
    // ============================================

    /// <summary>
    /// Seeds database_size_stats with 3 databases for idle-database testing.
    /// "ActiveDB" will have query_stats activity (seeded separately).
    /// "OldReportsDB" (50GB) and "ArchiveDB" (100GB) have no activity — should be detected as idle.
    /// </summary>
    internal async Task SeedDatabaseSizesForIdleTestAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var databases = new (string name, int dbId, decimal totalSizeMb)[]
        {
            ("ActiveDB",      10, 20_480),   // 20GB — active
            ("OldReportsDB",  11, 51_200),   // 50GB — idle
            ("ArchiveDB",     12, 102_400),  // 100GB — idle
        };

        foreach (var (name, dbId, totalSizeMb) in databases)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, database_id, file_id, file_type_desc, file_name, physical_name,
     total_size_mb, used_size_mb)
VALUES ($1, $2, $3, $4, $5, $6, 1, 'ROWS', $7, $8, $9, $10)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = name });
            cmd.Parameters.Add(new DuckDBParameter { Value = dbId });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"{name}.mdf" });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"D:\\Data\\{name}.mdf" });
            cmd.Parameters.Add(new DuckDBParameter { Value = totalSizeMb });
            cmd.Parameters.Add(new DuckDBParameter { Value = totalSizeMb * 0.8m }); // 80% used

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds query_stats with activity for a specific database.
    /// Used to mark a database as "active" so it's excluded from idle detection.
    /// </summary>
    internal async Task SeedQueryStatsForDatabaseAsync(string databaseName, long executions, long cpuMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        // Spread across 16 collection points so it falls within time-range queries
        var execsPerPoint = executions / 16;
        var cpuPerPoint = cpuMs * 1000 / 16; // convert ms to microseconds for delta_worker_time

        for (var i = 0; i < 16; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, query_hash, delta_execution_count,
     delta_worker_time, delta_elapsed_time, delta_logical_reads)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

            var t = TestPeriodStart.AddMinutes(i * 15);
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"0xACTIVE{i:D4}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = execsPerPoint });
            cmd.Parameters.Add(new DuckDBParameter { Value = cpuPerPoint });
            cmd.Parameters.Add(new DuckDBParameter { Value = cpuPerPoint * 2 });
            cmd.Parameters.Add(new DuckDBParameter { Value = execsPerPoint * 500L });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds query_stats for high-impact skew testing.
    /// 5 queries with one dominant (80% CPU):
    ///   AAAA — 800,000ms CPU, 10,000 executions (the monster)
    ///   BBBB — 50,000ms CPU, 5,000 executions
    ///   CCCC — 50,000ms CPU, 2,000 executions
    ///   DDDD — 50,000ms CPU, 1,000 executions
    ///   EEEE — 50,000ms CPU, 500 executions
    /// </summary>
    internal async Task SeedQueryStatsForHighImpactAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var queries = new (string hash, long cpuMs, long executions, long reads, long writes, long memoryKb)[]
        {
            ("AAAA", 800_000, 10_000, 50_000_000, 1_000_000, 512_000),  // The monster
            ("BBBB",  50_000,  5_000,  5_000_000,   100_000,  64_000),
            ("CCCC",  50_000,  2_000,  3_000_000,    50_000,  32_000),
            ("DDDD",  50_000,  1_000,  2_000_000,    25_000,  16_000),
            ("EEEE",  50_000,    500,  1_000_000,    10_000,   8_000),
        };

        foreach (var (hash, cpuMs, executions, reads, writes, memoryKb) in queries)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, query_hash, query_text,
     delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_logical_writes, max_grant_kb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd.AddMinutes(-30) }); // recent
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = "UserDB" });
            cmd.Parameters.Add(new DuckDBParameter { Value = hash });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"SELECT /* {hash} */ * FROM dbo.SomeTable" });
            cmd.Parameters.Add(new DuckDBParameter { Value = executions });
            cmd.Parameters.Add(new DuckDBParameter { Value = cpuMs * 1000L }); // microseconds
            cmd.Parameters.Add(new DuckDBParameter { Value = cpuMs * 2000L }); // elapsed ~2x CPU
            cmd.Parameters.Add(new DuckDBParameter { Value = reads });
            cmd.Parameters.Add(new DuckDBParameter { Value = writes });
            cmd.Parameters.Add(new DuckDBParameter { Value = memoryKb });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds database_size_stats with named databases.
    /// Used for dev/test detection testing and general size seeding.
    /// </summary>
    internal async Task SeedDatabaseSizesWithNamesAsync(params string[] databaseNames)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        for (var i = 0; i < databaseNames.Length; i++)
        {
            var name = databaseNames[i];
            var sizeMb = 10_240m + (i * 5_120m); // 10GB, 15GB, 20GB, 25GB, ...

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, database_id, file_id, file_type_desc, file_name, physical_name,
     total_size_mb, used_size_mb)
VALUES ($1, $2, $3, $4, $5, $6, 1, 'ROWS', $7, $8, $9, $10)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = name });
            cmd.Parameters.Add(new DuckDBParameter { Value = 10 + i }); // database_id
            cmd.Parameters.Add(new DuckDBParameter { Value = $"{name}.mdf" });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"D:\\Data\\{name}.mdf" });
            cmd.Parameters.Add(new DuckDBParameter { Value = sizeMb });
            cmd.Parameters.Add(new DuckDBParameter { Value = sizeMb * 0.7m }); // 70% used

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds running_jobs for maintenance window testing.
    /// Creates a "Weekly Index Rebuild" job that ran long 5 times in 7 days,
    /// and a normal "Stats Update" job for contrast.
    /// </summary>
    internal async Task SeedRunningJobsForMaintenanceTestAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        // "Weekly Index Rebuild" — ran long 5 times
        var jobId = Guid.NewGuid().ToString();
        for (var i = 0; i < 5; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO running_jobs
    (collection_time, server_id, server_name, job_name, job_id,
     job_enabled, start_time, current_duration_seconds,
     avg_duration_seconds, p95_duration_seconds, successful_run_count,
     is_running_long, percent_of_average)
VALUES ($1, $2, $3, $4, $5, true, $6, $7, $8, $9, 50, true, $10)";

            // Spread collections across the 7-day window the recommendation engine queries
            var collectionTime = DateTime.UtcNow.AddDays(-6).AddDays(i * 1.2);
            cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = "Weekly Index Rebuild" });
            cmd.Parameters.Add(new DuckDBParameter { Value = jobId });
            cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime.AddSeconds(-900) }); // started 15min ago
            cmd.Parameters.Add(new DuckDBParameter { Value = 900L });   // current_duration_seconds (15min)
            cmd.Parameters.Add(new DuckDBParameter { Value = 300L });   // avg_duration_seconds (5min historical)
            cmd.Parameters.Add(new DuckDBParameter { Value = 450L });   // p95_duration_seconds
            cmd.Parameters.Add(new DuckDBParameter { Value = 300.0 });  // percent_of_average = 300%

            await cmd.ExecuteNonQueryAsync();
        }

        // "Stats Update" — normal job, not running long
        using var normalCmd = connection.CreateCommand();
        normalCmd.CommandText = @"
INSERT INTO running_jobs
    (collection_time, server_id, server_name, job_name, job_id,
     job_enabled, start_time, current_duration_seconds,
     avg_duration_seconds, p95_duration_seconds, successful_run_count,
     is_running_long, percent_of_average)
VALUES ($1, $2, $3, $4, $5, true, $6, 120, 100, 130, 200, false, 120.0)";

        normalCmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd.AddMinutes(-5) });
        normalCmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
        normalCmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
        normalCmd.Parameters.Add(new DuckDBParameter { Value = "Stats Update" });
        normalCmd.Parameters.Add(new DuckDBParameter { Value = Guid.NewGuid().ToString() });
        normalCmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd.AddMinutes(-7) });

        await normalCmd.ExecuteNonQueryAsync();
    }

    // ============================================
    // Phase 3 FinOps Test Scenarios
    // ============================================

    /// <summary>
    /// Scenario 7: VM Right-Sizing Target.
    /// 32 cores, 256GB RAM, but P95 CPU only 12%, buffer pool 50GB of 256GB (19%).
    ///
    /// Expected recommendations:
    ///   - CPU: reduce from 32 to 8 cores (P95 &lt; 15% → /4)
    ///   - Memory: reduce from 256GB to 64GB (ratio &lt; 25% → /4)
    /// </summary>
    public async Task SeedVmRightSizingTargetAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // 32 cores, 256GB RAM, but P95 CPU only 12%, buffer pool 50GB of 256GB (19%)
        // Should recommend: 8 cores (P95 < 15%), 64GB RAM (ratio < 25%)
        await SeedCpuUtilizationAsync(12, 2);
        await SeedMemoryStatsAsync(totalPhysicalMb: 262_144, bufferPoolMb: 51_200, targetMb: 245_760);
        await SeedServerPropertiesAsync(cpuCount: 32, htRatio: 2, physicalMemMb: 262_144);
        await SeedFileSizeAsync(totalDataSizeMb: 51_200);
    }

    /// <summary>
    /// Scenario 8: Low IO Latency (Storage Tier).
    /// Avg read latency 1ms, avg write latency 0.5ms — doesn't need premium storage.
    ///
    /// Expected recommendations:
    ///   - Storage tier: database(s) with low IO latency — standard storage may suffice
    /// </summary>
    public async Task SeedLowIoLatencyAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // Avg read latency 1ms, avg write latency 0.5ms — doesn't need premium storage
        await SeedIoLatencyAsync(totalReads: 1_000_000, stallReadMs: 1_000_000,
                                  totalWrites: 200_000, stallWriteMs: 100_000);
    }

    /// <summary>
    /// Scenario 9: Stable CPU for Reserved Capacity.
    /// 24+ samples all between 38-42% — very low variance, CV ~0.04.
    ///
    /// Expected recommendations:
    ///   - Reserved capacity candidate (CV &lt; 0.15, avg &gt; 20%)
    /// </summary>
    public async Task SeedStableCpuForReservedCapacityAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // 24+ samples all between 38-42% — very low variance, CV ~0.04
        // Should trigger reserved capacity recommendation with "High" confidence
        await SeedCpuUtilizationWithVarianceAsync(mean: 40, variance: 2);
    }

    /// <summary>
    /// Scenario 10: Bursty CPU — should NOT trigger reserved capacity.
    /// Alternating 5% and 85% — high CV, should NOT fire.
    ///
    /// Expected: no reserved capacity recommendation.
    /// </summary>
    public async Task SeedBurstyCpuAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // Alternating 5% and 85% — high CV, should NOT trigger
        await SeedCpuUtilizationAlternatingAsync(low: 5, high: 85);
    }

    /// <summary>
    /// Scenario 11: Azure SQL DB vCore tier.
    /// sys.dm_os_sys_info reports 20 cores (compute node total), but the database
    /// is provisioned as HS_Gen5_14 (14 vCores). FinOps should use 14, not 20.
    ///
    /// Expected: CPU right-sizing uses vcore_count (14) instead of cpu_count (20).
    /// </summary>
    public async Task SeedAzureSqlDbVcoreAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        // Azure SQL DB: node has 20 cores, but this DB has HS_Gen5_14 (14 vCores)
        // CPU at 8% avg — overprovisioned relative to 14 vCores
        await SeedCpuUtilizationAsync(8, 2);
        await SeedMemoryStatsAsync(totalPhysicalMb: 65_536, bufferPoolMb: 40_960, targetMb: 57_344);
        await SeedServerPropertiesAsync(cpuCount: 20, htRatio: 1, physicalMemMb: 65_536,
            edition: "SQL Azure", engineEdition: 5,
            serviceObjective: "HS_Gen5_14", vcoreCount: 14);
        await SeedFileSizeAsync(totalDataSizeMb: 51_200);
    }

    // ============================================
    // Phase 3 Seed Helpers
    // ============================================

    /// <summary>
    /// Seeds CPU utilization with controlled variance around a mean value.
    /// Produces 32 samples (8 hours at 15-min intervals) with values cycling
    /// through mean-variance, mean, mean+variance, mean in a deterministic pattern.
    /// </summary>
    internal async Task SeedCpuUtilizationWithVarianceAsync(int mean, int variance)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        // Pattern: mean-variance, mean, mean+variance, mean — repeating
        var offsets = new[] { -variance, 0, variance, 0 };

        for (var i = 0; i < 32; i++)
        {
            var cpuValue = Math.Clamp(mean + offsets[i % 4], 0, 100);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name,
     sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6, $7)";

            var t = TestPeriodStart.AddMinutes(i * 15);
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = cpuValue });
            cmd.Parameters.Add(new DuckDBParameter { Value = 2 });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds CPU utilization alternating between low and high values.
    /// Produces 32 samples with high coefficient of variation — should NOT
    /// trigger reserved capacity recommendations.
    /// </summary>
    internal async Task SeedCpuUtilizationAlternatingAsync(int low, int high)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        for (var i = 0; i < 32; i++)
        {
            var cpuValue = i % 2 == 0 ? low : high;

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name,
     sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6, $7)";

            var t = TestPeriodStart.AddMinutes(i * 15);
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            cmd.Parameters.Add(new DuckDBParameter { Value = cpuValue });
            cmd.Parameters.Add(new DuckDBParameter { Value = 2 });

            await cmd.ExecuteNonQueryAsync();
        }
    }
}
