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
/// #7000: every Overview read must cost what its WINDOW holds, not what the archive holds (#6096: Top Queries by CPU ranked every
/// archived procedure_stats row, 432,000 rows from one day of history at 344 databases). Each read runs against two stores in the
/// shape a real install reaches over days (<c>ArchiveOldDataAsync</c> moves each day past the hot window into a compacted Parquet
/// month file, one pass per day) that differ only in how many days sit in the archive behind the same window. The test sums the
/// rows DuckDB's own profiler says the <c>READ_PARQUET</c> nodes scanned: a read that follows its window prunes the archive away
/// (no node, or the same rows in both stores), a read that does not scans the archive in proportion to its size. Rows, never time.
/// </summary>
/* ArchiveOldDataAsync takes ArchiveService's process-wide s_archiveLock, so this class joins the serialized collection
   CollectionResetGateTests defines. */
[Collection("CollectionResetGate")]
public sealed class OverviewReadScalingTests : IDisposable
{
    private const int ServerId = 1;
    private const int Databases = 300;
    private const int SnapshotsPerDay = 48;
    /* Days of history that stay in DuckDB (past the widest window any read here uses, collection health's 7 days). */
    private const int HotDays = 8;
    /* A read that follows its window scans the same Parquet rows in both stores; the slack is a small constant, not a ratio. */
    private const long AllowedExtraParquetRows = 100;
    /* A WINDOW operator (the ranking #6096 ran over the archive) may rank only a fraction of the window's rows of the busiest
       table. Growth between the two stores cannot see a ranking over the WINDOW's rows that should have been only the ranked
       handles' (#6362: 14,400 procedure_stats rows in the 24 h window against about 2,700 for the handles it ranks), so this
       absolute ceiling is what fails when that filter goes. */
    private const long MaxWindowRows = Databases * SnapshotsPerDay / 2;
    /* The test's one known exception. Top Queries by CPU takes each statement's newest module identity from ALL procedure_stats
       history, as Darling's twin does on purpose (a collection_time bound would turn a module into "ad hoc" when the procedure_stats
       collector is disabled or failed for the window), so its READ_PARQUET scan and its WINDOW rows grow with the archive: 72,000
       rows with one archived day, 576,000 with eight, and 167 ms for the whole read at 30 archived days. It is exempt from the
       two growth checks. It still fails above a third of the procedure_stats rows both stores hold: #6362's handle filter ranks
       the handles of the page's groups (23,815 and 42,295 rows measured), a revert ranks every row (129,600 and 230,400). */
    private const string TopQueriesRead = "top queries by CPU";
    private const long MaxTopQueriesWindowRows = Databases * SnapshotsPerDay * (HotDays + 8) / 3;

    private const string InventoryRead = "FinOps server inventory";

    private readonly string _tempDir;
    private readonly List<DuckDbInitializer> _initializers = [];

    public OverviewReadScalingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
            initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); } catch { /* Best-effort cleanup */ }
    }

    private const string CollectionLogSql = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT s * 20 + c, {1}, 'SRV', 'collector_' || c, t, 'SUCCESS' FROM slots, range(20) b(c) ORDER BY t;";

    private const string MetricTablesSql = @"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
    delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
SELECT s * {2} + d, t, {1}, 'SRV', 'Db' || d, 'Db' || d || '_data', 'ROWS', 'D:\Db' || d || '.mdf', 100,
    100 + d, 10, 819200, 81920, 50 + d % 7, 5, 1800
FROM slots, range({2}) b(d) ORDER BY t;

INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
    delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
SELECT s * 30 + w, t, {1}, 'SRV', 'WAIT_' || w, 1000, 5000, 100, 10, 500 + w, 5, 1800
FROM slots, range(30) b(w) ORDER BY t;

INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT s, t, {1}, 'SRV', t, 20 + s % 30, 2 FROM slots ORDER BY t;

INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
    target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
SELECT s, t, {1}, 'SRV', 65536, 10000, 50000, 40000, 30000 FROM slots ORDER BY t;

INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
    utc_offset_minutes)
SELECT s, t, {1}, 'SRV', 'Edition', '15.0.2000.5', 'RTM', 3, -240 FROM slots ORDER BY t;

INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, last_execution_time,
    delta_execution_count, delta_worker_time, delta_elapsed_time, query_text, sql_handle, sample_interval_seconds)
SELECT s * {2} + d, t, {1}, 'SRV', 'Db' || d, '0xQ' || d, '0xP' || d, t, 5, 900000 + d, 900000, 'SELECT ' || d, '0xH' || d, 1800
FROM slots, range({2}) b(d) ORDER BY t;

INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name,
    physical_name, total_size_mb, used_size_mb)
SELECT s * 40 + d, t, {1}, 'SRV', 'Db' || d, d + 5, 1, 'ROWS', 'Db' || d || '_data', 'D:\Db' || d || '.mdf', 1000 + d, 400 + d
FROM slots, range(40) b(d) ORDER BY t;

INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, database_id,
    is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb, current_aborted_transaction_count)
SELECT s * 40 + d, t, {1}, 'SRV', 'Db' || d, d + 5, true, 100 + d, 1000 + d, d % 3
FROM slots, range(40) b(d) ORDER BY t;

INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, user_object_reserved_mb, internal_object_reserved_mb,
    version_store_reserved_mb, total_reserved_mb, unallocated_mb, total_sessions_using_tempdb, top_session_id, top_session_tempdb_mb)
SELECT s, t, {1}, 'SRV', 10, 5, 5, 20, 10, 3, 55, 7 FROM slots ORDER BY t;

/* #7216: the other snapshot tables whose newest-snapshot reads had no time bound. */
INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time, current_duration_seconds,
    avg_duration_seconds, p95_duration_seconds, successful_run_count, is_running_long, percent_of_average)
SELECT t, {1}, 'SRV', 'Job', 'job-1', true, t, 3600, 900, 1200, 42, true, 400.0 FROM slots ORDER BY t;

INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value,
    delta_cntr_value, sample_interval_seconds)
SELECT s * 10 + c, t, {1}, 'SRV', 'SQLServer:SQL Statistics', 'Counter_' || c, '', 100, 10, 1800 FROM slots, range(10) b(c) ORDER BY t;

INSERT INTO memory_clerks (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
SELECT s * 10 + c, t, {1}, 'SRV', 'CLERK_' || c, 100 + c FROM slots, range(10) b(c) ORDER BY t;

INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count)
SELECT s * 10 + c, t, {1}, 'SRV', 'App' || c, 5, 1, 3, 1 FROM slots, range(10) b(c) ORDER BY t;

INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, score)
SELECT s * 10 + c, t, {1}, 'SRV', 'Db' || c, 'PR_' || c, 'Active', 50 FROM slots, range(10) b(c) ORDER BY t;

INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle, sample_interval_seconds)
SELECT s * {2} + d, t, {1}, 'SRV', 'Db' || d, 'dbo', 'usp_' || d, '0xH' || d, 1800
FROM slots, range({2}) b(d) ORDER BY t;

/* #7216: the Index Usage grid and its match count take the newest index_object_stats snapshot. */
INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id,
    table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates)
SELECT s * 10 + c, t, {1}, 'SRV', 'Db' || c, c + 5, 'dbo', 1000 + c, 'Tbl' || c, 2, 'IX_' || c, 'NONCLUSTERED', 10 + c, 10 + c, 1000, 5, 1, 1, 3
FROM slots, range(10) b(c) ORDER BY t;
";

    /// <summary>
    /// A store as the app leaves it after days of running: <see cref="HotDays"/> days in DuckDB and <paramref name="archivedDays"/>
    /// older days moved to Parquet by one real <c>ArchiveOldDataAsync</c> pass per day (each pass compacts, so the archive is a
    /// merged month file). Rows are stamped from .NET UTC; DuckDB's <c>now()::TIMESTAMP</c> goes through the session time zone.
    /// </summary>
    private async Task<LocalDataService> BuildStoreAsync(string name, int archivedDays, int hotDays = HotDays)
    {
        var dir = Path.Combine(_tempDir, name);
        var archiveDir = Path.Combine(dir, "archive");
        Directory.CreateDirectory(archiveDir);
        var dbPath = Path.Combine(dir, "test.duckdb");
        var initializer = new DuckDbInitializer(dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        var totalDays = hotDays + archivedDays;
        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            var now = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
            var slotsSql = $"CREATE TEMP TABLE slots AS SELECT s, TIMESTAMP '{now}' - (s * INTERVAL 30 MINUTE) AS t FROM range({totalDays * SnapshotsPerDay}) a(s);";
            cmd.CommandText = slotsSql + (MetricTablesSql + CollectionLogSql).Replace("{1}", ServerId.ToString()).Replace("{2}", Databases.ToString());
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var archive = new ArchiveService(initializer, archiveDir);
        for (var pass = 1; pass <= archivedDays; pass++)
        {
            /* Pass n archives everything older than (totalDays - n) days: one more day per pass, the last leaves HotDays. */
            archive.TimestampForTests = DateTime.UtcNow.AddMinutes(pass).ToString("yyyyMMdd_HHmm");
            await archive.ArchiveOldDataAsync(hotDataHours: (totalDays - pass) * 24);
        }

        return new LocalDataService(initializer);
    }

    /// <summary>One <c>READ_PARQUET</c> scan node in a statement's profile.</summary>
    private sealed record ParquetScan(string Statement, long RowsScanned, long RowsOut, string Columns);

    /// <summary>What one read's profile holds: its Parquet scans, and the rows its WINDOW operators produced (the ranking #6096 ran over the archive).</summary>
    private sealed class ProfiledRead
    {
        public List<ParquetScan> Scans { get; } = [];
        public long WindowRows { get; set; }
    }

    private static void CollectParquetScans(JsonElement node, string statement, ProfiledRead profiled)
    {
        var scans = profiled.Scans;
        if (node.TryGetProperty("operator_type", out var type)
            && (type.GetString() ?? "").Contains("WINDOW", StringComparison.OrdinalIgnoreCase)
            && node.TryGetProperty("operator_cardinality", out var windowRows))
            profiled.WindowRows += windowRows.GetInt64();

        if (node.TryGetProperty("extra_info", out var extra)
            && extra.ValueKind == JsonValueKind.Object
            && extra.TryGetProperty("Function", out var function)
            && string.Equals(function.GetString(), "READ_PARQUET", StringComparison.OrdinalIgnoreCase))
        {
            var columns = extra.TryGetProperty("Projections", out var projections) && projections.ValueKind == JsonValueKind.Array
                ? string.Join(",", projections.EnumerateArray().Take(3).Select(p => p.GetString()))
                : "";
            scans.Add(new ParquetScan(
                statement,
                node.TryGetProperty("operator_rows_scanned", out var scanned) ? scanned.GetInt64() : 0,
                node.TryGetProperty("operator_cardinality", out var rowsOut) ? rowsOut.GetInt64() : 0,
                columns));
        }

        if (node.TryGetProperty("children", out var children))
            foreach (var child in children.EnumerateArray())
                CollectParquetScans(child, statement, profiled);
    }

    /// <summary>
    /// Runs a public read with DuckDB's JSON profiler on for every connection it opens and returns every <c>READ_PARQUET</c> node of
    /// the profiled statements. A read that runs several statements on one connection reports its last one.
    /// </summary>
    private async Task<ProfiledRead> ParquetScansAsync(LocalDataService data, string tag, Func<LocalDataService, Task> read)
    {
        var profileDir = Path.Combine(_tempDir, "profile_" + tag);
        Directory.CreateDirectory(profileDir);
        var opened = 0;
        data.ConnectionOpened = connection =>
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"PRAGMA enable_profiling = 'json'; PRAGMA profiling_output = '{Path.Combine(profileDir, $"p{++opened}.json").Replace('\\', '/')}'";
            cmd.ExecuteNonQuery();
        };
        try
        {
            await read(data);
        }
        finally
        {
            data.ConnectionOpened = null;
        }

        var profiled = new ProfiledRead();
        foreach (var file in Directory.GetFiles(profileDir, "p*.json"))
        {
            using var profile = JsonDocument.Parse(await File.ReadAllTextAsync(file, TestContext.Current.CancellationToken));
            CollectParquetScans(profile.RootElement, Path.GetFileName(file), profiled);
        }

        return profiled;
    }

    /// <summary>The reads of the Overview page.</summary>
    private static (string Name, Func<LocalDataService, Task> Read)[] OverviewReads() =>
    [
        ("file I/O latency trend", d => d.GetFileIoLatencyTrendAsync(ServerId, 24)),
        ("wait stats trend (picked types)", d => d.GetWaitStatsTrendsByTypesAsync(ServerId, ["WAIT_1", "WAIT_2"], 24)),
        ("wait stats trend (total)", d => d.GetTotalWaitTrendAsync(ServerId, 24)),
        ("utilization card", d => d.GetUtilizationEfficiencyAsync(ServerId)),
        (TopQueriesRead, d => d.GetTopQueriesByCpuAsync(ServerId, 24, 50)),
        ("collection health", d => d.GetCollectionHealthAsync(ServerId)),
        /* #7014: the FinOps Server Inventory read, with 40 databases per size snapshot (the field's shape, not Databases). */
        (InventoryRead, d => d.GetServerMetricsAsync()),
        /* #7014: the per-server idle-coverage check (the Optimization tab's Idle Databases grid and the recommendation row). */
        ("query stats idle coverage", d => d.HasQueryStatsCoverageAsync(ServerId)),
        /* #7216: the PVS alert read, the FinOps PVS chart (its top-5 lookup) and grid, and the tempdb alert read all take a server's newest row. */
        ("PVS pressure alert read", d => d.GetPvsPressureAsync(ServerId)),
        ("PVS trend", d => d.GetPvsTrendAsync(ServerId, DateTime.UtcNow.AddHours(-24))),
        ("PVS grid", d => d.GetPvsStatsLatestAsync(ServerId)),
        ("tempdb alert read", d => d.GetLatestTempDbSpaceAsync(ServerId)),
        /* #7216: the reads that take a server's newest snapshot of a table, by the same unbounded pattern. */
        ("volume free space alert read", d => d.GetVolumeFreeSpaceAsync(ServerId)),
        ("running jobs", d => d.ReadRunningJobsAsync(ServerId)),
        ("running jobs snapshot time", d => d.GetLatestRunningJobsSnapshotTimeAsync(ServerId)),
        ("anomalous jobs alert read", d => d.GetAnomalousJobsAsync(ServerId, 2)),
        /* #7216: the "newest server_properties row that carries an offset" reads (the anomalous jobs read above joins the same one). */
        ("server UTC offset", d => d.GetServerUtcOffsetMinutesAsync(ServerId)),
        ("server clock", d => d.GetServerClockAsync(ServerId)),
        ("tempdb card", d => d.GetTempdbSummaryAsync(ServerId)),
        ("database size latest (FinOps)", d => d.GetDatabaseSizeLatestAsync(ServerId)),
        ("database size summary", d => d.GetDatabaseSizeSummaryAsync(ServerId)),
        ("idle databases", d => d.GetIdleDatabasesAsync(ServerId)),
        ("database size stats (server info)", d => d.GetLatestDatabaseSizeStatsAsync(ServerId)),
        ("session stats", d => d.GetLatestSessionStatsAsync(ServerId)),
        ("file I/O latest", d => d.GetLatestFileIoStatsAsync(ServerId)),
        ("perfmon latest", d => d.GetLatestPerfmonStatsAsync(ServerId)),
        ("memory clerks", d => d.GetLatestMemoryClerksAsync(ServerId)),
        ("automatic tuning latest", d => d.GetLatestAutomaticTuningAsync(ServerId)),
        /* #7216: the index usage grid, its match count, and the newest memory, server properties and engine edition lookups. */
        ("index usage", d => d.GetIndexUsageAsync(ServerId)),
        ("index usage match count", d => d.GetIndexUsageMatchCountAsync(ServerId)),
        ("latest memory stats", d => d.GetLatestMemoryStatsAsync(ServerId)),
        ("latest server properties", d => d.GetLatestServerPropertiesAsync(ServerId)),
        ("SQL engine edition", d => d.GetSqlEngineEditionAsync(ServerId)),
    ];

    private async Task<(List<string> Failures, List<string> Report)> CheckReadsAsync(LocalDataService oneDay, LocalDataService eightDays)
    {
        var failures = new List<string>();
        var report = new List<string>();
        foreach (var (name, read) in OverviewReads())
        {
            var tag = new string(name.Where(char.IsLetterOrDigit).ToArray());
            var small = await ParquetScansAsync(oneDay, tag + "_s", read);
            var large = await ParquetScansAsync(eightDays, tag + "_l", read);
            var smallRows = small.Scans.Sum(s => s.RowsScanned);
            var largeRows = large.Scans.Sum(s => s.RowsScanned);
            var line = $"{name}: READ_PARQUET rows scanned {smallRows:N0} ({small.Scans.Count} nodes) with 1 archived day, {largeRows:N0} ({large.Scans.Count} nodes) with 8; rows out {small.Scans.Sum(x => x.RowsOut):N0} / {large.Scans.Sum(x => x.RowsOut):N0}; WINDOW rows {small.WindowRows:N0} / {large.WindowRows:N0}; nodes [{string.Join(" | ", large.Scans.Select(x => x.Columns))}]";
            report.Add(line);
            var window = Math.Max(small.WindowRows, large.WindowRows);
            var failed = name == TopQueriesRead
                ? window > MaxTopQueriesWindowRows
                : largeRows > smallRows + AllowedExtraParquetRows
                    || large.WindowRows > small.WindowRows + AllowedExtraParquetRows
                    || window > MaxWindowRows;
            if (failed)
                failures.Add(line);
        }

        return (failures, report);
    }

    [Fact]
    public async Task OverviewReadsScanTheirWindowNotTheArchive()
    {
        var oneDay = await BuildStoreAsync("a1", archivedDays: 1);
        var eightDays = await BuildStoreAsync("a8", archivedDays: 8);

        var (failures, report) = await CheckReadsAsync(oneDay, eightDays);

        Assert.True(failures.Count == 0,
            "an Overview read scans the archive, not its window:" + Environment.NewLine + string.Join(Environment.NewLine, failures)
            + Environment.NewLine + "all reads:" + Environment.NewLine + string.Join(Environment.NewLine, report));
    }

    /// <summary>
    /// The idle-coverage rule's second condition, the oldest sample at or before now minus 7 days, with a store whose hot table holds only
    /// 3 days, so the hot table cannot decide it and the archive does (#7014). The answer is true in both stores (every older day is
    /// archived, the 7 complete days before today are all sampled), and the archive rows the fleet rule and the per-server check read
    /// must not grow with the archived days: the per-day count reads only its window, and the oldest-sample question stops at the first
    /// matching row. Measured by rows OUT of the scans: with the archive compacted into one month file, DuckDB's rows-scanned count is the
    /// file's row count whatever the filter (864,000 for a one-hour window and for the whole archive alike), while rows out are what the
    /// pushed-down filter and a LIMIT let through (600 for one hour).
    /// </summary>
    [Fact]
    public async Task IdleCoverageDecidedByTheArchive_ReadsABoundedPartOfItNotTheArchive()
    {
        const int hotDays = 3;
        var small = await BuildStoreAsync("c5", archivedDays: 5, hotDays: hotDays);
        var large = await BuildStoreAsync("c12", archivedDays: 12, hotDays: hotDays);

        var lines = new List<string>();
        long smallRows = 0, largeRows = 0;
        foreach (var (name, read) in new (string, Func<LocalDataService, Task>)[]
        {
            ("per-server check", d => d.HasQueryStatsCoverageAsync(ServerId)),
            ("fleet rule", d => d.IdleCoveredServerIdsAsync(DateTime.UtcNow, serverId: null)),
        })
        {
            var tag = new string(name.Where(char.IsLetterOrDigit).ToArray());
            var smallScans = await ParquetScansAsync(small, tag + "_s", read);
            var largeScans = await ParquetScansAsync(large, tag + "_l", read);
            smallRows += smallScans.Scans.Sum(x => x.RowsOut);
            largeRows += largeScans.Scans.Sum(x => x.RowsOut);
            lines.Add($"{name}: READ_PARQUET rows out {smallScans.Scans.Sum(x => x.RowsOut):N0} ({smallScans.Scans.Count} nodes) with 5 archived days, "
                + $"{largeScans.Scans.Sum(x => x.RowsOut):N0} ({largeScans.Scans.Count} nodes) with 12");
        }

        Assert.True(await small.HasQueryStatsCoverageAsync(ServerId), "5 archived days: covered");
        Assert.True(await large.HasQueryStatsCoverageAsync(ServerId), "12 archived days: covered");
        Assert.Equal(new[] { ServerId }, (await large.IdleCoveredServerIdsAsync(DateTime.UtcNow, serverId: null)).ToArray());
        /* The probe's LIMIT 1 ends after a few 2,048-row vectors (about 10,000 rows out), so the slack is a few vectors. */
        Assert.True(largeRows <= smallRows + 5 * 2048,
            "the idle-coverage reads scan the archive, not their window:" + Environment.NewLine + string.Join(Environment.NewLine, lines));
    }

    /// <summary>
    /// The utilization card reads the hot memory_stats / server_properties table while it holds a row for the server (#7000). A
    /// server whose rows there have all been archived (collection stopped for longer than the hot window) must still show its last
    /// values, which only the v_ views can give. Built with the real archive pass.
    /// </summary>
    [Fact]
    public async Task UtilizationCard_ServerWhoseMemoryRowsAreAllArchived_StillShowsItsLastValues()
    {
        var dir = Path.Combine(_tempDir, "fallback");
        var archiveDir = Path.Combine(dir, "archive");
        Directory.CreateDirectory(archiveDir);
        var dbPath = Path.Combine(dir, "test.duckdb");
        var initializer = new DuckDbInitializer(dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            var now = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
            /* The memory and properties rows are 10 days old (the server stopped being collected); a CPU sample is recent, so the card has a row. */
            cmd.CommandText = $@"
INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
    target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 65536, 10000, 50000, 40000, 30000);
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, cpu_count)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'Edition', '15.0.2000.5', 'RTM', 3, 16);
INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 1 HOUR, {ServerId}, 'SRV', TIMESTAMP '{now}' - INTERVAL 1 HOUR, 20, 2);";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var archive = new ArchiveService(initializer, archiveDir);
        archive.TimestampForTests = DateTime.UtcNow.ToString("yyyyMMdd_HHmm");
        await archive.ArchiveOldDataAsync(hotDataHours: HotDays * 24);

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT (SELECT COUNT(*) FROM memory_stats) + (SELECT COUNT(*) FROM server_properties)";
            Assert.Equal(0L, Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken)));
        }

        var row = await new LocalDataService(initializer).GetUtilizationEfficiencyAsync(ServerId);

        Assert.NotNull(row);
        Assert.Equal(65536, row.PhysicalMemoryMb);
        Assert.Equal(40000, row.TotalMemoryMb);
        Assert.Equal(16, row.CpuCount);
    }

    /// <summary>
    /// The PVS and tempdb newest-row reads (#7216) read the hot tables first. A server that stopped being collected has every row in Parquet,
    /// and each read must still answer from the archive with that server's last values. Built with the real archive pass.
    /// </summary>
    [Fact]
    public async Task NewestRowReads_ServerWhoseRowsAreAllArchived_StillShowItsLastValues()
    {
        var dir = Path.Combine(_tempDir, "pvsfallback");
        var archiveDir = Path.Combine(dir, "archive");
        Directory.CreateDirectory(archiveDir);
        var dbPath = Path.Combine(dir, "test.duckdb");
        var initializer = new DuckDbInitializer(dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            var now = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
            /* Two snapshots, 10 and 11 days old: the newer one (PVS 300 MB, tempdb 20 MB reserved) is what the reads must return. */
            cmd.CommandText = $@"
INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, database_id,
    is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 'DbA', 5, true, 100, 1000),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'DbA', 5, true, 300, 1000);
INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, user_object_reserved_mb, internal_object_reserved_mb,
    version_store_reserved_mb, total_reserved_mb, unallocated_mb, total_sessions_using_tempdb, top_session_id, top_session_tempdb_mb)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 1, 1, 1, 10, 10, 1, 55, 1),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 10, 5, 5, 20, 10, 3, 55, 7);
/* #7216: the other newest-snapshot reads; the 10 day old snapshot carries the 2s, the 11 day old one the 1s. */
INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
    file_name, physical_name, total_size_mb, used_size_mb, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 'DbA', 5, 1, 'ROWS', 'DbA_data', 'D:\DbA.mdf', 1000, 400, 'D:', 1000, 100),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'DbA', 5, 1, 'ROWS', 'DbA_data', 'D:\DbA.mdf', 2000, 400, 'D:', 1000, 200);
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
    delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 'DbA', 'DbA_data', 'ROWS', 'D:\DbA.mdf', 100, 1, 1, 1, 1, 1, 1, 1800),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'DbA', 'DbA_data', 'ROWS', 'D:\DbA.mdf', 100, 2, 1, 1, 1, 1, 1, 1800);
INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value,
    delta_cntr_value, sample_interval_seconds)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 'SQLServer:SQL Statistics', 'Counter', '', 1, 1, 1800),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'SQLServer:SQL Statistics', 'Counter', '', 2, 2, 1800);
INSERT INTO memory_clerks (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 'CLERK', 1),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'CLERK', 2);
INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 'App', 1, 0, 0, 0),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'App', 2, 0, 0, 0);
INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, force_last_good_plan_actual_state, recommendation_name, recommendation_state, score)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 'DbA', 'OLD', 'PR_1', 'Active', 50),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'DbA', 'NEW', 'PR_1', 'Active', 50);
INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time, current_duration_seconds,
    avg_duration_seconds, p95_duration_seconds, successful_run_count, is_running_long, percent_of_average)
VALUES (TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 'OldJob', 'job-1', true, TIMESTAMP '{now}' - INTERVAL 11 DAY, 3600, 900, 1200, 42, true, 400.0),
       (TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'NewJob', 'job-1', true, TIMESTAMP '{now}' - INTERVAL 10 DAY, 3600, 900, 1200, 42, true, 400.0);
INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id,
    table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 'DbA', 5, 'dbo', 1000, 'Tbl', 2, 'IX_Old', 'NONCLUSTERED', 10, 10, 1000, 5, 1, 1, 3),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'DbA', 5, 'dbo', 1000, 'Tbl', 2, 'IX_New', 'NONCLUSTERED', 10, 10, 1000, 5, 1, 1, 3);
INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
    target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 65536, 10000, 50000, 1111, 30000),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 65536, 10000, 50000, 2222, 30000);
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, cpu_count,
    utc_offset_minutes)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 11 DAY, {ServerId}, 'SRV', 'OldEdition', '15.0.2000.5', 'RTM', 3, 4, -300),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, {ServerId}, 'SRV', 'NewEdition', '15.0.2000.5', 'RTM', 5, 16, -240);";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var archive = new ArchiveService(initializer, archiveDir);
        archive.TimestampForTests = DateTime.UtcNow.ToString("yyyyMMdd_HHmm");
        await archive.ArchiveOldDataAsync(hotDataHours: HotDays * 24);

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT (SELECT COUNT(*) FROM pvs_stats) + (SELECT COUNT(*) FROM tempdb_stats) + (SELECT COUNT(*) FROM database_size_stats)"
                + " + (SELECT COUNT(*) FROM file_io_stats) + (SELECT COUNT(*) FROM perfmon_stats) + (SELECT COUNT(*) FROM memory_clerks)"
                + " + (SELECT COUNT(*) FROM session_stats) + (SELECT COUNT(*) FROM plan_correction) + (SELECT COUNT(*) FROM running_jobs)"
                + " + (SELECT COUNT(*) FROM index_object_stats) + (SELECT COUNT(*) FROM memory_stats) + (SELECT COUNT(*) FROM server_properties)";
            Assert.Equal(0L, Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken)));
        }

        var data = new LocalDataService(initializer);

        var pressure = await data.GetPvsPressureAsync(ServerId);
        Assert.Equal(300, Assert.Single(pressure).PvsSizeMb);

        var grid = await data.GetPvsStatsLatestAsync(ServerId);
        Assert.Equal(300m, Assert.Single(grid).PvsSizeMb);

        var trend = await data.GetPvsTrendAsync(ServerId, DateTime.UtcNow.AddDays(-30));
        Assert.Equal(2, trend.Count);

        var tempDb = await data.GetLatestTempDbSpaceAsync(ServerId);
        Assert.NotNull(tempDb);
        Assert.Equal(20, tempDb.TotalReservedMb);

        /* #7216 */
        var tempDbCard = await data.GetTempdbSummaryAsync(ServerId);
        Assert.Equal(20m, tempDbCard.Single(r => r.Metric == "Total Reserved").CurrentMb);

        var volumes = await data.GetVolumeFreeSpaceAsync(ServerId);
        Assert.Equal(200, Assert.Single(volumes).FreeMb);

        Assert.Equal(2000m, Assert.Single(await data.GetDatabaseSizeLatestAsync(ServerId)).TotalSizeMb);
        Assert.Equal(2000m, Assert.Single(await data.GetDatabaseSizeSummaryAsync(ServerId)).TotalMb);
        Assert.Equal(2000, Assert.Single(await data.GetLatestDatabaseSizeStatsAsync(ServerId)).TotalSizeMb);
        Assert.Equal(2, Assert.Single(await data.GetLatestFileIoStatsAsync(ServerId)).DeltaReads);
        Assert.Equal(2, Assert.Single(await data.GetLatestPerfmonStatsAsync(ServerId)).Value);
        Assert.Equal(2, Assert.Single(await data.GetLatestMemoryClerksAsync(ServerId)).MemoryMb);
        Assert.Equal(2, Assert.Single(await data.GetLatestSessionStatsAsync(ServerId)).ConnectionCount);
        Assert.Equal("NEW", Assert.Single(await data.GetLatestAutomaticTuningAsync(ServerId)).ActualState);

        var jobsSnapshot = await data.GetLatestRunningJobsSnapshotTimeAsync(ServerId);
        Assert.NotNull(jobsSnapshot);
        Assert.InRange(DateTime.UtcNow - jobsSnapshot.Value, TimeSpan.FromDays(9.9), TimeSpan.FromDays(10.1));
        var anomalous = Assert.Single(await data.GetAnomalousJobsAsync(ServerId, 2));
        Assert.Equal("NewJob", anomalous.JobName);
        /* #7216: the svr join found the archived offset (the newest one, -240; the older row carries -300). */
        Assert.Equal(-240, anomalous.UtcOffsetMinutes);

        Assert.Equal("IX_New", Assert.Single(await data.GetIndexUsageAsync(ServerId)).IndexName);
        Assert.Equal(1L, await data.GetIndexUsageMatchCountAsync(ServerId));
        Assert.Equal(2222, (await data.GetLatestMemoryStatsAsync(ServerId))!.TotalServerMemoryMb);
        var properties = await data.GetLatestServerPropertiesAsync(ServerId);
        Assert.NotNull(properties);
        Assert.Equal("NewEdition", properties.Edition);
        Assert.Equal(5, await data.GetSqlEngineEditionAsync(ServerId));

        /* #7216: the offset reads probe the hot table for a non-NULL offset and fall back to the view; every row is archived, so the view answers. */
        Assert.Equal(-240, await data.GetServerUtcOffsetMinutesAsync(ServerId));
        var clock = await data.GetServerClockAsync(ServerId);
        Assert.NotNull(clock);
        Assert.Equal(-240, clock.OffsetMinutesAt(DateTime.UtcNow));
    }

    /// <summary>
    /// One probe decides the database size relation for a refresh (#7216). A server whose rows are all archived starts being collected
    /// again between the snapshot lookup and the read at that snapshot: the snapshot came from the archive view, so the read must stay on the
    /// view and return the archived rows, not re-probe, find the new hot row and look for the archived timestamp in the hot table.
    /// </summary>
    [Fact]
    public async Task DatabaseSizeRead_HotRowLandsAfterTheSnapshotLookup_StillReadsTheArchivedSnapshot()
    {
        var dir = Path.Combine(_tempDir, "sizeonce");
        var archiveDir = Path.Combine(dir, "archive");
        Directory.CreateDirectory(archiveDir);
        var dbPath = Path.Combine(dir, "test.duckdb");
        var initializer = new DuckDbInitializer(dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        const string sizeRow = @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id,
    file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
VALUES ({0}, {1}, {2}, 'SRV', 'DbA', 5, 1, 'ROWS', 'DbA_data', 'D:\DbA.mdf', 2000, 400);";
        var now = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = string.Format(sizeRow, 1, $"TIMESTAMP '{now}' - INTERVAL 10 DAY", ServerId);
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var archive = new ArchiveService(initializer, archiveDir);
        archive.TimestampForTests = DateTime.UtcNow.ToString("yyyyMMdd_HHmm");
        await archive.ArchiveOldDataAsync(hotDataHours: HotDays * 24);

        var data = new LocalDataService(initializer);
        var snapshot = await data.GetLatestDatabaseSizeSnapshotAsync(ServerId);
        Assert.NotNull(snapshot);

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = string.Format(sizeRow, 2, $"TIMESTAMP '{now}'", ServerId);
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var rows = await data.GetDatabaseSizeAtAsync(ServerId, snapshot.Value, null);

        Assert.Equal("DbA", Assert.Single(rows).DatabaseName);
    }

    /// <summary>
    /// The FinOps server inventory reads the hot memory / size / properties tables for a server that still has rows there and the archive for a
    /// server whose rows are all archived (#7014). Server 1 is collected now, with an older and a newer size snapshot, and must read its NEWEST
    /// snapshot; server 2 stopped being collected 10 days ago, so every row of it is in Parquet, and it must still be listed with its last
    /// storage. Built with the real archive pass.
    /// </summary>
    [Fact]
    public async Task ServerInventory_ServerWhoseRowsAreAllArchived_IsStillListedBesideAHotServer()
    {
        var dir = Path.Combine(_tempDir, "inventoryFallback");
        var archiveDir = Path.Combine(dir, "archive");
        Directory.CreateDirectory(archiveDir);
        var dbPath = Path.Combine(dir, "test.duckdb");
        var initializer = new DuckDbInitializer(dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            var now = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
            cmd.CommandText = $@"
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 1 HOUR, 1, 'HOT', 'Edition', '15.0.2000.5', 'RTM', 3),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, 2, 'GONE', 'Edition', '15.0.2000.5', 'RTM', 3);
INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
    target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 1 HOUR, 1, 'HOT', 65536, 10000, 50000, 40000, 30000),
       (2, TIMESTAMP '{now}' - INTERVAL 10 DAY, 2, 'GONE', 65536, 10000, 50000, 40000, 30000);
INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
    file_name, physical_name, total_size_mb, used_size_mb)
VALUES (1, TIMESTAMP '{now}' - INTERVAL 2 HOUR, 1, 'HOT', 'A', 5, 1, 'ROWS', 'A_data', 'D:\A.mdf', 1, 1),
       (2, TIMESTAMP '{now}' - INTERVAL 1 HOUR, 1, 'HOT', 'A', 5, 1, 'ROWS', 'A_data', 'D:\A.mdf', 4096, 1024),
       (3, TIMESTAMP '{now}' - INTERVAL 10 DAY, 2, 'GONE', 'B', 5, 1, 'ROWS', 'B_data', 'D:\B.mdf', 2048, 1024),
       (4, TIMESTAMP '{now}' - INTERVAL 10 DAY, 2, 'GONE', 'C', 6, 1, 'ROWS', 'C_data', 'D:\C.mdf', 1024, 512);";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var archive = new ArchiveService(initializer, archiveDir);
        archive.TimestampForTests = DateTime.UtcNow.ToString("yyyyMMdd_HHmm");
        await archive.ArchiveOldDataAsync(hotDataHours: HotDays * 24);

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT (SELECT COUNT(*) FROM memory_stats WHERE server_id = 2) + (SELECT COUNT(*) FROM server_properties WHERE server_id = 2)"
                + " + (SELECT COUNT(*) FROM database_size_stats WHERE server_id = 2)";
            Assert.Equal(0L, Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken)));
        }

        var metrics = await new LocalDataService(initializer).GetServerMetricsAsync();

        Assert.Equal([1, 2], metrics.Keys.OrderBy(k => k).ToArray());
        Assert.Equal(4m, metrics[1].StorageTotalGb);
        Assert.Equal(3m, metrics[2].StorageTotalGb);
    }
}
