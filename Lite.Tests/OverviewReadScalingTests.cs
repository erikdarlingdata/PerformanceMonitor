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

INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition)
SELECT s, t, {1}, 'SRV', 'Edition', '15.0.2000.5', 'RTM', 3 FROM slots ORDER BY t;

INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, last_execution_time,
    delta_execution_count, delta_worker_time, delta_elapsed_time, query_text, sql_handle, sample_interval_seconds)
SELECT s * {2} + d, t, {1}, 'SRV', 'Db' || d, '0xQ' || d, '0xP' || d, t, 5, 900000 + d, 900000, 'SELECT ' || d, '0xH' || d, 1800
FROM slots, range({2}) b(d) ORDER BY t;

INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name,
    physical_name, total_size_mb, used_size_mb)
SELECT s * 40 + d, t, {1}, 'SRV', 'Db' || d, d + 5, 1, 'ROWS', 'Db' || d || '_data', 'D:\Db' || d || '.mdf', 1000 + d, 400 + d
FROM slots, range(40) b(d) ORDER BY t;

INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle, sample_interval_seconds)
SELECT s * {2} + d, t, {1}, 'SRV', 'Db' || d, 'dbo', 'usp_' || d, '0xH' || d, 1800
FROM slots, range({2}) b(d) ORDER BY t;
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
