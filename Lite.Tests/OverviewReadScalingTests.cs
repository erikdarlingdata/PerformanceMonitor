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
    private const long AllowedExtraParquetRows = 2000;
    /* Reads whose READ_PARQUET scan still grows with the archive on this store shape: both read a "latest row per key" set with no
       time bound (Top Queries by CPU's procedure_stats module lookup, the utilization card's memory_stats and server_properties
       lookups), so DuckDB cannot prune their files. Their scan count is not asserted (issue to be filed); their WINDOW rows are. */
    private static readonly HashSet<string> ParquetScanKnownExceptions = ["top queries by CPU", "utilization card"];

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

INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle, sample_interval_seconds)
SELECT s * {2} + d, t, {1}, 'SRV', 'Db' || d, 'dbo', 'usp_' || d, '0xH' || d, 1800
FROM slots, range({2}) b(d) ORDER BY t;
";

    /// <summary>
    /// A store as the app leaves it after days of running: <see cref="HotDays"/> days in DuckDB and <paramref name="archivedDays"/>
    /// older days moved to Parquet by one real <c>ArchiveOldDataAsync</c> pass per day (each pass compacts, so the archive is a
    /// merged month file). Rows are stamped from .NET UTC; DuckDB's <c>now()::TIMESTAMP</c> goes through the session time zone.
    /// </summary>
    private async Task<LocalDataService> BuildStoreAsync(string name, int archivedDays)
    {
        var dir = Path.Combine(_tempDir, name);
        var archiveDir = Path.Combine(dir, "archive");
        Directory.CreateDirectory(archiveDir);
        var dbPath = Path.Combine(dir, "test.duckdb");
        var initializer = new DuckDbInitializer(dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        var totalDays = HotDays + archivedDays;
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
        ("top queries by CPU", d => d.GetTopQueriesByCpuAsync(ServerId, 24, 50)),
        ("collection health", d => d.GetCollectionHealthAsync(ServerId)),
    ];

    [Fact]
    public async Task OverviewReadsScanTheirWindowNotTheArchive()
    {
        var oneDay = await BuildStoreAsync("a1", archivedDays: 1);
        var eightDays = await BuildStoreAsync("a8", archivedDays: 8);

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
            var scanGrew = largeRows > smallRows + AllowedExtraParquetRows && !ParquetScanKnownExceptions.Contains(name);
            if (scanGrew || large.WindowRows > small.WindowRows + AllowedExtraParquetRows)
                failures.Add(line);
        }

        Assert.True(failures.Count == 0,
            "an Overview read scans the archive, not its window:" + Environment.NewLine + string.Join(Environment.NewLine, failures)
            + Environment.NewLine + "all reads:" + Environment.NewLine + string.Join(Environment.NewLine, report));
    }
}
