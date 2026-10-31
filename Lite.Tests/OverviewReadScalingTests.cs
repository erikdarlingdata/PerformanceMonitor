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
/// archived procedure_stats row, 432,000 rows from one day of history at 344 databases). Each read runs against two stores built
/// through the real <c>ArchiveAllAndResetAsync</c> (so every row is read from Parquet) that differ only in how much history sits
/// behind the same window, and the rows DuckDB's own profiler says it scanned are compared. Rows, never time.
/// </summary>
/* ArchiveAllAndResetAsync takes CollectionResetGate and ArchiveService's process-wide s_archiveLock, so this class joins the
   serialized collection CollectionResetGateTests defines. */
[Collection("CollectionResetGate")]
public sealed class OverviewReadScalingTests : IDisposable
{
    private const int ServerId = 1;
    private const int Databases = 300;
    private const int SnapshotsPerDay = 48;
    /* A read that follows its window scans the same rows in both stores; 2x leaves room for row-group rounding. */
    private const double AllowedGrowth = 2.0;

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

    /// <summary>A store of <paramref name="days"/> days of history, every row archived to Parquet through the real reset.</summary>
    private async Task<LocalDataService> BuildStoreAsync(string name, int days, bool collectionLogOnly)
    {
        var dir = Path.Combine(_tempDir, name);
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
            var slotsSql = $"CREATE TEMP TABLE slots AS SELECT s, now()::TIMESTAMP - (s * INTERVAL 30 MINUTE) AS t FROM range({days * SnapshotsPerDay}) a(s);";
            var rows = collectionLogOnly ? "" : MetricTablesSql;
            cmd.CommandText = slotsSql + (rows + CollectionLogSql).Replace("{1}", ServerId.ToString()).Replace("{2}", Databases.ToString());
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        await new ArchiveService(initializer, archiveDir).ArchiveAllAndResetAsync();
        return new LocalDataService(initializer);
    }

    /// <summary>
    /// Runs a public read with DuckDB's JSON profiler on for every connection it opens, and returns the rows the profiler says
    /// were scanned (the statement's <c>cumulative_rows_scanned</c>, every operator's scan counted). A read that runs several
    /// statements on one connection reports its last one.
    /// </summary>
    private async Task<long> ScannedRowsAsync(LocalDataService data, string tag, Func<LocalDataService, Task> read)
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

        long total = 0;
        foreach (var file in Directory.GetFiles(profileDir, "p*.json"))
        {
            using var profile = JsonDocument.Parse(await File.ReadAllTextAsync(file, TestContext.Current.CancellationToken));
            total += profile.RootElement.GetProperty("cumulative_rows_scanned").GetInt64();
        }

        return total;
    }

    [Fact]
    public async Task OverviewReadsScanTheWindowNotTheArchive()
    {
        var oneDay = await BuildStoreAsync("d1", 1, collectionLogOnly: false);
        var eightDays = await BuildStoreAsync("d8", 8, collectionLogOnly: false);
        /* Collection health reads a 7-day window, so its pair is 7 and 14 days of history. */
        var sevenDays = await BuildStoreAsync("d7", 7, collectionLogOnly: true);
        var fourteenDays = await BuildStoreAsync("d14", 14, collectionLogOnly: true);

        var reads = new (string Name, LocalDataService Small, LocalDataService Large, Func<LocalDataService, Task> Read)[]
        {
            ("file I/O latency trend", oneDay, eightDays, d => d.GetFileIoLatencyTrendAsync(ServerId, 24)),
            ("wait stats trend (picked types)", oneDay, eightDays, d => d.GetWaitStatsTrendsByTypesAsync(ServerId, ["WAIT_1", "WAIT_2"], 24)),
            ("wait stats trend (total)", oneDay, eightDays, d => d.GetTotalWaitTrendAsync(ServerId, 24)),
            ("utilization card", oneDay, eightDays, d => d.GetUtilizationEfficiencyAsync(ServerId)),
            ("top queries by CPU", oneDay, eightDays, d => d.GetTopQueriesByCpuAsync(ServerId, 24, 50)),
            ("collection health", sevenDays, fourteenDays, d => d.GetCollectionHealthAsync(ServerId)),
        };

        var failures = new List<string>();
        var report = new List<string>();
        foreach (var (name, small, large, read) in reads)
        {
            var tag = new string(name.Where(char.IsLetterOrDigit).ToArray());
            var smallRows = await ScannedRowsAsync(small, tag + "_s", read);
            var largeRows = await ScannedRowsAsync(large, tag + "_l", read);
            report.Add($"{name}: {smallRows:N0} rows scanned on the small store, {largeRows:N0} on the large one");
            if (largeRows > Math.Max(smallRows * AllowedGrowth, 1000))
                failures.Add(report[^1]);
        }

        Assert.True(failures.Count == 0,
            "an Overview read scans the archive, not its window:" + Environment.NewLine + string.Join(Environment.NewLine, failures)
            + Environment.NewLine + "all reads:" + Environment.NewLine + string.Join(Environment.NewLine, report));
    }
}
