using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

public sealed class AnomalyDetectorReadTimingProbe : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly List<string> _lines = new();
    private long _base = 1;

    public void Dispose()
    {
        File.WriteAllLines(Path.Combine(Path.GetTempPath(), "anomaly-probe2.txt"), _lines);
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    private void Exec(DuckDBConnection c, string sql, params object[] ps)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in ps) cmd.Parameters.Add(new DuckDBParameter { Value = p });
        var sw = Stopwatch.StartNew();
        cmd.ExecuteNonQuery();
        _lines.Add($"seed {sw.ElapsedMilliseconds} ms: {sql.Trim().Split('\n')[0]}");
    }

    private long Next() { var b = _base; _base += 100_000_000; return b; }

    private async Task SeedAsync(string dbPath, string archiveDir, int days, int hotDays, int sliceHours)
    {
        using var c = new DuckDBConnection($"Data Source={dbPath}");
        await c.OpenAsync();
        Directory.CreateDirectory(archiveDir);
        var end = DateTime.UtcNow;
        var hotStart = end.AddDays(-hotDays);
        var (cpu, wait, perf, sess, qs, io, mem, log1, log2) = (
            "SELECT {B} + row_number() OVER () AS collection_id, t AS collection_time, sid AS server_id, 'S' AS server_name, t AS sample_time, (20 + 30*random())::INT AS sqlserver_cpu_utilization, 2 AS other_process_cpu_utilization FROM {G}",
            "SELECT {B} + row_number() OVER () AS collection_id, t AS collection_time, sid AS server_id, 'S' AS server_name, 'WAIT_' || w AS wait_type, 0 AS waiting_tasks_count, 0 AS wait_time_ms, 0 AS signal_wait_time_ms, 0 AS delta_waiting_tasks, (random()*1000)::BIGINT AS delta_wait_time_ms, 0 AS delta_signal_wait_time_ms, 60 AS sample_interval_seconds FROM {G} CROSS JOIN range(30) r(w)",
            "SELECT {B} + row_number() OVER () AS collection_id, t AS collection_time, sid AS server_id, 'S' AS server_name, 'SQLServer:SQL Statistics' AS object_name, CASE WHEN w = 0 THEN 'Batch Requests/sec' ELSE 'Counter ' || w END AS counter_name, '' AS instance_name, 1 AS cntr_value, (random()*600)::BIGINT AS delta_cntr_value, 60 AS sample_interval_seconds FROM {G} CROSS JOIN range(40) r(w)",
            "SELECT {B} + row_number() OVER () AS collection_id, t AS collection_time, sid AS server_id, 'S' AS server_name, 'App ' || w AS program_name, (random()*20)::BIGINT AS connection_count, 0 AS running_count, 0 AS sleeping_count, 0 AS dormant_count FROM {G} CROSS JOIN range(10) r(w)",
            "SELECT {B} + row_number() OVER () AS collection_id, t AS collection_time, sid AS server_id, 'S' AS server_name, 10 AS execution_count, 100 AS total_elapsed_time, 0 AS total_worker_time, 0 AS total_logical_reads, 0 AS total_logical_writes, 0 AS total_physical_reads, 1 + (random()*5)::INT AS delta_execution_count, (random()*100000)::BIGINT AS delta_elapsed_time, 0 AS delta_worker_time, 0 AS delta_logical_reads, 0 AS delta_logical_writes, 0 AS delta_physical_reads, 0 AS delta_rows, 0 AS delta_spills FROM {G} CROSS JOIN range(100) r(w)",
            "SELECT {B} + row_number() OVER () AS collection_id, t AS collection_time, sid AS server_id, 'S' AS server_name, 'Db' || w AS database_name, 'f' || w AS file_name, 'ROWS' AS file_type, 'Dx' || w AS physical_name, 100 AS size_mb, 100 + (random()*100)::BIGINT AS delta_reads, 50 AS delta_writes, 81920 AS delta_read_bytes, 1000 AS delta_write_bytes, (random()*1000)::BIGINT AS delta_stall_read_ms, (random()*500)::BIGINT AS delta_stall_write_ms, 60 AS sample_interval_seconds FROM {G} CROSS JOIN range(10) r(w)",
            "SELECT {B} + row_number() OVER () AS collection_id, t AS collection_time, sid AS server_id, 'S' AS server_name, 65536 AS total_physical_memory_mb, 10000 AS available_physical_memory_mb, 50000 AS target_server_memory_mb, 40000 + (random()*5000) AS total_server_memory_mb, 30000 AS buffer_pool_mb FROM {G}",
            "SELECT {B} + row_number() OVER () AS log_id, sid AS server_id, 'S' AS server_name, 'collector_' || w AS collector_name, t AS collection_time, 'SUCCESS' AS status, 1 AS rows_collected FROM {G} CROSS JOIN range(20) r(w)",
            "SELECT {B} + row_number() OVER () AS log_id, sid AS server_id, 'S' AS server_name, 'session_stats' AS collector_name, t AS collection_time, 'SUCCESS' AS status, 1 AS rows_collected FROM {G}");
        var tables = new (string Table, string Sel)[] {
            ("cpu_utilization_stats", cpu), ("wait_stats", wait), ("perfmon_stats", perf), ("session_stats", sess), ("query_stats", qs),
            ("file_io_stats", io), ("memory_stats", mem), ("collection_log", log1), ("collection_log", log2) };
        var n = 0;
        foreach (var (table, sel) in tables)
        {
            var sw = Stopwatch.StartNew();
            string Build(DateTime a, DateTime b) => sel.Replace("{B}", Next().ToString()).Replace("{G}", "generate_series('" + a.ToString("yyyy-MM-dd HH:mm:ss") + "'::TIMESTAMP, '" + b.AddMinutes(-1).ToString("yyyy-MM-dd HH:mm:ss") + "'::TIMESTAMP, INTERVAL 1 MINUTE) AS g(t) CROSS JOIN (VALUES (1),(2)) s(sid)");
            Exec(c, $"INSERT INTO {table} BY NAME " + Build(hotStart, end));
            if (sliceHours > 0)
            {
                for (var a = end.AddDays(-days); a < hotStart; a = a.AddHours(sliceHours))
                {
                    var b = a.AddHours(sliceHours) < hotStart ? a.AddHours(sliceHours) : hotStart;
                    var file = Path.Combine(archiveDir, $"{a:yyyyMMdd_HHmm}_{table}.parquet").Replace("\\", "/");
                    if (File.Exists(file)) file = file.Replace(".parquet", "_" + (n++) + ".parquet").Replace("_" + table + "_", "_" + table + "_");
                    Exec(c, $"COPY ({Build(a, b)}) TO '{file}' (FORMAT PARQUET)");
                }
            }
        }
        Exec(c, "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition) VALUES (999999999, $1, 1, 'S', 'E', '16.0', 'RTM', 3), (999999998, $1, 2, 'S', 'E', '16.0', 'RTM', 3)", end.AddDays(-1));
        Exec(c, "CHECKPOINT");
    }

    private async Task TimeAsync(DuckDbInitializer init, string tag)
    {
        var end = DateTime.UtcNow;
        foreach (var sid in new[] { 1, 2 })
        {
            var bp0 = new BaselineProvider(init);
            var bl = new System.Text.StringBuilder();
            foreach (var m in new[] { MetricNames.Cpu, MetricNames.WaitMsPerSec, MetricNames.Blocking, MetricNames.Deadlock, MetricNames.BlockingSnapshot, MetricNames.IoLatency, MetricNames.IoWriteLatency, MetricNames.BatchRequests, MetricNames.SessionCount, MetricNames.QueryDuration, MetricNames.Memory })
            {
                var sw0 = Stopwatch.StartNew();
                await bp0.GetBaselineAsync(sid, m, end.AddHours(-4));
                bl.Append($" {m}={sw0.ElapsedMilliseconds}");
            }
            _lines.Add($"{tag} server {sid} cold baselines ms:{bl}");
        }
        var bp = new BaselineProvider(init);
        var det = new AnomalyDetector(init, bp);
        for (var i = 0; i < 3; i++)
        {
            foreach (var sid in new[] { 1, 2 })
            {
                var ctx = new AnalysisContext { ServerId = sid, ServerName = "S", TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end };
                var sw = Stopwatch.StartNew();
                var f = await det.DetectAnomaliesAsync(ctx);
                _lines.Add($"{tag} detector call {i} server {sid}: {sw.ElapsedMilliseconds} ms, {f.Count} facts");
            }
        }
    }

    [Fact]
    public async Task Probe()
    {
        Directory.CreateDirectory(_tempDir);
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        var arch = Path.Combine(_tempDir, "archive");
        using var init = new DuckDbInitializer(dbPath);
        await init.InitializeFromTemplateAsync();
        await SeedAsync(dbPath, arch, 31, 7, 6);
        await init.CreateArchiveViewsAsync();
        _lines.Add("files: " + Directory.GetFiles(arch).Length);
        await TimeAsync(init, "archived");
    }
}
