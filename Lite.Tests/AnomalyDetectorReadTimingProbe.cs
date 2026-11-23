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
        File.WriteAllLines(Path.Combine(Path.GetTempPath(), "anomaly-probe.txt"), _lines);
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

    private async Task SeedAsync(string dbPath, int days)
    {
        using var c = new DuckDBConnection($"Data Source={dbPath}");
        await c.OpenAsync();
        var end = DateTime.UtcNow;
        var start = end.AddDays(-days);
        var g = "generate_series($1::TIMESTAMP, $2::TIMESTAMP, INTERVAL 1 MINUTE) AS g(t) CROSS JOIN (VALUES (1),(2)) s(sid)";
        Exec(c, $@"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT {Next()} + row_number() OVER (), t, sid, 'S', t, (20 + 30*random())::INT, 2 FROM {g}", start, end);
        Exec(c, $@"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
SELECT {Next()} + row_number() OVER (), t, sid, 'S', 'WAIT_' || w, 0, 0, 0, 0, (random()*1000)::BIGINT, 0, 60 FROM {g} CROSS JOIN range(30) r(w)", start, end);
        Exec(c, $@"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
SELECT {Next()} + row_number() OVER (), t, sid, 'S', 'SQLServer:SQL Statistics', CASE WHEN w = 0 THEN 'Batch Requests/sec' ELSE 'Counter ' || w END, '', 1, (random()*600)::BIGINT, 60 FROM {g} CROSS JOIN range(40) r(w)", start, end);
        Exec(c, $@"INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count)
SELECT {Next()} + row_number() OVER (), t, sid, 'S', 'App ' || w, (random()*20)::BIGINT, 0, 0, 0 FROM {g} CROSS JOIN range(10) r(w)", start, end);
        Exec(c, $@"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, execution_count, total_elapsed_time, total_worker_time, total_logical_reads, total_logical_writes, total_physical_reads, delta_execution_count, delta_elapsed_time, delta_worker_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills)
SELECT {Next()} + row_number() OVER (), t, sid, 'S', 10, 100, 0, 0, 0, 0, 1 + (random()*5)::INT, (random()*100000)::BIGINT, 0, 0, 0, 0, 0, 0 FROM {g} CROSS JOIN range(100) r(w)", start, end);
        Exec(c, $@"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
SELECT {Next()} + row_number() OVER (), t, sid, 'S', 'Db' || w, 'f' || w, 'ROWS', 'D:\x' || w, 100, 100 + (random()*100)::BIGINT, 50, 81920, 1000, (random()*1000)::BIGINT, (random()*500)::BIGINT, 60 FROM {g} CROSS JOIN range(10) r(w)", start, end);
        Exec(c, $@"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb, target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
SELECT {Next()} + row_number() OVER (), t, sid, 'S', 65536, 10000, 50000, 40000 + (random()*5000), 30000 FROM {g}", start, end);
        Exec(c, $@"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
SELECT {Next()} + row_number() OVER (), sid, 'S', 'collector_' || w, t, 'SUCCESS', 1 FROM {g} CROSS JOIN range(20) r(w)", start, end);
        Exec(c, $@"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
SELECT {Next()} + row_number() OVER (), sid, 'S', 'session_stats', t, 'SUCCESS', 1 FROM {g}", start, end);
        Exec(c, "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition) VALUES (999999999, $1, 1, 'S', 'E', '16.0', 'RTM', 3), (999999998, $1, 2, 'S', 'E', '16.0', 'RTM', 3)", end.AddDays(-1));
        Exec(c, "CHECKPOINT");
    }

    private async Task TimeAsync(DuckDbInitializer init, string tag)
    {
        var end = DateTime.UtcNow;
        foreach (var sid in new[] { 1, 2 })
        {
            var bp0 = new BaselineProvider(init);
            foreach (var m in new[] { MetricNames.Cpu, MetricNames.WaitMsPerSec, MetricNames.Blocking, MetricNames.Deadlock, MetricNames.BlockingSnapshot, MetricNames.IoLatency, MetricNames.IoWriteLatency, MetricNames.BatchRequests, MetricNames.SessionCount, MetricNames.QueryDuration, MetricNames.Memory })
            {
                var sw0 = Stopwatch.StartNew();
                await bp0.GetBaselineAsync(sid, m, end.AddHours(-4));
                _lines.Add($"{tag} server {sid} baseline {m} cold {sw0.ElapsedMilliseconds} ms");
            }
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
        using var init = new DuckDbInitializer(dbPath);
        await init.InitializeFromTemplateAsync();
        await SeedAsync(dbPath, 31);
        await init.CreateArchiveViewsAsync();
        await TimeAsync(init, "hot");
        var archive = new ArchiveService(init, Path.Combine(_tempDir, "archive"));
        var sw = Stopwatch.StartNew();
        await archive.ArchiveOldDataAsync(hotDataDays: 7);
        _lines.Add($"archive took {sw.ElapsedMilliseconds} ms");
        await init.CreateArchiveViewsAsync();
        await TimeAsync(init, "archived");
    }
}
