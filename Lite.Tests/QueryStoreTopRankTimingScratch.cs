using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

public sealed class QueryStoreTopRankTimingScratch
{
    private static readonly string[] MinMax =
    {
        "duration_us", "cpu_time_us", "logical_io_reads", "logical_io_writes", "physical_io_reads", "clr_time_us", "rowcount",
        "log_bytes_used", "tempdb_space_used", "query_max_used_memory", "num_physical_io_reads",
    };

    [Fact]
    public async Task Timing()
    {
        var report = new StringBuilder();
        foreach (var archiveHeavy in new[] { true, false })
        {
            var dir = Path.Combine(Path.GetTempPath(), "q9195_" + Guid.NewGuid().ToString("N")[..6]);
            Directory.CreateDirectory(Path.Combine(dir, "archive"));
            var db = new DuckDbInitializer(Path.Combine(dir, "t.duckdb"));
            await db.InitializeFromTemplateAsync();
            var now = DateTime.UtcNow;
            now = new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);
            var minMaxNames = string.Join(", ", MinMax.SelectMany(c => new[] { "min_" + c, "max_" + c }));
            var minMaxVals = string.Join(", ", MinMax.SelectMany(c => new[] { "(i % 97) + 1", "(i % 97) * 3 + 5" }));
            using (var readLock = db.AcquireReadLock())
            using (var c = db.CreateConnection())
            {
                c.Open();
                void Exec(string sql) { using var cmd = c.CreateCommand(); cmd.CommandText = sql; cmd.ExecuteNonQuery(); }
                Exec("SET threads = 8");
                /* server 1 is the busiest: 600 queries x 168 hourly intervals x 13 snapshots = 1.31 M rows. Servers 2-6 hold 8000 keys each. */
                var servers = new (int Id, int Keys)[] { (1, 600 * 168), (2, 8000), (3, 8000), (4, 8000), (5, 8000), (6, 8000) };
                long idBase = 0;
                foreach (var (id, keys) in servers)
                {
                    var rows = (long)keys * 13;
                    Exec($@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, last_execution_time, query_hash, module_name, execution_count, avg_duration_us, avg_cpu_time_us,
    avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, min_dop, max_dop, query_plan_hash, is_forced_plan,
    plan_forcing_type, avg_clr_time_us, avg_tempdb_space_used, avg_log_bytes_used, plan_type, force_failure_count, last_force_failure_reason,
    compatibility_level, avg_query_max_used_memory, avg_num_physical_io_reads, runtime_stats_interval_id, interval_start_time_utc, replica_role, {minMaxNames})
SELECT {idBase} + i, istart + to_minutes(CAST(5 * (j + 1) AS INTEGER)), {id}, 'S{id}', 'Db' || (q % 3), q, q, 'Regular', istart, istart + to_minutes(CAST(5 * (j + 1) AS INTEGER)),
    '0xQ' || q, NULL, 5 * (j + 1) + (q % 7), 1000 + (i % 977), 700 + (i % 313), 50 + (i % 41), 20 + (i % 17), 10 + (i % 11), (i % 11),
    1 + (i % 3), 4 + (i % 3), '0xP' || q, (i % 2 = 0), 'PLAN', 5 + (i % 7), 3 + (i % 5), 8 + (i % 9), 'Compiled', i % 4, 'none',
    150 + (i % 4), 6 + (i % 13), 2 + (i % 5), iv, istart, NULL, {minMaxVals}
FROM (SELECT i, (i // 13) % 600 + 1 AS q, (i // 13) // 600 AS iv, i % 13 AS j,
             TIMESTAMP '{now:yyyy-MM-dd HH:mm:ss}' - to_hours(168) + to_hours(CAST((i // 13) // 600 AS INTEGER)) AS istart
      FROM range({rows}) t(i))");
                    idBase += rows;
                }
                if (archiveHeavy)
                {
                    var cut = now.AddHours(-2);
                    var span = (cut - now.AddHours(-168)).TotalHours / 8;
                    for (var f = 0; f < 8; f++)
                    {
                        var lo = now.AddHours(-168).AddHours(span * f);
                        var hi = f == 7 ? cut : now.AddHours(-168).AddHours(span * (f + 1));
                        var path = Path.Combine(dir, "archive", $"{lo:yyyyMMdd_HHmmss}_query_store_stats.parquet").Replace("\\", "/");
                        var pred = $"collection_time >= TIMESTAMP '{lo:yyyy-MM-dd HH:mm:ss}' AND collection_time < TIMESTAMP '{hi:yyyy-MM-dd HH:mm:ss}'";
                        Exec($"COPY (SELECT * FROM query_store_stats WHERE {pred}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)");
                        Exec($"DELETE FROM query_store_stats WHERE {pred}");
                    }
                }
            }
            await db.CreateArchiveViewsAsync();

            string ArgMaxHead()
            {
                var h = LocalDataService.QueryStoreTopRankHeadSql("", "");
                var a = h.IndexOf("max(", h.IndexOf("database_name, query_id, plan_id, runtime_stats_interval_id", StringComparison.Ordinal), StringComparison.Ordinal);
                var b = h.IndexOf("}) AS w", a, StringComparison.Ordinal);
                var body = h.Substring(a + 4, b - a - 4);
                return h.Substring(0, a) + "arg_max(" + body + "}, {'ct': collection_time, 'er': COALESCE(execution_count, -1), 'ci': collection_id}) AS w" + h.Substring(b + 7);
            }
            string Sql(int v) => (v == 0 ? QueryStoreTopRankOnePassTests.OldHead.Replace("{EXEC}", "") : v == 1 ? LocalDataService.QueryStoreTopRankHeadSql("", "") : ArgMaxHead())
                + LocalDataService.QueryStoreTopRankTailSql("", 1000);

            report.AppendLine($"=== {(archiveHeavy ? "archive-heavy (just after a reset)" : "hot-only (just before a reset)")}");
            using (var readLock = db.AcquireReadLock())
            using (var c = db.CreateConnection())
            {
                c.Open();
                using (var set = c.CreateCommand()) { set.CommandText = "SET threads = 8"; set.ExecuteNonQuery(); }
                using (var cnt = c.CreateCommand()) { cnt.CommandText = "SELECT (SELECT count(*) FROM query_store_stats WHERE server_id = 1), (SELECT count(*) FROM v_query_store_stats WHERE server_id = 1)"; using var r = cnt.ExecuteReader(); r.Read(); report.AppendLine($"server 1 rows hot={r.GetValue(0)} view={r.GetValue(1)}"); }
                List<string> Run(string sql, int cmdTimes, bool explain, out List<double> ms)
                {
                    ms = new List<double>();
                    var lines = new List<string>();
                    for (var n = 0; n < cmdTimes; n++)
                    {
                        using var cmd = c.CreateCommand();
                        cmd.CommandText = (explain ? "EXPLAIN ANALYZE " : "") + sql;
                        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
                        cmd.Parameters.Add(new DuckDBParameter { Value = now.AddHours(-168) });
                        cmd.Parameters.Add(new DuckDBParameter { Value = now });
                        var sw = Stopwatch.StartNew();
                        using var r = cmd.ExecuteReader();
                        var rows = 0;
                        while (r.Read()) { rows++; if (explain) lines.Add(r.GetValue(1)?.ToString() ?? ""); }
                        ms.Add(sw.Elapsed.TotalMilliseconds);
                        if (!explain && n == 0) lines.Add("rows=" + rows);
                    }
                    return lines;
                }
                foreach (var threads in new[] { 8, 2 })
                {
                    using (var set = c.CreateCommand()) { set.CommandText = "SET threads = " + threads; set.ExecuteNonQuery(); }
                    foreach (var v in new[] { 0, 1, 2, 0, 1, 2 })
                    {
                        var rowsLine = Run(Sql(v), 3, false, out var ms);
                        report.AppendLine($"T{threads} {new[] { "OLD", "NEW-max", "NEW-argmax" }[v]} {string.Join(" ", rowsLine)} ms: {string.Join(", ", ms.Select(m => m.ToString("0")))}");
                    }
                }
                using (var set = c.CreateCommand()) { set.CommandText = "SET threads = 8"; set.ExecuteNonQuery(); }
                foreach (var v in new[] { 0, 1 })
                {
                    var plan = Run(Sql(v), 1, true, out _);
                    report.AppendLine($"--- EXPLAIN ANALYZE {(v == 0 ? "OLD" : "NEW")}");
                    foreach (var line in plan.SelectMany(p => p.Split((char)10)))
                        if (line.Contains("HASH_") || line.Contains("SCAN") || line.Contains("JOIN") || line.Contains("Total Time") || line.Contains("rows") || line.Contains("~") || line.Contains("Table:") || line.Contains("Function"))
                            report.AppendLine(line.Trim());
                }
            }
            db.Dispose();
            try { Directory.Delete(dir, true); } catch { }
        }
        File.WriteAllText(Path.Combine(Path.GetTempPath(), "q9195_rank_timing.txt"), report.ToString());
    }
}
