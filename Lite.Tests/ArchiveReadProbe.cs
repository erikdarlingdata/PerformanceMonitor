using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>Timing probe over a field-shaped archived store (#9195). Runs only when PM_PROBE_DIR names a folder.</summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveReadProbe
{
    private readonly StringBuilder _out = new();

    private void Line(string s)
    {
        _out.AppendLine(s);
    }

    [Fact]
    public async Task Probe()
    {
        var dir = Environment.GetEnvironmentVariable("PM_PROBE_DIR");
        if (string.IsNullOrEmpty(dir))
        {
            return;
        }

        var ct = TestContext.Current.CancellationToken;
        Directory.CreateDirectory(dir);
        var dbPath = Path.Combine(dir, "test.duckdb");
        var archiveDir = Path.Combine(dir, "archive");
        var ready = Path.Combine(dir, "ready.txt");
        var outPath = Path.Combine(dir, "out.txt");
        var initializer = new DuckDbInitializer(dbPath);
        try
        {
            DateTime now;
            if (!File.Exists(ready))
            {
                foreach (var f in new[] { dbPath, dbPath + ".wal" })
                {
                    if (File.Exists(f)) File.Delete(f);
                }

                if (Directory.Exists(archiveDir)) Directory.Delete(archiveDir, recursive: true);
                Directory.CreateDirectory(archiveDir);
                await initializer.InitializeFromTemplateAsync();
                now = DateTime.UtcNow;
                var sw = Stopwatch.StartNew();
                long rows;
                using (var c = new DuckDBConnection($"Data Source={dbPath}"))
                {
                    await c.OpenAsync(ct);
                    rows = await QueryStoreArchiveFixture.SeedAsync(c, now, 14, [2400, 120], 20, 500, ct);
                }

                Line($"seeded {rows} rows in {sw.Elapsed.TotalSeconds:F1}s");
                var svc = new ArchiveService(initializer, archiveDir);
                foreach (var d in new[] { 11, 8, 5, 2, 1 })
                {
                    svc.TimestampForTests = now.AddDays(-d).ToString("yyyyMMdd_HHmm");
                    sw.Restart();
                    await svc.ArchiveOldDataAsync(hotDataDays: d);
                    Line($"archive pass d={d} took {sw.Elapsed.TotalSeconds:F1}s");
                }

                File.WriteAllText(ready, now.ToString("O"));
            }
            else
            {
                now = DateTime.Parse(File.ReadAllText(ready), null, System.Globalization.DateTimeStyles.RoundtripKind);
                await initializer.InitializeFromTemplateAsync();
            }

            var data = new LocalDataService(initializer);
            now = DateTime.UtcNow;
            using (var c = await data.OpenConnectionAsync())
            {
                var hot = await ScalarAsync(c, "SELECT count(*) FROM query_store_stats");
                var all = await ScalarAsync(c, "SELECT count(*) FROM v_query_store_stats");
                Line($"hot {hot} view {all} s1 {await ScalarAsync(c, "SELECT count(*) FROM v_query_store_stats WHERE server_id = 1")}");
            }

            foreach (var f in Directory.GetFiles(archiveDir, "*query_store_stats*.parquet"))
            {
                using var c = await data.OpenConnectionAsync();
                var rg = await ScalarAsync(c, $"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{f.Replace('\\', '/')}')");
                var n = await ScalarAsync(c, $"SELECT count(*) FROM read_parquet('{f.Replace('\\', '/')}')");
                Line($"file {Path.GetFileName(f)} {new FileInfo(f).Length / 1048576.0:F1}MB rows {n} rowgroups {rg}");
            }

            foreach (var hours in new[] { 24, 168 })
            {
                var times = new List<string>();
                for (var i = 0; i < 3; i++)
                {
                    var sw = Stopwatch.StartNew();
                    var rows = await data.GetQueryStoreTopQueriesAsync(1, hours, 50);
                    times.Add($"{sw.Elapsed.TotalSeconds:F2}s/{rows.Count}");
                }

                Line($"TOP {hours}h: {string.Join(" ", times)}");
            }

            foreach (var (name, sql) in new[] { ("REG before", QueryStoreArchiveReadReferences.RegressionCostSqlBefore), ("REG after", DuckDbFactCollector.PlanRegressionCostSql) })
            {
                var times = new List<string>();
                var n = 0;
                for (var i = 0; i < 3; i++)
                {
                    var sw = Stopwatch.StartNew();
                    n = await RunAsync(data, sql, now.AddHours(-4).AddDays(-14), now, 3);
                    times.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                }

                Line($"{name} 4h: {string.Join(" ", times)} rows {n}");
            }

            foreach (var hours in new[] { 24, 168 })
            {
                var start = now.AddHours(-hours);
                var top = await data.GetQueryStoreTopQueriesAsync(1, hours, 250);
                var keys = top.Select(r => (r.DatabaseName, r.QueryId)).ToList();
                Dictionary<(string, long), (long, DateTime)>? oldRows = null;
                Dictionary<(string DatabaseName, long QueryId), (long CollectionId, DateTime CollectionTime)>? newRows = null;
                var ot = new List<string>();
                var nt = new List<string>();
                for (var i = 0; i < 3; i++)
                {
                    var sw = Stopwatch.StartNew();
                    oldRows = await OldLatestAsync(data, keys);
                    ot.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                    sw.Restart();
                    using (var c = await data.OpenConnectionAsync())
                    {
                        newRows = await LocalDataService.ReadQueryStoreLatestRowsAsync(c, 1, keys, "v_query_store_stats", start.AddDays(-1));
                    }

                    nt.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                }

                var same = oldRows!.Count == newRows!.Count && oldRows.All(kv => newRows.TryGetValue(kv.Key, out var v) && v.CollectionId == kv.Value.Item1 && v.CollectionTime == kv.Value.Item2);
                Line($"LATEST {hours}h keys {keys.Count} before: {string.Join(" ", ot)} after: {string.Join(" ", nt)} same {same}");
                var tt = new List<string>();
                for (var i = 0; i < 3; i++)
                {
                    var sw = Stopwatch.StartNew();
                    using var c = await data.OpenConnectionAsync();
                    await LocalDataService.ReadQueryStoreTextByRowAsync(c, 1, newRows.Values);
                    tt.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                }

                Line($"TEXTBYROW {hours}h: {string.Join(" ", tt)}");
            }

            await ProfileAsync(data, "REG4", DuckDbFactCollector.PlanRegressionCostSql, now.AddHours(-4).AddDays(-14), now, 3, dir);
        }
        finally
        {
            initializer.Dispose();
            File.WriteAllText(outPath, _out.ToString());
        }
    }

    private static async Task<Dictionary<(string, long), (long, DateTime)>> OldLatestAsync(LocalDataService data, List<(string DatabaseName, long QueryId)> keys)
    {
        var result = new Dictionary<(string, long), (long, DateTime)>();
        using var c = await data.OpenConnectionAsync();
        for (var i = 0; i < keys.Count; i += 100)
        {
            var slice = keys.Skip(i).Take(100).ToList();
            using var cmd = c.CreateCommand();
            cmd.CommandText = QueryStoreArchiveReadReferences.LatestRowsSqlBefore(slice.Count);
            cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
            foreach (var (db, q) in slice)
            {
                cmd.Parameters.Add(new DuckDBParameter { Value = db });
                cmd.Parameters.Add(new DuckDBParameter { Value = q });
            }

            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                result[(r.GetString(0), r.GetInt64(1))] = (r.GetInt64(2), r.GetDateTime(3));
            }
        }

        return result;
    }

    private static async Task<long> ScalarAsync(LockedConnection c, string sql)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task<int> RunAsync(LocalDataService data, string sql, DateTime start, DateTime end, int paramCount)
    {
        using var c = await data.OpenConnectionAsync();
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = start });
        cmd.Parameters.Add(new DuckDBParameter { Value = end });
        var n = 0;
        using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync()) n++;
        return n;
    }

    private async Task ProfileAsync(LocalDataService data, string name, string sql, DateTime start, DateTime end, int paramCount, string dir)
    {
        var path = Path.Combine(dir, name + ".json").Replace('\\', '/');
        using (var c = await data.OpenConnectionAsync())
        {
            await ExecAsync(c, "PRAGMA enable_profiling = 'json'");
            await ExecAsync(c, $"PRAGMA profiling_output = '{path}'");
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
            cmd.Parameters.Add(new DuckDBParameter { Value = start });
            cmd.Parameters.Add(new DuckDBParameter { Value = end });
            using (var r = await cmd.ExecuteReaderAsync())
            {
                while (await r.ReadAsync()) { }
            }

            await ExecAsync(c, "PRAGMA disable_profiling");
        }

        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var nodes = new List<(string Type, double Time, long Card, string Extra)>();
        Walk(doc.RootElement, nodes);
        Line($"{name}: latency {doc.RootElement.GetProperty("latency").GetDouble():F2}s");
        foreach (var n in nodes.OrderByDescending(x => x.Time).Take(7))
        {
            Line($"  {n.Type} {n.Time:F2}s card {n.Card} {n.Extra}");
        }
    }

    private static void Walk(JsonElement e, List<(string, double, long, string)> nodes)
    {
        if (e.TryGetProperty("operator_type", out var t))
        {
            var extra = "";
            if (e.TryGetProperty("extra_info", out var x) && x.ValueKind == JsonValueKind.Object)
            {
                var parts = new List<string>();
                foreach (var p in x.EnumerateObject())
                {
                    if (p.Name is "Function" or "Total Files Read" or "Filters" or "Table" or "Projections")
                    {
                        var v = p.Value.ToString().Replace("\n", " ");
                        parts.Add(p.Name + "=" + (v.Length > 110 ? v[..110] : v));
                    }
                }

                extra = string.Join(" ", parts);
            }

            nodes.Add((t.GetString() ?? "", e.TryGetProperty("operator_timing", out var ot) ? ot.GetDouble() : 0,
                e.TryGetProperty("operator_cardinality", out var oc) ? oc.GetInt64() : 0, extra));
        }

        if (e.TryGetProperty("children", out var ch))
        {
            foreach (var c in ch.EnumerateArray()) Walk(c, nodes);
        }
    }

    private static async Task ExecAsync(LockedConnection c, string sql)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}
