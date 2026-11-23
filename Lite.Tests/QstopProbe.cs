using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>Timing probe (#9195 qstop). Runs only when PM_PROBE_DIR names a folder.</summary>
[Collection("CollectionResetGate")]
public sealed class QstopProbe
{
    private static int Scale => int.Parse(Environment.GetEnvironmentVariable("PM_PROBE_SCALE") ?? "1");
    private readonly StringBuilder _out = new();
    private void Line(string s) => _out.AppendLine(s);

    [Fact]
    public async Task Probe()
    {
        var dir = Environment.GetEnvironmentVariable("PM_PROBE_DIR");
        if (string.IsNullOrEmpty(dir)) return;
        var ct = TestContext.Current.CancellationToken;
        Directory.CreateDirectory(dir);
        var dbPath = Path.Combine(dir, "test.duckdb");
        var archiveDir = Path.Combine(dir, "archive");
        var ready = Path.Combine(dir, "ready.txt");
        var outPath = Path.Combine(dir, "out-" + (Environment.GetEnvironmentVariable("PM_PROBE_TAG") ?? "x") + ".txt");
        var initializer = new DuckDbInitializer(dbPath);
        try
        {
            DateTime now;
            if (!File.Exists(ready))
            {
                foreach (var f in new[] { dbPath, dbPath + ".wal" }) if (File.Exists(f)) File.Delete(f);
                if (Directory.Exists(archiveDir)) Directory.Delete(archiveDir, recursive: true);
                Directory.CreateDirectory(archiveDir);
                await initializer.InitializeFromTemplateAsync();
                now = DateTime.UtcNow;
                var sw = Stopwatch.StartNew();
                long rows;
                using (var c = new DuckDBConnection($"Data Source={dbPath}"))
                {
                    await c.OpenAsync(ct);
                    rows = await QueryStoreArchiveFixture.SeedAsync(c, now, 14, [Scale * 2400, Scale * 120], 20, 500, ct);
                }
                Line($"seeded {rows} rows in {sw.Elapsed.TotalSeconds:F1}s");
                var svc = new ArchiveService(initializer, archiveDir);
                foreach (var d in new[] { 13, 11, 9, 8, 7 })
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
                await initializer.InitializeFromTemplateAsync();
            }

            var data = new LocalDataService(initializer);
            now = DateTime.UtcNow;
            using (var c = await data.OpenConnectionAsync())
            {
                Line($"hot {await ScalarAsync(c, "SELECT count(*) FROM query_store_stats")} view {await ScalarAsync(c, "SELECT count(*) FROM v_query_store_stats")}");
            }

            foreach (var hours in new[] { 24, 168 })
            {
                var times = new List<string>();
                var n = 0;
                for (var i = 0; i < 3; i++)
                {
                    var sw = Stopwatch.StartNew();
                    var rows = await data.GetQueryStoreTopQueriesAsync(1, hours, 21);
                    n = rows.Count;
                    times.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                }
                Line($"TOP {hours}h: {string.Join(" ", times)} rows {n}");

                var ft = new List<string>();
                DateTime? floor = null;
                for (var i = 0; i < 3; i++)
                {
                    var sw = Stopwatch.StartNew();
                    floor = await data.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStoreStats, 1, now.AddHours(-hours), now);
                    ft.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                }
                Line($"FLOOR {hours}h: {string.Join(" ", ft)} -> {floor:O} start {now.AddHours(-hours):O}");
                ft.Clear();
                for (var i = 0; i < 3; i++)
                {
                    var sw = Stopwatch.StartNew();
                    floor = await data.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStoreStats, 1, now.AddDays(-30).AddHours(-hours), now.AddDays(-30));
                    ft.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                }
                Line($"FLOOR-BEFORE-HISTORY {hours}h: {string.Join(" ", ft)} -> {floor:O}");
                ft.Clear();
                for (var i = 0; i < 3; i++)
                {
                    var sw = Stopwatch.StartNew();
                    floor = await data.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStoreStats, 1, now.AddDays(-8), now);
                    ft.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                }
                Line($"FLOOR-8D {hours}h: {string.Join(" ", ft)} -> {floor:O}");
                var top = await data.GetQueryStoreTopQueriesAsync(1, hours, 250);
                var keys = top.Select(r => (r.DatabaseName, r.QueryId)).ToList();
                var lt = new List<string>(); var tt = new List<string>();
                Dictionary<(string DatabaseName, long QueryId), (long CollectionId, DateTime CollectionTime)>? latest = null;
                for (var i = 0; i < 3; i++)
                {
                    var sw = Stopwatch.StartNew();
                    using (var c = await data.OpenConnectionAsync())
                        latest = await LocalDataService.ReadQueryStoreLatestRowsAsync(c, 1, keys, "v_query_store_stats", now.AddHours(-hours).AddDays(-1));
                    lt.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                    sw.Restart();
                    using (var c = await data.OpenConnectionAsync())
                        await LocalDataService.ReadQueryStoreTextByRowAsync(c, 1, latest.Values);
                    tt.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                }
                Line($"LATEST {hours}h keys {keys.Count}: {string.Join(" ", lt)}  TEXT: {string.Join(" ", tt)}");
            }

            var vdir = Path.Combine(dir, "variants");
            if (Directory.Exists(vdir))
            {
                File.WriteAllText(Path.Combine(vdir, "R0.sql"), RankSql(FindSource()));
                File.WriteAllText(Path.Combine(vdir, "P1.sql"), "--PRAGMA SET preserve_insertion_order = false" + (char)10 + RankSql(FindSource()));
                File.WriteAllText(Path.Combine(vdir, "P2.sql"), "--PRAGMA SET threads = 2" + (char)10 + RankSql(FindSource()));
                File.WriteAllText(Path.Combine(vdir, "P3.sql"), "--PRAGMA SET preserve_insertion_order = false; SET threads = 2" + (char)10 + RankSql(FindSource()));
                foreach (var f in Directory.GetFiles(vdir, "*.sql").OrderBy(x => x))
                {
                    var sql = File.ReadAllText(f);
                    var vt = new List<string>(); long rc = 0;
                    try {
                    for (var i = 0; i < 4; i++)
                    {
                        var sw = Stopwatch.StartNew();
                        using var c = await data.OpenConnectionAsync();
                        if (sql.StartsWith("--PRAGMA ")) { await ExecAsync(c, sql.Substring(9, sql.IndexOf((char)10) - 9)); }
                        using var cmd = c.CreateCommand();
                        cmd.CommandText = sql;
                        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
                        cmd.Parameters.Add(new DuckDBParameter { Value = now.AddHours(-168) });
                        cmd.Parameters.Add(new DuckDBParameter { Value = now });
                        var h = 17L; rc = 0;
                        using var r = await cmd.ExecuteReaderAsync();
                        while (await r.ReadAsync()) { rc++; for (var k = 0; k < r.FieldCount; k++) h = h * 31 + (r.IsDBNull(k) ? 7 : StableHash(r.GetValue(k).ToString()!)); }
                        vt.Add($"{sw.Elapsed.TotalSeconds:F2}s");
                        if (i == 0) Line($"VARIANT {Path.GetFileName(f)} rows {rc} hash {h}");
                    }
                    } catch (Exception ex) { Line($"VARIANT {Path.GetFileName(f)} FAILED {ex.Message.Replace((char)10, (char)32)}"); continue; }
                    Line($"VARIANT {Path.GetFileName(f)}: {string.Join(" ", vt)}");
                }
                return;
            }

            var src = FindSource();
            foreach (var hours in new[] { 24, 168 })
            {
                await ProfileAsync(data, $"RANK{hours}", RankSql(src), now.AddHours(-hours), now, dir);
                await ProfileAsync(data, $"FLOORMIN{hours}", "SELECT LEAST((SELECT MIN(collection_time) FROM query_store_stats WHERE server_id = $1 AND collection_time > $2 AND collection_time <= $3), (SELECT MIN(collection_time) FROM collection_log WHERE server_id = $1 AND collector_name = 'query_store_stats' AND collection_time > $2 AND collection_time <= $3))", now.AddHours(-hours), now, dir);
                await ProfileAsync(data, $"FLOOROLD{hours}", "SELECT 1 FROM v_query_store_stats WHERE server_id = $1 AND collection_time <= $2 LIMIT 1", now.AddHours(-hours), null, dir);
            }
        }
        finally
        {
            initializer.Dispose();
            File.WriteAllText(outPath, _out.ToString());
        }
    }

    private static long StableHash(string t) { long h = 7; foreach (var ch in t) h = h * 131 + ch; return h; }

    private static string FindSource()
    {
        var d = AppContext.BaseDirectory;
        while (d is not null && !File.Exists(Path.Combine(d, "Lite", "Services", "LocalDataService.QueryStore.cs"))) d = Path.GetDirectoryName(d);
        return File.ReadAllText(Path.Combine(d!, "Lite", "Services", "LocalDataService.QueryStore.cs"));
    }

    private static string RankSql(string src)
    {
        var s = src.IndexOf("WITH latest AS (", StringComparison.Ordinal);
        var e = src.IndexOf("ORDER BY p.page_ord\"", s, StringComparison.Ordinal) + "ORDER BY p.page_ord".Length;
        var sql = src.Substring(s, e - s);
        return sql.Replace("\" + dbClause + executionTypeClause + @\"", "").Replace("\" + moduleClause + @\"", "").Replace("\" + candidates + @\"", "251");
    }

    private static async Task<long> ScalarAsync(LockedConnection c, string sql)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private async Task ProfileAsync(LocalDataService data, string name, string sql, DateTime start, DateTime? end, string dir)
    {
        var path = Path.Combine(dir, name + ".json").Replace('\\', '/');
        double total;
        using (var c = await data.OpenConnectionAsync())
        {
            await ExecAsync(c, "PRAGMA enable_profiling = 'json'");
            await ExecAsync(c, $"PRAGMA profiling_output = '{path}'");
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
            cmd.Parameters.Add(new DuckDBParameter { Value = start });
            if (end is not null)
            {
                cmd.Parameters.Add(new DuckDBParameter { Value = end });
            }
            var sw = Stopwatch.StartNew();
            using (var r = await cmd.ExecuteReaderAsync()) { while (await r.ReadAsync()) { } }
            total = sw.Elapsed.TotalSeconds;
            await ExecAsync(c, "PRAGMA disable_profiling");
        }
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var nodes = new List<(string Type, double Time, long Card, string Extra)>();
        Walk(doc.RootElement, nodes);
        Line($"{name}: wall {total:F2}s latency {doc.RootElement.GetProperty("latency").GetDouble():F2}s");
        foreach (var n in nodes.OrderByDescending(x => x.Time).Take(8))
            Line($"  {n.Type} {n.Time:F2}s card {n.Card} {n.Extra}");
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
                    if (p.Name is "Function" or "Total Files Read" or "Filters" or "Table" or "Projections")
                    {
                        var v = p.Value.ToString().Replace("\n", " ");
                        parts.Add(p.Name + "=" + (v.Length > 110 ? v[..110] : v));
                    }
                extra = string.Join(" ", parts);
            }
            nodes.Add((t.GetString() ?? "", e.TryGetProperty("operator_timing", out var ot) ? ot.GetDouble() : 0,
                e.TryGetProperty("operator_cardinality", out var oc) ? oc.GetInt64() : 0, extra));
        }
        if (e.TryGetProperty("children", out var ch)) foreach (var c in ch.EnumerateArray()) Walk(c, nodes);
    }

    private static async Task ExecAsync(LockedConnection c, string sql)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}
