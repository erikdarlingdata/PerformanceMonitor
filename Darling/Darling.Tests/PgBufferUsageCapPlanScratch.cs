using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: scratch database through ScratchPostgres. */
[Collection("pg-cluster-roles")]
public sealed class PgBufferUsageCapPlanScratch
{
    [Fact]
    public async Task PrintPlans()
    {
        var admin = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(admin), "no rig");
        var ct = TestContext.Current.CancellationToken;
        await using var target = await ScratchPostgres.CreateAsync(admin!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(target.ConnectionString);
        await connection.OpenAsync(ct);
        await Exec(connection, "CREATE EXTENSION pg_buffercache; CREATE TABLE pm9508_t AS SELECT g FROM generate_series(1,50000) g; SELECT count(*) FROM pm9508_t;", ct);
        var ver = await Scalar(connection, "SELECT extversion FROM pg_extension WHERE extname='pg_buffercache'", ct);
        Console.WriteLine("GATE-PLAN extension version " + ver + ", pool " + await Scalar(connection, "SHOW shared_buffers", ct));
        foreach (var (label, cap, summary) in new (string, long?, bool)[] { ("OVER cap, summary", 100, true), ("UNDER cap, summary", null, true), ("OVER cap, no summary", 100, false), ("UNDER cap, no summary", null, false) })
        {
            var context = new CollectorContext
            {
                ServerId = 1, ServerName = "x", CollectionTime = DateTime.UtcNow, Deltas = new CollectorDeltaCalculator(),
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = connection.PostgreSqlVersion.Major },
                PgExtensionSchemas = new Dictionary<string, string>(StringComparer.Ordinal) { ["pg_buffercache"] = "public" },
                PgExtensionMemberFunctions = summary ? new HashSet<string>(StringComparer.Ordinal) { "pg_buffercache_summary" } : new HashSet<string>(StringComparer.Ordinal),
                PgBufferUsagePoolCapBuffers = cap,
            };
            var sql = PgBufferUsageCollector.Instance.BuildQuery(context).Text;
            var lines = new List<string>();
            await using (var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + sql, connection))
            await using (var r = await cmd.ExecuteReaderAsync(ct))
            {
                while (await r.ReadAsync(ct)) { lines.Add(r.GetString(0)); }
            }
            Console.WriteLine("GATE-PLAN == " + label);
            foreach (var l in lines.Where(l => l.Contains("Function Scan") || l.Contains("never executed") || l.Contains("One-Time Filter") || l.Contains("Append") || l.Contains("pg_buffercache") || l.Contains("Result")))
            {
                Console.WriteLine("GATE-PLAN " + l.Trim());
            }
            var rows = new List<string>();
            await using (var cmd = new NpgsqlCommand("SELECT coalesce(relation_name,'<null>') || ' b=' || coalesce(buffers::text,'NULL') || ' tot=' || coalesce(pool_buffers_total::text,'NULL') || ' used=' || coalesce(pool_buffers_used::text,'NULL') FROM (" + sql + ") q", connection))
            await using (var r = await cmd.ExecuteReaderAsync(ct))
            {
                while (await r.ReadAsync(ct)) { rows.Add(r.GetString(0)); }
            }
            Console.WriteLine("GATE-PLAN rows: " + string.Join(" | ", rows.Take(4)) + " (" + rows.Count + ")");
        }
    }

    private static async Task Exec(NpgsqlConnection c, string sql, System.Threading.CancellationToken ct) { await using var cmd = new NpgsqlCommand(sql, c); await cmd.ExecuteNonQueryAsync(ct); }
    private static async Task<string> Scalar(NpgsqlConnection c, string sql, System.Threading.CancellationToken ct) { await using var cmd = new NpgsqlCommand(sql, c); return (await cmd.ExecuteScalarAsync(ct))!.ToString()!; }
}
