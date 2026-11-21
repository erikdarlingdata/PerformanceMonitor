using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: temporary measurement class, removed before the final push. */
public sealed class Measure9223Tmp
{
    private const string Hash = "0xQS9223";
    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c) { CommandTimeout = 600 };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task Refresh(NpgsqlConnection c, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var r = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", c) { CommandTimeout = 600 };
        r.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        r.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await r.ExecuteNonQueryAsync(ct);
    }

    private const string Current = @"SELECT f.server_id, f.database_name, SUM(f.execution_count_sum)::double precision AS executions, SUM(f.cpu_us_weighted_sum) AS cpu_us, SUM(f.duration_us_weighted_sum) AS elapsed_us, MIN(f.bucket) AS first_hour, MAX(f.bucket) AS last_hour
FROM collect.query_store_stats_corrected_hourly AS f WHERE f.query_hash = $1 AND f.bucket >= $2 AND f.bucket < $3 GROUP BY f.server_id, f.database_name";
    private const string PerServer = @"SELECT f.server_id, f.database_name, SUM(f.execution_count_sum)::double precision AS executions, SUM(f.cpu_us_weighted_sum) AS cpu_us, SUM(f.duration_us_weighted_sum) AS elapsed_us, MIN(f.bucket) AS first_hour, MAX(f.bucket) AS last_hour
FROM servers AS s CROSS JOIN LATERAL (SELECT x.server_id, x.database_name, x.execution_count_sum, x.cpu_us_weighted_sum, x.duration_us_weighted_sum, x.bucket FROM collect.query_store_stats_corrected_hourly AS x WHERE x.server_id = s.server_id AND x.query_hash = $1 AND x.bucket >= $2 AND x.bucket < $3) AS f
GROUP BY f.server_id, f.database_name";

    private static async Task<string> Explain(NpgsqlConnection c, string sql, DateTime start, DateTime dStart, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, c) { CommandTimeout = 600 };
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, Hash);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(dStart, DateTimeKind.Unspecified));
        var json = (string)(await cmd.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement[0].GetProperty("Plan");
        var sb = new StringBuilder();
        Collect(root, sb);
        return $"pages={ExplainBuffers.DataPages(root.Clone())} exec_ms={doc.RootElement[0].GetProperty("Execution Time").GetDouble():F1} nodes={sb}";
    }

    private static void Collect(JsonElement n, StringBuilder sb)
    {
        var t = n.GetProperty("Node Type").GetString();
        if (t is "Seq Scan" or "Index Scan" or "Index Only Scan" or "Custom Scan" or "Bitmap Heap Scan")
        {
            sb.Append(t == "Custom Scan" ? n.GetProperty("Custom Plan Provider").GetString() : t + (n.TryGetProperty("Index Name", out var ix) ? "(" + ix.GetString() + ")" : "")).Append('|');
        }
        if (n.TryGetProperty("Plans", out var ch)) foreach (var x in ch.EnumerateArray()) Collect(x, sb);
    }

    [Fact]
    public async Task Measure()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "no pg");
        var servers = int.Parse(Environment.GetEnvironmentVariable("M9223_SERVERS") ?? "30");
        var groups = int.Parse(Environment.GetEnvironmentVariable("M9223_GROUPS") ?? "600");
        var end = DateTime.SpecifyKind(LiveClock.Now().Date.AddDays(-8).AddHours(12), DateTimeKind.Unspecified);
        var start = end.AddHours(-167);
        var dStart = start.Date.AddDays(1);
        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var _ = scratch;
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(c, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
        await TimescaleSupport.EnsureCollectionLogHypertableAsync(c, null, ct);
        await Exec(c, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(c, null, ct);
        await Exec(c, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        var outp = new StringBuilder();
        for (var i = 1; i <= servers; i++) await DarlingMcpTestData.RegisterServerAsync(c, -922300 - i, "p9223q-" + i, ct);
        await Exec(c, $@"INSERT INTO collect.query_store_stats
                  (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, replica_role,
                   runtime_stats_interval_id, interval_start_time_utc, first_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
                SELECT 900000000::bigint + row_number() OVER (), h.ts + interval '10 minutes', -922300 - s, 'p9223q-' || s, 'db' || (g % 10), 'dbo.P',
                  CASE WHEN g = 1 THEN '{Hash}' ELSE '0x' || g END, g, g, 'Regular', 'PRIMARY', g, h.ts, h.ts, 1 + (extract(hour FROM h.ts))::int % 5, 2000 + g, 1000 + g, 900, 400, h.ts + interval '1 hour'
                FROM generate_series(TIMESTAMP '{start.Date:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{end.AddHours(-1):yyyy-MM-dd HH:mm:ss}', interval '1 hour') AS h(ts)
                CROSS JOIN generate_series(1, {servers}) s CROSS JOIN generate_series(1, {groups}) g
                WHERE extract(hour FROM h.ts)::int % 2 = 0 OR g = 1", ct);
        await Refresh(c, TimescaleSupport.QueryStoreStatsIntervalHourlyView, start.Date.AddDays(-1), end, ct);
        await Refresh(c, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, start.Date.AddDays(-1), end, ct);
        await Refresh(c, TimescaleSupport.QueryStoreStatsCorrectedDailyView, start.Date, end.Date, ct);
        await Exec(c, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await Exec(c, "VACUUM (ANALYZE) collect.query_store_stats_corrected_hourly", ct);
        outp.AppendLine("M uncompressed current " + await Explain(c, Current, start, dStart, ct));
        outp.AppendLine("M uncompressed perserver " + await Explain(c, PerServer, start, dStart, ct));
        await Exec(c, TimescaleSupport.EnableAggregateCompressionSql(TimescaleSupport.QueryStoreStatsCorrectedHourlyView), ct);
        await Exec(c, "SELECT compress_chunk(ch, if_not_compressed => true) FROM show_chunks((SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) FROM timescaledb_information.continuous_aggregates WHERE view_name = 'query_store_stats_corrected_hourly')::regclass) ch", ct);
        await Exec(c, "VACUUM (ANALYZE) collect.query_store_stats_corrected_hourly", ct);
        await using (var q = new NpgsqlCommand("SELECT count(*), count(*) FILTER (WHERE is_compressed) FROM timescaledb_information.chunks WHERE hypertable_name LIKE '_materialized_hypertable_%'", c))
        await using (var r = await q.ExecuteReaderAsync(ct)) { await r.ReadAsync(ct); outp.AppendLine($"M chunks={r.GetInt64(0)} compressed={r.GetInt64(1)} servers={servers} groups={groups}"); }
        await using (var q = new NpgsqlCommand(@"SELECT string_agg(format('%s heap=%s toast=%s idx=%s batches=%s', t.relname, pg_relation_size(t.oid)/8192, coalesce(pg_relation_size(t.reltoastrelid)/8192, 0), pg_indexes_size(t.oid)/8192, t.reltuples::bigint), ' ; ')
                FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = '_timescaledb_internal' AND t.relname LIKE '%hyper%chunk' AND t.relkind = 'r' AND t.reltuples > 0", c))
        { outp.AppendLine("M compressed " + await q.ExecuteScalarAsync(ct)); }
        await using (var q = new NpgsqlCommand(@"SELECT string_agg(substr(indexdef, 1, 220), ' ; ') FROM pg_indexes WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '%hyper%chunk'", c))
        { outp.AppendLine("M compidx " + await q.ExecuteScalarAsync(ct)); }
        await Exec(c, "SET enable_seqscan = off", ct);
        outp.AppendLine("M noseq current " + await Explain(c, Current, start, dStart, ct));
        outp.AppendLine("M noseq perserver " + await Explain(c, PerServer, start, dStart, ct));
        await using (var tp = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) " + PerServer, c) { CommandTimeout = 600 })
        {
            tp.Parameters.AddWithValue(NpgsqlDbType.Text, Hash);
            tp.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
            tp.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(dStart, DateTimeKind.Unspecified));
            await using var rd = await tp.ExecuteReaderAsync(ct);
            while (await rd.ReadAsync(ct))
            {
                var line = rd.GetString(0);
                if (line.Contains("Scan") || line.Contains("Cond") || line.Contains("Buffers") || line.Contains("Batches") || line.Contains("Filter") || line.Contains("Removed"))
                    outp.AppendLine("M plan " + line.Trim());
            }
        }
        await Exec(c, "RESET enable_seqscan", ct);
        for (var round = 1; round <= 2; round++)
        {
            outp.AppendLine($"M current r{round} " + await Explain(c, Current, start, dStart, ct));
            outp.AppendLine($"M perserver r{round} " + await Explain(c, PerServer, start, dStart, ct));
        }
        File.WriteAllText("C:/runner/logs/m9223.txt", outp.ToString());
    }
}
