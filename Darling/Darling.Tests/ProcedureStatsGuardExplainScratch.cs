using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>Scratch (#9357 S3, not kept): EXPLAIN (ANALYZE, BUFFERS) of the procedure_stats count guard on a seeded store.</summary>
public sealed class ProcedureStatsGuardExplainScratch
{
    [Fact]
    public async Task ExplainTheGuard()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")) || Environment.GetEnvironmentVariable("PM_EXPLAIN_9357") != "1", "scratch");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await ProcedureStatsHourLedgerLiveTests.Rig.OpenAsync(ct, timescale: true);
        var c = rig.Connection;
        await DarlingMcpTestData.RegisterServerAsync(c, -945403, "ProcLedgerC", ct);
        await DarlingMcpTestData.RegisterServerAsync(c, -945404, "ProcLedgerD", ct);
        var report = new StringBuilder();

        var end = new DateTime(2026, 1, 5, 12, 0, 0, DateTimeKind.Unspecified);
        var start = end.AddDays(-5);

        await ExecAsync(c, $@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT 1000000000 + row_number() OVER (), t, s.id, s.name, 'Db' || d, 'dbo', 'usp_' || p, '0x' || p, 100, 100, 1, 300
FROM generate_series('{start:yyyy-MM-dd HH:mm:ss}'::timestamp, '{end:yyyy-MM-dd HH:mm:ss}'::timestamp, interval '5 minutes') AS t,
     (VALUES (-945401, 'ProcLedgerA'), (-945402, 'ProcLedgerB'), (-945403, 'ProcLedgerC'), (-945404, 'ProcLedgerD')) AS s(id, name),
     generate_series(1, 3) AS d, generate_series(1, 40) AS p", ct);
        report.AppendLine("raw rows: " + await ScalarAsync(c, "SELECT count(*) FROM collect.procedure_stats", ct));

        await ExecAsync(c, @"
INSERT INTO collect.procedure_stats_hour_ledger (server_id, server_name, bucket, n)
SELECT server_id, server_name, date_trunc('hour', collection_time), count(*) FILTER (WHERE sample_interval_seconds IS DISTINCT FROM 0)
FROM collect.procedure_stats GROUP BY 1, 2, 3", ct);
        await ExecAsync(c, "UPDATE collect.procedure_stats_hour_ledger_state SET counted_since = '2000-01-01'", ct);

        await using (var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", c))
        {
            refresh.CommandTimeout = 600;
            refresh.Parameters.AddWithValue(start.AddHours(-2));
            refresh.Parameters.AddWithValue(end.AddHours(1));
            await refresh.ExecuteNonQueryAsync(ct);
        }

        report.AppendLine("ledger rows: " + await ScalarAsync(c, "SELECT count(*) FROM collect.procedure_stats_hour_ledger", ct));
        report.AppendLine("rollup rows: " + await ScalarAsync(c, "SELECT count(*) FROM collect.procedure_stats_interval_hourly", ct));
        var mat = await TextAsync(c, "SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) FROM timescaledb_information.continuous_aggregates WHERE view_name = 'procedure_stats_interval_hourly'", ct);
        report.AppendLine("materialization hypertable: " + mat);
        await ExecAsync(c, "ANALYZE collect.procedure_stats_hour_ledger", ct);
        await ExecAsync(c, $"ANALYZE {mat}", ct);
        report.AppendLine("rollup indexes: " + await TextAsync(c, $"SELECT string_agg(indexname || ' ' || indexdef, E'\\n') FROM pg_indexes WHERE schemaname || '.' || tablename = '{mat}'", ct));
        report.AppendLine("ledger indexes: " + await TextAsync(c, "SELECT string_agg(indexname || ' ' || indexdef, E'\\n') FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'procedure_stats_hour_ledger'", ct));
        report.AppendLine("query_stats ledger indexes: " + await TextAsync(c, "SELECT string_agg(indexname || ' ' || indexdef, E'\\n') FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'query_stats_hour_ledger'", ct));

        var winEnd = end;
        var winStart = end.AddHours(-23);
        await ExplainAllAsync(c, report, "uncompressed", winStart, winEnd, ct);

        try
        {
            await TimescaleSupport.EnsureAggregateCompressionAsync(c, null, ct);
            var n = await TextAsync(c, "SELECT count(compress_chunk(ch, true)) FROM show_chunks('collect.procedure_stats_interval_hourly') AS ch", ct);
            report.AppendLine("compressed rollup chunks: " + n);
            await ExecAsync(c, $"ANALYZE {mat}", ct);
            await ExplainAllAsync(c, report, "compressed rollup chunks", winStart, winEnd, ct);
        }
        catch (Exception ex)
        {
            report.AppendLine("compress failed: " + ex.Message);
        }

        File.WriteAllText(@"C:\runner\logs\explain-9357-s3.txt", report.ToString());
        Console.Error.WriteLine("GATE EXPLAIN written");
    }

    private static async Task ExplainAllAsync(NpgsqlConnection c, StringBuilder report, string label, DateTime from, DateTime to, System.Threading.CancellationToken ct)
    {
        foreach (var (scopeLabel, scope) in new (string, string[]?)[] { ("fleet (NULL scope)", null), ("one server", new[] { "ProcLedgerB" }) })
        {
            report.AppendLine($"=== {label}, {scopeLabel} ===");
            await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + IntervalRollupCountGuard.ProcedureStatsSql, c);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = from });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = to });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = scope is null ? DBNull.Value : scope });
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                report.AppendLine(reader.GetString(0));
            }
        }
    }

    private static async Task ExecAsync(NpgsqlConnection c, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, c) { CommandTimeout = 600 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> TextAsync(NpgsqlConnection c, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, c) { CommandTimeout = 600 };
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static Task<string?> ScalarAsync(NpgsqlConnection c, string sql, System.Threading.CancellationToken ct) => TextAsync(c, sql, ct);
}
