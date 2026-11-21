using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/* Scratch measurement rig for #9280 round 4a. Not shipped: removed before the push. Phases by env S9280. */
public sealed class Scratch9280Rig
{
    private const int Busy = 1;
    private const string Db = "m9280";
    private const string PlansFile = @"C:\runner\logs\m9280r4a-plans.txt";

    private static string? Phase => Environment.GetEnvironmentVariable("S9280");
    private static string BaseCs => Environment.GetEnvironmentVariable("DARLING_TEST_PG")!;
    private static string Scale(string name, string dflt) => Environment.GetEnvironmentVariable(name) ?? dflt;

    [Fact]
    public async Task Seed()
    {
        Assert.SkipUnless(Phase == "seed", "S9280 != seed");
        var ct = TestContext.Current.CancellationToken;
        var objects = int.Parse(Scale("S9280_OBJS", "850"), CultureInfo.InvariantCulture);

        await using var scratch = await ScratchPostgres.CreateAsync(BaseCs, ct);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            var servers = new[] { (1, 3), (2, 10), (3, 10), (4, 10), (5, 10), (6, 10) };
            foreach (var (id, _) in servers)
            {
                await DarlingMcpTestData.RegisterServerAsync(connection, id, "s9280-" + id, ct);
            }

            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            var procEnd = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-5), DateTimeKind.Unspecified);
            procEnd = new DateTime(procEnd.Year, procEnd.Month, procEnd.Day, procEnd.Hour, procEnd.Minute, 0, DateTimeKind.Unspecified);
            await Exec(connection, "CREATE TABLE public.m9280_anchor (proc_end timestamp)", ct);
            await Exec(connection, $"INSERT INTO public.m9280_anchor VALUES ('{procEnd:yyyy-MM-dd HH:mm:ss}')", ct);

            var from = procEnd.AddHours(-170);
            foreach (var (server, cadence) in servers)
            {
                await using var cmd = new NpgsqlCommand($@"
INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                             object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                             delta_logical_reads, delta_spills, min_worker_time, max_worker_time, sample_interval_seconds)
SELECT {server}::bigint * 1000000000000 + row_number() OVER (), t, {server}, 's9280', 'db' || (n % 20),
       'dbo', 'P' || lpad(n::text, 6, '0'), 'P', 'sh' || n, 'ph' || n,
       1 + (n * 7 + extract(epoch from t)::bigint / {cadence * 60}) % 50,
       1000 + (n * 7919 + extract(epoch from t)::bigint / {cadence * 60} * 31) % 90001,
       2000 + (n * 104729 + extract(epoch from t)::bigint / {cadence * 60} * 17) % 190001,
       10 * (n % 500), n % 2, n % 97, 2 * (n % 97), {cadence * 60}
FROM generate_series(0, {objects - 1}) AS n
CROSS JOIN generate_series($1::timestamp, $2::timestamp, interval '{cadence} minutes') AS t", connection) { CommandTimeout = 3000 };
                cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from);
                cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, procEnd);
                await cmd.ExecuteNonQueryAsync(ct);
            }

            Console.WriteLine($"Total: S9280 proc seeded {sw.Elapsed.TotalSeconds:F0}s");
            await using (var refresh = new NpgsqlCommand(
                $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection) { CommandTimeout = 3000 })
            {
                refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from.AddHours(-1));
                refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, procEnd.AddHours(2));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            await Exec(connection, TimescaleSupport.EnableCompressionSql("collect.procedure_stats"), ct);
            await Exec(connection, "SELECT count(compress_chunk(c)) FROM show_chunks('collect.procedure_stats', older_than => (now() AT TIME ZONE 'UTC') - interval '1 day') AS c", ct);
            Console.WriteLine($"Total: S9280 cagg+compress {sw.Elapsed.TotalSeconds:F0}s");
            await Exec(connection, ProcedureStatsNameIndex.CreateSql, ct);
            Console.WriteLine($"Total: S9280 name index {sw.Elapsed.TotalSeconds:F0}s");
            await Exec(connection, "ANALYZE collect.procedure_stats", ct);
            await Exec(connection, "SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE job_id >= 1000", ct);
            await using var c = new NpgsqlCommand("SELECT count(*), count(DISTINCT (database_name, object_name)) FROM collect.procedure_stats WHERE server_id = 1", connection);
            await using var r = await c.ExecuteReaderAsync(ct);
            await r.ReadAsync(ct);
            Console.WriteLine($"Total: S9280 busy rows {r.GetInt64(0)} keys {r.GetInt64(1)} {sw.Elapsed.TotalSeconds:F0}s");
        }

        NpgsqlConnection.ClearAllPools();
        await using var admin = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(BaseCs) { Database = "postgres", Pooling = false }.ConnectionString);
        await admin.OpenAsync(ct);
        await Exec(admin, $"DROP DATABASE IF EXISTS {Db}", ct);
        await Exec(admin, $"CREATE DATABASE {Db} TEMPLATE \"{scratch.DatabaseName}\"", ct);
        Console.WriteLine($"Total: S9280 cloned {sw.Elapsed.TotalSeconds:F0}s");
    }

    [Fact]
    public async Task AddPlainIndex()
    {
        Assert.SkipUnless(Phase == "plainindex", "S9280 != plainindex");
        var ct = TestContext.Current.CancellationToken;
        var cs = new NpgsqlConnectionStringBuilder(BaseCs) { Database = Db, Pooling = false }.ConnectionString;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await Exec(connection, "CREATE INDEX idx_ps_plain_name ON collect.procedure_stats (server_id, database_name, object_name, collection_time)", ct);
        await Exec(connection, "DROP INDEX collect." + ProcedureStatsNameIndex.IndexName, ct);
        await Exec(connection, "ANALYZE collect.procedure_stats", ct);
        Console.WriteLine($"Total: S9280 plain index built {sw.Elapsed.TotalSeconds:F0}s");
    }

    [Fact]
    public Task ExplainCold() => ExplainAsync("cold", 1);

    [Fact]
    public Task ExplainWarm() => ExplainAsync("warm", 3);

    private static async Task ExplainAsync(string label, int runs)
    {
        Assert.SkipUnless(Phase == label, "S9280 != " + label);
        var ct = TestContext.Current.CancellationToken;
        var cs = new NpgsqlConnectionStringBuilder(BaseCs) { Database = Db, Pooling = false, SearchPath = "collect,public" }.ConnectionString;
        await using var source = NpgsqlDataSource.Create(cs);
        await using var connection = await source.OpenConnectionAsync(ct);
        DateTime procEnd;
        await using (var a = new NpgsqlCommand("SELECT proc_end FROM public.m9280_anchor", connection))
        await using (var r = await a.ExecuteReaderAsync(ct))
        {
            await r.ReadAsync(ct);
            procEnd = r.GetDateTime(0);
        }

        var variant = Scale("S9280_VARIANT", "dev");
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(source, ct);
        foreach (var (hours, top) in new[] { (168, 10), (168, 25), (24, 10) })
        {
            var start = procEnd.AddHours(-hours);
            var mid = DarlingDataReader.PlanProceduresHourlyMid(rollups, coverage, TopRanking.Cpu, start, procEnd);
            Assert.NotNull(mid);
            var sql = SqlFor(variant, mid!);
            for (var i = 0; i < runs; i++)
            {
                await using var cmd = Cmd("EXPLAIN (ANALYZE, BUFFERS) " + sql, connection, start, procEnd, top, mid!);
                await Record($"{label} {variant} {hours}h top{top} run{i}", cmd, ct, i == runs - 1);
            }

            if (label == "warm" && variant != "dev")
            {
                var devRows = await Rows(Cmd(SqlFor("dev", mid!), connection, start, procEnd, top, mid!), ct);
                var newRows = await Rows(Cmd(sql, connection, start, procEnd, top, mid!), ct);
                Console.WriteLine($"Total: S9280 compare {variant} {hours}h top{top} rows {devRows.Count} equal={devRows.SequenceEqual(newRows)}");
                Assert.Equal(devRows, newRows);
            }
        }
    }

    private static string SqlFor(string variant, DarlingDataReader.HourlyMid mid) => variant switch
    {
        "dev" => DarlingDataReader.BuildProceduresMidSql(TopRanking.Cpu, mid, seek: false),
        "seek" => DarlingDataReader.BuildProceduresMidSql(TopRanking.Cpu, mid, seek: true),
        _ => DarlingDataReader.BuildProceduresMidSql(TopRanking.Cpu, mid, seek: true)
            .Replace(DarlingDataReader.MidSeekNameMatch, "", StringComparison.Ordinal),
    };

    private static NpgsqlCommand Cmd(string sql, NpgsqlConnection connection, DateTime start, DateTime end, int top, DarlingDataReader.HourlyMid mid)
    {
        var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Busy });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = top });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mid.MidStart });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mid.MidEnd });
        return cmd;
    }

    private static async Task<System.Collections.Generic.List<string>> Rows(NpgsqlCommand cmd, CancellationToken ct)
    {
        var list = new System.Collections.Generic.List<string>();
        await using (cmd)
        await using (var r = await cmd.ExecuteReaderAsync(ct))
        {
            while (await r.ReadAsync(ct))
            {
                list.Add(string.Join("|", Enumerable.Range(0, r.FieldCount).Select(i => r.IsDBNull(i) ? "~" : Convert.ToString(r.GetValue(i), CultureInfo.InvariantCulture))));
            }
        }

        return list;
    }

    private static async Task Record(string label, NpgsqlCommand cmd, CancellationToken ct, bool print = true)
    {
        var sb = new StringBuilder();
        sb.AppendLine("=== " + label);
        await using (var r = await cmd.ExecuteReaderAsync(ct))
        {
            while (await r.ReadAsync(ct))
            {
                sb.AppendLine(r.GetString(0));
            }
        }

        var text = sb.ToString();
        var exec = text.Split('\n').FirstOrDefault(l => l.StartsWith("Execution Time", StringComparison.Ordinal))?.Trim();
        var buf = text.Split('\n').FirstOrDefault(l => l.TrimStart().StartsWith("Buffers:", StringComparison.Ordinal))?.Trim();
        if (print)
        {
            Console.WriteLine($"Total: S9280 {label} {exec} {buf}");
        }

        File.AppendAllText(PlansFile, text + Environment.NewLine);
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 3000 };
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
