using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/* Scratch measurement rig for #9280. Not shipped: removed in the last commit. Phases run by env S9280 (seed, cold, warm). */
public sealed class Scratch9280Rig
{
    private const int Busy = 1;
    private const string Db = "m9280";
    private const string PlansFile = @"C:\runner\logs\m9280-plans.txt";

    private static string? Phase => Environment.GetEnvironmentVariable("S9280");
    private static string BaseCs => Environment.GetEnvironmentVariable("DARLING_TEST_PG")!;
    private static string Scale(string name, string dflt) => Environment.GetEnvironmentVariable(name) ?? dflt;

    [Fact]
    public async Task Seed()
    {
        Assert.SkipUnless(Phase == "seed", "S9280 != seed");
        var ct = TestContext.Current.CancellationToken;
        var keys = int.Parse(Scale("S9280_KEYS", "60000"), CultureInfo.InvariantCulture);
        var procs = int.Parse(Scale("S9280_PROCS", "3000"), CultureInfo.InvariantCulture);

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
            foreach (var (id, name) in new[] { (1, "busy"), (2, "mid"), (3, "small") })
            {
                await DarlingMcpTestData.RegisterServerAsync(connection, id, "s9280-" + name, ct);
            }

            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            var today = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
            var qsEnd = today.AddHours(-12);                       /* yesterday 12:00 */
            var procEnd = today.AddHours(-12).AddMinutes(20);      /* yesterday 12:20 */
            await Exec(connection, "CREATE TABLE public.m9280_anchor (qs_end timestamp, proc_end timestamp)", ct);
            await Exec(connection, $"INSERT INTO public.m9280_anchor VALUES ('{qsEnd:yyyy-MM-dd HH:mm:ss}', '{procEnd:yyyy-MM-dd HH:mm:ss}')", ct);

            /* ---- procedures ---- */
            var procStart = procEnd.AddHours(-168);
            var from = procStart.AddHours(-2);
            var to = procEnd.AddHours(3);
            foreach (var (server, n) in new[] { (1, procs), (2, procs / 8), (3, procs / 40) })
            {
                await using var cmd = new NpgsqlCommand($@"
INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                             object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                             delta_logical_reads, delta_spills, min_worker_time, max_worker_time, sample_interval_seconds)
SELECT {server}::bigint * 1000000000000 + row_number() OVER (), t, {server}, 's9280', 'db' || (n % 20),
       'dbo', 'P' || lpad(n::text, 6, '0'), 'P', 'sh' || n, 'ph' || n,
       1 + (n * 7 + extract(epoch from t)::bigint / 1800) % 50,
       1000 + (n * 7919 + extract(epoch from t)::bigint / 1800 * 31) % 90001,
       2000 + (n * 104729 + extract(epoch from t)::bigint / 1800 * 17) % 190001,
       10 * (n % 500), n % 2, n % 97, 2 * (n % 97), 120
FROM generate_series(0, {n - 1}) AS n
CROSS JOIN generate_series($1::timestamp, $2::timestamp, interval '30 minutes') AS t", connection) { CommandTimeout = 1800 };
                cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from);
                cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, to);
                await cmd.ExecuteNonQueryAsync(ct);
            }

            Console.WriteLine($"Total: S9280 proc seeded {sw.Elapsed.TotalSeconds:F0}s");
            await using (var refresh = new NpgsqlCommand(
                $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection) { CommandTimeout = 1800 })
            {
                refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from.AddHours(-1));
                refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, procEnd.AddHours(2));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            await Exec(connection, TimescaleSupport.EnableCompressionSql("collect.procedure_stats"), ct);
            /* The store's policy compresses a chunk once it is a day old. */
            await Exec(connection, "SELECT count(compress_chunk(c)) FROM show_chunks('collect.procedure_stats', older_than => (now() AT TIME ZONE 'UTC') - interval '1 day') AS c", ct);
            Console.WriteLine($"Total: S9280 proc cagg+compress {sw.Elapsed.TotalSeconds:F0}s");

            /* ---- query store ---- */
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc);
            var promoted = await QueryStoreIntervalPartitions.RunPromotionAsync(
                connection, QueryStoreIntervalPartitions.Wide, utcNow.AddDays(-19), NullLogger.Instance,
                TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(50), ct);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, promoted.Outcome);
            var created = await QueryStoreIntervalPartitions.CreateAheadAsync(connection, QueryStoreIntervalPartitions.Wide, utcNow, NullLogger.Instance, ct);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, created.Outcome);

            var day0 = qsEnd.Date.AddDays(-8);   /* 9 days: day0 .. yesterday 12:00 */
            foreach (var (server, u) in new[] { (1, keys), (2, keys / 8), (3, keys / 40) })
            {
                await using var cmd = new NpgsqlCommand($@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
 avg_physical_io_reads, avg_rowcount, query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT ct, {server}, 'db' || (q % 20), q, q, 'Regular', ct - interval '10 minutes', ct - interval '1 minute',
       CASE WHEN q % 4 = 0 THEN 'dbo.m1' ELSE NULL END, 'h' || q, 1 + (q * 13 + d * 7 + h) % 23,
       1000 + (q * 7919 + d * 104729 + h * 31) % 900001, 10 + (q * 31 + d * 17) % 5003, 100 + (q * 101 + d * 13) % 500009,
       (q * 3 + d) % 97, (q + h * 5) % 211, 1 + (q * 29 + d * 3) % 1009, 'ph' || (q % 5), NULL,
       d * 24 + h, ct
FROM (
    SELECT q, d, h, $1::timestamp + make_interval(days => d, hours => h) AS ct
    FROM generate_series(1, {u}) AS q
    CROSS JOIN generate_series(0, 8) AS d
    CROSS JOIN generate_series(0, 3) AS k
    CROSS JOIN LATERAL (SELECT (q % 6) + 6 * k AS h) AS hh
    WHERE (q * 31 + d * 17) % 4 < 3
) AS s
WHERE ct <= $2::timestamp", connection) { CommandTimeout = 1800 };
                cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, day0);
                cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, qsEnd);
                await cmd.ExecuteNonQueryAsync(ct);
            }

            Console.WriteLine($"Total: S9280 qs seeded {sw.Elapsed.TotalSeconds:F0}s");
            var buildNow = qsEnd.Date.AddDays(2);
            for (var d = 0; d < 8; d++)
            {
                foreach (var server in new[] { 1, 2, 3 })
                {
                    await QueryStoreTopDaily.BuildDayAsync(connection, server, DateOnly.FromDateTime(day0.AddDays(d)), 2, buildNow, ct);
                }
            }

            await Exec(connection, "ANALYZE collect.query_store_interval_wide; ANALYZE collect.query_store_top_daily; ANALYZE collect.procedure_stats;", ct);
            await Exec(connection, "SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE job_id >= 1000", ct);
            Console.WriteLine($"Total: S9280 qs built+analyzed {sw.Elapsed.TotalSeconds:F0}s");
            var counts = new StringBuilder();
            foreach (var t in new[] { "collect.procedure_stats", "collect.query_store_interval_wide", "collect.query_store_top_daily" })
            {
                await using var c = new NpgsqlCommand($"SELECT count(*) FROM {t} WHERE server_id = 1", connection);
                counts.Append(t).Append('=').Append(await c.ExecuteScalarAsync(ct)).Append(' ');
            }

            Console.WriteLine("Total: S9280 counts " + counts);
            Append("SEED " + counts);
        }

        NpgsqlConnection.ClearAllPools();
        await using var admin = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(BaseCs) { Database = "postgres", Pooling = false }.ConnectionString);
        await admin.OpenAsync(ct);
        await Exec(admin, $"DROP DATABASE IF EXISTS {Db}", ct);
        await Exec(admin, $"CREATE DATABASE {Db} TEMPLATE \"{scratch.DatabaseName}\"", ct);
        Console.WriteLine($"Total: S9280 cloned {sw.Elapsed.TotalSeconds:F0}s");
    }

    [Fact]
    public async Task Decompress()
    {
        Assert.SkipUnless(Phase == "decompress", "S9280 != decompress");
        var ct = TestContext.Current.CancellationToken;
        var cs = new NpgsqlConnectionStringBuilder(BaseCs) { Database = Db, Pooling = false }.ConnectionString;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await Exec(connection, "SELECT count(decompress_chunk(c, true)) FROM show_chunks('collect.procedure_stats') AS c", ct);
        await Exec(connection, "ANALYZE collect.procedure_stats", ct);
        Console.WriteLine("Total: S9280 decompressed");
    }

    [Fact]
    public async Task Recompress()
    {
        Assert.SkipUnless(Phase == "recompress", "S9280 != recompress");
        var ct = TestContext.Current.CancellationToken;
        var cs = new NpgsqlConnectionStringBuilder(BaseCs) { Database = Db, Pooling = false }.ConnectionString;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await Exec(connection, "SELECT count(decompress_chunk(c, true)) FROM show_chunks('collect.procedure_stats') AS c", ct);
        var seg = Scale("S9280_SEG", "server_id");
        var ord = Scale("S9280_ORD", "collection_time DESC");
        await Exec(connection, $"ALTER TABLE collect.procedure_stats SET (timescaledb.compress_segmentby = '{seg}', timescaledb.compress_orderby = '{ord}')", ct);
        await Exec(connection, "SELECT count(compress_chunk(c)) FROM show_chunks('collect.procedure_stats', older_than => (now() AT TIME ZONE 'UTC') - interval '1 day') AS c", ct);
        await Exec(connection, "ANALYZE collect.procedure_stats", ct);
        Console.WriteLine($"Total: S9280 recompressed seg=[{seg}] ord=[{ord}] {sw.Elapsed.TotalSeconds:F0}s");
        await using var size = new NpgsqlCommand("SELECT pg_size_pretty(sum(after_compression_total_bytes)), count(*) FROM chunk_compression_stats('collect.procedure_stats')", connection);
        await using var r = await size.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        Console.WriteLine($"Total: S9280 compressed size {r.GetString(0)} chunks {r.GetInt64(1)}");
    }

    [Fact]
    public Task ExplainCold() => ExplainAsync("cold", 1);

    [Fact]
    public Task ExplainWarm() => ExplainAsync("warm", 2);

    private static async Task ExplainAsync(string label, int runs)
    {
        Assert.SkipUnless(Phase == label, "S9280 != " + label);
        var ct = TestContext.Current.CancellationToken;
        var cs = new NpgsqlConnectionStringBuilder(BaseCs) { Database = Db, Pooling = false }.ConnectionString;
        await using var source = NpgsqlDataSource.Create(cs);
        await using var connection = await source.OpenConnectionAsync(ct);
        DateTime qsEnd, procEnd;
        await using (var a = new NpgsqlCommand("SELECT qs_end, proc_end FROM public.m9280_anchor", connection))
        await using (var r = await a.ExecuteReaderAsync(ct))
        {
            await r.ReadAsync(ct);
            qsEnd = r.GetDateTime(0);
            procEnd = r.GetDateTime(1);
        }

        var variantQs = Scale("S9280_VARIANT", "dev");
        /* QS daily */
        if (Scale("S9280_QS", "1") == "1")
        {
            var readStart = qsEnd.AddHours(-168);
            var s = readStart.Date.AddDays(1);
            var e = qsEnd.Date;
            for (var i = 0; i < runs; i++)
            {
                await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + QsSql(variantQs), connection);
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Busy });
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = readStart });
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = qsEnd });
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 3 });
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = DateOnly.FromDateTime(s) });
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = DateOnly.FromDateTime(e) });
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = TopFill.FirstCandidates(3) });
                await Record($"{label} {variantQs} QS_DAILY run{i}", cmd, ct);
            }
        }

        /* procedures mid */
        {
            await using var availability = NpgsqlDataSource.Create(cs);
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(availability, ct);
            foreach (var hours in new[] { 24, 168 })
            foreach (var variant in new[] { "dev", "new" })
            {
                var start = procEnd.AddHours(-hours);
                var mid = DarlingDataReader.PlanProceduresHourlyMid(rollups, coverage, TopRanking.Cpu, start, procEnd);
                Assert.NotNull(mid);
                for (var i = 0; i < runs; i++)
                {
                    await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + ProcSql(variant, mid!), connection);
                    cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Busy });
                    cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
                    cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = procEnd });
                    cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 3 });
                    cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
                    cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
                    cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mid!.MidStart });
                    cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mid.MidEnd });
                    await Record($"{label} {Scale("S9280_TAG", "base")} {variant} {hours}h run{i}", cmd, ct, i == runs - 1);
                }
            }
        }
    }

    private const string OldWide = """
        /* #5226 pass 2 of 2: the wide row for pass 1's winners, over the same window and filters, matched on the
           full key NULL-safely (any of the four can be NULL; a plain equality would drop that group). The
           COALESCE equality is only the hashable pre-filter (see TopQueriesSql); IS NOT DISTINCT FROM makes the
           match exact. This ORDER BY is the page's final order: the same keys as pass 1, group key last. */
        SELECT
            database_name,
            schema_name,
            object_name,
            object_type,
            MAX(sql_handle) AS sql_handle,
            MAX(plan_handle) AS plan_handle,
            CAST(SUM(delta_execution_count) AS bigint) AS total_executions,
            CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
            CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
            CAST(SUM(delta_logical_reads) AS bigint) AS total_reads,
            CAST(SUM(delta_logical_writes) AS bigint) AS total_writes,
            CAST(SUM(delta_physical_reads) AS bigint) AS total_physical_reads,
            CAST(SUM(delta_spills) AS bigint) AS total_spills,
            MIN(min_worker_time) AS min_worker_time,
            MAX(max_worker_time) AS max_worker_time,
            MIN(min_elapsed_time) AS min_elapsed_time,
            MAX(max_elapsed_time) AS max_elapsed_time,
            MAX(last_execution_time) AS last_execution_time,
            MAX(cached_time) AS cached_time,
            MIN(min_logical_reads) AS min_logical_reads,
            MAX(max_logical_reads) AS max_logical_reads,
            MIN(min_physical_reads) AS min_physical_reads,
            MAX(max_physical_reads) AS max_physical_reads,
            MIN(min_logical_writes) AS min_logical_writes,
            MAX(max_logical_writes) AS max_logical_writes,
            MIN(min_spills) AS min_spills,
            MAX(max_spills) AS max_spills
        FROM procedure_stats
        JOIN winners AS w
            ON  COALESCE(object_name, '') = COALESCE(w.win_object_name, '')
            AND database_name IS NOT DISTINCT FROM w.win_database_name
            AND schema_name IS NOT DISTINCT FROM w.win_schema_name
            AND object_name IS NOT DISTINCT FROM w.win_object_name
            AND object_type IS NOT DISTINCT FROM w.win_object_type
        WHERE server_id = $1
        AND   collection_time > $2
        AND   collection_time <= $3
        AND   ($5::text[] IS NULL OR database_name = ANY($5))
        AND   ($6::text[] IS NULL OR database_name IS NULL OR database_name <> ALL($6))
        AND   sample_interval_seconds IS DISTINCT FROM 0
        GROUP BY database_name, schema_name, object_name, object_type
        ORDER BY MAX(w.rank_metric) DESC NULLS LAST, MAX(w.rank_cpu) DESC NULLS LAST, database_name, schema_name, object_name, object_type
        """;

    private static string QsSql(string variant) => DarlingDataReader.QueryStoreTopDailyTableSql;

    private static string ProcSql(string variant, DarlingDataReader.HourlyMid mid) => variant == "new"
        ? DarlingDataReader.BuildProceduresMidSql(TopRanking.Cpu, mid)
        : TopRankings.Apply(DarlingDataReader.TopProceduresMidWinnersSql, TopRanking.Cpu, hourly: true).Replace(DarlingDataReader.HourlyMidRelationPlaceholder, mid.Relation, StringComparison.Ordinal)
          + "\n" + OldWide.Replace("AND object_type IS NOT DISTINCT FROM w.win_object_type", "", StringComparison.Ordinal);

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
        Append(text);
    }

    private static void Append(string text) => File.AppendAllText(PlansFile, text + Environment.NewLine);

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 1800 };
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
