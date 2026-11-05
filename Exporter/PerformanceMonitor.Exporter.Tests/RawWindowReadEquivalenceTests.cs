using System.Diagnostics;
using System.Globalization;
using DuckDB.NET.Data;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Load-bearing equivalence + benchmark for the raw window reads (procedure_stats, query_stats, Query Store).
/// Needs Docker (PostgreSQL container); with no Docker the tests return without asserting. The legacy read loop
/// (boxed GetValue + per-cell type-name switch, default reader behaviour) lives here, as the oracle only.
/// Benchmark: <c>DARLING_EXPORT_TEST_BENCH=1 dotnet test Exporter/PerformanceMonitor.Exporter.Tests -c Release --filter "FullyQualifiedName~RawWindowReadEquivalence" --logger "console;verbosity=detailed"</c>
/// (rows: DARLING_EXPORT_TEST_BENCH_ROWS, default 400000; runs: DARLING_EXPORT_TEST_BENCH_RUNS, default 5).
/// </summary>
[Trait("Category", "Slow")]
public sealed class RawWindowReadEquivalenceTests : IClassFixture<RawWindowReadEquivalenceTests.PgFixture>
{
    private static readonly DateTime From = new(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime To = From.AddHours(1);

    private static ExportOptions Options() => new()
    {
        Store = "bench", FromUtc = From, ToUtc = To, OutDir = "x", Tables = [],
    };

    public sealed class PgFixture : IDisposable
    {
        private readonly string _container = "darling-export-rawread-" + Guid.NewGuid().ToString("N")[..8];
        public NpgsqlConnectionStringBuilder? Builder { get; }

        public PgFixture()
        {
            try
            {
                var run = Sh("docker", $"run -d --rm --name {_container} -e POSTGRES_PASSWORD=pw -e POSTGRES_INITDB_ARGS=--locale=C -p 127.0.0.1::5432 postgres:17");
                if (run.code != 0) { return; }
                var deadline = DateTime.UtcNow.AddSeconds(90);
                var ok = 0;
                while (DateTime.UtcNow < deadline && ok < 3)
                {
                    ok = Psql("SELECT 1").code == 0 ? ok + 1 : 0;
                    Thread.Sleep(1000);
                }
                if (ok < 3) { Dispose(); return; }
                var port = Sh("docker", $"port {_container} 5432/tcp").output.Trim().Split(':')[^1];
                Builder = new NpgsqlConnectionStringBuilder
                {
                    Host = "127.0.0.1", Port = int.Parse(port), Username = "postgres", Password = "pw", Database = "postgres", Timeout = 30, CommandTimeout = 600,
                };
                CreateSchema();
            }
            catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException)
            {
                Builder = null; // no docker CLI
            }
        }

        public (int code, string output) Psql(string sql)
        {
            var psi = new ProcessStartInfo("docker", $"exec -i {_container} psql -U postgres -d postgres -v ON_ERROR_STOP=1 -q")
            {
                RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true,
            };
            using var p = Process.Start(psi)!;
            p.StandardInput.Write(sql);
            p.StandardInput.Close();
            var o = p.StandardOutput.ReadToEnd() + p.StandardError.ReadToEnd();
            p.WaitForExit();
            return (p.ExitCode, o);
        }

        private static (int code, string output) Sh(string file, string args)
        {
            var psi = new ProcessStartInfo(file, args) { RedirectStandardOutput = true, RedirectStandardError = true };
            using var p = Process.Start(psi)!;
            var o = p.StandardOutput.ReadToEnd() + p.StandardError.ReadToEnd();
            p.WaitForExit();
            return (p.ExitCode, o);
        }

        private void CreateSchema()
        {
            var r = Psql($$"""
                CREATE SCHEMA collect;
                CREATE TABLE collect.query_stats (
                  collection_id bigint, collection_time timestamp NOT NULL, server_id int NOT NULL,
                  server_name text NOT NULL, database_name text, query_hash text, query_plan_hash text,
                  query_plan_digest bytea, query_text_digest bytea,
                  delta_execution_count bigint, delta_worker_time bigint, delta_elapsed_time bigint,
                  delta_logical_reads bigint, delta_logical_writes bigint, delta_physical_reads bigint,
                  delta_rows bigint, delta_spills bigint, sample_interval_seconds int);
                CREATE INDEX ON collect.query_stats (collection_time);
                CREATE TABLE collect.procedure_stats (
                  collection_id bigint, collection_time timestamp NOT NULL, server_id int NOT NULL,
                  server_name text NOT NULL, database_name text, schema_name text, object_name text, object_type text,
                  query_plan_digest bytea, query_plan_xml text, query_plan_xml_bytes bigint,
                  plan_handle text, cached_time timestamp,
                  delta_execution_count bigint, delta_worker_time bigint, delta_elapsed_time bigint,
                  delta_logical_reads bigint, delta_logical_writes bigint, delta_physical_reads bigint,
                  delta_spills bigint, sample_interval_seconds int);
                CREATE INDEX ON collect.procedure_stats (collection_time);
                CREATE TABLE collect.query_store_stats (
                  collection_id bigint, server_id int, server_name text, database_name text,
                  {{string.Join(", ", QueryStoreWindow.RawColumns.Where(c => c is not ("server_id" or "server_name" or "database_name"))
                      .Select(c => $"{c} {PgType(c)}"))}});
                CREATE INDEX ON collect.query_store_stats (collection_time);
                """);
            Assert.True(r.code == 0, r.output);
        }

        private static string PgType(string column) => QueryStoreWindow.DuckDbTypeFor(column) switch
        {
            "INTEGER" => "int", "BIGINT" => "bigint", "VARCHAR" => "text", "BOOLEAN" => "boolean", "TIMESTAMP" => "timestamp",
            _ => throw new NotSupportedException(column),
        };

        public void Dispose()
        {
            try { Sh("docker", $"rm -f {_container}"); } catch (Exception) { /* best effort */ }
        }
    }

    private readonly ITestOutputHelper output;
    private PgFixture Pg { get; }

    public RawWindowReadEquivalenceTests(ITestOutputHelper output, PgFixture pg)
    {
        this.output = output;
        Pg = pg;
    }

    private const string Inside = "timestamp '2026-10-22 11:00:00' + (i % 3500) * interval '1 second' + (i % 997) * interval '1 microsecond'";

    private static string BulkSql(int n) => $$"""
        INSERT INTO collect.procedure_stats
        SELECT i, {{Inside}}, 1 + i % 3, 'srv' || (i % 7), CASE WHEN i % 11 = 0 THEN NULL ELSE 'db' || (i % 40) END,
               'dbo', 'proc_' || (i % 5000), CASE WHEN i % 13 = 0 THEN NULL ELSE 'P' END,
               CASE WHEN i % 9 = 0 THEN NULL ELSE decode(md5(i::text) || md5((i % 5000)::text), 'hex') END,
               CASE WHEN i % 17 = 0 THEN NULL ELSE repeat('x', 50) END, CASE WHEN i % 19 = 0 THEN NULL ELSE i * 10 END,
               CASE WHEN i % 8 = 0 THEN NULL ELSE '0x' || md5(i::text) END,
               CASE WHEN i % 6 = 0 THEN NULL ELSE timestamp '2026-10-20 00:00:00' + i * interval '1 second' END,
               i % 100, i * 1000, i * 7919, i * 31, i % 50, i % 9, i % 2, CASE WHEN i % 23 = 0 THEN NULL ELSE 60 + i % 3 END
        FROM generate_series(1::bigint, {{n}}::bigint) i;
        INSERT INTO collect.query_stats
        SELECT i, {{Inside}}, 1 + i % 3, 'srv' || (i % 7), CASE WHEN i % 11 = 0 THEN NULL ELSE 'db' || (i % 40) END,
               '0x' || md5(i::text), CASE WHEN i % 13 = 0 THEN NULL ELSE '0x' || md5((i % 9000)::text) END,
               CASE WHEN i % 9 = 0 THEN NULL ELSE decode(md5(i::text) || md5((i % 9000)::text), 'hex') END,
               CASE WHEN i % 10 = 0 THEN NULL ELSE decode(md5((i % 7000)::text) || md5('t'), 'hex') END,
               i % 100, i * 1000, i * 7919, i * 31, i % 50, i % 9, i % 123, i % 2, CASE WHEN i % 23 = 0 THEN NULL ELSE 60 + i % 3 END
        FROM generate_series(1::bigint, {{n}}::bigint) i;
        INSERT INTO collect.query_store_stats ({{string.Join(", ", QueryStoreWindow.RawColumns.Prepend("collection_id"))}})
        SELECT {{string.Join(", ", QueryStoreWindow.RawColumns.Select(QsExpr).Prepend("i"))}}
        FROM generate_series(1::bigint, {{n}}::bigint) i;
        """;

    private static string QsExpr(string column) => QueryStoreWindow.DuckDbTypeFor(column) switch
    {
        "INTEGER" => $"CASE WHEN i % 29 = 0 THEN NULL ELSE (i % 140)::int END",
        "TIMESTAMP" when column == "interval_start_time_utc" => "timestamp '2026-10-22 11:00:00' + (i % 3500) * interval '1 second'",
        "TIMESTAMP" when column == "collection_time" => "timestamp '2026-10-22 11:00:00' + (i % 3500) * interval '1 second' + (i % 997) * interval '1 microsecond'",
        "TIMESTAMP" => "CASE WHEN i % 6 = 0 THEN NULL ELSE timestamp '2026-10-22 10:00:00' + i * interval '1 millisecond' END",
        "BOOLEAN" => "CASE WHEN i % 5 = 0 THEN NULL ELSE i % 2 = 0 END",
        "VARCHAR" => $"CASE WHEN i % 7 = 0 THEN NULL ELSE '{column}_' || (i % 300) END",
        _ => $"CASE WHEN i % 31 = 0 THEN NULL ELSE i * 17 END",
    };

    private static string EdgeSql() => $$"""
        INSERT INTO collect.procedure_stats VALUES
         (-1, '2026-10-22 11:00:00', 1, '', '', 'sch', 'obj', '', '\x', '', 0, '', '2026-10-22 11:00:00.000001', 9223372036854775807, -9223372036854775808, 0, 1, -1, 2, 3, 2147483647),
         (-2, '2026-10-22 11:59:59.999999', 2, 'sérvér-日本語-🦆', 'dä', 'sch', 'öbj', 'P', '\x00ff80', NULL, NULL, 'h', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, -2147483648),
         (-3, '2026-10-22 11:30:00.123457', 3, 'n', NULL, NULL, NULL, NULL, NULL, 'plan', 5, NULL, NULL, 1, 2, 3, 4, 5, 6, 7, NULL);
        INSERT INTO collect.query_stats VALUES
         (-1, '2026-10-22 11:00:00', 1, '', '', '', '', '\x', '\x', 9223372036854775807, -9223372036854775808, 0, 1, -1, 2, 3, 4, 2147483647),
         (-2, '2026-10-22 11:59:59.999999', 2, 'sérvér-日本語-🦆', 'dä', '0xabc', NULL, '\x00ff80', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, -2147483648),
         (-3, '2026-10-22 11:30:00.123457', 3, 'n', NULL, NULL, NULL, NULL, '\xdeadbeef', 1, 2, 3, 4, 5, 6, 7, 8, NULL);
        INSERT INTO collect.query_store_stats ({{string.Join(", ", QueryStoreWindow.RawColumns.Prepend("collection_id"))}})
        SELECT -1, {{string.Join(", ", QueryStoreWindow.RawColumns.Select(c => EdgeExpr(c, 0)))}}
        UNION ALL SELECT -2, {{string.Join(", ", QueryStoreWindow.RawColumns.Select(c => EdgeExpr(c, 1)))}}
        UNION ALL SELECT -3, {{string.Join(", ", QueryStoreWindow.RawColumns.Select(c => EdgeExpr(c, 2)))}};
        """;

    private static string EdgeExpr(string column, int variant)
    {
        var t = QueryStoreWindow.DuckDbTypeFor(column);
        if (variant == 2)
        {
            return column is "server_id" or "server_name" or "interval_start_time_utc" or "collection_time"
                ? EdgeExpr(column, 0) : $"NULL::{(t == "VARCHAR" ? "text" : t == "TIMESTAMP" ? "timestamp" : t == "BOOLEAN" ? "boolean" : t == "INTEGER" ? "int" : "bigint")}";
        }
        return t switch
        {
            "INTEGER" => variant == 0 ? "2147483647" : "-2147483648",
            "BIGINT" => variant == 0 ? "9223372036854775807::bigint" : "(-9223372036854775808)::bigint",
            "VARCHAR" => variant == 0 ? "''" : "'wïde-日本語-🦆'",
            "BOOLEAN" => variant == 0 ? "true" : "false",
            _ => column == "interval_start_time_utc" || column == "collection_time"
                ? (variant == 0 ? "timestamp '2026-10-22 11:00:00.000001'" : "timestamp '2026-10-22 11:59:59.999999'")
                : "timestamp '2026-10-22 11:00:00.000001'",
        };
    }

    // ---- legacy oracle: the pre-change read loop, verbatim in behaviour ----
    private static async Task<string> LegacyBuildAsync(NpgsqlConnectionStringBuilder b, string sql, string table,
        IReadOnlyList<string> columns, Func<string, string> typeFor, bool qs, CancellationToken ct)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-export-legacy-{Guid.NewGuid():N}.duckdb");
        await using var pg = await StoreConnectionFactory.OpenAsync(b, ct);
        await using var command = new NpgsqlCommand(sql, pg);
        command.Parameters.Add(new NpgsqlParameter("from", NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(From, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter("to", NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(To, DateTimeKind.Unspecified) });
        if (qs)
        {
            command.Parameters.Add(new NpgsqlParameter("to_plus_2h", NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(Options().QueryStoreCollectionToUtc, DateTimeKind.Unspecified) });
        }
        await using var reader = await command.ExecuteReaderAsync(ct);
        using var duck = DuckDbSession.Open(path);
        using (var create = duck.CreateCommand())
        {
            create.CommandText = $"CREATE TABLE {table} ({string.Join(", ", columns.Select(c => $"\"{c}\" {typeFor(c)}"))})";
            create.ExecuteNonQuery();
        }
        using var appender = duck.CreateAppender(table);
        while (reader.Read())
        {
            var row = appender.CreateRow();
            for (var i = 0; i < columns.Count; i++) { LegacyAppend(row, reader, i, typeFor(columns[i])); }
            row.EndRow();
        }
        return path;
    }

    private static void LegacyAppend(IDuckDBAppenderRow row, System.Data.IDataReader reader, int ordinal, string t)
    {
        if (reader.IsDBNull(ordinal)) { row.AppendNullValue(); return; }
        switch (t)
        {
            case "BOOLEAN": row.AppendValue(Convert.ToBoolean(reader.GetValue(ordinal))); break;
            case "INTEGER": row.AppendValue(Convert.ToInt32(reader.GetValue(ordinal))); break;
            case "BIGINT": row.AppendValue(Convert.ToInt64(reader.GetValue(ordinal))); break;
            case "VARCHAR": row.AppendValue(reader.GetValue(ordinal)?.ToString()); break;
            case "TIMESTAMP": row.AppendValue(reader.GetDateTime(ordinal)); break;
            case "BLOB": row.AppendValue((byte[])reader.GetValue(ordinal)); break;
            default: throw new NotSupportedException(t);
        }
    }

    private static async Task<(long reads, double ms)> ReadOnlyAsync(NpgsqlConnectionStringBuilder b, string sql, int cols, bool qs, bool typed, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        await using var pg = await StoreConnectionFactory.OpenAsync(b, ct);
        await using var command = new NpgsqlCommand(sql, pg);
        command.Parameters.Add(new NpgsqlParameter("from", NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(From, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter("to", NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(To, DateTimeKind.Unspecified) });
        if (qs) { command.Parameters.Add(new NpgsqlParameter("to_plus_2h", NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(Options().QueryStoreCollectionToUtc, DateTimeKind.Unspecified) }); }
        await using var reader = await command.ExecuteReaderAsync(typed ? System.Data.CommandBehavior.SequentialAccess : System.Data.CommandBehavior.Default, ct);
        long n = 0, sink = 0;
        if (typed)
        {
            var types = Enumerable.Range(0, cols).Select(reader.GetFieldType).ToArray();
            while (await reader.ReadAsync(ct))
            {
                n++;
                for (var i = 0; i < cols; i++)
                {
                    if (reader.IsDBNull(i)) { continue; }
                    var t = types[i];
                    if (t == typeof(long)) { sink += reader.GetInt64(i); }
                    else if (t == typeof(int)) { sink += reader.GetInt32(i); }
                    else if (t == typeof(string)) { sink += reader.GetString(i).Length; }
                    else if (t == typeof(DateTime)) { sink += reader.GetDateTime(i).Ticks; }
                    else if (t == typeof(byte[])) { sink += reader.GetFieldValue<byte[]>(i).Length; }
                    else if (t == typeof(bool)) { sink += reader.GetBoolean(i) ? 1 : 0; }
                }
            }
        }
        else
        {
            while (reader.Read())
            {
                n++;
                for (var i = 0; i < cols; i++) { sink += reader.IsDBNull(i) ? 0 : reader.GetValue(i) is { } v ? 1 : 0; }
            }
        }
        GC.KeepAlive(sink);
        return (n, sw.Elapsed.TotalMilliseconds);
    }

    private static long Scalar(DuckDBConnection c, string sql)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private static void AssertSameTable(string legacyPath, string newPath, string table)
    {
        using var duck = new DuckDBConnection($"Data Source={legacyPath}");
        duck.Open();
        using (var attach = duck.CreateCommand())
        {
            attach.CommandText = $"ATTACH '{newPath.Replace("'", "''")}' AS nw (READ_ONLY)";
            attach.ExecuteNonQuery();
        }
        var legacyCount = Scalar(duck, $"SELECT count(*) FROM main.{table}");
        Assert.True(legacyCount > 3, "fixture produced no rows");
        Assert.Equal(legacyCount, Scalar(duck, $"SELECT count(*) FROM nw.main.{table}"));
        Assert.Equal(0, Scalar(duck, $"SELECT count(*) FROM (SELECT * FROM main.{table} EXCEPT ALL SELECT * FROM nw.main.{table})"));
        Assert.Equal(0, Scalar(duck, $"SELECT count(*) FROM (SELECT * FROM nw.main.{table} EXCEPT ALL SELECT * FROM main.{table})"));
        // column types must match as well (EXCEPT would hide a widening cast)
        Assert.Equal(0, Scalar(duck, "SELECT count(*) FROM (SELECT column_name, data_type FROM duckdb_columns() WHERE database_name = 'nw' AND table_name = '" + table + "' " +
            "EXCEPT SELECT column_name, data_type FROM duckdb_columns() WHERE database_name <> 'nw' AND database_name <> 'system' AND database_name <> 'temp' AND table_name = '" + table + "')"));
    }

    private static readonly Dictionary<string, (string Sql, string Table, bool Qs)> Windows = new()
    {
        ["procedure_stats"] = (SqlBuilder.ProcedureStatsWindowSql, "procedure_stats_raw", false),
        ["query_stats"] = (SqlBuilder.QueryStatsWindowSql, "query_stats_raw", false),
        ["query_store"] = (SqlBuilder.QueryStoreStatsRawSql, "query_store_raw", true),
    };

    private static IReadOnlyList<string> ColumnsOf(string w) => w switch
    {
        "procedure_stats" => ProcedureStatsWindow.RawColumns,
        "query_stats" => QueryStatsWindow.RawColumns,
        _ => QueryStoreWindow.RawColumns,
    };

    private static Func<string, string> TypeOf(string w) => w switch
    {
        "procedure_stats" => ProcedureStatsWindow.DuckDbTypeFor,
        "query_stats" => QueryStatsWindow.DuckDbTypeFor,
        _ => QueryStoreWindow.DuckDbTypeFor,
    };

    private static Task<string> NewBuildAsync(string w, NpgsqlConnectionStringBuilder b) => w switch
    {
        "procedure_stats" => ProcedureStatsWindow.BuildRawWindowFileAsync(b, Options(), default),
        "query_stats" => QueryStatsWindow.BuildRawWindowFileAsync(b, Options(), default),
        _ => QueryStoreWindow.BuildRawWindowFileAsync(b, Options(), default),
    };

    private static void DeleteAny(string p) => DuckDbSession.DeleteTempDatabase(p);

    [Theory]
    [InlineData("procedure_stats")]
    [InlineData("query_stats")]
    [InlineData("query_store")]
    public async Task New_read_path_builds_the_same_table_as_the_legacy_loop(string window)
    {
        if (Pg.Builder is null) { output.WriteLine("Docker not available; skipped."); return; }
        var r = Pg.Psql("TRUNCATE collect.procedure_stats, collect.query_stats, collect.query_store_stats;" + BulkSql(3000) + EdgeSql());
        Assert.True(r.code == 0, r.output);

        var (sql, table, qs) = Windows[window];
        var legacy = await LegacyBuildAsync(Pg.Builder, sql, table, ColumnsOf(window), TypeOf(window), qs, default);
        var fresh = await NewBuildAsync(window, Pg.Builder);
        try { AssertSameTable(legacy, fresh, table); }
        finally { DeleteAny(legacy); DeleteAny(fresh); }
    }

    [Fact]
    public async Task An_append_failure_surfaces_as_itself_not_as_the_read_cancellation()
    {
        // The append task fails at once (its appender targets a table that does not exist), which cancels the
        // still-running read. The caller must see the DuckDB error, not an OperationCanceledException.
        // (A type mismatch inside the appender crashes the native DuckDB layer instead of throwing, so it can't be used here.)
        if (Pg.Builder is null) { output.WriteLine("Docker not available; skipped."); return; }
        var path = Path.Combine(Path.GetTempPath(), $"darling-export-appendfail-{Guid.NewGuid():N}.duckdb");
        try
        {
            await using var pg = new NpgsqlConnection(Pg.Builder.ConnectionString);
            await pg.OpenAsync();
            await using var cmd = new NpgsqlCommand("SELECT g::bigint FROM generate_series(1, 2000000) g", pg);
            await using var reader = await cmd.ExecuteReaderAsync(RawWindowAppender.Behavior);
            using var duck = DuckDbSession.Open(path);
            var ex = await Record.ExceptionAsync(() => RawWindowAppender.AppendAllAsync(reader, duck, "no_such_table", ["BIGINT"], default));
            Assert.NotNull(ex);
            Assert.False(ex is OperationCanceledException, $"got the read cancellation instead of the append failure: {ex}");
        }
        finally { DeleteAny(path); }
    }

    [Fact]
    public async Task Benchmark_old_vs_new_read_paths()
    {
        if (Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_BENCH") != "1" || Pg.Builder is null) { return; }
        var rows = int.Parse(Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_BENCH_ROWS") ?? "400000", CultureInfo.InvariantCulture);
        var runs = int.Parse(Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_BENCH_RUNS") ?? "5", CultureInfo.InvariantCulture);
        var r = Pg.Psql("TRUNCATE collect.procedure_stats, collect.query_stats, collect.query_store_stats;" + BulkSql(rows) + "ANALYZE;");
        Assert.True(r.code == 0, r.output);

        static double Median(List<double> v) { v.Sort(); return v[v.Count / 2]; }
        output.WriteLine($"rows per table: {rows}; runs: {runs} (first run is warm-up, not counted); median wall ms. Old full = store waits the whole time; new store-facing = socket drain only");
        output.WriteLine("window | old full | old read-only | new full | new socket-drain only (store-facing) | new read-only");
        foreach (var (w, (sql, table, qs)) in Windows)
        {
            var oldFull = new List<double>(); var oldRead = new List<double>(); var newFull = new List<double>(); var newDrain = new List<double>(); var newRead = new List<double>();
            for (var i = 0; i <= runs; i++)
            {
                var sw = Stopwatch.StartNew();
                var p1 = await LegacyBuildAsync(Pg.Builder, sql, table, ColumnsOf(w), TypeOf(w), qs, default);
                var t1 = sw.Elapsed.TotalMilliseconds; DeleteAny(p1);
                var (n1, t2) = await ReadOnlyAsync(Pg.Builder, sql, ColumnsOf(w).Count, qs, false, default);
                sw.Restart();
                var p2 = await NewBuildAsync(w, Pg.Builder);
                var t3 = sw.Elapsed.TotalMilliseconds; var d3 = RawWindowAppender.LastDrainMs; DeleteAny(p2);
                var (n2, t4) = await ReadOnlyAsync(Pg.Builder, sql, ColumnsOf(w).Count, qs, true, default);
                Assert.Equal(n1, n2);
                if (i > 0) { oldFull.Add(t1); oldRead.Add(t2); newFull.Add(t3); newDrain.Add(d3); newRead.Add(t4); }
            }
            output.WriteLine($"{w} ({rows} rows) | {Median(oldFull):F0} | {Median(oldRead):F0} | {Median(newFull):F0} | {Median(newDrain):F0} | {Median(newRead):F0}");
        }
    }
}
