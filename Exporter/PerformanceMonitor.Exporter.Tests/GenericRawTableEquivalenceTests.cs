using System.Diagnostics;
using System.Globalization;
using DuckDB.NET.Data;
using Npgsql;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Load-bearing equivalence + benchmark for the generic raw-table export (TableExporter). Needs Docker; with no
/// Docker the tests return without asserting. The legacy read loop (boxed GetValue + per-cell type-name switch,
/// default reader behaviour, inline append) lives here as the oracle only.
/// Benchmark: <c>DARLING_EXPORT_TEST_BENCH=1 dotnet test Exporter/PerformanceMonitor.Exporter.Tests -c Release --filter "FullyQualifiedName~GenericRawTableEquivalence" --logger "console;verbosity=detailed"</c>
/// (rows: DARLING_EXPORT_TEST_BENCH_ROWS_PERFMON default 143000, DARLING_EXPORT_TEST_BENCH_ROWS_SPIN default 100000; runs: DARLING_EXPORT_TEST_BENCH_RUNS, default 5).
/// </summary>
[Trait("Category", "Slow")]
public sealed class GenericRawTableEquivalenceTests : IClassFixture<GenericRawTableEquivalenceTests.GenericPgFixture>
{
    private static readonly DateTime From = new(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime To = From.AddHours(1);

    private static ExportOptions Options(string outDir) => new()
    {
        Store = "bench", FromUtc = From, ToUtc = To, OutDir = outDir, Tables = [],
    };

    private const string Ddl = """
        CREATE SCHEMA collect;
        CREATE TABLE collect.all_types_probe (
          collection_id bigint, collection_time timestamp NOT NULL, server_id int NOT NULL, server_name text NOT NULL,
          c_bool boolean, c_int2 smallint, c_int4 int, c_int8 bigint, c_real real, c_double double precision,
          c_num_18_2 numeric(18,2), c_num_10 numeric(10), c_num_unc numeric, c_num_40_5 numeric(40,5),
          c_text text, c_varchar varchar, c_char name, c_jsonb jsonb, c_bytea bytea,
          c_ts timestamp, c_tstz timestamptz, c_date date, c_uuid uuid);
        CREATE INDEX ON collect.all_types_probe (collection_time);
        CREATE TABLE collect.perfmon_probe (
          collection_id bigint, collection_time timestamp NOT NULL, server_id int NOT NULL, server_name text NOT NULL,
          object_name text, counter_name text, instance_name text, cntr_value bigint, cntr_type int,
          cntr_value_delta bigint, sample_interval_seconds int, cntr_value_per_second numeric(33,12));
        CREATE INDEX ON collect.perfmon_probe (collection_time);
        CREATE TABLE collect.spinlock_probe (
          collection_id bigint, collection_time timestamp NOT NULL, server_id int NOT NULL, server_name text NOT NULL,
          spinlock_name text, collisions bigint, spins bigint, spins_per_collision real, sleep_time bigint, backoffs int,
          collisions_delta bigint, spins_delta bigint, sleep_time_delta bigint, backoffs_delta bigint,
          sample_interval_seconds int, spins_per_second double precision);
        CREATE INDEX ON collect.spinlock_probe (collection_time);
        """;

    public sealed class GenericPgFixture : IDisposable
    {
        private readonly string _container = "darling-export-generic-" + Guid.NewGuid().ToString("N")[..8];
        public NpgsqlConnectionStringBuilder? Builder { get; }

        public GenericPgFixture()
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
            var r = Psql(Ddl);
            Assert.True(r.code == 0, r.output);
        }

        public void Dispose()
        {
            try { Sh("docker", $"rm -f {_container}"); } catch (Exception) { /* best effort */ }
        }
    }

    private readonly ITestOutputHelper output;
    private GenericPgFixture Pg { get; }

    public GenericRawTableEquivalenceTests(ITestOutputHelper output, GenericPgFixture pg)
    {
        this.output = output;
        Pg = pg;
    }

    private const string Inside = "timestamp '2026-10-22 11:00:00' + (i % 3500) * interval '1 second' + (i % 997) * interval '1 microsecond'";

    private static string AllTypesSql(int n) => $$"""
        INSERT INTO collect.all_types_probe
        SELECT i, {{Inside}}, 1 + i % 3, 'srv' || (i % 7),
          CASE WHEN i % 5 = 0 THEN NULL ELSE i % 2 = 0 END,
          CASE WHEN i % 7 = 0 THEN NULL ELSE ((i % 65535) - 32768)::smallint END,
          CASE WHEN i % 11 = 0 THEN NULL ELSE (i * 7919 % 4000000000 - 2000000000)::int END,
          CASE WHEN i % 13 = 0 THEN NULL ELSE i * 1000003 * (CASE WHEN i % 2 = 0 THEN 1 ELSE -1 END) END,
          CASE WHEN i % 17 = 0 THEN NULL ELSE (i / 7.0)::real END,
          CASE WHEN i % 19 = 0 THEN NULL ELSE i / 7.0 END,
          CASE WHEN i % 23 = 0 THEN NULL ELSE (i / 7.0)::numeric(18,2) END,
          CASE WHEN i % 29 = 0 THEN NULL ELSE (i * 31)::numeric(10) END,
          CASE WHEN i % 31 = 0 THEN NULL ELSE (i / 3.0)::numeric END,
          CASE WHEN i % 37 = 0 THEN NULL ELSE (i / 3.0)::numeric(40,5) END,
          CASE WHEN i % 3 = 0 THEN NULL ELSE 'text_' || (i % 300) END,
          CASE WHEN i % 41 = 0 THEN NULL ELSE 'v' || i END,
          CASE WHEN i % 43 = 0 THEN NULL ELSE chr(97 + (i % 26)::int) END,
          CASE WHEN i % 47 = 0 THEN NULL ELSE ('{"k": ' || i || '}')::jsonb END,
          CASE WHEN i % 9 = 0 THEN NULL ELSE decode(md5(i::text), 'hex') END,
          CASE WHEN i % 53 = 0 THEN NULL ELSE timestamp '2026-10-20 00:00:00' + i * interval '1 microsecond' END,
          CASE WHEN i % 59 = 0 THEN NULL ELSE timestamptz '2026-10-20 00:00:00+00' + i * interval '1 microsecond' END,
          CASE WHEN i % 61 = 0 THEN NULL ELSE date '2026-01-01' + (i % 400)::int END,
          CASE WHEN i % 67 = 0 THEN NULL ELSE md5(i::text)::uuid END
        FROM generate_series(1::bigint, {{n}}::bigint) i;
        INSERT INTO collect.all_types_probe VALUES
         (-1, '2026-10-22 11:00:00', 1, '', true, -32768, -2147483648, -9223372036854775808, -3.4028235e38, -1.7976931348623157e308,
          -9999999999999999.99, -9999999999, -123456789012345.678901, -1234567890123456789012.12345,
          '', '', ' ', '""', '\x', '2026-10-22 11:00:00.000001', '2026-10-22 11:00:00.000001+00', '0001-01-01', '00000000-0000-0000-0000-000000000000'),
         (-2, '2026-10-22 11:59:59.999999', 2, 'sérvér-日本語-🦆', false, 32767, 2147483647, 9223372036854775807, 3.4028235e38, 1.7976931348623157e308,
          9999999999999999.99, 9999999999, 123456789012345.678901, 1234567890123456789012.12345,
          'wïde-日本語-🦆', 'dä', 'é', '{"é": "日"}', '\x00ff80', '2026-10-22 11:59:59.999999', '2026-10-22 11:59:59.999999+00', '9999-12-31', 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
         (-3, '2026-10-22 11:30:00.123457', 3, 'n', NULL, NULL, NULL, NULL, 'NaN', 'Infinity',
          NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
         (-4, '2026-10-22 11:30:00.5', 3, 'n', NULL, 0, 0, 0, '-Infinity', 'NaN',
          0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
        """;

    private static string PerfmonSql(int n) => $$"""
        INSERT INTO collect.perfmon_probe
        SELECT i, {{Inside}}, 1, 'srv' || (i % 3), 'SQLServer:' || (i % 40), 'counter ' || (i % 300), CASE WHEN i % 5 = 0 THEN NULL ELSE 'inst' || (i % 20) END,
               i * 13, 65792 + i % 4, i % 1000, 60 + i % 3, (i % 1000) / 60.0
        FROM generate_series(1::bigint, {{n}}::bigint) i;
        """;

    private static string SpinSql(int n) => $$"""
        INSERT INTO collect.spinlock_probe
        SELECT i, {{Inside}}, 1, 'srv' || (i % 3), 'LOCK_' || (i % 200), i * 5, i * 11, (i % 100) / 3.0, i % 9000, i % 50,
               i % 77, i % 1234, i % 99, i % 7, 60 + i % 3, (i % 1234) / 60.0
        FROM generate_series(1::bigint, {{n}}::bigint) i;
        """;

    private static IReadOnlyList<string> Columns(NpgsqlConnectionStringBuilder b, string table)
    {
        using var c = new NpgsqlConnection(b.ConnectionString);
        c.Open();
        using var cmd = new NpgsqlCommand(SqlBuilder.RawColumnDiscoverySql, c);
        cmd.Parameters.AddWithValue("table", table);
        using var r = cmd.ExecuteReader();
        var l = new List<string>();
        while (r.Read()) { l.Add(r.GetString(0)); }
        return l;
    }

    // ---- legacy oracle: the pre-change TableExporter read loop, verbatim in behaviour ----
    private static async Task<(string path, List<ColumnShape> cols, double ms)> LegacyBuildAsync(
        NpgsqlConnectionStringBuilder b, string table, IReadOnlyList<string> raw, string store, Func<string, string>? mapType = null)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-export-legacy-{Guid.NewGuid():N}.duckdb");
        var sw = Stopwatch.StartNew();
        await using var pg = await StoreConnectionFactory.OpenAsync(b, default);
        await using var command = new NpgsqlCommand(SqlBuilder.BuildSelect(table, raw), pg);
        command.Parameters.AddRange(SqlBuilder.WindowParameters(From, To));
        await using var reader = await command.ExecuteReaderAsync(default);
        var cols = new List<ColumnShape> { new("source_store", "VARCHAR") };
        for (var i = 0; i < reader.FieldCount; i++)
        {
            cols.Add(new ColumnShape(reader.GetName(i), DuckDbTypeMapper.Map(reader.GetName(i), reader.GetDataTypeName(i))));
        }
        using var duck = DuckDbSession.Open(path);
        using (var create = duck.CreateCommand())
        {
            create.CommandText = $"CREATE TABLE \"{table}\" ({string.Join(", ", cols.Select(c => $"\"{c.Name}\" {c.DuckDbType}"))})";
            create.ExecuteNonQuery();
        }
        using (var appender = duck.CreateAppender(table))
        {
            while (reader.Read())
            {
                var row = appender.CreateRow();
                row.AppendValue(store);
                for (var i = 0; i < reader.FieldCount; i++) { LegacyAppend(row, reader, i, cols[i + 1].DuckDbType); }
                row.EndRow();
            }
        }
        return (path, cols, sw.Elapsed.TotalMilliseconds);
    }

    private static void LegacyAppend(IDuckDBAppenderRow row, System.Data.IDataReader reader, int ordinal, string duckDbType)
    {
        if (reader.IsDBNull(ordinal)) { row.AppendNullValue(); return; }
        switch (duckDbType)
        {
            case "BOOLEAN": row.AppendValue(reader.GetBoolean(ordinal)); break;
            case "SMALLINT": row.AppendValue(Convert.ToInt16(reader.GetValue(ordinal))); break;
            case "INTEGER": row.AppendValue(Convert.ToInt32(reader.GetValue(ordinal))); break;
            case "BIGINT": row.AppendValue(Convert.ToInt64(reader.GetValue(ordinal))); break;
            case "REAL": row.AppendValue(Convert.ToSingle(reader.GetValue(ordinal))); break;
            case "DOUBLE": row.AppendValue(Convert.ToDouble(reader.GetValue(ordinal))); break;
            case "VARCHAR": row.AppendValue(reader.GetValue(ordinal)?.ToString()); break;
            case "BLOB": row.AppendValue((byte[])reader.GetValue(ordinal)); break;
            case "TIMESTAMP": case "TIMESTAMPTZ": row.AppendValue(reader.GetDateTime(ordinal)); break;
            case "DATE": row.AppendValue(DateOnly.FromDateTime(reader.GetDateTime(ordinal))); break;
            case "UUID": row.AppendValue(reader.GetGuid(ordinal)); break;
            case var d when d.StartsWith("DECIMAL(", StringComparison.Ordinal): row.AppendValue(reader.GetDecimal(ordinal)); break;
            default: throw new NotSupportedException(duckDbType);
        }
    }

    private static async Task<(long rows, double ms)> DrainOnlyAsync(NpgsqlConnectionStringBuilder b, string table, IReadOnlyList<string> raw)
    {
        var sw = Stopwatch.StartNew();
        await using var pg = await StoreConnectionFactory.OpenAsync(b, default);
        await using var command = new NpgsqlCommand(SqlBuilder.BuildSelect(table, raw), pg);
        command.Parameters.AddRange(SqlBuilder.WindowParameters(From, To));
        await using var reader = await command.ExecuteReaderAsync(RawWindowAppender.Behavior);
        var types = Enumerable.Range(0, reader.FieldCount).Select(reader.GetFieldType).ToArray();
        long n = 0, sink = 0;
        while (await reader.ReadAsync())
        {
            n++;
            for (var i = 0; i < types.Length; i++)
            {
                if (reader.IsDBNull(i)) { continue; }
                var t = types[i];
                if (t == typeof(long)) { sink += reader.GetInt64(i); }
                else if (t == typeof(int)) { sink += reader.GetInt32(i); }
                else if (t == typeof(short)) { sink += reader.GetInt16(i); }
                else if (t == typeof(float)) { sink += (long)reader.GetFloat(i); }
                else if (t == typeof(double)) { sink += (long)reader.GetDouble(i); }
                else if (t == typeof(decimal)) { sink += (long)reader.GetDecimal(i); }
                else if (t == typeof(string)) { sink += reader.GetString(i).Length; }
                else if (t == typeof(DateTime)) { sink += reader.GetDateTime(i).Ticks; }
                else if (t == typeof(byte[])) { sink += reader.GetFieldValue<byte[]>(i).Length; }
                else if (t == typeof(bool)) { sink += reader.GetBoolean(i) ? 1 : 0; }
                else if (t == typeof(Guid)) { sink += reader.GetGuid(i).GetHashCode(); }
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

    private static List<string> TypesOf(DuckDBConnection c, string from)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = $"SELECT column_name || ':' || column_type FROM (DESCRIBE SELECT * FROM {from})";
        using var r = cmd.ExecuteReader();
        var l = new List<string>();
        while (r.Read()) { l.Add(r.GetString(0)); }
        return l;
    }

    /// <summary>Row-for-row equality of the legacy DuckDB table and the new path's Parquet output.</summary>
    private static void AssertSame(string legacyDb, string parquet, string table)
    {
        using var duck = new DuckDBConnection($"Data Source={legacyDb}");
        duck.Open();
        var np = $"read_parquet('{parquet.Replace("'", "''")}')";
        var legacyCount = Scalar(duck, $"SELECT count(*) FROM main.\"{table}\"");
        Assert.True(legacyCount > 3, "fixture produced no rows");
        Assert.Equal(legacyCount, Scalar(duck, $"SELECT count(*) FROM {np}"));
        Assert.Equal(0, Scalar(duck, $"SELECT count(*) FROM (SELECT * FROM main.\"{table}\" EXCEPT ALL SELECT * FROM {np})"));
        Assert.Equal(0, Scalar(duck, $"SELECT count(*) FROM (SELECT * FROM {np} EXCEPT ALL SELECT * FROM main.\"{table}\")"));
        Assert.Equal(TypesOf(duck, $"main.\"{table}\""), TypesOf(duck, np));
    }

    [Fact]
    public async Task New_generic_export_matches_the_legacy_loop_for_every_supported_type()
    {
        if (Pg.Builder is null) { output.WriteLine("Docker not available; skipped."); return; }
        var r = Pg.Psql("TRUNCATE collect.all_types_probe;" + AllTypesSql(3000));
        Assert.True(r.code == 0, r.output);
        await CompareAsync("all_types_probe");
    }

    [Theory]
    [InlineData("perfmon_probe")]
    [InlineData("spinlock_probe")]
    public async Task New_generic_export_matches_the_legacy_loop_for_the_shaped_tables(string table)
    {
        if (Pg.Builder is null) { output.WriteLine("Docker not available; skipped."); return; }
        var r = Pg.Psql($"TRUNCATE collect.{table};" + (table == "perfmon_probe" ? PerfmonSql(5000) : SpinSql(5000)));
        Assert.True(r.code == 0, r.output);
        await CompareAsync(table);
    }

    private async Task CompareAsync(string table)
    {
        var raw = Columns(Pg.Builder!, table);
        var outDir = Path.Combine(Path.GetTempPath(), "darling-export-gen-" + Guid.NewGuid().ToString("N"));
        var (legacy, _, _) = await LegacyBuildAsync(Pg.Builder!, table, raw, "bench");
        try
        {
            var entry = await TableExporter.ExportAsync(Pg.Builder!, Options(outDir), table, raw, default);
            var parquet = OutputPaths.ParquetFilePath(outDir, "bench", table, From, To);
            AssertSame(legacy, parquet, table);
        }
        finally
        {
            DuckDbSession.DeleteTempDatabase(legacy);
            try { Directory.Delete(outDir, true); } catch (IOException) { }
        }
    }

    [Fact]
    public async Task Benchmark_old_vs_new_generic_export()
    {
        if (Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_BENCH") != "1" || Pg.Builder is null) { return; }
        var perfRows = int.Parse(Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_BENCH_ROWS_PERFMON") ?? "143000", CultureInfo.InvariantCulture);
        var spinRows = int.Parse(Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_BENCH_ROWS_SPIN") ?? "100000", CultureInfo.InvariantCulture);
        var runs = int.Parse(Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_BENCH_RUNS") ?? "5", CultureInfo.InvariantCulture);
        var r = Pg.Psql("TRUNCATE collect.perfmon_probe, collect.spinlock_probe;" + PerfmonSql(perfRows) + SpinSql(spinRows) + "ANALYZE;");
        Assert.True(r.code == 0, r.output);

        static double Median(List<double> v) { v.Sort(); return v[v.Count / 2]; }
        output.WriteLine($"runs: {runs} (first is warm-up, not counted); median wall ms. Old: store waits for the whole read+append. New store-facing = socket drain only (RawWindowAppender.LastDrainMs).");
        output.WriteLine("table (rows) | old drain=full | drain only, no append | new store-facing drain | new full (read+append)");
        foreach (var (table, rows) in new[] { ("perfmon_probe", perfRows), ("spinlock_probe", spinRows) })
        {
            var raw = Columns(Pg.Builder, table);
            var oldFull = new List<double>(); var drainOnly = new List<double>(); var newDrain = new List<double>(); var newFull = new List<double>();
            for (var i = 0; i <= runs; i++)
            {
                var (p1, _, t1) = await LegacyBuildAsync(Pg.Builder, table, raw, "bench"); DuckDbSession.DeleteTempDatabase(p1);
                var (n2, t2) = await DrainOnlyAsync(Pg.Builder, table, raw);
                var path = Path.Combine(Path.GetTempPath(), $"darling-export-new-{Guid.NewGuid():N}.duckdb");
                var sw = Stopwatch.StartNew();
                long n3;
                await using (var pg = await StoreConnectionFactory.OpenAsync(Pg.Builder, default))
                {
                    await using var command = new NpgsqlCommand(SqlBuilder.BuildSelect(table, raw), pg);
                    command.Parameters.AddRange(SqlBuilder.WindowParameters(From, To));
                    await using var reader = await command.ExecuteReaderAsync(RawWindowAppender.Behavior);
                    var cols = new List<ColumnShape> { new("source_store", "VARCHAR") };
                    for (var c = 0; c < reader.FieldCount; c++) { cols.Add(new ColumnShape(reader.GetName(c), DuckDbTypeMapper.Map(reader.GetName(c), reader.GetDataTypeName(c)))); }
                    using var duck = DuckDbSession.Open(path);
                    using (var create = duck.CreateCommand())
                    {
                        create.CommandText = $"CREATE TABLE \"{table}\" ({string.Join(", ", cols.Select(c => $"\"{c.Name}\" {c.DuckDbType}"))})";
                        create.ExecuteNonQuery();
                    }
                    n3 = await RawWindowAppender.AppendAllAsync(reader, duck, table, [.. cols.Skip(1).Select(c => c.DuckDbType)], default, "bench");
                }
                var t3 = sw.Elapsed.TotalMilliseconds; var d3 = RawWindowAppender.LastDrainMs; DuckDbSession.DeleteTempDatabase(path);
                Assert.Equal(n2, n3);
                if (i > 0) { oldFull.Add(t1); drainOnly.Add(t2); newDrain.Add(d3); newFull.Add(t3); }
            }
            output.WriteLine($"{table} ({rows}) | {Median(oldFull):F0} | {Median(drainOnly):F0} | {Median(newDrain):F0} | {Median(newFull):F0}");
        }
    }
}
