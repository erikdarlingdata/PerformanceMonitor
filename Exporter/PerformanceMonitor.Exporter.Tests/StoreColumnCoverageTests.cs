using DuckDB.NET.Data;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// The guard against a silent drop. Every hand-written store read in the exporter names its columns, so a column a later
/// store rung adds is left out of the export without any error, and the history of it cannot be backfilled. This builds a
/// real store schema from the migrations (<see cref="PgMigrations.MigrateAsync(NpgsqlConnection, CancellationToken)"/>, on a throwaway PostgreSQL
/// in Docker, the same container fixture the other store-read tests use), then for each hand-written read compares the
/// source table's columns, from pg_attribute, with the columns the export writes. Every store column must be exported or
/// be on <see cref="DroppedStoreColumns"/> with a reason. A new store column that nobody classified fails here.
/// Needs Docker; with no Docker each test returns without asserting, like the other Docker-backed suites.
/// </summary>
[Trait("Category", "Slow")]
public sealed class StoreColumnCoverageTests : IClassFixture<StoreColumnCoverageTests.MigratedStore>
{
    private static readonly DateTime From = new(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime To = From.AddHours(1);

    /// <summary>A PostgreSQL whose `collect` and `config` schemas come from the store's own migration ladder.</summary>
    public sealed class MigratedStore : IDisposable
    {
        private readonly GenericRawTableEquivalenceTests.GenericPgFixture pg = new();

        public NpgsqlConnectionStringBuilder? Builder { get; }

        public MigratedStore()
        {
            if (pg.Builder is null) { return; }

            // A database of its own: the container's default one already holds the hand-made schemas of the other tests.
            var created = pg.Psql("CREATE DATABASE migrated_store;");
            Assert.True(created.code == 0, created.output);
            var builder = new NpgsqlConnectionStringBuilder(pg.Builder.ConnectionString) { Database = "migrated_store" };
            using var connection = new NpgsqlConnection(builder.ConnectionString);
            connection.Open();
            PgMigrations.MigrateAsync(connection).GetAwaiter().GetResult();
            Builder = builder;
        }

        public void Dispose() => pg.Dispose();
    }

    private readonly MigratedStore _store;

    public StoreColumnCoverageTests(MigratedStore store) => _store = store;

    // ---- the one place that says what is left out on purpose ---------------------------------------------------------

    /// <summary>
    /// Store columns the export leaves out on purpose, by store table, each with its reason. A column belongs here only when
    /// the export carries what it says some other way, or it is bookkeeping with no meaning outside the store. Real data that
    /// cannot be derived from what is exported goes into the export instead. "*" applies to every table that has the column.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> DroppedStoreColumns =
        new Dictionary<string, IReadOnlyDictionary<string, string>>
        {
            ["*"] = new Dictionary<string, string>
            {
                ["server_id"] = "local to one store; server_name carries the server's identity in every export",
                ["collection_id"] = "store-local row id of the collection sweep; no meaning outside the store",
            },
            ["query_stats"] = new Dictionary<string, string>
            {
                ["query_text"] = "carried by the query_text dimension through query_text_digest, which is read",
                ["query_plan_xml"] = "carried by the plan_shape dimension and plan_features through query_plan_digest, which is read",
            },
            ["procedure_stats"] = new Dictionary<string, string>
            {
                ["query_plan_xml"] = "carried by the procedure_plan dimension through query_plan_digest, which is read; the window reads only its nullness (plan_xml_is_null)",
            },
            ["query_store_stats"] = new Dictionary<string, string>
            {
                ["query_text"] = "always NULL in this table; the text is in query_store_text and reaches the query_text dimension",
                ["query_plan_text"] = "always NULL in this table; the plan is in query_store_plan_map and reaches the plan_shape dimension",
            },
            ["pg_statement_stats"] = new Dictionary<string, string>
            {
                ["mean_exec_time_ms"] = "derivable: total_exec_time_ms_last / calls_last, from the counters the rollup exports",
            },
            ["ag_database_replica_states"] = new Dictionary<string, string>
            {
                ["group_id"] = "an identifier that is 1:1 with ag_name, which is part of the rollup's key",
            },
            ["query_store_interval_wide"] = new Dictionary<string, string>
            {
                ["query_text"] = "always NULL in this table; the text is in query_store_text and reaches the query_text dimension",
            },
        };

    /// <summary>
    /// Columns a window read carries that the hourly rollup built over it does not write, with the reason. The window feeds more
    /// than the hourly table: the dimension keys and the oversized-plan count read it too.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> WindowOnlyColumns =
        new Dictionary<string, IReadOnlyDictionary<string, string>>
        {
            ["query_stats"] = new Dictionary<string, string>
            {
                ["query_plan_digest"] = "the key of the plan_shape dimension; the hourly row is keyed by query_plan_hash",
                ["query_text_digest"] = "the key of the query_text dimension; the hourly row is keyed by query_hash",
            },
            ["procedure_stats"] = new Dictionary<string, string>
            {
                ["query_plan_digest"] = "the key of the procedure_plan dimension",
                ["plan_xml_is_null"] = "a flag computed in the read (not a store column) for the oversized-plan count",
                ["has_plan_xml_bytes"] = "a flag computed in the read (not a store column) for the oversized-plan count",
            },
        };

    // ---- the reads ---------------------------------------------------------------------------------------------------

    private sealed record Read(string Name, string StoreTable, Func<NpgsqlCommand, NpgsqlCommand> Bind);

    private static NpgsqlCommand Window(NpgsqlCommand c, bool plus2h = false)
    {
        c.Parameters.AddRange(SqlBuilder.WindowParameters(From, To));
        if (plus2h)
        {
            c.Parameters.Add(new NpgsqlParameter("to_plus_2h", NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(To.AddHours(2), DateTimeKind.Unspecified) });
        }

        return c;
    }

    private static readonly Dictionary<string, (string Sql, string Table, Func<NpgsqlCommand, NpgsqlCommand> Bind)> HandWrittenReads = new()
    {
        ["query_stats.window"] = (SqlBuilder.QueryStatsWindowSql, "query_stats", c => Window(c)),
        ["procedure_stats.window"] = (SqlBuilder.ProcedureStatsWindowSql, "procedure_stats", c => Window(c)),
        ["query_store_stats.window"] = (SqlBuilder.QueryStoreStatsRawSql, "query_store_stats", c => Window(c, plus2h: true)),
        ["query_store_stats.wide"] = (SqlBuilder.QueryStoreStatsWideSql, "query_store_interval_wide", c => Window(c)),
        ["pg_statement_text.batch"] = (SqlBuilder.PgStatementTextBatchSql, "pg_statement_text", c =>
        {
            c.Parameters.Add(new NpgsqlParameter("sids", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = new List<int>() });
            c.Parameters.Add(new NpgsqlParameter("qids", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = new List<long>() });
            return c;
        }),
    };

    public static IEnumerable<object[]> HandWrittenReadNames() => HandWrittenReads.Keys.Select(k => new object[] { k });

    public static IEnumerable<object[]> RollupSpecNames() => RollupSql.Specs.Keys.Select(k => new object[] { k });

    // ---- helpers -----------------------------------------------------------------------------------------------------

    private async Task<List<string>> StoreColumnsAsync(string table)
    {
        await using var connection = new NpgsqlConnection(_store.Builder!.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT a.attname FROM pg_attribute a WHERE a.attrelid = to_regclass(@name) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", connection);
        command.Parameters.AddWithValue("name", "collect." + table);
        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns.Add(reader.GetString(0));
        }

        Assert.True(columns.Count > 0, $"collect.{table} is not in the migrated store");
        return columns;
    }

    private async Task<List<string>> ReadColumnsAsync(string readName)
    {
        var (sql, _, bind) = HandWrittenReads[readName];
        await using var connection = new NpgsqlConnection(_store.Builder!.ConnectionString);
        await connection.OpenAsync();
        // The migrated store is empty: the read returns no rows, and the reader still names the columns it would return.
        await using var command = bind(new NpgsqlCommand(sql, connection));
        await using var reader = await command.ExecuteReaderAsync();
        return Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
    }

    private static string? DroppedReason(string table, string column) =>
        DroppedStoreColumns.TryGetValue(table, out var own) && own.TryGetValue(column, out var reason) ? reason
        : DroppedStoreColumns["*"].GetValueOrDefault(column);

    private static void AssertEveryStoreColumnIsClassified(string what, string table, IEnumerable<string> storeColumns, IEnumerable<string> exported)
    {
        var exportedSet = exported.ToHashSet(StringComparer.Ordinal);
        var unclassified = storeColumns.Where(c => !exportedSet.Contains(c) && DroppedReason(table, c) is null).ToList();
        Assert.True(unclassified.Count == 0,
            $"{what}: collect.{table} has column(s) the export neither writes nor lists in DroppedStoreColumns with a reason: " +
            string.Join(", ", unclassified) + ". Export them, or add them to DroppedStoreColumns if they are truly redundant.");
    }

    // ---- the guard ---------------------------------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(HandWrittenReadNames))]
    public async Task Every_store_column_of_a_hand_written_read_is_exported_or_dropped_on_purpose(string readName)
    {
        if (_store.Builder is null) { return; }

        var table = HandWrittenReads[readName].Table;
        AssertEveryStoreColumnIsClassified(readName, table, await StoreColumnsAsync(table), await ReadColumnsAsync(readName));
    }

    [Theory]
    [InlineData("query_stats", HourlyTables.QueryStatsHourly)]
    [InlineData("procedure_stats", HourlyTables.ProcedureStatsHourly)]
    public async Task Every_column_the_window_read_carries_reaches_the_hourly_table_or_is_window_only(string storeTable, string hourlyTable)
    {
        if (_store.Builder is null) { return; }

        // The hourly table is built in DuckDB over the window's raw table, so run its rollup over an empty one and read its columns.
        var (rawColumns, typeFor, rawTable, rollupSql) = storeTable == "query_stats"
            ? (QueryStatsWindow.RawColumns, (Func<string, string>)QueryStatsWindow.DuckDbTypeFor, "query_stats_raw", SqlBuilder.QueryStatsHourlyDuckDbSql)
            : (ProcedureStatsWindow.RawColumns, ProcedureStatsWindow.DuckDbTypeFor, "procedure_stats_raw", SqlBuilder.ProcedureStatsHourlyDuckDbSql);

        Assert.Equal(rawColumns.OrderBy(c => c), (await ReadColumnsAsync($"{storeTable}.window")).OrderBy(c => c));

        using var duck = new DuckDBConnection("Data Source=:memory:");
        duck.Open();
        Exec(duck, $"CREATE TABLE {rawTable} ({string.Join(", ", rawColumns.Select(c => $"\"{c}\" {typeFor(c)}"))})");
        using var describe = duck.CreateCommand();
        describe.CommandText = $"SELECT column_name FROM (DESCRIBE {rollupSql})";
        var output = new List<string>();
        using (var reader = describe.ExecuteReader())
        {
            while (reader.Read()) { output.Add(reader.GetString(0)); }
        }

        var windowOnly = WindowOnlyColumns[storeTable];
        var lost = rawColumns.Where(c => !windowOnly.ContainsKey(c) && !CarriedBy(c, output)).ToList();
        Assert.True(lost.Count == 0,
            $"{hourlyTable}: the window read carries column(s) the hourly rollup drops, and WindowOnlyColumns does not list them: {string.Join(", ", lost)}");
    }

    /// <summary>A window column reaches the hourly table under its own name, as `x_sum` for `delta_x` or `x`, or as `hour` for the time.</summary>
    private static bool CarriedBy(string windowColumn, List<string> output) =>
        output.Contains(windowColumn)
        || output.Contains(windowColumn + "_sum")
        || (windowColumn.StartsWith("delta_", StringComparison.Ordinal) && output.Contains(windowColumn["delta_".Length..] + "_sum"))
        || (windowColumn == "collection_time" && output.Contains("hour"));

    [Theory]
    [MemberData(nameof(RollupSpecNames))]
    public async Task Every_store_column_of_a_rollup_source_is_rolled_up_or_dropped_on_purpose(string specName)
    {
        if (_store.Builder is null) { return; }

        var spec = RollupSql.Specs[specName];
        var table = spec.SourceTable.Replace("collect.", "", StringComparison.Ordinal);

        // What a spec reads: its server and time columns, its key, the interval a delta rule is conditioned on, and each rule's columns.
        var read = new List<string> { "server_name", spec.TimeColumn };
        read.AddRange(spec.Key);
        if (spec.HasSampleInterval) { read.Add("sample_interval_seconds"); }
        foreach (var rule in spec.Rules)
        {
            read.AddRange(rule.Columns.Select(entry => entry.Contains('=') ? entry[..entry.IndexOf('=')] : entry));
        }

        AssertEveryStoreColumnIsClassified(specName, table, await StoreColumnsAsync(table), read);
    }

    [Fact]
    public async Task Every_dropped_column_is_still_a_column_of_its_store_table()
    {
        if (_store.Builder is null) { return; }

        // A stale entry would hide the next column that takes the name, so each entry must still name a real column.
        foreach (var (table, columns) in DroppedStoreColumns.Where(t => t.Key != "*"))
        {
            var storeColumns = await StoreColumnsAsync(table);
            foreach (var (column, reason) in columns)
            {
                Assert.Contains(column, storeColumns);
                Assert.False(string.IsNullOrWhiteSpace(reason), $"{table}.{column} has no reason");
            }
        }

        foreach (var (column, reason) in DroppedStoreColumns["*"])
        {
            Assert.False(string.IsNullOrWhiteSpace(reason), $"*.{column} has no reason");
        }
    }

    // ---- the hourly rollup, from the real store schema ---------------------------------------------------------------

    /// <summary>
    /// The store-side hourly query is the oracle for the DuckDB rollup: both must give the same rows and the same cells from the
    /// same store rows. Here the rows are in the migrated store (so the column types are the store's own), the DuckDB side is
    /// loaded by the real window read, and the samples differ in every attribute so a wrong pick shows.
    /// </summary>
    [Theory]
    [InlineData("query_stats", new[] { "server_name", "database_name", "query_hash", "query_plan_hash", "host_object_name", "hour" })]
    [InlineData("procedure_stats", new[] { "server_name", "database_name", "schema_name", "object_name", "hour" })]
    public async Task The_duckdb_hourly_rollup_gives_the_same_rows_as_the_store_side_oracle_from_the_migrated_schema(string table, string[] keyColumns)
    {
        if (_store.Builder is null) { return; }

        var isQueryStats = table == "query_stats";
        var rawColumns = isQueryStats ? QueryStatsWindow.RawColumns : ProcedureStatsWindow.RawColumns;
        Func<string, string> typeFor = isQueryStats ? QueryStatsWindow.DuckDbTypeFor : ProcedureStatsWindow.DuckDbTypeFor;
        var oracleSql = isQueryStats ? SqlBuilder.QueryStatsHourlySql : SqlBuilder.ProcedureStatsHourlySql;
        var rollupSql = isQueryStats ? SqlBuilder.QueryStatsHourlyDuckDbSql : SqlBuilder.ProcedureStatsHourlyDuckDbSql;
        var storeColumns = await StoreColumnsAsync(table);
        var writable = rawColumns.Where(c => storeColumns.Contains(c) && typeFor(c) != "BLOB" && c != "collection_time" && c != "sample_interval_seconds").ToList();

        // (sample number, key variant, time, interval): variant 0 and 1 are two keys; the 10:40 sample of variant 0 is an interval-0 reset.
        (int N, int Variant, string Time, int Interval)[] samples =
        [
            (3, 0, "2026-10-22 11:40:00", 0),
            (1, 0, "2026-10-22 11:05:00", 60),
            (5, 1, "2026-10-22 11:15:00", 60),
            (2, 0, "2026-10-22 11:20:00", 60),
            (4, 0, "2026-10-22 12:10:00", 60),
            (6, 0, "2026-10-22 11:50:00", 30),
            // Two plans of one group at the same instant (the grain does not tell them apart), in the opposite order of their handles ...
            (8, 0, "2026-10-22 13:30:00", 60),
            (7, 0, "2026-10-22 13:30:00", 60),
            // ... and an hour whose last sample leaves query_plan_xml_bytes NULL, after an older one that has it.
            (9, 0, "2026-10-22 14:45:00", 60),
            (10, 0, "2026-10-22 14:05:00", 60),
        ];
        await using (var connection = new NpgsqlConnection(_store.Builder.ConnectionString))
        {
            await connection.OpenAsync();
            foreach (var (n, variant, time, interval) in samples)
            {
                var values = new List<(string Column, string Literal)>
                {
                    ("collection_id", n.ToString()), ("server_id", "1"), ("server_name", "'parity'"), ("database_name", "'db1'"),
                    ("collection_time", $"'{time}'"), ("sample_interval_seconds", interval.ToString()),
                    ("delta_execution_count", n.ToString()), ("delta_worker_time", (n * 10).ToString()),
                };
                if (isQueryStats)
                {
                    values.AddRange([("query_hash", "'h1'"), ("query_plan_hash", "'p1'"), ("host_object_name", variant == 0 ? "NULL" : "'dbo.usp_x'")]);
                }
                else
                {
                    values.AddRange([("schema_name", "'dbo'"), ("object_name", $"'proc{variant}'")]);
                }

                var named = values.Select(v => v.Column).ToHashSet();
                var index = 0;
                foreach (var column in writable.Where(c => !named.Contains(c) && !c.StartsWith("delta_", StringComparison.Ordinal)))
                {
                    if (n == 9 && column == "query_plan_xml_bytes") { continue; }
                    index++;
                    values.Add((column, typeFor(column) switch
                    {
                        "TIMESTAMP" => $"timestamp '2026-10-01 00:00:00' + interval '{n} hours'",
                        "VARCHAR" => $"'{column}-{n}'",
                        "INTEGER" => n.ToString(),
                        _ => (n * 1000 + index).ToString(),
                    }));
                }

                await using var insert = new NpgsqlCommand(
                    $"INSERT INTO collect.{table} ({string.Join(", ", values.Select(v => v.Column))}) VALUES ({string.Join(", ", values.Select(v => v.Literal))})", connection);
                await insert.ExecuteNonQueryAsync();
            }
        }

        // The oracle, in the store.
        List<Dictionary<string, object?>> oracle = [];
        await using (var connection = new NpgsqlConnection(_store.Builder.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(oracleSql, connection);
            command.Parameters.AddRange(SqlBuilder.WindowParameters(From, From.AddHours(4)));
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                oracle.Add(Enumerable.Range(0, reader.FieldCount).ToDictionary(reader.GetName, i => Normalize(reader.IsDBNull(i) ? null : reader.GetValue(i))));
            }
        }

        // The DuckDB rollup, over the window the real read loads.
        var options = new ExportOptions { Store = "parity", FromUtc = From, ToUtc = From.AddHours(4), OutDir = "x", Tables = [] };
        var path = isQueryStats
            ? await QueryStatsWindow.BuildRawWindowFileAsync(_store.Builder, options, default)
            : await ProcedureStatsWindow.BuildRawWindowFileAsync(_store.Builder, options, default);
        List<Dictionary<string, object?>> duck = [];
        try
        {
            using var connection = DuckDbSession.Open(path);
            using var command = connection.CreateCommand();
            command.CommandText = rollupSql;
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                duck.Add(Enumerable.Range(0, reader.FieldCount).ToDictionary(reader.GetName, i => Normalize(reader.IsDBNull(i) ? null : reader.GetValue(i))));
            }
        }
        finally
        {
            DuckDbSession.DeleteTempDatabase(path);
        }

        string Key(Dictionary<string, object?> row) => string.Join("|", keyColumns.Select(k => row[k] ?? "<null>"));
        Assert.Equal(oracle.Select(Key).Order(), duck.Select(Key).Order());                       // the same rows ...
        Assert.Equal(oracle[0].Keys.Order(), duck[0].Keys.Order());                               // ... with the same columns ...
        foreach (var expected in oracle)
        {
            var actual = duck.Single(d => Key(d) == Key(expected));
            foreach (var (column, value) in expected)
            {
                Assert.True(Equals(value, actual[column]), $"{table} {Key(expected)} {column}: oracle {value ?? "NULL"}, duckdb {actual[column] ?? "NULL"}");   // ... and the same cells
            }
        }

        // And the attributes are the last counted sample's: the 11:50 sample (n = 6) for the 11:00 hour, not the later-numbered but interval-0 11:40 one (n = 3).
        var lastOfHour = duck.Single(d => (DateTime)d["hour"]! == new DateTime(2026, 10, 22, 11, 0, 0) && (isQueryStats ? d["host_object_name"] is null : (string?)d["object_name"] == "proc0"));
        Assert.Equal(6L, (long)lastOfHour["execution_count"]! / 1000);   // the sample number is the thousands of the planted value
        Assert.Equal(3L, lastOfHour["sample_count"]);

        // Two rows at one instant: every attribute is from the one whose handles sort last (n = 8), not a mix with n = 7.
        var tied = duck.Single(d => (DateTime)d["hour"]! == new DateTime(2026, 10, 22, 13, 0, 0) && (isQueryStats ? d["host_object_name"] is null : (string?)d["object_name"] == "proc0"));
        Assert.Equal(2L, tied["sample_count"]);
        Assert.Equal(8L, (long)tied["execution_count"]! / 1000);
        Assert.Equal("plan_handle-8", tied["plan_handle"]);
        Assert.Equal("sql_handle-8", tied["sql_handle"]);

        // The last sample's NULL is the hour's value, not the older sample's.
        var nulled = duck.Single(d => (DateTime)d["hour"]! == new DateTime(2026, 10, 22, 14, 0, 0) && (isQueryStats ? d["host_object_name"] is null : (string?)d["object_name"] == "proc0"));
        Assert.Null(nulled["query_plan_xml_bytes"]);
        Assert.Equal(9L, (long)nulled["execution_count"]! / 1000);
    }

    [Fact]
    public async Task The_pg_statement_text_fetch_returns_own_entries_as_json_text_and_null_when_the_store_has_none()
    {
        if (_store.Builder is null) { return; }

        await using var connection = new NpgsqlConnection(_store.Builder.ConnectionString);
        await connection.OpenAsync();
        await using (var insert = new NpgsqlCommand(
            "INSERT INTO collect.pg_statement_text (server_id, queryid, query_text, first_seen, last_seen, own_entries) VALUES " +
            "(1, 101, 'select 1', '2026-10-01 00:00:00', '2026-10-22 11:00:00', '[{\"u\": 10, \"d\": 16384}]'::jsonb), " +
            "(1, 102, 'select 2', '2026-10-01 00:00:00', '2026-10-22 11:00:00', NULL)", connection))
        {
            await insert.ExecuteNonQueryAsync();
        }

        var rows = await PgStatementTextExporter.FetchContentAsync(
            connection, [(1, 101L), (1, 102L)], _ => Task.CompletedTask, default, pacerOverride: new ContentFetchPacer(allowCompressed: true, sleep: (_, _) => Task.CompletedTask));

        Assert.Equal(2, rows.Count);
        using var own = System.Text.Json.JsonDocument.Parse(rows.Single(r => r.QueryId == 101).OwnEntries!);
        var entry = Assert.Single(own.RootElement.EnumerateArray());
        Assert.Equal(10, entry.GetProperty("u").GetInt64());
        Assert.Equal(16384, entry.GetProperty("d").GetInt64());
        Assert.Null(rows.Single(r => r.QueryId == 102).OwnEntries);
    }

    private static object? Normalize(object? value) => value switch
    {
        short or int or long => Convert.ToInt64(value),
        _ => value,
    };

    private static void Exec(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
