using System.Text.Json;
using System.Text.RegularExpressions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Runs the exporter's store-side reads against a throwaway PostgreSQL (Docker) laid out like a Darling store, and
/// asserts what comes back: which rows a window returns, which columns leave the store, what the planner does with
/// the statement, and what an operator sees in pg_stat_activity. Needs Docker; with no Docker each test returns
/// without asserting, like the other Docker-backed suites.
/// </summary>
[Trait("Category", "Slow")]
public sealed class StoreReadSqlBehaviourTests : IClassFixture<StoreReadSqlBehaviourTests.StoreFixture>
{
    private static readonly DateTime From = new(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime To = From.AddHours(1);

    public sealed class StoreFixture : IDisposable
    {
        public GenericRawTableEquivalenceTests.GenericPgFixture Pg { get; } = new();

        public StoreFixture()
        {
            if (Pg.Builder is null) { return; }
            var result = Pg.Psql(SchemaSql());
            Assert.True(result.code == 0, result.output);
        }

        public void Dispose() => Pg.Dispose();

        private static string SchemaSql()
        {
            var sql = new System.Text.StringBuilder();
            sql.AppendLine("CREATE SCHEMA IF NOT EXISTS config;");

            // One minimal table per registry entry (time column + server_name), seeded with a row on each side of
            // the window edges, plus 5000 older rows so the planner has a reason to use the time index.
            foreach (var t in OptionalTables.All.Where(t => t.Kind != OptionalTableKind.Dimension))
            {
                var name = $"{t.Schema}.{SqlBuilder.QuoteIdentifier(t.Name)}";
                if (t.Kind == OptionalTableKind.Snapshot)
                {
                    sql.AppendLine($"CREATE TABLE {name} (server_name text);");
                    sql.AppendLine($"INSERT INTO {name} VALUES ('before'), ('at_from'), ('last_in'), ('at_to');");
                    continue;
                }

                sql.AppendLine($"CREATE TABLE {name} (server_name text, {t.TimeColumn} timestamp);");
                sql.AppendLine($"CREATE INDEX ON {name} ({t.TimeColumn});");
                sql.AppendLine(Seed(name, t.TimeColumn!));
            }

            // Tables the tests widen with the extra columns they read.
            sql.AppendLine("ALTER TABLE collect.plan_correction ADD COLUMN \"Weird Name\" text;");
            sql.AppendLine("ALTER TABLE collect.job_history ADD COLUMN run_status int, ADD COLUMN message text;");
            sql.AppendLine("ALTER TABLE config.config_alert_log ADD COLUMN send_error text;");
            sql.AppendLine("ALTER TABLE collect.pg_log_events ADD COLUMN occurred_at timestamp, ADD COLUMN message text, ADD COLUMN family text, ADD COLUMN a_brand_new_column text;");
            sql.AppendLine("ALTER TABLE collect.query_snapshots ADD COLUMN session_id int, ADD COLUMN query_text text, ADD COLUMN query_plan text, ADD COLUMN live_query_plan text;");
            sql.AppendLine("INSERT INTO collect.plan_correction (server_name, collection_time, \"Weird Name\") VALUES ('weird', '2026-10-22 11:30:00', 'w1');");
            sql.AppendLine($"INSERT INTO collect.job_history (server_name, collection_time, run_status, message) VALUES " +
                $"('jh_failed', '2026-10-22 11:30:00', 0, repeat('x', 3000)), ('jh_ok', '2026-10-22 11:30:00', 1, 'all fine');");
            sql.AppendLine("INSERT INTO config.config_alert_log (server_name, alert_time, send_error) VALUES ('alert', '2026-10-22 11:30:00', 'smtp refused');");
            sql.AppendLine("INSERT INTO collect.pg_log_events (server_name, collection_time, occurred_at, message, family, a_brand_new_column) VALUES ('log', '2026-10-22 11:30:00', '2026-10-22 11:29:00', 'secret text', 'vacuum', 'new');");
            sql.AppendLine("INSERT INTO collect.query_snapshots (server_name, collection_time, session_id, query_text, query_plan, live_query_plan) VALUES ('snap', '2026-10-22 11:30:00', 7, 'select 1', '<plan/>', '<live/>');");

            // Discovery probes: identity, serial, store-local ids, and a same-named table in another schema.
            sql.AppendLine("CREATE TABLE collect.disc_probe (server_id int, collection_id bigint, ident bigint GENERATED ALWAYS AS IDENTITY, seq serial, query_id bigint, plain text, collection_time timestamp);");
            sql.AppendLine("CREATE TABLE config.disc_probe (only_in_config int, query_id bigint);");

            // Raw tables.
            sql.AppendLine("CREATE TABLE collect.perfmon_stats (collection_id bigint, collection_time timestamp NOT NULL, server_id int NOT NULL, server_name text NOT NULL, cntr_value bigint);");
            sql.AppendLine("CREATE INDEX ON collect.perfmon_stats (collection_time);");
            sql.AppendLine(Seed("collect.perfmon_stats", "collection_time", extraColumns: "server_id", extraValues: "1"));
            sql.AppendLine("CREATE TABLE collect.system_health_events (server_name text, collection_time timestamp, event_time timestamp, event_type text, event_xml text);");
            sql.AppendLine("CREATE INDEX ON collect.system_health_events (collection_time);");
            sql.AppendLine(Seed("collect.system_health_events", "collection_time"));

            sql.AppendLine("""
                CREATE TABLE collect.query_stats (
                  collection_id bigint, server_id int, server_name text, database_name text, query_hash text, query_plan_hash text,
                  collection_time timestamp, sample_interval_seconds int,
                  delta_execution_count bigint, delta_worker_time bigint, delta_elapsed_time bigint, delta_logical_reads bigint,
                  delta_logical_writes bigint, delta_physical_reads bigint, delta_rows bigint, delta_spills bigint,
                  query_plan_digest bytea, query_text_digest bytea);
                CREATE INDEX ON collect.query_stats (collection_time);
                CREATE TABLE collect.procedure_stats (
                  collection_id bigint, server_id int, server_name text, database_name text, schema_name text, object_name text, object_type text,
                  collection_time timestamp, sample_interval_seconds int,
                  delta_execution_count bigint, delta_worker_time bigint, delta_elapsed_time bigint, delta_logical_reads bigint,
                  delta_logical_writes bigint, delta_physical_reads bigint, delta_spills bigint,
                  query_plan_digest bytea, plan_handle text, cached_time timestamp, query_plan_xml text, query_plan_xml_bytes int);
                CREATE INDEX ON collect.procedure_stats (collection_time);
                INSERT INTO collect.query_stats (server_name, collection_time) SELECT 'bulk', timestamp '2026-09-01' + i * interval '7 minutes' FROM generate_series(1, 5000) i;
                INSERT INTO collect.procedure_stats (server_name, collection_time) SELECT 'bulk', timestamp '2026-09-01' + i * interval '7 minutes' FROM generate_series(1, 5000) i;
                CREATE TABLE collect.pg_statement_text (server_id int, queryid bigint, query_text text, first_seen timestamp, last_seen timestamp, PRIMARY KEY (server_id, queryid));
                INSERT INTO collect.pg_statement_text SELECT 1 + i % 2, i, 'q' || i, now(), now() FROM generate_series(1, 20000) i;
                CREATE TABLE config.config_collector_schedules (server_id int, collector_name text, frequency_minutes int, enabled boolean);
                INSERT INTO config.config_collector_schedules VALUES (1, 'memory_grant_stats', 5, true), (NULL, 'memory_grant_stats', 10, true);
                """);

            // Query Store: the raw table and Darling's deduplicated wide table share the column list.
            string Ddl(string table, IEnumerable<string> columns) =>
                $"CREATE TABLE {table} ({string.Join(", ", columns.Select(c => $"{c} {PgTypeFor(c)}"))});";
            sql.AppendLine(Ddl("collect.query_store_stats", QueryStoreWindow.RawColumns.Concat(["collection_id", "query_text", "query_plan_text"])));
            sql.AppendLine("CREATE INDEX ON collect.query_store_stats (collection_time);");
            sql.AppendLine(Ddl("collect.query_store_interval_wide", QueryStoreWindow.RawColumns.Where(c => c != "server_name")));
            sql.AppendLine("CREATE INDEX ON collect.query_store_interval_wide (first_execution_time);");
            sql.AppendLine("INSERT INTO collect.query_store_stats (server_id, server_name, query_id, interval_start_time_utc, collection_time, collection_id, query_text, query_plan_text) VALUES " +
                "(1, 'in_normal', 1, '2026-10-22 11:00:00', '2026-10-22 12:10:00', 1, 'q', 'p')," +
                "(1, 'in_late', 2, '2026-10-22 11:59:00', '2026-10-22 13:59:59.999999', 2, 'q', 'p')," +
                "(1, 'out_interval_at_to', 3, '2026-10-22 12:00:00', '2026-10-22 12:10:00', 3, 'q', 'p')," +
                "(1, 'out_collected_at_to_plus_2h', 4, '2026-10-22 11:30:00', '2026-10-22 14:00:00', 4, 'q', 'p')," +
                "(1, 'out_interval_before_from', 5, '2026-10-22 10:59:59.999999', '2026-10-22 11:30:00', 5, 'q', 'p');");
            sql.AppendLine("INSERT INTO collect.query_store_stats (server_id, server_name, collection_time, interval_start_time_utc) " +
                "SELECT 1, 'bulk', timestamp '2026-09-01' + i * interval '7 minutes', timestamp '2026-09-01' + i * interval '7 minutes' FROM generate_series(1, 5000) i;");
            sql.AppendLine("INSERT INTO collect.query_store_interval_wide (server_id, database_name, query_id, first_execution_time, interval_start_time_utc) VALUES " +
                "(1, 'in_start', 1, '2026-10-22 11:00:00', '2026-10-22 11:00:00')," +
                "(1, 'in_odd_interval', 2, '2026-10-22 11:59:59.999999', '2026-10-22 10:00:00')," +
                "(1, 'out_at_to', 3, '2026-10-22 12:00:00', '2026-10-22 11:00:00')," +
                "(1, 'out_before_from', 4, '2026-10-22 10:59:59.999999', '2026-10-22 11:00:00');");
            sql.AppendLine("INSERT INTO collect.query_store_interval_wide (server_id, database_name, first_execution_time) " +
                "SELECT 1, 'bulk', timestamp '2026-09-01' + i * interval '7 minutes' FROM generate_series(1, 5000) i;");
            sql.AppendLine("ANALYZE;");
            return sql.ToString();
        }

        private static string Seed(string table, string timeColumn, string extraColumns = "", string extraValues = "")
        {
            var cols = extraColumns.Length == 0 ? "" : ", " + extraColumns;
            var vals = extraValues.Length == 0 ? "" : ", " + extraValues;
            return $"INSERT INTO {table} (server_name, {timeColumn}{cols}) VALUES " +
                   $"('before', '2026-10-22 10:59:59.999999'{vals}), ('at_from', '2026-10-22 11:00:00'{vals}), " +
                   $"('last_in', '2026-10-22 11:59:59.999999'{vals}), ('at_to', '2026-10-22 12:00:00'{vals});\n" +
                   $"INSERT INTO {table} (server_name, {timeColumn}{cols}) SELECT 'bulk', timestamp '2026-09-01' + i * interval '7 minutes'{vals} " +
                   "FROM generate_series(1, 5000) i;";
        }

        private static string PgTypeFor(string column) => column switch
        {
            "server_id" => "int",
            _ when column.EndsWith("_time", StringComparison.Ordinal) || column.EndsWith("_utc", StringComparison.Ordinal) => "timestamp",
            "server_name" or "database_name" or "execution_type_desc" or "module_name" or "query_hash" or "plan_type"
                or "plan_forcing_type" or "last_force_failure_reason" or "query_plan_hash" or "replica_role"
                or "query_text" or "query_plan_text" => "text",
            _ => "bigint",
        };
    }

    private readonly StoreFixture _store;

    public StoreReadSqlBehaviourTests(StoreFixture store) => _store = store;

    private NpgsqlConnectionStringBuilder? Builder => _store.Pg.Builder;

    // ---- helpers ---------------------------------------------------------

    private static NpgsqlParameter[] Window() => SqlBuilder.WindowParameters(From, To);

    private static NpgsqlParameter[] WindowPlus2h() =>
    [
        .. SqlBuilder.WindowParameters(From, To),
        new NpgsqlParameter("to_plus_2h", NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(To.AddHours(2), DateTimeKind.Unspecified) },
    ];

    private sealed record Result(List<string> Columns, List<object?[]> Rows)
    {
        public IEnumerable<object?> Column(string name) { var i = Columns.IndexOf(name); return Rows.Select(r => r[i]); }
        public IEnumerable<string> Names(string column = "server_name") => Column(column).Select(v => (string)v!).OrderBy(v => v, StringComparer.Ordinal);
    }

    private async Task<Result> RunAsync(string sql, NpgsqlParameter[]? parameters = null)
    {
        await using var connection = await StoreConnectionFactory.OpenAsync(Builder!, default);
        return await ReadAsync(connection, null, sql, parameters);
    }

    private static async Task<Result> ReadAsync(NpgsqlConnection connection, NpgsqlTransaction? tx, string sql, NpgsqlParameter[]? parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection, tx);
        if (parameters is not null) { command.Parameters.AddRange(parameters); }
        await using var reader = await command.ExecuteReaderAsync();
        var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
        var rows = new List<object?[]>();
        while (await reader.ReadAsync())
        {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < row.Length; i++) { row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i); }
            rows.Add(row);
        }
        return new Result(columns, rows);
    }

    private static readonly string[] BlockingNodes = ["Sort", "Incremental Sort", "Aggregate", "HashAggregate", "GroupAggregate", "Unique", "WindowAgg"];

    private static void CollectNodes(JsonElement plan, List<(string Type, string? Index)> into)
    {
        into.Add((plan.GetProperty("Node Type").GetString()!, plan.TryGetProperty("Index Name", out var i) ? i.GetString() : null));
        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray()) { CollectNodes(child, into); }
        }
    }

    /// <summary>
    /// The store-gentleness check, shared by every window read: the plan holds no sort, aggregate or de-duplication node
    /// (the store does no work beyond the scan) and reaches the rows through an index on the named column.
    /// </summary>
    private async Task AssertIndexRangeReadAsync(string sql, NpgsqlParameter[]? parameters, string? indexColumn, string? setupSql = null)
    {
        var nodes = await PlanNodesAsync(sql, parameters, setupSql);
        Assert.DoesNotContain(nodes, n => BlockingNodes.Contains(n.Type));
        if (indexColumn is not null)
        {
            Assert.Contains(nodes, n => n.Index is not null && n.Index.Contains(indexColumn, StringComparison.Ordinal));
        }
    }

    private async Task<List<(string Type, string? Index)>> PlanNodesAsync(string sql, NpgsqlParameter[]? parameters, string? setupSql = null)
    {
        await using var connection = await StoreConnectionFactory.OpenAsync(Builder!, default);
        await using var tx = await connection.BeginTransactionAsync();
        if (setupSql is not null)
        {
            await using var setup = new NpgsqlCommand(setupSql, connection, tx);
            await setup.ExecuteNonQueryAsync();
        }
        await using var command = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, connection, tx);
        if (parameters is not null) { command.Parameters.AddRange(parameters); }
        var json = (string)(await command.ExecuteScalarAsync())!;
        using var doc = JsonDocument.Parse(json);
        var nodes = new List<(string, string?)>();
        CollectNodes(doc.RootElement[0].GetProperty("Plan"), nodes);
        return nodes;
    }

    /// <summary>Runs a statement on a connection that stays open, then reads that backend's last statement as an operator would.</summary>
    private async Task<string> ActivityQueryAfterAsync(string sql, NpgsqlParameter[]? parameters)
    {
        await using var connection = await StoreConnectionFactory.OpenAsync(Builder!, default);
        await ReadAsync(connection, null, sql, parameters);
        await using var observer = new NpgsqlConnection(Builder!.ConnectionString);
        await observer.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT query FROM pg_stat_activity WHERE pid = @pid", observer);
        command.Parameters.AddWithValue("pid", connection.ProcessID);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private static IEnumerable<OptionalTable> WindowTables => OptionalTables.All.Where(t => t.Kind == OptionalTableKind.Window);

    private static string[] ColumnsFor(OptionalTable table) =>
        table.Kind == OptionalTableKind.Snapshot ? ["server_name"] : [table.TimeColumn!, "server_name"];

    // ---- window reads ----------------------------------------------------

    [Fact]
    public async Task Raw_select_returns_the_half_open_window_and_only_the_discovered_columns()
    {
        if (Builder is null) { return; }
        var discovered = (await RunAsync(SqlBuilder.RawColumnDiscoverySql, [new NpgsqlParameter("table", "perfmon_stats")])).Names("column_name").ToList();
        Assert.DoesNotContain("server_id", discovered);

        var result = await RunAsync(SqlBuilder.BuildRawSelect("perfmon_stats", ["collection_time", "server_name", "cntr_value"]), Window());

        Assert.Equal(["collection_time", "server_name", "cntr_value"], result.Columns);
        Assert.Equal(["at_from", "last_in"], result.Names());
    }

    [Fact]
    public async Task Raw_select_with_a_sub_hour_window_returns_only_that_window_with_no_hour_bucketing()
    {
        if (Builder is null) { return; }
        var from = new DateTime(2026, 10, 22, 11, 10, 0, DateTimeKind.Utc);
        var to = from.AddMinutes(10);
        await using var setup = new NpgsqlConnection(Builder.ConnectionString);
        await setup.OpenAsync();
        await using (var insert = new NpgsqlCommand(
            "INSERT INTO collect.perfmon_stats (server_name, collection_time, server_id) VALUES " +
            "('sub_before', '2026-10-22 11:09:59.999999', 1), ('sub_at_from', '2026-10-22 11:10:00', 1), " +
            "('sub_last', '2026-10-22 11:19:59.999999', 1), ('sub_at_to', '2026-10-22 11:20:00', 1)", setup))
        {
            await insert.ExecuteNonQueryAsync();
        }

        var result = await RunAsync(SqlBuilder.BuildRawSelect("perfmon_stats", ["collection_time", "server_name"]), SqlBuilder.WindowParameters(from, to));

        Assert.Equal(["sub_at_from", "sub_last"], result.Names());

        var health = await RunAsync(SystemHealthExporter.WindowSql, Window());
        Assert.Equal(["at_from", "last_in"], health.Names());
    }

    [Fact]
    public async Task Every_window_optional_table_returns_exactly_the_half_open_window()
    {
        if (Builder is null) { return; }
        foreach (var table in WindowTables)
        {
            var result = await RunAsync(SqlBuilder.BuildOptionalSelect(table, ColumnsFor(table)), Window());
            var names = result.Names().ToList();
            Assert.True(names.Contains("at_from") && names.Contains("last_in"), $"{table.Name} lost a row inside the window: {string.Join(",", names)}");
            Assert.True(!names.Contains("before") && !names.Contains("at_to") && !names.Contains("bulk"), $"{table.Name} returned a row outside the window: {string.Join(",", names)}");
        }
    }

    [Fact]
    public async Task Snapshot_optional_tables_are_read_whole_with_no_window()
    {
        if (Builder is null) { return; }
        foreach (var table in OptionalTables.All.Where(t => t.Kind == OptionalTableKind.Snapshot))
        {
            var result = await RunAsync(SqlBuilder.BuildOptionalSelect(table, ColumnsFor(table)));
            Assert.Equal(["at_from", "at_to", "before", "last_in"], result.Names());
        }
    }

    [Fact]
    public async Task Optional_select_qualifies_the_config_schema_and_quotes_identifiers()
    {
        if (Builder is null) { return; }
        var alertLog = OptionalTables.TryGet("config_alert_log")!;
        var alerts = await RunAsync(SqlBuilder.BuildOptionalSelect(alertLog, ["alert_time", "server_name"]), Window());
        Assert.Contains("alert", alerts.Names());

        var correction = OptionalTables.TryGet("plan_correction")!;
        var weird = await RunAsync(SqlBuilder.BuildOptionalSelect(correction, ["collection_time", "Weird Name"]), Window());
        Assert.Equal(["collection_time", "Weird Name"], weird.Columns);
        Assert.Contains("w1", weird.Column("Weird Name"));
    }

    [Fact]
    public async Task Query_stats_window_read_is_a_plain_projection_of_the_raw_columns_one_row_per_store_row()
    {
        if (Builder is null) { return; }
        await using var connection = new NpgsqlConnection(Builder.ConnectionString);
        await connection.OpenAsync();
        await using (var insert = new NpgsqlCommand(
            "INSERT INTO collect.query_stats (server_name, database_name, query_hash, query_plan_hash, collection_time, sample_interval_seconds, delta_execution_count) VALUES " +
            "('qsw', 'db1', 'h1', 'p1', '2026-10-22 11:00:00', 60, 1), ('qsw', 'db1', 'h1', 'p1', '2026-10-22 11:00:00', 60, 2), " +
            "('qsw', 'db1', 'h1', 'p1', '2026-10-22 12:00:00', 60, 4), ('qsw', 'db1', 'h1', 'p1', '2026-10-22 10:59:59.999999', 60, 8)", connection))
        {
            await insert.ExecuteNonQueryAsync();
        }

        var result = await RunAsync(SqlBuilder.QueryStatsWindowSql, Window());

        Assert.Equal(QueryStatsWindow.RawColumns, result.Columns);
        Assert.Equal(["qsw", "qsw"], result.Names().Where(n => n == "qsw"));      // two rows, not one grouped row
        Assert.Equal([1L, 2L], result.Rows.Where(r => (string?)r[0] == "qsw").Select(r => (long)r[6]!).OrderBy(v => v));
    }

    [Fact]
    public async Task Procedure_stats_window_read_returns_the_raw_columns_including_plan_identity_and_never_the_plan_xml()
    {
        if (Builder is null) { return; }
        await using var connection = new NpgsqlConnection(Builder.ConnectionString);
        await connection.OpenAsync();
        await using (var insert = new NpgsqlCommand(
            "INSERT INTO collect.procedure_stats (server_name, collection_time, plan_handle, cached_time, query_plan_xml, query_plan_xml_bytes) VALUES " +
            "('psw', '2026-10-22 11:30:00', '0xAB', '2026-10-22 08:00:00', '<plan/>', 7)", connection))
        {
            await insert.ExecuteNonQueryAsync();
        }

        var result = await RunAsync(SqlBuilder.ProcedureStatsWindowSql, Window());

        Assert.Equal(ProcedureStatsWindow.RawColumns, result.Columns);
        var row = result.Rows.Single(r => (string?)r[0] == "psw");
        Assert.Equal("0xAB", row[result.Columns.IndexOf("plan_handle")]);
        Assert.Equal(new DateTime(2026, 10, 22, 8, 0, 0), row[result.Columns.IndexOf("cached_time")]);
        Assert.Equal(false, row[result.Columns.IndexOf("plan_xml_is_null")]);
        Assert.Equal(true, row[result.Columns.IndexOf("has_plan_xml_bytes")]);
        Assert.DoesNotContain("query_plan_xml", result.Columns);
    }

    [Fact]
    public async Task Query_store_raw_read_needs_both_the_collection_window_and_the_interval_start_and_leaves_text_and_ids_out()
    {
        if (Builder is null) { return; }

        var result = await RunAsync(SqlBuilder.QueryStoreStatsRawSql, WindowPlus2h());

        Assert.Equal(["in_late", "in_normal"], result.Names());
        Assert.DoesNotContain("query_text", result.Columns);
        Assert.DoesNotContain("query_plan_text", result.Columns);
        Assert.DoesNotContain("collection_id", result.Columns);
    }

    [Fact]
    public async Task Query_store_wide_read_selects_by_first_execution_time_in_raw_column_order_with_a_null_server_name()
    {
        if (Builder is null) { return; }

        var result = await RunAsync(SqlBuilder.QueryStoreStatsWideSql, Window());

        // An interval whose start lies outside the window is still returned when its first execution is inside: the
        // loader validates interval_start_time_utc in DuckDB and falls back to the raw read.
        Assert.Equal(["in_odd_interval", "in_start"], result.Names("database_name"));
        Assert.Equal(QueryStoreWindow.RawColumns, result.Columns);
        Assert.All(result.Column("server_name"), Assert.Null);
    }

    [Fact]
    public async Task Query_store_wide_read_setup_makes_the_transaction_read_only_and_takes_the_planner_off_plain_index_scans()
    {
        if (Builder is null) { return; }

        var without = await PlanNodesAsync(SqlBuilder.QueryStoreStatsWideSql, Window());
        var with = await PlanNodesAsync(SqlBuilder.QueryStoreStatsWideSql, Window(), SqlBuilder.QueryStoreWideReadSetupSql);

        Assert.Contains(without, n => n.Type == "Index Scan");                 // the planner's own choice: one random page per row
        Assert.DoesNotContain(with, n => n.Type is "Index Scan" or "Index Only Scan");
        Assert.Contains(with, n => n.Type == "Bitmap Heap Scan");

        await using var connection = await StoreConnectionFactory.OpenAsync(Builder!, default);
        await using var tx = await connection.BeginTransactionAsync();
        await using (var setup = new NpgsqlCommand(SqlBuilder.QueryStoreWideReadSetupSql, connection, tx))
        {
            await setup.ExecuteNonQueryAsync();
        }
        await using var write = new NpgsqlCommand("INSERT INTO collect.perfmon_stats (server_name, collection_time, server_id) VALUES ('w', now(), 1)", connection, tx);
        var ex = await Assert.ThrowsAsync<PostgresException>(() => write.ExecuteNonQueryAsync());
        Assert.Equal(PostgresErrorCodes.ReadOnlySqlTransaction, ex.SqlState);
    }

    // ---- store gentleness: no sort, aggregate or de-duplication; the rows come through the time index ----------

    [Fact]
    public async Task Every_window_optional_read_is_an_index_range_read_with_no_sort_or_aggregate()
    {
        if (Builder is null) { return; }
        foreach (var table in WindowTables)
        {
            await AssertIndexRangeReadAsync(SqlBuilder.BuildOptionalSelect(table, ColumnsFor(table)), Window(), table.TimeColumn);
        }
        foreach (var table in OptionalTables.All.Where(t => t.Kind == OptionalTableKind.Snapshot))
        {
            await AssertIndexRangeReadAsync(SqlBuilder.BuildOptionalSelect(table, ColumnsFor(table)), null, indexColumn: null);
        }
    }

    [Fact]
    public async Task Core_window_reads_are_index_range_reads_with_no_sort_or_aggregate()
    {
        if (Builder is null) { return; }
        await AssertIndexRangeReadAsync(SqlBuilder.BuildRawSelect("perfmon_stats", ["collection_time", "server_name"]), Window(), "collection_time");
        await AssertIndexRangeReadAsync(SystemHealthExporter.WindowSql, Window(), "collection_time");
        await AssertIndexRangeReadAsync(SqlBuilder.QueryStatsWindowSql, Window(), "collection_time");
        await AssertIndexRangeReadAsync(SqlBuilder.ProcedureStatsWindowSql, Window(), "collection_time");
        await AssertIndexRangeReadAsync(SqlBuilder.QueryStoreStatsRawSql, WindowPlus2h(), "collection_time");
        await AssertIndexRangeReadAsync(SqlBuilder.QueryStoreStatsWideSql, Window(), "first_execution_time", SqlBuilder.QueryStoreWideReadSetupSql);
        await AssertIndexRangeReadAsync(
            QuerySnapshotsExporter.BuildSelect(["collection_time", "server_name", "query_plan"]), Window(), "collection_time");
    }

    [Fact]
    public async Task Pg_statement_text_batch_fetch_probes_the_primary_key_for_exactly_the_requested_keys()
    {
        if (Builder is null) { return; }
        NpgsqlParameter[] Keys() =>
        [
            new NpgsqlParameter("sids", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = new[] { 1, 2, 1 } },
            new NpgsqlParameter("qids", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = new[] { 2L, 3L, 3L } },
        ];

        var result = await RunAsync(SqlBuilder.PgStatementTextBatchSql, Keys());

        // Server 1 owns the even ids and server 2 the odd ones: (1,2) and (2,3) exist, (1,3) does not.
        Assert.Equal(["q2", "q3"], result.Names("query_text"));
        await AssertIndexRangeReadAsync(SqlBuilder.PgStatementTextBatchSql, Keys(), "pkey");
    }

    // ---- column discovery --------------------------------------------------

    [Fact]
    public async Task Optional_column_discovery_returns_ordinal_ordered_name_type_pairs_without_store_local_columns()
    {
        if (Builder is null) { return; }

        var collect = await RunAsync(SqlBuilder.OptionalColumnDiscoverySql,
            [new NpgsqlParameter("schema", "collect"), new NpgsqlParameter("table", "disc_probe")]);

        // server_id and collection_id, the identity column and the serial (nextval default) stay behind; query_id stays.
        Assert.Equal(["column_name", "data_type", "udt_name"], collect.Columns);
        Assert.Equal(["query_id", "plain", "collection_time"], collect.Column("column_name").Select(v => (string)v!));
        Assert.Equal(["int8", "text", "timestamp"], collect.Column("udt_name").Select(v => (string)v!));
        Assert.Equal("timestamp without time zone", collect.Column("data_type").Last());

        var config = await RunAsync(SqlBuilder.OptionalColumnDiscoverySql,
            [new NpgsqlParameter("schema", "config"), new NpgsqlParameter("table", "disc_probe")]);
        Assert.Equal(["only_in_config", "query_id"], config.Column("column_name").Select(v => (string)v!));
    }

    [Fact]
    public async Task Raw_column_discovery_drops_server_id_only_and_reads_only_the_collect_schema()
    {
        if (Builder is null) { return; }

        var result = await RunAsync(SqlBuilder.RawColumnDiscoverySql, [new NpgsqlParameter("table", "disc_probe")]);

        Assert.Equal(
            ["collection_id", "ident", "seq", "query_id", "plain", "collection_time"],
            result.Column("column_name").Select(v => (string)v!));
    }

    // ---- column policies, run as the store runs them -------------------------

    private static List<DiscoveredColumn> Discovered(params string[] names) => names.Select(n => new DiscoveredColumn(n, "text", "text")).ToList();

    [Fact]
    public async Task Policy_drop_keeps_the_column_out_of_the_returned_columns()
    {
        if (Builder is null) { return; }
        var table = OptionalTables.TryGet("config_alert_log")!;

        var sql = SqlBuilder.BuildOptionalSelect(table, Discovered("alert_time", "server_name", "send_error"), ColumnPolicies.For("config_alert_log"));
        var result = await RunAsync(sql, Window());

        Assert.Equal(["alert_time", "server_name"], result.Columns);
    }

    [Fact]
    public async Task Policy_keep_when_blanks_the_message_of_a_succeeded_run_and_truncates_a_failed_one_to_2000_characters()
    {
        if (Builder is null) { return; }
        var table = OptionalTables.TryGet("job_history")!;

        var sql = SqlBuilder.BuildOptionalSelect(table, Discovered("server_name", "run_status", "message"), ColumnPolicies.For("job_history"));
        var result = await RunAsync(sql, Window());

        string? MessageOf(string server) => (string?)result.Rows.Single(r => (string?)r[0] == server)[2];
        Assert.Equal(2000, MessageOf("jh_failed")!.Length);
        Assert.Null(MessageOf("jh_ok"));
    }

    [Fact]
    public async Task Policy_keep_when_without_a_truncate_returns_the_column_untouched()
    {
        if (Builder is null) { return; }
        var table = OptionalTables.TryGet("servers")!;
        var policy = new TablePolicy(new[] { new ColumnRule("server_name", ColumnAction.KeepWhen, PredicateSql: "1 = 1") });

        var result = await RunAsync(SqlBuilder.BuildOptionalSelect(table, Discovered("server_name"), policy));

        Assert.Equal(["at_from", "at_to", "before", "last_in"], result.Names());
    }

    [Fact]
    public async Task Policy_keep_only_returns_the_intersection_with_the_discovered_columns_in_discovery_order()
    {
        if (Builder is null) { return; }
        var table = OptionalTables.TryGet("pg_log_events")!;

        var sql = SqlBuilder.BuildOptionalSelect(
            table, Discovered("collection_time", "occurred_at", "message", "family", "a_brand_new_column", "server_name"), ColumnPolicies.For("pg_log_events"));
        var result = await RunAsync(sql, Window());

        Assert.Equal(["collection_time", "occurred_at", "family", "server_name"], result.Columns);
    }

    [Fact]
    public async Task Query_snapshots_read_never_returns_the_live_plan_and_ends_with_the_query_plan()
    {
        if (Builder is null) { return; }
        var columns = new[]
        {
            "collection_time", "server_name", "session_id", "query_text", "query_plan", "live_query_plan",
        };

        var result = await RunAsync(QuerySnapshotsExporter.BuildSelect(columns), Window());

        Assert.DoesNotContain("live_query_plan", result.Columns);
        Assert.Equal("query_plan", result.Columns[^1]);                       // the reader loop streams the last ordinal
        Assert.Equal(["collection_time", "server_name", "session_id", "query_text", "query_plan"], result.Columns);
        Assert.Contains("<plan/>", result.Column("query_plan"));
    }

    // ---- the two hourly oracle statements the DuckDB rollups were checked against -------------------------------

    private async Task SeedQueryStatsAsync(string label)
    {
        await using var connection = new NpgsqlConnection(Builder!.ConnectionString);
        await connection.OpenAsync();
        await using var insert = new NpgsqlCommand($"""
            INSERT INTO collect.query_stats
              (server_name, database_name, query_hash, query_plan_hash, collection_time, sample_interval_seconds, delta_execution_count, delta_spills)
            VALUES
              ('{label}', 'db1', 'h1', 'p1', '2026-10-22 11:00:00', 60, 5, 2),
              ('{label}', 'db1', 'h1', 'p1', '2026-10-22 11:05:00', 0, 100, 40),
              ('{label}', 'db1', 'h1', 'p2', '2026-10-22 11:10:00', 60, 8, 0)
            """, connection);
        await insert.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Query_stats_hourly_oracle_leaves_interval_zero_rows_out_and_groups_by_plan_hash()
    {
        if (Builder is null) { return; }
        await SeedQueryStatsAsync("oracle_hourly");

        var result = await RunAsync(SqlBuilder.QueryStatsHourlySql, Window());

        var mine = result.Rows.Where(r => (string?)r[0] == "oracle_hourly").ToList();
        Assert.Equal(2, mine.Count);
        var p1 = mine.Single(r => (string?)r[result.Columns.IndexOf("query_plan_hash")] == "p1");
        Assert.Equal(5L, p1[result.Columns.IndexOf("execution_count_sum")]);
        Assert.Equal(60L, p1[result.Columns.IndexOf("sample_interval_seconds_sum")]);
    }

    [Fact]
    public async Task Procedure_stats_hourly_oracle_leaves_interval_zero_rows_out_and_has_no_rows_or_plan_hash_column()
    {
        if (Builder is null) { return; }
        await using var connection = new NpgsqlConnection(Builder.ConnectionString);
        await connection.OpenAsync();
        await using (var insert = new NpgsqlCommand(
            "INSERT INTO collect.procedure_stats (server_name, database_name, schema_name, object_name, collection_time, sample_interval_seconds, delta_execution_count) VALUES " +
            "('oracle_proc', 'db1', 'dbo', 'p', '2026-10-22 11:00:00', 60, 5), ('oracle_proc', 'db1', 'dbo', 'p', '2026-10-22 11:05:00', 0, 100)", connection))
        {
            await insert.ExecuteNonQueryAsync();
        }

        var result = await RunAsync(SqlBuilder.ProcedureStatsHourlySql, Window());

        var row = Assert.Single(result.Rows, r => (string?)r[0] == "oracle_proc");
        Assert.Equal(5L, row[result.Columns.IndexOf("execution_count_sum")]);
        Assert.DoesNotContain("rows_sum", result.Columns);
        Assert.DoesNotContain("query_plan_hash", result.Columns);
    }

    // ---- operators find the exporter's statements by their tag -----------------------------------------

    [Fact]
    public async Task An_operator_sees_each_store_read_by_its_darling_export_tag_in_pg_stat_activity()
    {
        if (Builder is null) { return; }
        var optional = OptionalTables.All.Where(t => t.Kind != OptionalTableKind.Dimension)
            .Select(t => ($"opt.{t.Name}", SqlBuilder.BuildOptionalSelect(t, ColumnsFor(t)), t.Kind == OptionalTableKind.Snapshot ? null : Window()));

        (string Tag, string Sql, NpgsqlParameter[]? Parameters)[] statements =
        [
            .. optional,
            ("raw.perfmon_stats", SqlBuilder.BuildRawSelect("perfmon_stats", ["collection_time"]), Window()),
            ("raw.column_discovery", SqlBuilder.RawColumnDiscoverySql, [new NpgsqlParameter("table", "disc_probe")]),
            ("opt.column_discovery", SqlBuilder.OptionalColumnDiscoverySql, [new NpgsqlParameter("schema", "collect"), new NpgsqlParameter("table", "disc_probe")]),
            ("query_stats.window", SqlBuilder.QueryStatsWindowSql, Window()),
            ("procedure_stats.window", SqlBuilder.ProcedureStatsWindowSql, Window()),
            ("query_store_stats.window", SqlBuilder.QueryStoreStatsRawSql, WindowPlus2h()),
            ("query_store_stats.wide", SqlBuilder.QueryStoreStatsWideSql, Window()),
            ("system_health_events.window", SystemHealthExporter.WindowSql, Window()),
            ("opt.query_snapshots", QuerySnapshotsExporter.BuildSelect(["collection_time", "query_plan"]), Window()),
            ("query_stats_hourly", SqlBuilder.QueryStatsHourlySql, Window()),
            ("procedure_stats_hourly", SqlBuilder.ProcedureStatsHourlySql, Window()),
        ];

        foreach (var (tag, sql, parameters) in statements)
        {
            var seen = await ActivityQueryAfterAsync(sql, parameters);
            Assert.StartsWith($"/* darling-export:{tag} */", seen.TrimStart(), StringComparison.Ordinal);
        }
    }

    // ---- the EXPLAIN gate script, run for real ------------------------------------------------------------

    private static readonly DateTime GateFrom = new(2026, 9, 27, 23, 0, 0, DateTimeKind.Utc);
    private static readonly Regex PlanRoot = new(@"^\s*(Seq Scan|Index Scan|Index Only Scan|Bitmap Heap Scan) ", RegexOptions.Multiline);

    [Fact]
    public void Gate_script_for_a_dimension_runs_the_schedule_check_and_plans_nothing()
    {
        if (Builder is null) { return; }
        var dimension = OptionalTables.All.First(t => t.Kind == OptionalTableKind.Dimension);

        var run = _store.Pg.Psql(OptionalGateScript.Build([dimension], GateFrom, GateFrom.AddHours(1), "store_a"));

        Assert.True(run.code == 0, run.output);
        Assert.Empty(PlanRoot.Matches(run.output));
    }

    [Fact]
    public void Gate_script_for_a_window_table_runs_clean_reports_the_schedule_and_plans_the_literal_window()
    {
        if (Builder is null) { return; }
        var table = OptionalTables.TryGet("memory_grant_stats")!;

        var run = _store.Pg.Psql(OptionalGateScript.Build([table], GateFrom, GateFrom.AddHours(1), "store_a"));

        Assert.True(run.code == 0, run.output);                                    // no @from/@to left unbound, no bad identifier
        Assert.Matches(@"SCHEDCOUNT\s*\|\s*memory_grant_stats\s*\|\s*2\s*\|\s*5\s*\|\s*10", run.output);
        Assert.Single(PlanRoot.Matches(run.output));
        Assert.Contains("2026-09-27 23:00:00", run.output);
        Assert.Contains("2026-09-28 00:00:00", run.output);
    }

    [Fact]
    public void Gate_script_for_the_whole_registry_plans_exactly_one_statement_per_non_dimension_table()
    {
        if (Builder is null) { return; }

        var run = _store.Pg.Psql(OptionalGateScript.Build(OptionalTables.All, GateFrom, GateFrom.AddHours(1), "all"));

        Assert.True(run.code == 0, run.output);
        Assert.Equal(OptionalTables.All.Count(t => t.Kind != OptionalTableKind.Dimension), PlanRoot.Matches(run.output).Count);
    }
}
