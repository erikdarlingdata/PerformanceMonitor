using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Runs the exporter's DuckDB-side SQL (the hourly rollups, the dimension keys, the Query Store dedup, the
/// pg_statement_text key set) against planted rows and asserts what comes back. The database holds only the
/// raw tables these statements read, so a statement that reached for a store table would fail here.
/// </summary>
public sealed class DuckDbSqlBehaviourTests
{
    private static DuckDBConnection Open()
    {
        var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        return connection;
    }

    private static void Exec(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static List<Dictionary<string, object?>> Rows(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var rows = new List<Dictionary<string, object?>>();
        while (reader.Read())
        {
            var row = new Dictionary<string, object?>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i) switch
                {
                    Stream blob => ReadAll(blob),
                    var v => v,
                };
            }
            rows.Add(row);
        }
        return rows;
    }

    private static byte[] ReadAll(Stream stream)
    {
        using var buffer = new MemoryStream();
        stream.CopyTo(buffer);
        return buffer.ToArray();
    }

    private static void CreateRaw(DuckDBConnection connection, string table, string[] columns, Func<string, string> typeFor) =>
        Exec(connection, $"CREATE TABLE {table} ({string.Join(", ", columns.Select(c => $"\"{c}\" {typeFor(c)}"))})");

    private static void CreateQueryStatsRaw(DuckDBConnection connection) =>
        CreateRaw(connection, "query_stats_raw", QueryStatsWindow.RawColumns, QueryStatsWindow.DuckDbTypeFor);

    private static void CreateProcedureStatsRaw(DuckDBConnection connection) =>
        CreateRaw(connection, "procedure_stats_raw", ProcedureStatsWindow.RawColumns, ProcedureStatsWindow.DuckDbTypeFor);

    private static string QueryStatsRow(string server, string db, string hash, string planHash, string time, string? interval,
        long exec = 0, long worker = 0, long spills = 0, string planDigest = "NULL", string textDigest = "NULL", string? host = null) =>
        $"INSERT INTO query_stats_raw (server_name, database_name, query_hash, query_plan_hash, collection_time, sample_interval_seconds, " +
        $"delta_execution_count, delta_worker_time, delta_spills, query_plan_digest, query_text_digest, host_object_name) VALUES " +
        $"('{server}', '{db}', '{hash}', '{planHash}', TIMESTAMP '{time}', {interval ?? "NULL"}, {exec}, {worker}, {spills}, {planDigest}, {textDigest}, {(host is null ? "NULL" : $"'{host}'")})";

    private static string ProcRow(string server, string time, string? interval, long exec, long spills) =>
        $"INSERT INTO procedure_stats_raw (server_name, database_name, schema_name, object_name, collection_time, sample_interval_seconds, " +
        $"delta_execution_count, delta_spills) VALUES ('{server}', 'db1', 'dbo', 'proc1', TIMESTAMP '{time}', {interval ?? "NULL"}, {exec}, {spills})";

    [Fact]
    public void QueryStatsHourly_leaves_interval_zero_rows_out_of_every_sum_and_the_sample_count()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        // Same hour, same key: a real 60 s sample and a sample the collector wrote with interval 0 (a counter reset).
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:00:00", "60", exec: 5, worker: 50, spills: 2));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:05:00", "0", exec: 100, worker: 9000, spills: 40));

        var rows = Rows(connection, SqlBuilder.QueryStatsHourlyDuckDbSql);

        var row = Assert.Single(rows);
        Assert.Equal(5L, row["execution_count_sum"]);
        Assert.Equal(50L, row["worker_time_sum"]);
        Assert.Equal(50L, row["worker_time_max"]);
        Assert.Equal(2L, row["spills_sum"]);
        Assert.Equal(60L, row["sample_interval_seconds_sum"]);
        Assert.Equal(1L, Convert.ToInt64(row["sample_count"]));
    }

    [Fact]
    public void QueryStatsHourly_keeps_null_interval_rows_groups_by_plan_hash_and_hour_and_returns_bigint_sums_in_order()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        // Inserted out of order on purpose: srv-b first, and the later hour first.
        Exec(connection, QueryStatsRow("srv-b", "db1", "h1", "p1", "2026-10-22 10:10:00", "60", exec: 1));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 11:10:00", "60", exec: 2));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:10:00", "60", exec: 3));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:20:00", null, exec: 4));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p2", "2026-10-22 10:10:00", "60", exec: 8));

        var rows = Rows(connection, SqlBuilder.QueryStatsHourlyDuckDbSql);

        Assert.Equal(4, rows.Count);
        Assert.Equal(["srv-a", "srv-a", "srv-a", "srv-b"], rows.Select(r => (string)r["server_name"]!));
        var tenAmP1 = rows.Single(r => (string)r["server_name"]! == "srv-a" && (string)r["query_plan_hash"]! == "p1"
            && (DateTime)r["hour"]! == new DateTime(2026, 10, 22, 10, 0, 0));
        Assert.Equal(7L, tenAmP1["execution_count_sum"]);           // 3 + 4: the NULL-interval row is kept
        Assert.Equal(2L, Convert.ToInt64(tenAmP1["sample_count"]));
        Assert.IsType<long>(tenAmP1["execution_count_sum"]);        // a BIGINT, not a HUGEINT
        Assert.IsType<long>(tenAmP1["spills_sum"]);
        Assert.Contains(rows, r => (string)r["query_plan_hash"]! == "p2" && (long)r["execution_count_sum"]! == 8L);
        var hours = rows.Take(3).Select(r => (DateTime)r["hour"]!).ToList();
        Assert.Equal(hours.OrderBy(h => h), hours);   // within a server, hours ascend
    }

    [Fact]
    public void QueryStatsHourly_has_a_host_object_name_column_after_query_plan_hash()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:00:00", "60", exec: 1, host: "dbo.usp_orders"));

        using var command = connection.CreateCommand();
        command.CommandText = SqlBuilder.QueryStatsHourlyDuckDbSql;
        using var reader = command.ExecuteReader();
        var names = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
        Assert.Equal(names.IndexOf("query_plan_hash") + 1, names.IndexOf("host_object_name"));
        Assert.True(reader.Read());
        Assert.Equal("dbo.usp_orders", reader.GetString(names.IndexOf("host_object_name")));
    }

    [Fact]
    public void QueryStatsHourly_gives_one_row_per_host_object_for_the_same_query_and_plan_hash()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:00:00", "60", exec: 3, host: "dbo.usp_a"));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:05:00", "60", exec: 4, host: "dbo.usp_b"));

        var rows = Rows(connection, SqlBuilder.QueryStatsHourlyDuckDbSql);

        Assert.Equal(2, rows.Count);
        Assert.Equal(3L, rows.Single(r => (string?)r["host_object_name"] == "dbo.usp_a")["execution_count_sum"]);
        Assert.Equal(4L, rows.Single(r => (string?)r["host_object_name"] == "dbo.usp_b")["execution_count_sum"]);
    }

    [Fact]
    public void QueryStatsHourly_groups_null_host_object_rows_into_one_row()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:00:00", "60", exec: 3));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:05:00", "60", exec: 4));

        var row = Assert.Single(Rows(connection, SqlBuilder.QueryStatsHourlyDuckDbSql));

        Assert.Null(row["host_object_name"]);
        Assert.Equal(7L, row["execution_count_sum"]);
    }

    [Fact]
    public void ProcedureStatsHourly_leaves_interval_zero_rows_out_and_carries_no_rows_or_plan_hash_column()
    {
        using var connection = Open();
        CreateProcedureStatsRaw(connection);
        Exec(connection, ProcRow("srv-a", "2026-10-22 10:00:00", "60", exec: 5, spills: 2));
        Exec(connection, ProcRow("srv-a", "2026-10-22 10:05:00", "0", exec: 100, spills: 40));

        var rows = Rows(connection, SqlBuilder.ProcedureStatsHourlyDuckDbSql);

        var row = Assert.Single(rows);
        Assert.Equal(5L, row["execution_count_sum"]);
        Assert.Equal(2L, row["spills_sum"]);
        Assert.Equal(60L, row["sample_interval_seconds_sum"]);
        Assert.Equal(1L, Convert.ToInt64(row["sample_count"]));
        Assert.DoesNotContain("rows_sum", row.Keys);
        Assert.DoesNotContain("query_plan_hash", row.Keys);
    }

    // ---- the DMV attributes the hourly tables carry as the value of the hour's last counted sample -------------------

    /// <summary>One raw row with only the named columns set; every other column stays NULL.</summary>
    private static string RawRow(string table, params (string Column, string Literal)[] values) =>
        $"INSERT INTO {table} ({string.Join(", ", values.Select(v => v.Column))}) VALUES ({string.Join(", ", values.Select(v => v.Literal))})";

    private static (string, string)[] QueryStatsSample(string time, string? interval, string planHandle, string creation, int maxDop, long maxWorker, long deltaWorker) =>
    [
        ("server_name", "'srv-a'"), ("database_name", "'db1'"), ("query_hash", "'h1'"), ("query_plan_hash", "'p1'"),
        ("collection_time", $"TIMESTAMP '{time}'"), ("sample_interval_seconds", interval ?? "NULL"),
        ("delta_worker_time", deltaWorker.ToString()), ("max_worker_time", maxWorker.ToString()), ("max_dop", maxDop.ToString()),
        ("plan_handle", $"'{planHandle}'"), ("sql_handle", $"'0xS{planHandle}'"), ("creation_time", $"TIMESTAMP '{creation}'"),
    ];

    [Fact]
    public void QueryStatsHourly_carries_the_dmv_attributes_under_the_store_names_and_types()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        Exec(connection, RawRow("query_stats_raw",
            ("server_name", "'srv-a'"), ("database_name", "'db1'"), ("query_hash", "'h1'"), ("query_plan_hash", "'p1'"),
            ("collection_time", "TIMESTAMP '2026-10-22 10:00:00'"), ("sample_interval_seconds", "60"),
            ("creation_time", "TIMESTAMP '2026-10-20 08:00:00'"), ("last_execution_time", "TIMESTAMP '2026-10-22 09:59:00'"),
            ("plan_generation_num", "3"), ("sql_handle", "'0x02000000AA'"), ("plan_handle", "'0x06000500BB'"),
            ("statement_start_offset", "10"), ("statement_end_offset", "-1"),
            ("execution_count", "40"), ("total_worker_time", "4000"), ("total_clr_time", "7"),
            ("min_worker_time", "5"), ("max_worker_time", "900"), ("min_dop", "1"), ("max_dop", "8"),
            ("min_grant_kb", "64"), ("max_grant_kb", "2048"), ("min_spills", "0"), ("max_spills", "12"), ("query_plan_xml_bytes", "123456")));

        using var command = connection.CreateCommand();
        command.CommandText = SqlBuilder.QueryStatsHourlyDuckDbSql;
        using var reader = command.ExecuteReader();
        var types = Enumerable.Range(0, reader.FieldCount).ToDictionary(reader.GetName, reader.GetFieldType);
        Assert.True(reader.Read());
        object? Value(string column) => reader.GetValue(reader.GetOrdinal(column));

        Assert.Equal(typeof(DateTime), types["creation_time"]);
        Assert.Equal(typeof(DateTime), types["last_execution_time"]);
        Assert.Equal(typeof(long), types["plan_generation_num"]);
        Assert.Equal(typeof(string), types["sql_handle"]);          // the store's hex text, not converted
        Assert.Equal(typeof(string), types["plan_handle"]);
        Assert.Equal(typeof(int), types["statement_start_offset"]);
        Assert.Equal(typeof(long), types["max_dop"]);
        Assert.Equal("0x02000000AA", Value("sql_handle"));
        Assert.Equal("0x06000500BB", Value("plan_handle"));
        Assert.Equal(new DateTime(2026, 10, 20, 8, 0, 0), Value("creation_time"));
        Assert.Equal(-1, Value("statement_end_offset"));
        Assert.Equal(40L, Value("execution_count"));
        Assert.Equal(4000L, Value("total_worker_time"));
        Assert.Equal(7L, Value("total_clr_time"));
        Assert.Equal(900L, Value("max_worker_time"));
        Assert.Equal(8L, Value("max_dop"));
        Assert.Equal(2048L, Value("max_grant_kb"));
        Assert.Equal(12L, Value("max_spills"));
        Assert.Equal(123456L, Value("query_plan_xml_bytes"));      // the plan's size, the only record of one the collector's cap left out
        // The per-sample delta columns keep their names: worker_time_max is not max_worker_time.
        Assert.Contains("worker_time_max", types.Keys);
    }

    [Fact]
    public void QueryStatsHourly_takes_each_dmv_attribute_from_the_last_counted_sample_not_the_largest_or_the_first_inserted()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        // Inserted out of time order on purpose. The latest counted sample is 10:40; it has the SMALLEST max_dop and max_worker_time.
        Exec(connection, RawRow("query_stats_raw", QueryStatsSample("2026-10-22 10:40:00", "60", "0xNEW", "2026-10-22 10:30:00", maxDop: 2, maxWorker: 300, deltaWorker: 30)));
        Exec(connection, RawRow("query_stats_raw", QueryStatsSample("2026-10-22 10:10:00", "60", "0xOLD", "2026-10-20 08:00:00", maxDop: 8, maxWorker: 900, deltaWorker: 10)));
        Exec(connection, RawRow("query_stats_raw", QueryStatsSample("2026-10-22 10:25:00", "60", "0xMID", "2026-10-20 08:00:00", maxDop: 4, maxWorker: 500, deltaWorker: 20)));
        // A later sample the collector wrote with interval 0 is not counted, so its attributes are not the hour's either.
        Exec(connection, RawRow("query_stats_raw", QueryStatsSample("2026-10-22 10:55:00", "0", "0xRESET", "2026-10-22 10:54:00", maxDop: 99, maxWorker: 1, deltaWorker: 0)));

        var row = Assert.Single(Rows(connection, SqlBuilder.QueryStatsHourlyDuckDbSql));

        Assert.Equal("0xNEW", row["plan_handle"]);
        Assert.Equal("0xS0xNEW", row["sql_handle"]);
        Assert.Equal(new DateTime(2026, 10, 22, 10, 30, 0), row["creation_time"]);
        Assert.Equal(2L, row["max_dop"]);
        Assert.Equal(300L, row["max_worker_time"]);
        // The per-sample delta rollup is untouched: its max is the largest delta of the hour.
        Assert.Equal(30L, row["worker_time_max"]);
        Assert.Equal(60L, row["worker_time_sum"]);
    }

    [Fact]
    public void QueryStatsHourly_keeps_one_row_per_hour_when_the_dmv_attributes_differ_between_the_samples()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        // Three samples of one query and plan hash in one hour that disagree on every attribute (the plan was recompiled twice).
        Exec(connection, RawRow("query_stats_raw", QueryStatsSample("2026-10-22 10:05:00", "60", "0xA", "2026-10-22 09:00:00", 1, 10, 1)));
        Exec(connection, RawRow("query_stats_raw", QueryStatsSample("2026-10-22 10:20:00", "60", "0xB", "2026-10-22 10:10:00", 2, 20, 2)));
        Exec(connection, RawRow("query_stats_raw", QueryStatsSample("2026-10-22 10:35:00", "60", "0xC", "2026-10-22 10:30:00", 3, 30, 3)));
        // The next hour has its own row.
        Exec(connection, RawRow("query_stats_raw", QueryStatsSample("2026-10-22 11:05:00", "60", "0xD", "2026-10-22 10:50:00", 4, 40, 4)));

        var rows = Rows(connection, SqlBuilder.QueryStatsHourlyDuckDbSql);

        Assert.Equal(2, rows.Count);
        Assert.Equal(3L, Convert.ToInt64(rows[0]["sample_count"]));
        Assert.Equal("0xC", rows[0]["plan_handle"]);
        Assert.Equal("0xD", rows[1]["plan_handle"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void QueryStatsHourly_takes_every_attribute_from_one_row_when_several_plans_share_the_last_collection_time(bool insertBFirst)
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        // Two plans (SET-option variants) of one statement collected at the same instant: the grain does not tell them apart.
        // B sorts after A on plan_handle, so B is the hour's row, whatever order the rows arrived in.
        (string, string)[] Plan(string planHandle, string sqlHandle, int maxDop, long totalWorker, string creation) =>
        [
            ("server_name", "'srv-a'"), ("database_name", "'db1'"), ("query_hash", "'h1'"), ("query_plan_hash", "'p1'"),
            ("collection_time", "TIMESTAMP '2026-10-22 10:40:00'"), ("sample_interval_seconds", "60"),
            ("plan_handle", $"'{planHandle}'"), ("sql_handle", $"'{sqlHandle}'"), ("max_dop", maxDop.ToString()),
            ("total_worker_time", totalWorker.ToString()), ("creation_time", $"TIMESTAMP '{creation}'"),
            ("statement_start_offset", "0"), ("statement_end_offset", "-1"),
        ];
        var a = RawRow("query_stats_raw", Plan("0xA", "0xSQL1", 1, 100, "2026-10-01 00:00:00"));
        var b = RawRow("query_stats_raw", Plan("0xB", "0xSQL2", 16, 9000, "2026-10-20 00:00:00"));
        Exec(connection, insertBFirst ? b : a);
        Exec(connection, insertBFirst ? a : b);

        var row = Assert.Single(Rows(connection, SqlBuilder.QueryStatsHourlyDuckDbSql));

        Assert.Equal(2L, Convert.ToInt64(row["sample_count"]));
        Assert.Equal("0xB", row["plan_handle"]);
        Assert.Equal("0xSQL2", row["sql_handle"]);
        Assert.Equal(16L, row["max_dop"]);
        Assert.Equal(9000L, row["total_worker_time"]);
        Assert.Equal(new DateTime(2026, 10, 20, 0, 0, 0), row["creation_time"]);
    }

    [Fact]
    public void QueryStatsHourly_breaks_a_tie_on_handles_and_offsets_with_null_sorting_before_a_value()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        (string, string)[] Statement(string? planHandle, string? sqlHandle, int start, int maxDop) =>
        [
            ("server_name", "'srv-a'"), ("database_name", "'db1'"), ("query_hash", "'h1'"), ("query_plan_hash", "'p1'"),
            ("collection_time", "TIMESTAMP '2026-10-22 10:40:00'"), ("sample_interval_seconds", "60"),
            ("plan_handle", planHandle is null ? "NULL" : $"'{planHandle}'"), ("sql_handle", sqlHandle is null ? "NULL" : $"'{sqlHandle}'"),
            ("statement_start_offset", start.ToString()), ("statement_end_offset", "-1"), ("max_dop", maxDop.ToString()),
        ];
        // Same plan_handle and sql_handle, different statement offsets inside one batch; and a row with no handles at all.
        Exec(connection, RawRow("query_stats_raw", Statement("0xP", "0xS", 40, 4)));
        Exec(connection, RawRow("query_stats_raw", Statement("0xP", "0xS", 120, 8)));
        Exec(connection, RawRow("query_stats_raw", Statement(null, null, 500, 99)));

        var row = Assert.Single(Rows(connection, SqlBuilder.QueryStatsHourlyDuckDbSql));

        Assert.Equal(8L, row["max_dop"]);                 // offset 120 beats 40, and the handle-less row sorts first
        Assert.Equal(120, row["statement_start_offset"]);
    }

    [Fact]
    public void QueryStatsHourly_reports_a_null_attribute_of_the_last_sample_as_null_not_an_older_samples_value()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        (string, string)[] Sample(string time, string? planBytes) =>
        [
            ("server_name", "'srv-a'"), ("database_name", "'db1'"), ("query_hash", "'h1'"), ("query_plan_hash", "'p1'"),
            ("collection_time", $"TIMESTAMP '{time}'"), ("sample_interval_seconds", "60"),
            ("query_plan_xml_bytes", planBytes ?? "NULL"), ("max_dop", planBytes is null ? "NULL" : "4"),
        ];
        Exec(connection, RawRow("query_stats_raw", Sample("2026-10-22 10:10:00", "5000")));
        Exec(connection, RawRow("query_stats_raw", Sample("2026-10-22 10:50:00", null)));

        var row = Assert.Single(Rows(connection, SqlBuilder.QueryStatsHourlyDuckDbSql));

        Assert.Null(row["query_plan_xml_bytes"]);
        Assert.Null(row["max_dop"]);
    }

    [Fact]
    public void ProcedureStatsHourly_takes_every_attribute_from_one_row_when_several_cached_plans_share_the_last_collection_time()
    {
        using var connection = Open();
        CreateProcedureStatsRaw(connection);
        (string, string)[] Plan(string planHandle, string sqlHandle, long maxWorker, string cached) =>
        [
            ("server_name", "'srv-a'"), ("database_name", "'db1'"), ("schema_name", "'dbo'"), ("object_name", "'proc1'"),
            ("collection_time", "TIMESTAMP '2026-10-22 10:40:00'"), ("sample_interval_seconds", "60"),
            ("plan_handle", $"'{planHandle}'"), ("sql_handle", $"'{sqlHandle}'"), ("max_worker_time", maxWorker.ToString()),
            ("cached_time", $"TIMESTAMP '{cached}'"),
        ];
        Exec(connection, RawRow("procedure_stats_raw", Plan("0xB", "0xSQL2", 900, "2026-10-20 00:00:00")));
        Exec(connection, RawRow("procedure_stats_raw", Plan("0xA", "0xSQL1", 10, "2026-10-01 00:00:00")));

        var row = Assert.Single(Rows(connection, SqlBuilder.ProcedureStatsHourlyDuckDbSql));

        Assert.Equal("0xB", row["plan_handle"]);
        Assert.Equal("0xSQL2", row["sql_handle"]);
        Assert.Equal(900L, row["max_worker_time"]);
        Assert.Equal(new DateTime(2026, 10, 20, 0, 0, 0), row["cached_time"]);
    }

    [Fact]
    public void ProcedureStatsHourly_carries_the_dmv_attributes_of_the_last_counted_sample_and_keeps_one_row_per_hour()
    {
        using var connection = Open();
        CreateProcedureStatsRaw(connection);
        (string, string)[] Sample(string time, string interval, string type, string handle, string modified, long maxWorker, long maxReads) =>
        [
            ("server_name", "'srv-a'"), ("database_name", "'db1'"), ("schema_name", "'dbo'"), ("object_name", "'proc1'"),
            ("collection_time", $"TIMESTAMP '{time}'"), ("sample_interval_seconds", interval),
            ("object_type", $"'{type}'"), ("plan_handle", $"'{handle}'"), ("sql_handle", $"'0xS{handle}'"),
            ("cached_time", $"TIMESTAMP '{modified}'"), ("module_modify_date", $"TIMESTAMP '{modified}'"),
            ("max_worker_time", maxWorker.ToString()), ("max_logical_reads", maxReads.ToString()), ("min_spills", "0"), ("execution_count", "5"), ("query_plan_xml_bytes", (maxReads * 1000).ToString()),
        ];
        Exec(connection, RawRow("procedure_stats_raw", Sample("2026-10-22 10:30:00", "60", "P", "0xNEW", "2026-10-22 10:00:00", maxWorker: 3, maxReads: 7)));
        Exec(connection, RawRow("procedure_stats_raw", Sample("2026-10-22 10:05:00", "60", "PC", "0xOLD", "2026-10-01 00:00:00", maxWorker: 99, maxReads: 99)));
        Exec(connection, RawRow("procedure_stats_raw", Sample("2026-10-22 10:50:00", "0", "X", "0xRESET", "2026-10-22 10:49:00", maxWorker: 1, maxReads: 1)));

        using var command = connection.CreateCommand();
        command.CommandText = SqlBuilder.ProcedureStatsHourlyDuckDbSql;
        using var reader = command.ExecuteReader();
        var types = Enumerable.Range(0, reader.FieldCount).ToDictionary(reader.GetName, reader.GetFieldType);
        Assert.Equal(typeof(string), types["object_type"]);
        Assert.Equal(typeof(string), types["sql_handle"]);
        Assert.Equal(typeof(DateTime), types["cached_time"]);
        Assert.Equal(typeof(DateTime), types["module_modify_date"]);
        Assert.Equal(typeof(long), types["max_logical_reads"]);
        Assert.True(reader.Read());
        object? Value(string column) => reader.GetValue(reader.GetOrdinal(column));
        Assert.Equal("P", Value("object_type"));
        Assert.Equal("0xNEW", Value("plan_handle"));
        Assert.Equal("0xS0xNEW", Value("sql_handle"));
        Assert.Equal(new DateTime(2026, 10, 22, 10, 0, 0), Value("cached_time"));
        Assert.Equal(new DateTime(2026, 10, 22, 10, 0, 0), Value("module_modify_date"));
        Assert.Equal(3L, Value("max_worker_time"));
        Assert.Equal(7L, Value("max_logical_reads"));
        Assert.Equal(7000L, Value("query_plan_xml_bytes"));
        Assert.False(reader.Read());      // one row for the key and hour, however many plans the samples saw
    }

    [Fact]
    public void QueryStoreDedup_carries_the_interval_end_time_of_the_latest_snapshot()
    {
        using var connection = Open();
        CreateRaw(connection, "qs_raw", QueryStoreWindow.RawColumns, QueryStoreWindow.DuckDbTypeFor);
        string Row(string collected, string? end) =>
            "INSERT INTO qs_raw (server_name, database_name, query_id, plan_id, runtime_stats_interval_id, execution_type_desc, replica_role, " +
            $"interval_start_time_utc, interval_end_time_utc, collection_time) VALUES ('srv-a', 'db1', 1, 2, 3, 'Regular', 'PRIMARY', " +
            $"TIMESTAMP '2026-10-22 11:00:00', {(end is null ? "NULL" : $"TIMESTAMP '{end}'")}, TIMESTAMP '{collected}')";
        Exec(connection, Row("2026-10-22 11:35:00", "2026-10-22 12:00:00"));
        Exec(connection, Row("2026-10-22 11:05:00", null));          // an earlier snapshot, taken before the end time was known

        var row = Assert.Single(Rows(connection, SqlBuilder.BuildQueryStoreDedupSql("qs_raw")));

        Assert.Equal(new DateTime(2026, 10, 22, 12, 0, 0), row["interval_end_time_utc"]);
        Assert.IsType<DateTime>(row["interval_start_time_utc"]);
    }

    [Fact]
    public void PlanShapeKey_groups_per_server_and_plan_hash_skips_null_digests_and_picks_the_byte_smallest_digest()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        Exec(connection, QueryStatsRow("srv-b", "db1", "h1", "p1", "2026-10-22 10:00:00", "60", exec: 1, planDigest: "'\\x80'::BLOB"));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:00:00", "60", exec: 2, worker: 20, planDigest: "'\\x80'::BLOB"));
        Exec(connection, QueryStatsRow("srv-a", "db2", "h2", "p1", "2026-10-22 10:30:00", "60", exec: 3, worker: 30, planDigest: "'\\x7F'::BLOB"));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:45:00", "60", exec: 4, planDigest: "NULL"));   // NULL digest: excluded
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "pX", "2026-10-22 10:45:00", "60", exec: 99, planDigest: "NULL"));  // only NULL digests: no key
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p3", "2026-10-22 10:50:00", "60", exec: 6, planDigest: "'\\x01'::BLOB"));

        var rows = Rows(connection, SqlBuilder.PlanShapeKeyDuckDbSql);

        Assert.Equal(3, rows.Count);
        Assert.Equal([("srv-a", "p1"), ("srv-a", "p3"), ("srv-b", "p1")], rows.Select(r => ((string)r["server_name"]!, (string)r["query_plan_hash"]!)));
        var p1 = rows[0];
        Assert.Equal(new byte[] { 0x7F }, (byte[])p1["example_digest"]!);   // unsigned byte order: 0x7F < 0x80
        Assert.Equal(new DateTime(2026, 10, 22, 10, 0, 0), p1["first_seen"]);
        Assert.Equal(new DateTime(2026, 10, 22, 10, 30, 0), p1["last_seen"]);
        Assert.Equal(5L, p1["executions"]);
        Assert.Equal(50L, p1["cpu_us"]);
    }

    [Fact]
    public void QueryTextKey_groups_per_server_database_and_query_hash_skips_null_digests_and_picks_the_byte_smallest_digest()
    {
        using var connection = Open();
        CreateQueryStatsRaw(connection);
        Exec(connection, QueryStatsRow("srv-a", "db2", "h1", "p1", "2026-10-22 10:00:00", "60", textDigest: "'\\x01'::BLOB"));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p1", "2026-10-22 10:10:00", "60", textDigest: "'\\x80'::BLOB"));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p2", "2026-10-22 10:40:00", "60", textDigest: "'\\x7F'::BLOB"));
        Exec(connection, QueryStatsRow("srv-a", "db1", "h1", "p2", "2026-10-22 10:50:00", "60", textDigest: "NULL"));
        Exec(connection, QueryStatsRow("srv-a", "db1", "hNull", "p2", "2026-10-22 10:50:00", "60", textDigest: "NULL"));

        var rows = Rows(connection, SqlBuilder.QueryTextKeyDuckDbSql);

        Assert.Equal(2, rows.Count);
        Assert.Equal([("db1", "h1"), ("db2", "h1")], rows.Select(r => ((string)r["database_name"]!, (string)r["query_hash"]!)));
        Assert.Equal(new byte[] { 0x7F }, (byte[])rows[0]["example_digest"]!);
        Assert.Equal(new DateTime(2026, 10, 22, 10, 10, 0), rows[0]["first_seen"]);
        Assert.Equal(new DateTime(2026, 10, 22, 10, 40, 0), rows[0]["last_seen"]);   // the NULL-digest row at 10:50 does not extend the key
    }

    [Fact]
    public void QueryStoreDedup_keeps_the_latest_snapshot_per_interval_identity_and_counts_the_snapshots()
    {
        using var connection = Open();
        CreateRaw(connection, "qs_raw", QueryStoreWindow.RawColumns, QueryStoreWindow.DuckDbTypeFor);
        string Row(string role, string collected, int executions) =>
            "INSERT INTO qs_raw (server_name, database_name, query_id, plan_id, runtime_stats_interval_id, execution_type_desc, replica_role, " +
            $"execution_count, collection_time) VALUES ('srv-a', 'db1', 1, 2, 3, 'Regular', '{role}', {executions}, TIMESTAMP '{collected}')";
        Exec(connection, Row("PRIMARY", "2026-10-22 11:05:00", 1));
        Exec(connection, Row("PRIMARY", "2026-10-22 11:35:00", 9));   // later snapshot of the same identity
        Exec(connection, Row("PRIMARY", "2026-10-22 11:20:00", 4));
        Exec(connection, Row("SECONDARY", "2026-10-22 11:05:00", 7)); // a different replica role is a different identity

        var rows = Rows(connection, SqlBuilder.BuildQueryStoreDedupSql("qs_raw"));

        Assert.Equal(2, rows.Count);
        var primary = rows.Single(r => (string)r["replica_role"]! == "PRIMARY");
        Assert.Equal(9L, Convert.ToInt64(primary["execution_count"]));
        Assert.Equal(new DateTime(2026, 10, 22, 11, 35, 0), primary["last_collection_time"]);
        Assert.Equal(3L, Convert.ToInt64(primary["snapshot_count"]));
        Assert.Equal(1L, Convert.ToInt64(rows.Single(r => (string)r["replica_role"]! == "SECONDARY")["snapshot_count"]));
        Assert.DoesNotContain("collection_time", primary.Keys);
        Assert.DoesNotContain("rn", primary.Keys);
    }

    [Fact]
    public void PgStatementText_key_set_is_the_distinct_non_null_keys_and_the_gate_passes_on_calls_or_exec_time_at_the_threshold()
    {
        using var connection = Open();
        Exec(connection, "CREATE TABLE stats_t (server_name VARCHAR, queryid BIGINT, calls_last BIGINT, total_exec_time_ms_last DOUBLE)");
        Exec(connection, """
            INSERT INTO stats_t VALUES
              ('srv-a', 1, 1, 99.9), ('srv-a', 1, 0, 10),          -- below both thresholds
              ('srv-a', 2, 2, 0), ('srv-a', 2, 1, 0),              -- calls reaches 2 on one row
              ('srv-a', 3, 0, 100.0),                              -- exec time reaches 100
              ('srv-a', 4, NULL, NULL),                            -- no numbers at all
              ('srv-a', NULL, 50, 5000), ('srv-b', 1, 5, 5)        -- NULL queryid dropped; same queryid on another server is its own key
            """);

        var plain = Rows(connection, PgStatementTextExporter.KeySetSql("stats_t"));
        Assert.Equal(5, plain.Count);
        Assert.All(plain, r => Assert.True((bool)r["passes"]!));
        Assert.DoesNotContain(plain, r => r["queryid"] is null);

        var gated = Rows(connection, PgStatementTextExporter.KeySetSql("stats_t", applyGate: true))
            .ToDictionary(r => ((string)r["server_name"]!, (long)r["queryid"]!), r => (bool)r["passes"]!);
        Assert.Equal(5, gated.Count);
        Assert.False(gated[("srv-a", 1)]);
        Assert.True(gated[("srv-a", 2)]);
        Assert.True(gated[("srv-a", 3)]);
        Assert.False(gated[("srv-a", 4)]);
        Assert.True(gated[("srv-b", 1)]);
    }
}
