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
        long exec = 0, long worker = 0, long spills = 0, string planDigest = "NULL", string textDigest = "NULL") =>
        $"INSERT INTO query_stats_raw (server_name, database_name, query_hash, query_plan_hash, collection_time, sample_interval_seconds, " +
        $"delta_execution_count, delta_worker_time, delta_spills, query_plan_digest, query_text_digest) VALUES " +
        $"('{server}', '{db}', '{hash}', '{planHash}', TIMESTAMP '{time}', {interval ?? "NULL"}, {exec}, {worker}, {spills}, {planDigest}, {textDigest})";

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
