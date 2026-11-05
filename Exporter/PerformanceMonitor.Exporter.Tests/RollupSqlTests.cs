using System.Text.RegularExpressions;
using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class RollupSqlTests
{
    private static string FixturePath(string name) =>
        Path.Combine(AppContext.BaseDirectory, "Fixtures", "RollupGrains", name);

    private static string ReadFixture(string name) => File.ReadAllText(FixturePath(name));

    private static DuckDBConnection OpenConnection()
    {
        var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        Exec(connection, "CREATE SCHEMA collect");
        return connection;
    }

    private static void Exec(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static object?[] Row1(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        var row = new object?[reader.FieldCount];
        for (var i = 0; i < reader.FieldCount; i++)
        {
            row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
        }
        return row;
    }

    private static long ToLong(object? value) => value switch
    {
        null => 0,
        System.Numerics.BigInteger b => (long)b,
        _ => Convert.ToInt64(value),
    };

    private static int RowCount(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT count(*) FROM ({sql}) t";
        return Convert.ToInt32(command.ExecuteScalar());
    }

    // -- Every one of the 13 specs from design doc §2.3.3 exists. --

    [Fact]
    public void Specs_has_all_13_hourly_tables()
    {
        var expected = new[]
        {
            "memory_grant_stats_hourly", "cpu_scheduler_stats_hourly", "session_summary_stats_hourly",
            "ag_database_replica_states_hourly", "pg_statement_stats_hourly", "pg_database_stats_hourly",
            "pg_wait_stats_hourly", "pg_io_stats_hourly", "pg_cpu_utilization_hourly", "pg_write_stats_hourly",
            "pg_replication_stats_hourly", "pg_replication_slot_stats_hourly", "pg_xmin_horizon_hourly",
        };

        Assert.Equal(expected.Length, RollupSql.Specs.Count);
        foreach (var name in expected)
        {
            Assert.True(RollupSql.Specs.ContainsKey(name), $"missing spec: {name}");
        }
    }

    [Fact]
    public void Unknown_spec_name_throws()
    {
        Assert.Throws<ArgumentException>(() => RollupSql.Build("not_a_real_spec"));
    }

    // -- 12 planted samples give exact avg, max and last. --

    [Fact]
    public void Gauge_and_gaugemin_rules_give_exact_avg_max_min_last()
    {
        using var connection = OpenConnection();
        Exec(connection, ReadFixture("sqlserver-tables.sql"));
        Exec(connection, "CREATE TABLE raw AS SELECT * FROM collect.memory_grant_stats LIMIT 0");

        for (var i = 1; i <= 12; i++)
        {
            Exec(connection, $"""
                INSERT INTO raw
                    (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id,
                     granted_memory_mb, available_memory_mb, timeout_error_count_delta, forced_grant_count_delta,
                     timeout_error_count, forced_grant_count, sample_interval_seconds)
                VALUES ({i}, TIMESTAMP '2026-09-28 10:00:00' + INTERVAL '{i}' MINUTE, 1, 'srv1', 0, 1,
                        {i * 10}.0, {i * 5}.0, 1, 0, {i}, 0, 60)
                """);
        }

        var sql = RollupSql.Build("memory_grant_stats_hourly");
        var row = Row1(connection, sql);
        var columns = ColumnsOf(connection, sql);

        Assert.Equal(65.0, Convert.ToDouble(row[columns["granted_memory_mb_avg"]]), 3); // avg(10..120 step 10)
        Assert.Equal(120.0, Convert.ToDouble(row[columns["granted_memory_mb_max"]]), 3);
        Assert.Equal(120.0, Convert.ToDouble(row[columns["granted_memory_mb_last"]]), 3);
        Assert.Equal(32.5, Convert.ToDouble(row[columns["available_memory_mb_avg"]]), 3); // avg(5..60 step 5)
        Assert.Equal(5.0, Convert.ToDouble(row[columns["available_memory_mb_min"]]), 3);
        Assert.Equal(60.0, Convert.ToDouble(row[columns["available_memory_mb_last"]]), 3);
        Assert.Equal(12, Convert.ToInt32(row[columns["sample_count"]]));
        Assert.Equal(720L, ToLong(row[columns["sample_interval_seconds_sum"]]));  // 12 samples of 60 s, a BIGINT sum
        Assert.Equal(["server_name", "resource_semaphore_id", "pool_id", "hour"], new[] { "server_name", "resource_semaphore_id", "pool_id", "hour" }.Where(columns.ContainsKey));
    }

    private static Dictionary<string, int> ColumnsOf(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var map = new Dictionary<string, int>();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            map[reader.GetName(i)] = i;
        }
        return map;
    }

    // -- delta_sum excludes rows with sample_interval_seconds = 0. --

    [Fact]
    public void DeltaSum_excludes_interval_zero_rows()
    {
        using var connection = OpenConnection();
        Exec(connection, ReadFixture("sqlserver-tables.sql"));
        Exec(connection, "CREATE TABLE raw AS SELECT * FROM collect.memory_grant_stats LIMIT 0");
        Exec(connection, """
            INSERT INTO raw
                (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id,
                 granted_memory_mb, available_memory_mb, timeout_error_count_delta, forced_grant_count_delta,
                 timeout_error_count, forced_grant_count, sample_interval_seconds)
            VALUES
                (1, TIMESTAMP '2026-09-28 10:00:00', 1, 'srv1', 0, 1, 1.0, 1.0, 5, 0, 5, 0, 60),
                (2, TIMESTAMP '2026-09-28 10:05:00', 1, 'srv1', 0, 1, 1.0, 1.0, 100, 0, 105, 0, 0)
            """);

        var sql = RollupSql.Build("memory_grant_stats_hourly");
        var row = Row1(connection, sql);
        var columns = ColumnsOf(connection, sql);

        Assert.Equal(5L, ToLong(row[columns["timeout_error_count_delta_sum"]]));
        Assert.Equal(60L, ToLong(row[columns["sample_interval_seconds_sum"]]));
        Assert.Equal(2, Convert.ToInt32(row[columns["sample_count"]]));   // the interval-0 sample is still a sample, only its delta is left out
    }

    // -- A counter reset: 100, 150, 20, 60 -> _delta_in_hour 110, _first 100, _last 60. --

    [Fact]
    public void Counter_rule_is_reset_safe()
    {
        using var connection = OpenConnection();
        Exec(connection, ReadFixture("postgres-tables.sql"));
        Exec(connection, "CREATE TABLE raw AS SELECT * FROM collect.pg_database_stats LIMIT 0");

        long[] values = [100, 150, 20, 60];
        for (var i = 0; i < values.Length; i++)
        {
            Exec(connection, $"""
                INSERT INTO raw
                    (collection_id, collection_time, server_id, server_name, database_name,
                     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks,
                     stats_reset, numbackends)
                VALUES ({i}, TIMESTAMP '2026-09-28 10:00:00' + INTERVAL '{i * 10}' MINUTE, 1, 'srv1', 'db1',
                        {values[i]}, 0, 0, 0, 0, 0, 0, NULL, 1)
                """);
        }

        var sql = RollupSql.Build("pg_database_stats_hourly");
        var row = Row1(connection, sql);
        var columns = ColumnsOf(connection, sql);

        Assert.Equal(100L, ToLong(row[columns["xact_commit_first"]]));
        Assert.Equal(60L, ToLong(row[columns["xact_commit_last"]]));
        Assert.Equal(110L, ToLong(row[columns["xact_commit_delta_in_hour"]]));
    }

    // -- One row per (server, key, hour) across 2 servers. --

    [Fact]
    public void Grouping_gives_one_row_per_server_key_and_hour()
    {
        using var connection = OpenConnection();
        Exec(connection, ReadFixture("postgres-tables.sql"));
        Exec(connection, "CREATE TABLE raw AS SELECT * FROM collect.pg_wait_stats LIMIT 0");
        Exec(connection, """
            INSERT INTO raw
                (collection_id, collection_time, server_id, server_name, wait_type, wait_event,
                 waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds)
            VALUES
                (1, TIMESTAMP '2026-09-28 10:00:00', 1, 'srv1', 'IO', 'DataFileRead', 10, 100, 1, 10, 60),
                (2, TIMESTAMP '2026-09-28 10:05:00', 1, 'srv1', 'IO', 'DataFileRead', 11, 110, 1, 10, 60),
                (3, TIMESTAMP '2026-09-28 10:00:00', 2, 'srv2', 'IO', 'DataFileRead', 20, 200, 1, 10, 60),
                (4, TIMESTAMP '2026-09-28 11:00:00', 1, 'srv1', 'IO', 'DataFileRead', 12, 120, 1, 10, 60)
            """);

        var sql = RollupSql.Build("pg_wait_stats_hourly");
        Assert.Equal(3, RowCount(connection, sql)); // (srv1, 10:00), (srv2, 10:00), (srv1, 11:00)
    }

    // -- CountFalse (pg_replication_slot_stats_hourly.inactive_samples). --

    [Fact]
    public void CountFalse_counts_rows_where_the_flag_is_false()
    {
        using var connection = OpenConnection();
        Exec(connection, ReadFixture("postgres-tables.sql"));
        Exec(connection, "CREATE TABLE raw AS SELECT * FROM collect.pg_replication_slot_stats LIMIT 0");
        Exec(connection, """
            INSERT INTO raw
                (collection_id, collection_time, server_id, server_name, slot_name, is_active,
                 retained_wal_bytes, xmin_age, catalog_xmin_age, safe_wal_size_bytes,
                 slot_type, plugin, database_name, wal_status, invalidation_reason, conflicting, inactive_since)
            VALUES
                (1, TIMESTAMP '2026-09-28 10:00:00', 1, 'srv1', 'slot1', true, 1, 1, 1, 1, 'physical', NULL, 'db1', 'reserved', NULL, false, NULL),
                (2, TIMESTAMP '2026-09-28 10:05:00', 1, 'srv1', 'slot1', false, 2, 2, 2, 2, 'physical', NULL, 'db1', 'reserved', NULL, false, NULL),
                (3, TIMESTAMP '2026-09-28 10:10:00', 1, 'srv1', 'slot1', false, 3, 3, 3, 3, 'physical', NULL, 'db1', 'reserved', NULL, false, NULL)
            """);

        var sql = RollupSql.Build("pg_replication_slot_stats_hourly");
        var row = Row1(connection, sql);
        var columns = ColumnsOf(connection, sql);

        Assert.Equal(2L, ToLong(row[columns["inactive_samples"]]));
    }

    // -- ArgMaxBy (pg_xmin_horizon_hourly.holder_at_max / detail_at_max). --

    [Fact]
    public void ArgMaxBy_picks_the_row_with_the_largest_order_column()
    {
        using var connection = OpenConnection();
        Exec(connection, ReadFixture("postgres-tables.sql"));
        Exec(connection, "CREATE TABLE raw AS SELECT * FROM collect.pg_xmin_horizon LIMIT 0");
        Exec(connection, """
            INSERT INTO raw
                (collection_id, collection_time, server_id, server_name, source, xmin_age, holder, detail, is_winner)
            VALUES
                (1, TIMESTAMP '2026-09-28 10:00:00', 1, 'srv1', 'replication_slot', 100, 'small-holder', 'small-detail', false),
                (2, TIMESTAMP '2026-09-28 10:05:00', 1, 'srv1', 'replication_slot', 500, 'big-holder', 'big-detail', true)
            """);

        var sql = RollupSql.Build("pg_xmin_horizon_hourly");
        var row = Row1(connection, sql);
        var columns = ColumnsOf(connection, sql);

        Assert.Equal(500L, ToLong(row[columns["xmin_age_max"]]));
        Assert.Equal("big-holder", row[columns["holder_at_max"]]);
        Assert.Equal("big-detail", row[columns["detail_at_max"]]);
        Assert.Equal(1L, ToLong(row[columns["is_winner_samples"]]));
    }

    // -- Every spec's SQL executes against an empty raw table built from the dump DDL. --

    public static IEnumerable<object[]> AllSpecNames() =>
        RollupSql.Specs.Keys.Select(name => new object[] { name });

    private static readonly Dictionary<string, string> SourceTableByOutput = new()
    {
        ["memory_grant_stats_hourly"] = "memory_grant_stats",
        ["cpu_scheduler_stats_hourly"] = "cpu_scheduler_stats",
        ["session_summary_stats_hourly"] = "session_summary_stats",
        ["ag_database_replica_states_hourly"] = "ag_database_replica_states",
        ["pg_statement_stats_hourly"] = "pg_statement_stats",
        ["pg_database_stats_hourly"] = "pg_database_stats",
        ["pg_wait_stats_hourly"] = "pg_wait_stats",
        ["pg_io_stats_hourly"] = "pg_io_stats",
        ["pg_cpu_utilization_hourly"] = "pg_cpu_utilization",
        ["pg_write_stats_hourly"] = "pg_write_stats",
        ["pg_replication_stats_hourly"] = "pg_replication_stats",
        ["pg_replication_slot_stats_hourly"] = "pg_replication_slot_stats",
        ["pg_xmin_horizon_hourly"] = "pg_xmin_horizon",
    };

    [Theory]
    [MemberData(nameof(AllSpecNames))]
    public void Every_spec_executes_against_an_empty_raw_table_from_the_dump_ddl(string specName)
    {
        using var connection = OpenConnection();
        Exec(connection, ReadFixture("sqlserver-tables.sql"));
        Exec(connection, ReadFixture("postgres-tables.sql"));

        var sourceTable = SourceTableByOutput[specName];
        Exec(connection, $"CREATE TABLE raw AS SELECT * FROM collect.\"{sourceTable}\" LIMIT 0");

        var sql = RollupSql.Build(specName);
        Assert.Equal(0, RowCount(connection, sql));
    }

    // -- Every spec column exists in the fixture or dump DDL. --

    [Theory]
    [MemberData(nameof(AllSpecNames))]
    public void Every_spec_column_exists_in_the_source_table_ddl(string specName)
    {
        using var connection = OpenConnection();
        Exec(connection, ReadFixture("sqlserver-tables.sql"));
        Exec(connection, ReadFixture("postgres-tables.sql"));

        var sourceTable = SourceTableByOutput[specName];
        var actualColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $"SELECT * FROM collect.\"{sourceTable}\" LIMIT 0";
            using var reader = command.ExecuteReader();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                actualColumns.Add(reader.GetName(i));
            }
        }

        var spec = RollupSql.Specs[specName];
        foreach (var key in spec.Key)
        {
            Assert.Contains(key, actualColumns);
        }

        foreach (var rule in spec.Rules)
        {
            foreach (var entry in rule.Columns)
            {
                // ArgMaxBy's first entry is an order column name, not a "col=alias" pair; every
                // other entry (and every entry for every other kind) may carry "=alias".
                var column = entry.Contains('=') ? entry[..entry.IndexOf('=')] : entry;
                Assert.Contains(column, actualColumns);
            }
        }
    }
}
