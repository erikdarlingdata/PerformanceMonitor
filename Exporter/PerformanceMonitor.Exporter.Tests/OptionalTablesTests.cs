using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class OptionalTablesTests
{
    // ---- registry shape -------------------------------------------------

    [Fact]
    public void Servers_is_a_snapshot_with_no_time_column()
    {
        var servers = OptionalTables.TryGet("servers");

        Assert.NotNull(servers);
        Assert.Equal(OptionalTableKind.Snapshot, servers!.Kind);
        Assert.Null(servers.TimeColumn);
    }

    [Theory]
    [InlineData("query_store_health")]
    [InlineData("server_config")]
    [InlineData("database_config")]
    [InlineData("database_scoped_config")]
    [InlineData("trace_flags")]
    public void Capture_time_tables_resolve_to_capture_time(string name)
    {
        var table = OptionalTables.TryGet(name);

        Assert.NotNull(table);
        Assert.Equal("capture_time", table!.TimeColumn);
        Assert.Equal(OptionalTableKind.Window, table.Kind);
    }

    [Fact]
    public void Analysis_findings_resolves_to_analysis_time()
    {
        var table = OptionalTables.TryGet("analysis_findings");

        Assert.NotNull(table);
        Assert.Equal("analysis_time", table!.TimeColumn);
    }

    [Fact]
    public void Config_alert_log_resolves_to_alert_time_and_config_schema()
    {
        var table = OptionalTables.TryGet("config_alert_log");

        Assert.NotNull(table);
        Assert.Equal("alert_time", table!.TimeColumn);
        Assert.Equal("config", table.Schema);
    }

    [Fact]
    public void Query_snapshots_is_registered_as_group_7b()
    {
        var table = OptionalTables.TryGet("query_snapshots");

        Assert.NotNull(table);
        Assert.Equal("7b", table!.Group);
        Assert.Equal(OptionalTableKind.Window, table.Kind);
        Assert.Equal("collection_time", table.TimeColumn);
    }

    [Fact]
    public void ByGroup_7a_has_the_28_tables_from_the_registry_table()
    {
        // 27 original 7a entries plus incident_occurrences (docs/optional-grains.md §1.1, lane G1).
        Assert.Equal(28, OptionalTables.ByGroup("7a").Count);
    }

    // ---- 7c: store_pg's pg_* tables (docs/optional-tables.md §8, lane O6) --------------------

    private static readonly string[] Group7cWindowNames =
    [
        "pg_autovacuum_stats", "pg_blocking_edges", "pg_column_stats", "pg_cpu_utilization",
        "pg_database_size_stats", "pg_database_stats", "pg_deadlocks", "pg_extension_availability",
        "pg_index_bloat", "pg_index_usage_stats", "pg_io_stats", "pg_lock_stats", "pg_log_events",
        "pg_plan_capture_readiness", "pg_replication_slot_stats", "pg_replication_stats",
        "pg_server_config", "pg_session_states", "pg_statement_stats", "pg_table_bloat_stats",
        "pg_wait_stats", "pg_wraparound_stats", "pg_write_stats", "pg_xmin_horizon",
    ];

    [Fact]
    public void ByGroup_7c_has_24_window_entries_plus_the_blocked_dimension()
    {
        var group7c = OptionalTables.ByGroup("7c");
        Assert.Equal(25, group7c.Count);
        Assert.Equal(24, group7c.Count(t => t.Kind == OptionalTableKind.Window));
    }

    [Fact]
    public void Every_7c_pg_name_is_registered()
    {
        foreach (var name in Group7cWindowNames)
        {
            Assert.NotNull(OptionalTables.TryGet(name));
        }
    }

    [Theory]
    [MemberData(nameof(Group7cWindowNamesMemberData))]
    public void Each_7c_window_table_time_column_is_collection_time(string name)
    {
        var table = OptionalTables.TryGet(name);
        Assert.NotNull(table);
        Assert.Equal("collection_time", table!.TimeColumn);
        Assert.Equal(OptionalTableKind.Window, table.Kind);
        Assert.Equal("7c", table.Group);
    }

    public static IEnumerable<object[]> Group7cWindowNamesMemberData() => Group7cWindowNames.Select(n => new object[] { n });

    [Fact]
    public void Pg_statement_stats_is_blocked_pending_the_index_range_read_proof()
    {
        var table = OptionalTables.TryGet("pg_statement_stats");
        Assert.NotNull(table);
        Assert.Equal(OptionalTableStatus.Candidate, table!.Status);
        Assert.Contains("EXPLAIN", table.Note);
    }

    [Fact]
    public void Pg_statement_text_is_a_candidate_dimension_routed_to_its_own_exporter()
    {
        // PgStatementTextExporter is now wired via OptionalTableRunner (lane O7), keyed off the same run's
        // pg_statement_stats read, and never runs through the generic Window/Snapshot select path.
        var table = OptionalTables.TryGet("pg_statement_text");
        Assert.NotNull(table);
        Assert.Equal(OptionalTableStatus.Candidate, table!.Status);
        Assert.Equal("7c", table.Group);
        Assert.Equal(OptionalTableKind.Dimension, table.Kind);
        Assert.Equal(new[] { "source_store", "server_name", "queryid" }, table.KeyColumns);
    }

    [Fact]
    public void No_7c_name_collides_with_7a_7b_or_a_core_table()
    {
        var coreNames = ExportOptions.AllTables.ToHashSet();
        var namesSeen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var table in OptionalTables.All)
        {
            Assert.False(coreNames.Contains(table.Name), $"'{table.Name}' collides with a core table name.");
            Assert.True(namesSeen.Add(table.Name), $"'{table.Name}' is registered more than once.");
        }
    }

    [Fact]
    public void TryGet_returns_null_for_an_unknown_name()
    {
        Assert.Null(OptionalTables.TryGet("not_a_real_table"));
    }

    [Fact]
    public void No_registry_name_collides_with_a_core_table()
    {
        var coreNames = ExportOptions.AllTables.ToHashSet();

        foreach (var table in OptionalTables.All)
        {
            Assert.False(coreNames.Contains(table.Name), $"'{table.Name}' collides with a core table name.");
        }
    }

    [Fact]
    public void Every_registry_name_matches_the_lowercase_underscore_pattern()
    {
        foreach (var table in OptionalTables.All)
        {
            Assert.Matches("^[a-z_]+$", table.Name);
        }
    }

    [Fact]
    public void Every_window_table_has_a_time_column_and_every_snapshot_table_does_not()
    {
        foreach (var table in OptionalTables.All)
        {
            if (table.Kind == OptionalTableKind.Window)
            {
                Assert.False(string.IsNullOrEmpty(table.TimeColumn), $"'{table.Name}' is a window table but has no TimeColumn.");
            }
            else
            {
                Assert.Null(table.TimeColumn);
            }
        }
    }

    // ---- OptionalColumnDiscoverySql --------------------------------------

    [Fact]
    public void OptionalColumnDiscoverySql_is_schema_and_table_parameterized()
    {
        Assert.Contains("@schema", SqlBuilder.OptionalColumnDiscoverySql);
        Assert.Contains("@table", SqlBuilder.OptionalColumnDiscoverySql);
        Assert.Contains("table_schema = @schema", SqlBuilder.OptionalColumnDiscoverySql);
        Assert.Contains("table_name = @table", SqlBuilder.OptionalColumnDiscoverySql);
    }

    [Fact]
    public void OptionalColumnDiscoverySql_returns_ordinal_ordered_column_name_data_type_udt_name()
    {
        Assert.Contains("SELECT column_name, data_type, udt_name FROM information_schema.columns", SqlBuilder.OptionalColumnDiscoverySql);
        Assert.EndsWith("ORDER BY ordinal_position", SqlBuilder.OptionalColumnDiscoverySql);
    }

    [Fact]
    public void OptionalColumnDiscoverySql_excludes_server_id_and_collection_id()
    {
        Assert.Contains("column_name NOT IN ('server_id', 'collection_id')", SqlBuilder.OptionalColumnDiscoverySql);
    }

    [Fact]
    public void OptionalColumnDiscoverySql_excludes_identity_columns()
    {
        Assert.Contains("is_identity <> 'YES'", SqlBuilder.OptionalColumnDiscoverySql);
    }

    [Fact]
    public void OptionalColumnDiscoverySql_excludes_nextval_defaulted_columns()
    {
        Assert.Contains("column_default NOT LIKE 'nextval(%'", SqlBuilder.OptionalColumnDiscoverySql);
    }

    // ---- BuildOptionalSelect ---------------------------------------------

    [Fact]
    public void BuildOptionalSelect_window_table_has_the_exact_shape()
    {
        var table = OptionalTables.TryGet("memory_grant_stats")!;
        var sql = SqlBuilder.BuildOptionalSelect(table, ["collection_time", "server_name", "grant_id"]);

        Assert.Equal(
            "/* darling-export:opt.memory_grant_stats */ SELECT \"collection_time\", \"server_name\", \"grant_id\" " +
            "FROM collect.\"memory_grant_stats\" WHERE collection_time >= @from AND collection_time < @to",
            sql);
    }

    [Fact]
    public void BuildOptionalSelect_snapshot_table_has_no_where()
    {
        var table = OptionalTables.TryGet("servers")!;
        var sql = SqlBuilder.BuildOptionalSelect(table, ["server_name", "created_date"]);

        Assert.Equal(
            "/* darling-export:opt.servers */ SELECT \"server_name\", \"created_date\" FROM collect.\"servers\"",
            sql);
        Assert.DoesNotContain("WHERE", sql);
    }

    [Fact]
    public void BuildOptionalSelect_capture_time_table_filters_on_capture_time()
    {
        var table = OptionalTables.TryGet("server_config")!;
        var sql = SqlBuilder.BuildOptionalSelect(table, ["capture_time", "server_name"]);

        Assert.Contains("WHERE capture_time >= @from AND capture_time < @to", sql);
    }

    [Fact]
    public void BuildOptionalSelect_config_table_qualifies_with_the_config_schema()
    {
        var table = OptionalTables.TryGet("config_alert_log")!;
        var sql = SqlBuilder.BuildOptionalSelect(table, ["alert_time", "detail_text"]);

        Assert.Contains("FROM config.\"config_alert_log\"", sql);
        Assert.Contains("WHERE alert_time >= @from AND alert_time < @to", sql);
    }

    [Fact]
    public void BuildOptionalSelect_tags_every_statement()
    {
        foreach (var table in OptionalTables.All.Where(t => t.Kind != OptionalTableKind.Dimension))
        {
            var columns = table.Kind == OptionalTableKind.Snapshot
                ? new[] { "server_name" }
                : new[] { table.TimeColumn!, "server_name" };

            var sql = SqlBuilder.BuildOptionalSelect(table, columns);
            Assert.StartsWith($"/* darling-export:opt.{table.Name} */", sql);
        }
    }

    [Fact]
    public void BuildOptionalSelect_quotes_identifiers_like_BuildRawSelect_does()
    {
        var table = OptionalTables.TryGet("plan_correction")!;
        var sql = SqlBuilder.BuildOptionalSelect(table, ["collection_time", "Weird Name"]);

        Assert.Contains("\"collection_time\", \"Weird Name\"", sql);
    }

    [Fact]
    public void BuildOptionalSelect_throws_for_an_empty_column_list()
    {
        var table = OptionalTables.TryGet("servers")!;
        Assert.Throws<ArgumentException>(() => SqlBuilder.BuildOptionalSelect(table, []));
    }

    // ---- BuildOptionalSelect with a TablePolicy (design §3.2) ---------

    [Fact]
    public void BuildOptionalSelect_with_null_policy_matches_the_unchanged_overload()
    {
        var table = OptionalTables.TryGet("memory_grant_stats")!;
        var columnNames = new[] { "collection_time", "server_name", "grant_id" };
        var discovered = columnNames.Select(c => new DiscoveredColumn(c, "text", "text")).ToList();

        var viaOldOverload = SqlBuilder.BuildOptionalSelect(table, columnNames);
        var viaNewOverloadNullPolicy = SqlBuilder.BuildOptionalSelect(table, discovered, policy: null);

        Assert.Equal(viaOldOverload, viaNewOverloadNullPolicy);
    }

    [Fact]
    public void BuildOptionalSelect_drop_leaves_the_column_out_of_the_projection()
    {
        var table = OptionalTables.TryGet("config_alert_log")!;
        var discovered = new[] { "alert_time", "server_name", "send_error" }
            .Select(c => new DiscoveredColumn(c, "text", "text")).ToList();
        var policy = ColumnPolicies.For("config_alert_log");

        var sql = SqlBuilder.BuildOptionalSelect(table, discovered, policy);

        Assert.DoesNotContain("send_error", sql);
        Assert.Contains("\"alert_time\", \"server_name\"", sql);
    }

    [Fact]
    public void BuildOptionalSelect_keep_when_wraps_the_column_in_a_case_with_the_predicate_inside()
    {
        var table = OptionalTables.TryGet("job_history")!;
        var discovered = new[] { "server_name", "run_status", "message" }
            .Select(c => new DiscoveredColumn(c, "text", "text")).ToList();
        var policy = ColumnPolicies.For("job_history");

        var sql = SqlBuilder.BuildOptionalSelect(table, discovered, policy);

        Assert.Contains(
            "CASE WHEN \"run_status\" <> 1 THEN left(\"message\", 2000) END AS \"message\"",
            sql);
    }

    [Fact]
    public void BuildOptionalSelect_keep_when_without_truncate_has_no_left_call()
    {
        var table = OptionalTables.TryGet("servers")!;
        var policy = new TablePolicy(new[] { new ColumnRule("server_name", ColumnAction.KeepWhen, PredicateSql: "1 = 1") });
        var discovered = new[] { new DiscoveredColumn("server_name", "text", "text") };

        var sql = SqlBuilder.BuildOptionalSelect(table, discovered, policy);

        Assert.DoesNotContain("left(", sql);
        Assert.Contains("\"server_name\"", sql);
    }

    [Fact]
    public void BuildOptionalSelect_keep_only_intersects_discovered_columns_in_discovery_order()
    {
        var table = OptionalTables.TryGet("pg_log_events")!;
        var discovered = new[] { "collection_time", "occurred_at", "message", "family", "a_brand_new_column", "server_name" }
            .Select(c => new DiscoveredColumn(c, "text", "text")).ToList();
        var policy = ColumnPolicies.For("pg_log_events");

        var sql = SqlBuilder.BuildOptionalSelect(table, discovered, policy);

        // message is on the discovered list but not on KeepOnly -> dropped.
        Assert.DoesNotContain("message", sql);
        // a_brand_new_column is discovered but not on KeepOnly -> dropped by default.
        Assert.DoesNotContain("a_brand_new_column", sql);
        // collection_time, occurred_at, family and server_name are on both lists, in discovery order.
        Assert.Contains("\"collection_time\", \"occurred_at\", \"family\", \"server_name\"", sql);
    }

    private static readonly Regex ForbiddenClause = new(@"\b(ORDER\s+BY|GROUP\s+BY|DISTINCT)\b", RegexOptions.IgnoreCase);

    [Fact]
    public void No_optional_sql_ever_contains_order_by_group_by_or_distinct()
    {
        foreach (var table in OptionalTables.All.Where(t => t.Kind != OptionalTableKind.Dimension))
        {
            var columns = table.Kind == OptionalTableKind.Snapshot
                ? new[] { "server_name" }
                : new[] { table.TimeColumn!, "server_name" };

            var sql = SqlBuilder.BuildOptionalSelect(table, columns);
            Assert.False(ForbiddenClause.IsMatch(sql), $"'{table.Name}' SQL unexpectedly contains ORDER BY/GROUP BY/DISTINCT: {sql}");
        }
    }

    // ---- §3.2 EXPLAIN gate script generator ------------------------------

    /// <summary>
    /// Emits, for every window entry in the registry, an `EXPLAIN (COSTS OFF) &lt;statement&gt;;`
    /// line built from <see cref="SqlBuilder.BuildOptionalSelect"/> with the literal bounds of
    /// the last closed hour before <paramref name="nowUtc"/>, one line per table in registry
    /// order (docs/optional-tables.md §3.2). Snapshot tables (servers) are included with no
    /// WHERE, since the gate still records their plan and relation size. This is deliberately
    /// test-only plumbing: the coordinator runs the generated file against store_a/store_b, it is
    /// never executed by the exporter itself.
    /// </summary>
    internal static string BuildExplainGateScript(DateTime nowUtc)
    {
        var lastClosedHourStart = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, 0, 0, DateTimeKind.Utc).AddHours(-1);
        return OptionalGateScript.Build(OptionalTables.All, lastClosedHourStart, lastClosedHourStart.AddHours(1), "all");
    }

    private static readonly DateTime GateFrom = new(2026, 9, 27, 23, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void OptionalGateScript_unknown_table_is_not_in_registry()
    {
        Assert.Null(OptionalTables.All.FirstOrDefault(t => t.Name == "no_such_table"));
    }

    [Fact]
    public void OptionalGateScript_dimension_gets_a_comment_not_an_explain()
    {
        var dim = OptionalTables.All.First(t => t.Kind == OptionalTableKind.Dimension);
        var script = OptionalGateScript.Build([dim], GateFrom, GateFrom.AddHours(1), "store_a");
        Assert.DoesNotContain("EXPLAIN (COSTS OFF)", script);
        Assert.Contains($"-- {dim.Name}: Dimension, no window read", script);
    }

    [Fact]
    public void OptionalGateScript_has_header_sched_lines_tag_and_window_literals()
    {
        var win = OptionalTables.All.First(t => t.Kind == OptionalTableKind.Window);
        var script = OptionalGateScript.Build([win], GateFrom, GateFrom.AddHours(1), "store_a");
        Assert.StartsWith("SET default_transaction_read_only = on; SET max_parallel_workers_per_gather = 0; SET statement_timeout = '60s';", script);
        Assert.Contains("SELECT 'SCHED',", script);
        Assert.Contains("SELECT 'SCHEDCOUNT',", script);
        Assert.Contains($"collector_name IN ('{win.Name}')", script);
        Assert.Contains($"EXPLAIN (COSTS OFF) /* darling-export:opt.{win.Name} */", script);
        Assert.Contains("'2026-09-27T23:00:00Z'", script);
        Assert.Contains("'2026-09-28T00:00:00Z'", script);
        Assert.DoesNotContain("@from", script);
    }

    [Fact]
    public void BuildExplainGateScript_emits_one_explain_per_registry_table()
    {
        var script = BuildExplainGateScript(new DateTime(2026, 9, 27, 14, 0, 0, DateTimeKind.Utc));

        var explainCount = Regex.Matches(script, @"EXPLAIN \(COSTS OFF\)").Count;
        Assert.Equal(OptionalTables.All.Count(t => t.Kind != OptionalTableKind.Dimension), explainCount);
        Assert.DoesNotContain("@from", script);
        Assert.DoesNotContain("@to", script);
    }
}
