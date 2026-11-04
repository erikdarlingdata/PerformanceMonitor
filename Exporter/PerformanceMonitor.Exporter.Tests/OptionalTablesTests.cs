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

    // ---- BuildOptionalSelect ---------------------------------------------

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
    public void OptionalGateScript_unknown_table_is_not_in_registry()
    {
        Assert.Null(OptionalTables.All.FirstOrDefault(t => t.Name == "no_such_table"));
    }
}
