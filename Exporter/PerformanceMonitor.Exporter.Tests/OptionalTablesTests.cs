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
    public void ByGroup_7a_has_the_44_tables_from_the_registry_table()
    {
        // 27 original 7a entries plus incident_occurrences (docs/optional-grains.md §1.1, lane G1), then attention_events,
        // column_stats and column_stats_runs (31), then 13 more: query_store_hints, query_store_forced_plans,
        // object_inventory, object_dependencies, identity_capacity, database_protection, object_usage_snapshots,
        // object_usage_deltas, query_tag_sightings, and the four query tag config tables.
        Assert.Equal(44, OptionalTables.ByGroup("7a").Count);
        Assert.Single(OptionalTables.ByGroup("7b"));
        Assert.Equal(71, OptionalTables.All.Count);
    }

    // ---- the owner-approved 2026-11-26 tables ---------------------------------------------------

    // name, group, time column (null = daily snapshot), schema, min darling schema
    public static IEnumerable<object?[]> ApprovedTables() =>
    [
        ["query_snapshots", "7b", "collection_time", "collect", null],
        ["waiting_tasks", "7a", "collection_time", "collect", 243],
        ["dmv_blocking_snapshots", "7a", "collection_time", "collect", 199],
        ["default_trace_events", "7a", "collection_time", "collect", 21],
        ["object_usage_snapshots", "7a", "collection_time", "collect", 299],
        ["object_usage_deltas", "7a", "collection_time", "collect", 299],
        ["query_store_hints", "7a", "collection_time", "collect", 217],
        ["query_store_forced_plans", "7a", "collection_time", "collect", 201],
        ["object_inventory", "7a", "collection_time", "collect", 268],
        ["object_dependencies", "7a", "collection_time", "collect", 297],
        ["identity_capacity", "7a", "collection_time", "collect", 224],
        ["database_protection", "7a", "collection_time", "collect", 278],
        ["query_tag_sightings", "7a", "last_seen", "collect", 192],
        ["pg_session_states", "7c", "collection_time", "collect", 86],
        ["pg_blocking_edges", "7c", "collection_time", "collect", 71],
        ["pg_extension_availability", "7c", "collection_time", "collect", 95],
        ["query_tags", "7a", null, "config", 192],
        ["query_tag_assignments", "7a", null, "config", 192],
        ["query_tag_rules", "7a", null, "config", 192],
        ["query_tag_map", "7a", null, "config", 192],
    ];

    [Theory]
    [MemberData(nameof(ApprovedTables))]
    public void Each_approved_table_is_a_candidate_event_entry_with_its_time_column_and_rung(
        string name, string group, string? timeColumn, string schema, int? minSchema)
    {
        var table = OptionalTables.TryGet(name);

        Assert.NotNull(table);
        Assert.Equal(OptionalTableStatus.Candidate, table!.Status);
        Assert.Equal(ExportGrain.Event, table.Grain);
        Assert.Equal(group, table.Group);
        Assert.Equal(schema, table.Schema);
        Assert.Equal(timeColumn, table.TimeColumn);
        Assert.Equal(timeColumn is null ? OptionalTableKind.Snapshot : OptionalTableKind.Window, table.Kind);
        Assert.Equal(minSchema, table.MinDarlingSchemaVersion);
    }

    [Fact]
    public void Only_the_tables_without_a_server_name_of_their_own_resolve_it_from_collect_servers()
    {
        var resolving = OptionalTables.All.Where(t => t.ResolveServerName).Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal);

        Assert.Equal(
            ["object_usage_deltas", "object_usage_snapshots", "query_tag_assignments", "query_tag_map", "query_tag_rules", "query_tag_sightings"],
            resolving);
    }

    [Fact]
    public void The_query_tag_tables_that_other_tables_name_by_id_keep_their_identity_columns()
    {
        var keeping = OptionalTables.All.Where(t => t.KeepIdentityColumns).Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal);

        Assert.Equal(["query_tag_assignments", "query_tag_rules", "query_tags"], keeping);
    }

    [Fact]
    public void Query_tag_sightings_reads_its_timestamptz_last_seen_as_utc_and_no_other_entry_does()
    {
        Assert.Equal(["query_tag_sightings"], OptionalTables.All.Where(t => t.TimeColumnHasTimeZone).Select(t => t.Name));
    }

    [Fact]
    public void The_two_object_usage_tables_name_the_missing_window_index_in_their_note()
    {
        foreach (var name in new[] { "object_usage_snapshots", "object_usage_deltas" })
        {
            var table = OptionalTables.TryGet(name)!;
            Assert.Contains("NO USABLE INDEX", table.Note, StringComparison.Ordinal);
            Assert.Contains("collection_time", table.Note, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ValidateRegistry_rejects_a_time_zone_flag_on_a_snapshot_table()
    {
        var bad = new OptionalTable("probe", "collect", null, OptionalTableKind.Snapshot, [], "7a", OptionalTableStatus.Candidate, "x")
        {
            TimeColumnHasTimeZone = true,
        };

        Assert.Throws<InvalidOperationException>(() => OptionalTables.ValidateRegistry([bad]));
    }

    [Fact]
    public void Attention_events_is_a_candidate_event_window_gated_on_its_rung()
    {
        var table = OptionalTables.TryGet("attention_events");
        Assert.NotNull(table);
        Assert.Equal(OptionalTableStatus.Candidate, table!.Status);
        Assert.Equal(OptionalTableKind.Window, table.Kind);
        Assert.Equal(ExportGrain.Event, table.Grain);
        Assert.Equal("collection_time", table.TimeColumn);
        Assert.Equal(179, table.MinDarlingSchemaVersion);
        Assert.Null(OptionalTables.TryGet("attention_events_legacy"));
        Assert.Null(OptionalTables.TryGet("attention_events_legacy_copy"));
    }

    [Fact]
    public void Column_stats_runs_is_a_candidate_event_window_gated_on_its_rung()
    {
        var table = OptionalTables.TryGet("column_stats_runs");
        Assert.NotNull(table);
        Assert.Equal(OptionalTableStatus.Candidate, table!.Status);
        Assert.Equal(OptionalTableKind.Window, table.Kind);
        Assert.Equal(ExportGrain.Event, table.Grain);
        Assert.Equal("collection_time", table.TimeColumn);
        Assert.Equal(272, table.MinDarlingSchemaVersion);
    }

    [Fact]
    public void Column_stats_is_a_blocked_snapshot_because_its_text_array_has_no_parquet_mapping_and_its_cursor_table_is_not_registered()
    {
        var table = OptionalTables.TryGet("column_stats");
        Assert.NotNull(table);
        Assert.Equal(OptionalTableStatus.Blocked, table!.Status);
        Assert.Equal(OptionalTableKind.Snapshot, table.Kind);
        Assert.Null(table.TimeColumn);
        Assert.Equal(272, table.MinDarlingSchemaVersion);
        Assert.Throws<NotSupportedException>(() => DuckDbTypeMapper.Map("columns", "text[]"));
        Assert.Null(OptionalTables.TryGet("column_stats_state"));
    }

    // ---- 7c: store_pg's pg_* tables (docs/optional-tables.md §8, lane O6) --------------------

    private static readonly string[] Group7cWindowNames =
    [
        "pg_autovacuum_stats", "pg_blocking_edges", "pg_buffer_usage", "pg_column_stats", "pg_cpu_utilization",
        "pg_database_size_stats", "pg_database_stats", "pg_deadlocks", "pg_extension_availability",
        "pg_index_bloat", "pg_index_usage_stats", "pg_io_stats", "pg_lock_stats", "pg_log_events",
        "pg_plan_capture_readiness", "pg_replication_slot_stats", "pg_replication_stats",
        "pg_server_config", "pg_session_states", "pg_statement_stats", "pg_table_bloat_stats",
        "pg_wait_stats", "pg_wraparound_stats", "pg_write_stats", "pg_xmin_horizon",
    ];

    [Fact]
    public void ByGroup_7c_has_25_window_entries_plus_the_blocked_dimension()
    {
        var group7c = OptionalTables.ByGroup("7c");
        Assert.Equal(26, group7c.Count);
        Assert.Equal(25, group7c.Count(t => t.Kind == OptionalTableKind.Window));
    }

    [Fact]
    public void Pg_log_events_is_gated_on_the_last_parsing_rung_and_pg_buffer_usage_is_a_candidate_event_table()
    {
        // A store at rung 239 or later parses log lines the way the keep-only export expects; below it, the table is
        // skipped. pg_buffer_usage holds numbers and catalog names only, so it needs no column policy or gate.
        var log = OptionalTables.TryGet("pg_log_events");
        Assert.NotNull(log);
        Assert.Equal(239, log!.MinDarlingSchemaVersion);

        var buffers = OptionalTables.TryGet("pg_buffer_usage");
        Assert.NotNull(buffers);
        Assert.Equal(OptionalTableStatus.Candidate, buffers!.Status);
        Assert.Equal(ExportGrain.Event, buffers.Grain);
        Assert.Null(buffers.MinDarlingSchemaVersion);
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
    public void Pg_lock_stats_and_pg_session_states_are_candidate_event_tables()
    {
        // pg_lock_stats holds numbers and lock states only. pg_session_states carries usernames and client addresses;
        // the owner approved exporting every column on 2026-11-26.
        var locks = OptionalTables.TryGet("pg_lock_stats");
        Assert.NotNull(locks);
        Assert.Equal(OptionalTableStatus.Candidate, locks!.Status);
        Assert.Equal(ExportGrain.Event, locks.Grain);

        var sessions = OptionalTables.TryGet("pg_session_states");
        Assert.NotNull(sessions);
        Assert.Equal(OptionalTableStatus.Candidate, sessions!.Status);
        Assert.Equal(ExportGrain.Event, sessions.Grain);
    }

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
