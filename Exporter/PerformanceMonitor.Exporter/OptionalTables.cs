namespace PerformanceMonitor.Exporter;

/// <summary>
/// How an optional table's rows relate to the export window. A window table is filtered on its time column for every
/// export window. A snapshot table carries no per-row time column of its own; the exporter stamps one on the way out
/// and the table is read whole, once a day.
/// </summary>
public enum OptionalTableKind
{
    Window,
    Snapshot,

    /// <summary>
    /// A content dimension keyed by <see cref="OptionalTable.KeyColumns"/>, fed by another table's already- streamed
    /// rows in the same run rather than its own window/snapshot read. The only entry today is
    /// <c>pg_statement_text</c>, routed to <c>PgStatementTextExporter</c> and never through the generic
    /// <c>OptionalTableExporter</c> path.
    /// </summary>
    Dimension,
}

/// <summary>
/// Where an optional table sits in the offer pipeline. Every registry entry starts life as <see cref="Candidate"/>.
/// The coordinator moves an entry to <see cref="Offered"/> once its EXPLAIN gate and verification runs pass, or to
/// <see cref="Blocked"/> if the gate fails (for example, a Seq Scan on a table too large to accept one).
/// </summary>
public enum OptionalTableStatus
{
    Candidate,
    Offered,
    Blocked,
}

/// <summary>
/// How an optional table's rows relate to a collection cycle. Every registry entry defaults to <see cref="Event"/>
/// (today's plain per-window path); the other three grains are pinned in the registry now but not yet run through the
/// export path.
/// </summary>
public enum ExportGrain
{
    /// <summary>Today's plain per-event export: every row in the window, unchanged.</summary>
    Event,

    /// <summary>A daily snapshot: the last complete collection cycle per server before 00:00Z.</summary>
    Daily,

    /// <summary>Change-only: a row is emitted only when its tracked columns changed since the last window.</summary>
    Change,

    /// <summary>An hourly rollup: no raw rows are exported, only the aggregated <c>*_hourly</c> output.</summary>
    Hourly,
}

/// <summary>
/// Change-only settings. <paramref name="Key"/> is the identity columns -- always includes <c>server_name</c>.
/// <paramref name="Ignore"/> is carried in the emitted row but not compared: volatile gauges, and ids Darling assigns
/// afresh on every collection. <c>config_id</c> is one: a plain <c>bigint NOT NULL</c> with no identity or
/// <c>nextval</c> default, so column discovery keeps it. If it were compared, each config re-collection (every Darling
/// restart) would come out as "changed" with identical values. <paramref name="EmitOnce"/> true means a key is emitted
/// the first time it is seen and never again (the tracked set is effectively empty).
/// </summary>
public sealed record ChangeSpec(IReadOnlyList<string> Key, IReadOnlyList<string> Ignore, bool EmitOnce = false);

/// <summary>
/// One entry in the optional-tables registry. Immutable: a change of <see cref="Status"/> means building a new record,
/// never mutating one in place.
/// </summary>
/// <param name="Name">The bare table name, as it appears in <paramref name="Schema"/> — never qualified, and never
/// colliding with <see cref="ExportOptions.AllTables"/>.</param> <param name="Schema">The store schema the table lives
/// in: <c>collect</c> or <c>config</c>.</param> <param name="TimeColumn">The column a window table is filtered on.
/// Null for a snapshot table, which has no time column of its own.</param> <param name="Kind">Window or snapshot (see
/// <see cref="OptionalTableKind"/>).</param> <param name="KeyColumns">The dimension key columns, for a future
/// dimension-kind entry. Empty for every window and snapshot table in this registry.</param> <param name="Group">The
/// roadmap grouping: <c>7a</c> (plain SQL Server/Darling tables), <c>7b</c> (query_snapshots), or <c>7c</c> (the
/// Postgres store's pg_* tables).</param> <param name="Status">Where the table sits in the offer pipeline (see <see
/// cref="OptionalTableStatus"/>). Every entry below starts as <see cref="OptionalTableStatus.Candidate"/>.</param>
/// <param name="Note">A one-line note: why the table is here, or what makes it unusual.</param>
public sealed record OptionalTable(
    string Name,
    string Schema,
    string? TimeColumn,
    OptionalTableKind Kind,
    IReadOnlyList<string> KeyColumns,
    string Group,
    OptionalTableStatus Status,
    string Note)
{
    /// <summary>How this table's rows relate to a collection cycle. Defaults to <see cref="ExportGrain.Event"/>,
    /// today's plain per-window path, for every entry that does not set it.</summary>
    public ExportGrain Grain { get; init; } = ExportGrain.Event;

    /// <summary>Daily-grain only: the lookback in hours. Null for the whole-table snapshot (<c>servers</c>).</summary>
    public int? DailyLookbackHours { get; init; }

    /// <summary>Change-grain only: the key/ignore columns.</summary>
    public ChangeSpec? Change { get; init; }

    /// <summary>Hourly-grain only: the <c>RollupSql.Specs</c> key for this table's rollup output.</summary>
    public string? RollupName { get; init; }

    /// <summary>The store's <c>darling_schema_version</c> below which this table is skipped with reason "needs darling
    /// schema &gt;= N". Null means no gate.</summary>
    public int? MinDarlingSchemaVersion { get; init; }

    /// <summary>True adds a constant <c>darling_schema_version</c> column to every row, the way <c>snapshot_time</c>
    /// is added for D tables.</summary>
    public bool StampSchemaVersion { get; init; }
}

/// <summary>
/// The optional-tables registry. Off by default: none of these tables are in <see cref="ExportOptions.AllTables"/> or
/// the no-<c>--tables</c> default. They run only when named by a future <c>--also</c> flag (a), or by <c>--tables</c>
/// on a manual run.
/// </summary>
public static class OptionalTables
{
    private const string Collect = "collect";
    private const string Config = "config";
    private const string CollectionTime = "collection_time";
    private const string CaptureTime = "capture_time";

    /// <summary>Group 7a: the plain SQL Server and Darling tables, plus the daily snapshot <c>servers</c>.</summary>
    private const string Group7a = "7a";

    /// <summary>Group 7b: <c>query_snapshots</c>, whose inline plans dedupe into <c>plan_shape</c>.</summary>
    private const string Group7b = "7b";

    /// <summary>Group 7c: the Postgres store's <c>pg_*</c> tables, plus <c>pg_statement_text</c> as a (blocked)
    /// dimension.</summary>
    private const string Group7c = "7c";

    /// <summary>
    /// The newest schema rung that the <c>pg_log_events</c> parsing rewrite lands in. Not yet known, so it is pinned to
    /// <c>int.MaxValue</c> and the table is always skipped by the schema gate until this constant is updated.
    /// </summary>
    public const int PgLogEventsMinSchema = int.MaxValue;

    public static readonly IReadOnlyList<OptionalTable> All =
    [
        new("plan_correction", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Text-heavy: query_text and 17 reason/state strings, very large on a big store.")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(["server_name", "database_name", "recommendation_name"],
                ["last_refresh", "score", "estimated_gain_seconds"]),
        },
        new("default_trace_events", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Blocked,
            "Blocked(LATER): text-heavy, carries text_data."),
        new("memory_grant_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Numeric only, small.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "memory_grant_stats_hourly",
        },
        new("plan_cache_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "numeric only, small."),
        new("session_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "numeric only, small."),
        new("session_summary_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Numeric only, tiny row rate.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "session_summary_stats_hourly",
        },
        new("waiting_tasks", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Blocked,
            "Blocked(OUT): 7-day retention; tiny row rate."),
        new("dmv_blocking_snapshots", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Blocked,
            "Blocked(OUT): text-heavy blocked/blocking SQL text; row count is usually zero."),
        new("memory_clerks", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "numeric only, small."),
        new("memory_pressure_events", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "numeric only, small."),
        new("cpu_scheduler_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Numeric only, small.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "cpu_scheduler_stats_hourly",
        },
        new("job_history", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Mildly text-heavy (job messages); the collector may re-read the same msdb rows each cycle."),
        new("index_object_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Numeric only; kept beyond 40 days.")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = 24,
        },
        new("database_size_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Numeric only; kept beyond 40 days.")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = 2,
        },
        new("database_states", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Numeric/state only, small.")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(["server_name", "database_name"], []),
        },
        new("ag_database_replica_states", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Numeric/state only, small. Change + Hourly from one store read (an exception to the usual one-grain rule).")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(
                ["server_name", "ag_name", "database_name", "replica_server_name", "group_id"],
                ["last_hardened_lsn", "last_commit_lsn", "log_send_queue_size", "redo_queue_size",
                 "log_send_rate", "redo_rate", "secondary_lag_seconds", "last_commit_time",
                 "last_hardened_time", "last_redone_time", "last_received_time",
                 "est_redo_completion_time_min", "est_send_drain_time_min"]),
            RollupName = "ag_database_replica_states_hourly",
            MinDarlingSchemaVersion = 151,
        },
        new("ag_replica_states", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Numeric/state only, small.")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(["server_name", "ag_name", "replica_server_name", "group_id"], []),
            MinDarlingSchemaVersion = 151,
        },
        new("pvs_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "numeric only, tiny row rate."),
        new("query_store_health", Collect, CaptureTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Captured state, not a collection cycle; filtered on capture_time.")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(["server_name", "database_name"], ["config_id", "current_storage_size_mb"]),
        },
        new("server_config", Collect, CaptureTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Captured state, not a collection cycle; filtered on capture_time.")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(["server_name", "configuration_name"], ["config_id"]),
        },
        new("server_properties", Collect, CollectionTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Row rate differs a lot between stores (capture-on-change vs. capture-every-cycle).")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(["server_name"], []),
        },
        new("database_config", Collect, CaptureTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Captured state, not a collection cycle; filtered on capture_time.")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(["server_name", "database_name"], ["config_id"]),
        },
        new("database_scoped_config", Collect, CaptureTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Captured state, not a collection cycle; filtered on capture_time.")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(["server_name", "database_name", "configuration_name"], ["config_id"]),
        },
        new("trace_flags", Collect, CaptureTime, OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Captured state, not a collection cycle; filtered on capture_time.")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(["server_name", "trace_flag"], ["config_id"]),
        },
        new("analysis_findings", Collect, "analysis_time", OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "Not a hypertable, large on a big store; conditional on the EXPLAIN gate finding a usable index (deferred if it shows a Seq Scan).")
        {
            StampSchemaVersion = true,
        },
        new("config_alert_log", Config, "alert_time", OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Candidate,
            "config schema, not collect; small, so a Seq Scan is accepted."),
        new("incident_occurrences", Config, "occurred_at", OptionalTableKind.Window, [], Group7a, OptionalTableStatus.Blocked,
            "DDL not yet fetched.")
        {
            StampSchemaVersion = true,
        },
        new("servers", Collect, null, OptionalTableKind.Snapshot, [], Group7a, OptionalTableStatus.Candidate,
            "Daily snapshot: the exporter stamps snapshot_time and exports it only in the 00:00Z fresh window."),
        new("query_snapshots", Collect, CollectionTime, OptionalTableKind.Window, [], Group7b, OptionalTableStatus.Blocked,
            "Blocked(LATER): inline query_plan/live_query_plan handled specially; live_query_plan is never selected, query_plan is parsed and dropped from the fact row."),

        // 7c: the Postgres store's pg_* tables. Only the Postgres store has rows in these; they exist but are empty on
        // a large store. Time column pinned as collection_time for every one, from the Postgres store schema
        // dump.
        new("pg_autovacuum_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Per-table autovacuum state; numeric/state only.")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = 2,
        },
        new("pg_blocking_edges", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Blocked,
            "Blocked(LATER): text-heavy blocked_query/blocking_query SQL text."),
        new("pg_column_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Planner column statistics; numeric/state only.")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = 24,
        },
        new("pg_cpu_utilization", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric only, small.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "pg_cpu_utilization_hourly",
        },
        new("pg_database_size_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric only, small.")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = 2,
        },
        new("pg_database_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric only, small.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "pg_database_stats_hourly",
        },
        new("pg_deadlocks", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Text-heavy: victim_statement, graph_text."),
        new("pg_extension_availability", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Blocked,
            "Blocked(OUT): numeric/state only, small."),
        new("pg_index_bloat", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric only.")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = 24,
        },
        new("pg_index_usage_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Mildly text-heavy (index_definition); mostly numeric.")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = 24,
        },
        new("pg_io_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric only; sizeable.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "pg_io_stats_hourly",
        },
        new("pg_lock_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Blocked,
            "Blocked(LATER): numeric/state only, small."),
        new("pg_log_events", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Text-heavy: message/detail/context from the Postgres log; keep-only column list.")
        {
            MinDarlingSchemaVersion = PgLogEventsMinSchema,
        },
        new("pg_plan_capture_readiness", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Blocked,
            "Blocked(OUT): small readiness-facet rows; mildly text-heavy (observed/detail)."),
        new("pg_replication_slot_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric/state only, small.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "pg_replication_slot_stats_hourly",
        },
        new("pg_replication_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric/state only, small.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "pg_replication_stats_hourly",
        },
        new("pg_server_config", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Captured GUC state; mildly text-heavy (short_desc); sizeable.")
        {
            Grain = ExportGrain.Change,
            Change = new ChangeSpec(["server_name", "name", "database_name", "role_name"], ["short_desc"]),
        },
        new("pg_session_states", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Blocked,
            "Blocked(OUT): session snapshot rows; mildly text-heavy (query text is not carried, only ids/state)."),
        new("pg_statement_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Very large; needs its index-range read proven by EXPLAIN before it is offered.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "pg_statement_stats_hourly",
        },
        new("pg_table_bloat_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric only.")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = 2,
        },
        new("pg_wait_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric only, small.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "pg_wait_stats_hourly",
        },
        new("pg_wraparound_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric only, small.")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = 2,
        },
        new("pg_write_stats", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Numeric only, checkpointer/bgwriter/WAL counters.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "pg_write_stats_hourly",
        },
        new("pg_xmin_horizon", Collect, CollectionTime, OptionalTableKind.Window, [], Group7c, OptionalTableStatus.Candidate,
            "Mildly text-heavy (source/holder/detail); small row rate.")
        {
            Grain = ExportGrain.Hourly,
            RollupName = "pg_xmin_horizon_hourly",
        },
        // Not a hypertable and carries no per-row time column (PK is (server_id, queryid)); its rows are keyed by the
        // queryids seen in a pg_statement_stats read, not by a window. Kind Dimension routes it to
        // PgStatementTextExporter, fed by the same run's pg_statement_stats keys, and it never runs through the plain
        // Window/Snapshot select path in OptionalTableExporter.
        new("pg_statement_text", Collect, null, OptionalTableKind.Dimension, ["source_store", "server_name", "queryid"], Group7c, OptionalTableStatus.Candidate,
            "Dimension fed by the same run's pg_statement_stats keys (PgStatementTextExporter). Requires pg_statement_stats in the same --also/--tables set; skipped with 'needs pg_statement_stats' otherwise."),
    ];

    /// <summary>Looks up a registry entry by name, or null if <paramref name="name"/> is not registered.</summary>
    public static OptionalTable? TryGet(string name) => All.FirstOrDefault(t => t.Name == name);

    /// <summary>All registry entries in the given roadmap group (<c>7a</c>, <c>7b</c> or <c>7c</c>), in registry order.</summary>
    public static IReadOnlyList<OptionalTable> ByGroup(string group) => [.. All.Where(t => t.Group == group)];

    /// <summary>
    /// Throws if any entry names a denied table. Runs once against <see cref="All"/>, from a static field initializer,
    /// so a denied entry stops the process at start. The overload here is the testable seam: it takes an arbitrary
    /// entry list.
    /// </summary>
    public static void ValidateRegistry(IEnumerable<OptionalTable> entries)
    {
        foreach (var entry in entries)
        {
            if (ColumnPolicies.IsDenied($"{entry.Schema}.{entry.Name}"))
            {
                throw new InvalidOperationException($"denied table in registry: {entry.Schema}.{entry.Name}");
            }

            // Grain rules.
            if (entry.Grain == ExportGrain.Change)
            {
                if (entry.Change is null)
                {
                    throw new InvalidOperationException($"'{entry.Name}' is Grain.Change but has no Change spec.");
                }

                if (!entry.Change.Key.Contains("server_name", StringComparer.Ordinal))
                {
                    throw new InvalidOperationException($"'{entry.Name}' Change.Key must contain server_name.");
                }

                // "Change + Rollup": Grain stays Change, but RollupName names a second output built from the SAME
                // single store read. Validated exactly like a plain Hourly entry's RollupName.
                if (entry.RollupName is not null && !RollupSql.Specs.ContainsKey(entry.RollupName))
                {
                    throw new InvalidOperationException($"'{entry.Name}' RollupName '{entry.RollupName}' has no RollupSql.Specs entry.");
                }
            }

            if (entry.Grain == ExportGrain.Hourly)
            {
                if (entry.RollupName is null)
                {
                    throw new InvalidOperationException($"'{entry.Name}' is Grain.Hourly but has no RollupName.");
                }

                if (!RollupSql.Specs.ContainsKey(entry.RollupName))
                {
                    throw new InvalidOperationException($"'{entry.Name}' RollupName '{entry.RollupName}' has no RollupSql.Specs entry.");
                }
            }

            // RollupName on any grain OTHER than Hourly or Change is meaningless -- it would never be read.
            if (entry.RollupName is not null && entry.Grain != ExportGrain.Hourly && entry.Grain != ExportGrain.Change)
            {
                throw new InvalidOperationException($"'{entry.Name}' has a RollupName but Grain is neither Hourly nor Change.");
            }

            if (entry.Grain == ExportGrain.Daily)
            {
                var validSnapshot = entry.Kind == OptionalTableKind.Snapshot;
                var validWindow = entry.Kind == OptionalTableKind.Window
                    && entry.DailyLookbackHours is >= 1 and <= 24;
                if (!validSnapshot && !validWindow)
                {
                    throw new InvalidOperationException(
                        $"'{entry.Name}' is Grain.Daily but is neither a Snapshot nor a Window with DailyLookbackHours in 1..24.");
                }
            }

            if (entry.Kind == OptionalTableKind.Dimension && entry.Grain != ExportGrain.Event)
            {
                throw new InvalidOperationException($"'{entry.Name}' is a Dimension but Grain is not Event.");
            }
        }
    }

    /// <summary>Validates the real registry at class load. Runs once, before <c>All</c> is ever read, because a static
    /// field initializer in a class runs in declaration order.</summary>
    private static readonly bool RegistryValidated = ValidateRealRegistry();

    private static bool ValidateRealRegistry()
    {
        ValidateRegistry(All);
        return true;
    }
}
