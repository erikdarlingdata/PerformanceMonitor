namespace PerformanceMonitor.Exporter;

/// <summary>
/// The shape of one hourly-rollup rule. Each kind maps a source column (or a small set of
/// related columns) to one or more output columns, aggregated over the hour.
/// </summary>
public enum RollupRuleKind
{
    /// <summary>A level: `_avg`, `_max`, `_last` (arg_max by the time column).</summary>
    Gauge,

    /// <summary>A "free"/"available" level: `_avg`, `_min`, `_last`.</summary>
    GaugeMin,

    /// <summary>The collector's own delta column, summed over rows where
    /// `sample_interval_seconds IS DISTINCT FROM 0` (same rule as `QueryStatsHourlySql`).</summary>
    DeltaSum,

    /// <summary>A cumulative counter with no delta column: reset-safe `_first`, `_last`,
    /// `_delta_in_hour` via `lag()`.</summary>
    Counter,

    /// <summary>A config-like or text value: `arg_max(c, time)`, alias unchanged.</summary>
    Last,

    /// <summary>A peak: `_max`.</summary>
    Max,

    /// <summary>A floor: `_min`.</summary>
    Min,

    /// <summary>A warning boolean: `_samples` = count_if(c).</summary>
    CountTrue,

    /// <summary>A boolean where the interesting state is false: `count_if(NOT c)`. Columns are
    /// `"col=alias"` pairs (alias required, since there is no generic naming convention for
    /// "count of false").</summary>
    CountFalse,

    /// <summary>`arg_max(target, orderColumn)` for one or more target columns that share a
    /// single "argmax at the row with the largest orderColumn" pick. Columns[0] is the order
    /// column; the rest are `"col=alias"` pairs (alias required). Used by
    /// `pg_xmin_horizon_hourly`'s `holder_at_max`/`detail_at_max`.</summary>
    ArgMaxBy,
}

/// <summary>One rollup rule: a kind plus the column(s) it applies to. For <see
/// cref="RollupRuleKind.CountFalse"/> and <see cref="RollupRuleKind.ArgMaxBy"/>, entries may be
/// `"column=alias"` to pin an output name that doesn't follow the `<column>_<suffix>`
/// convention; <see cref="RollupRuleKind.ArgMaxBy"/>'s first entry is the order column instead
/// of a `column=alias` pair.</summary>
public sealed record RollupRule(RollupRuleKind Kind, IReadOnlyList<string> Columns);

/// <summary>
/// One hourly rollup: the output table name, the source table it reads (informational --
/// <see cref="RollupSql.Build"/> always reads from a table named `raw`), the time column, the
/// group key beyond `server_name`/`hour` (which <see cref="RollupSql.Build"/> always adds), and
/// the rules that turn source columns into rollup columns.
/// </summary>
public sealed record RollupSpec(
    string Name,
    string SourceTable,
    string TimeColumn,
    IReadOnlyList<string> Key,
    IReadOnlyList<RollupRule> Rules,
    bool HasSampleInterval = false,
    int Version = 1)
{
    /// <summary>True when a rule is a <see cref="RollupRuleKind.Counter"/>: the rollup then needs each key's last
    /// sample before the window to bridge the counter into the window's first hour.</summary>
    public bool HasCounters => Rules.Any(r => r.Kind == RollupRuleKind.Counter);
}

/// <summary>
/// Pure SQL generation for the H (hourly rollup) grain. <see cref="Build"/> turns a <see cref="RollupSpec"/> into a
/// single DuckDB SELECT over a table named `raw`; it has no I/O and no dependency on how `raw` gets populated.
/// `source_store` is emitted as a typed NULL placeholder; the exporters (<c>HourlyRollupExporter</c>,
/// <c>ChangeOnlyExporter</c>) run `UPDATE rolled SET source_store = '&lt;store&gt;'` right after `CREATE TABLE rolled
/// AS...`, so the store name lives in exactly one place (the exporter's `store` argument). Anything that writes this
/// SQL's output without that step writes NULL source_store and per-store views downstream miss it.
/// </summary>
public static class RollupSql
{
    private static string Q(string identifier) => SqlBuilder.QuoteIdentifier(identifier);

    /// <summary>Splits a `"column=alias"` entry. If there is no `=`, the column is its own alias.</summary>
    private static (string Column, string Alias) SplitAlias(string entry)
    {
        var i = entry.IndexOf('=');
        return i < 0 ? (entry, entry) : (entry[..i], entry[(i + 1)..]);
    }

    /// <summary>The 13 rollup specs, keyed by output table name.</summary>
    public static readonly IReadOnlyDictionary<string, RollupSpec> Specs = BuildSpecs();

    /// <summary>Builds the DuckDB SQL for the named spec's rollup, over a table called `raw`.</summary>
    public static string Build(string name)
    {
        if (!Specs.TryGetValue(name, out var spec))
        {
            throw new ArgumentException($"unknown rollup spec: {name}", nameof(name));
        }

        return Build(spec);
    }

    /// <summary>
    /// Builds the DuckDB SQL for a spec directly, over a table called `raw`. With <paramref name="windowFrom"/> set and
    /// a spec that has Counter rules, rows of `raw` before it are bridge samples: each key's last sample before the
    /// window. They feed only the counters' `_delta_hour_bridged` (the step from that sample into the window's first
    /// hour); every other aggregate, `_delta_in_hour` included, sees the window's own rows alone, and a bridge sample is
    /// never counted. Without <paramref name="windowFrom"/> every row of `raw` is a window row.
    /// </summary>
    public static string Build(RollupSpec spec, DateTime? windowFrom = null)
    {
        var counterColumns = spec.Rules
            .Where(r => r.Kind == RollupRuleKind.Counter)
            .SelectMany(r => r.Columns)
            .ToList();

        var partitionCols = new List<string> { "server_name" };
        partitionCols.AddRange(spec.Key);
        var partitionBy = string.Join(", ", partitionCols.Select(Q));

        string source;
        var where = string.Empty;
        if (counterColumns.Count == 0)
        {
            source = "raw";
        }
        else
        {
            var over = $"OVER (PARTITION BY {partitionBy} ORDER BY {Q(spec.TimeColumn)})";
            var extras = new List<string>();
            string? inWindow = null;
            if (windowFrom is { } from)
            {
                inWindow = $"{Q(spec.TimeColumn)} >= TIMESTAMP '{from.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)}'";
                extras.Add($"{inWindow} AS {Q("_in_window")}");
                where = $"WHERE {Q("_in_window")}\n";
            }

            foreach (var c in counterColumns)
            {
                // _db_: the step from the key's previous sample, on whichever side of the window start it lies.
                extras.Add($"{Q(c)} - lag({Q(c)}) {over} AS {Q("_db_" + c)}");
                // _d_: the step counted inside the hour, only when the previous sample is a window row too.
                extras.Add(inWindow is null
                    ? $"{Q(c)} - lag({Q(c)}) {over} AS {Q("_d_" + c)}"
                    : $"CASE WHEN lag({inWindow}) {over} THEN {Q(c)} - lag({Q(c)}) {over} END AS {Q("_d_" + c)}");
            }

            source = $"(SELECT *, {string.Join(", ", extras)} FROM raw)";
        }

        var select = new List<string>
        {
            $"CAST(NULL AS VARCHAR) AS {Q("source_store")}",
            Q("server_name"),
        };
        select.AddRange(spec.Key.Select(Q));
        select.Add($"date_trunc('hour', {Q(spec.TimeColumn)}) AS {Q("hour")}");

        foreach (var rule in spec.Rules)
        {
            select.AddRange(BuildRuleOutputs(rule, spec.TimeColumn));
        }

        select.Add($"count(*) AS {Q("sample_count")}");
        if (spec.HasSampleInterval)
        {
            select.Add($"CAST(sum({Q("sample_interval_seconds")}) AS BIGINT) AS {Q("sample_interval_seconds_sum")}");
        }

        return $"""
            SELECT {string.Join(",\n               ", select)}
            FROM {source} AS raw
            {where}GROUP BY ALL
            """;
    }

    private static IEnumerable<string> BuildRuleOutputs(RollupRule rule, string timeColumn) => rule.Kind switch
    {
        RollupRuleKind.Gauge => rule.Columns.SelectMany(c => new[]
        {
            $"avg({Q(c)}) AS {Q(c + "_avg")}",
            $"max({Q(c)}) AS {Q(c + "_max")}",
            $"arg_max({Q(c)}, {Q(timeColumn)}) AS {Q(c + "_last")}",
        }),

        RollupRuleKind.GaugeMin => rule.Columns.SelectMany(c => new[]
        {
            $"avg({Q(c)}) AS {Q(c + "_avg")}",
            $"min({Q(c)}) AS {Q(c + "_min")}",
            $"arg_max({Q(c)}, {Q(timeColumn)}) AS {Q(c + "_last")}",
        }),

        RollupRuleKind.DeltaSum => rule.Columns.Select(c =>
            $"sum({Q(c)}) FILTER (WHERE {Q("sample_interval_seconds")} IS DISTINCT FROM 0) AS {Q(c + "_sum")}"),

        RollupRuleKind.Counter => rule.Columns.SelectMany(c => new[]
        {
            $"arg_min({Q(c)}, {Q(timeColumn)}) AS {Q(c + "_first")}",
            $"arg_max({Q(c)}, {Q(timeColumn)}) AS {Q(c + "_last")}",
            // a drop (_d_c < 0) is a reset: count the post-reset value (c) instead of the negative step.
            $"sum(CASE WHEN {Q("_d_" + c)} >= 0 THEN {Q("_d_" + c)} WHEN {Q("_d_" + c)} < 0 THEN {Q(c)} END) AS {Q(c + "_delta_in_hour")}",
            // the same sum with the step from the previous hour's last sample into this hour counted too.
            $"sum(CASE WHEN {Q("_db_" + c)} >= 0 THEN {Q("_db_" + c)} WHEN {Q("_db_" + c)} < 0 THEN {Q(c)} END) AS {Q(c + "_delta_hour_bridged")}",
        }),

        RollupRuleKind.Last => rule.Columns.Select(c =>
            $"arg_max({Q(c)}, {Q(timeColumn)}) AS {Q(c)}"),

        RollupRuleKind.Max => rule.Columns.Select(c => $"max({Q(c)}) AS {Q(c + "_max")}"),

        RollupRuleKind.Min => rule.Columns.Select(c => $"min({Q(c)}) AS {Q(c + "_min")}"),

        RollupRuleKind.CountTrue => rule.Columns.Select(c => $"count_if({Q(c)}) AS {Q(c + "_samples")}"),

        RollupRuleKind.CountFalse => rule.Columns.Select(entry =>
        {
            var (column, alias) = SplitAlias(entry);
            return $"count_if(NOT {Q(column)}) AS {Q(alias)}";
        }),

        RollupRuleKind.ArgMaxBy => BuildArgMaxBy(rule.Columns),

        _ => throw new ArgumentOutOfRangeException(nameof(rule), rule.Kind, "unknown rollup rule kind"),
    };

    private static IEnumerable<string> BuildArgMaxBy(IReadOnlyList<string> columns)
    {
        if (columns.Count < 2)
        {
            throw new ArgumentException("ArgMaxBy needs an order column plus at least one target column", nameof(columns));
        }

        var orderColumn = columns[0];
        for (var i = 1; i < columns.Count; i++)
        {
            var (target, alias) = SplitAlias(columns[i]);
            yield return $"arg_max({Q(target)}, {Q(orderColumn)}) AS {Q(alias)}";
        }
    }

    private static IReadOnlyDictionary<string, RollupSpec> BuildSpecs()
    {
        var specs = new List<RollupSpec>
        {
            new(
                "memory_grant_stats_hourly", "collect.memory_grant_stats", "collection_time",
                Key: ["resource_semaphore_id", "pool_id"],
                Rules:
                [
                    new(RollupRuleKind.Gauge, ["target_memory_mb", "max_target_memory_mb", "total_memory_mb", "granted_memory_mb", "used_memory_mb", "grantee_count", "waiter_count"]),
                    new(RollupRuleKind.GaugeMin, ["available_memory_mb"]),
                    new(RollupRuleKind.DeltaSum, ["timeout_error_count_delta", "forced_grant_count_delta"]),
                    new(RollupRuleKind.Last, ["timeout_error_count", "forced_grant_count"]),
                ],
                HasSampleInterval: true),

            new(
                "cpu_scheduler_stats_hourly", "collect.cpu_scheduler_stats", "collection_time",
                Key: [],
                Rules:
                [
                    new(RollupRuleKind.Gauge, ["total_runnable_tasks_count", "avg_runnable_tasks_count", "total_work_queue_count", "total_current_workers_count", "total_active_request_count", "total_queued_request_count", "total_blocked_task_count", "total_active_parallel_thread_count", "runnable_request_count", "total_request_count", "runnable_percent"]),
                    new(RollupRuleKind.GaugeMin, ["available_physical_memory_kb"]),
                    new(RollupRuleKind.Last, ["max_workers_count", "scheduler_count", "cpu_count", "total_physical_memory_kb", "system_memory_state_desc", "total_node_count", "nodes_online_count", "offline_cpu_count"]),
                    new(RollupRuleKind.CountTrue, ["worker_thread_exhaustion_warning", "runnable_tasks_warning", "blocked_tasks_warning", "queued_requests_warning", "physical_memory_pressure_warning", "offline_cpu_warning"]),
                ]),

            new(
                "session_summary_stats_hourly", "collect.session_summary_stats", "collection_time",
                Key: [],
                Rules:
                [
                    new(RollupRuleKind.Gauge, ["total_sessions", "running_sessions", "sleeping_sessions", "background_sessions", "dormant_sessions", "idle_sessions_over_30min", "sessions_waiting_for_memory", "databases_with_connections"]),
                    new(RollupRuleKind.Last, ["top_application_name", "top_host_name"]),
                    new(RollupRuleKind.Max, ["top_application_connections", "top_host_connections"]),
                ]),

            new(
                "ag_database_replica_states_hourly", "collect.ag_database_replica_states", "collection_time",
                Key: ["ag_name", "database_name", "replica_server_name"],
                Rules:
                [
                    new(RollupRuleKind.Gauge, ["log_send_queue_size", "redo_queue_size", "secondary_lag_seconds", "log_send_rate", "redo_rate"]),
                    new(RollupRuleKind.Max, ["est_redo_completion_time_min", "est_send_drain_time_min"]),
                    new(RollupRuleKind.Last, ["last_commit_time", "last_hardened_time", "last_redone_time", "last_received_time", "synchronization_state_desc"]),
                ]),

            new(
                "pg_statement_stats_hourly", "collect.pg_statement_stats", "collection_time",
                Key: ["queryid", "database_id", "user_id", "toplevel"],
                Rules:
                [
                    new(RollupRuleKind.DeltaSum, ["delta_calls", "delta_total_exec_time_ms", "delta_rows"]),
                    new(RollupRuleKind.Counter, ["calls", "total_exec_time_ms", "rows_returned", "shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written", "temp_blks_read", "temp_blks_written", "blk_read_time_ms", "blk_write_time_ms", "storage_blks_read", "orcache_blks_hit", "storage_blk_read_time_ms", "orcache_blk_read_time_ms", "wal_records", "wal_fpi", "wal_bytes", "total_exec_peakmem_bytes"]),
                    new(RollupRuleKind.Max, ["max_exec_time_ms", "max_exec_peakmem_bytes"]),
                    new(RollupRuleKind.Min, ["min_exec_time_ms"]),
                    // mean_exec_time_ms is dropped: it's derivable from the counter columns.
                ],
                HasSampleInterval: true),

            new(
                "pg_database_stats_hourly", "collect.pg_database_stats", "collection_time",
                Key: ["database_name"],
                Rules:
                [
                    new(RollupRuleKind.Counter, ["xact_commit", "xact_rollback", "blks_read", "blks_hit", "temp_files", "temp_bytes", "deadlocks"]),
                    new(RollupRuleKind.Gauge, ["numbackends"]),
                    new(RollupRuleKind.Last, ["stats_reset"]),
                ]),

            new(
                "pg_wait_stats_hourly", "collect.pg_wait_stats", "collection_time",
                Key: ["wait_type", "wait_event"],
                Rules:
                [
                    new(RollupRuleKind.DeltaSum, ["delta_waits", "delta_wait_time_us"]),
                    new(RollupRuleKind.Last, ["waits", "wait_time_us"]),
                ],
                HasSampleInterval: true),

            new(
                "pg_io_stats_hourly", "collect.pg_io_stats", "collection_time",
                Key: ["backend_type", "object_type", "context"],
                Rules:
                [
                    new(RollupRuleKind.Counter, ["reads", "read_time_ms", "writes", "write_time_ms", "writebacks", "writeback_time_ms", "extends", "extend_time_ms", "hits", "evictions", "reuses", "fsyncs", "fsync_time_ms", "read_bytes", "write_bytes", "extend_bytes"]),
                    new(RollupRuleKind.Last, ["op_bytes", "stats_reset"]),
                ]),

            new(
                "pg_cpu_utilization_hourly", "collect.pg_cpu_utilization", "collection_time",
                Key: [],
                Rules:
                [
                    new(RollupRuleKind.Gauge, ["cpu_percent", "acu_utilization_percent", "serverless_capacity_acu", "memory_active_bytes", "memory_cached_bytes", "memory_buffers_bytes"]),
                    new(RollupRuleKind.GaugeMin, ["memory_free_bytes"]),
                    new(RollupRuleKind.Last, ["max_configured_acu", "memory_total_bytes", "configured_memory_bytes"]),
                ]),

            new(
                "pg_write_stats_hourly", "collect.pg_write_stats", "collection_time",
                Key: [],
                Rules:
                [
                    new(RollupRuleKind.Counter, ["num_timed", "num_requested", "num_done", "restartpoints_timed", "restartpoints_req", "restartpoints_done", "checkpoint_write_time_ms", "checkpoint_sync_time_ms", "buffers_written_checkpoint", "slru_written", "buffers_clean", "maxwritten_clean", "buffers_alloc", "buffers_backend", "buffers_backend_fsync", "wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full", "wal_write", "wal_sync", "wal_write_time_ms", "wal_sync_time_ms"]),
                    new(RollupRuleKind.Last, ["checkpointer_stats_reset", "bgwriter_stats_reset", "wal_stats_reset", "postmaster_start_time"]),
                ]),

            new(
                "pg_replication_stats_hourly", "collect.pg_replication_stats", "collection_time",
                Key: ["application_name", "client_addr"],
                Rules:
                [
                    new(RollupRuleKind.Gauge, ["sent_bytes_behind", "write_bytes_behind", "flush_bytes_behind", "replay_bytes_behind", "write_lag_ms", "flush_lag_ms", "replay_lag_ms"]),
                    new(RollupRuleKind.Last, ["state", "sync_state", "sync_priority", "backend_start"]),
                ]),

            new(
                "pg_replication_slot_stats_hourly", "collect.pg_replication_slot_stats", "collection_time",
                Key: ["slot_name"],
                Rules:
                [
                    new(RollupRuleKind.Max, ["retained_wal_bytes", "xmin_age", "catalog_xmin_age"]),
                    new(RollupRuleKind.GaugeMin, ["safe_wal_size_bytes"]),
                    new(RollupRuleKind.Last, ["retained_wal_bytes", "xmin_age", "catalog_xmin_age", "slot_type", "plugin", "database_name", "is_active", "wal_status", "invalidation_reason", "conflicting", "inactive_since"]),
                    new(RollupRuleKind.CountFalse, ["is_active=inactive_samples"]),
                ]),

            new(
                "pg_xmin_horizon_hourly", "collect.pg_xmin_horizon", "collection_time",
                Key: ["source"],
                Rules:
                [
                    new(RollupRuleKind.Max, ["xmin_age"]),
                    new(RollupRuleKind.ArgMaxBy, ["xmin_age", "holder=holder_at_max", "detail=detail_at_max"]),
                    new(RollupRuleKind.CountTrue, ["is_winner"]),
                ]),
        };

        return specs.ToDictionary(s => s.Name, s => s with { Name = s.Name });
    }
}
