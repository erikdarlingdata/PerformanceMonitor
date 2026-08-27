using System.Collections.Generic;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// DuckDB table schema definitions.
///
/// <para>The 35 COLLECTOR tables (wait_stats, query_stats, index_object_stats, …) are NOT hand-written
/// here anymore — their DDL is GENERATED from the shared <see cref="PerformanceMonitor.Collectors.CollectorCatalog"/>
/// by <see cref="DuckDbSchemaGenerator"/>, the DuckDB analog of Darling's <c>PgSchemaGenerator</c>, so a
/// collector column change lands in one place and can never silently drift from Darling's store. The
/// generated DDL is proven byte-for-byte storage-equivalent to the former hand-written tables by
/// <c>DuckDbSchemaEquivalenceTests</c>.</para>
///
/// <para>The tables below are the NON-collector tables (registry, schedule, log, and the alert /
/// coordination surface). They are not in the collector catalog, so they stay hand-written.</para>
/// </summary>
public static class Schema
{
    public const string CreateServersTable = @"
CREATE TABLE IF NOT EXISTS servers (
    server_id INTEGER PRIMARY KEY,
    server_name VARCHAR NOT NULL,
    display_name VARCHAR,
    use_windows_auth BOOLEAN NOT NULL DEFAULT TRUE,
    username VARCHAR,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)";

    public const string CreateCollectionScheduleTable = @"
CREATE TABLE IF NOT EXISTS collection_schedule (
    schedule_id INTEGER PRIMARY KEY,
    collector_name VARCHAR NOT NULL UNIQUE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    frequency_minutes INTEGER NOT NULL DEFAULT 15,
    last_run_time TIMESTAMP,
    next_run_time TIMESTAMP,
    max_duration_minutes INTEGER DEFAULT 5,
    retention_days INTEGER DEFAULT 30,
    description VARCHAR,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)";

    public const string CreateCollectionLogTable = @"
CREATE TABLE IF NOT EXISTS collection_log (
    log_id BIGINT PRIMARY KEY,
    server_id INTEGER NOT NULL,
    server_name VARCHAR,
    collector_name VARCHAR NOT NULL,
    collection_time TIMESTAMP NOT NULL,
    duration_ms INTEGER,
    status VARCHAR NOT NULL,
    error_message VARCHAR,
    rows_collected INTEGER,
    sql_duration_ms INTEGER,
    duckdb_duration_ms INTEGER,
    fanout_item_count INTEGER,
    slowest_item VARCHAR,
    slowest_item_ms INTEGER
)";

    public const string CreateCollectionLogIndex = @"
CREATE INDEX IF NOT EXISTS idx_collection_log_time ON collection_log(server_id, collection_time)";

    public const string CreateAlertLogTable = @"
CREATE TABLE IF NOT EXISTS config_alert_log (
    alert_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    metric_name VARCHAR NOT NULL,
    current_value DOUBLE PRECISION NOT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    alert_sent BOOLEAN NOT NULL DEFAULT false,
    notification_type VARCHAR NOT NULL DEFAULT 'tray',
    send_error VARCHAR,
    dismissed BOOLEAN NOT NULL DEFAULT false,
    muted BOOLEAN NOT NULL DEFAULT false,
    detail_text VARCHAR,
    context_json VARCHAR
)";

    /* Edge-trigger watermarks for the rolling-count blocking/deadlock alert gate (#1091) and the
       time-based failed-Agent-job watermark. Persisted so the watermark survives an app restart
       (#1145): without it the in-memory watermark resets and the first post-restart sweep re-fires
       the same alert (and re-posts the same webhook) for events still lingering in the lookback
       window — including failed-job toasts the user already saw and dismissed before the restart.
       Keyed (server_id, metric_name); one short row per server/metric, upserted on change.
       Count metrics (blocking/deadlock) use the INTEGER watermark column; the failed-job metric
       uses watermark_time (the newest already-alerted failure's server-local run time). */
    public const string CreateEdgeTriggerWatermarksTable = @"
CREATE TABLE IF NOT EXISTS config_edge_trigger_watermarks (
    server_id INTEGER NOT NULL,
    metric_name VARCHAR NOT NULL,
    watermark INTEGER NOT NULL,
    watermark_time TIMESTAMP,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (server_id, metric_name)
)";

    /* Monotonic per-fingerprint occurrence counters for the alert engine's incidents (#2216), the twin of
       Darling's config.incident_occurrences (PgMigrations V61) — same columns, same key. The count that
       rides on an alert incident is a rolling-window GAUGE: it rises as events arrive and falls as they age
       out, so a consumer that only sees throttled deliveries (one per #1154 per-fingerprint cooldown) cannot
       tell nothing-happened from three-happened-while-three-aged-out. This table is the accumulator's
       memory across deliveries and across restarts.

       A SEPARATE table rather than columns on config_edge_trigger_watermarks, for two reasons that hold
       independently. The key is wrong: watermarks are per (server, metric), occurrences are per
       (server, metric, dedup_key) — two deadlocks on different tables are different incidents with
       different totals. And that row is written with INSERT OR REPLACE over a PARTIAL column list, which
       resets every unlisted column to its default: a counter living there would zero itself on every fired
       alert, i.e. exactly when it is read.

       last_observed_at is not display data — it is what makes a row's staleness decidable. Rows are deleted
       when the incident ends, but a crash mid-incident strands one, and a stranded row trusted on that
       fingerprint's NEXT incident would decay its already-counted mark to the new window count and report
       the recurrence as nothing new. */
    public const string CreateIncidentOccurrencesTable = @"
CREATE TABLE IF NOT EXISTS config_incident_occurrences (
    server_id INTEGER NOT NULL,
    metric_name VARCHAR NOT NULL,
    dedup_key VARCHAR NOT NULL,
    total_occurrences BIGINT NOT NULL,
    observed_window_count INTEGER NOT NULL,
    incident_started_at TIMESTAMP NOT NULL,
    last_observed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (server_id, metric_name, dedup_key)
)";

    /* Per-server collector state that is NOT derivable from the collected rows, so it cannot be a MAX()
       over the collector's own table the way the event_time / instance_id watermarks are (#1962). Today
       one collector declares state: default_trace_events stores the trace FILE it read, and compares it
       next cycle to decide whether it can read just the current rollover file (the 5.0x steady-state
       saving) or must re-read the whole set because the trace rolled. It has to live here rather than on
       the payload because the cycles that need it most collect zero rows — a server whose trace churns
       without producing curated events must still notice the rollover. Keyed (server_id, collector_name,
       state_key); one short row per collector that declares a key, upserted after every cycle. Darling's
       twin is collect.collector_state (PgMigrations V44) — same columns, same key. */
    public const string CreateCollectorStateTable = @"
CREATE TABLE IF NOT EXISTS collector_state (
    server_id INTEGER NOT NULL,
    collector_name VARCHAR NOT NULL,
    state_key VARCHAR NOT NULL,
    state_value VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (server_id, collector_name, state_key)
)";

    public const string CreateMuteRulesTable = @"
CREATE TABLE IF NOT EXISTS config_mute_rules (
    id VARCHAR NOT NULL PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at_utc TIMESTAMP NOT NULL,
    expires_at_utc TIMESTAMP,
    reason VARCHAR,
    server_name VARCHAR,
    metric_name VARCHAR,
    database_pattern VARCHAR,
    query_text_pattern VARCHAR,
    wait_type_pattern VARCHAR,
    job_name_pattern VARCHAR
)";

    public const string CreateDismissedArchiveAlertsTable = @"
CREATE TABLE IF NOT EXISTS dismissed_archive_alerts (
    alert_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    metric_name VARCHAR NOT NULL,
    dismissed_at TIMESTAMP NOT NULL DEFAULT current_timestamp
)";

    public const string CreateDismissedArchiveAlertsIndex = @"
CREATE INDEX IF NOT EXISTS idx_dismissed_archive_alerts
ON dismissed_archive_alerts (alert_time, server_id, metric_name)";

    /* The per-(server, database) expected state the baseline-deviation database-state alert compares
       the current collected state against. Auto-seeded from first observation (is_user_override =
       false) and user-editable via the override editor (is_user_override = true); the "(ignore)"
       sentinel in expected_state opts a database out of the alert entirely. Keyed (server_id,
       database_name); the Darling twin is config.database_state_expected (PgMigrations V40). */
    public const string CreateDatabaseStateExpectedTable = @"
CREATE TABLE IF NOT EXISTS config_database_state_expected (
    server_id INTEGER NOT NULL,
    database_name VARCHAR NOT NULL,
    expected_state VARCHAR NOT NULL,
    is_user_override BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    /* #2203: the edge-trigger memory, Darling's V60 pair ported. Nullable because NULL means never
       announced -- which is what a first observation, a fresh store and a recovered database all look
       like, and all three must be free to alert. */
    last_alerted_state VARCHAR,
    last_alerted_at TIMESTAMP,
    PRIMARY KEY (server_id, database_name)
)";

    /* Fleet tags (#2020 2b-i): the user's visual organisation of the server list, the twin of the Darling
       viewer's config.server_tags tree + config.server_tag_map (PgMigrations V32 + the V50 colour column).
       server_tags is hierarchical (parent_id null = a root tag) with an optional #RRGGBB colour; server_tag_map
       is the many-to-many join keyed on the SAME server_id hash the Overview cards use (RemoteCollectorService
       .GetServerId). Deliberately NO IDENTITY, foreign keys, or expression unique index — DuckDB has none of
       those the way Postgres does — so ids are assigned in C# and the tree invariants (unique name per parent,
       cascade-on-delete, cycle + depth caps) are enforced in the tag store and the Manage Tags window, exactly
       where the Darling viewer already enforces the ones Postgres can't. */
    public const string CreateServerTagsTable = @"
CREATE TABLE IF NOT EXISTS server_tags (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    parent_id INTEGER,
    sort_order INTEGER NOT NULL DEFAULT 0,
    colour VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
)";

    public const string CreateServerTagMapTable = @"
CREATE TABLE IF NOT EXISTS server_tag_map (
    server_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (server_id, tag_id)
)";

    public const string CreateServerTagMapIndex =
        "CREATE INDEX IF NOT EXISTS idx_server_tag_map_tag ON server_tag_map(tag_id)";

    /// <summary>
    /// All table creation statements: the hand-written non-collector tables, then the 36 collector
    /// tables generated from <see cref="PerformanceMonitor.Collectors.CollectorCatalog"/> by
    /// <see cref="DuckDbSchemaGenerator"/> (43 total). Order is immaterial — every statement is
    /// <c>CREATE TABLE IF NOT EXISTS</c> and the tables have no inter-table dependencies.
    /// </summary>
    public static IEnumerable<string> GetAllTableStatements()
    {
        yield return CreateServersTable;
        yield return CreateCollectionScheduleTable;
        yield return CreateCollectionLogTable;
        yield return CreateAlertLogTable;
        yield return CreateEdgeTriggerWatermarksTable;
        yield return CreateIncidentOccurrencesTable;
        yield return CreateCollectorStateTable;
        yield return CreateMuteRulesTable;
        yield return CreateDismissedArchiveAlertsTable;
        yield return CreateDatabaseStateExpectedTable;
        yield return CreateServerTagsTable;
        yield return CreateServerTagMapTable;

        foreach (var statement in DuckDbSchemaGenerator.CreateTableStatements())
        {
            yield return statement;
        }
    }

    /// <summary>
    /// All index creation statements: the hand-written non-collector indexes (collection_log,
    /// dismissed_archive_alerts) plus the 34 generated collector indexes (server_config and
    /// database_config have none), 36 total.
    /// </summary>
    public static IEnumerable<string> GetAllIndexStatements()
    {
        yield return CreateCollectionLogIndex;

        foreach (var statement in DuckDbSchemaGenerator.CreateIndexStatements())
        {
            yield return statement;
        }

        yield return CreateDismissedArchiveAlertsIndex;
    }
}
