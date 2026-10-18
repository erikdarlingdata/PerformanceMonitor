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

       A SEPARATE table rather than columns on config_edge_trigger_watermarks, because the key is wrong:
       watermarks are per (server, metric), occurrences are per (server, metric, dedup_key) — two deadlocks
       on different tables are different incidents with different totals.

       last_observed_at is not display data — it is what makes a row's staleness decidable. Rows are deleted
       when the incident ends, but a crash mid-incident strands one, and a stranded row trusted on that
       fingerprint's NEXT incident would decay its already-counted mark to the new window count and report
       the recurrence as nothing new. */
    /* The BUILT-IN alert catalog's persistence-gate state (#3282), the twin of Darling's
       config.alert_persistence_state (PgMigrations V118) — same columns, same key. "How long must this
       condition hold before it counts" for the gauge alerts: consecutive breaching samples so far,
       consecutive clearing samples so far, and whether an incident is currently open.

       A SEPARATE table rather than columns on config_edge_trigger_watermarks: that column is one
       monotonic integer meaning "the highest already-alerted rolling-window count", and a resettable
       counter pair is not that shape.

       last_observed_sample_at is the gate's observation identity, not display data. The gate counts
       consecutive breaching SAMPLES while the sweep runs twice as often as a CPU sample arrives, so
       without it a re-read of one sample would count as a second observation and the streak would fill
       from data that never changed. */
    public const string CreateAlertPersistenceStateTable = @"
CREATE TABLE IF NOT EXISTS config_alert_persistence_state (
    server_id INTEGER NOT NULL,
    metric_name VARCHAR NOT NULL,
    consecutive_breaches INTEGER NOT NULL,
    consecutive_clears INTEGER NOT NULL,
    firing BOOLEAN NOT NULL,
    last_observed_sample_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (server_id, metric_name)
)";

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
    job_name_pattern VARCHAR,
    client_app_pattern VARCHAR
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

    /* Query tags (#5989, schema v70): the Lite twin of Darling's config.query_tags / query_tag_assignments /
       query_tag_rules / query_tag_map / query_tag_state and collect.query_tag_sightings (QueryTagSchema). Same
       columns, but only two of Darling's CHECKs are enforced here: ck_query_tag_assignments_key (the kind and column
       shape) and ck_query_tag_rules_pattern (non-blank pattern). The archive restore writes these tables verbatim, so
       the store is the one guard. DuckDB has no IDENTITY, no self-FK cascade and no partial or expression
       unique index, so ids are assigned in C# (max + 1 under the write lock), the subtree delete is walked by hand
       and "unique name per parent" and "one assignment per key" are enforced by the store (LocalDataService.QueryTags).
       The tags, assignments and rules are user choices and survive an archive reset; the sightings and the map are
       derived and are rebuilt. */
    public const string CreateQueryTagsTable = @"
CREATE TABLE IF NOT EXISTS query_tags (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    parent_id INTEGER,
    sort_order INTEGER NOT NULL DEFAULT 0,
    colour VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    CHECK (trim(name) <> '')
)";

    public const string CreateQueryTagAssignmentsTable = @"
CREATE TABLE IF NOT EXISTS query_tag_assignments (
    id INTEGER PRIMARY KEY,
    tag_id INTEGER NOT NULL,
    kind VARCHAR NOT NULL,
    server_id INTEGER,
    query_hash VARCHAR,
    database_name VARCHAR,
    schema_name VARCHAR,
    object_name VARCHAR,
    query_id BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    created_by VARCHAR NOT NULL,
    CHECK (kind IN ('hash', 'object'))
)";

    public const string CreateQueryTagRulesTable = @"
CREATE TABLE IF NOT EXISTS query_tag_rules (
    id INTEGER PRIMARY KEY,
    tag_id INTEGER,
    priority INTEGER NOT NULL DEFAULT 100,
    enabled BOOLEAN NOT NULL DEFAULT true,
    server_id INTEGER,
    match_field VARCHAR NOT NULL,
    pattern VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    created_by VARCHAR NOT NULL,
    CHECK (match_field IN ('application', 'login', 'host', 'database', 'object', 'text', 'text_tag')),
    CHECK (match_field = 'text_tag' OR tag_id IS NOT NULL)
)";

    /* The text_sample cap is the shared constant, so the table and the collectors cannot disagree. One row per
       (server_id, key_kind, key) AND caller triple, as Darling's ux_query_tag_sightings_key: that index COALESCEs the
       three caller fields to '', so a sighting with no application is one row rather than many, and two callers of one
       query_hash are two rows the resolver can match separately. DuckDB has no expression index and treats two NULLs as
       distinct in a unique key, so the fold is in the column defaults instead: NOT NULL DEFAULT '' gives a seven-part key
       (the database is part of the identity since v71, so one hash seen in two databases is two rows) as a plain PRIMARY KEY, which is what the sightings write (#5990) needs for its ON CONFLICT upsert. */
    public static readonly string CreateQueryTagSightingsTable = string.Create(System.Globalization.CultureInfo.InvariantCulture, $@"
CREATE TABLE IF NOT EXISTS query_tag_sightings (
    server_id INTEGER NOT NULL,
    key_kind VARCHAR NOT NULL,
    key VARCHAR NOT NULL,
    database_name VARCHAR NOT NULL DEFAULT '',
    object_name VARCHAR,
    application_name VARCHAR NOT NULL DEFAULT '',
    login_name VARCHAR NOT NULL DEFAULT '',
    host_name VARCHAR NOT NULL DEFAULT '',
    text_sample VARCHAR,
    first_seen TIMESTAMP NOT NULL DEFAULT current_timestamp,
    last_seen TIMESTAMP NOT NULL DEFAULT current_timestamp,
    seen_count BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (server_id, key_kind, key, database_name, application_name, login_name, host_name),
    CHECK (key_kind IN ('hash', 'object', 'caller')),
    CHECK (text_sample IS NULL OR length(text_sample) <= {PerformanceMonitor.Common.QueryTags.QueryTagLimits.TextSampleCap}),
    CHECK (seen_count >= 1)
)");

    public const string CreateQueryTagMapTable = @"
CREATE TABLE IF NOT EXISTS query_tag_map (
    server_id INTEGER NOT NULL,
    key_kind VARCHAR NOT NULL,
    key VARCHAR NOT NULL,
    tag_id INTEGER NOT NULL,
    source VARCHAR NOT NULL,
    rule_id INTEGER,
    matched_field VARCHAR,
    matched_value VARCHAR,
    built_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (server_id, key_kind, key),
    CHECK (key_kind IN ('hash', 'object', 'caller')),
    CHECK (source IN ('hand_hash', 'hand_object', 'rule'))
)";

    public const string CreateQueryTagStateTable = @"
CREATE TABLE IF NOT EXISTS query_tag_state (
    id INTEGER PRIMARY KEY,
    rules_version BIGINT NOT NULL DEFAULT 1,
    built_version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    CHECK (id = 1)
)";

    /// <summary>The six query-tag tables, in the order the v70 step creates them (none depends on another).</summary>
    public static IEnumerable<string> QueryTagTableStatements()
    {
        yield return CreateQueryTagsTable;
        yield return CreateQueryTagAssignmentsTable;
        yield return CreateQueryTagRulesTable;
        yield return CreateQueryTagSightingsTable;
        yield return CreateQueryTagMapTable;
        yield return CreateQueryTagStateTable;
    }

    /* #5750 (schema v69): the scoped notification routes, the Lite twin of Darling's
       config.config_notification_routes. A route says where an alert goes for a metric match, optionally limited to
       ONE server (scope_server_id: the hash id the alert path and server_tag_map use) or to a tag and its whole
       subtree (scope_tag_id), never both. NO destination value is stored here: the Teams, Slack and Generic URLs and
       the PagerDuty key are secrets and live in Windows Credential Manager keyed by route id and channel, exactly
       like the app-wide webhook URLs; the *_set columns only record that one was saved, so a credential that cannot
       be read later turns that channel OFF for the route instead of falling through to the app-wide URL (#5366).
       The *_secret_gen columns (#5775) hold, per channel, the random generation token of the Credential Manager key the
       row reads (NotificationRoute_<id>_<Channel>_<gen>). A save writes the new value under a new token first and moves the
       row to it with one UPDATE, so a crash between the two leaves the row and the key it points at in agreement; a key no
       row points at is swept at start. NULL with the flag set means the destination is unreadable.
       smtp_recipients is a plain list of addresses, not a secret, and is stored as the app-wide recipients are
       (a plain setting). Ids are assigned in C# (max + 1) under the write lock, like server_tags.
       Two CHECKs, which DuckDB enforces at INSERT/UPDATE: at most one scope, and the '*' (every metric) match is legal
       only on a scoped route. Deliberately NO foreign keys, ON DELETE cascade or SET NULL: a route whose server or tag
       is gone must stay, match nothing and show as orphaned, never widen to fleet-wide. */
    public const string CreateNotificationRoutesTable = @"
CREATE TABLE IF NOT EXISTS notification_routes (
    route_id INTEGER PRIMARY KEY,
    metric_match VARCHAR NOT NULL,
    scope_server_id INTEGER,
    scope_tag_id INTEGER,
    teams_set BOOLEAN NOT NULL DEFAULT false,
    slack_set BOOLEAN NOT NULL DEFAULT false,
    generic_set BOOLEAN NOT NULL DEFAULT false,
    pagerduty_set BOOLEAN NOT NULL DEFAULT false,
    smtp_recipients VARCHAR NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT true,
    teams_secret_gen VARCHAR,
    slack_secret_gen VARCHAR,
    generic_secret_gen VARCHAR,
    pagerduty_secret_gen VARCHAR,
    agent_level INTEGER,
    agent_max_per_hour INTEGER,
    agent_max_per_day INTEGER,
    agent_lease_minutes INTEGER,
    agent_expiry_minutes INTEGER,
    CHECK (scope_server_id IS NULL OR scope_tag_id IS NULL),
    CHECK (metric_match <> '*' OR scope_server_id IS NOT NULL OR scope_tag_id IS NOT NULL)
)";

    /* #5820 (schema v69): the six tables of Level 1 investigations, the Lite twins of Darling's V184 tables (plus
       V188's alert facts on investigation_request_alerts). Darling's text is InvestigationTables.CreateSql; the names
       here are its bare table names. What DuckDB lacks shapes them, as for the route and tag tables above:
       - No IDENTITY and no SEQUENCE: every BIGINT id is assigned max + 1 by the store under the write lock.
       - No foreign keys and no ON DELETE CASCADE: retention deletes the child rows (alerts, reports, journal) first.
       - No partial index: investigation_journal.once_key is a plain UNIQUE column. DuckDB treats NULLs as distinct
         in it, so any number of rows carry no key, and INSERT ... ON CONFLICT (once_key) DO NOTHING skips a repeated
         key (InvestigationDuckDbSpikeTests proves both).
       - JSON is VARCHAR (DuckDB has no jsonb); the store validates the shape before it writes.
       - No secondary index, and none on a column an UPDATE writes: DuckDB runs an UPDATE of an indexed column as a
         delete and an insert, which a second update of the same row in one transaction trips over.
       - CHECKs only on state, verdict and family, written out from InvestigationToolCore.States, Verdicts and
         Families (and the note states). Darling's other CHECKs (claim count, agent id pattern, text lengths, the
         evidence shape) are the store's and the tool core's to enforce here.
       - Timestamps are TIMESTAMP holding UTC to the microsecond, and have no DEFAULT: DuckDB's current_timestamp is
         local-zone, so the store binds every time. Request and claim ids are UUID, as in Darling. */

    /// <summary>
    /// One investigation request per server, alert family and 30-minute grouping window, with its state, the current
    /// claim and its lease, the claim count, the expiry and the brief composed at claim. Mirrors Darling's
    /// <c>config.investigation_requests</c>.
    /// </summary>
    public const string CreateInvestigationRequestsTable = @"
CREATE TABLE IF NOT EXISTS investigation_requests (
    request_id UUID PRIMARY KEY,
    route_id INTEGER NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    family VARCHAR NOT NULL,
    first_metric VARCHAR NOT NULL,
    first_dedup_key VARCHAR,
    story_path_hash VARCHAR,
    first_alert_at TIMESTAMP NOT NULL,
    last_alert_at TIMESTAMP NOT NULL,
    window_until TIMESTAMP NOT NULL,
    alert_count INTEGER NOT NULL DEFAULT 1,
    state VARCHAR NOT NULL DEFAULT 'open',
    autonomy_level SMALLINT NOT NULL DEFAULT 1,
    opened_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    lease_minutes INTEGER NOT NULL DEFAULT 15,
    claimed_by VARCHAR,
    claim_id UUID,
    claimed_at TIMESTAMP,
    lease_until TIMESTAMP,
    claim_count INTEGER NOT NULL DEFAULT 0,
    brief_format SMALLINT,
    brief VARCHAR,
    reported_at TIMESTAMP,
    closed_at TIMESTAMP,
    closed_by VARCHAR,
    closed_reason VARCHAR,
    evidence_state VARCHAR,
    CHECK (family IN ('blocking', 'query_performance', 'resources', 'availability', 'jobs', 'self_monitor', 'other')),
    CHECK (state IN ('open', 'claimed', 'reported', 'closed', 'expired'))
)";

    /// <summary>
    /// One row per alert that opened or joined a request, and per incident dedup key of that alert, with the alert's
    /// facts (value, threshold, muted, detail, context). Mirrors Darling's <c>config.investigation_request_alerts</c>
    /// (V184 plus V188).
    /// </summary>
    public const string CreateInvestigationRequestAlertsTable = @"
CREATE TABLE IF NOT EXISTS investigation_request_alerts (
    request_alert_id BIGINT PRIMARY KEY,
    request_id UUID NOT NULL,
    alert_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    metric_name VARCHAR NOT NULL,
    dedup_key VARCHAR,
    current_value DOUBLE,
    threshold_value DOUBLE,
    muted BOOLEAN,
    detail_text VARCHAR,
    context_json VARCHAR
)";

    /// <summary>
    /// The one report an agent returned for a request (UNIQUE request_id; the row is never updated). Mirrors Darling's
    /// <c>config.investigation_reports</c>.
    /// </summary>
    public const string CreateInvestigationReportsTable = @"
CREATE TABLE IF NOT EXISTS investigation_reports (
    report_id BIGINT PRIMARY KEY,
    request_id UUID NOT NULL UNIQUE,
    report_format SMALLINT NOT NULL,
    agent_id VARCHAR NOT NULL,
    claim_id UUID NOT NULL,
    submitted_at TIMESTAMP NOT NULL,
    verdict VARCHAR NOT NULL,
    root_cause VARCHAR NOT NULL,
    root_cause_found BOOLEAN NOT NULL DEFAULT false,
    summary VARCHAR,
    evidence VARCHAR NOT NULL,
    server_id INTEGER NOT NULL,
    metric_name VARCHAR,
    dedup_key VARCHAR,
    story_path_hash VARCHAR,
    CHECK (verdict IN ('real', 'false_positive', 'needs_person'))
)";

    /// <summary>
    /// The append-only journal of every open, join, limit, claim, lease expiry, report and refusal. An event recorded
    /// once carries a once_key, UNIQUE where set. Mirrors Darling's <c>config.investigation_journal</c>.
    /// </summary>
    public const string CreateInvestigationJournalTable = @"
CREATE TABLE IF NOT EXISTS investigation_journal (
    journal_id BIGINT PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    request_id UUID,
    route_id INTEGER,
    server_id INTEGER,
    event VARCHAR NOT NULL,
    actor VARCHAR NOT NULL,
    reason VARCHAR,
    detail VARCHAR,
    once_key VARCHAR UNIQUE
)";

    /// <summary>
    /// Notes about a server or a finding that a person approves before a brief carries them. Mirrors Darling's
    /// <c>config.investigation_notes</c>.
    /// </summary>
    public const string CreateInvestigationNotesTable = @"
CREATE TABLE IF NOT EXISTS investigation_notes (
    note_id BIGINT PRIMARY KEY,
    server_id INTEGER,
    story_path_hash VARCHAR,
    dedup_key VARCHAR,
    metric_name VARCHAR,
    note_text VARCHAR NOT NULL,
    author VARCHAR NOT NULL,
    source_request_id UUID,
    state VARCHAR NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    decided_at TIMESTAMP,
    decided_by VARCHAR,
    CHECK (state IN ('pending', 'approved', 'rejected'))
)";

    /// <summary>
    /// The free-text environment profile an agent reads, per server, per tag or fleet-wide (both scopes NULL). One
    /// profile per scope is the store's rule (DuckDB has no expression unique index). Mirrors Darling's
    /// <c>config.agent_environment_profiles</c>.
    /// </summary>
    public const string CreateAgentEnvironmentProfilesTable = @"
CREATE TABLE IF NOT EXISTS agent_environment_profiles (
    profile_id BIGINT PRIMARY KEY,
    scope_server_id INTEGER,
    scope_tag_id INTEGER,
    profile_text VARCHAR NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    modified_by VARCHAR
)";

    /// <summary>
    /// One frozen evidence section per request and section name (v72, #6010). Mirrors Darling's
    /// <c>config.investigation_evidence</c> (V193). A section is <c>pending</c>, then <c>ready</c> or <c>not_collected</c>,
    /// and the store never rewrites it after that. No identity, no foreign key and no CHECK on the content size, as the tables
    /// above: the ids are max + 1 under the write lock, retention deletes the rows with their requests, and the capture holds
    /// the content to the report evidence's cap.
    /// </summary>
    public const string CreateInvestigationEvidenceTable = @"
CREATE TABLE IF NOT EXISTS investigation_evidence (
    evidence_id BIGINT PRIMARY KEY,
    request_id UUID NOT NULL,
    section VARCHAR NOT NULL,
    state VARCHAR NOT NULL DEFAULT 'pending',
    content VARCHAR,
    captured_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    UNIQUE (request_id, section),
    CHECK (state IN ('pending', 'ready', 'not_collected'))
)";

    /// <summary>The investigation tables in the order the v69 step creates them (none depends on another).</summary>
    public static IEnumerable<string> InvestigationTableStatements()
    {
        yield return CreateInvestigationRequestsTable;
        yield return CreateInvestigationRequestAlertsTable;
        yield return CreateInvestigationReportsTable;
        yield return CreateInvestigationJournalTable;
        yield return CreateInvestigationNotesTable;
        yield return CreateAgentEnvironmentProfilesTable;
        yield return CreateInvestigationEvidenceTable;
    }

    /// <summary>
    /// All table creation statements: the hand-written non-collector tables, then one table per
    /// collector in <see cref="PerformanceMonitor.Collectors.CollectorCatalog"/>, generated by
    /// <see cref="DuckDbSchemaGenerator"/>. Order is immaterial — every statement is
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
        yield return CreateAlertPersistenceStateTable;
        yield return CreateCollectorStateTable;
        yield return CreateMuteRulesTable;
        yield return CreateDismissedArchiveAlertsTable;
        yield return CreateDatabaseStateExpectedTable;
        yield return CreateServerTagsTable;
        yield return CreateServerTagMapTable;
        yield return CreateNotificationRoutesTable;

        foreach (var statement in QueryTagTableStatements())
        {
            yield return statement;
        }

        foreach (var statement in InvestigationTableStatements())
        {
            yield return statement;
        }

        foreach (var statement in DuckDbSchemaGenerator.CreateTableStatements())
        {
            yield return statement;
        }
    }

    /// <summary>
    /// All index creation statements: the hand-written non-collector indexes (collection_log,
    /// dismissed_archive_alerts) plus one generated index per catalog collector (server_config and
    /// database_config have none).
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
