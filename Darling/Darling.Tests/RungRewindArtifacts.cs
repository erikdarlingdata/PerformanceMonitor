/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Darling.Tests;

/// <summary>
/// The store's probe ladder, written ONCE (#5950): for each rung the viewer's schema probe names
/// (<c>ViewerDataService.MapProbedSchemaVersion</c>), the probe parameter that is the rung's sentinel and, for the rungs the live
/// tests rewind a store below, the SQL that removes the sentinel. The probe maps the NEWEST sentinel present, so a rewind that
/// only deletes the version rows and the older rungs' objects still reads as the newest rung while that rung's sentinel is on
/// disk (#5641: V177's three <c>config.config_alert_settings</c> columns made <c>CaggGroupIndexDropLiveTests</c> read 177 on a
/// store rewound to V150/V151).
/// <para>A new rung that adds a probe sentinel adds ONE entry to <see cref="Ladder"/>, in this file: its version, the
/// parameter it adds to <c>MapProbedSchemaVersion</c>, and the SQL that removes it. <see cref="DropSentinelsAboveAsync"/>
/// then removes it from every rewind chain, and <c>MigrationProbeLadderTests</c> checks the probe answers it.</para>
/// </summary>
internal static class RungRewindArtifacts
{
    /// <summary>One rung of the ladder. <c>DropSql</c> is null for a rung no live test rewinds below through this helper.</summary>
    internal sealed record LadderRung(int Version, string ProbeParameter, string? DropSql = null);

    /// <summary>
    /// Every rung from V140 up, oldest first. Each parameter sits later in the probe's signature than the previous entry's, which
    /// is how the probe's newest-first arms read it: with entry k's parameter and every earlier one present and every later one
    /// absent, the probe answers rung k.
    /// </summary>
    internal static readonly IReadOnlyList<LadderRung> Ladder =
    [
        new(140, "hasCheckpointsTimed"),
        new(141, "hasCollectionCaveats"),
        new(142, "hasIndexObjectStatsServerTimeIndex"),
        new(143, "hasQueryStoreIntervalLatest"),
        new(144, "hasRawChunkIntervalRungHistory"),
        new(145, "hasQueryStoreIntervalWide"),
        new(146, "hasManagedConfVerdicts"),
        new(147, "hasComposeTimeoutSixty"),
        new(148, "hasReadLatency"),
        new(149, "hasHotLivenessTouch"),
        new(150, "hasCollectionLogWatermarkAndJobHistoryIndexes"),
        new(151, "hasAgGroupId"),
        new(152, "hasCaggGroupIndexDrop"),
        new(153, "hasIntervalFirstExecIndexes"),
        new(154, "hasIntervalWideFirstExecIndex"),
        new(155, "hasQueryStoreIntervalEnd"),
        new(156, "hasCheckpointLongestSync"),
        new(157, "hasMuteRuleServerId"),
        new(158, "hasInstallId"),
        new(159, "hasInstallIdTableOid"),
        new(160, "hasCollectorRunAt"),
        new(161, "hasQueryStoreTopDaily"),
        new(162, "hasSlowReads"),
        new(163, "hasStoreStatementHistory"),
        new(164, "hasQueryStatsHourLedger"),
        new(165, "hasPasswordKey"),
        new(166, "hasPagerDutyAutoResolve"),
        new(167, "hasLegacyPinCandidates"),
        new(168, "hasPlanRegressionDaily"),
        new(169, "hasAwsRole"),
        new(170, "hasPgIoStatsHourly"),
        new(171, "hasQueryStoreIntervalWidePartitioned"),
        new(172, "hasQueryStoreIntervalLatestPartitioned"),
        new(173, "hasQueryStoreComposeStamp"),
        new(174, "hasServiceVersion"),
        new(175, "hasPlanRegressionDailyRowcount"),
        new(176, "hasProcedureStatsTotals"),

        // /// V177 (#5641): the Runnable Queue settings columns of config.config_alert_settings, the probe's newest sentinel
        // until V178 (#5684) went on top of it. Safe on a store that never had them.
        new(177, "hasRunnableQueueSettings",
            "ALTER TABLE IF EXISTS config.config_alert_settings"
            + " DROP COLUMN IF EXISTS runnable_queue_enabled,"
            + " DROP COLUMN IF EXISTS runnable_queue_warning_per_scheduler,"
            + " DROP COLUMN IF EXISTS runnable_queue_critical_per_scheduler"),

        // /// The removal of a store rung's probe sentinel, written ONCE for the live tests that rewind a store below that rung
        // and then read the probed schema version (ViewerDataService.MapProbedSchemaVersion). The probe maps the NEWEST sentinel
        // present, so a rewind that only deletes the version rows and the older rungs' objects still reads as the newest rung
        // while the newest rung's sentinel is on disk (#5641: V177's three config.config_alert_settings columns made
        // CaggGroupIndexDropLiveTests read 177 on a store rewound to V150/V151). A new rung that adds a probe sentinel adds its
        // removal here, and both rewind chains (CaggGroupIndexDropLiveTests and QueryStoreIntervalPartitionRungTests) call it.
        // </summary> internal static class RungRewindArtifacts { <summary> V178 (#5684): the per-batch Query Store watermark
        // table, the probe's newest sentinel until V179 (#5640) went on top, and the coverage table that goes with it. Safe on a
        // store that never had them.
        new(178, "hasQueryStoreWatermarks",
            "DROP TABLE IF EXISTS collect.query_store_watermarks, collect.query_store_watermarks_coverage"),

        // /// V179 (#5640): the Client Timeouts settings columns of config.config_alert_settings, the probe's newest sentinel
        // until V180 (#5654) went on top of it. Safe on a store that never had them.
        new(179, "hasClientTimeoutSettings",
            "ALTER TABLE IF EXISTS config.config_alert_settings"
            + " DROP COLUMN IF EXISTS client_timeouts_enabled,"
            + " DROP COLUMN IF EXISTS client_timeouts_warning_per_minute,"
            + " DROP COLUMN IF EXISTS client_timeouts_critical_per_minute"),

        // /// V180 (#5654): the registry of the services that raise alerts through the store, the probe's newest sentinel, and
        // its two dependants (the pending queue and the firing state). Safe on a store that never had them.
        new(180, "hasExternalAlerts",
            "DROP TABLE IF EXISTS config.external_alert_state, config.external_alert_pending, config.external_alert_source"),

        // /// V181 (#5750): the route scope columns of config.config_notification_routes, the probe's newest sentinel. The two
        // CHECK constraints name the columns, so dropping the columns drops them. Safe on a store that never had them.
        new(181, "hasNotificationRouteScope",
            "ALTER TABLE IF EXISTS config.config_notification_routes"
            + " DROP COLUMN IF EXISTS scope_server_id,"
            + " DROP COLUMN IF EXISTS scope_tag_id"),

        // /// V182 (#5506): the window band, in-flight and connection-wait columns of collect.read_latency (the band is the
        // probe's newest sentinel) and the two slow-read columns. Dropping the band drops the key that names it, so the V181 key
        // is put back. Safe on a store that never had them.
        new(182, "hasReadLatencyCapacityFields",
            @"
ALTER TABLE IF EXISTS collect.read_latency
    DROP COLUMN IF EXISTS window_band,
    DROP COLUMN IF EXISTS max_in_flight,
    DROP COLUMN IF EXISTS wait_total_ms;
DO $$
BEGIN
    IF to_regclass('collect.read_latency') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'collect.read_latency'::regclass AND contype = 'p') THEN
        ALTER TABLE collect.read_latency ADD PRIMARY KEY (metric_time, surface, route, outcome);
    END IF;
END $$;
ALTER TABLE IF EXISTS collect.slow_reads
    DROP COLUMN IF EXISTS in_flight,
    DROP COLUMN IF EXISTS wait_ms;"),

        // /// V183 (#5793): the read caller column of collect.slow_reads and the distinct caller count of collect.read_latency.
        // Safe on a store that never had them.
        new(183, "hasReadCaller",
            @"
ALTER TABLE IF EXISTS collect.read_latency
    DROP COLUMN IF EXISTS distinct_callers;
ALTER TABLE IF EXISTS collect.slow_reads
    DROP COLUMN IF EXISTS caller;"),

        // /// V184 (#5632): the investigation tables (the probe's V184 sentinel is the request table) and the agent columns of
        // config.config_notification_routes. Safe on a store that never had them.
        new(184, "hasInvestigations",
            @"
DROP TABLE IF EXISTS config.investigation_reports;
DROP TABLE IF EXISTS config.investigation_request_alerts;
DROP TABLE IF EXISTS config.investigation_requests;
DROP TABLE IF EXISTS config.investigation_journal;
DROP TABLE IF EXISTS config.investigation_notes;
DROP TABLE IF EXISTS config.agent_environment_profiles;
ALTER TABLE IF EXISTS config.config_notification_routes
    DROP CONSTRAINT IF EXISTS ck_config_notification_routes_agent,
    DROP COLUMN IF EXISTS agent_level,
    DROP COLUMN IF EXISTS agent_max_per_hour,
    DROP COLUMN IF EXISTS agent_max_per_day,
    DROP COLUMN IF EXISTS agent_lease_minutes,
    DROP COLUMN IF EXISTS agent_expiry_minutes;"),

        // /// V185 (#5812): the partial-day columns of collect.plan_regression_daily_built, built_through (the probe's V185
        // sentinel) and partial_seq. Safe on a store that never had them. A whole build made under V185 keeps built_seq =
        // late_seq and reads as a whole-day build, as every day built before V185 does. A partial build already has built_seq
        // NULL, so V184 reads it as not built, and its V184 builder rebuilds it whole.
        new(185, "hasPlanRegressionPartialDays",
            @"
ALTER TABLE IF EXISTS collect.plan_regression_daily_built
    DROP COLUMN IF EXISTS built_through,
    DROP COLUMN IF EXISTS partial_seq;"),

        // /// V186 (#5810): collect.collection_log_hourly (the probe's V186 sentinel), with its index. Safe on a store that
        // never had it. Nothing before V186 reads it.
        new(186, "hasCollectionLogHourly",
            "DROP TABLE IF EXISTS collect.collection_log_hourly;"),

        // /// V187 (#5825): the four per-channel resolution flags, the route override and the alert log's three resolution
        // columns (resolution_outcome is the probe's newest sentinel). Safe on a store that never had them.
        new(187, "hasAlertResolutionDelivery",
            @"
ALTER TABLE IF EXISTS config.config_notification
    DROP COLUMN IF EXISTS slack_send_resolutions,
    DROP COLUMN IF EXISTS teams_send_resolutions,
    DROP COLUMN IF EXISTS generic_send_resolutions,
    DROP COLUMN IF EXISTS email_send_resolutions;
ALTER TABLE IF EXISTS config.config_notification_routes DROP COLUMN IF EXISTS send_resolutions;
ALTER TABLE IF EXISTS config.config_alert_log
    DROP COLUMN IF EXISTS resolution_sent_at,
    DROP COLUMN IF EXISTS resolution_channels,
    DROP COLUMN IF EXISTS resolution_outcome;"),

        // /// V188 (#5821): the alert values on config.investigation_request_alerts, context_json (the probe's newest sentinel)
        // and the other four. Safe on a store that never had them or has no investigation tables. A brief composed below V188
        // reads every alert value from the history row, as it did before.
        new(188, "hasInvestigationAlertFacts",
            @"
ALTER TABLE IF EXISTS config.investigation_request_alerts
    DROP COLUMN IF EXISTS current_value,
    DROP COLUMN IF EXISTS threshold_value,
    DROP COLUMN IF EXISTS muted,
    DROP COLUMN IF EXISTS detail_text,
    DROP COLUMN IF EXISTS context_json;"),

        // /// V189 (#5867): collect.query_store_compose_stamp_failures (the probe's newest sentinel), the compose builder's
        // failure record. Safe on a store that never had it. Nothing before V189 reads it.
        new(189, "hasComposeStampFailures",
            "DROP TABLE IF EXISTS collect.query_store_compose_stamp_failures;"),

        // /// V190 (#5857): size_estimated on collect.store_metrics (the probe's newest sentinel). Safe on a store that never
        // had it. A store below V190 sizes every row exactly, as before.
        new(190, "hasStoreMetricsSizeEstimated",
            @"
ALTER TABLE IF EXISTS collect.store_metrics
    DROP COLUMN IF EXISTS size_estimated;"),

        // /// V191 (#5909): the Level 2 store. Drops the feedback table (the probe's newest sentinel), the threshold overrides,
        // the request's practice_of and the agent-actions switch, and puts the two level CHECKs back to their V184 shape (route
        // 0 to 1, request 1), so a re-run of V191 does everything again. Safe on a store that never had any of it.
        new(191, "hasAgentLevel2Store",
            @"
DROP TABLE IF EXISTS config.investigation_feedback;
DROP TABLE IF EXISTS config.config_threshold_overrides;
ALTER TABLE IF EXISTS config.investigation_requests
    DROP COLUMN IF EXISTS practice_of;
ALTER TABLE IF EXISTS config.config_service
    DROP COLUMN IF EXISTS agent_actions_enabled;
DO $do$
BEGIN
    IF to_regclass('config.config_notification_routes') IS NOT NULL
       AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_config_notification_routes_agent'
                   AND conrelid = 'config.config_notification_routes'::regclass) THEN
        ALTER TABLE config.config_notification_routes DROP CONSTRAINT ck_config_notification_routes_agent;
        ALTER TABLE config.config_notification_routes
            ADD CONSTRAINT ck_config_notification_routes_agent CHECK (
                (agent_level IS NULL OR agent_level IN (0, 1))
                AND agent_max_per_hour BETWEEN 1 AND 60
                AND agent_max_per_day BETWEEN 1 AND 500
                AND agent_lease_minutes BETWEEN 5 AND 120
                AND agent_expiry_minutes BETWEEN 15 AND 1440);
    END IF;
    IF to_regclass('config.investigation_requests') IS NOT NULL
       AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_investigation_requests_autonomy'
                   AND conrelid = 'config.investigation_requests'::regclass) THEN
        ALTER TABLE config.investigation_requests DROP CONSTRAINT ck_investigation_requests_autonomy;
        ALTER TABLE config.investigation_requests ADD CONSTRAINT ck_investigation_requests_autonomy CHECK (autonomy_level = 1);
    END IF;
END $do$;"),
        // /// V192 (#5977): the query-tag store, the probe's newest sentinel until V193 (#5997) went on top of it. Drops the tables, the trigger function and the cascade
        // targets with them, so a re-run of V192 does everything again. Safe on a store that never had any of it.
        new(192, "hasQueryTagsStore",
            "DROP TABLE IF EXISTS config.query_tag_map, config.query_tag_state, config.query_tag_rules,"
            + " config.query_tag_assignments, config.query_tags, collect.query_tag_sightings;"
            + " DROP FUNCTION IF EXISTS config.query_tag_bump_rules_version()"),

        // /// V193 (#5997): the Phase C store. Drops the evidence table (the probe's sentinel until V194 (#5985) went on top of it), the two signature tables and
        // every column the rung added, so a re-run of V193 does everything again. Safe on a store that never had any of it.
        new(193, "hasPhaseCStore",
            @"
DROP TABLE IF EXISTS config.investigation_evidence;
DROP TABLE IF EXISTS config.investigation_signature_days;
DROP TABLE IF EXISTS config.investigation_signature_merges;
ALTER TABLE IF EXISTS config.investigation_journal
    DROP COLUMN IF EXISTS event_id;
ALTER TABLE IF EXISTS config.investigation_requests
    DROP COLUMN IF EXISTS max_severity, DROP COLUMN IF EXISTS evidence_state, DROP COLUMN IF EXISTS follow_up_of,
    DROP COLUMN IF EXISTS follow_up_at, DROP COLUMN IF EXISTS follow_up_condition;
ALTER TABLE IF EXISTS config.investigation_reports
    DROP COLUMN IF EXISTS family_of, DROP COLUMN IF EXISTS queries_run;
ALTER TABLE IF EXISTS config.config_notification
    DROP COLUMN IF EXISTS signing_key, DROP COLUMN IF EXISTS previous_signing_key, DROP COLUMN IF EXISTS previous_signing_key_until;
ALTER TABLE IF EXISTS config.config_notification_routes
    DROP COLUMN IF EXISTS signing_key, DROP COLUMN IF EXISTS previous_signing_key, DROP COLUMN IF EXISTS previous_signing_key_until,
    DROP COLUMN IF EXISTS fallback_route_id;
ALTER TABLE IF EXISTS collect.procedure_stats
    DROP COLUMN IF EXISTS module_modify_date;"),

        // /// V194 (#5985): the query-tag sightings key gains the database. The new unique index is the probe's newest sentinel. The
        // rewind drops it and puts the V192 index back (guarded on the table), so a re-run of V194 does everything again. Safe on a
        // store that never had either.
        new(194, "hasQueryTagSightingsDatabaseKey",
            @"
DROP INDEX IF EXISTS collect.ux_query_tag_sightings_key_db;
DO $do$
BEGIN
    IF to_regclass('collect.query_tag_sightings') IS NOT NULL THEN
        CREATE UNIQUE INDEX IF NOT EXISTS ux_query_tag_sightings_key
            ON collect.query_tag_sightings (server_id, key_kind, key, COALESCE(application_name, ''), COALESCE(login_name, ''), COALESCE(host_name, ''));
    END IF;
END $do$;"),

        // /// V195 (#5632): the forced-plans collector table. The table is the probe's newest sentinel (V194's index was until this rung). The
        // rewind drops it, so a re-run of V195 does everything again. Safe on a store that never had it.
        new(195, "hasQueryStoreForcedPlans",
            "DROP TABLE IF EXISTS collect.query_store_forced_plans;"),
    ];

    /// <summary>
    /// Removes every probe sentinel above <paramref name="rungVersion"/>, newest first, so a store rewound to that rung reads as
    /// that rung rather than as the newest sentinel left on disk. Each removal is safe on a store that never had the objects.
    /// </summary>
    internal static async Task DropSentinelsAboveAsync(NpgsqlConnection connection, int rungVersion, CancellationToken cancellationToken)
    {
        foreach (var rung in Ladder.Where(r => r.Version > rungVersion && r.DropSql is not null).OrderByDescending(r => r.Version))
        {
            await using var command = new NpgsqlCommand(rung.DropSql, connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}
