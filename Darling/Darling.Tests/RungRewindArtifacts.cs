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
    /// <summary>
    /// The statement the README's rollback list gives for V206 (#6227): every NULL enabled state becomes the value it inherits, which is
    /// the fleet-wide row's value for a server's row, else the collector's shipped default (off for long_query_completions, on for the
    /// rest). The subquery reads the rows as they were before the UPDATE, so a server's row takes a fleet row set to false. Written once
    /// so the rewind and the test that proves it run the same text.
    /// </summary>
    internal const string ResolveInheritedEnabledSql =
        "UPDATE config.config_collector_schedules AS s SET enabled = COALESCE((SELECT f.enabled FROM config.config_collector_schedules AS f " +
        "WHERE s.server_id IS NOT NULL AND f.server_id IS NULL AND lower(f.collector_name) = lower(s.collector_name) LIMIT 1), " +
        "lower(s.collector_name) <> 'long_query_completions') WHERE s.enabled IS NULL";

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

        // /// V196 (#5632): the per-server action switches. The table is the probe's newest sentinel. The rewind drops it and puts the two
        // level CHECKs back to their V191 shape (route 0 to 2, request 1 to 2), so a re-run of V196 does everything again. A row at level
        // 3 would refuse that, so the rewind lowers it first (a rewound store holds no Level 3 row it could have kept). Safe on a store
        // that never had any of it.
        new(196, "hasServerActionSwitches",
            @"
DROP TABLE IF EXISTS config.config_server_actions;
DO $do$
BEGIN
    IF to_regclass('config.config_notification_routes') IS NOT NULL
       AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_config_notification_routes_agent'
                   AND conrelid = 'config.config_notification_routes'::regclass) THEN
        ALTER TABLE config.config_notification_routes DROP CONSTRAINT ck_config_notification_routes_agent;
        UPDATE config.config_notification_routes SET agent_level = 2 WHERE agent_level = 3;
        ALTER TABLE config.config_notification_routes
            ADD CONSTRAINT ck_config_notification_routes_agent CHECK (
                (agent_level IS NULL OR agent_level IN (0, 1, 2))
                AND agent_max_per_hour BETWEEN 1 AND 60
                AND agent_max_per_day BETWEEN 1 AND 500
                AND agent_lease_minutes BETWEEN 5 AND 120
                AND agent_expiry_minutes BETWEEN 15 AND 1440);
    END IF;
    IF to_regclass('config.investigation_requests') IS NOT NULL
       AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_investigation_requests_autonomy'
                   AND conrelid = 'config.investigation_requests'::regclass) THEN
        ALTER TABLE config.investigation_requests DROP CONSTRAINT ck_investigation_requests_autonomy;
        UPDATE config.investigation_requests SET autonomy_level = 2 WHERE autonomy_level = 3;
        ALTER TABLE config.investigation_requests
            ADD CONSTRAINT ck_investigation_requests_autonomy CHECK (autonomy_level IN (1, 2));
    END IF;
END $do$;"),

        // /// V197 (#5632): the force-check link columns on the requests. The column is the probe's newest sentinel. The rewind drops the
        // index, the CHECK and both columns, so a re-run of V197 does everything again. Safe on a store that never had them.
        new(197, "hasForceChecks",
            @"
DROP INDEX IF EXISTS config.uq_investigation_requests_force_check;
ALTER TABLE IF EXISTS config.investigation_requests DROP CONSTRAINT IF EXISTS ck_investigation_requests_force_check;
ALTER TABLE IF EXISTS config.investigation_requests DROP COLUMN IF EXISTS force_check_of, DROP COLUMN IF EXISTS force_check_no;"),

        // /// V198 (#6066): the one-row pseudonym key table. The table is the probe's newest sentinel. The rewind drops it (its triggers go with
        // it), so a re-run of V198 makes a fresh one and seeds a new key. Safe on a store that never had it.
        new(198, "hasPseudonymKey",
            @"
DROP TABLE IF EXISTS config.pseudonym_key;"),

        // /// V199 (#5632): the head-blocker evidence columns on the blocking snapshots. The last column is the probe's newest sentinel. The
        // rewind drops all four (dropping a column on a compressed hypertable is allowed for a nullable, default-less one), so a re-run of V199 adds
        // them again. The view is re-expanded so it no longer names them. Safe on a store that never had them.
        new(199, "hasBlockingHeadEvidence",
            @"
DROP VIEW IF EXISTS collect.v_dmv_blocking_snapshots;
ALTER TABLE IF EXISTS collect.dmv_blocking_snapshots
    DROP COLUMN IF EXISTS blocking_login_time,
    DROP COLUMN IF EXISTS blocking_last_request_end_time,
    DROP COLUMN IF EXISTS blocking_open_transaction_count,
    DROP COLUMN IF EXISTS blocking_tran_log_bytes_used;
CREATE OR REPLACE VIEW collect.v_dmv_blocking_snapshots AS SELECT * FROM collect.dmv_blocking_snapshots;"),

        // /// V200 (#6098): the excluded-volumes list on the singleton alert settings row. The column is the probe's newest sentinel. The
        // rewind drops it, so a re-run of V200 adds it again (the row keeps every other knob). Safe on a store that never had it.
        new(200, "hasExcludedVolumes",
            @"
ALTER TABLE IF EXISTS config.config_alert_settings DROP COLUMN IF EXISTS excluded_volumes;"),

        // /// V201 (#6160): the variant and plan-guide columns on the forced-plan capture. The last column is the probe's newest sentinel. The
        // rewind drops all four, so a re-run of V201 adds them again. Safe on a store that never had them.
        new(201, "hasForcedPlanVariantsAndGuides",
            @"
ALTER TABLE IF EXISTS collect.query_store_forced_plans
    DROP COLUMN IF EXISTS variant_plan_ids,
    DROP COLUMN IF EXISTS plan_guide_name,
    DROP COLUMN IF EXISTS plan_guide_enabled,
    DROP COLUMN IF EXISTS plan_guide_scope;"),

        // /// V202 (#6159): the primary replica's name on the AG replica states. The column is the probe's newest sentinel. The rewind drops
        // it, so a re-run of V202 adds it again. Safe on a store that never had it.
        new(202, "hasAgPrimaryReplica",
            @"
ALTER TABLE IF EXISTS collect.ag_replica_states DROP COLUMN IF EXISTS primary_replica;"),

        // /// V203 (#6170): the hypothetical-index login pair on the server registry. The password column is the probe's newest sentinel. The
        // rewind drops both, so a re-run of V203 adds them again. Safe on a store that never had them.
        new(203, "hasHypotheticalIndexLogin",
            @"
ALTER TABLE IF EXISTS config.config_monitored_servers
    DROP COLUMN IF EXISTS hypothetical_index_username,
    DROP COLUMN IF EXISTS hypothetical_index_encrypted_password;"),

        // /// V204 (#6214): the folder, personal flag and creator on the custom views. The creator column is the probe's newest sentinel.
        // The rewind restores V31's one store-wide unique name and drops the three columns (their constraints and indexes go with them),
        // so a re-run of V204 builds them again. Safe on a store that never had them.
        new(204, "hasCustomViewFolders",
            @"
DROP INDEX IF EXISTS config.ux_custom_views_shared_name;
DROP INDEX IF EXISTS config.ux_custom_views_personal_name;
ALTER TABLE IF EXISTS config.custom_views
    DROP COLUMN IF EXISTS folder,
    DROP COLUMN IF EXISTS personal,
    DROP COLUMN IF EXISTS created_by;
DO $$
BEGIN
    IF to_regclass('config.custom_views') IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conrelid = 'config.custom_views'::regclass AND conname = 'custom_views_name_key')
    THEN
        ALTER TABLE config.custom_views ADD CONSTRAINT custom_views_name_key UNIQUE (name);
    END IF;
END $$;"),

        // /// V205 (#6235): the optional expiry on the custom alert rules. The column is the probe's newest sentinel. The rewind drops it,
        // so a re-run of V205 adds it again. Safe on a store that never had it.
        new(205, "hasCustomAlertRuleExpiry",
            @"
ALTER TABLE IF EXISTS config.custom_alert_rules
    DROP COLUMN IF EXISTS expires_at_utc;"),

        // /// V206 (#6227): the collector schedule rows may hold a NULL enabled state, and the table carries the name and value limits and
        // the clear-on-register trigger. The rewind gives every NULL the value it inherits (the statement the README's rollback list
        // gives), puts NOT NULL back, and removes the three CHECK constraints, the trigger and its function, so a re-run of V206 adds
        // them again and a test can seed the rows an older store held. Safe on a store that never had the table.
        new(206, "hasInheritableCollectorEnabled",
            @"
DO $$
BEGIN
    IF to_regclass('config.config_collector_schedules') IS NOT NULL THEN
        " + ResolveInheritedEnabledSql + @";
        ALTER TABLE config.config_collector_schedules ALTER COLUMN enabled SET NOT NULL;
        ALTER TABLE config.config_collector_schedules DROP CONSTRAINT IF EXISTS ck_config_collector_schedules_name;
        ALTER TABLE config.config_collector_schedules DROP CONSTRAINT IF EXISTS ck_config_collector_schedules_frequency_cap;
        ALTER TABLE config.config_collector_schedules DROP CONSTRAINT IF EXISTS ck_config_collector_schedules_retention_cap;
    END IF;
    IF to_regclass('config.config_monitored_servers') IS NOT NULL THEN
        DROP TRIGGER IF EXISTS trg_clear_unregistered_schedules ON config.config_monitored_servers;
    END IF;
END $$;
DROP FUNCTION IF EXISTS config.config_clear_unregistered_schedules();"),

        // /// V207 (#6234): the change watch tables and the request trigger column. The tables are the probe's newest sentinel. The rewind
        // drops the three watch tables (their attempts and plan_id columns go with them), the marker index on the forced-plan table and the
        // trigger column on the investigation requests, so a re-run of V207 builds them all again. Safe on a store that never had them.
        new(207, "hasChangeWatches",
            @"
DROP TABLE IF EXISTS config.change_watch_changes;
DROP TABLE IF EXISTS config.change_watch_marks;
DROP TABLE IF EXISTS config.change_watches;
DROP INDEX IF EXISTS collect.idx_query_store_forced_plans_markers;
ALTER TABLE IF EXISTS config.investigation_requests
    DROP COLUMN IF EXISTS trigger_kind;"),

        // /// V208 (#6231): the start time on the mute rules. The column is the probe's newest sentinel. The rewind drops it, so a re-run of V208
        // adds it again. Safe on a store that never had it.
        new(208, "hasMuteRuleStartsAt",
            @"
ALTER TABLE IF EXISTS config.config_mute_rules DROP COLUMN IF EXISTS starts_at_utc;"),

        // V209 (#6211): the Query Store fetch top-N setting. The column is the probe's newest sentinel. The rewind drops it, so a re-run of V209
        // adds it again at its default. Safe on a store that never had it.
        new(209, "hasQueryStoreFetchTopN",
            @"
ALTER TABLE IF EXISTS config.config_service DROP COLUMN IF EXISTS query_store_fetch_top_n;"),

        // /// V210 (#6281): the guard on the MCP tag writes. The trigger on the map is the probe's newest sentinel. The rewind drops both
        // triggers and the function, so a re-run of V210 builds them again. Safe on a store that never had them.
        new(210, "hasAgentRouteTagGuard",
            @"
DROP TRIGGER IF EXISTS trg_agent_route_tag_guard ON config.server_tag_map;
DROP TRIGGER IF EXISTS trg_agent_route_tag_guard ON config.server_tags;
DROP FUNCTION IF EXISTS config.agent_route_tag_guard();"),

        // V211 (#5736): the relative Client Timeouts bars' two multiples. The warning multiple column is the probe's newest sentinel. The rewind drops both,
        // so a re-run of V211 adds them again at their defaults. Safe on a store that never had them.
        new(211, "hasClientTimeoutsMultiples",
            @"
ALTER TABLE IF EXISTS config.config_alert_settings DROP COLUMN IF EXISTS client_timeouts_warning_multiple, DROP COLUMN IF EXISTS client_timeouts_critical_multiple;"),

        // V212 (#6285): the machine identity on the server registry. The column is the probe's newest sentinel. Safe on a store that never had it.
        new(212, "hasServerMachineIdentity",
            @"
ALTER TABLE IF EXISTS collect.servers DROP COLUMN IF EXISTS machine_identity;"),

        // V213 (#6254): the Query Store replica setting. The config column is the probe's newest sentinel. The rewind drops both new columns;
        // collect.v_collection_log freezes its column list at CREATE, so it is dropped first and made again over the table without read_source
        // (the shape V210 left), and a re-run of V213 appends the column to it again. Safe on a store that never had them.
        new(213, "hasQueryStoreReplica",
            @"
ALTER TABLE IF EXISTS config.config_monitored_servers DROP COLUMN IF EXISTS query_store_replica;
DROP VIEW IF EXISTS collect.v_collection_log;
ALTER TABLE IF EXISTS collect.collection_log DROP COLUMN IF EXISTS read_source;
DO $$ BEGIN IF to_regclass('collect.collection_log') IS NOT NULL THEN
  EXECUTE 'CREATE VIEW collect.v_collection_log AS SELECT * FROM collect.collection_log';
END IF; END $$;"),
        // V214 (#6334): the known-pattern entries. The table is the probe's newest sentinel. The rewind drops it (and its trigger with it), so a re-run
        // of V214 builds both again. Safe on a store that never had it.
        new(214, "hasKnownPatterns",
            @"
DROP TABLE IF EXISTS config.config_known_patterns;"),
        // V215: the agent-table indexes. The reports index is the probe's newest sentinel. The rewind drops all six, so a re-run of V215 builds them
        // again. Safe on a store that never had them.
        new(215, "hasAgentTableIndexes",
            @"
DROP INDEX IF EXISTS config.ix_investigation_requests_reported_condition, config.ix_investigation_requests_follow_up_of,
    config.ix_investigation_requests_follow_up_at, config.ix_investigation_requests_opened_at,
    config.ix_investigation_requests_practice_of, config.ix_investigation_reports_family_of;"),
        // V216 (#5736): the Client Timeouts switch's column default is TRUE, the probe's newest sentinel. The rewind sets the default back to
        // FALSE (V179's), so a re-run of V216 sets it again. The row's value is left as it is: V216 only ever moves FALSE to TRUE. Guarded, so
        // it is safe on a store whose V179 columns are already gone.
        new(216, "hasClientTimeoutsOnByDefault",
            @"
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_alert_settings') AND attname = 'client_timeouts_enabled' AND NOT attisdropped) THEN
        ALTER TABLE config.config_alert_settings ALTER COLUMN client_timeouts_enabled SET DEFAULT FALSE;
    END IF;
END $$;"),
        // V217 (#6581): the Query Store hints table. The table is the probe's newest sentinel. The rewind drops it (its indexes go with it), so a
        // re-run of V217 does everything again. Safe on a store that never had it.
        new(217, "hasQueryStoreHints",
            "DROP TABLE IF EXISTS collect.query_store_hints;"),
        // V218 (#6569): the module recompile and plan guide switches. The stored CHECK naming it is the probe's newest sentinel. The rewind puts V196's three-word CHECK
        // back (it deletes any recompile_module row first, which the old CHECK could not hold), so a re-run of V218 widens it again. Safe on a store that never had it.
        new(218, "hasServerActionRecompile",
            @"
DO $$ BEGIN IF to_regclass('config.config_server_actions') IS NOT NULL THEN
  DELETE FROM config.config_server_actions WHERE action IN ('recompile_module', 'create_plan_guide', 'drop_plan_guide');
  ALTER TABLE config.config_server_actions DROP CONSTRAINT IF EXISTS ck_config_server_actions_action;
  ALTER TABLE config.config_server_actions ADD CONSTRAINT ck_config_server_actions_action CHECK (action IN ('force_plan', 'evict_plan', 'kill_session'));
END IF; END $$;"),
        // V219 (#6750): the Query Store self-statement key tables and the two triggers on the text table that fill the key table. The state
        // table is the probe's newest sentinel. The rewind drops the triggers and their function first (a trigger left behind would write to a
        // dropped table on the next marked text), then both tables, so a re-run of V219 creates them again. Safe on a store that never had them.
        new(219, "hasQueryStoreSelfKeys",
            @"
DROP TRIGGER IF EXISTS trg_query_store_self_keys_ins ON collect.query_store_text;
DROP TRIGGER IF EXISTS trg_query_store_self_keys_upd ON collect.query_store_text;
DROP FUNCTION IF EXISTS collect.query_store_self_keys_track();
DROP TABLE IF EXISTS collect.query_store_self_keys_state, collect.query_store_self_keys;"),
        // V220 (#6798): the Job History statistics index on collect.job_history. The index is the probe's newest sentinel. The rewind drops
        // it, so a re-run of V220 builds it again. Safe on a store that never had it.
        new(220, "hasJobHistorySuccessIndex",
            "DROP INDEX IF EXISTS collect.idx_job_history_job_success;"),
        // V221 (#6800): the Query Store hash-hourly table, the day tables and the ledger's hash_built_seq column. The table is the probe's newest sentinel.
        // The rewind drops both, so a re-run of V221 builds them again. Safe on a store that never had them.
        new(221, "hasQueryStoreHashHourly",
            @"DROP TABLE IF EXISTS collect.query_store_hash_hourly, collect.query_store_hash_daily, collect.query_store_hash_daily_built, collect.query_store_hash_daily_failures;
ALTER TABLE collect.query_store_compose_stamp_built DROP COLUMN IF EXISTS hash_built_seq;"),
        // V222 (#6874): the Agent alerts table. The table is the probe's newest sentinel. The rewind drops it (its index goes with it), so a
        // re-run of V222 builds it again. Safe on a store that never had it.
        new(222, "hasAgentAlerts",
            @"DROP TABLE IF EXISTS collect.agent_alerts;
ALTER TABLE config.config_alert_settings DROP COLUMN IF EXISTS agent_alert_gaps_enabled;"),
        // V223 (#6873): the database protection table. The table is the probe's newest sentinel. The rewind drops it (its index goes with it), so a
        // re-run of V223 does everything again. Safe on a store that never had it.
        new(223, "hasDatabaseProtection",
            @"DROP TABLE IF EXISTS collect.database_protection;
ALTER TABLE config.config_alert_settings DROP COLUMN IF EXISTS backup_overdue_enabled, DROP COLUMN IF EXISTS log_backup_overdue_enabled, DROP COLUMN IF EXISTS checkdb_overdue_enabled;"),
        // V224 (#6875): the identity capacity table. The table is the probe's newest sentinel. The rewind drops it (its indexes go with it), so a
        // re-run of V224 does everything again. Safe on a store that never had it.
        new(224, "hasIdentityCapacity",
            @"DROP TABLE IF EXISTS collect.identity_capacity;
ALTER TABLE config.config_alert_settings
    DROP COLUMN IF EXISTS identity_capacity_enabled,
    DROP COLUMN IF EXISTS identity_capacity_warning_percent,
    DROP COLUMN IF EXISTS identity_capacity_critical_percent;"),
        // V225 (#6894): the AWS RDS fact on the server registry. The column is the probe's newest sentinel. The rewind drops it, so a re-run of
        // V225 does everything again. Safe on a store that never had it.
        new(225, "hasServerAwsRds",
            @"ALTER TABLE collect.servers DROP COLUMN IF EXISTS is_aws_rds;"),
        // V226 (#6893): the AWS RDS Web or Express fact on the server registry. The column is the probe's newest sentinel. The rewind drops it,
        // so a re-run of V226 does everything again. Safe on a store that never had it.
        new(226, "hasServerAwsRdsWebOrExpress",
            @"ALTER TABLE collect.servers DROP COLUMN IF EXISTS is_aws_rds_web_or_express;"),
        // V227 (#6926): the tag scope on the threshold overrides. The column is the probe's newest sentinel. The rewind removes the tag rows,
        // the two constraints and the tag index, puts server_id back to NOT NULL and drops the column, so a re-run of V227 does everything
        // again. Safe on a store that never had it.
        new(227, "hasThresholdOverrideTagScope",
            @"DELETE FROM config.config_threshold_overrides WHERE server_id IS NULL;
ALTER TABLE config.config_threshold_overrides DROP CONSTRAINT IF EXISTS ck_config_threshold_overrides_one_scope;
ALTER TABLE config.config_threshold_overrides DROP CONSTRAINT IF EXISTS ck_config_threshold_overrides_tag_no_database;
DROP INDEX IF EXISTS config.ux_config_threshold_overrides_tag;
ALTER TABLE config.config_threshold_overrides ALTER COLUMN server_id SET NOT NULL;
ALTER TABLE config.config_threshold_overrides DROP COLUMN IF EXISTS scope_tag_id;"),
        // V228 (#6927): the four blocking band columns on the alert settings row. The warn per hour column is the probe's newest sentinel. The
        // rewind drops all four, so a re-run of V228 adds them again. Safe on a store that never had them.
        new(228, "hasBlockingBands",
            @"ALTER TABLE config.config_alert_settings
    DROP COLUMN IF EXISTS blocking_warn_per_hour,
    DROP COLUMN IF EXISTS blocking_critical_per_hour,
    DROP COLUMN IF EXISTS blocking_warn_wait_seconds,
    DROP COLUMN IF EXISTS blocking_critical_wait_seconds;"),
        // V229 (#6928): the severity filter on the notification routes. The column is the probe's newest sentinel. The rewind drops the column
        // (its CHECK goes with it), so a re-run of V229 does everything again. Safe on a store that never had it.
        new(229, "hasRouteSeverities",
            @"ALTER TABLE config.config_notification_routes DROP COLUMN IF EXISTS severities;
ALTER TABLE IF EXISTS config.external_alert_state DROP COLUMN IF EXISTS fired_severities;
ALTER TABLE IF EXISTS config.investigation_request_alerts DROP COLUMN IF EXISTS severity;"),
        // V230 (#7028): the two snapshot wait tiers of the blocking band on the alert settings row. The warn tier column is the probe's newest
        // sentinel. The rewind drops both, so a re-run of V230 adds them again. Safe on a store that never had them.
        new(230, "hasBlockingSnapshotBands",
            @"ALTER TABLE config.config_alert_settings
    DROP COLUMN IF EXISTS blocking_warn_snapshot_wait_seconds,
    DROP COLUMN IF EXISTS blocking_critical_snapshot_wait_seconds;"),
        // V231 (#7040): the recurring schedule on the mute rules. The start time column is the probe's newest sentinel. The rewind drops all
        // four columns, so a re-run of V231 adds them again. Safe on a store that never had them.
        new(231, "hasMuteRecurrence",
            @"ALTER TABLE config.config_mute_rules
    DROP COLUMN IF EXISTS recur_days,
    DROP COLUMN IF EXISTS recur_start_time,
    DROP COLUMN IF EXISTS recur_end_time,
    DROP COLUMN IF EXISTS recur_time_zone;"),
        // V232 (#7041): the Daily Health Report columns on the alert settings row. The enabled column is the probe's newest sentinel. The rewind
        // drops all four, so a re-run of V232 adds them again. Safe on a store that never had them.
        new(232, "hasDailyReportSettings",
            @"ALTER TABLE config.config_alert_settings
    DROP COLUMN IF EXISTS daily_report_enabled,
    DROP COLUMN IF EXISTS daily_report_send_at,
    DROP COLUMN IF EXISTS daily_report_time_zone,
    DROP COLUMN IF EXISTS daily_report_title;"),
        // V233 (#6979): the report notice setting on the notification routes. The column is the probe's newest sentinel. The rewind drops the
        // column (its CHECK goes with it), so a re-run of V233 does everything again. Safe on a store that never had it.
        new(233, "hasReportNotice",
            @"ALTER TABLE config.config_notification_routes DROP COLUMN IF EXISTS report_notice;"),
        // V234 (#7076): the exact alert key on the alert log. The column is the probe's newest sentinel. The rewind drops it, so a re-run of
        // V234 adds it again. Safe on a store that never had it.
        new(234, "hasAlertLogKey",
            @"ALTER TABLE IF EXISTS config.config_alert_log DROP COLUMN IF EXISTS alert_key;"),
        // V235 (#7083): the blocking bars' column defaults are 3, 60 and 3, the probe's newest sentinel (the count bar's default). The rewind sets the
        // defaults back to V1's 1, V40's 0 and V122's 1, so a re-run of V235 sets them again. The row's values are left as they are. Guarded, so it is
        // safe on a store whose V122 column is already gone.
        new(235, "hasBlockingBarsOnWaitTime",
            @"
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_alert_settings') AND attname = 'pg_blocking_count_threshold' AND NOT attisdropped) THEN
        ALTER TABLE config.config_alert_settings
            ALTER COLUMN blocking_count_threshold SET DEFAULT 1,
            ALTER COLUMN blocking_wait_seconds_threshold SET DEFAULT 0,
            ALTER COLUMN pg_blocking_count_threshold SET DEFAULT 1;
    END IF;
END $$;"),
        // V236 (#7083): the action on the mute rules. The column is the probe's newest sentinel. The rewind drops it (its CHECK goes with it), so a
        // re-run of V236 adds it again. Safe on a store that never had it.
        new(236, "hasMuteRuleAction",
            @"ALTER TABLE IF EXISTS config.config_mute_rules DROP COLUMN IF EXISTS action;"),
        // V237 (#7107): the people, identity and grant tables. The grant table is the probe's newest sentinel. The rewind drops all three
        // (the two child tables first), so a re-run of V237 creates them again. Safe on a store that never had them.
        new(237, "hasPeopleGrants",
            @"DROP TABLE IF EXISTS config.person_grant;
DROP TABLE IF EXISTS config.person_identity;
DROP TABLE IF EXISTS config.person;"),
        // V238 (#7109): the action proposals table and the two route approval columns. The table is the probe's newest sentinel. The rewind
        // drops all three, so a re-run of V238 adds them again (and runs the backfill again, on a store whose column is gone). Safe on a store
        // that never had them.
        new(238, "hasActionProposals",
            @"DROP TABLE IF EXISTS config.agent_action_proposals;
ALTER TABLE config.config_notification_routes DROP COLUMN IF EXISTS action_approval, DROP COLUMN IF EXISTS approval_single_person;"),
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
