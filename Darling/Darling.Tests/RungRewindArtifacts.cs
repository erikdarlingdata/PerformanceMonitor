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
        // V239 (#7146): the fleet rollup of the Query Store compose stamp. The table is the probe's newest sentinel. The rewind drops it and
        // the ledger column, so a re-run of V239 creates them again. Safe on a store that never had them.
        new(239, "hasComposeFleet",
            @"DROP TABLE IF EXISTS collect.query_store_compose_fleet;
ALTER TABLE IF EXISTS collect.query_store_compose_stamp_built DROP COLUMN IF EXISTS fleet_built_seq;"),
        // V240 (#7169): the execution-weighted sums on the Query Store daily summary. The last column is the probe's newest sentinel. The
        // rewind drops all six, so a re-run of V240 adds them again. Safe on a store that never had them.
        new(240, "hasQueryStoreTopDailyWsum",
            @"ALTER TABLE IF EXISTS collect.query_store_top_daily
    DROP COLUMN IF EXISTS avg_duration_us_wsum, DROP COLUMN IF EXISTS avg_cpu_time_us_wsum, DROP COLUMN IF EXISTS avg_logical_io_reads_wsum,
    DROP COLUMN IF EXISTS avg_logical_io_writes_wsum, DROP COLUMN IF EXISTS avg_physical_io_reads_wsum, DROP COLUMN IF EXISTS avg_rowcount_wsum;"),
        // V241 (#7497): the blocking process's open transaction count on the blocked process reports. The last column is the probe's newest
        // sentinel. The rung re-expands the SELECT * view over it, so the rewind (V199's shape) drops the view, drops the column and creates
        // the view again over the older shape, then a re-run of V241 adds the column and re-expands the view. Safe on a store that never had it.
        new(241, "hasBlockingOpenTranCount",
            @"DROP VIEW IF EXISTS collect.v_blocked_process_reports;
ALTER TABLE IF EXISTS collect.blocked_process_reports DROP COLUMN IF EXISTS blocking_open_transaction_count;
CREATE OR REPLACE VIEW collect.v_blocked_process_reports AS SELECT * FROM collect.blocked_process_reports;"),

        // V242 (#7567): the service-own entry list on the statement text lookup. The last column is the probe's newest sentinel, so the
        // rewind drops it and a re-run of V242 adds it again. Safe on a store that never had it.
        new(242, "hasStatementTextOwnEntries",
            @"ALTER TABLE IF EXISTS collect.pg_statement_text DROP COLUMN IF EXISTS own_entries;"),
        // V243 (#7573): the session's program name on the waiting tasks. A plain nullable column with no view over it, so the rewind is the drop
        // alone, and the re-run of V243 adds it back. Safe on a store that never had it.
        new(243, "hasWaitingTasksProgramName",
            @"ALTER TABLE IF EXISTS collect.waiting_tasks DROP COLUMN IF EXISTS program_name;"),
        // V244 (#7590): the connect-failure state on the server registry. The last column is the probe's newest sentinel. The rewind drops the three
        // columns so a re-run of V244 adds them again. Safe on a store that never had them.
        new(244, "hasServerConnectDownState",
            @"ALTER TABLE IF EXISTS collect.servers DROP COLUMN IF EXISTS connect_down_since, DROP COLUMN IF EXISTS last_connect_error, DROP COLUMN IF EXISTS next_connect_attempt;"),
        // V245 (#7611): the three Database Protection thresholds on the alert settings. Plain NOT NULL DEFAULT columns, so the rewind drops all three
        // (the probe's sentinel is the CHECKDB one) and the re-run of V245 adds them back with their defaults. Safe on a store that never had them.
        new(245, "hasProtectionThresholds",
            @"ALTER TABLE IF EXISTS config.config_alert_settings DROP COLUMN IF EXISTS backup_overdue_threshold_hours, DROP COLUMN IF EXISTS log_backup_overdue_threshold_minutes, DROP COLUMN IF EXISTS checkdb_overdue_threshold_hours;"),
        // V246 (#7609): the (query_hash, bucket DESC) index on the corrected hourly Query Store rollup's materialization. The rewind drops it by name,
        // found by catalog (it lives in the materialization's schema), and a no-op without it; the re-run of V246 builds it again.
        new(246, "hasQueryStoreHashIndex",
            @"DO $$ DECLARE v regclass; BEGIN SELECT c.oid::regclass INTO v FROM pg_class c WHERE c.relkind = 'i' AND c.relname = 'ix_qs_corrected_hourly_hash_bucket'; IF v IS NOT NULL THEN EXECUTE format('DROP INDEX %s', v); END IF; END $$;"),
        // V247 (#7639): the per-server list of databases that get Extended Events sessions on the server registry. The column is the probe's newest
        // sentinel, so the rewind drops it and the re-run of V247 adds it back empty. Safe on a store that never had it.
        new(247, "hasXeSessionDatabases",
            @"ALTER TABLE IF EXISTS config.config_monitored_servers DROP COLUMN IF EXISTS xe_session_databases;"),
        // V248 (#7730): the key trigger also recognizes the old statements, and the state row is deleted so the backfill runs again. The rewind puts
        // V219's marker-only function back (the probe's sentinel is the digest test in the function's source), so a re-run of V248 replaces it
        // again. The state row is left as it is: the re-run deletes it.
        new(248, "hasSelfKeysOldStatements", PerformanceMonitor.Darling.Storage.QueryStoreSelfKeys.MarkerOnlyTriggerSql),
        // V249 (#7731): the database name on the statement stats. A plain nullable column with no view over it, so the rewind is the drop alone, and the
        // re-run of V249 adds it back. Safe on a store that never had it.
        new(249, "hasStatementStatsDatabaseName",
            @"ALTER TABLE IF EXISTS collect.pg_statement_stats DROP COLUMN IF EXISTS database_name;"),
        // V250 (#7691): the memory node on the memory pressure events. The column is the probe's newest sentinel, so the rewind drops the view,
        // drops the column and creates the view again over the older shape (V199's shape); the re-run of V250 adds the column and re-expands the
        // view. Safe on a store that never had it.
        new(250, "hasMemoryPressureNode",
            @"DROP VIEW IF EXISTS collect.v_memory_pressure_events;
ALTER TABLE IF EXISTS collect.memory_pressure_events DROP COLUMN IF EXISTS memory_node_id;
CREATE OR REPLACE VIEW collect.v_memory_pressure_events AS SELECT * FROM collect.memory_pressure_events;"),
        // V251 (#7762): the compose rollup's row trigger marks from the hour of the writer's clock. The rewind puts V173's function and triggers back (the
        // probe's sentinel is the one-hour subtraction in the function's source), so a re-run of V251 replaces them again. V173's text is idempotent.
        new(251, "hasComposeStampLateMarkOffsetZero", PerformanceMonitor.Darling.Storage.QueryStoreComposeStamp.CreateSql),
        // V252 (#7656): the Slack bot's route channel and its stamp with the stamp's trigger, the sealed tokens on the notification settings row
        // and the request thread table. The probe's sentinel is the thread table, so the rewind drops it, the trigger and its function, and the
        // four columns, and a re-run of V252 adds them back. Safe on a store that never had them.
        new(252, "hasSlackBotThreads",
            @"DROP TABLE IF EXISTS config.investigation_request_threads;
DROP TRIGGER IF EXISTS trg_notification_routes_slack_bot_channel_stamp ON config.config_notification_routes;
DROP FUNCTION IF EXISTS config.notification_routes_stamp_slack_bot_channel();
ALTER TABLE IF EXISTS config.config_notification_routes DROP COLUMN IF EXISTS slack_bot_channel;
ALTER TABLE IF EXISTS config.config_notification_routes DROP COLUMN IF EXISTS slack_bot_channel_set_at;
ALTER TABLE IF EXISTS config.config_notification DROP COLUMN IF EXISTS slack_bot_token;
ALTER TABLE IF EXISTS config.config_notification DROP COLUMN IF EXISTS slack_app_token;"),
        // V253 (#7658): the Slack card table. The probe's sentinel is the table, so the rewind drops it and a re-run of V253 makes it again. Safe on a
        // store that never had it.
        new(253, "hasSlackCards",
            @"DROP TABLE IF EXISTS config.investigation_slack_posts;"),
        // V254 (#7659): the chat requests. The probe's sentinel is the message table, so the rewind drops it, the asker table, the thread reverse index
        // and the link column (with its index), and a re-run of V254 makes them again. Safe on a store that never had them.
        new(254, "hasChatRequests",
            @"DROP TABLE IF EXISTS config.investigation_person_messages;
DROP TABLE IF EXISTS config.investigation_request_askers;
DROP INDEX IF EXISTS config.ix_investigation_request_threads_channel_ts;
ALTER TABLE IF EXISTS config.investigation_requests DROP COLUMN IF EXISTS chat_thread_of;"),
        // V255 (#8106): the cadence wait baseline supply. The probe's sentinel is the relation itself, a continuous aggregate or the plain
        // fallback view, so the rewind drops whichever shape stands (the product's own discriminating drop), and a re-run of V255 or the
        // ensure sweep creates it again. Safe on a store that never had it.
        new(255, "hasWaitStatsCadenceBaseline", PerformanceMonitor.Darling.Storage.TimescaleSupport.DropRetiredBaselineRelationSql(PerformanceMonitor.Darling.Storage.TimescaleSupport.WaitStatsCadenceBaselineView)),
        // V256 (#7643): the schema tracking tables. The probe's sentinel is the object table, so the rewind drops all seven (the four child tables
        // reference it, so they go first) and a re-run of V256 makes them again. Safe on a store that never had them.
        new(256, "hasSchemaTracking",
            @"DROP TABLE IF EXISTS collect.schema_triggers;
DROP TABLE IF EXISTS collect.schema_constraints;
DROP TABLE IF EXISTS collect.schema_indexes;
DROP TABLE IF EXISTS collect.schema_columns;
DROP TABLE IF EXISTS collect.schema_objects;
DROP TABLE IF EXISTS collect.schema_capture_state;
DROP TABLE IF EXISTS collect.schema_capture_passes;"),
        // V257 (#8464): the status-leading collection_log index. The probe's sentinel is the index itself, so the rewind drops it and a re-run
        // of V257 makes it again. Safe on a store that never had it.
        new(257, "hasCollectionLogStatusTimeIndex", "DROP INDEX IF EXISTS collect.idx_collection_log_status_time;"),
        // V258 (#8567): the oversized-plan backlog's dropped-row index. The probe's sentinel is the index itself, so the rewind drops it and a
        // re-run of V258 makes it again. Safe on a store that never had it.
        new(258, "hasOversizedPlanBacklogDroppedIndex", "DROP INDEX IF EXISTS collect.idx_oversized_plan_backlog_dropped;"),
        // V259 (#8549): the daily wait totals. The probe's sentinel is the relation itself, a continuous aggregate or the plain view, so the
        // rewind drops whichever shape stands (the product's own discriminating drop), and a re-run of V259 or the ensure sweep makes it
        // again. Safe on a store that never had it.
        new(259, "hasWaitStatsDaily", PerformanceMonitor.Darling.Storage.TimescaleSupport.DropRetiredBaselineRelationSql(PerformanceMonitor.Darling.Storage.TimescaleSupport.WaitStatsDailyView)),
        // V260 (#8570): the oversized-plan backlog's two median indexes. The probe's sentinel is the first of them, so the rewind drops both and
        // a re-run of V260 makes them again. Safe on a store that never had them.
        new(260, "hasOversizedPlanBacklogMedianIndexes", "DROP INDEX IF EXISTS collect.idx_oversized_plan_backlog_server_bytes; DROP INDEX IF EXISTS collect.idx_oversized_plan_backlog_collector_bytes;"),
        new(261, "hasWebAccessGroups", """
            DROP TRIGGER IF EXISTS web_access_tag_deleted ON config.server_tags;
            DROP TABLE IF EXISTS config.web_access_group_tags, config.web_access_group_events, config.web_access_groups CASCADE;
            DROP FUNCTION IF EXISTS config.manage_web_access_group(text,uuid,bigint,text,text,integer[],uuid,text,boolean,text);
            DROP FUNCTION IF EXISTS config.activate_web_access_group(uuid,bigint,uuid,text);
            DROP FUNCTION IF EXISTS config.web_access_snapshot(uuid);
            DROP FUNCTION IF EXISTS config.web_access_tag_deleted();
            DROP FUNCTION IF EXISTS config.web_access_owner_guard();
            DROP FUNCTION IF EXISTS config.harden_web_access_groups();
            """),
        new(262, "hasAdvancedWebAccess", """
            DROP FUNCTION IF EXISTS config.set_web_access_advanced(boolean);
            DROP FUNCTION IF EXISTS config.harden_web_access_advanced();
            DROP TABLE IF EXISTS config.web_access_advanced;
            """),
        // V263 (#8549): the oldest-run rollup under the Job History note. The probe's sentinel is the relation itself, so the rewind drops whichever shape stands and a re-run of V263 or the ensure sweep makes it again. Safe on a store that never had it.
        new(263, "hasJobHistoryOldestDaily", PerformanceMonitor.Darling.Storage.TimescaleSupport.DropRetiredBaselineRelationSql(PerformanceMonitor.Darling.Storage.TimescaleSupport.JobHistoryOldestDailyView)),
        // V264 (#8691): the per-hour drop counts of the oversized-plan backlog. A new table; the rewind drops it and a re-run of V264 makes it again.
        new(264, "hasOversizedPlanBacklogDropCounts", "DROP TABLE IF EXISTS collect.oversized_plan_backlog_drops;"),
        // V265 (#8549): the hourly summary of job_history's categories and its coverage table. Two new tables; the rewind drops both and a re-run of V265 makes them again.
        new(265, "hasJobHistoryCategoryHourly", "DROP TABLE IF EXISTS collect.job_history_category_hourly; DROP TABLE IF EXISTS collect.job_history_category_coverage;"),
        // V266 (#8716): fillfactor 80 on collect.query_tag_sightings. The rewind resets it and a re-run of V266 sets it again.
        new(266, "hasQueryTagSightingsFillfactor", "ALTER TABLE collect.query_tag_sightings RESET (fillfactor);"),
        // V267 (#8570): the (server_id, day) index on collect.plan_regression_daily. The rewind drops it and a re-run of V267 builds it again.
        new(267, "hasPlanRegressionDailyServerDayIndex", "DROP INDEX IF EXISTS collect.ix_plan_regression_daily_server_day;"),
        // V268 (#8784): the answering instance on index_object_stats and the module execution summary. The probe's sentinel is the column and
        // the build table together, so the rewind drops the view, drops the column, creates the view again over the older shape and drops both
        // summary tables; a re-run of V268 adds them all back. Safe on a store that never had them.
        new(268, "hasObjectUsage",
            @"DROP VIEW IF EXISTS collect.v_index_object_stats;
ALTER TABLE IF EXISTS collect.index_object_stats DROP COLUMN IF EXISTS is_memory_optimized;
ALTER TABLE IF EXISTS collect.index_object_stats DROP COLUMN IF EXISTS answering_server_name;
CREATE OR REPLACE VIEW collect.v_index_object_stats AS SELECT * FROM collect.index_object_stats;
DROP TABLE IF EXISTS collect.object_executions_daily;
DROP TABLE IF EXISTS collect.object_executions_daily_built;
DROP TABLE IF EXISTS collect.object_executions_daily_databases;
DROP TABLE IF EXISTS collect.object_executions_daily_retention;
DROP TABLE IF EXISTS collect.object_inventory;"),
        // V269 (#8881): the tempdb log columns on collect.tempdb_stats. The sentinel is log_used_percent; the rewind drops the view, drops all
        // eleven columns and creates the view again over the older shape, and a re-run of V269 adds them back. Safe on a store that never had them.
        new(269, "hasTempDbLogSpace",
            @"DROP VIEW IF EXISTS collect.v_tempdb_stats;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS holder_log_bytes_reserved;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS holder_log_bytes_used;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS holder_begin_time;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS holder_program_name;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS holder_login_name;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS holder_session_id;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS log_truncation_holdup_reason;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS log_reuse_wait_desc;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS log_used_percent;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS log_used_mb;
ALTER TABLE IF EXISTS collect.tempdb_stats DROP COLUMN IF EXISTS log_total_mb;
ALTER TABLE IF EXISTS config.config_alert_settings DROP COLUMN IF EXISTS tempdb_log_space_threshold_percent;
ALTER TABLE IF EXISTS config.config_alert_settings DROP COLUMN IF EXISTS tempdb_log_space_enabled;
CREATE OR REPLACE VIEW collect.v_tempdb_stats AS SELECT * FROM collect.tempdb_stats;"),
        // V270 (#8882): tempdb's autogrow-all-files flag on collect.database_size_stats. The rewind drops the view, drops the column and creates the
        // view again over the older shape; a re-run of V270 adds the column back and re-expands the view. Safe on a store that never had it.
        new(270, "hasDatabaseSizeAutogrowAllFiles",
            @"DROP VIEW IF EXISTS collect.v_database_size_stats;
ALTER TABLE IF EXISTS collect.database_size_stats DROP COLUMN IF EXISTS is_autogrow_all_files;
CREATE OR REPLACE VIEW collect.v_database_size_stats AS SELECT * FROM collect.database_size_stats;"),
        // V271 (#8884): the Instance Restarted and Memory Dumps switches on the alert settings. Plain NOT NULL DEFAULT columns, so the rewind drops both
        // (the probe's sentinel is the Memory Dumps one) and the re-run of V271 adds them back on. Safe on a store that never had them.
        new(271, "hasInstanceEventAlertSwitches",
            @"ALTER TABLE IF EXISTS config.config_alert_settings DROP COLUMN IF EXISTS instance_restarted_enabled, DROP COLUMN IF EXISTS memory_dumps_enabled;"),
        // V272 (#8798): the column statistics store. Three new tables; the probe's sentinel is the state table. A re-run of V272 makes them again.
        new(272, "hasColumnStats",
            @"DROP TABLE IF EXISTS collect.column_stats;
DROP TABLE IF EXISTS collect.column_stats_state;
DROP TABLE IF EXISTS collect.column_stats_runs;"),
        // V273 (#8824): the hourly wait totals. The probe's sentinel is the relation itself, a continuous aggregate or the plain view, so the
        // rewind drops whichever shape stands (the product's own discriminating drop), and a re-run of V273 or the ensure sweep makes it
        // again. Safe on a store that never had it.
        new(273, "hasWaitStatsHourly", PerformanceMonitor.Darling.Storage.TimescaleSupport.DropRetiredBaselineRelationSql(PerformanceMonitor.Darling.Storage.TimescaleSupport.WaitStatsHourlyView)),
        // V274 (#8978): the store's trigram text index options on the service row. Plain nullable columns with no default, so the rewind drops both
        // (the probe's sentinel is the Query Store one) and the re-run of V274 adds them back as NULL. Safe on a store that never had them.
        new(274, "hasTrigramIndexStoreOptions",
            @"ALTER TABLE IF EXISTS config.config_service DROP COLUMN IF EXISTS query_text_trigram_index_enabled, DROP COLUMN IF EXISTS query_store_text_trigram_index_enabled;"),
        // V275 (#8737): the personal MCP token table. The rewind drops it (its triggers go with it); a re-run of V275 creates it again.
        new(275, "hasPersonalTokens", "DROP TABLE IF EXISTS config.mcp_personal_token;"),
        // V276 (#9000): the Store Growth alert's switch and two floors on the alert settings. Plain NOT NULL DEFAULT columns, so the rewind drops all three
        // (the probe's sentinel is the object floor one) and the re-run of V276 adds them back on their defaults. Safe on a store that never had them.
        new(276, "hasStoreGrowthAlertSettings",
            @"ALTER TABLE IF EXISTS config.config_alert_settings DROP COLUMN IF EXISTS self_store_growth_enabled, DROP COLUMN IF EXISTS self_store_growth_floor_gb, DROP COLUMN IF EXISTS self_store_growth_object_floor_gb;"),
        // V277 (#8967): the progress table of the attention_events copy, the probe's sentinel. The rewind drops it and the re-run of V277 makes it
        // again. The swap itself (a populated plain table renamed to _legacy beside an empty one) is not undone here: V277's re-run does nothing
        // once _legacy exists, and a rewound store keeps both tables, which an older build reads as just the new one. Safe on a store that never had it.
        new(277, "hasAttentionLegacyCopy", @"DROP TABLE IF EXISTS collect.attention_events_legacy_copy;"),
        // V278 (#8865): the ten error-log columns on collect.database_protection. The probe's sentinel is the
        // last one, replica_readable; the rewind drops all ten and a re-run of V278 adds them back. Safe on a store that never had them.
        new(278, "hasDatabaseProtectionErrorLog",
            @"ALTER TABLE IF EXISTS collect.database_protection
    DROP COLUMN IF EXISTS errorlog_state, DROP COLUMN IF EXISTS errorlog_message, DROP COLUMN IF EXISTS errorlog_grant,
    DROP COLUMN IF EXISTS errorlog_backfill_complete, DROP COLUMN IF EXISTS errorlog_skipped_archives, DROP COLUMN IF EXISTS errorlog_unparsed_lines,
    DROP COLUMN IF EXISTS errorlog_checkdb_time, DROP COLUMN IF EXISTS errorlog_checkdb_options, DROP COLUMN IF EXISTS errorlog_checkdb_errors,
    DROP COLUMN IF EXISTS replica_readable;"),
        // V279 (#8570): the aggregate compression order history, the probe's sentinel. The rewind drops it and the re-run of V279 makes it again
        // and re-states the order. The order itself is not undone: an older build reads the aggregate the same way in either order. Safe on a
        // store that never had it.
        new(279, "hasAggregateCompressionOrderHistory", @"DROP TABLE IF EXISTS collect.aggregate_compression_order_history;"),
        // V280 (#8570): the plan_correction recommendation index. The index is the probe's newest sentinel. The rewind drops it, so a re-run of V280
        // builds it again. Safe on a store that never had it.
        new(280, "hasPlanCorrectionServerDbQueryIndex", "DROP INDEX IF EXISTS collect.idx_plan_correction_server_db_query_time;"),
        // V281 (#9043): the Query Store text liveness table, its trigger on the text table and the trigger's function. The table is the probe's
        // newest sentinel. The rewind drops all three, so a re-run of V281 creates the table, fills it from the text rows and re-arms the trigger.
        // Safe on a store that never had them.
        new(281, "hasQueryStoreTextSeen", @"DROP TRIGGER IF EXISTS trg_query_store_text_seen_ins ON collect.query_store_text;
DROP FUNCTION IF EXISTS collect.query_store_text_seen_track();
DROP TABLE IF EXISTS collect.query_store_text_seen;"),
        // V282 (#9110): the job outcome daily rollup's three tables. The three are the probe's newest sentinel. The rewind drops all three, so a re-run of V282
        // creates them empty and the service builds the days again. Safe on a store that never had them.
        new(282, "hasJobOutcomeDaily", @"DROP TABLE IF EXISTS collect.job_outcome_daily;
DROP TABLE IF EXISTS collect.job_outcome_daily_days;
DROP TABLE IF EXISTS collect.job_outcome_daily_watermark;"),
        // V283 (#9111): the Query Store closed-day rollups' four tables. The four are the probe's newest sentinel. The rewind drops all four, so a re-run of
        // V283 creates them empty and the service builds the days again. Safe on a store that never had them.
        new(283, "hasQueryStoreClosedDayRollups", @"DROP TABLE IF EXISTS collect.query_store_grid_daily;
DROP TABLE IF EXISTS collect.query_store_slicer_hourly;
DROP TABLE IF EXISTS collect.query_store_closed_day_built;
DROP TABLE IF EXISTS collect.query_store_closed_day_tick;"),
        // V284 (#9149): the covering hash indexes on the two plan cache hourly rollups, and the covering one on the Query Store rollup. The interval one is the probe's
        // newest sentinel until V285 (#9148) went on top. The rewind drops the two plan cache indexes by name, found by catalog (they live in the materialization's schema), and a no-op without them.
        // The Query Store index keeps its V246 name, so it stays: the re-run of V284 finds it covering and builds the two plan cache ones again.
        new(284, "hasFleetSummaryCoveringIndexes",
            @"DO $$ DECLARE v regclass; BEGIN FOR v IN SELECT c.oid::regclass FROM pg_class c WHERE c.relkind = 'i' AND c.relname IN ('ix_query_stats_interval_hourly_hash_cov', 'ix_query_stats_hourly_hash_cov') LOOP EXECUTE format('DROP INDEX %s', v); END LOOP; END $$;"),
        // V285 (#9148): the Query Store clutter rollups' two tables and the built table's clutter_built_at. The two tables are the probe's newest
        // sentinel until V286 (#9147) went on top. The rewind drops them and the column, so a re-run of V285 creates them empty and the service builds the days again.
        new(285, "hasQueryStoreClutterDaily", @"DROP TABLE IF EXISTS collect.query_store_clutter_plan_daily;
DROP TABLE IF EXISTS collect.query_store_clutter_collections_daily;
DO $$ BEGIN IF to_regclass('collect.query_store_closed_day_built') IS NOT NULL THEN ALTER TABLE collect.query_store_closed_day_built DROP COLUMN IF EXISTS clutter_built_at; END IF; END $$;"),
        // V286 (#9147): the query_stats per-collection summary table, the probe's newest sentinel until V287 (#9164) went on top. The rewind drops it, so a re-run of V286 creates it
        // empty and the duration trend reads raw until it covers a window. Safe on a store that never had it.
        new(286, "hasQueryStatsCollectionSummary", @"DROP TABLE IF EXISTS collect.query_stats_collection;"),
        // V287 (#9164): the hourly host rollup's two tables. The pair is the probe's newest sentinel until V288 (#9151) went on top. The rewind drops both, so a re-run of V287
        // creates them empty and the service builds the hours again. Safe on a store that never had them.
        new(287, "hasQueryStatsHostHourly", @"DROP TABLE IF EXISTS collect.query_stats_host_hourly;
DROP TABLE IF EXISTS collect.query_stats_host_hourly_built;"),
        // V288 (#9151): idx_collection_log_watermark with its INCLUDE column. It is the probe's newest sentinel until V289 (#9173) went on top, found by the index's shape, not its name. The rewind puts back V150's narrow index under the same name,
        // so V150's own sentinel stays true and a re-run of V288 finds the narrow index and replaces it.
        new(288, "hasCollectionLogWatermarkCovering", @"DROP INDEX IF EXISTS collect.idx_collection_log_watermark;
CREATE INDEX idx_collection_log_watermark ON collect.collection_log (server_id, collector_name, collection_time DESC);"),
        // V289 (#9173): the hourly Query Store clutter rollups' three tables and the hour heartbeat column on the tick table. The three tables are the
        // probe's newest sentinel until V290 (#9193) went on top. The rewind drops them and the column, so a re-run of V289 creates them empty and the service builds the hours again.
        new(289, "hasQueryStoreClutterHourly", @"DROP TABLE IF EXISTS collect.query_store_clutter_plan_hourly;
DROP TABLE IF EXISTS collect.query_store_clutter_collections_hourly;
DROP TABLE IF EXISTS collect.query_store_clutter_hour_built;
DO $$ BEGIN IF to_regclass('collect.query_store_closed_day_tick') IS NOT NULL THEN ALTER TABLE collect.query_store_closed_day_tick DROP COLUMN IF EXISTS hour_ticked_at; END IF; END $$;"),
        // V290 (#9193): the query_stats summary's per-server floor table, the probe's newest sentinel. The rewind drops it, so a re-run of V290 creates it empty
        // and a server has no floor until a batch finds a hole. Safe on a store that never had it.
        new(290, "hasQueryStatsCollectionFloor", @"DROP TABLE IF EXISTS collect.query_stats_collection_floor;"),
        // V291 (#9215): the covering (server_id, bucket DESC) index on the corrected hourly Query Store rollup, the probe's newest sentinel until V292 (#9223) went on top. The rewind drops it by name,
        // found by catalog (it lives in the materialization's schema), and a no-op without it. The plain (server_id, bucket) index it replaced is not put back: a re-run of V291 builds the covering one again.
        new(291, "hasQueryStoreServerCoveringIndex",
            @"DO $$ DECLARE v regclass; BEGIN SELECT c.oid::regclass INTO v FROM pg_class c WHERE c.relkind = 'i' AND c.relname = 'ix_qs_corrected_hourly_server_bucket'; IF v IS NOT NULL THEN EXECUTE format('DROP INDEX %s', v); END IF; END $$;"),
        // V292 (#9223): the covering hash index on the corrected daily Query Store rollup. It is the probe's newest sentinel until V294 (#9284) went on top, found by the index's name. The rewind drops it by name, found by
        // catalog (it lives in the materialization's schema), so a re-run of V292 builds it again. Safe on a store that never had it.
        new(292, "hasQueryStoreCorrectedDailyHashIndex",
            @"DO $$ DECLARE v regclass; BEGIN SELECT c.oid::regclass INTO v FROM pg_class c WHERE c.relkind = 'i' AND c.relname = 'ix_qs_corrected_daily_hash_bucket'; IF v IS NOT NULL THEN EXECUTE format('DROP INDEX %s', v); END IF; END $$;"),
        // V293 (#9277): the partial counter-row index on collect.attention_events. It is the probe's newest sentinel until V294 (#9284) went on top, found by the index's name. The rewind drops it by name, so a re-run of V293
        // builds it again. Safe on a store that never had it.
        new(293, "hasAttentionCountIndex", @"DROP INDEX IF EXISTS collect.idx_attention_events_count_time;"),
        // V294 (#9284): the view-and-time index on the aggregate compression order history. It is the probe's newest sentinel until V295 (#9225) and V296 (#9280) went on top, found by the index's name. The rewind drops it, so a re-run of V294
        // makes it again and moves the daily Query Store rollup's order if it is compressed. The history table stays (V279's). Safe on a store that never had it.
        new(294, "hasAggregateCompressionOrderHistoryIndex",
            @"DROP INDEX IF EXISTS collect.ix_aggregate_compression_order_history_view;"),
        // V295 (#9225): the plan regression block tables, the probe's sentinel under V296's. The rewind drops both, so a re-run of V295 creates them empty and the builder
        // sums the blocks again from the daily rows. Safe on a store that never had them.
        new(295, "hasPlanRegressionBlock", @"DROP TABLE IF EXISTS collect.plan_regression_block; DROP TABLE IF EXISTS collect.plan_regression_block_built;"),
        // V296 (#9280): the hourly Query Store summary table and the ledger's top_built_seq column. The table's index is the probe's sentinel under V297's (#8784 part 2) and goes
        // with the table. The rewind drops both, so a re-run of V296 builds them again. Safe on a store that never had them.
        new(296, "hasQueryStoreTopHourly",
            @"DROP TABLE IF EXISTS collect.query_store_top_hourly;
ALTER TABLE IF EXISTS collect.query_store_compose_stamp_built DROP COLUMN IF EXISTS top_built_seq;"),
        // V297 (#8784 part 2): the dependency list and the per-day referenced-names summary. The pair object_dependencies and
        // object_references_daily_databases is the probe's newest sentinel. The rewind drops all four tables, so a re-run of V297 creates them
        // empty, the collector captures the list again and the service builds the days again. Safe on a store that never had them.
        new(297, "hasObjectReferences", @"DROP TABLE IF EXISTS collect.object_dependencies;
DROP TABLE IF EXISTS collect.object_references_daily;
DROP TABLE IF EXISTS collect.object_references_daily_built;
DROP TABLE IF EXISTS collect.object_references_daily_databases;"),
        // V298 (#9304): the payload dimension tables' stamp tables, their insert triggers on the dimensions and the triggers' functions. The pair of stamp tables is the
        // probe's newest sentinel. The rewind drops all six and puts back the two last_seen indexes the rung dropped, so a re-run of V298 creates the tables, fills them from
        // the dimension rows, re-arms the triggers and drops the indexes again. Safe on a store that never had them.
        new(298, "hasPayloadDimensionSeen", @"DROP TRIGGER IF EXISTS trg_query_text_dim_seen_ins ON collect.query_text_dim;
DROP TRIGGER IF EXISTS trg_query_plan_dim_seen_ins ON collect.query_plan_dim;
DROP FUNCTION IF EXISTS collect.query_text_dim_seen_track();
DROP FUNCTION IF EXISTS collect.query_plan_dim_seen_track();
DROP TABLE IF EXISTS collect.query_text_dim_seen;
DROP TABLE IF EXISTS collect.query_plan_dim_seen;
CREATE INDEX IF NOT EXISTS idx_query_text_dim_last_seen ON collect.query_text_dim (last_seen);
CREATE INDEX IF NOT EXISTS idx_query_plan_dim_last_seen ON collect.query_plan_dim (last_seen);"),
        // V299 (#9349): the object usage chain's three tables. The three together are the probe's newest sentinel. The rewind drops all three, so a re-run of V299 creates them
        // empty and the chain builder folds the snapshots again. Safe on a store that never had them.
        new(299, "hasObjectUsageChain", @"DROP TABLE IF EXISTS collect.object_usage_snapshots;
DROP TABLE IF EXISTS collect.object_usage_deltas;
DROP TABLE IF EXISTS collect.object_usage_runs;"),
        // V300 (#9357): the procedure_stats hour ledger and its state table. The pair is the probe's newest sentinel. The rewind drops both, so a re-run of V300
        // creates them empty with a fresh counted_since. Safe on a store that never had them.
        new(300, "hasProcedureStatsHourLedger", @"DROP TABLE IF EXISTS collect.procedure_stats_hour_ledger_state;
DROP TABLE IF EXISTS collect.procedure_stats_hour_ledger;"),
        // V301 (#9374): the last measured relation sizes and the one-row time they were measured. The pair is the probe's newest sentinel. The rewind drops both, so a re-run of V301
        // creates them empty. Safe on a store that never had them.
        new(301, "hasStoreRelationSizes", @"DROP TABLE IF EXISTS collect.store_relation_sizes_latest;
DROP TABLE IF EXISTS collect.store_relation_sizes_measured;"),
        // V302 (#8570): the partial index of failed Agent job outcomes on collect.job_history. It is the probe's newest sentinel. The rewind drops it, so a re-run of V302
        // creates it again. Safe on a store that never had it.
        new(302, "hasJobHistoryFailedIndex", @"DROP INDEX IF EXISTS collect.idx_job_history_job_failed;"),
        // V303 (#9384): the clutter-day stamp table. It is the probe's newest sentinel. The rewind drops it, so a re-run of V303 creates it empty. Safe on a store
        // that never had it.
        new(303, "hasClutterDayBuilt", @"DROP TABLE IF EXISTS collect.query_store_clutter_day_built;"),
        // V304 (#9410): the write counters on the statement history and its baseline. The first history column is the probe's newest sentinel. The rewind drops all eight, so a re-run of V304
        // adds them again, empty. Safe on a store that never had them.
        new(304, "hasStatementHistoryWriteCounters", @"ALTER TABLE collect.store_statement_history DROP COLUMN IF EXISTS delta_shared_blks_dirtied, DROP COLUMN IF EXISTS delta_shared_blks_written, DROP COLUMN IF EXISTS delta_wal_bytes, DROP COLUMN IF EXISTS delta_wal_fpi;
ALTER TABLE config.store_statement_baseline DROP COLUMN IF EXISTS shared_blks_dirtied, DROP COLUMN IF EXISTS shared_blks_written, DROP COLUMN IF EXISTS wal_bytes, DROP COLUMN IF EXISTS wal_fpi;"),
        // V305 (#9419): the WAL columns on collect.store_metrics. wal_stats_reset is the probe's newest sentinel. The rewind drops the eleven columns, so a re-run of V305
        // adds them again. Safe on a store that never had them.
        new(305, "hasStoreWalFigures", @"ALTER TABLE collect.store_metrics DROP COLUMN IF EXISTS wal_records, DROP COLUMN IF EXISTS wal_fpi, DROP COLUMN IF EXISTS wal_bytes, DROP COLUMN IF EXISTS wal_buffers_full, DROP COLUMN IF EXISTS wal_write_count, DROP COLUMN IF EXISTS wal_sync_count, DROP COLUMN IF EXISTS wal_write_ms, DROP COLUMN IF EXISTS wal_sync_ms, DROP COLUMN IF EXISTS wal_stats_reset, DROP COLUMN IF EXISTS wal_dir_bytes, DROP COLUMN IF EXISTS wal_dir_files;"),
        // V306 (#9411): the background pass step history table. It is the probe's newest sentinel. The rewind drops it, so a re-run of V306 creates it empty. Safe on a store
        // that never had it.
        new(306, "hasStorePassSteps", @"DROP TABLE IF EXISTS collect.store_pass_steps;"),
        // V307 (#9415): the service start history's two tables. The pair is the probe's newest sentinel. The rewind drops both, so a re-run of V307 creates them empty. Safe on a
        // store that never had them.
        new(307, "hasServiceStartHistory", @"DROP TABLE IF EXISTS collect.service_start_rungs;
DROP TABLE IF EXISTS collect.service_starts;"),
        // V308 (#9413): the per-table activity capture and its baseline. They are the probe's newest sentinel. The rewind drops both, so a re-run of V308 creates them empty.
        // Safe on a store that never had them.
        new(308, "hasStoreTableActivity", @"DROP TABLE IF EXISTS collect.store_table_activity;
DROP TABLE IF EXISTS collect.store_table_activity_baseline;"),
        // V309 (#9414): the store checkpoint rows. It is the probe's newest sentinel. The rewind drops it, so a re-run of V309 creates it empty. Safe on a store
        // that never had it.
        new(309, "hasStoreCheckpoints", @"DROP TABLE IF EXISTS collect.store_checkpoints;"),
        // V310 (#9452): the partial index of the oversized-plan backlog's pending rows. It is the probe's newest sentinel. The rewind drops it, so a re-run of V310 builds it again.
        // Safe on a store that never had it.
        new(310, "hasOversizedPlanBacklogPendingIndex", @"DROP INDEX IF EXISTS collect.idx_oversized_plan_backlog_pending;"),
        // V311 (#9438): the migration rung object table. It is the probe's newest sentinel. The rewind drops it, so a re-run of V311 creates it empty. Safe on a store that never
        // had it.
        new(311, "hasServiceStartRungObjects", @"DROP TABLE IF EXISTS collect.service_start_rung_objects;"),
        // V312 (#9432): the upgrade run table. The rewind drops it, so a re-run of V312 creates it again. Safe on a store that never had it.
        new(312, "hasUpgradeRuns", @"DROP TABLE IF EXISTS collect.upgrade_runs;"),
        // V313 (#9427): the store setting state and changes tables. The probe's newest sentinel is both. The rewind drops both, so a re-run of V313 creates them empty. Safe on a store
        // that never had them.
        new(313, "hasStoreSettingChanges", @"DROP TABLE IF EXISTS collect.store_setting_changes;
DROP TABLE IF EXISTS collect.store_setting_state;"),
        // V314 (#9437): the two store connection tables. collect.store_connection_slots is the probe's newest sentinel. The rewind drops both, so a re-run of V314 creates them
        // empty. Safe on a store that never had them.
        new(314, "hasStoreConnectionSamples", @"DROP TABLE IF EXISTS collect.store_connection_slots; DROP TABLE IF EXISTS collect.store_connection_samples;"),
        // V315 (#9434): the process start column and the unique index that is the read_latency bucket key. The index is the probe's newest sentinel. The rewind drops the column,
        // which drops the index with it, then puts the primary key back, so a re-run of V315 moves the key again. Safe on a store that never had them.
        new(315, "hasReadLatencyProcessStart", @"ALTER TABLE collect.read_latency DROP COLUMN IF EXISTS process_started_at;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'collect.read_latency'::regclass AND contype = 'p') THEN ALTER TABLE collect.read_latency ADD CONSTRAINT read_latency_pkey PRIMARY KEY (metric_time, surface, route, outcome, window_band); END IF; END $$;"),
        // V316 (#9439): collect.store_lock_waits, the probe's newest sentinel. The rewind drops the table, so a re-run of V316 creates it again, empty. Safe on a store that never had it.
        new(316, "hasStoreLockWaits", @"DROP TABLE IF EXISTS collect.store_lock_waits;"),
        // V317 (#9433): the reply size on the slow read record and the read latency histogram. The slow read column is the probe's newest sentinel. The rewind drops both columns, so a re-run of V317 adds them again, empty. Safe on a store that never had them.
        new(317, "hasReadReplyBytes", @"ALTER TABLE collect.slow_reads DROP COLUMN IF EXISTS reply_bytes;
ALTER TABLE collect.read_latency DROP COLUMN IF EXISTS reply_bytes_total;
ALTER TABLE collect.read_latency DROP COLUMN IF EXISTS reply_bytes_count;"),
        // V318 (#9485): the planned change windows table. It is the probe's newest sentinel. The rewind drops it, the partial index on the request link and the two
        // request columns, so a re-run of V318 makes them again. Safe on a store that never had them.
        new(318, "hasInvestigationChangeWindows", @"DROP TABLE IF EXISTS config.investigation_change_windows;
DROP INDEX IF EXISTS config.ix_investigation_requests_held_window;
ALTER TABLE IF EXISTS config.investigation_requests DROP COLUMN IF EXISTS held_window_id, DROP COLUMN IF EXISTS hold_released_at;"),
        // V319 (#9662): the three argument advanced web access setter. It is the probe's newest sentinel. The one argument setter V319 replaced calls the three argument one,
        // so the rewind puts the V262 one argument body back before it drops the three argument form, and a rewound store keeps a working setter. A re-run of V319 makes both again.
        // Safe on a store that never had it.
        new(319, "hasWebAccessAdvancedActor", """
            CREATE OR REPLACE FUNCTION config.set_web_access_advanced(p_enabled boolean) RETURNS void
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $fn$
            DECLARE prior boolean;
            BEGIN
                SELECT enabled INTO prior FROM config.web_access_advanced WHERE id=1 FOR UPDATE;
                IF NOT FOUND OR prior IS NOT DISTINCT FROM p_enabled THEN RETURN; END IF;
                UPDATE config.web_access_advanced SET enabled=p_enabled,session_epoch=gen_random_uuid() WHERE id=1;
                INSERT INTO config.web_access_group_events(group_id,event,actor,before_state,after_state)
                    VALUES('ffffffff-ffff-ffff-ffff-ffffffffffff',
                        CASE WHEN p_enabled THEN 'advanced_enabled' ELSE 'advanced_disabled' END,
                        session_user,jsonb_build_object('enabled',prior),jsonb_build_object('enabled',p_enabled));
            END $fn$;
            DROP FUNCTION IF EXISTS config.set_web_access_advanced(boolean,text,boolean);
            """),
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
