/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Darling.Tests;

/// <summary>
/// The removal of a store rung's probe sentinel, written ONCE for the live tests that rewind a store below that rung
/// and then read the probed schema version (<c>ViewerDataService.MapProbedSchemaVersion</c>). The probe maps the
/// NEWEST sentinel present, so a rewind that only deletes the version rows and the older rungs' objects still reads
/// as the newest rung while the newest rung's sentinel is on disk (#5641: V177's three
/// <c>config.config_alert_settings</c> columns made <c>CaggGroupIndexDropLiveTests</c> read 177 on a store rewound to
/// V150/V151). A new rung that adds a probe sentinel adds its removal here, and both rewind chains
/// (<c>CaggGroupIndexDropLiveTests</c> and <c>QueryStoreIntervalPartitionRungTests</c>) call it.
/// </summary>
internal static class RungRewindArtifacts
{
    /// <summary>
    /// V178 (#5684): the per-batch Query Store watermark table, the probe's newest sentinel until V179 (#5640) went on top, and the coverage table that goes
    /// with it. Safe on a store that never had them.
    /// </summary>
    internal static async Task DropV178SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "DROP TABLE IF EXISTS collect.query_store_watermarks, collect.query_store_watermarks_coverage",
            connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V177 (#5641): the Runnable Queue settings columns of <c>config.config_alert_settings</c>, the probe's newest
    /// sentinel until V178 (#5684) went on top of it. Safe on a store that never had them.
    /// </summary>
    internal static async Task DropV177SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "ALTER TABLE IF EXISTS config.config_alert_settings"
            + " DROP COLUMN IF EXISTS runnable_queue_enabled,"
            + " DROP COLUMN IF EXISTS runnable_queue_warning_per_scheduler,"
            + " DROP COLUMN IF EXISTS runnable_queue_critical_per_scheduler",
            connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V179 (#5640): the Client Timeouts settings columns of <c>config.config_alert_settings</c>, the probe's newest
    /// sentinel until V180 (#5654) went on top of it. Safe on a store that never had them.
    /// </summary>
    internal static async Task DropV179SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "ALTER TABLE IF EXISTS config.config_alert_settings"
            + " DROP COLUMN IF EXISTS client_timeouts_enabled,"
            + " DROP COLUMN IF EXISTS client_timeouts_warning_per_minute,"
            + " DROP COLUMN IF EXISTS client_timeouts_critical_per_minute",
            connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V180 (#5654): the registry of the services that raise alerts through the store, the probe's newest sentinel,
    /// and its two dependants (the pending queue and the firing state). Safe on a store that never had them.
    /// </summary>
    internal static async Task DropV180SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "DROP TABLE IF EXISTS config.external_alert_state, config.external_alert_pending, config.external_alert_source",
            connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V181 (#5750): the route scope columns of <c>config.config_notification_routes</c>, the probe's newest sentinel. The two
    /// CHECK constraints name the columns, so dropping the columns drops them. Safe on a store that never had them.
    /// </summary>
    internal static async Task DropV181SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "ALTER TABLE IF EXISTS config.config_notification_routes"
            + " DROP COLUMN IF EXISTS scope_server_id,"
            + " DROP COLUMN IF EXISTS scope_tag_id",
            connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V182 (#5506): the window band, in-flight and connection-wait columns of <c>collect.read_latency</c> (the band is the probe's
    /// newest sentinel) and the two slow-read columns. Dropping the band drops the key that names it, so the V181 key is put back.
    /// Safe on a store that never had them.
    /// </summary>
    internal static async Task DropV182SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(@"
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
    DROP COLUMN IF EXISTS wait_ms;", connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V183 (#5793): the read caller column of <c>collect.slow_reads</c> and the distinct caller count of
    /// <c>collect.read_latency</c>. Safe on a store that never had them.
    /// </summary>
    internal static async Task DropV183SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(@"
ALTER TABLE IF EXISTS collect.read_latency
    DROP COLUMN IF EXISTS distinct_callers;
ALTER TABLE IF EXISTS collect.slow_reads
    DROP COLUMN IF EXISTS caller;", connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V184 (#5632): the investigation tables (the probe's V184 sentinel is the request table) and the agent columns of
    /// <c>config.config_notification_routes</c>. Safe on a store that never had them.
    /// </summary>
    internal static async Task DropV184SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(@"
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
    DROP COLUMN IF EXISTS agent_expiry_minutes;", connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V185 (#5812): the partial-day columns of <c>collect.plan_regression_daily_built</c>, <c>built_through</c> (the probe's
    /// V185 sentinel) and <c>partial_seq</c>. Safe on a store that never had them. A whole build made under V185 keeps
    /// <c>built_seq = late_seq</c> and reads as a whole-day build, as every day built before V185 does. A partial build already
    /// has <c>built_seq</c> NULL, so V184 reads it as not built, and its V184 builder rebuilds it whole.
    /// </summary>
    internal static async Task DropV185SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(@"
ALTER TABLE IF EXISTS collect.plan_regression_daily_built
    DROP COLUMN IF EXISTS built_through,
    DROP COLUMN IF EXISTS partial_seq;", connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V186 (#5810): <c>collect.collection_log_hourly</c> (the probe's V186 sentinel), with its index. Safe on a store that
    /// never had it. Nothing before V186 reads it.
    /// </summary>
    internal static async Task DropV186SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("DROP TABLE IF EXISTS collect.collection_log_hourly;", connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V187 (#5825): the four per-channel resolution flags, the route override and the alert log's three resolution columns
    /// (<c>resolution_outcome</c> is the probe's newest sentinel). Safe on a store that never had them.
    /// </summary>
    internal static async Task DropV187SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(@"
ALTER TABLE IF EXISTS config.config_notification
    DROP COLUMN IF EXISTS slack_send_resolutions,
    DROP COLUMN IF EXISTS teams_send_resolutions,
    DROP COLUMN IF EXISTS generic_send_resolutions,
    DROP COLUMN IF EXISTS email_send_resolutions;
ALTER TABLE IF EXISTS config.config_notification_routes DROP COLUMN IF EXISTS send_resolutions;
ALTER TABLE IF EXISTS config.config_alert_log
    DROP COLUMN IF EXISTS resolution_sent_at,
    DROP COLUMN IF EXISTS resolution_channels,
    DROP COLUMN IF EXISTS resolution_outcome;", connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V188 (#5821): the alert values on <c>config.investigation_request_alerts</c>, <c>context_json</c> (the probe's newest
    /// sentinel) and the other four. Safe on a store that never had them or has no investigation tables. A brief composed
    /// below V188 reads every alert value from the history row, as it did before.
    /// </summary>
    internal static async Task DropV188SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(@"
ALTER TABLE IF EXISTS config.investigation_request_alerts
    DROP COLUMN IF EXISTS current_value,
    DROP COLUMN IF EXISTS threshold_value,
    DROP COLUMN IF EXISTS muted,
    DROP COLUMN IF EXISTS detail_text,
    DROP COLUMN IF EXISTS context_json;", connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// V189 (#5867): <c>collect.query_store_compose_stamp_failures</c> (the probe's newest sentinel), the compose builder's
    /// failure record. Safe on a store that never had it. Nothing before V189 reads it.
    /// </summary>
    internal static async Task DropV189SentinelAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("DROP TABLE IF EXISTS collect.query_store_compose_stamp_failures;", connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
