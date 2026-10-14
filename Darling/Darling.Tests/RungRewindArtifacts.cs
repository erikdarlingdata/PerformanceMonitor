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
    /// <c>collect.read_latency</c> (the probe's newest sentinel). Safe on a store that never had them.
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
}
