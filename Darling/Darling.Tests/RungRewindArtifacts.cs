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
    /// V177 (#5641): the Runnable Queue settings columns of <c>config.config_alert_settings</c>, the probe's newest
    /// sentinel. Safe on a store that never had them.
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
}
