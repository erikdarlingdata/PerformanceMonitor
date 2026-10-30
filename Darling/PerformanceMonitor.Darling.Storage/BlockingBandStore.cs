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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one read of the blocking health band's four tiers (#6927, V228) from the store's singleton settings row. It lives in Storage because
/// every Darling reader of the band needs the same values: the fleet cards and <c>get_fleet_overview</c>, the fleet sweep, the Daily Health
/// Calendar and <c>get_daily_health</c> in the service, and the Overview tab, the calendar and the fleet grid in the viewer. Values come back
/// RAW and <see cref="BlockingBandThresholds"/> clamps on read. A store with no row yet bands on today's constants, which is what such a
/// store would seed.
/// </summary>
public static class BlockingBandStore
{
    /// <summary>The singleton row's four tiers. No parameters: this is the current configuration, not history.</summary>
    public const string ReadSql = @"
SELECT blocking_warn_per_hour, blocking_critical_per_hour, blocking_warn_wait_seconds, blocking_critical_wait_seconds
FROM config_alert_settings
WHERE id = 1";

    /// <summary>Reads the tiers, or <see cref="BlockingBandThresholds.Default"/> when the row is absent.</summary>
    public static async Task<BlockingBandThresholds> ReadAsync(
        NpgsqlDataSource postgres, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(ReadSql);
        command.CommandTimeout = commandTimeoutSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return new BlockingBandThresholds(
                reader.GetDouble(0), reader.GetDouble(1), reader.GetDouble(2), reader.GetDouble(3));
        }

        return BlockingBandThresholds.Default;
    }
}
