/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #6828: which servers report Always On as OFF, read for every server at once. The two Always On collectors run on a SQL Server
/// without Always On, find nothing and log HEALTHY, and <c>get_collection_health</c> lists them as <c>not_collected</c> there. The fleet
/// card and the Viewer count collectors from the same rows, so they need the same fact to count those two as not collected and not healthy.
/// The newest <c>server_properties</c> row per server decides, as the per-server read does (<c>is_hadr_enabled = false</c>). A server with no row,
/// or a NULL flag, makes no claim.
/// </summary>
public static class FleetAlwaysOnOff
{
    /// <summary>One row per server with a properties snapshot. <c>server_properties</c> is written once per service start, so the table is small.</summary>
    public const string Sql = @"
SELECT DISTINCT ON (server_id) server_id, is_hadr_enabled
FROM server_properties
ORDER BY server_id, collection_time DESC";

    /// <summary>The ids of the servers whose newest snapshot says Always On is off. A failed read answers none, as the per-server read does:
    /// with no evidence the log-driven bands stand.</summary>
    public static async Task<HashSet<int>> ReadAsync(NpgsqlDataSource postgres, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var off = new HashSet<int>();
        try
        {
            await using var command = postgres.CreateCommand(Sql);
            command.CommandTimeout = commandTimeoutSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(1) && !reader.GetBoolean(1))
                {
                    off.Add(reader.GetInt32(0));
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            off.Clear();
        }

        return off;
    }
}
