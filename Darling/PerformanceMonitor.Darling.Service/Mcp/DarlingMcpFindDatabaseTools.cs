/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6074: <c>find_database</c>, which monitored server holds a database. Tickets and traces name the tenant database, not the
/// server, and agents kept a hand-written map from one to the other. The answer comes from the store's own database lists:
/// <c>database_states</c>, written by the <see cref="PerformanceMonitor.Collectors.DatabaseStateCollector"/> (the
/// <c>database_states</c> collector, every minute by default on a SQL Server target that is not Azure SQL Database). For each server the
/// NEWEST snapshot is the list; a database is on the server when that list names it. The same list decides the dropped-database
/// scope (<see cref="PerformanceMonitor.Darling.Storage.CurrentDatabaseScope"/>), so this read and those agree. Lite's twin is
/// <c>McpDiscoveryTools.FindDatabase</c>.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpFindDatabaseTools
{
    /// <summary>How far back a server's newest database list is looked for. A server with no list inside it is not searched.</summary>
    public static readonly TimeSpan LookBack = TimeSpan.FromDays(7);

    /// <summary>A list newer than this is current: the same six hours the dropped-database scope trusts a newest snapshot for.</summary>
    public static readonly TimeSpan CurrentHorizon = TimeSpan.FromHours(6);

    /// <summary>One row per server that has a database list in the look-back: the newest list's time and the stored spelling of
    /// the database when that list names it. $1 the name, $2 the look-back floor (naive UTC). The lateral MAX reads
    /// <c>idx_database_states_time (server_id, collection_time)</c> once per server.</summary>
    public const string FindDatabaseSql = @"
SELECT s.server_id,
       s.server_name,
       s.display_name,
       s.is_enabled,
       n.t,
       (SELECT MIN(d.database_name)
        FROM database_states d
        WHERE d.server_id = s.server_id
        AND   d.collection_time = n.t
        AND   lower(d.database_name) = lower($1)) AS matched_name
FROM servers s
CROSS JOIN LATERAL (SELECT MAX(collection_time) AS t
                    FROM database_states
                    WHERE server_id = s.server_id
                    AND   collection_time > $2) n
WHERE n.t IS NOT NULL
ORDER BY s.server_name";

    [McpServerTool(Name = "find_database"), Description("Finds which monitored server holds a database, by exact name (case-insensitive), from each server's newest database list. Tickets and traces name the tenant database, not the server. One row per matching server: registry name, display name, is_monitored (false = disabled), newest_snapshot_at (when the store last saw that list) and snapshot_current (false = older than 6 hours, so the match may be stale). empty = no newest list names it; a dropped, renamed or not-yet-collected database looks the same. Servers with no list in 7 days are not searched. not_collected = every server is an Azure SQL Database, which writes no list.")]
    public static async Task<string> FindDatabase(
        NpgsqlDataSource postgres,
        [Description("Database name to find. Exact match, case-insensitive.")] string database_name,
        CancellationToken cancellationToken = default)
    {
        var requested = (database_name ?? string.Empty).Trim();
        if (requested.Length == 0)
        {
            return McpHelpers.Refusal("database_name", "database_name is required: the name of the database to find.");
        }

        try
        {
            var now = DateTime.UtcNow;
            var rows = new List<(int ServerId, string ServerName, string? DisplayName, bool Enabled, DateTime Newest, string? Matched)>();
            await using (var command = postgres.CreateCommand(FindDatabaseSql))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                DarlingMcpReadParameters.AddText(command, requested);
                DarlingMcpReadParameters.AddTimestamp(command, now - LookBack);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    rows.Add((reader.GetInt32(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetBoolean(3),
                        DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc), reader.IsDBNull(5) ? null : reader.GetString(5)));
                }
            }

            var matches = rows.Where(r => r.Matched is not null).ToList();
            if (matches.Count == 0)
            {
                /* #6178: no server has a list. When every registered server runs an engine that never writes one (Azure SQL
                   Database), that is not_collected, not a true negative. One server that could have had the list keeps the empty answer. */
                var fleetNotCollected = rows.Count == 0
                    ? await DarlingEngineCapability.FleetNotCollectedStatusAsync(postgres, "database_states", cancellationToken)
                    : null;
                return fleetNotCollected ?? McpHelpers.Status(
                    "empty",
                    rows.Count == 0
                        ? $"No server has a database list in the last {(int)LookBack.TotalDays} days, so nothing was searched for '{requested}'. The database_states collector writes the list every minute by default on a SQL Server target (not on Azure SQL Database)."
                        : $"No monitored server's newest database list names '{requested}' (searched {rows.Count} server(s) that have a list in the last {(int)LookBack.TotalDays} days). A database that was dropped, renamed, or created since the last list looks the same; servers with no list in that time (Azure SQL Database targets, or a stopped collector) were not searched.");
            }

            return JsonSerializer.Serialize(new
            {
                database_name = requested,
                servers_searched = rows.Count,
                matches = matches.Select(r => new
                {
                    server_id = r.ServerId,
                    server_name = r.ServerName,
                    display_name = r.DisplayName,
                    database_name = r.Matched,
                    is_monitored = r.Enabled,
                    newest_snapshot_at = r.Newest.ToString("o"),
                    snapshot_current = now - r.Newest <= CurrentHorizon,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("find_database", ex);
        }
    }
}
