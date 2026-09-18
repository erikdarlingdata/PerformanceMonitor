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
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The least-privilege read/write surface over <c>config.config_notification_routes</c> (#3598, V131) the
/// MCP tools use — the <c>mcp</c> role's view of the table, which is NOT the service's.
///
/// <para><b>Two readers, deliberately.</b> The service's <c>StoreConfigProvider</c> reads the routes with
/// their destinations on the owner connection, because the fan-out needs the URLs. This store never
/// selects a destination column: four of the five are bearer secrets carved from the <c>mcp</c> and
/// <c>viewer</c> roles' SELECT (<c>DarlingManagedRoles.ViewerRestrictedConfigTables</c>), and one denied
/// column fails the whole statement with 42501. What it reads instead is the GENERATED
/// <c>configured_channels</c> presence column, which is the one fact a settings read needs — "route 3 sets
/// Slack and PagerDuty" — and discloses no value.</para>
///
/// <para><b>Two writes, and no third.</b> <see cref="SetEnabledAsync"/> and <see cref="DeleteAsync"/> are
/// the writes that move no destination, which is the parent row's posture for a network token-holder (the
/// one <c>config_notification</c> write mcp holds is the cooldown column). Authoring a route — a URL of the
/// caller's choosing — is the Viewer's Settings grid, as the parent's channels are.</para>
/// </summary>
public sealed class PgNotificationRouteStore
{
    private readonly NpgsqlDataSource _dataSource;

    public PgNotificationRouteStore(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    }

    /// <summary>A route as a role denied the destination columns sees it: what it matches, which channels
    /// it sets (names only), the one non-secret destination, and its state.</summary>
    public sealed record RouteSummary(
        int RouteId,
        string MetricMatch,
        string? Family,
        IReadOnlyList<string> ConfiguredChannels,
        string EmailRecipients,
        bool Enabled,
        DateTime ModifiedAtUtc);

    /// <summary>The non-secret projection. Public-const so the ACL pin can assert every column named here is
    /// in the <c>mcp</c>/<c>viewer</c> carve's non-secret list — the read that would 42501 in production is
    /// the one a superuser-run test never notices.</summary>
    public const string SelectSummariesSql = @"
SELECT route_id, metric_match, configured_channels, smtp_recipients, enabled, modified_at
FROM config_notification_routes
ORDER BY route_id";

    /// <summary>The enabled-flag write: the flag and its stamp, nothing else, so the column-level grant is
    /// exactly these two.</summary>
    public const string SetEnabledSql = @"
UPDATE config_notification_routes
   SET enabled = $2,
       modified_at = (now() AT TIME ZONE 'UTC')
 WHERE route_id = $1";

    public const string DeleteSql = "DELETE FROM config_notification_routes WHERE route_id = $1";

    public async Task<IReadOnlyList<RouteSummary>> LoadSummariesAsync(CancellationToken cancellationToken = default)
    {
        var routes = new List<RouteSummary>();
        await using var command = _dataSource.CreateCommand(SelectSummariesSql);
        command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var match = reader.GetString(1);
            routes.Add(new RouteSummary(
                reader.GetInt32(0),
                match,
                AlertFamily.NormalizeFamily(match),
                reader.IsDBNull(2) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(2),
                reader.GetString(3),
                reader.GetBoolean(4),
                DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc)));
        }

        return routes;
    }

    /// <summary>Sets the flag; returns the number of rows updated (0 = no such route).</summary>
    public async Task<int> SetEnabledAsync(int routeId, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(SetEnabledSql);
        command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Deletes the route; returns the number of rows deleted (0 = no such route).</summary>
    public async Task<int> DeleteAsync(int routeId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(DeleteSql);
        command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
