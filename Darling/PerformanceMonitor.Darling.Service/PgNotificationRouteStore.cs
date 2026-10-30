/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
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
        DateTime ModifiedAtUtc,
        int? ScopeServerId = null,
        int? ScopeTagId = null)
    {
        /// <summary>The scoped server's registry name, or null when the route is not server-scoped or the server no longer exists (#5750).</summary>
        public string? ScopeServerName { get; init; }

        /// <summary>The scope tag's name, or null when the route is not tag-scoped or the tag no longer exists (#5750).</summary>
        public string? ScopeTagName { get; init; }

        /// <summary>How many servers the scope covers right now: the one server, or the tag's whole subtree; null for a fleet-wide route and for a scoped route whose scope could not be read (scope_unreadable tells the two apart) (#5750).</summary>
        public int? ExpandedServerCount { get; init; }

        /// <summary>True when the scoped server or tag no longer exists: the route stays as written and matches nothing (#5750).</summary>
        public bool Orphaned { get; init; }

        /// <summary>The agent channel (V184, #5632): null inherits, 0 off (and stops the lookup), 1 investigate.</summary>
        public int? AgentLevel { get; init; }

        /// <summary>The route's hourly request limit (#5632).</summary>
        public int AgentMaxPerHour { get; init; } = NotificationRoute.DefaultAgentMaxPerHour;

        /// <summary>The route's daily request limit (#5632).</summary>
        public int AgentMaxPerDay { get; init; } = NotificationRoute.DefaultAgentMaxPerDay;

        /// <summary>The claim lease, in minutes (#5632).</summary>
        public int AgentLeaseMinutes { get; init; } = NotificationRoute.DefaultAgentLeaseMinutes;

        /// <summary>How long an unclaimed request stays open, in minutes (#5632).</summary>
        public int AgentExpiryMinutes { get; init; } = NotificationRoute.DefaultAgentExpiryMinutes;

        /// <summary>Whether the route's destinations get an alert's resolution (#5825): null follows each channel's setting,
        /// true or false overrides it.</summary>
        public bool? SendResolutions { get; init; }

        /// <summary>The route an expired investigation request goes to first (#6036, D8 of #5632 Phase C), or null for none.</summary>
        public int? FallbackRouteId { get; init; }

        /// <summary>The severity filter (V229, #6928): both, critical or warning.</summary>
        public string Severities { get; init; } = NotificationRouteKinds.SeveritiesBoth;

        /// <summary>The fallback route's name (its <c>metric_match</c>), or null when there is no fallback.</summary>
        public string? FallbackRouteName { get; init; }

        /// <summary>True when the route is scoped but its scope could not be read just now (the tag tables failed, for a tag-scoped route):
        /// it matches nothing until a read succeeds, and is not orphaned, since the tag may still exist. The loader's rule
        /// (<see cref="StoreConfigProvider.ExpandRoutes"/>), so the tool never shows such a route as an ordinary active one (#5750).</summary>
        public bool ScopeUnreadable { get; init; }
    }

    /// <summary>The non-secret projection. Public-const so the ACL pin can assert every column named here is
    /// in the <c>mcp</c>/<c>viewer</c> carve's non-secret list — the read that would 42501 in production is
    /// the one a superuser-run test never notices.</summary>
    public const string SelectSummariesSql = @"
SELECT route_id, metric_match, configured_channels, smtp_recipients, enabled, modified_at, scope_server_id, scope_tag_id,
       agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes, send_resolutions, fallback_route_id, severities
FROM config_notification_routes
ORDER BY route_id";

    /// <summary>The registry's server ids and names, for the scope labels of <see cref="LoadSummariesAsync"/> (#5750). Both columns are in the
    /// <c>mcp</c> role's non-secret carve of <c>config_monitored_servers</c>.</summary>
    public const string ScopeServersSql = "SELECT server_id, name FROM config_monitored_servers";

    /// <summary>The tag tree with names (#5750).</summary>
    public const string ScopeTagsSql = "SELECT id, name, parent_id FROM server_tags";

    /// <summary>The server-to-tag assignments (#5750).</summary>
    public const string ScopeTagMapSql = "SELECT server_id, tag_id FROM server_tag_map";

    /// <summary>The enabled-flag write: the flag and its stamp, nothing else, so the column-level grant is
    /// exactly these two.</summary>
    public const string SetEnabledSql = @"
UPDATE config_notification_routes
   SET enabled = $2,
       modified_at = (now() AT TIME ZONE 'UTC')
 WHERE route_id = $1
   AND (agent_level IS NULL OR (agent_level = 0 AND $2))";

    /// <summary>The fallback write (#6036): the id and the stamp, nothing else.</summary>
    public const string SetFallbackSql = @"
UPDATE config_notification_routes
   SET fallback_route_id = $2,
       modified_at = (now() AT TIME ZONE 'UTC')
 WHERE route_id = $1";

    /// <summary>The fallback column has no foreign key (V193 adds the column and a not-itself CHECK only), so a delete clears
    /// every reference to the deleted route first, in the same transaction (#6036).</summary>
    public const string ClearFallbackReferencesSql = @"
UPDATE config_notification_routes
   SET fallback_route_id = NULL,
       modified_at = (now() AT TIME ZONE 'UTC')
 WHERE fallback_route_id = $1";

    public const string DeleteSql = "DELETE FROM config_notification_routes WHERE route_id = $1 AND agent_level IS NULL";

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
                DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc),
                reader.IsDBNull(6) ? null : reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetInt32(7))
            {
                AgentLevel = reader.IsDBNull(8) ? null : reader.GetInt16(8),
                AgentMaxPerHour = reader.GetInt32(9),
                AgentMaxPerDay = reader.GetInt32(10),
                AgentLeaseMinutes = reader.GetInt32(11),
                AgentExpiryMinutes = reader.GetInt32(12),
                SendResolutions = reader.IsDBNull(13) ? null : reader.GetBoolean(13),
                FallbackRouteId = reader.IsDBNull(14) ? null : reader.GetInt32(14),
                Severities = NotificationRouteKinds.NormalizeSeverities(reader.GetString(15)),
            });
        }

        var named = routes.Select(r => r.FallbackRouteId is { } fid
            ? r with { FallbackRouteName = routes.FirstOrDefault(o => o.RouteId == fid)?.MetricMatch }
            : r).ToList();
        return named.Exists(r => r.ScopeServerId.HasValue || r.ScopeTagId.HasValue)
            ? await DescribeScopesAsync(named, cancellationToken)
            : named;
    }

    /// <summary>
    /// Names each scope and expands it with the shared <see cref="NotificationRouteScopeExpander"/>, the same code (and the same unreadable-tag rule) the service's loader
    /// uses, so the count and the orphan flag the tool reports are the ones delivery acts on (#5750). Only run when a route is scoped.
    /// </summary>
    private async Task<List<RouteSummary>> DescribeScopesAsync(List<RouteSummary> routes, CancellationToken cancellationToken)
    {
        var serverNames = new Dictionary<int, string>();
        var tagNames = new Dictionary<int, string>();
        var tagParents = new Dictionary<int, int?>();
        var serverTags = new Dictionary<int, HashSet<int>>();

        await using (var command = _dataSource.CreateCommand(ScopeServersSql))
        {
            command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                serverNames[reader.GetInt32(0)] = reader.GetString(1);
            }
        }

        /* The tag tables are a side read, as in the service's loader: a failure there narrows what a tag-scoped route matches and
           is reported, never an error for the whole tool, and a server-scoped route still expands from the server list. */
        var tagsReadable = true;
        try
        {
            await using (var command = _dataSource.CreateCommand(ScopeTagsSql))
            {
                command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    tagNames[reader.GetInt32(0)] = reader.GetString(1);
                    tagParents[reader.GetInt32(0)] = reader.IsDBNull(2) ? null : reader.GetInt32(2);
                }
            }

            await using (var command = _dataSource.CreateCommand(ScopeTagMapSql))
            {
                command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var serverId = reader.GetInt32(0);
                    if (!serverTags.TryGetValue(serverId, out var tags))
                    {
                        serverTags[serverId] = tags = new HashSet<int>();
                    }

                    tags.Add(reader.GetInt32(1));
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            tagsReadable = false;
            tagNames.Clear();
            tagParents.Clear();
            serverTags.Clear();
        }

        var scopeOnly = routes
            .Select(r => new NotificationRoute(r.RouteId, r.MetricMatch, "", "", "", "", "", r.Enabled) { ScopeServerId = r.ScopeServerId, ScopeTagId = r.ScopeTagId })
            .ToList();
        var expanded = StoreConfigProvider.ExpandRoutes(
            scopeOnly,
            new StoreConfigProvider.RouteScopeInputs(
                serverNames.Keys.ToHashSet(),
                tagParents,
                serverTags.ToDictionary(p => p.Key, p => (IReadOnlyCollection<int>)p.Value),
                tagsReadable));

        var described = new List<RouteSummary>(routes.Count);
        for (var i = 0; i < routes.Count; i++)
        {
            var r = routes[i];
            var e = expanded[i];
            described.Add(r with
            {
                ScopeServerName = r.ScopeServerId is { } sid && serverNames.TryGetValue(sid, out var sn) ? sn : null,
                ScopeTagName = r.ScopeTagId is { } tid && tagNames.TryGetValue(tid, out var tn) ? tn : null,
                ExpandedServerCount = e.IsScoped && !e.ScopeUnexpanded ? e.ExpandedServerIds?.Count ?? 0 : null,
                Orphaned = e.Orphaned,
                ScopeUnreadable = e.ScopeUnexpanded,
            });
        }

        return described;
    }

    /// <summary>Sets the flag; returns the number of rows updated. 0 is no such route, or a route that has an agent setting and so is
    /// not turned off or changed over MCP (#6281): the condition is in the same statement as the write, so a setting a person adds
    /// after the caller's read is still honoured. Only a route with no agent setting, or an Off one being turned on, is written.</summary>
    public async Task<int> SetEnabledAsync(int routeId, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(SetEnabledSql);
        command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>The refusal text for a fallback that cannot be set on <paramref name="routeId"/>, or null when it can: a route
    /// is never its own fallback and the fallback must be an existing route (#6036).</summary>
    public static string? FallbackRefusal(int routeId, int fallbackRouteId, IEnumerable<int> existingRouteIds)
    {
        if (fallbackRouteId == routeId)
        {
            return $"fallback_route_id {fallbackRouteId} is the route itself; a route cannot be its own fallback.";
        }

        return existingRouteIds.Contains(fallbackRouteId)
            ? null
            : $"No notification route with route_id {fallbackRouteId} to use as the fallback; see get_notification_routes.";
    }

    /// <summary>Sets (or, with null, clears) the fallback; returns the number of rows updated (0 = no such route).</summary>
    public async Task<int> SetFallbackAsync(int routeId, int? fallbackRouteId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(SetFallbackSql);
        command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        command.Parameters.Add(new NpgsqlParameter<int?> { TypedValue = fallbackRouteId });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Deletes the route and clears every other route's reference to it as its fallback, in one transaction;
    /// returns the number of rows deleted. 0 is no such route, or a route that has an agent setting (#6281): the statement itself
    /// holds back such a route, and nothing is changed then (the fallback references are kept).</summary>
    public async Task<int> DeleteAsync(int routeId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using (var clear = new NpgsqlCommand(ClearFallbackReferencesSql, connection, transaction))
        {
            clear.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
            clear.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
            await clear.ExecuteNonQueryAsync(cancellationToken);
        }

        int deleted;
        await using (var command = new NpgsqlCommand(DeleteSql, connection, transaction))
        {
            command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
            deleted = await command.ExecuteNonQueryAsync(cancellationToken);
        }

        if (deleted == 0)
        {
            await transaction.RollbackAsync(cancellationToken);
            return 0;
        }

        await transaction.CommitAsync(cancellationToken);
        return deleted;
    }
}
