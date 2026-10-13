/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>What <see cref="NotificationRouteScopeExpander.Expand"/> needs besides the routes themselves (#5750):
/// the configured servers' hash ids, the tag parent map and the server-to-tags map. One value so the parity pin
/// can feed Lite's real gathered inputs to the same expander Darling's loader calls.</summary>
public sealed record NotificationRouteScopeInputs(
    IReadOnlyCollection<int> KnownServerIds,
    IReadOnlyDictionary<int, int?> TagParents,
    IReadOnlyDictionary<int, IReadOnlyCollection<int>> ServerTags);

/// <summary>
/// DuckDB-backed store for the scoped notification routes (#5750) over <c>notification_routes</c> (schema v69), with
/// the destinations kept in Windows Credential Manager.
///
/// <para><b>Secrets (ruling 6).</b> A route's Teams, Slack and Generic URLs and its PagerDuty key never reach DuckDB.
/// Each goes to Credential Manager under <c>NotificationRoute_&lt;routeId&gt;_&lt;Teams|Slack|Generic|PagerDuty&gt;</c>
/// (Lite's <c>CredentialService</c> prefixes it with <c>PerformanceMonitorLite_</c>), through the same two
/// delegates Lite's app-wide webhook URLs use (<c>App.GetWebhookUrl</c> / <c>App.SaveWebhookUrl</c>), so a test passes
/// an in-memory pair and never touches the real Credential Manager. The table keeps a has-value flag per channel: a
/// flag that says "saved" with no readable credential turns that channel OFF for the route
/// (<see cref="NotificationRoute.OffChannels"/>, #5366) rather than falling through to the app-wide URL, which would send
/// the alert somewhere it was not configured to go. SMTP recipients are not secrets and are stored in the table, the way
/// the app-wide recipients are a plain setting. Generic headers and the body template stay app-wide (ruling 7).</para>
///
/// <para><b>Validation is in the store as well as in the table's CHECKs</b> (DuckDB enforces both): a route with both
/// scopes, or the <c>*</c> match without a scope, is refused with an <see cref="ArgumentException"/> before the
/// statement runs, so the caller gets a message instead of a constraint error.</para>
///
/// <para><b>Locks.</b> Modeled on <see cref="DuckDbMuteRuleStore"/>: every write takes the write lock and refuses while a
/// restore of the table is pending, every read takes the read lock. The rule and the measurements are in #2463 and on
/// <c>DuckDbInitializer.s_dbLock</c>. Ids are max + 1 under that lock, like <c>server_tags</c>.</para>
///
/// <para><b>Reload.</b> Every write here calls <see cref="NotificationRouteCache.Invalidate"/>; so do the tag writers and
/// the server writers (see <see cref="NotificationRouteCache"/>).</para>
/// </summary>
public sealed class DuckDbNotificationRouteStore
{
    internal const string TableName = "notification_routes";

    private static readonly string[] s_channels =
    [
        NotificationRouter.TeamsChannel,
        NotificationRouter.SlackChannel,
        NotificationRouter.GenericChannel,
        NotificationRouter.PagerDutyChannel
    ];

    private readonly DuckDbInitializer _dbInitializer;
    private readonly Func<IReadOnlyCollection<int>> _knownServerIds;
    private readonly Func<string, string> _getSecret;
    private readonly Action<string, string> _saveSecret;

    /// <param name="dbInitializer">The Lite database.</param>
    /// <param name="knownServerIds">The configured servers' hash ids (the ids the alert path and server_tag_map use).</param>
    /// <param name="getSecret">Reads a credential by key; empty when absent or unreadable. Defaults to the app-wide
    /// webhook helper, <c>App.GetWebhookUrl</c>.</param>
    /// <param name="saveSecret">Saves a credential by key; an empty value deletes it. Defaults to <c>App.SaveWebhookUrl</c>.</param>
    public DuckDbNotificationRouteStore(
        DuckDbInitializer dbInitializer,
        Func<IReadOnlyCollection<int>> knownServerIds,
        Func<string, string>? getSecret = null,
        Action<string, string>? saveSecret = null)
    {
        _dbInitializer = dbInitializer;
        _knownServerIds = knownServerIds;
        _getSecret = getSecret ?? App.GetWebhookUrl;
        _saveSecret = saveSecret ?? App.SaveWebhookUrl;
    }

    /// <summary>The Credential Manager key for one destination of one route.</summary>
    public static string SecretKey(int routeId, string channel) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture, $"NotificationRoute_{routeId}_{channel}");

    /// <summary>Every route with its destinations read from Credential Manager and its scope expanded. Callers must not
    /// log the URLs. A tag-read failure is logged and leaves the scoped routes unexpanded, so they match nothing.</summary>
    public async Task<IReadOnlyList<NotificationRoute>> ListAsync(CancellationToken cancellationToken = default)
    {
        var rows = await ReadRowsAsync(cancellationToken);
        var routes = new List<NotificationRoute>(rows.Count);
        foreach (var row in rows)
        {
            routes.Add(Hydrate(row));
        }

        NotificationRouteScopeInputs inputs;
        try
        {
            inputs = await GatherScopeInputsAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            /* Fail closed: a scoped route that was never expanded matches nothing (NotificationRoute.CanMatch), and no
               tag read means the sets cannot be known. Fleet-wide routes are untouched. */
            AppLogger.Warn("NotificationRoutes", $"Could not read the server tags, so scoped notification routes match nothing until the next successful reload: {ex.Message}");
            return routes;
        }

        return NotificationRouteScopeExpander.Expand(routes, inputs.KnownServerIds, inputs.TagParents, inputs.ServerTags);
    }

    /// <summary>One route (secrets and expansion included), or null.</summary>
    public async Task<NotificationRoute?> GetAsync(int routeId, CancellationToken cancellationToken = default) =>
        (await ListAsync(cancellationToken)).FirstOrDefault(r => r.RouteId == routeId);

    /// <summary>The three inputs <see cref="NotificationRouteScopeExpander.Expand"/> takes besides the routes: the configured
    /// servers' hash ids, the tag parent map (<c>server_tags</c>) and the server-to-tags map (<c>server_tag_map</c>). Throws
    /// when a tag table cannot be read. The one place Lite gathers them; the parity pin calls it.</summary>
    public async Task<NotificationRouteScopeInputs> GatherScopeInputsAsync(CancellationToken cancellationToken = default)
    {
        var known = _knownServerIds().ToArray();
        var parents = new Dictionary<int, int?>();
        var serverTags = new Dictionary<int, List<int>>();

        using var readLock = _dbInitializer.AcquireReadLock(cancellationToken);
        using var connection = _dbInitializer.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        using (var tags = connection.CreateCommand())
        {
            tags.CommandText = "SELECT id, parent_id FROM server_tags";
            using var reader = await tags.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                parents[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetInt32(1);
            }
        }

        using (var map = connection.CreateCommand())
        {
            map.CommandText = "SELECT server_id, tag_id FROM server_tag_map";
            using var reader = await map.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var serverId = reader.GetInt32(0);
                if (!serverTags.TryGetValue(serverId, out var list))
                {
                    serverTags[serverId] = list = [];
                }

                list.Add(reader.GetInt32(1));
            }
        }

        return new NotificationRouteScopeInputs(
            known,
            parents,
            serverTags.ToDictionary(kv => kv.Key, kv => (IReadOnlyCollection<int>)kv.Value));
    }

    /// <summary>Saves a new route and returns its id (the route's own <see cref="NotificationRoute.RouteId"/> is ignored).</summary>
    public async Task<int> InsertAsync(NotificationRoute route)
    {
        var normalized = Validate(route);
        int id;
        using (var writeLock = _dbInitializer.AcquireWriteLock())
        {
            PreservedTableRestore.ThrowIfRestorePending(_dbInitializer.ArchivePath, TableName);
            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();

            using (var next = connection.CreateCommand())
            {
                next.CommandText = $"SELECT COALESCE(MAX(route_id), 0) + 1 FROM {TableName}";
                id = Convert.ToInt32(await next.ExecuteScalarAsync());
            }

            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                INSERT INTO {TableName}
                    (route_id, metric_match, scope_server_id, scope_tag_id,
                     teams_set, slack_set, generic_set, pagerduty_set, smtp_recipients, enabled)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
            AddWriteParameters(cmd, id, normalized);
            await cmd.ExecuteNonQueryAsync();

            /* After the row: a failed insert leaves no stray credential behind. */
            SaveSecrets(id, normalized);
        }

        NotificationRouteCache.Invalidate();
        return id;
    }

    /// <summary>Replaces a route's match, scope, destinations and enabled flag. An empty destination removes that channel's
    /// credential. Returns false when no such route exists.</summary>
    public async Task<bool> UpdateAsync(NotificationRoute route)
    {
        var normalized = Validate(route);
        bool found;
        using (var writeLock = _dbInitializer.AcquireWriteLock())
        {
            PreservedTableRestore.ThrowIfRestorePending(_dbInitializer.ArchivePath, TableName);
            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                UPDATE {TableName} SET
                    metric_match = $2, scope_server_id = $3, scope_tag_id = $4,
                    teams_set = $5, slack_set = $6, generic_set = $7, pagerduty_set = $8,
                    smtp_recipients = $9, enabled = $10
                WHERE route_id = $1";
            AddWriteParameters(cmd, route.RouteId, normalized);
            found = await cmd.ExecuteNonQueryAsync() > 0;
            if (found)
            {
                SaveSecrets(route.RouteId, normalized);
            }
        }

        NotificationRouteCache.Invalidate();
        return found;
    }

    /// <summary>Turns a route on or off. Returns false when no such route exists.</summary>
    public async Task<bool> SetEnabledAsync(int routeId, bool enabled)
    {
        bool found;
        using (var writeLock = _dbInitializer.AcquireWriteLock())
        {
            PreservedTableRestore.ThrowIfRestorePending(_dbInitializer.ArchivePath, TableName);
            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"UPDATE {TableName} SET enabled = $2 WHERE route_id = $1";
            cmd.Parameters.Add(new DuckDBParameter { Value = routeId });
            cmd.Parameters.Add(new DuckDBParameter { Value = enabled });
            found = await cmd.ExecuteNonQueryAsync() > 0;
        }

        NotificationRouteCache.Invalidate();
        return found;
    }

    /// <summary>Deletes a route and its four credentials. Returns false when no such route exists (the credentials are
    /// still cleared, so a half-deleted route cannot leave a secret behind).</summary>
    public async Task<bool> DeleteAsync(int routeId)
    {
        bool found;
        using (var writeLock = _dbInitializer.AcquireWriteLock())
        {
            PreservedTableRestore.ThrowIfRestorePending(_dbInitializer.ArchivePath, TableName);
            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {TableName} WHERE route_id = $1";
            cmd.Parameters.Add(new DuckDBParameter { Value = routeId });
            found = await cmd.ExecuteNonQueryAsync() > 0;

            foreach (var channel in s_channels)
            {
                _saveSecret(SecretKey(routeId, channel), "");
            }
        }

        NotificationRouteCache.Invalidate();
        return found;
    }

    private sealed record Row(
        int RouteId, string MetricMatch, int? ScopeServerId, int? ScopeTagId,
        bool TeamsSet, bool SlackSet, bool GenericSet, bool PagerDutySet, string SmtpRecipients, bool Enabled);

    private async Task<List<Row>> ReadRowsAsync(CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        using var readLock = _dbInitializer.AcquireReadLock(cancellationToken);
        using var connection = _dbInitializer.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT route_id, metric_match, scope_server_id, scope_tag_id,
                   teams_set, slack_set, generic_set, pagerduty_set, smtp_recipients, enabled
            FROM {TableName}
            ORDER BY route_id";
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.GetBoolean(4),
                reader.GetBoolean(5),
                reader.GetBoolean(6),
                reader.GetBoolean(7),
                reader.IsDBNull(8) ? "" : reader.GetString(8),
                reader.GetBoolean(9)));
        }

        return rows;
    }

    private NotificationRoute Hydrate(Row row)
    {
        var off = new List<string>();
        string Read(bool saved, string channel)
        {
            if (!saved)
            {
                return "";
            }

            var value = _getSecret(SecretKey(row.RouteId, channel)) ?? "";
            if (string.IsNullOrWhiteSpace(value))
            {
                /* Saved, but Credential Manager gave nothing back: off, not inherited (#5366). */
                off.Add(channel);
                return "";
            }

            return value;
        }

        var teams = Read(row.TeamsSet, NotificationRouter.TeamsChannel);
        var slack = Read(row.SlackSet, NotificationRouter.SlackChannel);
        var generic = Read(row.GenericSet, NotificationRouter.GenericChannel);
        var pagerDuty = Read(row.PagerDutySet, NotificationRouter.PagerDutyChannel);

        return new NotificationRoute(
            row.RouteId, row.MetricMatch, teams, slack, generic, pagerDuty, row.SmtpRecipients, row.Enabled)
        {
            ScopeServerId = row.ScopeServerId,
            ScopeTagId = row.ScopeTagId,
            OffChannels = off.Count == 0 ? null : off
        };
    }

    private static NotificationRoute Validate(NotificationRoute route)
    {
        ArgumentNullException.ThrowIfNull(route);
        var match = (route.MetricMatch ?? "").Trim();
        if (match.Length == 0)
        {
            throw new ArgumentException("A notification route needs a metric match.", nameof(route));
        }

        if (route.ScopeServerId.HasValue && route.ScopeTagId.HasValue)
        {
            throw new ArgumentException("A notification route is scoped to a server or to a tag, never both.", nameof(route));
        }

        if (match == NotificationRoute.WildcardMatch && !route.IsScoped)
        {
            throw new ArgumentException("The every-metric match ('*') needs a server or tag scope; a fleet-wide route must name a metric or family.", nameof(route));
        }

        return route with
        {
            MetricMatch = match,
            TeamsUrl = (route.TeamsUrl ?? "").Trim(),
            SlackUrl = (route.SlackUrl ?? "").Trim(),
            GenericUrl = (route.GenericUrl ?? "").Trim(),
            PagerDutyRoutingKey = (route.PagerDutyRoutingKey ?? "").Trim(),
            SmtpRecipients = (route.SmtpRecipients ?? "").Trim()
        };
    }

    private static void AddWriteParameters(DuckDBCommand cmd, int id, NotificationRoute r)
    {
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.MetricMatch });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.ScopeServerId ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.ScopeTagId ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.TeamsUrl.Length > 0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.SlackUrl.Length > 0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.GenericUrl.Length > 0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.PagerDutyRoutingKey.Length > 0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.SmtpRecipients });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.Enabled });
    }

    private void SaveSecrets(int routeId, NotificationRoute r)
    {
        /* An empty value deletes the credential (App.SaveWebhookUrl), so clearing a channel in an edit removes it. */
        _saveSecret(SecretKey(routeId, NotificationRouter.TeamsChannel), r.TeamsUrl);
        _saveSecret(SecretKey(routeId, NotificationRouter.SlackChannel), r.SlackUrl);
        _saveSecret(SecretKey(routeId, NotificationRouter.GenericChannel), r.GenericUrl);
        _saveSecret(SecretKey(routeId, NotificationRouter.PagerDutyChannel), r.PagerDutyRoutingKey);
    }
}
