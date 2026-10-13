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
/// can feed Lite's real gathered inputs to the same expander Darling's loader calls. <see cref="TagsReadable"/> is false
/// when the tag tables could not be read: the tag maps are then empty and
/// <see cref="DuckDbNotificationRouteStore.ExpandRoutes"/> leaves a tag-scoped route unexpanded, as Darling's loader does.</summary>
public sealed record NotificationRouteScopeInputs(
    IReadOnlyCollection<int> KnownServerIds,
    IReadOnlyDictionary<int, int?> TagParents,
    IReadOnlyDictionary<int, IReadOnlyCollection<int>> ServerTags,
    bool TagsReadable = true);

/// <summary>
/// DuckDB-backed store for the scoped notification routes (#5750) over <c>notification_routes</c> (schema v69), with
/// the destinations kept in Windows Credential Manager.
///
/// <para><b>Secrets (ruling 6).</b> A route's Teams, Slack and Generic URLs and its PagerDuty key never reach DuckDB.
/// Each goes to Credential Manager under <c>NotificationRoute_&lt;routeId&gt;_&lt;Teams|Slack|Generic|PagerDuty&gt;</c>
/// (Lite's <c>CredentialService</c> prefixes it with <c>PerformanceMonitorLite_</c>), through two delegates (a
/// read, and a save that reports whether it worked), so a test passes an in-memory pair and never touches the real Credential
/// Manager. A save is checked: the old credential is deleted, the new one written and read back, and a refusal throws
/// <see cref="RouteSecretException"/> with the route unchanged (secrets are written before the row, and put back if the row
/// write fails). The table keeps a has-value flag per channel: a
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
    private readonly Func<string, string, bool> _saveSecret;

    /* Route and channel pairs already warned about (#5750): a saved destination Credential Manager cannot give back is logged
       once per process per route and channel, not on every reload. The key is the credential name, which carries the route id
       and the channel and never the destination. The one store instance lives for the process. */
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _warnedUnreadable = new();

    /// <param name="dbInitializer">The Lite database.</param>
    /// <param name="knownServerIds">The configured servers' hash ids (the ids the alert path and server_tag_map use).</param>
    /// <param name="getSecret">Reads a credential by key; empty when absent or unreadable (<c>App.GetWebhookUrl</c> in the app).</param>
    /// <param name="saveSecret">Saves a credential by key and says whether it worked; an empty value deletes it. The app wires it
    /// straight to <c>CredentialService</c> in <c>MainWindow</c>, not through <c>App.SaveWebhookUrl</c>, which swallows the result
    /// (that method and the 512-character limit are #5772). Required, so nothing falls back to a path that ignores a failed save.</param>
    public DuckDbNotificationRouteStore(
        DuckDbInitializer dbInitializer,
        Func<IReadOnlyCollection<int>> knownServerIds,
        Func<string, string> getSecret,
        Func<string, string, bool> saveSecret)
    {
        _dbInitializer = dbInitializer;
        _knownServerIds = knownServerIds;
        _getSecret = getSecret;
        _saveSecret = saveSecret;
    }

    /// <summary>The save delegate the app uses: straight to <see cref="CredentialService"/>, so a refused save is reported (an
    /// empty value deletes). <c>App.SaveWebhookUrl</c> ignores the result, and it and the 512-character limit are #5772.</summary>
    public static bool SaveToCredentialManager(string key, string value)
    {
        var credentials = new CredentialService();
        return string.IsNullOrEmpty(value)
            ? credentials.DeleteCredential(key)
            : credentials.SaveCredential(key, "webhook", value);
    }

    /// <summary>The Credential Manager key for one destination of one route.</summary>
    public static string SecretKey(int routeId, string channel) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture, $"NotificationRoute_{routeId}_{channel}");

    /// <summary>Every route with its destinations read from Credential Manager and its scope expanded. Callers must not
    /// log the URLs. A tag-read failure is logged and leaves only the tag-scoped routes unexpanded, so they match nothing;
    /// server-scoped and fleet-wide routes still work, as in Darling's loader (#5750).</summary>
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
            /* The server list itself could not be built, so no scope can be expanded. Fail closed: a scoped route that was
               never expanded matches nothing (NotificationRoute.CanMatch). Fleet-wide routes are untouched. */
            AppLogger.Warn("NotificationRoutes", $"Could not read the servers, so scoped notification routes match nothing until the next successful reload: {ex.Message}");
            return routes;
        }

        return ExpandRoutes(routes, inputs);
    }

    /// <summary>Expands the routes with the given inputs. When the tag tables were unreadable, a tag-scoped route is returned
    /// unexpanded (it matches nothing and is not marked orphaned, since the tag may still exist); server-scoped routes expand
    /// from the server list. The same rule as Darling's <c>StoreConfigProvider.ExpandRoutes</c> (#5750).</summary>
    public static IReadOnlyList<NotificationRoute> ExpandRoutes(IReadOnlyList<NotificationRoute> routes, NotificationRouteScopeInputs inputs)
    {
        var expanded = NotificationRouteScopeExpander.Expand(routes, inputs.KnownServerIds, inputs.TagParents, inputs.ServerTags);
        if (inputs.TagsReadable)
        {
            return expanded;
        }

        var result = new List<NotificationRoute>(expanded.Count);
        for (var i = 0; i < expanded.Count; i++)
        {
            result.Add(routes[i].ScopeTagId.HasValue ? routes[i] : expanded[i]);
        }

        return result;
    }

    /// <summary>One route (secrets and expansion included), or null.</summary>
    public async Task<NotificationRoute?> GetAsync(int routeId, CancellationToken cancellationToken = default) =>
        (await ListAsync(cancellationToken)).FirstOrDefault(r => r.RouteId == routeId);

    /// <summary>The three inputs <see cref="NotificationRouteScopeExpander.Expand"/> takes besides the routes: the configured
    /// servers' hash ids, the tag parent map (<c>server_tags</c>) and the server-to-tags map (<c>server_tag_map</c>). A failed
    /// tag read is logged and reported through <see cref="NotificationRouteScopeInputs.TagsReadable"/> (the tag maps are then
    /// empty), the same as Darling's <c>ReadRouteScopeInputsAsync</c>: a transient failure there must narrow what a route
    /// matches, never widen it, and the server list does not come from the tag tables. The one place Lite gathers them; the
    /// parity pin calls it.</summary>
    public async Task<NotificationRouteScopeInputs> GatherScopeInputsAsync(CancellationToken cancellationToken = default)
    {
        var known = _knownServerIds().ToArray();
        var parents = new Dictionary<int, int?>();
        var serverTags = new Dictionary<int, List<int>>();

        using var readLock = _dbInitializer.AcquireReadLock(cancellationToken);
        using var connection = _dbInitializer.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        try
        {
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
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            AppLogger.Warn("NotificationRoutes", $"Could not read the server tags, so tag-scoped notification routes match nothing until the next successful reload: {ex.Message}");
            return new NotificationRouteScopeInputs(
                known,
                new Dictionary<int, int?>(),
                new Dictionary<int, IReadOnlyCollection<int>>(),
                TagsReadable: false);
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

            /* Secrets BEFORE the row (#5750): a secret that cannot be stored throws here and no row is written, so a route never
               exists whose has-value flag points at a destination that was not saved. A channel the new route does not set has
               any credential already under this id deleted: a delete that failed on an earlier route with the reused id must not
               be inherited. If the row insert then fails, the credentials just written are put back as they were. */
            var undo = SaveSecrets(id, normalized, keep: NoChannels, strictClear: false);
            try
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $@"
                INSERT INTO {TableName}
                    (route_id, metric_match, scope_server_id, scope_tag_id,
                     teams_set, slack_set, generic_set, pagerduty_set, smtp_recipients, enabled)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
                AddWriteParameters(cmd, id, normalized, NoChannels);
                await cmd.ExecuteNonQueryAsync();
            }
            catch
            {
                RestoreSecrets(id, undo);
                throw;
            }
        }

        NotificationRouteCache.Invalidate();
        return id;
    }

    /// <summary>Replaces a route's match, scope, destinations and enabled flag. An empty destination removes that channel's
    /// credential, except a channel named in <see cref="NotificationRoute.OffChannels"/> whose saved flag is still set: that
    /// destination was unreadable when the route was loaded, so leaving it alone keeps the flag (the channel stays off for the
    /// route, #5366) instead of letting an untouched save clear it into the fleet-wide route or the app-wide URL. Returns false
    /// when no such route exists. A destination Credential Manager refuses throws <see cref="RouteSecretException"/> and the
    /// route is unchanged.</summary>
    public async Task<bool> UpdateAsync(NotificationRoute route)
    {
        var normalized = Validate(route);
        bool found;
        using (var writeLock = _dbInitializer.AcquireWriteLock())
        {
            PreservedTableRestore.ThrowIfRestorePending(_dbInitializer.ArchivePath, TableName);
            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();

            var savedFlags = new List<string>();
            using (var probe = connection.CreateCommand())
            {
                probe.CommandText = $"SELECT teams_set, slack_set, generic_set, pagerduty_set FROM {TableName} WHERE route_id = $1";
                probe.Parameters.Add(new DuckDBParameter { Value = route.RouteId });
                using var reader = await probe.ExecuteReaderAsync();
                found = await reader.ReadAsync();
                if (found)
                {
                    for (var i = 0; i < s_channels.Length; i++)
                    {
                        if (reader.GetBoolean(i))
                        {
                            savedFlags.Add(s_channels[i]);
                        }
                    }
                }
            }

            if (found)
            {
                var keep = (normalized.OffChannels ?? []).Where(savedFlags.Contains).ToHashSet(StringComparer.Ordinal);

                /* Order (#5750): secrets first, then the row, with the previous credentials put back if either step fails.
                   The row (scope, flags) therefore never commits ahead of a secret that did not save: a route moved to
                   another server with a destination Credential Manager refuses keeps its old row AND its old destination,
                   and the caller is told. */
                var undo = SaveSecrets(route.RouteId, normalized, keep, strictClear: true);
                try
                {
                    using var cmd = connection.CreateCommand();
                    cmd.CommandText = $@"
                UPDATE {TableName} SET
                    metric_match = $2, scope_server_id = $3, scope_tag_id = $4,
                    teams_set = $5, slack_set = $6, generic_set = $7, pagerduty_set = $8,
                    smtp_recipients = $9, enabled = $10
                WHERE route_id = $1";
                    AddWriteParameters(cmd, route.RouteId, normalized, keep);
                    await cmd.ExecuteNonQueryAsync();
                }
                catch
                {
                    RestoreSecrets(route.RouteId, undo);
                    throw;
                }
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
    /// still cleared, so a half-deleted route cannot leave a secret behind). A credential Credential Manager will not remove
    /// is logged and does not undo the delete.</summary>
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

            /* The row is gone whatever happens next. A credential that will not delete is logged (the channel, never the
               value) and left: it has no row to point at, and an insert that reuses this id deletes it again. */
            foreach (var channel in s_channels)
            {
                ClearSecret(routeId, channel, strict: false);
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
                /* Saved, but Credential Manager gave nothing back: off, not inherited (#5366). Say so once; never the URL. */
                off.Add(channel);
                if (_warnedUnreadable.TryAdd(SecretKey(row.RouteId, channel), 0))
                {
                    AppLogger.Warn("NotificationRoutes", $"Notification route {row.RouteId} has a saved {channel} destination that could not be read from Credential Manager, so that channel is off for the route until the destination is entered again.");
                }

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

    private static void AddWriteParameters(DuckDBCommand cmd, int id, NotificationRoute r, IReadOnlySet<string> keep)
    {
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.MetricMatch });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.ScopeServerId ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.ScopeTagId ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.TeamsUrl.Length > 0 || keep.Contains(NotificationRouter.TeamsChannel) });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.SlackUrl.Length > 0 || keep.Contains(NotificationRouter.SlackChannel) });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.GenericUrl.Length > 0 || keep.Contains(NotificationRouter.GenericChannel) });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.PagerDutyRoutingKey.Length > 0 || keep.Contains(NotificationRouter.PagerDutyChannel) });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.SmtpRecipients });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.Enabled });
    }

    private static readonly IReadOnlySet<string> NoChannels = new HashSet<string>(StringComparer.Ordinal);

    /// <summary>Where a credential stood before a save changed it, so a failure can put it back.</summary>
    private sealed record PreviousSecret(string Channel, string Value);

    /// <summary>Saves the four destinations. For a channel with a value: delete the old credential, write the new one, read it
    /// back and compare. For a channel without one: delete it (a channel in <paramref name="keep"/> is left alone). On any
    /// failure the credentials already changed are put back and a <see cref="RouteSecretException"/> is thrown; on success the
    /// list of previous values is returned so the caller can undo them if the row write then fails.</summary>
    private List<PreviousSecret> SaveSecrets(int routeId, NotificationRoute r, IReadOnlySet<string> keep, bool strictClear)
    {
        var changed = new List<PreviousSecret>();
        var destinations = new[]
        {
            (NotificationRouter.TeamsChannel, r.TeamsUrl),
            (NotificationRouter.SlackChannel, r.SlackUrl),
            (NotificationRouter.GenericChannel, r.GenericUrl),
            (NotificationRouter.PagerDutyChannel, r.PagerDutyRoutingKey)
        };

        try
        {
            foreach (var (channel, value) in destinations)
            {
                if (value.Length == 0 && keep.Contains(channel))
                {
                    continue;
                }

                changed.Add(new PreviousSecret(channel, ReadSecret(routeId, channel)));
                if (value.Length == 0)
                {
                    ClearSecret(routeId, channel, strictClear);
                }
                else
                {
                    WriteSecret(routeId, channel, value);
                }
            }
        }
        catch
        {
            RestoreSecrets(routeId, changed);
            throw;
        }

        return changed;
    }

    private string ReadSecret(int routeId, string channel)
    {
        try
        {
            return _getSecret(SecretKey(routeId, channel)) ?? "";
        }
        catch (Exception)
        {
            return "";
        }
    }

    private void WriteSecret(int routeId, string channel, string value)
    {
        var key = SecretKey(routeId, channel);
        bool ok;
        try
        {
            /* Delete the old one first, so a value that cannot be stored never leaves the previous destination in place. */
            _saveSecret(key, "");
            ok = _saveSecret(key, value) && string.Equals(_getSecret(key), value, StringComparison.Ordinal);
        }
        catch (Exception)
        {
            ok = false;
        }

        if (!ok)
        {
            throw new RouteSecretException(channel, value.Length);
        }
    }

    private void ClearSecret(int routeId, string channel, bool strict)
    {
        var key = SecretKey(routeId, channel);
        bool ok;
        try
        {
            /* A delete of something that is not there may report false; what matters is that nothing is left to read. */
            ok = _saveSecret(key, "") || string.IsNullOrWhiteSpace(_getSecret(key));
        }
        catch (Exception)
        {
            ok = false;
        }

        if (ok)
        {
            return;
        }

        if (strict)
        {
            throw new RouteSecretException(channel, removing: true);
        }

        AppLogger.Warn("NotificationRoutes", $"Notification route {routeId}: the {channel} credential could not be removed from Credential Manager.");
    }

    /// <summary>Best effort: puts each changed credential back to its previous value (an empty one deletes it). A failure here
    /// is logged, with the channel and never the value.</summary>
    private void RestoreSecrets(int routeId, List<PreviousSecret> changed)
    {
        foreach (var previous in changed)
        {
            try
            {
                if (!_saveSecret(SecretKey(routeId, previous.Channel), previous.Value) && previous.Value.Length > 0)
                {
                    AppLogger.Warn("NotificationRoutes", $"Notification route {routeId}: the previous {previous.Channel} credential could not be put back in Credential Manager.");
                }
            }
            catch (Exception)
            {
                AppLogger.Warn("NotificationRoutes", $"Notification route {routeId}: the previous {previous.Channel} credential could not be put back in Credential Manager.");
            }
        }
    }
}

/// <summary>Windows Credential Manager would not store (or remove) a route destination, so the route was not saved (#5750). The
/// message names the channel and never the value. The edit dialog shows it inline.</summary>
public sealed class RouteSecretException : InvalidOperationException
{
    /// <summary>The longest value Lite's Credential Manager wrapper stores today (#5772 is the app-wide fix).</summary>
    public const int StorableLength = 512;

    public RouteSecretException(string channel, int length)
        : base($"The {channel} destination could not be saved to Windows Credential Manager, so the route was not changed."
               + (length > StorableLength ? $" It may be longer than Lite can store today ({StorableLength} characters)." : ""))
    {
    }

    public RouteSecretException(string channel, bool removing)
        : base($"The saved {channel} destination could not be removed from Windows Credential Manager, so the route was not changed.")
    {
    }
}
