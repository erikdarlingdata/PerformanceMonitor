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
/// Each goes to Credential Manager under <c>NotificationRoute_&lt;routeId&gt;_&lt;Teams|Slack|Generic|PagerDuty&gt;_&lt;gen&gt;</c>
/// (Lite's <c>CredentialService</c> prefixes it with <c>PerformanceMonitorLite_</c>), through a <see cref="RouteSecretAccess"/>
/// (read, write, delete, list), so a test passes an in-memory fake and never touches the real Credential Manager.</para>
///
/// <para><b>Generations (#5775).</b> The row cannot be changed in the same step as Credential Manager, so the two are tied
/// together by a token, not by order of writes. Each channel has a nullable <c>*_secret_gen</c> column holding the token
/// of the key the row currently reads; every write of a value goes to a NEW random token, and only the row's one UPDATE
/// moves the route to it. A crash at any point therefore leaves the row and the key it points at in agreement: before the
/// UPDATE the old row reads the old keys, after it the new row reads the new keys, and whichever keys the row does not
/// point at are harmless leftovers that <see cref="SweepOrphanedSecretsAsync"/> removes at start. A random token also means
/// a reused route id can never inherit a leftover key. The order is: (1) write each changed value under a new token and
/// read it back; (2) one UPDATE (or INSERT) of scope, flags and tokens; (3) delete the old tokens' keys, leniently. A channel
/// the user left alone because it was unreadable is never copied: it keeps its token and its (missing) key.</para>
///
/// <para>The table keeps a has-value flag per channel: a flag that says "saved" with no readable credential at its token turns
/// that channel OFF for the route (<see cref="NotificationRoute.OffChannels"/>, #5366) rather than falling through to the
/// app-wide URL, which would send the alert somewhere it was not configured to go. SMTP recipients are not secrets and are
/// stored in the table, the way the app-wide recipients are a plain setting. Generic headers and the body template stay
/// app-wide (ruling 7).</para>
///
/// <para><b>Validation is in the store as well as in the table's CHECKs</b> (DuckDB enforces both): a route with both
/// scopes, or the <c>*</c> match without a scope, is refused with an <see cref="ArgumentException"/> before the
/// statement runs, so the caller gets a message instead of a constraint error.</para>
///
/// <para><b>Locks.</b> Modeled on <see cref="DuckDbMuteRuleStore"/>: every write takes the write lock (and holds it across
/// the secret writes and the sweep, so none can interleave) and refuses while a restore of the table is pending, every read
/// takes the read lock, the secret reads of a load included. The rule and the measurements are in #2463 and on
/// <c>DuckDbInitializer.s_dbLock</c>. Ids are max + 1 under that lock, like <c>server_tags</c>.</para>
///
/// <para><b>Reload.</b> Every write here calls <see cref="NotificationRouteCache.Invalidate"/>; so do the tag writers and
/// the server writers (see <see cref="NotificationRouteCache"/>).</para>
/// </summary>
public sealed class DuckDbNotificationRouteStore
{
    internal const string TableName = "notification_routes";

    /// <summary>Every Credential Manager key this store writes starts with this.</summary>
    internal const string KeyPrefix = "NotificationRoute_";

    private static readonly string[] s_channels =
    [
        NotificationRouter.TeamsChannel,
        NotificationRouter.SlackChannel,
        NotificationRouter.GenericChannel,
        NotificationRouter.PagerDutyChannel
    ];

    private const string RowColumns =
        "route_id, metric_match, scope_server_id, scope_tag_id, teams_set, slack_set, generic_set, pagerduty_set, "
        + "smtp_recipients, enabled, teams_secret_gen, slack_secret_gen, generic_secret_gen, pagerduty_secret_gen, "
        + "agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes, "
        + "generic_signing_set, generic_signing_until";

    private readonly DuckDbInitializer _dbInitializer;
    private readonly Func<IReadOnlyCollection<int>> _knownServerIds;
    private readonly RouteSecretAccess _secrets;

    /// <summary>Test seam (#5775): runs just before the one statement that writes the row, inside the try that decides whether
    /// a throw means "not committed" or "committed". Null in the app.</summary>
    internal Action? BeforeRowWriteForTests { get; set; }

    /// <summary>Test seam (#5775): runs just after that statement, in the same try. Null in the app.</summary>
    internal Action? AfterRowWriteForTests { get; set; }

    /* Route and channel pairs already warned about (#5750): a saved destination Credential Manager cannot give back is logged
       once per process per route, channel and token, not on every reload. The key is the credential name, which carries the
       route id, the channel and the token and never the destination. The one store instance lives for the process. */
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _warnedUnreadable = new();

    /// <param name="dbInitializer">The Lite database.</param>
    /// <param name="knownServerIds">The configured servers' hash ids (the ids the alert path and server_tag_map use).</param>
    /// <param name="secrets">Where the destinations live: <see cref="RouteSecretAccess.CredentialManager"/> in the app. Required,
    /// so nothing falls back to a path that ignores a failed save. A value of up to <see cref="PerformanceMonitor.Common.CredentialStore.MaxSecretLength"/> characters is stored (#5772).</param>
    public DuckDbNotificationRouteStore(
        DuckDbInitializer dbInitializer,
        Func<IReadOnlyCollection<int>> knownServerIds,
        RouteSecretAccess secrets)
    {
        _dbInitializer = dbInitializer;
        _knownServerIds = knownServerIds;
        _secrets = secrets;
    }

    /// <summary>The Credential Manager key for one destination of one route at one generation (#5775).</summary>
    public static string SecretKey(int routeId, string channel, string gen) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{KeyPrefix}{routeId}_{channel}_{gen}");

    /// <summary>A fresh generation token. Random, so a reused route id can never collide with a leftover key.</summary>
    private static string NewGeneration() => Guid.NewGuid().ToString("N");

    /// <summary>Removes every key this store wrote that no row points at (#5775): keys a crash left between writing a new token
    /// and committing the row, and old tokens whose delete failed. Run once after the database is initialized (the constructor
    /// cannot: the file is not open yet). It holds the write lock for the whole pass so a save cannot interleave, does nothing
    /// when a restore of the table is pending (the rows are not back yet, and every key would look orphaned) or when the rows
    /// cannot be read, and logs only the count. Returns how many keys it removed.</summary>
    public async Task<int> SweepOrphanedSecretsAsync()
    {
        try
        {
            using var writeLock = _dbInitializer.AcquireWriteLock();
            PreservedTableRestore.ThrowIfRestorePending(_dbInitializer.ArchivePath, TableName);

            var keys = _secrets.ListKeys(KeyPrefix);
            if (keys.Count == 0)
            {
                return 0;
            }

            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();
            var pointedAt = new HashSet<string>(StringComparer.Ordinal);
            foreach (var row in await ReadAllRowsAsync(connection))
            {
                for (var i = 0; i < s_channels.Length; i++)
                {
                    if (row.Set[i] && row.Gens[i] is { Length: > 0 } gen)
                    {
                        pointedAt.Add(SecretKey(row.RouteId, s_channels[i], gen));
                    }
                }
            }

            var removed = 0;
            var failed = 0;
            foreach (var key in keys)
            {
                if (pointedAt.Contains(key))
                {
                    continue;
                }

                if (SafeDelete(key))
                {
                    removed++;
                }
                else
                {
                    failed++;
                }
            }

            if (removed > 0 || failed > 0)
            {
                AppLogger.Info("NotificationRoutes", $"Removed {removed} leftover notification route credential(s) from Credential Manager"
                    + (failed > 0 ? $"; {failed} could not be removed and are tried again at the next start." : "."));
            }

            return removed;
        }
        catch (Exception ex)
        {
            /* Nothing is deleted when the rows could not be read: an unreadable table is never taken for "no routes". */
            AppLogger.Warn("NotificationRoutes", $"The leftover notification route credential check was skipped: {ex.Message}");
            return 0;
        }
    }

    /// <summary>Every route with its destinations read from Credential Manager and its scope expanded. Callers must not
    /// log the URLs. A tag-read failure is logged and leaves only the tag-scoped routes unexpanded, so they match nothing;
    /// server-scoped and fleet-wide routes still work, as in Darling's loader (#5750).</summary>
    public async Task<IReadOnlyList<NotificationRoute>> ListAsync(CancellationToken cancellationToken = default)
    {
        var routes = await ReadRoutesAsync(cancellationToken);

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
    public static IReadOnlyList<NotificationRoute> ExpandRoutes(IReadOnlyList<NotificationRoute> routes, NotificationRouteScopeInputs inputs) =>
        NotificationRouteScopeExpander.Expand(routes, inputs.KnownServerIds, inputs.TagParents, inputs.ServerTags, inputs.TagsReadable);

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

    /// <summary>Saves a new route and returns its id (the route's own <see cref="NotificationRoute.RouteId"/> is ignored).
    /// Order (#5775): each destination goes to Credential Manager under a new generation and is read back; then the one INSERT
    /// writes the row with those generations. A destination that cannot be stored removes the ones already written and throws
    /// <see cref="RouteSecretException"/> with no row. An INSERT that throws removes the new keys only if the row is absent.</summary>
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

            /* The keys are written before the row, under tokens no row points at yet, so a route never exists whose has-value
               flag points at a destination that was not saved. A leftover key from an earlier route with this id is under a
               different token and is never read. */
            var plans = BuildPlans(null, normalized, NoChannels);
            var written = WriteNewKeys(id, plans);
            try
            {
                BeforeRowWriteForTests?.Invoke();
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $@"
                INSERT INTO {TableName}
                    (route_id, metric_match, scope_server_id, scope_tag_id,
                     teams_set, slack_set, generic_set, pagerduty_set, smtp_recipients, enabled,
                     teams_secret_gen, slack_secret_gen, generic_secret_gen, pagerduty_secret_gen,
                     agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)";
                AddWriteParameters(cmd, id, normalized, plans);
                await cmd.ExecuteNonQueryAsync();
                AfterRowWriteForTests?.Invoke();
            }
            catch (Exception ex)
            {
                if (!await RowCommittedAsync(connection, id, normalized, plans, written, ex))
                {
                    throw;
                }
            }
        }

        NotificationRouteCache.Invalidate();
        return id;
    }

    /// <summary>Replaces a route's match, scope, destinations and enabled flag. An empty destination removes that channel's
    /// credential, except a channel named in <see cref="NotificationRoute.OffChannels"/> whose saved flag is still set: that
    /// destination was unreadable when the route was loaded, so leaving it alone keeps the flag and the generation (the channel
    /// stays off for the route, #5366) instead of letting an untouched save clear it into the fleet-wide route or the app-wide
    /// URL. Returns false when no such route exists. A destination Credential Manager refuses throws
    /// <see cref="RouteSecretException"/> and the route is unchanged.
    /// <para>Order (#5775): (1) every CHANGED destination is written under a new generation and read back (a failure removes
    /// the new keys and throws, row and old keys untouched); (2) one UPDATE writes scope, flags and generations (if it throws,
    /// the row is read again: new generations mean it committed and the save goes on, otherwise the new keys are removed and
    /// the exception is rethrown); (3) the old generations of the changed and cleared channels are deleted, leniently, since an
    /// orphaned key is harmless.</para></summary>
    public async Task<bool> UpdateAsync(NotificationRoute route)
    {
        var normalized = Validate(route);
        bool found;
        using (var writeLock = _dbInitializer.AcquireWriteLock())
        {
            PreservedTableRestore.ThrowIfRestorePending(_dbInitializer.ArchivePath, TableName);
            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();

            var current = await ReadRowAsync(connection, route.RouteId);
            found = current is not null;
            if (current is not null)
            {
                var keep = (normalized.OffChannels ?? [])
                    .Where(c => { var i = Array.IndexOf(s_channels, c); return i >= 0 && current.Set[i]; })
                    .ToHashSet(StringComparer.Ordinal);
                var plans = BuildPlans(current, normalized, keep);
                var written = WriteNewKeys(route.RouteId, plans);
                try
                {
                    BeforeRowWriteForTests?.Invoke();
                    using var cmd = connection.CreateCommand();
                    cmd.CommandText = $@"
                UPDATE {TableName} SET
                    metric_match = $2, scope_server_id = $3, scope_tag_id = $4,
                    teams_set = $5, slack_set = $6, generic_set = $7, pagerduty_set = $8,
                    smtp_recipients = $9, enabled = $10,
                    teams_secret_gen = $11, slack_secret_gen = $12, generic_secret_gen = $13, pagerduty_secret_gen = $14,
                    agent_level = $15, agent_max_per_hour = $16, agent_max_per_day = $17,
                    agent_lease_minutes = $18, agent_expiry_minutes = $19
                WHERE route_id = $1";
                    AddWriteParameters(cmd, route.RouteId, normalized, plans);
                    await cmd.ExecuteNonQueryAsync();
                    AfterRowWriteForTests?.Invoke();
                }
                catch (Exception ex)
                {
                    if (!await RowCommittedAsync(connection, route.RouteId, normalized, plans, written, ex))
                    {
                        throw;
                    }
                }

                /* (3) The row points at the new generations now: the old ones are garbage. A key that will not go is logged and
                   left; no row points at it, and the sweep at the next start tries again. */
                DeleteKeysLeniently(
                    route.RouteId,
                    plans.Where(p => p.OldGen is not null && p.Gen != p.OldGen).Select(p => (p.Channel, SecretKey(route.RouteId, p.Channel, p.OldGen!))),
                    "old");
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

    /// <summary>Deletes a route and its credentials. Returns false when no such route exists (the credentials under that id are
    /// still cleared, so a half-deleted route cannot leave a secret behind). The row goes first; then the keys at the
    /// generations it pointed at and any other key under the route id, leniently: a key Credential Manager will not remove
    /// is logged and does not undo the delete, since no row points at it (#5775) and the sweep at the next start retries it.</summary>
    public async Task<bool> DeleteAsync(int routeId)
    {
        bool found;
        using (var writeLock = _dbInitializer.AcquireWriteLock())
        {
            PreservedTableRestore.ThrowIfRestorePending(_dbInitializer.ArchivePath, TableName);
            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();

            var current = await ReadRowAsync(connection, routeId);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {TableName} WHERE route_id = $1";
            cmd.Parameters.Add(new DuckDBParameter { Value = routeId });
            found = await cmd.ExecuteNonQueryAsync() > 0;

            /* The row is gone whatever happens next. */
            var keys = new Dictionary<string, string>(StringComparer.Ordinal);
            for (var i = 0; i < s_channels.Length; i++)
            {
                if (current?.Gens[i] is { Length: > 0 } gen)
                {
                    keys[SecretKey(routeId, s_channels[i], gen)] = s_channels[i];
                }
            }

            try
            {
                /* The trailing underscore keeps route 1 from matching route 12. */
                var routePrefix = string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{KeyPrefix}{routeId}_");
                foreach (var key in _secrets.ListKeys(routePrefix))
                {
                    keys.TryAdd(key, ChannelOfKey(key));
                }
            }
            catch (Exception)
            {
                /* The listing is a second net under the row's own generations; the sweep is the third. */
            }

            DeleteKeysLeniently(routeId, keys.Select(kv => (kv.Value, kv.Key)), "route");

            /* #6012: the route's signing keys go with it; a key that will not go is read by no route. */
            DeleteKeysLeniently(routeId, new[]
            {
                (NotificationRouter.GenericChannel, LiteWebhookSigningKeys.RouteName(routeId, false)),
                (NotificationRouter.GenericChannel, LiteWebhookSigningKeys.RouteName(routeId, true)),
            }, "signing");
        }

        NotificationRouteCache.Invalidate();
        return found;
    }

    /// <summary>One row of the table. <see cref="Set"/> and <see cref="Gens"/> are in channel order (<c>s_channels</c>).</summary>
    private sealed record Row(
        int RouteId, string MetricMatch, int? ScopeServerId, int? ScopeTagId,
        bool[] Set, string SmtpRecipients, bool Enabled, string?[] Gens,
        int? AgentLevel, int AgentMaxPerHour, int AgentMaxPerDay, int AgentLeaseMinutes, int AgentExpiryMinutes,
        bool SigningSet, DateTime? SigningUntilUtc);

    private static Row ReadRowFrom(System.Data.Common.DbDataReader reader) =>
        new(
            reader.GetInt32(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetInt32(2),
            reader.IsDBNull(3) ? null : reader.GetInt32(3),
            [reader.GetBoolean(4), reader.GetBoolean(5), reader.GetBoolean(6), reader.GetBoolean(7)],
            reader.IsDBNull(8) ? "" : reader.GetString(8),
            reader.GetBoolean(9),
            [reader.IsDBNull(10) ? null : reader.GetString(10),
             reader.IsDBNull(11) ? null : reader.GetString(11),
             reader.IsDBNull(12) ? null : reader.GetString(12),
             reader.IsDBNull(13) ? null : reader.GetString(13)],
            /* #5820: a NULL level is no agent; a NULL limit (a row written before the column existed) is the default. */
            reader.IsDBNull(14) ? null : reader.GetInt32(14),
            reader.IsDBNull(15) ? NotificationRoute.DefaultAgentMaxPerHour : reader.GetInt32(15),
            reader.IsDBNull(16) ? NotificationRoute.DefaultAgentMaxPerDay : reader.GetInt32(16),
            reader.IsDBNull(17) ? NotificationRoute.DefaultAgentLeaseMinutes : reader.GetInt32(17),
            reader.IsDBNull(18) ? NotificationRoute.DefaultAgentExpiryMinutes : reader.GetInt32(18),
            /* #6012: the signing key's flag and its rotation end (UTC); NULL is no key. */
            !reader.IsDBNull(19) && reader.GetBoolean(19),
            reader.IsDBNull(20) ? null : DateTime.SpecifyKind(reader.GetDateTime(20), DateTimeKind.Utc));

    private static async Task<List<Row>> ReadAllRowsAsync(DuckDBConnection connection, CancellationToken cancellationToken = default)
    {
        var rows = new List<Row>();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT {RowColumns} FROM {TableName} ORDER BY route_id";
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadRowFrom(reader));
        }

        return rows;
    }

    private static async Task<Row?> ReadRowAsync(DuckDBConnection connection, int routeId)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT {RowColumns} FROM {TableName} WHERE route_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = routeId });
        using var reader = await cmd.ExecuteReaderAsync();
        return await reader.ReadAsync() ? ReadRowFrom(reader) : null;
    }

    /// <summary>The rows hydrated from Credential Manager, all under ONE read lock: a save (write lock) cannot delete an old
    /// generation between the row being read and its key being read (#5775).</summary>
    private async Task<List<NotificationRoute>> ReadRoutesAsync(CancellationToken cancellationToken)
    {
        using var readLock = _dbInitializer.AcquireReadLock(cancellationToken);
        using var connection = _dbInitializer.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        var rows = await ReadAllRowsAsync(connection, cancellationToken);
        var routes = new List<NotificationRoute>(rows.Count);
        foreach (var row in rows)
        {
            routes.Add(Hydrate(row));
        }

        return routes;
    }

    private NotificationRoute Hydrate(Row row)
    {
        var off = new List<string>();
        string Read(int index)
        {
            if (!row.Set[index])
            {
                return "";
            }

            var channel = s_channels[index];
            var gen = row.Gens[index];
            var value = string.IsNullOrEmpty(gen) ? "" : SafeRead(SecretKey(row.RouteId, channel, gen));
            if (string.IsNullOrWhiteSpace(value))
            {
                /* Saved, but there is no key at its generation (or none was recorded) or Credential Manager gave nothing back:
                   off, not inherited (#5366). Say so once; never the URL. */
                off.Add(channel);
                if (_warnedUnreadable.TryAdd($"{row.RouteId}_{channel}_{gen}", 0))
                {
                    AppLogger.Warn("NotificationRoutes", $"Notification route {row.RouteId} has a saved {channel} destination that could not be read from Credential Manager, so that channel is off for the route until the destination is entered again.");
                }

                return "";
            }

            return value;
        }

        var teams = Read(0);
        var slack = Read(1);
        var generic = Read(2);
        var pagerDuty = Read(3);

        /* #6012 (Darling's #5998): a route's own generic URL is signed with the route's own keys. A saved key that will not open
           turns the generic channel off for this route, so its alerts are neither sent unsigned nor sent to the app-wide URL. */
        var signing = WebhookSigningKeys.None;
        if (row.SigningSet && !string.IsNullOrEmpty(generic))
        {
            signing = LiteWebhookSigningKeys.Open(
                _secrets, LiteWebhookSigningKeys.RouteName(row.RouteId, false), LiteWebhookSigningKeys.RouteName(row.RouteId, true),
                new LiteSigningState(true, row.SigningUntilUtc), DateTime.UtcNow, out var unreadable);
            if (unreadable)
            {
                generic = "";
                if (!off.Contains(NotificationRouter.GenericChannel))
                {
                    off.Add(NotificationRouter.GenericChannel);
                }

                if (_warnedUnreadable.TryAdd($"{row.RouteId}_signing", 0))
                {
                    AppLogger.Warn("NotificationRoutes", $"Notification route {row.RouteId} has a saved generic webhook signing key that could not be read from Credential Manager, so its generic channel is off until the key is made again.");
                }
            }
        }

        return new NotificationRoute(
            row.RouteId, row.MetricMatch, teams, slack, generic, pagerDuty, row.SmtpRecipients, row.Enabled)
        {
            ScopeServerId = row.ScopeServerId,
            ScopeTagId = row.ScopeTagId,
            OffChannels = off.Count == 0 ? null : off,
            AgentLevel = row.AgentLevel,
            AgentMaxPerHour = row.AgentMaxPerHour,
            AgentMaxPerDay = row.AgentMaxPerDay,
            AgentLeaseMinutes = row.AgentLeaseMinutes,
            AgentExpiryMinutes = row.AgentExpiryMinutes,
            GenericSigning = signing
        };
    }

    /// <summary>
    /// Makes a signing key for a route's own generic URL, or rotates it (#6012, Darling's #5998 D2 and D3): the current key
    /// becomes the previous one for <see cref="WebhookSigning.RotationOverlap"/>. Returns the new key, to be shown once, or a
    /// sentence saying why not (no such route, no saved generic URL, Credential Manager refused). The key goes only to Credential
    /// Manager; the row keeps the flag and the rotation's end. A URL change keeps the key: Credential Manager does not bind a key
    /// to the URL text as Darling's seal does.
    /// </summary>
    public async Task<(string? Key, string? Error)> GenerateSigningKeyAsync(int routeId)
    {
        string? key;
        string? error;
        using (var writeLock = _dbInitializer.AcquireWriteLock())
        {
            PreservedTableRestore.ThrowIfRestorePending(_dbInitializer.ArchivePath, TableName);
            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();
            var current = await ReadRowAsync(connection, routeId);
            if (current is null)
            {
                return (null, "This route no longer exists.");
            }

            if (!current.Set[Array.IndexOf(s_channels, NotificationRouter.GenericChannel)])
            {
                return (null, "Save a generic webhook URL on this route first. A key signs the route's own generic URL.");
            }

            var result = LiteWebhookSigningKeys.Generate(
                _secrets, LiteWebhookSigningKeys.RouteName(routeId, false), LiteWebhookSigningKeys.RouteName(routeId, true),
                new LiteSigningState(current.SigningSet, current.SigningUntilUtc), DateTime.UtcNow);
            key = result.NewKey;
            error = result.Error;
            if (key is not null)
            {
                await WriteSigningFlagAsync(connection, routeId, result.State);
            }
        }

        NotificationRouteCache.Invalidate();
        return (key, error);
    }

    /// <summary>Removes a route's signing keys; its generic URL is sent unsigned from then on. Null when done, else why not.</summary>
    public async Task<string?> RemoveSigningKeyAsync(int routeId)
    {
        string? error;
        using (var writeLock = _dbInitializer.AcquireWriteLock())
        {
            PreservedTableRestore.ThrowIfRestorePending(_dbInitializer.ArchivePath, TableName);
            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();
            var current = await ReadRowAsync(connection, routeId);
            if (current is null)
            {
                return "This route no longer exists.";
            }

            var state = LiteWebhookSigningKeys.Remove(
                _secrets, LiteWebhookSigningKeys.RouteName(routeId, false), LiteWebhookSigningKeys.RouteName(routeId, true),
                new LiteSigningState(current.SigningSet, current.SigningUntilUtc), out error);
            if (error is null)
            {
                await WriteSigningFlagAsync(connection, routeId, state);
            }
        }

        NotificationRouteCache.Invalidate();
        return error;
    }

    private static async Task WriteSigningFlagAsync(DuckDBConnection connection, int routeId, LiteSigningState state)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"UPDATE {TableName} SET generic_signing_set = $2, generic_signing_until = $3 WHERE route_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = routeId });
        cmd.Parameters.Add(new DuckDBParameter { Value = state.HasKey });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)state.PreviousUntilUtc ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
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

        /* #5820: the bounds of Darling's V184 CHECK, said in words before the statement runs. */
        if (route.AgentLevel is { } level && level is not (NotificationRoute.AgentLevelOff or NotificationRoute.AgentLevelInvestigate))
        {
            throw new ArgumentException("The agent channel is Inherit, Off or Investigate.", nameof(route));
        }

        CheckAgentLimit("Requests per hour", route.AgentMaxPerHour, NotificationRoute.MinAgentMaxPerHour, NotificationRoute.MaxAgentMaxPerHour);
        CheckAgentLimit("Requests per day", route.AgentMaxPerDay, NotificationRoute.MinAgentMaxPerDay, NotificationRoute.MaxAgentMaxPerDay);
        CheckAgentLimit("Lease minutes", route.AgentLeaseMinutes, NotificationRoute.MinAgentLeaseMinutes, NotificationRoute.MaxAgentLeaseMinutes);
        CheckAgentLimit("Expiry minutes", route.AgentExpiryMinutes, NotificationRoute.MinAgentExpiryMinutes, NotificationRoute.MaxAgentExpiryMinutes);

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

    private static void CheckAgentLimit(string name, int value, int min, int max)
    {
        if (value < min || value > max)
        {
            throw new ArgumentException(string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{name} must be from {min} to {max}."));
        }
    }

    private static readonly IReadOnlySet<string> NoChannels = new HashSet<string>(StringComparer.Ordinal);

    /// <summary>What a save does with one channel (#5775). <see cref="Gen"/> is the generation the row will hold, null when the
    /// channel is not set; <see cref="OldGen"/> is the one it holds now. <see cref="Replace"/> is true only when a value is
    /// written under a new generation: a kept channel and a value equal to the one already stored keep their generation, so
    /// <see cref="Gen"/> equals <see cref="OldGen"/>.</summary>
    private sealed record ChannelPlan(string Channel, string Value, bool Replace, string? OldGen, string? Gen, bool Flag);

    private List<ChannelPlan> BuildPlans(Row? current, NotificationRoute r, IReadOnlySet<string> keep)
    {
        string[] values = [r.TeamsUrl, r.SlackUrl, r.GenericUrl, r.PagerDutyRoutingKey];
        var plans = new List<ChannelPlan>(s_channels.Length);
        for (var i = 0; i < s_channels.Length; i++)
        {
            var channel = s_channels[i];
            var value = values[i];
            var oldSet = current?.Set[i] ?? false;
            var oldGen = current?.Gens[i];
            if (value.Length == 0)
            {
                /* Left alone because it was unreadable: no copy is possible, so it keeps its flag and generation. */
                plans.Add(keep.Contains(channel)
                    ? new ChannelPlan(channel, "", false, oldGen, oldGen, true)
                    : new ChannelPlan(channel, "", false, oldGen, null, false));
            }
            else if (current is not null && oldSet && !string.IsNullOrEmpty(oldGen)
                && string.Equals(SafeRead(SecretKey(current.RouteId, channel, oldGen)), value, StringComparison.Ordinal))
            {
                /* The stored value already is this one: nothing to write, nothing to delete. */
                plans.Add(new ChannelPlan(channel, value, false, oldGen, oldGen, true));
            }
            else
            {
                plans.Add(new ChannelPlan(channel, value, true, oldGen, NewGeneration(), true));
            }
        }

        return plans;
    }

    /// <summary>Step 1 (#5775): writes each replaced value under its new generation and reads it back. On any failure the keys
    /// written so far (the failing one included, since a partial write can leave an entry) are deleted leniently and a
    /// <see cref="RouteSecretException"/> naming the channel is thrown; the row and the old keys are untouched. Returns the
    /// keys written so a failed row write can remove them.</summary>
    private List<(string Channel, string Key)> WriteNewKeys(int routeId, List<ChannelPlan> plans)
    {
        var written = new List<(string Channel, string Key)>();
        foreach (var plan in plans.Where(p => p.Replace))
        {
            var key = SecretKey(routeId, plan.Channel, plan.Gen!);
            written.Add((plan.Channel, key));
            /* The reason is Credential Manager's own (CredentialSaveResult.Failure), which never carries the value; the two
               reasons built here do not either. */
            string? failure;
            try
            {
                failure = _secrets.Write(key, plan.Value);
                if (failure is null && !string.Equals(_secrets.Read(key), plan.Value, StringComparison.Ordinal))
                {
                    failure = "it did not read back the same as it was written";
                }
            }
            catch (Exception)
            {
                failure = "Windows Credential Manager could not be reached";
            }

            if (failure is not null)
            {
                DeleteKeysLeniently(routeId, written, "new");
                throw new RouteSecretException(plan.Channel, failure);
            }
        }

        return written;
    }

    /// <summary>Step 2 failed with <paramref name="error"/> (#5775). Reads the row again to learn whether the statement
    /// committed. Committed (the row holds what was meant to be written): returns true, the caller goes on to delete the old
    /// keys, and nothing is rolled back. Not committed: deletes the new keys and returns false, and the caller rethrows. The
    /// row cannot be read: returns false and deletes nothing, because neither side is known to be garbage; the sweep at the
    /// next start keeps whichever keys the row points at.</summary>
    private async Task<bool> RowCommittedAsync(
        DuckDBConnection connection, int routeId, NotificationRoute r, List<ChannelPlan> plans,
        List<(string Channel, string Key)> written, Exception error)
    {
        bool? committed;
        try
        {
            var row = await ReadRowAsync(connection, routeId);
            committed = row is not null && RowHolds(row, r, plans);
        }
        catch (Exception)
        {
            committed = null;
        }

        if (committed == true)
        {
            AppLogger.Warn("NotificationRoutes", $"Notification route {routeId}: the row write reported an error but the row holds the new values, so the save is kept: {error.Message}");
            return true;
        }

        if (committed == false)
        {
            DeleteKeysLeniently(routeId, written, "new");
        }
        else
        {
            AppLogger.Warn("NotificationRoutes", $"Notification route {routeId}: the row write failed and the row could not be read again, so the new credentials are left for the next start to sort out: {error.Message}");
        }

        return false;
    }

    private static bool RowHolds(Row row, NotificationRoute r, List<ChannelPlan> plans)
    {
        if (row.MetricMatch != r.MetricMatch || row.ScopeServerId != r.ScopeServerId || row.ScopeTagId != r.ScopeTagId
            || row.SmtpRecipients != r.SmtpRecipients || row.Enabled != r.Enabled
            || row.AgentLevel != r.AgentLevel || row.AgentMaxPerHour != r.AgentMaxPerHour || row.AgentMaxPerDay != r.AgentMaxPerDay
            || row.AgentLeaseMinutes != r.AgentLeaseMinutes || row.AgentExpiryMinutes != r.AgentExpiryMinutes)
        {
            return false;
        }

        for (var i = 0; i < plans.Count; i++)
        {
            if (row.Set[i] != plans[i].Flag || !string.Equals(row.Gens[i], plans[i].Gen, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static void AddWriteParameters(DuckDBCommand cmd, int id, NotificationRoute r, List<ChannelPlan> plans)
    {
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.MetricMatch });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.ScopeServerId ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.ScopeTagId ?? DBNull.Value });
        foreach (var plan in plans)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = plan.Flag });
        }

        cmd.Parameters.Add(new DuckDBParameter { Value = r.SmtpRecipients });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.Enabled });
        foreach (var plan in plans)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)plan.Gen ?? DBNull.Value });
        }

        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.AgentLevel ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.AgentMaxPerHour });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.AgentMaxPerDay });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.AgentLeaseMinutes });
        cmd.Parameters.Add(new DuckDBParameter { Value = r.AgentExpiryMinutes });
    }

    /// <summary>Deletes keys one by one; a key that will not go is counted, not thrown. One warning names the channels (never a
    /// value or a key): what is left is not read by any route and is removed by the sweep at the next start.</summary>
    private void DeleteKeysLeniently(int routeId, IEnumerable<(string Channel, string Key)> keys, string which)
    {
        var failed = new List<string>();
        foreach (var (channel, key) in keys)
        {
            if (!SafeDelete(key))
            {
                failed.Add(channel);
            }
        }

        if (failed.Count > 0)
        {
            AppLogger.Warn("NotificationRoutes", $"Notification route {routeId}: the {which} {string.Join(", ", failed.Distinct())} credential(s) could not be removed from Credential Manager. No route reads them, and they are removed at the next start.");
        }
    }

    private static string ChannelOfKey(string key) =>
        s_channels.FirstOrDefault(c => key.Contains("_" + c + "_", StringComparison.Ordinal)) ?? "unknown";

    private string SafeRead(string key)
    {
        try
        {
            return _secrets.Read(key) ?? "";
        }
        catch (Exception)
        {
            return "";
        }
    }

    private bool SafeDelete(string key)
    {
        try
        {
            return _secrets.Delete(key);
        }
        catch (Exception)
        {
            return false;
        }
    }
}


/// <summary>Windows Credential Manager would not store a route destination, so the route was not saved (#5750). The
/// message names the channel and never the value. The edit dialog shows it inline.</summary>
public sealed class RouteSecretException : InvalidOperationException
{
    /// <param name="channel">The channel whose destination was refused.</param>
    /// <param name="reason">Why, as <see cref="PerformanceMonitor.Common.CredentialSaveResult.Failure"/> words it (for a value over
    /// <see cref="PerformanceMonitor.Common.CredentialStore.MaxSecretLength"/> characters it says how long it is and what the limit is).
    /// Never the value.</param>
    public RouteSecretException(string channel, string reason)
        : base($"The {channel} destination was not saved to Windows Credential Manager because {reason}, so the route was not changed.")
    {
    }
}
