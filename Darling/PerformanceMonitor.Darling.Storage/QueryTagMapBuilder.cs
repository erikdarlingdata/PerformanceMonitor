/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The query-tag map step (#5985, part of #5971): resolves the sighted query keys to tags and keeps
/// <c>config.query_tag_map</c>, the one table the tag reads join. Nothing evaluates a rule at read time.
///
/// <para><b>A pass flushes, then rebuilds or extends.</b> The pass first flushes the sightings buffer
/// (<see cref="QueryTagSightingBuffer"/>) in one upsert, so it tags what the last minute sighted. Then, when
/// <c>rules_version</c> is ahead of <c>built_version</c> (any write to the tags, assignments or rules bumps it) it
/// rebuilds every listed server's map from all of its sightings, one transaction per server
/// (<see cref="QueryTagMapStore.ReplaceMapAsync"/>: a read sees the old map or the new one, never half), then stamps
/// <c>built_version</c> with the <c>rules_version</c> it read at the start, so a rule written mid-rebuild leaves the map
/// stale and the next pass rebuilds again. Otherwise the pass extends: it resolves only the keys whose sightings changed
/// since this server's cursor, at most <see cref="MapKeysPerServerPerPass"/> a pass, and the remainder waits for the
/// next one. A key with no sightings left (retention removed them) leaves the map at the next rebuild; the extend path
/// never deletes.</para>
///
/// <para><b>The cursor is in memory, and a copy is stored (#9308).</b> A server with no in-memory cursor (a fresh process,
/// a newly listed server) first looks for the cursor the last process stored, and extends from it when the map it
/// describes is still current: its <c>rules_version</c> equals the current one, <c>built_version</c> equals it too, and
/// the cursor is under <see cref="MaxStoredCursorAge"/> old. Otherwise it gets the one-transaction rebuild a rule change
/// gets, then a cursor two minutes behind the clock. It never drains from the start of time: that would re-read the whole
/// sighting history, one aggregate-and-sort statement per page, once a minute until it finished. Each rebuild and each
/// extend stores its cursor in the transaction of the map write it describes
/// (<see cref="QueryTagMapStore.ReplaceMapAsync"/>, <see cref="QueryTagMapStore.UpsertMapAsync"/>), so a stored cursor is never
/// ahead of the map, and a rebuild-all stamp drops the stored cursor of every server outside the pass.</para>
///
/// <para><b>A weekly staggered rebuild trims the map (#9308).</b> A process that extends from a stored cursor never
/// rebuilds on its own, and only a rebuild removes a key whose sightings retention deleted or re-resolves a key whose
/// winning sighting retention deleted. So each cursor also carries <c>rebuilt_at</c>, the store clock at the server's last
/// rebuild (an extend carries it forward). A server whose last rebuild is <see cref="MaxRebuildAge"/> old is due, and a
/// pass rebuilds at most ONE due server, the one with the oldest <c>rebuilt_at</c>, never within
/// <see cref="RebuildStartupGrace"/> of the builder's construction, so the minutes after a start stay clear (#9302,
/// #9303). A due server that is skipped extends as usual. A cursor stored before this field decodes as due, and the
/// one-a-pass limit spreads those out. A rebuild-all ignores all of this.</para>
///
/// <para><b>A failed age rebuild backs off and the server still extends (#9308).</b> A rebuild that throws leaves the
/// server's cursor and <c>rebuilt_at</c> as they were, so it would be the oldest due server again a minute later, and
/// every pass would take the rebuild path and never extend it: its tags would freeze while a failing all-sightings read
/// ran once a minute. So when the rebuild the age rule picked fails, the server is not picked for age for
/// <see cref="AgeRebuildBackoff"/> (store clock, held in memory), the slot goes to the next due server, and the pass
/// runs the server's extend from its intact cursor. A rebuild that succeeds clears the back-off. A rebuild from a failed
/// stored-cursor adopt and a rebuild-all are not age rebuilds and do not back off.</para>
///
/// <para><b>Caller carry-forward (decision 3 of #5971).</b> A hash key is resolved against every sighting row it has,
/// each carrying the application, login and host it was seen under, so a rule on application X tags a hash seen under
/// X. A hand assignment wins over a rule (the resolver's precedence).</para>
/// </summary>
public sealed class QueryTagMapBuilder
{
    /// <summary>
    /// The most keys one pass resolves for one server. A pass reads the sighting rows of these keys (a few each) and
    /// runs every enabled rule over them, which at 5000 keys is tens of milliseconds to a couple of seconds and one
    /// bounded upsert, well inside a pass interval. The cap pages the extend, which reads only what changed after the
    /// cursor (the last two minutes, or the tail of a burst), never the whole history: a server's whole history is read
    /// by a rebuild, in one transaction. It stays at half of <see cref="QueryTagLimits.MaxKeysPerServerPerPass"/>, the ceiling
    /// both stores share, so neither store's pass can outgrow the other's. The value is threaded into the SQL as a
    /// parameter (#3684), never written there.
    /// </summary>
    public const int MapKeysPerServerPerPass = 5000;

    /// <summary>
    /// How far behind a pass's start the next extend looks again. A sighting's <c>last_seen</c> is its statement's
    /// start, and its commit can land after a pass has read the clock; re-resolving the last two minutes is idempotent
    /// and closes that gap.
    /// </summary>
    public static readonly TimeSpan ExtendOverlap = TimeSpan.FromMinutes(2);

    /// <summary>
    /// #9308: the oldest stored cursor a fresh process extends from. An older one rebuilds, so a long outage does not turn
    /// into a page-at-a-time drain of everything sighted meanwhile.
    /// </summary>
    public static readonly TimeSpan MaxStoredCursorAge = TimeSpan.FromHours(24);

    /// <summary>#9308: how old a server's last rebuild may get before the weekly staggered rebuild takes it.</summary>
    public static readonly TimeSpan MaxRebuildAge = TimeSpan.FromDays(7);

    /// <summary>#9308: the time after the builder is constructed in which the staggered rebuild never runs.</summary>
    public static readonly TimeSpan RebuildStartupGrace = TimeSpan.FromMinutes(20);

    /// <summary>#9308: how long a server whose age-picked rebuild failed is left out of the age pick, on the store clock.</summary>
    public static readonly TimeSpan AgeRebuildBackoff = TimeSpan.FromHours(1);

    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;
    private readonly TimeSpan _maxRebuildAge;
    private readonly TimeSpan _rebuildStartupGrace;
    private readonly TimeSpan _ageRebuildBackoff;
    private readonly Dictionary<int, DateTime> _ageBackoffUntil = new();
    private readonly Stopwatch _sinceConstruction = Stopwatch.StartNew();
    private readonly int _keysPerServerPerPass;
    private readonly QueryTagSightingBuffer? _sightings;
    private readonly Dictionary<int, Cursor> _cursors = new();

    /// <param name="sightings">The buffer the collectors append to; each pass flushes it first. Null for a builder that only
    /// reads sightings other code wrote.</param>
    public QueryTagMapBuilder(NpgsqlDataSource postgres, ILogger? logger = null, int keysPerServerPerPass = MapKeysPerServerPerPass, QueryTagSightingBuffer? sightings = null,
        TimeSpan? maxRebuildAge = null, TimeSpan? rebuildStartupGrace = null, TimeSpan? ageRebuildBackoff = null)
    {
        _maxRebuildAge = maxRebuildAge ?? MaxRebuildAge;
        _rebuildStartupGrace = rebuildStartupGrace ?? RebuildStartupGrace;
        _ageRebuildBackoff = ageRebuildBackoff ?? AgeRebuildBackoff;
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
        _sightings = sightings;
        _keysPerServerPerPass = keysPerServerPerPass > 0 ? keysPerServerPerPass : MapKeysPerServerPerPass;
    }

    /// <summary>
    /// Flushes the sightings buffer, then runs one pass over <paramref name="serverIds"/>. Failure-isolated per server: a
    /// server whose step fails logs, keeps its previous map, and leaves <c>built_version</c> unstamped so the next pass
    /// tries again. A failed flush is logged inside the buffer and does not stop the pass.
    /// </summary>
    /// <returns>True when every server's step succeeded.</returns>
    public async Task<bool> RunMapPassAsync(IReadOnlyCollection<int> serverIds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(serverIds);

        if (_sightings is not null)
        {
            await _sightings.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        QueryTagMapSnapshot snapshot;
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(StoreSessionNames.QueryTags, cancellationToken).ConfigureAwait(false);
            snapshot = await QueryTagMapStore.ReadSnapshotAsync(connection, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogWarning("Query-tag map: reading the rules and version failed; skipping this pass: {Message}", ex.Message);
            return false;
        }

        bool rebuildAll = snapshot.RulesVersion > snapshot.BuiltVersion;
        bool allSucceeded = true;

        /* Which servers rebuild this pass, decided before any of them runs so the staggered rebuild can pick the oldest. */
        var rebuilds = new HashSet<int>();
        var failedAdopt = new HashSet<int>();
        int? ageRebuild = null;
        DateTime ageClock = default;
        if (!rebuildAll)
        {
            foreach (int serverId in serverIds)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_cursors.ContainsKey(serverId))
                {
                    continue;
                }

                try
                {
                    await using var connection = await _postgres.OpenConnectionAsync(StoreSessionNames.QueryTags, cancellationToken).ConfigureAwait(false);
                    if (!await TryAdoptStoredCursorAsync(connection, serverId, snapshot, cancellationToken).ConfigureAwait(false))
                    {
                        rebuilds.Add(serverId);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    allSucceeded = false;
                    failedAdopt.Add(serverId);
                    _logger?.LogWarning("Query-tag map: the extend for server {ServerId} failed and its previous map stays: {Message}", serverId, ex.Message);
                }
            }

            if (_sinceConstruction.Elapsed >= _rebuildStartupGrace)
            {
                try
                {
                    var due = await PickOldestDueServerAsync(serverIds.Where(id => !rebuilds.Contains(id) && !failedAdopt.Contains(id)), cancellationToken).ConfigureAwait(false);
                    if (due is var (dueServer, clock))
                    {
                        rebuilds.Add(dueServer);
                        ageRebuild = dueServer;
                        ageClock = clock;
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    allSucceeded = false;
                    _logger?.LogWarning("Query-tag map: reading the clock for the staggered rebuild failed; no server is rebuilt for age this pass: {Message}", ex.Message);
                }
            }
        }

        foreach (int serverId in serverIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (failedAdopt.Contains(serverId))
            {
                continue;
            }

            bool rebuild = rebuildAll || rebuilds.Contains(serverId);
            bool agePicked = rebuild && !rebuildAll && ageRebuild == serverId;
            try
            {
                await using var connection = await _postgres.OpenConnectionAsync(StoreSessionNames.QueryTags, cancellationToken).ConfigureAwait(false);
                if (rebuild)
                {
                    await RebuildServerAsync(connection, serverId, snapshot, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await ExtendServerAsync(connection, serverId, snapshot, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                allSucceeded = false;
                _logger?.LogWarning(
                    "Query-tag map: the {Step} for server {ServerId} failed and its previous map stays: {Message}",
                    rebuild ? "rebuild" : "extend", serverId, ex.Message);

                if (agePicked)
                {
                    /* The cursor is intact (a rebuild moves it only after its map write commits), so extend from it now and keep
                       this server out of the age pick for a while: the next pass hands the slot to the next due server. */
                    _ageBackoffUntil[serverId] = ageClock + _ageRebuildBackoff;
                    try
                    {
                        await using var connection = await _postgres.OpenConnectionAsync(StoreSessionNames.QueryTags, cancellationToken).ConfigureAwait(false);
                        await ExtendServerAsync(connection, serverId, snapshot, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception extendEx) when (extendEx is not OperationCanceledException)
                    {
                        _logger?.LogWarning("Query-tag map: the extend for server {ServerId} after its failed rebuild also failed and its previous map stays: {Message}", serverId, extendEx.Message);
                    }
                }
            }
        }

        /* A rebuild-all pass stamps the version it read. A server that was NOT in this pass (disabled, say, or the list was
           empty) was not rebuilt, and after the stamp rebuildAll is false, so when it returns it would be extended: its map
           rows would keep the tags of the old rules while the state row says the map is current (#5971). So the stamp also
           drops the cursor of every server outside this pass, and each of them is rebuilt (no cursor) when it returns. That
           is also why an empty list may stamp: no map is claimed current that was not built, which matches Lite, whose pass
           covers every server that has sightings. The in-memory cursors go FIRST: if the stamp or the drop then fails, the
           next pass is rebuild-all anyway, but a cursor kept after a committed stamp would extend a returning server over map
           rows tagged under the old rules. The stamp and the stored-cursor drop commit in ONE transaction, so a failed drop
           leaves the stamp unwritten (and a stored cursor that outlived it carries the old rules_version, which the adopt
           check rejects). A failed stamp changes nothing: the next pass is rebuild-all again. */
        if (rebuildAll && allSucceeded)
        {
            try
            {
                var inPass = new HashSet<int>(serverIds);
                foreach (int cursorServer in _cursors.Keys.Where(id => !inPass.Contains(id)).ToList())
                {
                    _cursors.Remove(cursorServer);
                }

                await using var connection = await _postgres.OpenConnectionAsync(StoreSessionNames.QueryTags, cancellationToken).ConfigureAwait(false);
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                await QueryTagMapStore.StampBuiltVersionAsync(connection, snapshot.RulesVersion, cancellationToken, transaction).ConfigureAwait(false);
                await QueryTagMapStore.DropCursorsOutsideAsync(connection, serverIds, cancellationToken, transaction).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                allSucceeded = false;
                _logger?.LogWarning("Query-tag map: stamping built_version {Version} failed; the next pass rebuilds again: {Message}", snapshot.RulesVersion, ex.Message);
            }
        }

        return allSucceeded;
    }

    /// <summary>
    /// The listed server with the oldest <c>rebuilt_at</c> among those whose last rebuild is at least
    /// <see cref="MaxRebuildAge"/> old on the store clock, or null when none is due. Only servers with a cursor count: a server
    /// with none rebuilds this pass anyway.
    /// </summary>
    private async Task<(int ServerId, DateTime Clock)?> PickOldestDueServerAsync(IEnumerable<int> serverIds, CancellationToken cancellationToken)
    {
        var candidates = serverIds.Where(_cursors.ContainsKey).Distinct().ToList();
        if (candidates.Count == 0)
        {
            return null;
        }

        await using var connection = await _postgres.OpenConnectionAsync(StoreSessionNames.QueryTags, cancellationToken).ConfigureAwait(false);
        DateTime clock = await QueryTagMapStore.ClockAsync(connection, cancellationToken).ConfigureAwait(false);
        int? oldest = null;
        DateTime oldestRebuiltAt = DateTime.MaxValue;
        foreach (int serverId in candidates)
        {
            DateTime rebuiltAt = _cursors[serverId].RebuiltAt;
            if (_ageBackoffUntil.TryGetValue(serverId, out DateTime until))
            {
                if (until > clock)
                {
                    continue;
                }

                _ageBackoffUntil.Remove(serverId);
            }

            if (clock - rebuiltAt >= _maxRebuildAge && rebuiltAt < oldestRebuiltAt)
            {
                oldest = serverId;
                oldestRebuiltAt = rebuiltAt;
            }
        }

        return oldest is { } picked ? (picked, clock) : null;
    }

    private async Task RebuildServerAsync(NpgsqlConnection connection, int serverId, QueryTagMapSnapshot snapshot, CancellationToken cancellationToken)
    {
        /* The clock is read before the sightings, so a sighting written while the rebuild runs is at or after the cursor. */
        DateTime clock = await QueryTagMapStore.ClockAsync(connection, cancellationToken).ConfigureAwait(false);

        var rows = new List<QueryTagMapRow>();
        await foreach (var sightings in QueryTagMapStore.ReadAllSightingsByKeyAsync(connection, serverId, cancellationToken).ConfigureAwait(false))
        {
            AddResolved(rows, serverId, sightings, snapshot);
        }

        var next = new Cursor(clock - ExtendOverlap, string.Empty, string.Empty, clock);
        await QueryTagMapStore.ReplaceMapAsync(connection, serverId, rows, cancellationToken, Stored(next, snapshot)).ConfigureAwait(false);
        _cursors[serverId] = next;
        _ageBackoffUntil.Remove(serverId);
    }

    /// <summary>
    /// A server with no in-memory cursor takes the cursor the last process stored, when every condition says the map on disk is
    /// the one that cursor describes (#9308). False means rebuild.
    /// </summary>
    private async Task<bool> TryAdoptStoredCursorAsync(NpgsqlConnection connection, int serverId, QueryTagMapSnapshot snapshot, CancellationToken cancellationToken)
    {
        if (snapshot.BuiltVersion != snapshot.RulesVersion)
        {
            return false;
        }

        var stored = await QueryTagMapStore.ReadCursorAsync(connection, serverId, cancellationToken).ConfigureAwait(false);
        if (stored is null || stored.RulesVersion != snapshot.RulesVersion)
        {
            return false;
        }

        DateTime clock = await QueryTagMapStore.ClockAsync(connection, cancellationToken).ConfigureAwait(false);
        if (clock - stored.Since >= MaxStoredCursorAge)
        {
            return false;
        }

        _cursors[serverId] = new Cursor(stored.Since, stored.AfterKind, stored.AfterKey, stored.RebuiltAt);
        return true;
    }

    private static QueryTagMapStoredCursor Stored(Cursor cursor, QueryTagMapSnapshot snapshot) =>
        new(snapshot.RulesVersion, cursor.Since, cursor.AfterKind, cursor.AfterKey, cursor.RebuiltAt);

    private async Task ExtendServerAsync(NpgsqlConnection connection, int serverId, QueryTagMapSnapshot snapshot, CancellationToken cancellationToken)
    {
        /* RunMapPassAsync rebuilds a server with no cursor, so by here it has one. */
        var cursor = _cursors[serverId];
        DateTime clock = await QueryTagMapStore.ClockAsync(connection, cancellationToken).ConfigureAwait(false);

        var changed = await QueryTagMapStore.ReadChangedKeysAsync(
            connection, serverId, cursor.Since, cursor.AfterKind, cursor.AfterKey, _keysPerServerPerPass, cancellationToken).ConfigureAwait(false);
        var rows = new List<QueryTagMapRow>();
        if (changed.Count > 0)
        {
            var sightingsByKey = await QueryTagMapStore.ReadSightingsOfKeysAsync(connection, serverId, changed, cancellationToken).ConfigureAwait(false);
            foreach (var key in changed)
            {
                if (sightingsByKey.TryGetValue((key.KeyKind, key.Key), out var sightings))
                {
                    AddResolved(rows, serverId, sightings, snapshot);
                }
            }
        }

        /* A full page means more may wait: step past the last key. A short page drained everything up to the clock. */
        var next = changed.Count >= _keysPerServerPerPass
            ? new Cursor(changed[^1].LastSeen, changed[^1].KeyKind, changed[^1].Key, cursor.RebuiltAt)
            : new Cursor(clock - ExtendOverlap, string.Empty, string.Empty, cursor.RebuiltAt);
        await QueryTagMapStore.UpsertMapAsync(connection, serverId, rows, cancellationToken, Stored(next, snapshot)).ConfigureAwait(false);
        _cursors[serverId] = next;
    }

    private static void AddResolved(List<QueryTagMapRow> rows, int serverId, List<QueryTagSighting> sightings, QueryTagMapSnapshot snapshot)
    {
        var resolution = QueryTagResolver.Resolve(serverId, sightings, snapshot.Assignments, snapshot.Rules, snapshot.TagIdsByName);
        if (!resolution.HasTag)
        {
            return;
        }

        var first = sightings[0];
        rows.Add(new QueryTagMapRow(
            QueryTagSightingWriter.KindText(first.KeyKind),
            first.Key,
            checked((int)resolution.TagId!.Value),
            QueryTagMapStore.SourceText(resolution.Source),
            resolution.RuleId is { } ruleId ? checked((int)ruleId) : null,
            resolution.MatchedField is { } field ? QueryTagMapStore.FieldText(field) : null,
            resolution.MatchedValue));
    }

    private readonly record struct Cursor(DateTime Since, string AfterKind, string AfterKey, DateTime RebuiltAt);
}
