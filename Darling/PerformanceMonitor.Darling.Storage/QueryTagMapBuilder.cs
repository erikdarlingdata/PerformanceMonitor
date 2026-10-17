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
/// <para><b>The cursor is in memory, and a server without one rebuilds.</b> A server with no cursor (a fresh process, a
/// newly listed server) gets the same one-transaction rebuild a rule change gets, then a cursor two minutes behind the
/// clock. It does not drain from the start of time: a restart would otherwise re-read the whole sighting history, one
/// aggregate-and-sort statement per page, once a minute until it finished, to re-derive a map that was already right.</para>
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

    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;
    private readonly int _keysPerServerPerPass;
    private readonly QueryTagSightingBuffer? _sightings;
    private readonly Dictionary<int, Cursor> _cursors = new();

    /// <param name="sightings">The buffer the collectors append to; each pass flushes it first. Null for a builder that only
    /// reads sightings other code wrote.</param>
    public QueryTagMapBuilder(NpgsqlDataSource postgres, ILogger? logger = null, int keysPerServerPerPass = MapKeysPerServerPerPass, QueryTagSightingBuffer? sightings = null)
    {
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
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            snapshot = await QueryTagMapStore.ReadSnapshotAsync(connection, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogWarning("Query-tag map: reading the rules and version failed; skipping this pass: {Message}", ex.Message);
            return false;
        }

        bool rebuildAll = snapshot.RulesVersion > snapshot.BuiltVersion;
        bool allSucceeded = true;
        foreach (int serverId in serverIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool rebuild = rebuildAll || !_cursors.ContainsKey(serverId);
            try
            {
                await using var connection = await _postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
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
            }
        }

        if (rebuildAll && allSucceeded)
        {
            try
            {
                await using var connection = await _postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                await QueryTagMapStore.StampBuiltVersionAsync(connection, snapshot.RulesVersion, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                allSucceeded = false;
                _logger?.LogWarning("Query-tag map: stamping built_version {Version} failed; the next pass rebuilds again: {Message}", snapshot.RulesVersion, ex.Message);
            }
        }

        return allSucceeded;
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

        await QueryTagMapStore.ReplaceMapAsync(connection, serverId, rows, cancellationToken).ConfigureAwait(false);
        _cursors[serverId] = new Cursor(clock - ExtendOverlap, string.Empty, string.Empty);
    }

    private async Task ExtendServerAsync(NpgsqlConnection connection, int serverId, QueryTagMapSnapshot snapshot, CancellationToken cancellationToken)
    {
        /* RunMapPassAsync rebuilds a server with no cursor, so by here it has one. */
        var cursor = _cursors[serverId];
        DateTime clock = await QueryTagMapStore.ClockAsync(connection, cancellationToken).ConfigureAwait(false);

        var changed = await QueryTagMapStore.ReadChangedKeysAsync(
            connection, serverId, cursor.Since, cursor.AfterKind, cursor.AfterKey, _keysPerServerPerPass, cancellationToken).ConfigureAwait(false);
        if (changed.Count > 0)
        {
            var sightingsByKey = await QueryTagMapStore.ReadSightingsOfKeysAsync(connection, serverId, changed, cancellationToken).ConfigureAwait(false);
            var rows = new List<QueryTagMapRow>();
            foreach (var key in changed)
            {
                if (sightingsByKey.TryGetValue((key.KeyKind, key.Key), out var sightings))
                {
                    AddResolved(rows, serverId, sightings, snapshot);
                }
            }

            await QueryTagMapStore.UpsertMapAsync(connection, serverId, rows, cancellationToken).ConfigureAwait(false);
        }

        /* A full page means more may wait: step past the last key. A short page drained everything up to the clock. */
        _cursors[serverId] = changed.Count >= _keysPerServerPerPass
            ? new Cursor(changed[^1].LastSeen, changed[^1].KeyKind, changed[^1].Key)
            : new Cursor(clock - ExtendOverlap, string.Empty, string.Empty);
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

    private readonly record struct Cursor(DateTime Since, string AfterKind, string AfterKey);
}
