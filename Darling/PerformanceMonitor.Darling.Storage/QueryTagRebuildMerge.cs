/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>What <see cref="QueryTagRebuildMerge.Finish"/> gives: the map rows of the keys whose tag no row order can change, and
/// the keys that must be resolved from their rows in order.</summary>
internal sealed record QueryTagRebuildMergeResult(IReadOnlyList<QueryTagMapRow> Rows, IReadOnlyList<QueryTagChangedKey> KeysNeedingOrder);

/// <summary>
/// Resolves one server's rows of the two sightings tables as they arrive, in any order (#9705). Pure: no store, no I/O. A
/// rebuild reads both tables in heap order, which is no key order, so it cannot hand the resolver one key's rows as an ordered
/// list. It hands each row to <see cref="AddSlim"/> or <see cref="Add"/> instead and holds one small state per tagged key.
/// <para>A key that has a sighting is resolved from its sightings alone, and a key with none from its slim rows. The slim rows
/// come first: each tagged key gets a state. Then the sightings: the first sighting of a key drops the state its slim rows
/// left, tagged or not, so no slim row counts for a key that has a sighting and no list of the sighted keys is held.</para>
/// <para>Why resolving row by row gives the tag
/// <see cref="QueryTagResolver.Resolve(int, IReadOnlyList{QueryTagSighting}, IReadOnlyList{QueryTagHandAssignment}, IReadOnlyList{QueryTagRule}, IReadOnlyDictionary{string, long})"/>
/// gives over the key's whole ordered row list: the resolver returns the hit of the lowest precedence step that any row
/// matches, on the first row in order that matches it. Each row is resolved alone here, with the number of the step that hit
/// (its rank). The rows that match the key's winning step are exactly the rows whose own rank is the lowest one seen. When
/// they all give an equal resolution, the order cannot change the answer and the key is done. When they differ (a pattern
/// that matched two values, a tag comment that named two tags), the first row in order decides. For sightings that order is
/// the store's (<see cref="QueryTagMapStore.SightingsOfKeysSql"/>), so the key is listed in
/// <see cref="QueryTagRebuildMergeResult.KeysNeedingOrder"/> and the builder reads that key's sightings in order. Slim rows
/// have no key index to read them back through, and no text sample, so the merge keeps the first of them itself
/// (<see cref="SlimRowOrder"/>).</para>
/// </summary>
internal sealed class QueryTagRebuildMerge
{
    private readonly int _serverId;
    private readonly IReadOnlyList<QueryTagHandAssignment> _assignments;
    private readonly IReadOnlyList<QueryTagRule> _rules;
    private readonly IReadOnlyDictionary<string, long> _tagIdsByName;
    private readonly Dictionary<(QueryTagKeyKind Kind, string Key), State> _states = new();
    private readonly QueryTagSighting[] _one = new QueryTagSighting[1];
    private bool _sightingsStarted;

    public QueryTagRebuildMerge(
        int serverId,
        IReadOnlyList<QueryTagHandAssignment> assignments,
        IReadOnlyList<QueryTagRule> rules,
        IReadOnlyDictionary<string, long> tagIdsByName)
    {
        _serverId = serverId;
        _assignments = assignments ?? throw new ArgumentNullException(nameof(assignments));
        _rules = rules ?? throw new ArgumentNullException(nameof(rules));
        _tagIdsByName = tagIdsByName ?? throw new ArgumentNullException(nameof(tagIdsByName));
    }

    /// <summary>
    /// The order of one key's slim rows: database, application, login, host, then object, each compared by code unit with
    /// none as the empty string. It is total over everything a slim row is resolved from, so two rows that compare equal
    /// resolve the same, and it is the order of <see cref="QueryTagMapStore.SightingsOfKeysSql"/> on a store whose text sorts
    /// by byte. It is fixed here, not left to the store's collation, so a key's tag never depends on the order rows arrived in.
    /// </summary>
    internal static int SlimRowOrder(QueryTagSighting a, QueryTagSighting b)
    {
        int order = string.CompareOrdinal(a.Database ?? string.Empty, b.Database ?? string.Empty);
        if (order == 0)
        {
            order = string.CompareOrdinal(a.Application ?? string.Empty, b.Application ?? string.Empty);
        }

        if (order == 0)
        {
            order = string.CompareOrdinal(a.Login ?? string.Empty, b.Login ?? string.Empty);
        }

        if (order == 0)
        {
            order = string.CompareOrdinal(a.Host ?? string.Empty, b.Host ?? string.Empty);
        }

        return order != 0 ? order : string.CompareOrdinal(a.ObjectRef?.Qualified ?? string.Empty, b.ObjectRef?.Qualified ?? string.Empty);
    }

    /// <summary>
    /// Resolves one slim row alone and folds it into its key's state. A row that gets no tag changes nothing. Every slim row
    /// comes before the first sighting: a slim row after it could tag a key whose sightings got no tag and left no state.
    /// </summary>
    public void AddSlim(QueryTagSighting row)
    {
        ArgumentNullException.ThrowIfNull(row);
        if (_sightingsStarted)
        {
            throw new InvalidOperationException("A slim row was added after a sighting. The slim rows are read first.");
        }

        var resolution = ResolveAlone(row, out int rank);
        if (!resolution.HasTag)
        {
            return;
        }

        var id = (row.KeyKind, row.Key);
        if (!_states.TryGetValue(id, out State state)
            || rank < state.Rank
            || (rank == state.Rank && SlimRowOrder(row, state.FirstSlimRow!) < 0))
        {
            /* The first tagged row of the key, a row an earlier step tags, or a row of the winning step that comes first in order. */
            _states[id] = new State(rank, resolution, NeedsOrder: false, FirstSlimRow: row);
        }
    }

    /// <summary>Resolves one sighting alone and folds it into its key's state. A sighting that gets no tag only drops what the key's slim rows left.</summary>
    public void Add(QueryTagSighting row)
    {
        ArgumentNullException.ThrowIfNull(row);
        _sightingsStarted = true;

        var id = (row.KeyKind, row.Key);
        bool known = _states.TryGetValue(id, out State state);
        if (known && state.FirstSlimRow is not null)
        {
            /* The key has a sighting, so it is resolved from its sightings alone, as it is with no slim table. */
            _states.Remove(id);
            known = false;
        }

        var resolution = ResolveAlone(row, out int rank);
        if (!resolution.HasTag)
        {
            return;
        }

        if (!known || rank < state.Rank)
        {
            /* The first tagged sighting of the key, or one an earlier step tags: that step wins the key. */
            _states[id] = new State(rank, resolution, NeedsOrder: false, FirstSlimRow: null);
        }
        else if (rank == state.Rank && !state.NeedsOrder && !resolution.Equals(state.Resolution))
        {
            /* Two sightings at the winning step disagree, so which one comes first in order decides. */
            _states[id] = state with { NeedsOrder = true };
        }
    }

    /// <summary>The map rows of the keys that need no order, and the keys that do.</summary>
    public QueryTagRebuildMergeResult Finish()
    {
        var rows = new List<QueryTagMapRow>(_states.Count);
        var needOrder = new List<QueryTagChangedKey>();
        foreach (var (id, state) in _states)
        {
            if (state.NeedsOrder)
            {
                needOrder.Add(new QueryTagChangedKey(QueryTagSightingWriter.KindText(id.Kind), id.Key, default));
            }
            else
            {
                rows.Add(QueryTagMapStore.MapRow(id.Kind, id.Key, state.Resolution));
            }
        }

        return new QueryTagRebuildMergeResult(rows, needOrder);
    }

    private QueryTagResolution ResolveAlone(QueryTagSighting row, out int rank)
    {
        _one[0] = row;
        return QueryTagResolver.Resolve(_serverId, _one, _assignments, _rules, _tagIdsByName, out rank);
    }

    /// <summary><paramref name="FirstSlimRow"/> is set while the state comes from slim rows: the row of the winning step that is first in <see cref="SlimRowOrder"/>.</summary>
    private readonly record struct State(int Rank, QueryTagResolution Resolution, bool NeedsOrder, QueryTagSighting? FirstSlimRow);
}
