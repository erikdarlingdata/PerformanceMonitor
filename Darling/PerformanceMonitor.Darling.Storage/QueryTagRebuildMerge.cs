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
/// Resolves one server's sighting rows as they arrive, in any order (#9705). Pure: no store, no I/O. A rebuild reads the two
/// sightings tables in heap order, which is no key order, so it cannot hand the resolver one key's rows as an ordered list. It
/// hands each row to <see cref="Add"/> instead and holds one small state per key, never the rows.
/// <para>Why that gives the tag <see cref="QueryTagResolver.Resolve(int, IReadOnlyList{QueryTagSighting}, IReadOnlyList{QueryTagHandAssignment}, IReadOnlyList{QueryTagRule}, IReadOnlyDictionary{string, long})"/>
/// gives over the key's whole ordered row list: the resolver returns the hit of the lowest precedence step that any row
/// matches, on the first row in order that matches it. Each row is resolved alone here, with the number of the step that hit
/// (its rank). The rows that match the key's winning step are exactly the rows whose own rank is the lowest one seen. When
/// they all give an equal resolution, the order cannot change the answer and the key is done. When they differ (a pattern
/// that matched two values, a tag comment that named two tags), the first row in order decides, so the key is listed in
/// <see cref="QueryTagRebuildMergeResult.KeysNeedingOrder"/> and the builder reads that key's rows in order.</para>
/// </summary>
internal sealed class QueryTagRebuildMerge
{
    private readonly int _serverId;
    private readonly IReadOnlyList<QueryTagHandAssignment> _assignments;
    private readonly IReadOnlyList<QueryTagRule> _rules;
    private readonly IReadOnlyDictionary<string, long> _tagIdsByName;
    private readonly Dictionary<(QueryTagKeyKind Kind, string Key), State> _states = new();
    private readonly QueryTagSighting[] _one = new QueryTagSighting[1];

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

    /// <summary>Resolves one row alone and folds it into its key's state. A row that gets no tag changes nothing.</summary>
    public void Add(QueryTagSighting row)
    {
        ArgumentNullException.ThrowIfNull(row);

        _one[0] = row;
        var resolution = QueryTagResolver.Resolve(_serverId, _one, _assignments, _rules, _tagIdsByName, out int rank);
        if (!resolution.HasTag)
        {
            return;
        }

        var id = (row.KeyKind, row.Key);
        if (!_states.TryGetValue(id, out State state) || rank < state.Rank)
        {
            /* The first tagged row of the key, or a row an earlier step tags: that step wins the key. */
            _states[id] = new State(rank, resolution, NeedsOrder: false);
        }
        else if (rank == state.Rank && !state.NeedsOrder && !resolution.Equals(state.Resolution))
        {
            /* Two rows at the winning step disagree, so which one comes first in order decides. */
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

    private readonly record struct State(int Rank, QueryTagResolution Resolution, bool NeedsOrder);
}
