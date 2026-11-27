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
/// <see cref="SlimRowOrder"/>, so the key is listed in <see cref="KeysNeedingOrder"/>, the builder reads that key's sightings
/// back through the unique index, sorts them and hands the answer to <see cref="SetOrdered"/>. Slim rows have no key index to
/// read them back through, and no text sample, so the merge keeps the first of them itself, by the same order.</para>
/// <para>What is held (#9705), per tagged key: one dictionary entry (the key's kind, the rank, three small numbers and two
/// references, 48 bytes and 4 for its bucket) and the key's text once. Measured on a lab store whose rebuild tags 8.36
/// million keys of 18 characters: 138.6 bytes a tagged key, 74.6 for the dictionary at its capacity of 12.0 million entries
/// and 64.0 for the key's text, 1,105 MiB in all. Nothing else grows with the keys. Equal resolutions are one object
/// (<see cref="_resolutions"/>: a rule that tags a million keys by one application name holds one resolution, and that
/// store's 8.36 million keys held 301). Of the slim row that won a key the state keeps three numbers, its database, its
/// caller and its object, each the number of one shared entry: <see cref="_texts"/> holds one string per distinct
/// database, application, login, host and object name (215 on that store), and <see cref="_callers"/> one entry per
/// distinct application, login and host together. So callers that combine a few values in many ways add nothing to a key.
/// No sighting, no text sample and no map row is kept: <see cref="MapRows"/> makes each map row as the write takes it. A key
/// no row tags holds nothing.</para>
/// </summary>
internal sealed class QueryTagRebuildMerge
{
    private readonly int _serverId;
    private readonly IReadOnlyList<QueryTagHandAssignment> _assignments;
    private readonly IReadOnlyList<QueryTagRule> _rules;
    private readonly IReadOnlyDictionary<string, long> _tagIdsByName;
    private readonly Dictionary<(QueryTagKeyKind Kind, string Key), State> _states = new();
    private readonly Dictionary<QueryTagResolution, QueryTagResolution> _resolutions = new();
    private readonly Dictionary<string, int> _textNumbers = new(StringComparer.Ordinal);
    private readonly List<string> _texts = new();
    private readonly Dictionary<(int Application, int Login, int Host), int> _callerNumbers = new();
    private readonly List<(int Application, int Login, int Host)> _callers = new();
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
    /// The one order of a key's rows, on every path that resolves a key (#9705): database, application, login, host, then
    /// object, each compared by Unicode code point (<see cref="CompareCodePoints"/>) with none as the empty string. The
    /// first row in this order that matches the key's winning step decides the key's tag: the merge keeps the first slim
    /// row by it, and <see cref="QueryTagMapStore.ReadSightingsOfKeysAsync"/> sorts each key's sightings by it for the extend
    /// and for the rebuild's ordered read. It is total over everything a slim row is resolved from, so two rows that compare
    /// equal resolve the same. The order is fixed in code: no statement's ORDER BY and no collation of the store has to
    /// agree with it, so a key's tag depends neither on the order its rows arrived in nor on how the store sorts text.
    /// </summary>
    internal static int SlimRowOrder(QueryTagSighting a, QueryTagSighting b) =>
        CompareOrder(a.Database, a.Application, a.Login, a.Host, a.ObjectRef?.Qualified, b.Database, b.Application, b.Login, b.Host, b.ObjectRef?.Qualified);

    /// <summary><see cref="SlimRowOrder"/> as a comparer, for a stable sort of one key's rows.</summary>
    internal static IComparer<QueryTagSighting> SlimRowComparer { get; } = Comparer<QueryTagSighting>.Create(SlimRowOrder);

    /// <summary>
    /// Compares two texts by Unicode code point, none as empty, which is the byte order of their UTF-8 form. A plain
    /// comparison of UTF-16 code units is not that order: the two units of a character above U+FFFF are both below U+E000,
    /// so it would put that character before U+E000 to U+FFFF, which its code point and its UTF-8 bytes come after.
    /// </summary>
    internal static int CompareCodePoints(string? a, string? b)
    {
        a ??= string.Empty;
        b ??= string.Empty;
        int shared = Math.Min(a.Length, b.Length);
        for (int i = 0; i < shared; i++)
        {
            char x = a[i];
            char y = b[i];
            if (x != y)
            {
                /* A surrogate is half of a character above U+FFFF, so it comes after every unit that is a whole character. */
                bool xHigh = char.IsSurrogate(x);
                return xHigh == char.IsSurrogate(y) ? x - y : xHigh ? 1 : -1;
            }
        }

        return a.Length - b.Length;
    }

    private static int CompareOrder(
        string? database, string? application, string? login, string? host, string? objectName,
        string? otherDatabase, string? otherApplication, string? otherLogin, string? otherHost, string? otherObjectName)
    {
        int order = CompareCodePoints(database, otherDatabase);
        if (order == 0)
        {
            order = CompareCodePoints(application, otherApplication);
        }

        if (order == 0)
        {
            order = CompareCodePoints(login, otherLogin);
        }

        if (order == 0)
        {
            order = CompareCodePoints(host, otherHost);
        }

        return order != 0 ? order : CompareCodePoints(objectName, otherObjectName);
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
            || (rank == state.Rank && ComesBeforeTheKept(row, state)))
        {
            /* The first tagged row of the key, a row an earlier step tags, or a row of the winning step that comes first in order. */
            _states[id] = new State(
                rank, Shared(resolution), CallerNumber(row), TextNumber(row.Database), TextNumber(row.ObjectRef?.Qualified));
        }
    }

    /// <summary>Resolves one sighting alone and folds it into its key's state. A sighting that gets no tag only drops what the key's slim rows left.</summary>
    public void Add(QueryTagSighting row)
    {
        ArgumentNullException.ThrowIfNull(row);
        _sightingsStarted = true;

        var id = (row.KeyKind, row.Key);
        bool known = _states.TryGetValue(id, out State state);
        if (known && state.FromSlimRows)
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
            _states[id] = State.OfSightings(rank, Shared(resolution));
        }
        else if (rank == state.Rank && !state.NeedsOrder && !resolution.Equals(state.Resolution))
        {
            /* Two sightings at the winning step disagree, so which one comes first in order decides. */
            _states[id] = state with { Origin = State.SightingsThatDisagree };
        }
    }

    /// <summary>The keys whose winning sightings disagree, so their first sighting in <see cref="SlimRowOrder"/> decides: one small entry per such key.</summary>
    public List<QueryTagChangedKey> KeysNeedingOrder()
    {
        var keys = new List<QueryTagChangedKey>();
        foreach (var (id, state) in _states)
        {
            if (state.NeedsOrder)
            {
                keys.Add(new QueryTagChangedKey(QueryTagSightingWriter.KindText(id.Kind), id.Key, default));
            }
        }

        return keys;
    }

    /// <summary>
    /// Settles a key of <see cref="KeysNeedingOrder"/> with the resolution of its sightings in order, so its map row comes
    /// out of <see cref="MapRows"/> with every other key's and the builder keeps no list of its own. No tag drops the key.
    /// </summary>
    public void SetOrdered(QueryTagKeyKind kind, string key, QueryTagResolution resolution)
    {
        ArgumentNullException.ThrowIfNull(resolution);
        var id = (kind, key);
        if (resolution.HasTag)
        {
            _states.TryGetValue(id, out State state);
            _states[id] = State.OfSightings(state.Rank, Shared(resolution));
        }
        else
        {
            _states.Remove(id);
        }
    }

    /// <summary>
    /// The map rows of the keys that need no order, each made as it is taken, so the map write can take a server's rows a
    /// batch at a time and no list of them exists (#9705).
    /// </summary>
    public IEnumerable<QueryTagMapRow> MapRows()
    {
        foreach (var (id, state) in _states)
        {
            if (!state.NeedsOrder)
            {
                yield return QueryTagMapStore.MapRow(id.Kind, id.Key, state.Resolution);
            }
        }
    }

    /// <summary>The map rows of the keys that need no order, and the keys that do, as two lists.</summary>
    public QueryTagRebuildMergeResult Finish() => new(new List<QueryTagMapRow>(MapRows()), KeysNeedingOrder());

    private QueryTagResolution ResolveAlone(QueryTagSighting row, out int rank)
    {
        _one[0] = row;
        return QueryTagResolver.Resolve(_serverId, _one, _assignments, _rules, _tagIdsByName, out rank);
    }

    /// <summary>The one object every key with an equal resolution holds.</summary>
    private QueryTagResolution Shared(QueryTagResolution resolution)
    {
        if (!_resolutions.TryGetValue(resolution, out var shared))
        {
            _resolutions.Add(resolution, shared = resolution);
        }

        return shared;
    }

    /// <summary>True when <paramref name="row"/> comes before the slim row <paramref name="kept"/> stands for, in <see cref="SlimRowOrder"/>.</summary>
    private bool ComesBeforeTheKept(QueryTagSighting row, State kept)
    {
        var caller = _callers[kept.Origin];
        return CompareOrder(
            row.Database, row.Application, row.Login, row.Host, row.ObjectRef?.Qualified,
            _texts[kept.Database], _texts[caller.Application], _texts[caller.Login], _texts[caller.Host], _texts[kept.ObjectName]) < 0;
    }

    /// <summary>The number of a text among the ones held, none as empty: equal texts get one number and are held as one string.</summary>
    private int TextNumber(string? text)
    {
        text ??= string.Empty;
        if (!_textNumbers.TryGetValue(text, out int number))
        {
            number = _texts.Count;
            _texts.Add(text);
            _textNumbers.Add(text, number);
        }

        return number;
    }

    /// <summary>The number of a row's application, login and host together, so the callers of a store are held once, however many keys each one won.</summary>
    private int CallerNumber(QueryTagSighting row)
    {
        var caller = (TextNumber(row.Application), TextNumber(row.Login), TextNumber(row.Host));
        if (!_callerNumbers.TryGetValue(caller, out int number))
        {
            number = _callers.Count;
            _callers.Add(caller);
            _callerNumbers.Add(caller, number);
        }

        return number;
    }

    /// <summary>
    /// One tagged key's state. <paramref name="Origin"/> says where it comes from: <see cref="Sightings"/>,
    /// <see cref="SightingsThatDisagree"/>, or, for a state that comes from slim rows, the caller's number in
    /// <see cref="_callers"/>, which is zero or more. Only that last kind reads <paramref name="Database"/> and
    /// <paramref name="ObjectName"/>, numbers in <see cref="_texts"/>: with the caller they are what
    /// <see cref="SlimRowOrder"/> reads of the row of the winning step that is first in that order. Five small fields and
    /// no reference but the shared resolution, so an entry of the dictionary is 48 bytes.
    /// </summary>
    private readonly record struct State(int Rank, QueryTagResolution Resolution, int Origin, int Database, int ObjectName)
    {
        public const int Sightings = -1;
        public const int SightingsThatDisagree = -2;

        public bool FromSlimRows => Origin >= 0;

        public bool NeedsOrder => Origin == SightingsThatDisagree;

        public static State OfSightings(int rank, QueryTagResolution resolution) => new(rank, resolution, Sightings, 0, 0);
    }
}
