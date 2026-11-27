/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// #9195: what the PLAN_REGRESSION inputs check decided about a pair of Query Store plans, kept across analysis passes
/// so the same two plans are not fetched live from the monitored server every 30 minutes. The outcome is cached and
/// never the plan XML: the XML is customer text and stays in the locals of one comparison.
///
/// <para>Two kinds of outcome go in. A conclusive one: both plans were fetched and compared and the rule reached a drop,
/// or a keep with a reason (<see cref="PlanInputOutcome.KeepSameValues"/>, <see cref="PlanInputOutcome.KeepStatisticsChanged"/>).
/// And, through <see cref="StoreUncomparable"/>, a pair that compared <see cref="PlanInputOutcome.KeepUnverified"/> while
/// neither fetched plan carried text the statement filter withheld: the two stored plans never change and nothing in them
/// was cut by the filter, so the same pair is uncomparable every time. A budget that ran out, a timeout, a missing plan, a
/// fault, a compare that threw and a pair whose plans carry withheld text are never stored as outcomes. The statement
/// filter judges a plan within a time budget and withholds what it cannot judge in time, and a withheld plan looks the same
/// whatever withheld it, so a plan read at a busy moment compares as unverified and must not be remembered as that. Only
/// the skip entries below keep those from being fetched again at once. <see cref="Store"/> itself still refuses
/// <see cref="PlanInputOutcome.KeepUnverified"/>, so a caller cannot remember "unknown" by accident.</para>
///
/// <para>Why an entry stays right: a Query Store plan's <c>query_plan</c> is written once when the plan is captured and is
/// not edited afterwards, and the filter's pattern is fixed in the build, so the same pair judges the same way. What can
/// change is the meaning of the key: <c>plan_id</c> restarts when a database of the same name is dropped and recreated or
/// restored, and Query Store can be cleared. An entry therefore expires after <see cref="Lifetime"/>, which bounds how long
/// such a reuse can be answered from the old pair. The cache holds at most <see cref="MaxEntries"/> entries; past it the
/// oldest is evicted.</para>
///
/// <para>Skip entries (#9195): the cache above is silent about a fetch that failed, so on a busy server the same slow
/// or empty fetches used the pass's whole fetch budget every pass. A plan whose fetch timed out, came back with no plan
/// or faulted, and a pair that compared unverified because a plan carried withheld text, are each remembered for <see cref="SkipLifetime"/>. While an entry
/// is live the plan is not fetched (or the pair is not judged): that uses no budget and the candidate stays unverified,
/// as an unreached one does. After the hour it is tried again. A skip entry says "do not try yet", never "the answer is
/// unverified", so it is kept apart from the outcomes and a conclusive outcome never reads it. The pass's own
/// cancellation and a spent budget are not failures of the plan and never make an entry. Skip entries are capped like
/// the outcomes, at <see cref="MaxSkipEntries"/> per kind, oldest evicted.</para>
///
/// <para>One instance per <see cref="DuckDbInitializer"/>, never a process static, like <see cref="BaselineCache"/>: the
/// scheduler builds a fresh analysis service per pass, so the instance has to live with the store.</para>
/// </summary>
public sealed class PlanInputOutcomeCache
{
    /// <summary>Entries kept. A pair is three numbers and a name, so this is a few tens of kilobytes at most.</summary>
    internal const int MaxEntries = 512;

    /// <summary>How long an entry answers for its pair, see the class remarks on a reused plan_id.</summary>
    internal static readonly TimeSpan Lifetime = TimeSpan.FromHours(6);

    /// <summary>#9195: how long a failed fetch, or a compare a time-limited filter may have cut short, is not tried again.</summary>
    internal static readonly TimeSpan SkipLifetime = TimeSpan.FromHours(1);

    /// <summary>#9195: skip entries kept per kind (plans, pairs).</summary>
    internal const int MaxSkipEntries = 512;

    private static readonly ConditionalWeakTable<DuckDbInitializer, PlanInputOutcomeCache> s_byStore = new();

    /// <summary>The cache that belongs to <paramref name="duckDb"/>'s store.</summary>
    internal static PlanInputOutcomeCache For(DuckDbInitializer duckDb) => s_byStore.GetValue(duckDb, _ => new PlanInputOutcomeCache());

    internal readonly record struct Key(int ServerId, string Database, long LatestPlanId, long BestPlanId);

    /// <summary>One plan whose fetch failed, on the same server and database as its pairs.</summary>
    internal readonly record struct PlanKey(int ServerId, string Database, long PlanId);

    private readonly record struct Entry(PlanInputOutcome Outcome, DateTime StoredUtc, long Sequence);

    private readonly record struct SkipEntry(DateTime StoredUtc, long Sequence);

    private readonly object _gate = new();
    private readonly Dictionary<Key, Entry> _entries = [];
    private readonly Dictionary<PlanKey, SkipEntry> _skippedPlans = [];
    private readonly Dictionary<Key, SkipEntry> _skippedPairs = [];
    private readonly TimeProvider _time;
    private long _sequence;

    internal PlanInputOutcomeCache(TimeProvider? time = null)
    {
        _time = time ?? TimeProvider.System;
    }

    /// <summary>Entries held, including any that expired and have not been asked for since.</summary>
    internal int Count
    {
        get { lock (_gate) return _entries.Count; }
    }

    /// <summary>The stored outcome of the pair, or false when there is none or it has expired.</summary>
    internal bool TryGet(Key key, out PlanInputOutcome outcome)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var entry))
            {
                if (_time.GetUtcNow().UtcDateTime - entry.StoredUtc < Lifetime)
                {
                    outcome = entry.Outcome;
                    return true;
                }

                _entries.Remove(key);
            }
        }

        outcome = PlanInputOutcome.KeepUnverified;
        return false;
    }

    /// <summary>Stores a conclusive outcome; anything else is ignored, so a caller cannot remember "unknown".</summary>
    internal void Store(Key key, PlanInputOutcome outcome)
    {
        if (outcome is PlanInputOutcome.NeedsPlans or PlanInputOutcome.KeepUnverified) return;

        lock (_gate) AddEntry(key, outcome);
    }

    /// <summary>
    /// #9195: remembers, for <see cref="Lifetime"/>, a pair whose two plans were fetched and compared
    /// <see cref="PlanInputOutcome.KeepUnverified"/> with nothing withheld in either, so the answer comes from the plans'
    /// content alone and does not change. <see cref="TryGet"/> then answers <see cref="PlanInputOutcome.KeepUnverified"/> for
    /// the pair. A caller never passes a pair whose plan carried withheld text.
    /// </summary>
    internal void StoreUncomparable(Key key)
    {
        lock (_gate) AddEntry(key, PlanInputOutcome.KeepUnverified);
    }

    private void AddEntry(Key key, PlanInputOutcome outcome)
    {
        _entries[key] = new Entry(outcome, _time.GetUtcNow().UtcDateTime, ++_sequence);

        /* Past the cap the oldest store goes. A scan of at most MaxEntries + 1 entries, run only on the store that
           overflows, which is one per new pair once the cache is full. */
        if (_entries.Count <= MaxEntries) return;

        var oldest = default(Key);
        var oldestSequence = long.MaxValue;
        foreach (var (key2, entry) in _entries)
        {
            if (entry.Sequence < oldestSequence)
            {
                oldestSequence = entry.Sequence;
                oldest = key2;
            }
        }

        _entries.Remove(oldest);
    }

    /// <summary>Skip entries held (plans and pairs), including any that expired and have not been asked for since.</summary>
    internal int SkipCount
    {
        get { lock (_gate) return _skippedPlans.Count + _skippedPairs.Count; }
    }

    /// <summary>True while the plan's last failed fetch is under <see cref="SkipLifetime"/> old.</summary>
    internal bool IsSkipped(PlanKey plan)
    {
        lock (_gate) return IsLive(_skippedPlans, plan);
    }

    /// <summary>True while the pair's last unverified compare (withheld text, or a compare that threw) is under <see cref="SkipLifetime"/> old.</summary>
    internal bool IsSkipped(Key pair)
    {
        lock (_gate) return IsLive(_skippedPairs, pair);
    }

    /// <summary>Remembers that the plan's fetch timed out, found no plan or faulted.</summary>
    internal void Skip(PlanKey plan)
    {
        lock (_gate) AddSkip(_skippedPlans, plan);
    }

    /// <summary>Remembers that the pair compared unverified with withheld text in a plan, or that its compare threw.</summary>
    internal void Skip(Key pair)
    {
        lock (_gate) AddSkip(_skippedPairs, pair);
    }

    private bool IsLive<TKey>(Dictionary<TKey, SkipEntry> entries, TKey key) where TKey : notnull
    {
        if (!entries.TryGetValue(key, out var entry)) return false;
        if (_time.GetUtcNow().UtcDateTime - entry.StoredUtc < SkipLifetime) return true;

        entries.Remove(key);
        return false;
    }

    private void AddSkip<TKey>(Dictionary<TKey, SkipEntry> entries, TKey key) where TKey : notnull
    {
        entries[key] = new SkipEntry(_time.GetUtcNow().UtcDateTime, ++_sequence);
        if (entries.Count <= MaxSkipEntries) return;

        /* The same scan as the outcomes: run only on the add that overflows. */
        var oldest = key;
        var oldestSequence = long.MaxValue;
        foreach (var (key2, entry) in entries)
        {
            if (entry.Sequence < oldestSequence)
            {
                oldestSequence = entry.Sequence;
                oldest = key2;
            }
        }

        entries.Remove(oldest);
    }
}
