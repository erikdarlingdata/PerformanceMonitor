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
/// <para>Only a conclusive outcome goes in: both plans were fetched and compared and the rule reached a drop, or a keep
/// with a reason (<see cref="PlanInputOutcome.KeepSameValues"/>, <see cref="PlanInputOutcome.KeepStatisticsChanged"/>).
/// A budget that ran out, a timeout, a missing plan, a fault and an uncomparable pair
/// (<see cref="PlanInputOutcome.KeepUnverified"/>) are never stored, so the next pass fetches them again. That last one
/// matters because the statement filter judges a plan within a time budget and withholds what it cannot judge in
/// time: a plan read at a busy moment compares as unverified, and must not be remembered as that.</para>
///
/// <para>Why an entry stays right: a Query Store plan's <c>query_plan</c> is written once when the plan is captured and is
/// not edited afterwards, and the filter's pattern is fixed in the build, so the same pair judges the same way. What can
/// change is the meaning of the key: <c>plan_id</c> restarts when a database of the same name is dropped and recreated or
/// restored, and Query Store can be cleared. An entry therefore expires after <see cref="Lifetime"/>, which bounds how long
/// such a reuse can be answered from the old pair. The cache holds at most <see cref="MaxEntries"/> entries; past it the
/// oldest is evicted.</para>
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

    private static readonly ConditionalWeakTable<DuckDbInitializer, PlanInputOutcomeCache> s_byStore = new();

    /// <summary>The cache that belongs to <paramref name="duckDb"/>'s store.</summary>
    internal static PlanInputOutcomeCache For(DuckDbInitializer duckDb) => s_byStore.GetValue(duckDb, _ => new PlanInputOutcomeCache());

    internal readonly record struct Key(int ServerId, string Database, long LatestPlanId, long BestPlanId);

    private readonly record struct Entry(PlanInputOutcome Outcome, DateTime StoredUtc, long Sequence);

    private readonly object _gate = new();
    private readonly Dictionary<Key, Entry> _entries = [];
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

        lock (_gate)
        {
            _entries[key] = new Entry(outcome, _time.GetUtcNow().UtcDateTime, ++_sequence);

            /* Past the cap the oldest store goes. A scan of at most MaxEntries + 1 entries, run only on the store that
               overflows, which is one per new pair once the cache is full. */
            if (_entries.Count > MaxEntries)
            {
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
        }
    }
}
