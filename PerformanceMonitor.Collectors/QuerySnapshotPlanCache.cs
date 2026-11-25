/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The estimated plans <c>query_snapshots</c> fetched recently, held by the host between runs so a statement that
/// is still running is not asked for the same plan every minute (#9529).
///
/// <para><b>Why it exists.</b> The estimated plan of one (plan_handle, statement offsets, query_plan_hash) does
/// not change while that plan stays cached, and a request that runs for an hour had it rendered and shipped
/// sixty times. The collector names the plans this cache holds in its query, the target renders nothing for
/// them, and the read fills each such row from here, so the stored row carries the same plan XML as before.</para>
///
/// <para><b>Host-owned, like the delta calculator.</b> One instance per host process, handed to the definition
/// through <see cref="CollectorContext.SnapshotPlanCache"/>. The definition itself stays stateless. A context
/// with no cache (Darling's on-demand live read) reuses nothing and fetches every plan under the per-run cap.</para>
///
/// <para><b>Bounds.</b> A plan is held for <see cref="PlanLifetime"/> from its fetch and then fetched again. A
/// server holds at most <see cref="MaxKnownPlansPerServer"/> plans and <see cref="MaxServerChars"/> characters,
/// and the whole cache at most <see cref="MaxTotalChars"/>. A plan that does not fit is not held, which only
/// means the next run fetches it again.</para>
/// </summary>
public sealed class QuerySnapshotPlanCache
{
    /// <summary>
    /// How long a fetched plan is reused. Ten minutes turns sixty fetches an hour into six for a long runner. It
    /// is not longer because the key cannot see everything a recompile changes: a statement recompiled to the
    /// same plan shape keeps its query_plan_hash, and its new compile-time values reach the store only at the
    /// next fetch.
    /// </summary>
    public static readonly TimeSpan PlanLifetime = TimeSpan.FromMinutes(10);

    /// <summary>
    /// The most plans held for one server, and so the most keys the collector binds per run
    /// (<see cref="QuerySnapshotsCollector.BuildQuery"/> binds exactly this many parameters, the unused ones NULL,
    /// so the batch text is the same every run). Above <see cref="QuerySnapshotsCollector.MaxPlansPerRun"/> so the
    /// requests of one full run can all be held while some of them leave and others arrive.
    /// </summary>
    public const int MaxKnownPlansPerServer = 32;

    /// <summary>Characters held for one server: 8M characters is 16 MB of UTF-16.</summary>
    public const long MaxServerChars = 8L * 1024 * 1024;

    /// <summary>Characters held across every server: 64M characters is 128 MB of UTF-16.</summary>
    public const long MaxTotalChars = 64L * 1024 * 1024;

    private readonly object _gate = new();
    private readonly Dictionary<int, Dictionary<string, Entry>> _servers = new();
    private long _totalChars;
    private DateTime _lastSweepUtc = DateTime.MinValue;

    private sealed class Entry
    {
        public required string PlanXml { get; init; }
        public required DateTime FetchedUtc { get; init; }
        public DateTime LastSeenUtc { get; set; }
    }

    /// <summary>
    /// The plans held for <paramref name="serverId"/> that are still inside their lifetime at
    /// <paramref name="nowUtc"/>, as a copy keyed by plan key. Expired plans are dropped here, so the next run
    /// fetches them again.
    /// </summary>
    public IReadOnlyDictionary<string, string> Known(int serverId, DateTime nowUtc)
    {
        lock (_gate)
        {
            SweepExpired(nowUtc);

            var known = new Dictionary<string, string>(StringComparer.Ordinal);
            if (_servers.TryGetValue(serverId, out var plans))
            {
                Expire(serverId, plans, nowUtc);
                foreach (var (key, entry) in plans)
                {
                    known[key] = entry.PlanXml;
                }
            }

            return known;
        }
    }

    /// <summary>Holds a plan fetched at <paramref name="nowUtc"/>, replacing any plan held under the same key.</summary>
    public void Store(int serverId, string planKey, string planXml, DateTime nowUtc)
    {
        ArgumentNullException.ThrowIfNull(planKey);
        ArgumentNullException.ThrowIfNull(planXml);

        lock (_gate)
        {
            _servers.TryGetValue(serverId, out var plans);

            /* A plan that does not fit even when every other plan of this server leaves is not held, and it makes
               no room: evicting for it would empty the server's cache for a plan that is then refused. */
            if (planXml.Length > MaxServerChars || _totalChars - (plans is null ? 0 : CharsOf(plans)) + planXml.Length > MaxTotalChars)
            {
                return;
            }

            if (plans is null)
            {
                plans = new Dictionary<string, Entry>(StringComparer.Ordinal);
                _servers[serverId] = plans;
            }

            if (plans.Remove(planKey, out var replaced))
            {
                _totalChars -= replaced.PlanXml.Length;
            }

            /* Make room among this server's own plans first, the one seen longest ago leaving first: a plan no
               request has used lately is the cheapest to lose. The check above means this ends with room. */
            while (plans.Count > 0
                && (plans.Count >= MaxKnownPlansPerServer
                    || CharsOf(plans) + planXml.Length > MaxServerChars
                    || _totalChars + planXml.Length > MaxTotalChars))
            {
                Evict(plans, OldestSeen(plans));
            }

            plans[planKey] = new Entry { PlanXml = planXml, FetchedUtc = nowUtc, LastSeenUtc = nowUtc };
            _totalChars += planXml.Length;
        }
    }

    /// <summary>Records that a held plan was used by a run at <paramref name="nowUtc"/>, so it outlives plans no request uses.</summary>
    public void Touch(int serverId, string planKey, DateTime nowUtc)
    {
        lock (_gate)
        {
            if (_servers.TryGetValue(serverId, out var plans) && plans.TryGetValue(planKey, out var entry) && entry.LastSeenUtc < nowUtc)
            {
                entry.LastSeenUtc = nowUtc;
            }
        }
    }

    /// <summary>Characters held across every server.</summary>
    public long TotalChars
    {
        get
        {
            lock (_gate)
            {
                return _totalChars;
            }
        }
    }

    /* A server that stopped being collected never calls Known again, so its plans are dropped by whichever
       server runs next, at most once per lifetime. */
    private void SweepExpired(DateTime nowUtc)
    {
        if (nowUtc >= _lastSweepUtc && nowUtc - _lastSweepUtc < PlanLifetime)
        {
            return;
        }

        _lastSweepUtc = nowUtc;
        var emptied = new List<int>();
        foreach (var (serverId, plans) in _servers)
        {
            ExpireEntries(plans, nowUtc, ownClock: false);
            if (plans.Count == 0)
            {
                emptied.Add(serverId);
            }
        }

        foreach (var serverId in emptied)
        {
            _servers.Remove(serverId);
        }
    }

    private void Expire(int serverId, Dictionary<string, Entry> plans, DateTime nowUtc)
    {
        ExpireEntries(plans, nowUtc, ownClock: true);
        if (plans.Count == 0)
        {
            _servers.Remove(serverId);
        }
    }

    /* ownClock: nowUtc is a run time of the server these plans belong to, so a fetch stamped after it is that
       server's clock having moved back, and the plan is dropped and fetched again. The sweep passes another
       server's run time, which can sit a few seconds behind a fetch that is perfectly current, so it drops on
       age alone. */
    private void ExpireEntries(Dictionary<string, Entry> plans, DateTime nowUtc, bool ownClock)
    {
        List<string>? expired = null;
        foreach (var (key, entry) in plans)
        {
            if (nowUtc - entry.FetchedUtc >= PlanLifetime || (ownClock && entry.FetchedUtc > nowUtc))
            {
                (expired ??= new List<string>()).Add(key);
            }
        }

        if (expired is null)
        {
            return;
        }

        foreach (var key in expired)
        {
            Evict(plans, key);
        }
    }

    private void Evict(Dictionary<string, Entry> plans, string planKey)
    {
        if (plans.Remove(planKey, out var entry))
        {
            _totalChars -= entry.PlanXml.Length;
        }
    }

    private static string OldestSeen(Dictionary<string, Entry> plans)
    {
        string? oldestKey = null;
        var oldest = DateTime.MaxValue;
        foreach (var (key, entry) in plans)
        {
            if (oldestKey is null || entry.LastSeenUtc < oldest)
            {
                oldestKey = key;
                oldest = entry.LastSeenUtc;
            }
        }

        return oldestKey!;
    }

    private static long CharsOf(Dictionary<string, Entry> plans)
    {
        long chars = 0;
        foreach (var entry in plans.Values)
        {
            chars += entry.PlanXml.Length;
        }

        return chars;
    }
}
