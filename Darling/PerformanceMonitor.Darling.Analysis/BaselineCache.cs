/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// The process's shared tier of baseline buckets (#3941): every successful compute any <see cref="PgBaselineProvider"/>
/// handed this object has made, so the scheduled pass, <c>analyze_server</c> and <c>compare_analysis</c> ask the store
/// for a (server, metric[, member]) series once per analysis hour between them instead of once each.
///
/// <para><b>Why it exists.</b> The worker builds a FRESH <see cref="DarlingAnalysisService"/> per pass (its
/// <c>IsAnalyzing</c> flag is per instance), and the service built its own providers, so the providers' bucket cache
/// never outlived a pass: every scheduled pass recomputed every 30-day baseline, and the MCP host's singleton paid for
/// all of them again. Measured on a 31-day rig with the store's hypertables and baseline aggregates: a PostgreSQL-target
/// pass made 12 baseline reads, 1,017 of its 1,252 ms, and a SQL Server pass 9, 159 of its 209 ms; a second pass in the
/// same analysis hour off this tier makes none.</para>
///
/// <para><b>Why a cache and not shared providers.</b> The three hosts read the store as three roles — the worker as
/// the owner, the MCP host as <c>mcp</c>, the web host as <c>viewer</c> (#3914) — and a provider carries its data
/// source. Sharing the provider would run the MCP and web reads as the worker's role; sharing only the computed
/// buckets does not. Each provider keeps its own data source and its own cache (the first tier, exactly as before);
/// this object is the second tier, consulted on a first-tier miss.</para>
///
/// <para><b>When an entry answers — the invalidation rule, stated in full.</b>
/// <list type="bullet">
/// <item><b>The analysis hour.</b> An entry answers only the hour it was computed for, and the compute's window ends AT
/// that hour (<see cref="PgBaselineProvider"/> snaps it, #3941), so every caller asking inside the hour is asking for
/// the same 30 days of rows — the cached answer is the answer a fresh compute would return, not an approximation of
/// it. The detectors ask at the analysis window's start (four hours back), so those rows are settled.</item>
/// <item><b>Time.</b> <see cref="PgBaselineProvider.CacheTtl"/> after the compute an entry is dead whatever its hour —
/// the bound on anything that did move inside a settled window (a late row, a purge, the target's clock changing zone,
/// which is re-keyed on the next compute exactly as before). Dead entries are swept, so the tier holds at most one
/// TTL's worth of computes.</item>
/// <item><b>Failure.</b> Only a SUCCESSFUL compute is shared. A failed one (a timeout, a role that cannot read a
/// relation) stays in the failing provider's own tier, where it has always meant "no baseline for this pass" — one
/// caller's timeout never blanks another caller's baselines, and a success from any caller beats a local failure.</item>
/// <item><b>Engine.</b> The provider's type is part of the key, so a SQL Server series and a PostgreSQL-target series
/// never answer for each other, even for one server id.</item>
/// <item><b>What does not invalidate.</b> Detector thresholds and alert settings: nothing configurable reaches the
/// baseline SQL — the buckets are statistics of stored rows, and every threshold is applied to them after the lookup.
/// And registration: removing a server leaves its history in place (<c>remove_server</c> deletes the definition only)
/// and a re-added server keeps its hash id, so its baseline is the same function of the same rows.</item>
/// </list></para>
///
/// <para>One instance per process, over that process's own store — the service registers it as a singleton and hands
/// it to the worker, MCP and web hosts. Nothing else shares one: a provider built without it (every test, the WPF
/// viewer) keeps a private cache exactly as before.</para>
/// </summary>
public sealed class BaselineCache
{
    private readonly ConcurrentDictionary<EntryKey, PgBaselineProvider.CachedBaseline> _entries = new();
    private long _lastSweepTicks = DateTime.UtcNow.Ticks;

    /// <summary>A shared entry's identity: the provider kind (engine), the provider's own cache key
    /// (<c>{server}:{metric}[:{member}]</c>) and the analysis hour — the whole of what the compute's answer is a
    /// function of, besides the stored rows.</summary>
    private readonly record struct EntryKey(string Kind, int ServerId, string CacheKey, DateTime AnalysisHour);

    /// <summary>How many entries the tier holds right now, live or not yet swept.</summary>
    internal int Count => _entries.Count;

    internal bool TryGet(
        string kind, int serverId, string cacheKey, DateTime analysisHour,
        [NotNullWhen(true)] out PgBaselineProvider.CachedBaseline? entry)
    {
        if (_entries.TryGetValue(new EntryKey(kind, serverId, cacheKey, analysisHour), out entry)
            && IsLive(entry, DateTime.UtcNow))
        {
            return true;
        }

        entry = null;
        return false;
    }

    /// <summary>Shares a SUCCESSFUL compute — the caller never hands a failed one (null buckets) here.</summary>
    internal void Put(string kind, int serverId, string cacheKey, PgBaselineProvider.CachedBaseline entry)
    {
        _entries[new EntryKey(kind, serverId, cacheKey, entry.ComputedAt)] = entry;
        SweepIfDue(DateTime.UtcNow);
    }

    /// <summary>Drops every entry for <paramref name="serverId"/>, of every kind and hour.</summary>
    internal void Invalidate(int serverId)
    {
        foreach (var key in _entries.Keys.Where(k => k.ServerId == serverId).ToList())
        {
            _entries.TryRemove(key, out _);
        }
    }

    internal void Clear() => _entries.Clear();

    private static bool IsLive(PgBaselineProvider.CachedBaseline entry, DateTime nowUtc)
        => nowUtc - entry.RealTime < PgBaselineProvider.CacheTtl;

    /// <summary>At most once per quarter of <see cref="PgBaselineProvider.CacheTtl"/>, drops the entries no lookup can
    /// take any more, so the tier holds little beyond one TTL of computes. Without it the tier would grow for the life of
    /// the process: the keyed series follow each server's top statements, whose ids change from pass to pass.</summary>
    internal void SweepIfDue(DateTime nowUtc)
    {
        if (SweepIsDue(ref _lastSweepTicks, nowUtc))
        {
            RemoveDead(_entries, nowUtc);
        }
    }

    /// <summary>The sweep's gate, shared with each provider's own cache: true at most once per quarter of
    /// <see cref="PgBaselineProvider.CacheTtl"/> per <paramref name="lastSweepTicks"/>, and to one caller only.</summary>
    internal static bool SweepIsDue(ref long lastSweepTicks, DateTime nowUtc)
    {
        var last = Interlocked.Read(ref lastSweepTicks);
        return nowUtc.Ticks - last >= PgBaselineProvider.CacheTtl.Ticks / 4
               && Interlocked.CompareExchange(ref lastSweepTicks, nowUtc.Ticks, last) == last;
    }

    /// <summary>Drops every entry of <paramref name="entries"/> no lookup can take any more (a full
    /// <see cref="PgBaselineProvider.CacheTtl"/> old), and says how many.</summary>
    internal static int RemoveDead<TKey>(ConcurrentDictionary<TKey, PgBaselineProvider.CachedBaseline> entries, DateTime nowUtc)
        where TKey : notnull
    {
        var removed = 0;
        foreach (var pair in entries)
        {
            /* The pair, not the key: a compute that replaced this entry since the enumeration read it stays. */
            if (!IsLive(pair.Value, nowUtc) && entries.TryRemove(pair))
            {
                removed++;
            }
        }

        return removed;
    }
}
