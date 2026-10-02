using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// The shared tier of baseline buckets over one DuckDB store (#3941) — Darling's <c>BaselineCache</c>, twinned. The
/// scheduled pass builds a FRESH <see cref="AnalysisService"/> per server (its <c>IsAnalyzing</c> flag is per instance),
/// and so does the Recommendations tab, and each service built its own <see cref="BaselineProvider"/>, so the
/// provider's bucket cache never outlived a pass: every pass recomputed every 30-day baseline, and the MCP host's
/// singleton and the overview lanes paid for them again. Every one of those sites hands its provider the store's tier
/// (<see cref="For"/>), so a series is computed once per analysis hour between them.
///
/// <para><b>When an entry answers — the invalidation rule.</b> The analysis hour: an entry answers only the hour it was
/// computed for, and the compute's window ends AT that hour (<see cref="BaselineProvider"/> snaps it), so every caller
/// in the hour is asking for the same 30 days of rows and the cached answer is the fresh one. Time: an entry is dead
/// <see cref="BaselineProvider.CacheTtl"/> after its compute whatever its hour — the bound on anything that moved inside
/// the window (a late row, a retention purge, the target's clock changing zone) — and dead entries are swept. Except
/// (#4248): a successful compute of a daily-cache metric (<see cref="BaselineProvider.IsDailyCacheMetric"/>) is never
/// eroded by the TTL — the EntryKey's analysis-day component already stops matching once that UTC day ends, exactly as
/// the hourly case's analysis-hour component always has; see <see cref="BaselineProvider.IsFresh"/>, which this tier's
/// live check and sweep both call. Failure:
/// only a SUCCESSFUL compute is shared; a failed one stays the failing provider's "no baseline this pass", and a shared
/// success beats it. What does not invalidate: thresholds and settings (nothing configurable reaches the baseline SQL —
/// every threshold is applied after the lookup), and a server's removal (its history stays in the store).</para>
///
/// <para>One instance per <see cref="DuckDbInitializer"/>, never a process static: a test fixture's store and the app's
/// are different stores, and two fixtures reusing a server id must not answer for each other. A provider built without
/// it (every test) keeps a private cache exactly as before.</para>
/// </summary>
public sealed class BaselineCache
{
    private static readonly ConditionalWeakTable<DuckDbInitializer, BaselineCache> s_byStore = new();

    private readonly ConcurrentDictionary<EntryKey, BaselineProvider.CachedBaseline> _entries = new();
    private long _lastSweepTicks = DateTime.UtcNow.Ticks;

    /// <summary>A shared entry's identity: the provider's own cache key (<c>{server}:{metric}</c>) and the analysis
    /// hour — the whole of what the compute's answer is a function of, besides the stored rows.</summary>
    private readonly record struct EntryKey(int ServerId, string CacheKey, DateTime AnalysisHour);

    /// <summary>The store's tier: the same instance for every caller holding the same <paramref name="duckDb"/>.</summary>
    public static BaselineCache For(DuckDbInitializer duckDb) => s_byStore.GetValue(duckDb, _ => new BaselineCache());

    /// <summary>How many entries the tier holds right now, live or not yet swept.</summary>
    internal int Count => _entries.Count;

    internal bool TryGet(
        int serverId, string cacheKey, DateTime analysisHour, [NotNullWhen(true)] out BaselineProvider.CachedBaseline? entry)
    {
        if (_entries.TryGetValue(new EntryKey(serverId, cacheKey, analysisHour), out entry)
            && IsLive(entry, DateTime.UtcNow))
        {
            return true;
        }

        entry = null;
        return false;
    }

    /// <summary>Shares a SUCCESSFUL compute — the caller never hands a failed one (null buckets) here.</summary>
    internal void Put(int serverId, string cacheKey, BaselineProvider.CachedBaseline entry)
    {
        _entries[new EntryKey(serverId, cacheKey, entry.ComputedAt)] = entry;
        SweepIfDue(DateTime.UtcNow);
    }

    /// <summary>Drops every entry for <paramref name="serverId"/>, of every hour.</summary>
    internal void Invalidate(int serverId)
    {
        foreach (var key in _entries.Keys.Where(k => k.ServerId == serverId).ToList())
        {
            _entries.TryRemove(key, out _);
        }
    }

    internal void Clear() => _entries.Clear();

    private static bool IsLive(BaselineProvider.CachedBaseline entry, DateTime nowUtc)
        => BaselineProvider.IsFresh(entry, nowUtc);

    /// <summary>At most once per quarter of <see cref="BaselineProvider.CacheTtl"/>, drops the entries no lookup can take
    /// any more, so the tier holds little beyond one TTL of computes.</summary>
    internal void SweepIfDue(DateTime nowUtc)
    {
        var last = Interlocked.Read(ref _lastSweepTicks);
        if (nowUtc.Ticks - last < BaselineProvider.CacheTtl.Ticks / 4
            || Interlocked.CompareExchange(ref _lastSweepTicks, nowUtc.Ticks, last) != last)
        {
            return;
        }

        foreach (var pair in _entries)
        {
            /* The pair, not the key: a compute that replaced this entry since the enumeration read it stays. */
            if (!IsLive(pair.Value, nowUtc))
            {
                _entries.TryRemove(pair);
            }
        }
    }
}
