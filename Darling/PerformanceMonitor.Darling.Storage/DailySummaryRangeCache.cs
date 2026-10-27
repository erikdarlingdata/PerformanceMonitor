/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #4232: a reading-side cache for a <see cref="DailySummarySql"/> range read. Both the WPF Daily Summary
/// calendar and the web/MCP <c>get_daily_summary_range</c> re-read a whole range of days on every poll even
/// though every day but today (and yesterday, briefly after midnight) is closed and its rows do not change.
/// This holds the CLOSED portion of one range as one block until the next UTC day boundary plus
/// <see cref="ClosedGrace"/> (#6809; it was one hour, which made the first load of every hour a cold read of
/// about 30 closed days); every refresh inside that window re-runs the statement only over the days still open
/// and joins them to the block. After that, one refresh recomputes the whole range and repopulates it. Expired
/// blocks are dropped on the next miss, and the cache never holds more than <see cref="MaxBlocks"/> blocks at once, evicting the oldest
/// first (#4232 follow-up: the key carries the range, so every distinct server/range/day combination otherwise
/// added a block that nothing ever removed). No migration, no store job -- this lives entirely on the
/// reading side, next to <see cref="DailySummarySql"/> so both its consumers (the viewer, which cannot see the
/// service, and the service) can share the shape.
///
/// <para><b>A day is closed two hours after it ends</b> (ruling): a collector that missed its cadence by a
/// short outage still has time to land its row before the day is treated as settled. So the open portion is
/// always "today", plus "yesterday" during its first two hours.</para>
///
/// <para><b>Callers own routing; this type never builds or chooses SQL.</b> The caller resolves the tier-routed
/// statement text ONCE, for the range as a whole (exactly as it did before this cache existed), and its
/// <c>runRange</c> delegate (see <see cref="GetRangeAsync"/>) must keep reading through that SAME text for
/// every sub-range this cache asks it to run, including the open-days-only re-read. A smaller [start, end)
/// resolved on its own could route to a different tier than the range it is part of -- a 30-day range old
/// enough to route its <c>queries</c> CTE to the hourly rollup, while a 1-day "yesterday" slice resolved fresh
/// would route to raw -- which would silently change what a day's <c>unique_queries</c> means between two rows
/// of the same read. The routed text also rides in the cache key: if the caller resolves a DIFFERENT statement
/// for the same server/range on a later call (a rollup materializes, coverage moves), the old block is not
/// reused.</para>
///
/// <para><b>No band or rank lives here (#4232 ruling item 7, checked).</b> Every column
/// <c>DailySummarySql.RangeSqlFor</c> returns is grouped strictly within its own day's rows -- nothing in the
/// statement compares one day to another, so there is no cross-range column to protect. <typeparamref
/// name="TRow"/> should still be the RAW per-day row: callers must cache rows from BEFORE any later
/// banding/threshold/horizon judgment is stamped onto them, and re-apply that judgment to every row (cached or
/// fresh) after <see cref="GetRangeAsync"/> returns. A purge horizon or a deadlock-rate-threshold setting can
/// move inside the cache's lifetime, and a stale stamp baked into a cached closed day would be wrong
/// on its own terms, not just "a day compared to the range". <typeparamref name="TRow"/> should also be
/// immutable (a record or struct) -- the same row instances are handed to every caller within a block's
/// lifetime.</para>
/// </summary>
public sealed class DailySummaryRangeCache<TRow>
{
    /// <summary>How long after midnight UTC yesterday stays in the open (always-recomputed) set.</summary>
    public static readonly TimeSpan ClosedGrace = TimeSpan.FromHours(2);

    /// <summary>The most blocks this cache holds at once. The key carries the store, server, range and routed
    /// SQL, so an unbounded cache grows with every distinct combination a caller ever asks for; the fleet case
    /// is one or two ranges per server, and a block holds at most 366 rows, so this cap is generous headroom,
    /// not a tuned limit.</summary>
    public const int MaxBlocks = 1024;

    private readonly Func<DateTime> _clock;
    private readonly ConcurrentDictionary<BlockKey, CachedBlock> _blocks = new();

    /// <summary>The number of blocks currently cached, for tests.</summary>
    internal int Count => _blocks.Count;

    /// <param name="clock">Defaults to the wall clock; tests pass one that can move (the same
    /// <c>Func&lt;DateTime&gt;?</c> shape <c>RdsLogSource</c> already uses), so the day-boundary and two-hour
    /// boundaries can be pinned without a real sleep.</param>
    public DailySummaryRangeCache(Func<DateTime>? clock = null)
        => _clock = clock ?? (() => DateTime.UtcNow);

    private readonly record struct BlockKey(object StoreKey, int ServerId, DateTime FromDate, DateTime ToDate, string RoutedSql, string ScopeKey);

    /// <summary><paramref name="ClosedRows"/> holds the days in [<paramref name="TrustFromUtc"/>, <paramref name="ClosedEndUtc"/>).
    /// <paramref name="ExpiresAtUtc"/> is the next UTC day boundary plus <see cref="ClosedGrace"/> after the block was built.</summary>
    private sealed record CachedBlock(List<TRow> ClosedRows, DateTime ClosedEndUtc, DateTime TrustFromUtc, DateTime ComputedAtUtc, DateTime ExpiresAtUtc);

    /// <summary>#6809: the <c>trustFromUtc</c> both callers pass: the day after the retention horizon. The horizon
    /// day itself can be half-deleted on the DELETE purge path, so it is read live too. One definition for the
    /// service and the Viewer.</summary>
    public static DateTime TrustFloorFor(DateTime retentionHorizon) => retentionHorizon.Date.AddDays(1);

    /// <summary>#6809: when a block built at <paramref name="nowUtc"/> stops being served: the next UTC day
    /// boundary plus <see cref="ClosedGrace"/>. By then the day after the block's last closed day has closed too,
    /// and a collector that was late for the days in the block has had a whole day to land its row. This replaces
    /// the old one-hour TTL, which sent the first load of every hour back to a cold read of ~30 compressed days.</summary>
    internal static DateTime BlockExpiryUtc(DateTime nowUtc) => nowUtc.Date.AddDays(1).Add(ClosedGrace);

    /// <summary>The UTC day at which the open (always-recomputed) portion of "now" begins: today, or yesterday
    /// while inside <see cref="ClosedGrace"/> of midnight.</summary>
    internal static DateTime OpenStartUtc(DateTime nowUtc)
    {
        var today = nowUtc.Date;
        return nowUtc < today.Add(ClosedGrace) ? today.AddDays(-1) : today;
    }

    /// <summary>
    /// Returns the rows for the half-open <paramref name="fromDate"/>/<paramref name="toDate"/> UTC-day range,
    /// using the closed-day cache when <paramref name="asOfNow"/> is true (#4232 ruling item 5 -- an explicit
    /// end time is not "as of now" and always runs <paramref name="runRange"/> over the whole range, unmemoized).
    /// </summary>
    /// <param name="storeKey">Distinguishes stores/connections sharing one cache instance (the service keeps ONE
    /// cache for every store it serves). Reference-typed store handles (e.g. <c>NpgsqlDataSource</c>) compare by
    /// identity here, which is exactly "same store"; a per-instance cache (the WPF viewer) can pass a constant.</param>
    /// <param name="routedSql">The exact statement text the caller resolved for THIS range, reused for every
    /// sub-range <paramref name="runRange"/> is asked to read (see the type doc) and folded into the cache key.</param>
    /// <param name="scopeKey">#4925: names the scope the rows were read under (see
    /// <see cref="DailySummaryAzureMasterScope.CacheScopeKey"/>); a read under another scope never shares a block.</param>
    /// <param name="day">Reads a row's UTC day, to split a freshly-read whole range into its closed/open rows.</param>
    /// <param name="trustFromUtc">#6809: days before this UTC day (the retention horizon) are never served from a
    /// block and are re-read on every call. A purge is an event that can land any time after the horizon moves, or
    /// after an operator shortens retention, and a closed day kept for up to a day would keep showing rows the
    /// store no longer holds (and judge it past_horizon, not purged). Those days are cheap to read: their signal
    /// tables are what the purge removes. Null means no floor.</param>
    /// <param name="runRange">Runs the statement for an arbitrary half-open [start, end) sub-range and returns its
    /// rows, oldest day first. Called with the WHOLE [<paramref name="fromDate"/>, <paramref name="toDate"/>) on a
    /// cache miss, and with only the open sub-range on a hit.</param>
    public Task<List<TRow>> GetRangeAsync(
        object storeKey,
        int serverId,
        DateTime fromDate,
        DateTime toDate,
        string routedSql,
        bool asOfNow,
        Func<TRow, DateTime> day,
        Func<DateTime, DateTime, CancellationToken, Task<List<TRow>>> runRange,
        CancellationToken cancellationToken = default)
        => GetRangeAsync(storeKey, serverId, fromDate, toDate, routedSql, asOfNow, day, runRange, "", cancellationToken);

    /// <inheritdoc cref="GetRangeAsync(object, int, DateTime, DateTime, string, bool, Func{TRow, DateTime}, Func{DateTime, DateTime, CancellationToken, Task{List{TRow}}}, CancellationToken)"/>
    public async Task<List<TRow>> GetRangeAsync(
        object storeKey,
        int serverId,
        DateTime fromDate,
        DateTime toDate,
        string routedSql,
        bool asOfNow,
        Func<TRow, DateTime> day,
        Func<DateTime, DateTime, CancellationToken, Task<List<TRow>>> runRange,
        string scopeKey,
        CancellationToken cancellationToken,
        DateTime? trustFromUtc = null)
    {
        ArgumentNullException.ThrowIfNull(day);
        ArgumentNullException.ThrowIfNull(runRange);

        if (!asOfNow || fromDate >= toDate)
        {
            return await runRange(fromDate, toDate, cancellationToken).ConfigureAwait(false);
        }

        var nowUtc = _clock();
        var openStartUtc = OpenStartUtc(nowUtc);
        if (openStartUtc <= fromDate)
        {
            /* The whole requested window is still open (a short, recent range) -- nothing closed to cache. */
            return await runRange(fromDate, toDate, cancellationToken).ConfigureAwait(false);
        }

        var closedEndUtc = openStartUtc < toDate ? openStartUtc : toDate;
        var key = new BlockKey(storeKey, serverId, fromDate, toDate, routedSql, scopeKey ?? "");
        var floorUtc = trustFromUtc is { } trust && trust > fromDate ? trust : fromDate;

        /* A block is served until it expires (next UTC day boundary plus ClosedGrace, #6809) and only if it
           covers what "now" asks for: its own closed end must not be LATER than this call's (clock moved back) and
           its trust floor not HIGHER (a retention raised since the build would leave a hole). A block built before yesterday's two-hour grace expired
           has an EARLIER closed end: that is not a miss, the days from its closed end onward are read now and the
           newly closed ones are folded into the block, so the grace crossing costs one short read, not the range. */
        if (_blocks.TryGetValue(key, out var cached)
            && nowUtc < cached.ExpiresAtUtc
            && cached.ClosedEndUtc <= closedEndUtc
            && cached.TrustFromUtc <= floorUtc)
        {
            var joined = new List<TRow>(cached.ClosedRows.Count + 8);

            /* Days before the trust floor (older than the retention horizon) are read live every time. */
            var liveEnd = floorUtc < cached.ClosedEndUtc ? floorUtc : cached.ClosedEndUtc;
            if (liveEnd > fromDate)
            {
                joined.AddRange(await runRange(fromDate, liveEnd, cancellationToken).ConfigureAwait(false));
            }

            joined.AddRange(cached.ClosedRows.Where(row => day(row) >= floorUtc));

            if (cached.ClosedEndUtc < toDate)
            {
                var tailRows = await runRange(cached.ClosedEndUtc, toDate, cancellationToken).ConfigureAwait(false);
                joined.AddRange(tailRows);

                if (closedEndUtc > cached.ClosedEndUtc)
                {
                    var grown = new List<TRow>(cached.ClosedRows);
                    grown.AddRange(tailRows.Where(row => day(row) < closedEndUtc));
                    _blocks[key] = cached with { ClosedRows = grown, ClosedEndUtc = closedEndUtc };
                }
            }

            return joined;
        }

        var full = await runRange(fromDate, toDate, cancellationToken).ConfigureAwait(false);
        var closedRows = full.Where(row => day(row) < closedEndUtc && day(row) >= floorUtc).ToList();

        /* #4232 follow-up: the key carries the range, so nothing ever replaces a block for a range nobody asks
           for again (a one-off days_back value, a server that's gone). Trim on every miss rather than on a
           timer -- this cache has no background thread, and a miss is exactly when growth happens. */
        foreach (var entry in _blocks)
        {
            if (nowUtc >= entry.Value.ExpiresAtUtc)
            {
                _blocks.TryRemove(entry); // KeyValuePair overload: never removes a block another caller just replaced
            }
        }

        while (_blocks.Count >= MaxBlocks && !_blocks.IsEmpty)
        {
            var oldest = _blocks.MinBy(entry => entry.Value.ComputedAtUtc);
            if (!_blocks.TryRemove(oldest))
            {
                break; // another caller changed it; the next miss trims again
            }
        }

        _blocks[key] = new CachedBlock(closedRows, closedEndUtc, floorUtc, nowUtc, BlockExpiryUtc(nowUtc));
        return full;
    }

    /// <summary>#4232: drops every cached block, for a test that needs its next read to hit the store rather than
    /// serve a block a prior call in the same process warmed. Public rather than <c>internal</c> -- this type's
    /// assembly (<c>PerformanceMonitor.Darling.Storage</c>) does not grant <c>InternalsVisibleTo</c> to the
    /// service, only to <c>Darling.Tests</c>, so a caller reaching this through the service's own reset seam
    /// (<c>DarlingHealthReader.ResetRangeCacheForTests</c>) needs a public member to call.</summary>
    public void Clear() => _blocks.Clear();
}
