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
/// This holds the CLOSED portion of one range as one block for one hour; every refresh inside that hour
/// re-runs the statement only over the days still open and joins them to the block. After an hour, one refresh
/// recomputes the whole range and repopulates it. No migration, no store job -- this lives entirely on the
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
/// move inside the cache's one-hour lifetime, and a stale stamp baked into a cached closed day would be wrong
/// on its own terms, not just "a day compared to the range". <typeparamref name="TRow"/> should also be
/// immutable (a record or struct) -- the same row instances are handed to every caller within a block's
/// lifetime.</para>
/// </summary>
public sealed class DailySummaryRangeCache<TRow>
{
    /// <summary>How long one closed-days block is trusted before a refresh recomputes the whole range again.</summary>
    public static readonly TimeSpan BlockTtl = TimeSpan.FromHours(1);

    /// <summary>How long after midnight UTC yesterday stays in the open (always-recomputed) set.</summary>
    public static readonly TimeSpan ClosedGrace = TimeSpan.FromHours(2);

    private readonly Func<DateTime> _clock;
    private readonly ConcurrentDictionary<BlockKey, CachedBlock> _blocks = new();

    /// <param name="clock">Defaults to the wall clock; tests pass one that can move (the same
    /// <c>Func&lt;DateTime&gt;?</c> shape <c>RdsLogSource</c> already uses), so the one-hour and two-hour
    /// boundaries can be pinned without a real sleep.</param>
    public DailySummaryRangeCache(Func<DateTime>? clock = null)
        => _clock = clock ?? (() => DateTime.UtcNow);

    private readonly record struct BlockKey(object StoreKey, int ServerId, DateTime FromDate, DateTime ToDate, string RoutedSql);

    private sealed record CachedBlock(List<TRow> ClosedRows, DateTime ClosedEndUtc, DateTime ComputedAtUtc);

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
    /// <param name="day">Reads a row's UTC day, to split a freshly-read whole range into its closed/open rows.</param>
    /// <param name="runRange">Runs the statement for an arbitrary half-open [start, end) sub-range and returns its
    /// rows, oldest day first. Called with the WHOLE [<paramref name="fromDate"/>, <paramref name="toDate"/>) on a
    /// cache miss, and with only the open sub-range on a hit.</param>
    public async Task<List<TRow>> GetRangeAsync(
        object storeKey,
        int serverId,
        DateTime fromDate,
        DateTime toDate,
        string routedSql,
        bool asOfNow,
        Func<TRow, DateTime> day,
        Func<DateTime, DateTime, CancellationToken, Task<List<TRow>>> runRange,
        CancellationToken cancellationToken = default)
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
        var key = new BlockKey(storeKey, serverId, fromDate, toDate, routedSql);

        /* The block's OWN closed-end boundary must match what "now" computes THIS call, not just be within
           BlockTtl -- otherwise a block built before yesterday's two-hour grace expired would still exclude
           yesterday from ClosedRows, and a caller who only re-read [today, toDate) after the grace expired
           would never read yesterday again: it is neither in the stale closed block nor in the open re-read.
           Falling through to a full recompute below is what keeps that from being a silent gap. */
        if (_blocks.TryGetValue(key, out var cached)
            && nowUtc - cached.ComputedAtUtc < BlockTtl
            && cached.ClosedEndUtc == closedEndUtc)
        {
            if (closedEndUtc >= toDate)
            {
                return new List<TRow>(cached.ClosedRows);
            }

            var openFromUtc = fromDate > openStartUtc ? fromDate : openStartUtc;
            var openRows = await runRange(openFromUtc, toDate, cancellationToken).ConfigureAwait(false);
            var joined = new List<TRow>(cached.ClosedRows.Count + openRows.Count);
            joined.AddRange(cached.ClosedRows);
            joined.AddRange(openRows);
            return joined;
        }

        var full = await runRange(fromDate, toDate, cancellationToken).ConfigureAwait(false);
        var closedRows = full.Where(row => day(row) < closedEndUtc).ToList();
        _blocks[key] = new CachedBlock(closedRows, closedEndUtc, nowUtc);
        return full;
    }
}
