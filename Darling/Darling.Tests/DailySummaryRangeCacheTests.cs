/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4232 ruling item 8: the closed-day range cache, pinned as pure logic against a stub <c>runRange</c> delegate
/// -- no database involved. <see cref="TestRow"/> stands in for a caller's per-day row (<c>DailySummaryReadRow</c>
/// / the viewer's raw parse); only <c>Day</c> matters to the cache itself.
/// </summary>
public sealed class DailySummaryRangeCacheTests
{
    private sealed record TestRow(DateTime Day, int Value);

    private const int ServerId = 1;
    private const string RoutedSql = "SELECT 1 /* test */";

    private static DateTime D(int day) => new(2026, 9, day, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task GetRangeAsync_LateRowForYesterdayInsideGrace_CachedAnswerEqualsFreshFullStatement()
    {
        /* Clock at 01:00 UTC on the 25th: inside the two-hour grace, so the 24th (yesterday) is still open. */
        var now = new DateTime(2026, 9, 25, 1, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);

        /* The "true" store: mutable so a second read can see a late-arriving row for yesterday, exactly like a
           collector landing a row a few minutes after midnight. */
        var store = new Dictionary<DateTime, int>
        {
            [D(1)] = 100,
            [D(24)] = 1, // yesterday, before its late row lands
        };

        Task<List<TestRow>> Fresh(DateTime start, DateTime end, CancellationToken ct) => Task.FromResult(
            store.Where(kv => kv.Key >= start && kv.Key < end)
                 .OrderBy(kv => kv.Key)
                 .Select(kv => new TestRow(kv.Key, kv.Value))
                 .ToList());

        var fromDate = D(1);
        var toDate = D(26); // half-open range covering the 1st..25th

        var first = await cache.GetRangeAsync(
            storeKey: "store", serverId: ServerId, fromDate: fromDate, toDate: toDate, routedSql: RoutedSql,
            asOfNow: true, day: r => r.Day, runRange: Fresh, cancellationToken: CancellationToken.None);
        Assert.Contains(first, r => r.Day == D(24) && r.Value == 1);

        /* The late row lands for yesterday. */
        store[D(24)] = 2;

        var second = await cache.GetRangeAsync(
            storeKey: "store", serverId: ServerId, fromDate: fromDate, toDate: toDate, routedSql: RoutedSql,
            asOfNow: true, day: r => r.Day, runRange: Fresh, cancellationToken: CancellationToken.None);

        var expected = await Fresh(fromDate, toDate, CancellationToken.None);
        Assert.Equal(expected, second);
        Assert.Contains(second, r => r.Day == D(24) && r.Value == 2);
    }

    [Fact]
    public async Task GetRangeAsync_SecondRefresh_RunsStatementOnlyOverOpenDays()
    {
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc); // well past the grace: only the 25th is open
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var calls = new List<(DateTime Start, DateTime End)>();

        Task<List<TestRow>> Recording(DateTime start, DateTime end, CancellationToken ct)
        {
            calls.Add((start, end));
            return Task.FromResult(new List<TestRow> { new(start, 0) });
        }

        var fromDate = D(1);
        var toDate = D(26);

        await cache.GetRangeAsync("store", ServerId, fromDate, toDate, RoutedSql, true, r => r.Day, Recording, CancellationToken.None);
        await cache.GetRangeAsync("store", ServerId, fromDate, toDate, RoutedSql, true, r => r.Day, Recording, CancellationToken.None);

        Assert.Equal(2, calls.Count);
        Assert.Equal((fromDate, toDate), calls[0]); // first refresh: the whole range
        Assert.Equal((D(25), toDate), calls[1]); // second refresh: only the open day
    }

    [Fact]
    public async Task GetRangeAsync_GraceBoundaryCrossesMidTtl_DoesNotDropYesterday()
    {
        /* Block built late in yesterday's grace window (01:45, grace ends 02:00), so the block's OWN
           closed-end boundary excludes yesterday. Moved 25 minutes later (02:10): still inside the block's
           lifetime from 01:45, but the grace has now expired, so a fresh call computes a DIFFERENT closed-end
           boundary (today, not yesterday). A cache that trusted the stale block's ClosedRows here and only
           re-read the (now-advanced) open portion would read neither the stale closed block's excluded
           yesterday nor the open re-read's [today, end) -- yesterday would vanish from the join entirely. */
        var now = new DateTime(2026, 9, 25, 1, 45, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);

        var store = new Dictionary<DateTime, int> { [D(23)] = 10, [D(24)] = 20, [D(25)] = 30 };
        Task<List<TestRow>> Fresh(DateTime start, DateTime end, CancellationToken ct) => Task.FromResult(
            store.Where(kv => kv.Key >= start && kv.Key < end)
                 .OrderBy(kv => kv.Key)
                 .Select(kv => new TestRow(kv.Key, kv.Value))
                 .ToList());

        var fromDate = D(1);
        var toDate = D(26);

        await cache.GetRangeAsync("store", ServerId, fromDate, toDate, RoutedSql, true, r => r.Day, Fresh, CancellationToken.None);

        now = now.AddMinutes(25); // 02:10 -- grace expired, still inside the block's lifetime
        var second = await cache.GetRangeAsync("store", ServerId, fromDate, toDate, RoutedSql, true, r => r.Day, Fresh, CancellationToken.None);

        Assert.Contains(second, r => r.Day == D(24) && r.Value == 20);
    }

    [Theory]
    [InlineData(2026, 9, 25, 0, 0, 24)] // midnight exactly: still inside grace -> yesterday
    [InlineData(2026, 9, 25, 1, 59, 24)] // just under two hours -> yesterday
    [InlineData(2026, 9, 25, 2, 0, 25)] // exactly two hours -> today, grace just expired
    [InlineData(2026, 9, 25, 12, 0, 25)] // mid-afternoon -> today
    public void OpenStartUtc_TwoHourGraceAfterMidnight(int y, int m, int d, int h, int min, int expectedDay)
    {
        var now = new DateTime(y, m, d, h, min, 0, DateTimeKind.Utc);
        Assert.Equal(D(expectedDay), DailySummaryRangeCache<TestRow>.OpenStartUtc(now));
    }

    [Fact]
    public async Task GetRangeAsync_ExpiredBlock_EvictedOnNextMissForAnotherKey_AndOldKeyMissesAgain()
    {
        /* #4232 follow-up: the cache never removed a block on its own -- every distinct server/range added one
           that nothing replaced. Here two different servers share a range, so each gets its own block, and
           moving the clock past the block's expiry before the second server's first (miss) call must sweep server 1's
           now-stale block, not just leave it to rot. */
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var calls = new List<(int ServerId, DateTime Start, DateTime End)>();

        Task<List<TestRow>> Recording(int serverId, DateTime start, DateTime end, CancellationToken ct)
        {
            calls.Add((serverId, start, end));
            return Task.FromResult(new List<TestRow> { new(start, 0) });
        }

        var fromDate = D(1);
        var toDate = D(26);

        await cache.GetRangeAsync("store", 1, fromDate, toDate, RoutedSql, true, r => r.Day,
            (s, e, ct) => Recording(1, s, e, ct), CancellationToken.None);
        Assert.Equal(1, cache.Count);

        now = now.Date.AddDays(1).Add(DailySummaryRangeCache<TestRow>.ClosedGrace).AddMinutes(1); // past server 1's block expiry

        await cache.GetRangeAsync("store", 2, fromDate, toDate, RoutedSql, true, r => r.Day,
            (s, e, ct) => Recording(2, s, e, ct), CancellationToken.None);
        Assert.Equal(1, cache.Count); // server 1's expired block swept, server 2's fresh block is the only one left

        var callsBeforeReread = calls.Count;
        await cache.GetRangeAsync("store", 1, fromDate, toDate, RoutedSql, true, r => r.Day,
            (s, e, ct) => Recording(1, s, e, ct), CancellationToken.None);
        Assert.Equal(callsBeforeReread + 1, calls.Count); // server 1's block is gone -- this is a miss, not a hit
        Assert.Equal((1, fromDate, toDate), calls[^1]); // whole range re-run, exactly like the very first read
    }

    [Fact]
    public async Task GetRangeAsync_MoreThanMaxBlocksDistinctKeys_CountStaysCapped_OldestKeysDropped()
    {
        /* #4232 follow-up: the key carries the range, so a fleet with many servers (or one MCP client stepping
           through days_back values) can add a block per distinct key forever. Filling MaxBlocks + 10 of them,
           each a fresh key, must cap Count at MaxBlocks by dropping the OLDEST blocks first, not the newest. */
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);

        var fromDate = D(1);
        var toDate = D(26);
        var total = DailySummaryRangeCache<TestRow>.MaxBlocks + 10;

        Task<List<TestRow>> Fresh(DateTime start, DateTime end, CancellationToken ct)
            => Task.FromResult(new List<TestRow> { new(start, 0) });

        for (var serverId = 1; serverId <= total; serverId++)
        {
            await cache.GetRangeAsync("store", serverId, fromDate, toDate, RoutedSql, true, r => r.Day, Fresh, CancellationToken.None);
            now = now.AddSeconds(1); // distinct ComputedAtUtc per block, well inside every block's lifetime for the whole loop
        }

        Assert.Equal(DailySummaryRangeCache<TestRow>.MaxBlocks, cache.Count);

        /* The oldest 10 keys (server 1..10) were evicted to make room for the newest ones. A read for the
           very first server is therefore a miss (runRange called again over the whole range). */
        var calls = new List<(DateTime Start, DateTime End)>();
        Task<List<TestRow>> Recording(DateTime start, DateTime end, CancellationToken ct)
        {
            calls.Add((start, end));
            return Task.FromResult(new List<TestRow> { new(start, 0) });
        }

        await cache.GetRangeAsync("store", 1, fromDate, toDate, RoutedSql, true, r => r.Day, Recording, CancellationToken.None);
        Assert.Single(calls);
        Assert.Equal((fromDate, toDate), calls[0]); // server 1's original block is gone -- whole range re-run

        Assert.True(cache.Count <= DailySummaryRangeCache<TestRow>.MaxBlocks); // the re-add still respects the cap
    }

    [Fact]
    public async Task GetRangeAsync_ExplicitEndTime_SkipsCacheEveryTime()
    {
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var calls = new List<(DateTime Start, DateTime End)>();

        Task<List<TestRow>> Recording(DateTime start, DateTime end, CancellationToken ct)
        {
            calls.Add((start, end));
            return Task.FromResult(new List<TestRow> { new(start, 0) });
        }

        var fromDate = D(1);
        var toDate = D(26);

        await cache.GetRangeAsync("store", ServerId, fromDate, toDate, RoutedSql, asOfNow: false, r => r.Day, Recording, CancellationToken.None);
        await cache.GetRangeAsync("store", ServerId, fromDate, toDate, RoutedSql, asOfNow: false, r => r.Day, Recording, CancellationToken.None);

        Assert.Equal(2, calls.Count);
        Assert.All(calls, call => Assert.Equal((fromDate, toDate), call)); // never narrowed -- no block ever built
    }

    /* ---- #6809: a closed day is kept until the next UTC day boundary plus ClosedGrace, not one hour ---- */

    /// <summary>A store that counts reads, so a test can say "served from cache, no store read".</summary>
    private sealed class CountingStore
    {
        public readonly Dictionary<DateTime, int> Days = new();
        public readonly List<(DateTime Start, DateTime End)> Reads = new();

        public Task<List<TestRow>> Run(DateTime start, DateTime end, CancellationToken ct)
        {
            Reads.Add((start, end));
            return Task.FromResult(Days.Where(kv => kv.Key >= start && kv.Key < end)
                .OrderBy(kv => kv.Key).Select(kv => new TestRow(kv.Key, kv.Value)).ToList());
        }
    }

    private static CountingStore Month()
    {
        var store = new CountingStore();
        for (var d = 1; d <= 25; d++) store.Days[D(d)] = d;
        return store;
    }

    [Fact]
    public async Task ClosedDay_FetchedAt1000_ServedFromCacheAt1059_1101_and2359_WithoutAClosedDayRead()
    {
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Month();

        var first = await cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        Assert.Equal(25, first.Count);
        Assert.Single(store.Reads);

        foreach (var (h, m) in new[] { (10, 59), (11, 1), (23, 59) })
        {
            now = new DateTime(2026, 9, 25, h, m, 0, DateTimeKind.Utc);
            var rows = await cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
            Assert.Equal(25, rows.Count);
            Assert.Equal(Enumerable.Range(1, 25).Select(D), rows.Select(r => r.Day));
        }

        /* Three more reads, each only today's partial day: no read ever touches a closed day. */
        Assert.Equal(4, store.Reads.Count);
        Assert.All(store.Reads.Skip(1), read => Assert.Equal((D(25), D(26)), read));
    }

    [Fact]
    public async Task TodaysPartialDay_IsReadLiveEveryCall_AndSeesNewRows()
    {
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Month();

        await cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        store.Days[D(25)] = 999; // today keeps filling
        now = now.AddHours(5);
        var rows = await cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);

        Assert.Equal(999, rows.Single(r => r.Day == D(25)).Value);
        Assert.Equal((D(25), D(26)), store.Reads[^1]);
    }

    [Fact]
    public async Task NextUtcDay_NewlyClosedDayIsFetchedOnce_ThenCachedAgain()
    {
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Month();
        var from = D(1);
        var to = D(26);

        await cache.GetRangeAsync("s", ServerId, from, to, RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);

        /* 26 Sep 02:30: past the grace, so the 25th is closed. A caller's window now ends on the 27th; whatever
           the key does, the 25th is read once and then cached. */
        now = new DateTime(2026, 9, 26, 2, 30, 0, DateTimeKind.Utc);
        store.Days[D(26)] = 26;
        to = D(27);
        store.Reads.Clear();
        var rows = await cache.GetRangeAsync("s", ServerId, from, to, RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        Assert.Contains(rows, r => r.Day == D(25));
        Assert.Contains(rows, r => r.Day == D(26));

        store.Reads.Clear();
        now = now.AddHours(3);
        await cache.GetRangeAsync("s", ServerId, from, to, RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        Assert.All(store.Reads, read => Assert.True(read.Start >= D(26), $"read reached back to {read.Start}"));
    }

    [Fact]
    public async Task BlockBuiltInsideGrace_WhenGraceEnds_ReadsOnlyTheNewlyClosedDay()
    {
        /* Built at 01:00 on the 25th: the 24th is still open, closed days end at the 23rd. At 02:30 the 24th is closed. */
        var now = new DateTime(2026, 9, 25, 1, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Month();

        await cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        store.Reads.Clear();

        now = new DateTime(2026, 9, 25, 2, 30, 0, DateTimeKind.Utc);
        var rows = await cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        Assert.Equal(25, rows.Count);
        Assert.Equal((D(24), D(26)), Assert.Single(store.Reads)); // two days, not the range

        store.Reads.Clear();
        now = now.AddHours(1);
        await cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        Assert.Equal((D(25), D(26)), Assert.Single(store.Reads)); // the 24th is now cached too
    }

    [Fact]
    public async Task LateRowForClosedDay_ShowsAfterNextUtcDayPlusGrace_NotBefore()
    {
        /* The bound this change accepts: a row that lands for a closed day after the block was built shows once the
           block expires (next UTC midnight plus the grace). */
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Month();
        var from = D(1);
        var to = D(26);

        await cache.GetRangeAsync("s", ServerId, from, to, RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        store.Days[D(20)] = 2000; // late backfill

        now = new DateTime(2026, 9, 26, 1, 59, 0, DateTimeKind.Utc);
        var before = await cache.GetRangeAsync("s", ServerId, from, to, RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        Assert.Equal(20, before.Single(r => r.Day == D(20)).Value);

        now = new DateTime(2026, 9, 26, 2, 0, 0, DateTimeKind.Utc);
        var after = await cache.GetRangeAsync("s", ServerId, from, to, RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        Assert.Equal(2000, after.Single(r => r.Day == D(20)).Value);
    }

    [Fact]
    public async Task PastHorizonDays_InsideTheSubHorizonTtl_AreServedNotReadPerCall()
    {
        /* A Viewer calendar on an older month: most of the window is before the retention horizon. */
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Month();
        var trustFrom = D(21); // the 20th is the horizon day

        Task<List<TestRow>> Read() => cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day,
            store.Run, "", CancellationToken.None, trustFrom);

        await Read();
        store.Reads.Clear();

        /* Ten auto-refresh ticks inside the hour: not one of them touches a closed day, past the horizon or not. */
        for (var i = 1; i <= 10; i++)
        {
            now = new DateTime(2026, 9, 25, 10, 5 * i, 0, DateTimeKind.Utc);
            var rows = await Read();
            Assert.Equal(25, rows.Count);
        }

        Assert.Equal(10, store.Reads.Count);
        Assert.All(store.Reads, read => Assert.Equal((D(25), D(26)), read));
    }

    [Fact]
    public async Task PurgeOfPastHorizonDays_ShowsAfterTheSubHorizonTtl_ReadingOnlyThoseDays()
    {
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Month();
        var trustFrom = D(4); // horizon is the 3rd

        Task<List<TestRow>> Read() => cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day,
            store.Run, "", CancellationToken.None, trustFrom);

        await Read();
        store.Days.Remove(D(2)); // the purge deletes these days' rows
        store.Days.Remove(D(3));
        store.Days.Remove(D(10)); // inside retention: not a purge target, so the block still holds it
        store.Days[D(4)] = 999; // the day after the segment: read with it, never taken from that read
        store.Reads.Clear();

        now = now.AddMinutes(59);
        var early = await Read();
        Assert.Contains(early, r => r.Day == D(2)); // still inside the sub-horizon bound: served, no read
        Assert.DoesNotContain(store.Reads, read => read.Start < D(25));

        now = now.AddMinutes(1);
        store.Reads.Clear();
        var rows = await Read();

        Assert.DoesNotContain(rows, r => r.Day == D(2) || r.Day == D(3));
        Assert.Contains(rows, r => r.Day == D(10)); // cached
        /* The purge-exposed days are read with the one day after them (a blocking wait that crosses the segment's
           last midnight is counted on its own day only when that day is in the read), and that day's row stays the
           block's. */
        Assert.Contains((D(1), D(5)), store.Reads);
        Assert.Equal(4, rows.Single(r => r.Day == D(4)).Value);
        Assert.DoesNotContain(store.Reads, read => read.Start >= D(4) && read.Start < D(25));

        /* And the next tick is cached again. */
        store.Reads.Clear();
        now = now.AddMinutes(5);
        await Read();
        Assert.Equal((D(25), D(26)), Assert.Single(store.Reads));
    }

    [Fact]
    public async Task RetentionShortenedAfterBlockBuilt_ReadsTheNewlyUntrustedDaysAtOnce()
    {
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Month();

        Task<List<TestRow>> Read(DateTime? trustFrom) => cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day,
            store.Run, "", CancellationToken.None, trustFrom);

        await Read(D(4));
        store.Days.Remove(D(8)); // the shorter retention purged it
        store.Reads.Clear();

        now = now.AddMinutes(5);
        var rows = await Read(D(10)); // the floor rose to the 10th inside the hour
        Assert.DoesNotContain(rows, r => r.Day == D(8));
        Assert.Contains((D(1), D(11)), store.Reads); // the newly untrusted days and the one day after them
    }

    [Fact]
    public async Task RetentionRaisedAfterBlockBuilt_ServesTheDaysBelowTheOldFloorWithoutAHole()
    {
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Month();

        Task<List<TestRow>> Read(DateTime? trustFrom) => cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day,
            store.Run, "", CancellationToken.None, trustFrom);

        await Read(D(10)); // built under a floor of the 10th: days 1..9 are the sub-horizon segment
        store.Reads.Clear();

        /* Retention raised: days 4..9 are trusted now. The read stays INSIDE the sub-horizon TTL, so nothing re-reads
           days 1..9: they can only be here if the block stored every closed day, not just the ones from the floor
           up. (An hour later the segment refresh would fill the same days from the store and hide the defect.) */
        now = now.AddMinutes(5);
        var rows = await Read(D(4));
        Assert.Equal(25, rows.Count);
        Assert.Contains(rows, r => r.Day == D(6));
        Assert.Equal((D(25), D(26)), Assert.Single(store.Reads)); // the open tail only: a hit, not a rebuild
    }

    [Fact]
    public async Task AsOfRead_NeverUsesTheBlock()
    {
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Month();

        await cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day, store.Run, CancellationToken.None);
        store.Reads.Clear();
        await cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, false, r => r.Day, store.Run, CancellationToken.None);

        Assert.Equal((D(1), D(26)), Assert.Single(store.Reads));
    }

    /// <summary>
    /// #8549: the warmer builds the Overview's 31-day block, and an MCP call with only server_name asks for 30 days,
    /// the same days under another key. Before, that call read the whole month cold. Now it starts from the 31-day
    /// block's closed days: its statement runs over the open days only, and its rows equal a fresh full read.
    /// </summary>
    [Fact]
    public async Task GetRangeAsync_ShorterRangeEndingTheSameDay_ReusesTheLongerBlock_AndRunsOnlyTheOpenDays()
    {
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var store = Enumerable.Range(1, 25).ToDictionary(n => D(n), n => n * 10);
        var runs = new List<(DateTime Start, DateTime End)>();

        Task<List<TestRow>> Fresh(DateTime start, DateTime end, CancellationToken ct)
        {
            runs.Add((start, end));
            return Task.FromResult(store.Where(kv => kv.Key >= start && kv.Key < end)
                .OrderBy(kv => kv.Key).Select(kv => new TestRow(kv.Key, kv.Value)).ToList());
        }

        var toDate = D(26);
        await cache.GetRangeAsync("store", ServerId, D(1), toDate, RoutedSql, true, r => r.Day, Fresh, CancellationToken.None);
        Assert.Single(runs);

        var shorter = await cache.GetRangeAsync("store", ServerId, D(2), toDate, RoutedSql, true, r => r.Day, Fresh, CancellationToken.None);

        Assert.Equal(2, runs.Count);
        Assert.Equal((D(25), toDate), runs[1]); // today only: no second full read
        Assert.Equal(await Fresh(D(2), toDate, CancellationToken.None), shorter);

        /* A range that starts BEFORE the block has no donor and reads in full, as it always did. */
        runs.Clear();
        await cache.GetRangeAsync("store", ServerId, D(1).AddDays(-1), toDate, RoutedSql, true, r => r.Day, Fresh, CancellationToken.None);
        Assert.Equal((D(1).AddDays(-1), toDate), Assert.Single(runs));
    }

    /// <summary>#8549: the cut of a cold range. Twenty-four closed days and four parts are four runs of six days, newest
    /// first; the newest is read to the range's end (it owns the open day), every other one a day past its own end.</summary>
    [Fact]
    public void MissSubRanges_CutsTheClosedDaysIntoBalancedRuns_NewestFirst_EachOlderOneReadADayPastItsEnd()
    {
        var plan = DailySummaryRangeCache.MissSubRanges(D(1), D(26), D(25), 4);

        Assert.Equal(
            new[] { (D(19), D(26), D(26)), (D(13), D(19), D(20)), (D(7), D(13), D(14)), (D(1), D(7), D(8)) },
            plan);

        /* Twenty-two closed days: five each and two left over, which the two oldest runs take. */
        Assert.Equal(
            new[] { (D(20), D(26), D(26)), (D(15), D(20), D(21)), (D(9), D(15), D(16)), (D(3), D(9), D(10)) },
            DailySummaryRangeCache.MissSubRanges(D(3), D(26), D(25), 4));

        /* A range that is closed to its end has no open day: the newest run still ends at the range's end. */
        Assert.Equal(
            new[] { (D(4), D(7), D(7)), (D(1), D(4), D(5)) },
            DailySummaryRangeCache.MissSubRanges(D(1), D(7), D(7), 4));
    }

    [Fact]
    public void MissSubRanges_ReadsAShortOrUnalignedRangeAsOneStatement()
    {
        Assert.Empty(DailySummaryRangeCache.MissSubRanges(D(1), D(26), D(25), 1)); // one part is the one statement
        Assert.Empty(DailySummaryRangeCache.MissSubRanges(D(20), D(26), D(25), 4)); // five closed days: under two runs of three
        Assert.Empty(DailySummaryRangeCache.MissSubRanges(D(1).AddHours(6), D(26), D(25), 4)); // not a whole UTC day
        Assert.Equal(2, DailySummaryRangeCache.MissSubRanges(D(19), D(26), D(25), 4).Count); // six closed days: two runs of three
    }

    /// <summary>A stand-in for the caller's side-by-side read: each range is read from the store, and the ranges asked
    /// for are kept so a test can see them.</summary>
    private static Func<IReadOnlyList<(DateTime Start, DateTime End)>, CancellationToken, Task<IReadOnlyList<List<TestRow>>>> SplitRead(
        CountingStore store, List<(DateTime Start, DateTime End)> asked) => async (ranges, ct) =>
        {
            asked.AddRange(ranges);
            var parts = new List<List<TestRow>>();
            foreach (var (start, end) in ranges) parts.Add(await store.Run(start, end, ct));
            return parts;
        };

    /// <summary>#8549: a miss read as sub-ranges answers and stores what the one statement does. Two caches read the same
    /// store, one as one statement and one as four sub-ranges. The rows are equal, and so are the rows of the next call,
    /// which each cache answers from its block after the store's closed days have changed.</summary>
    [Fact]
    public async Task AMissReadAsSubRanges_ReturnsAndStoresTheOneStatementsRows()
    {
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var store = Month();
        var whole = new DailySummaryRangeCache<TestRow>(() => now);
        var split = new DailySummaryRangeCache<TestRow>(() => now);
        var asked = new List<(DateTime Start, DateTime End)>();

        Task<List<TestRow>> ReadWhole() => whole.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day,
            store.Run, "", CancellationToken.None);
        Task<List<TestRow>> ReadSplit() => split.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day,
            store.Run, "", CancellationToken.None, missParts: 4, runMissRanges: SplitRead(store, asked));

        var expected = await ReadWhole();
        store.Reads.Clear();
        var rows = await ReadSplit();

        Assert.Equal(expected, rows);
        Assert.Equal(new[] { (D(19), D(26)), (D(13), D(20)), (D(7), D(14)), (D(1), D(8)) }, asked);
        Assert.Equal(asked, store.Reads); // the sub-ranges are the whole read: no one statement beside them

        /* The store's closed days change. Both caches hold them, so both answer the closed days as first read and
           read only the open day. */
        for (var d = 1; d <= 24; d++) store.Days[D(d)] = 1000 + d;
        store.Reads.Clear();
        now = now.AddMinutes(5);
        var wholeAgain = await ReadWhole();
        var splitAgain = await ReadSplit();

        Assert.Equal(expected, wholeAgain);
        Assert.Equal(wholeAgain, splitAgain);
        Assert.Equal(new[] { (D(25), D(26)), (D(25), D(26)) }, store.Reads);
        Assert.Equal(4, asked.Count); // the hit ran no sub-range
    }

    /// <summary>#8549: the day an older sub-range reads past its own end is only there so that sub-range counts its last
    /// day right; its row belongs to the sub-range that owns that day. Every row here carries the start of the sub-range
    /// that read it, so a returned or stored row read by the wrong sub-range shows.</summary>
    [Fact]
    public async Task TheDayReadPastASubRangesEnd_IsNeitherReturnedNorStoredFromThatSubRange()
    {
        var now = new DateTime(2026, 9, 25, 10, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var tailReads = 0;

        static List<TestRow> Days(DateTime start, DateTime end) => Enumerable.Range(0, (int)(end - start).TotalDays)
            .Select(i => new TestRow(start.AddDays(i), start.Day)).ToList();

        Task<List<TestRow>> Read() => cache.GetRangeAsync("s", ServerId, D(1), D(26), RoutedSql, true, r => r.Day,
            (start, end, _) =>
            {
                tailReads++;
                return Task.FromResult(Days(start, end));
            },
            "", CancellationToken.None, missParts: 4,
            runMissRanges: (ranges, _) => Task.FromResult<IReadOnlyList<List<TestRow>>>(ranges.Select(range => Days(range.Start, range.End)).ToList()));

        /* The owner of each day under the four-part cut: 1..6, 7..12, 13..18, and 19..25 with the open day. */
        static int OwnerStart(DateTime day) => day.Day >= 19 ? 19 : day.Day >= 13 ? 13 : day.Day >= 7 ? 7 : 1;

        var rows = await Read();
        Assert.Equal(Enumerable.Range(1, 25).Select(D), rows.Select(r => r.Day)); // every day once, oldest first
        Assert.All(rows, row => Assert.Equal(OwnerStart(row.Day), row.Value));
        Assert.Equal(0, tailReads);

        /* The next call answers the closed days from the block: the days 7, 13 and 19 were each read twice on the
           miss, and the block holds the owner's row for each. */
        now = now.AddMinutes(5);
        var again = await Read();
        Assert.Equal(Enumerable.Range(1, 25).Select(D), again.Select(r => r.Day));
        Assert.All(again.Where(r => r.Day < D(25)), row => Assert.Equal(OwnerStart(row.Day), row.Value));
        Assert.Equal(1, tailReads); // the open day only
    }

    [Fact]
    public async Task GetRangeAsync_SkipOpenDayTail_OnAHeldBlock_ReadsNoOpenDay_AndAVisitorStillGetsIt()
    {
        /* #9039: the warmer throws the rows away, so on a block that holds every closed day its read must not run the statement
           over the open day. A visitor's read of the same block still returns the open day, freshly read. */
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var open = 7;
        var calls = new List<(DateTime Start, DateTime End)>();

        Task<List<TestRow>> Run(DateTime start, DateTime end, CancellationToken ct)
        {
            calls.Add((start, end));
            var rows = new List<TestRow>();
            for (var day = start; day < end; day = day.AddDays(1))
            {
                rows.Add(new TestRow(day, day == D(25) ? open : 1));
            }

            return Task.FromResult(rows);
        }

        Task<List<TestRow>> Read(bool skipOpenDayTail) => cache.GetRangeAsync(
            storeKey: "store", serverId: ServerId, fromDate: D(1), toDate: D(26), routedSql: RoutedSql,
            asOfNow: true, day: r => r.Day, runRange: Run, scopeKey: "", cancellationToken: CancellationToken.None,
            skipOpenDayTail: skipOpenDayTail);

        var cold = await Read(skipOpenDayTail: true);
        Assert.Single(calls);
        Assert.Contains(cold, r => r.Day == D(25) && r.Value == 7); /* a cold block is built whole, open day included */

        calls.Clear();
        open = 9;
        var warm = await Read(skipOpenDayTail: true);
        Assert.Empty(calls);
        Assert.DoesNotContain(warm, r => r.Day == D(25));
        Assert.Equal(24, warm.Count);

        var visitor = await Read(skipOpenDayTail: false);
        Assert.Single(calls);
        Assert.Equal((D(25), D(26)), calls[0]);
        Assert.Contains(visitor, r => r.Day == D(25) && r.Value == 9);
        Assert.Equal(25, visitor.Count);
    }

    [Fact]
    public async Task GetRangeAsync_SkipOpenDayTail_WhenTheBlockHasNotClosedYesterday_StillGrowsTheBlock()
    {
        /* Inside the grace after midnight the block's closed end is the 24th: the tail read is what closes the 24th into it. */
        var now = new DateTime(2026, 9, 25, 1, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var calls = new List<(DateTime Start, DateTime End)>();

        Task<List<TestRow>> Run(DateTime start, DateTime end, CancellationToken ct)
        {
            calls.Add((start, end));
            var rows = new List<TestRow>();
            for (var day = start; day < end; day = day.AddDays(1))
            {
                rows.Add(new TestRow(day, 1));
            }

            return Task.FromResult(rows);
        }

        Task<List<TestRow>> Read(bool skipOpenDayTail) => cache.GetRangeAsync(
            storeKey: "store", serverId: ServerId, fromDate: D(1), toDate: D(26), routedSql: RoutedSql,
            asOfNow: true, day: r => r.Day, runRange: Run, scopeKey: "", cancellationToken: CancellationToken.None,
            skipOpenDayTail: skipOpenDayTail);

        await Read(skipOpenDayTail: true);
        now = new DateTime(2026, 9, 25, 3, 0, 0, DateTimeKind.Utc); /* past the grace: the 24th is now closed */
        calls.Clear();
        await Read(skipOpenDayTail: true);
        Assert.Single(calls);
        Assert.Equal((D(24), D(26)), calls[0]);

        calls.Clear();
        await Read(skipOpenDayTail: true);
        Assert.Empty(calls); /* the block now holds the 24th, so the next warm read has no open-day work */
    }

    [Fact]
    public async Task GetRangeAsync_SkipOpenDayTail_InsideTheGrace_OnABlockClosedThroughYesterday_SkipsTheTail_AndAVisitorStillGetsToday()
    {
        /* #9047: within two hours after midnight the closed end is the 24th, so the 24th and the 25th are both open. A block built in
           that window already holds every day closed so far, so the warmer's read skips the tail and a visitor's read still runs it. */
        var now = new DateTime(2026, 9, 25, 0, 30, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<TestRow>(() => now);
        var calls = new List<(DateTime Start, DateTime End)>();

        Task<List<TestRow>> Run(DateTime start, DateTime end, CancellationToken ct)
        {
            calls.Add((start, end));
            var rows = new List<TestRow>();
            for (var day = start; day < end; day = day.AddDays(1))
            {
                rows.Add(new TestRow(day, 1));
            }

            return Task.FromResult(rows);
        }

        Task<List<TestRow>> Read(bool skipOpenDayTail) => cache.GetRangeAsync(
            storeKey: "store", serverId: ServerId, fromDate: D(1), toDate: D(26), routedSql: RoutedSql,
            asOfNow: true, day: r => r.Day, runRange: Run, scopeKey: "", cancellationToken: CancellationToken.None,
            skipOpenDayTail: skipOpenDayTail);

        await Read(skipOpenDayTail: false); /* builds the block, closed through the 23rd */

        calls.Clear();
        var skipped = await Read(skipOpenDayTail: true);
        Assert.Empty(calls);
        Assert.DoesNotContain(skipped, r => r.Day == D(25));

        var visitor = await Read(skipOpenDayTail: false);
        Assert.Single(calls);
        Assert.Equal((D(24), D(26)), calls[0]);
        Assert.Contains(visitor, r => r.Day == D(25));
    }
}
