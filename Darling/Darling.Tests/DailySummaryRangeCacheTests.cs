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
        Assert.Contains((D(1), D(4)), store.Reads); // the purge-exposed days are read, only they
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
        Assert.Contains((D(1), D(10)), store.Reads);
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
}
