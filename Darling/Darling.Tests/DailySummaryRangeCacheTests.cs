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
    public async Task GetRangeAsync_WithinOneHour_ReusesBlock_AfterOneHour_RecomputesWhole()
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

        await cache.GetRangeAsync("store", ServerId, fromDate, toDate, RoutedSql, true, r => r.Day, Recording, CancellationToken.None);

        now = now.AddMinutes(59); // still inside the one-hour block TTL
        await cache.GetRangeAsync("store", ServerId, fromDate, toDate, RoutedSql, true, r => r.Day, Recording, CancellationToken.None);
        Assert.Equal(2, calls.Count);
        Assert.Equal((D(25), toDate), calls[1]); // open-only, block still trusted

        now = now.AddMinutes(2); // now 61 minutes after the block was built -- past BlockTtl
        await cache.GetRangeAsync("store", ServerId, fromDate, toDate, RoutedSql, true, r => r.Day, Recording, CancellationToken.None);
        Assert.Equal(3, calls.Count);
        Assert.Equal((fromDate, toDate), calls[2]); // whole range recomputed
    }

    [Fact]
    public async Task GetRangeAsync_GraceBoundaryCrossesMidTtl_DoesNotDropYesterday()
    {
        /* Block built late in yesterday's grace window (01:45, grace ends 02:00), so the block's OWN
           closed-end boundary excludes yesterday. Moved 25 minutes later (02:10): still inside the one-hour
           BlockTtl from 01:45, but the grace has now expired, so a fresh call computes a DIFFERENT closed-end
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

        now = now.AddMinutes(25); // 02:10 -- grace expired, still inside the one-hour block TTL
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
           moving the clock past BlockTtl before the second server's first (miss) call must sweep server 1's
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

        now = now.Add(DailySummaryRangeCache<TestRow>.BlockTtl).AddMinutes(1); // past BlockTtl for server 1's block

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
            now = now.AddSeconds(1); // distinct ComputedAtUtc per block, well inside BlockTtl for the whole loop
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
}
