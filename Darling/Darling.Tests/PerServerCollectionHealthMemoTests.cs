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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The single-flight memo over <c>get_collection_health</c>'s per-server 7-day read (#3856) — the twin of the
/// fleet rollup's (#3735/#3738, <see cref="FleetCollectionHealthMemoTests"/>). The production photo:
/// 2026-09-21 21:3xZ, the largest production store, the <c>mcp</c> role's 15 s <c>statement_timeout</c>
/// cancelled this read (57014) for the first time on record inside the hourly successor materializations'
/// write burst; the sequential retry succeeded. One caller, so no fan-out amplifier — the exposure is the
/// band, which is why it took a week to appear once and why memoizing it is the answer rather than touching
/// the plan or the cap, neither of which this change goes near.
///
/// <para>These pins hold what the fix promises: N concurrent callers of one server cost ONE statement even
/// with a fleet call racing beside them, a call inside the minute is a memo hit whose
/// <c>collection_health_age_seconds</c> is above zero, the window LENGTH is in the KEY so a genuinely
/// different window falls through to its own read rather than being served someone else's, and the two
/// servers' readings never cross. The key is the length and not the start since #3894 — as the start, it
/// differed by ticks on every production call, so the memo never hit once and its dictionary never evicted;
/// the last two pins in this file are the ones that would have caught that and did not exist. Style, primitive and seam are <see cref="FleetCollectionHealthMemoTests"/>' deliberately: the memo
/// is exercised through its <c>Func</c> seam with a counting scan rather than a mocked Npgsql, and the seam is
/// the SAME object the reader routes through, so a count here IS the number of statements the store would have
/// seen.</para>
/// </summary>
public sealed class PerServerCollectionHealthMemoTests
{
    private static readonly DateTime T0 = new(2026, 9, 21, 21, 34, 17, DateTimeKind.Utc);

    /// <summary>The fixed trailing window both production call sites cut. Its LENGTH is the memo key's second
    /// half (#3894); its start moves with whatever clock the caller read, which is exactly why the start
    /// cannot be the key.</summary>
    private static DateTime WindowFrom(DateTime now) => now.AddDays(-7);

    /// <summary>A scan that counts how often it was invoked, records the key and token it was handed, and
    /// completes only when the test says so — so N callers can be parked on it before it produces anything,
    /// which is the only way to prove they shared it rather than merely ran in sequence.</summary>
    private sealed class CountingScan
    {
        private int _calls;
        public TaskCompletionSource<List<CollectorHealth>> Gate { get; private set; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public List<(int ServerId, DateTime WindowStartUtc)> KeysSeen { get; } = new();
        public List<CancellationToken> TokensSeen { get; } = new();
        public int Calls => Volatile.Read(ref _calls);

        public Task<List<CollectorHealth>> Run(int serverId, DateTime windowStartUtc, CancellationToken token)
        {
            Interlocked.Increment(ref _calls);
            lock (KeysSeen)
            {
                KeysSeen.Add((serverId, windowStartUtc));
                TokensSeen.Add(token);
            }

            return Gate.Task;
        }

        public void Release(long totalRuns = 1, string collector = "query_stats")
        {
            var gate = Gate;
            Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            gate.SetResult(new List<CollectorHealth>
            {
                new() { CollectorName = collector, TotalRuns = totalRuns, SuccessCount = totalRuns },
            });
        }

        public void Fail(Exception ex)
        {
            var gate = Gate;
            Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            gate.SetException(ex);
        }
    }

    /* ───────────────────────── the issue's two pins ───────────────────────── */

    /// <summary>
    /// #3856's first pin: N concurrent per-server calls cost the store ONE statement. The fleet call racing
    /// beside them is in this test because the issue named it — and because its absence from the count is the
    /// finding the shape decision turned on: the rollup's memo is a DIFFERENT memo over a different statement,
    /// so a fleet call neither satisfies nor amplifies this read. Five callers here rather than the photo's
    /// one, because the memo has to hold for the fan-out this surface does not have yet as well as the band it
    /// already met.
    /// </summary>
    [Fact]
    public async Task ConcurrentPerServerCallers_AndAFleetCall_CostOnePerServerScan()
    {
        var memo = new DarlingDataReader.PerServerCollectionHealthMemo();
        var perServer = new CountingScan();

        var callers = Enumerable.Range(0, 5)
            .Select(_ => memo.GetAsync(7, WindowFrom(T0), T0, perServer.Run, CancellationToken.None))
            .ToArray();

        /* Every caller is in flight and the statement has run exactly once before anything completes. */
        Assert.All(callers, c => Assert.False(c.IsCompleted));
        Assert.Equal(1, perServer.Calls);
        Assert.Equal(1, memo.ScansStarted);

        /* The fleet rollup's own memo, racing: it starts its OWN scan (twelve columns, per-server counts) and
           takes nothing from this one. Both memos bound their own statement to one per minute per host; what
           they do not do is answer each other's read, which is why #3856's preferred derivation was
           unavailable and this file exists. */
        var fleet = new DarlingFleetReader.CollectionHealthMemo();
        var fleetGate = new TaskCompletionSource<Dictionary<int, DarlingFleetReader.CollectorCounts>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var fleetCall = fleet.GetAsync(T0, (_, _) => fleetGate.Task, CancellationToken.None);
        Assert.Equal(1, perServer.Calls);

        perServer.Release(totalRuns: 41);
        var results = await Task.WhenAll(callers);

        Assert.Equal(1, perServer.Calls);
        Assert.All(results, r => Assert.Equal(0, r.AgeSeconds));
        Assert.All(results, r => Assert.Same(results[0].Rows, r.Rows));
        Assert.Equal(41, results[0].Rows[0].TotalRuns);

        fleetGate.SetResult(new Dictionary<int, DarlingFleetReader.CollectorCounts>());
        await fleetCall;
        Assert.Equal(1, perServer.Calls);
    }

    /// <summary>
    /// #3856's second pin: a per-server call inside the minute is a memo HIT, and its
    /// <c>collection_health_age_seconds</c> is above zero. The age is the half that makes the hit legible —
    /// a memo whose hit cannot be told from a fresh read has not reported (#3574's self-proving-flag class),
    /// and a caller watching a collector it just fixed has to know it is reading a minute-old answer before
    /// concluding the fix did not take. The lifetime is asserted as a VALUE and against the fleet memo's, so
    /// neither a change to it nor a fork between the two twins can pass here.
    /// </summary>
    [Fact]
    public async Task ACallInsideTheMinute_IsAHit_WithAnAgeAboveZero()
    {
        Assert.Equal(TimeSpan.FromSeconds(60), DarlingDataReader.CollectionHealthMemoLifetime);
        Assert.Equal(DarlingFleetReader.CollectionHealthMemoLifetime, DarlingDataReader.CollectionHealthMemoLifetime);

        var memo = new DarlingDataReader.PerServerCollectionHealthMemo();
        var scan = new CountingScan();

        var first = memo.GetAsync(7, WindowFrom(T0), T0, scan.Run, CancellationToken.None);
        scan.Release(totalRuns: 1155);
        var fresh = await first;
        Assert.Equal(0, fresh.AgeSeconds);
        Assert.Equal(1155, fresh.Rows[0].TotalRuns);

        /* The hit, with its age: 30 s on, no statement, the same rows. */
        var hit = await memo.GetAsync(7, WindowFrom(T0), T0.AddSeconds(30), scan.Run, CancellationToken.None);
        Assert.True(hit.AgeSeconds > 0, "a memo hit published age 0, which is indistinguishable from a fresh read");
        Assert.Equal(30, hit.AgeSeconds);
        Assert.Same(fresh.Rows, hit.Rows);
        Assert.Equal(1, scan.Calls);

        /* Still a hit at 59.9 s; re-read at 60. */
        Assert.Equal(59, (await memo.GetAsync(7, WindowFrom(T0), T0.AddSeconds(59.9), scan.Run, CancellationToken.None)).AgeSeconds);
        Assert.Equal(1, scan.Calls);

        var expired = memo.GetAsync(7, WindowFrom(T0), T0.AddSeconds(60), scan.Run, CancellationToken.None);
        Assert.Equal(2, scan.Calls);
        scan.Release(totalRuns: 1160);
        var reread = await expired;
        Assert.Equal(0, reread.AgeSeconds);
        Assert.Equal(1160, reread.Rows[0].TotalRuns);
        Assert.NotSame(fresh.Rows, reread.Rows);
        Assert.Equal(2, memo.ScansStarted);
    }

    /* ───────────────────────── the key ───────────────────────── */

    /// <summary>
    /// The window is in the KEY, which is the guard #3856 asked for in place of the derivation it preferred: a
    /// caller asking for a DIFFERENT window gets its own statement rather than a reading cut from somebody
    /// else's window. This arm holds `now` fixed and varies the SPAN — seven days against one — so it pins the
    /// fall-through on the axis that still discriminates after #3894 moved the key from the window's start to
    /// its length. Both production callers ask for a trailing seven days, so production holds one key per
    /// server; before #3894 it held one per CALL, which is the defect the next two pins exist for.
    /// </summary>
    [Fact]
    public async Task ADifferentWindow_FallsThroughToItsOwnRead()
    {
        var memo = new DarlingDataReader.PerServerCollectionHealthMemo();
        var scan = new CountingScan();

        var sevenDays = memo.GetAsync(7, WindowFrom(T0), T0, scan.Run, CancellationToken.None);
        scan.Release(totalRuns: 100);
        var week = await sevenDays;

        /* Same server, same instant, a one-day window: a second statement, and the week's reading is NOT
           handed to it. */
        var oneDay = memo.GetAsync(7, T0.AddDays(-1), T0, scan.Run, CancellationToken.None);
        Assert.Equal(2, scan.Calls);
        scan.Release(totalRuns: 20);
        var day = await oneDay;
        Assert.Equal(20, day.Rows[0].TotalRuns);
        Assert.NotSame(week.Rows, day.Rows);

        /* Each key was read with its OWN window, and the week's reading is still served on its own key. */
        Assert.Equal(new[] { WindowFrom(T0), T0.AddDays(-1) }, scan.KeysSeen.Select(k => k.WindowStartUtc).ToArray());
        Assert.Same(week.Rows, (await memo.GetAsync(7, WindowFrom(T0), T0.AddSeconds(5), scan.Run, CancellationToken.None)).Rows);
        Assert.Equal(2, scan.Calls);
    }

    /// <summary>Two servers are two keys: one server's collection health can never be served as another's,
    /// which on a store reading fifty targets is the difference between a memo and a defect.</summary>
    [Fact]
    public async Task TwoServers_AreTwoKeys()
    {
        var memo = new DarlingDataReader.PerServerCollectionHealthMemo();
        var scan = new CountingScan();

        var first = memo.GetAsync(7, WindowFrom(T0), T0, scan.Run, CancellationToken.None);
        scan.Release(totalRuns: 11, collector: "query_stats");
        var seven = await first;

        var second = memo.GetAsync(9, WindowFrom(T0), T0, scan.Run, CancellationToken.None);
        Assert.Equal(2, scan.Calls);
        scan.Release(totalRuns: 22, collector: "pg_deadlocks");
        var nine = await second;

        Assert.Equal(11, seven.Rows[0].TotalRuns);
        Assert.Equal(22, nine.Rows[0].TotalRuns);
        Assert.Equal(new[] { 7, 9 }, scan.KeysSeen.Select(k => k.ServerId).ToArray());
    }

    /* ───────────────────── #3894: the two pins that did not exist ───────────────────── */

    /// <summary>
    /// TWO CALLS ON TWO CLOCKS, which is the only shape production ever takes — and the shape every pin above
    /// avoids by construction.
    ///
    /// <para>#3894: the tool reads <c>DateTime.UtcNow</c> fresh on every call and cuts its window from it, so
    /// consecutive calls differ in the window's START by however many ticks elapsed between them. While the
    /// START was the key, that made every call its own key: the memo never hit once in production, every
    /// <c>get_collection_health</c> ran its own seven-day scan during the write band #3856 existed to
    /// survive, and the payload's <c>collection_health_age_seconds</c> read 0 forever — which was the tell,
    /// sitting in the response the whole time.</para>
    ///
    /// <para>Every arm above passes a window start held constant across calls (a fixed <c>WindowFrom(T0)</c>
    /// while only <c>now</c> advances), so they prove the memo works when handed the same key and can never
    /// prove production hands it one. This arm moves BOTH clocks the way the caller does. It fails on the
    /// pre-#3894 key and passes on the length key, which is the whole reason to write it.</para>
    /// </summary>
    [Fact]
    public async Task TwoCallsOnTwoClocks_AsTheToolActuallyCallsIt_CostOneStatement()
    {
        var memo = new DarlingDataReader.PerServerCollectionHealthMemo();
        var scan = new CountingScan();

        /* Exactly the production shape: each call reads its own clock, then cuts its own trailing seven days
           from it. 250 ms apart, so both are comfortably inside the 60 s lifetime. */
        var firstClock = T0;
        var first = memo.GetAsync(7, WindowFrom(firstClock), firstClock, scan.Run, CancellationToken.None);
        scan.Release(totalRuns: 77);
        var week = await first;

        var secondClock = T0.AddMilliseconds(250);
        var repeatCall = memo.GetAsync(7, WindowFrom(secondClock), secondClock, scan.Run, CancellationToken.None);

        /* Asserted BEFORE awaiting, deliberately. A memo miss would start a second scan synchronously and
           then park on a gate nobody releases — so an await here would turn the defect into a hang, and a
           hang in CI reads as "something is slow", not "the memo missed". Counting first makes the pre-#3894
           key fail as an assertion that names the problem. The release below only matters on that failure
           path, so a regression reports instead of timing out. */
        Assert.Equal(1, scan.Calls);
        if (!repeatCall.IsCompleted)
        {
            scan.Release(totalRuns: 0);
        }

        var repeat = await repeatCall;

        /* One statement, and the second caller was served the first's rows from memory rather than its own
           scan of a window 250 ms to the left. */
        Assert.Equal(1, memo.ScansStarted);
        Assert.Same(week.Rows, repeat.Rows);
        Assert.Equal(77, repeat.Rows[0].TotalRuns);

        /* And the key space did not grow with the call count — the other half of #3894. */
        Assert.Equal(1, memo.TrackedKeys);
    }

    /// <summary>
    /// The dictionary is BOUNDED: a key whose reading has aged out and which has no statement in flight is
    /// dropped rather than held for the life of the process.
    ///
    /// <para>#3894's second half. The memo had no eviction of any kind — no <c>Remove</c>, no cap, no sweep —
    /// which was survivable only under the belief that the key space was one entry per server. With a moving
    /// window start it was one entry per CALL, each holding its rows list, in a host that runs for weeks. The
    /// bound is not a nicety: it is the difference between a memo and a leak, and nothing above could observe
    /// it because nothing above asked how many keys were left.</para>
    ///
    /// <para>Eviction runs on whether the KEY is in use, not on whether its reading is still servable. The
    /// first version of this sweep used the reading's age and deleted the last good reading a failed re-read
    /// is contractually forbidden to destroy — <c>AFailedReRead_DoesNotEvictTheLastGoodReading</c> caught it
    /// by hanging, which is why the retention here is ten lifetimes and keyed on last touch.</para>
    /// </summary>
    [Fact]
    public async Task KeysThatHaveAgedOut_AreDropped_SoTheDictionaryDoesNotGrowForever()
    {
        var memo = new DarlingDataReader.PerServerCollectionHealthMemo();
        var scan = new CountingScan();

        /* Five genuinely different windows at one instant — five legitimate keys, each with a completed
           reading. Distinct LENGTHS, since that is the key: one through five days. */
        for (var days = 1; days <= 5; days++)
        {
            var call = memo.GetAsync(7, T0.AddDays(-days), T0, scan.Run, CancellationToken.None);
            scan.Release(totalRuns: days);
            await call;
        }

        Assert.Equal(5, scan.Calls);
        Assert.Equal(5, memo.TrackedKeys);

        /* Eleven minutes later nobody has asked for any of those five windows, so the next call sweeps them
           and keeps only its own. Before #3894 this would have read 6 and kept climbing for the life of the
           host. Eleven minutes because retention is ten reading-lifetimes: long enough that an in-use key is
           never yanked, short enough that a one-off window does not occupy the host. */
        var later = T0.AddMinutes(11);
        var fresh = memo.GetAsync(7, WindowFrom(later), later, scan.Run, CancellationToken.None);
        scan.Release(totalRuns: 99);
        await fresh;

        Assert.Equal(1, memo.TrackedKeys);
        Assert.Equal(6, scan.Calls);
    }
    /* ───────────────────────── the primitive, copied ───────────────────────── */

    /// <summary>A failed scan is not memoized: every waiter present sees the ORIGINAL exception — the 57014
    /// this lane exists for — and the next caller at the same instant runs a fresh statement rather than being
    /// handed the failure again or an empty reading.</summary>
    [Fact]
    public async Task AFailedScan_IsNotMemoized_TheNextCallReRuns()
    {
        var memo = new DarlingDataReader.PerServerCollectionHealthMemo();
        var scan = new CountingScan();

        var a = memo.GetAsync(7, WindowFrom(T0), T0, scan.Run, CancellationToken.None);
        var b = memo.GetAsync(7, WindowFrom(T0), T0, scan.Run, CancellationToken.None);
        Assert.Equal(1, scan.Calls);

        scan.Fail(new InvalidOperationException("57014: canceling statement due to statement timeout"));

        var exA = await Assert.ThrowsAsync<InvalidOperationException>(() => a);
        var exB = await Assert.ThrowsAsync<InvalidOperationException>(() => b);
        Assert.Contains("57014", exA.Message, StringComparison.Ordinal);
        Assert.Same(exA, exB);

        var retry = memo.GetAsync(7, WindowFrom(T0), T0, scan.Run, CancellationToken.None);
        Assert.Equal(2, scan.Calls);
        scan.Release(totalRuns: 3);
        Assert.Equal(3, (await retry).Rows[0].TotalRuns);
    }

    /// <summary>A failure does not evict a GOOD reading either — the tempting simplification ("on failure
    /// clear everything") would turn one timed-out statement into a burst of retries from every caller of the
    /// next minute, which is the pile-up this memo exists to prevent.</summary>
    [Fact]
    public async Task AFailedReRead_DoesNotEvictTheLastGoodReading()
    {
        var memo = new DarlingDataReader.PerServerCollectionHealthMemo();
        var scan = new CountingScan();

        var first = memo.GetAsync(7, WindowFrom(T0), T0, scan.Run, CancellationToken.None);
        scan.Release(totalRuns: 5);
        var good = await first;

        var failing = memo.GetAsync(7, WindowFrom(T0), T0.AddSeconds(61), scan.Run, CancellationToken.None);
        Assert.Equal(2, scan.Calls);
        scan.Fail(new TimeoutException("simulated"));
        await Assert.ThrowsAsync<TimeoutException>(() => failing);

        var stillGood = await memo.GetAsync(7, WindowFrom(T0), T0.AddSeconds(45), scan.Run, CancellationToken.None);
        Assert.Same(good.Rows, stillGood.Rows);
        Assert.Equal(45, stillGood.AgeSeconds);
        Assert.Equal(2, scan.Calls);
    }

    /// <summary>The riskiest line, copied from the twin and stated as behaviour: the first caller's
    /// cancellation releases that caller alone, the statement keeps running for the rest, and it never saw a
    /// cancellable token — bounded by the command deadline and the role's <c>statement_timeout</c>, not by one
    /// caller's patience. The MCP surface hands no token down at all (<c>McpCommandDeadlines</c>), so this is
    /// insurance against the web surface and the next caller, not the one on the photo.</summary>
    [Fact]
    public async Task TheFirstCallersCancellation_ReleasesOnlyThatCaller()
    {
        var memo = new DarlingDataReader.PerServerCollectionHealthMemo();
        var scan = new CountingScan();

        using var firstCallersToken = new CancellationTokenSource();
        var first = memo.GetAsync(7, WindowFrom(T0), T0, scan.Run, firstCallersToken.Token);
        var second = memo.GetAsync(7, WindowFrom(T0), T0, scan.Run, CancellationToken.None);
        Assert.Equal(1, scan.Calls);
        Assert.Single(scan.TokensSeen);
        Assert.False(scan.TokensSeen[0].CanBeCanceled);

        firstCallersToken.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        Assert.False(second.IsCompleted);
        Assert.Equal(1, scan.Calls);

        scan.Release(totalRuns: 9);
        Assert.Equal(9, (await second).Rows[0].TotalRuns);

        /* And the departure cost nobody a statement: a third caller 20 s on is a hit. */
        var third = await memo.GetAsync(7, WindowFrom(T0), T0.AddSeconds(20), scan.Run, CancellationToken.None);
        Assert.Equal(20, third.AgeSeconds);
        Assert.Equal(1, scan.Calls);
        Assert.Equal(1, memo.ScansStarted);
    }

    /// <summary>A scan every waiter abandoned still completes and is still memoized on its own key — the
    /// statement was already paid for, and the next tick must not pay for it again.</summary>
    [Fact]
    public async Task AScanEveryWaiterAbandoned_StillCompletes_AndIsStillMemoized()
    {
        var memo = new DarlingDataReader.PerServerCollectionHealthMemo();
        var scan = new CountingScan();

        using var cts = new CancellationTokenSource();
        var a = memo.GetAsync(7, WindowFrom(T0), T0, scan.Run, cts.Token);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => a);

        scan.Release(totalRuns: 4);

        var later = await memo.GetAsync(7, WindowFrom(T0), T0.AddSeconds(10), scan.Run, CancellationToken.None);
        Assert.Equal(1, scan.Calls);
        Assert.Equal(10, later.AgeSeconds);
        Assert.Equal(4, later.Rows[0].TotalRuns);
    }

    /// <summary>The memo is per data source, not per process — the MCP host and the web host build their own,
    /// so "one scan per minute per HOST per server" is the bound, and the same instance always maps to the same
    /// memo or the single-flight would be no flight at all.</summary>
    [Fact]
    public void TheMemo_IsOnePerDataSource()
    {
        using var mcpHost = NpgsqlDataSource.Create("Host=localhost;Username=mcp;Database=darling");
        using var webHost = NpgsqlDataSource.Create("Host=localhost;Username=viewer;Database=darling");

        var mcpMemo = DarlingDataReader.CollectionHealthMemoFor(mcpHost);
        Assert.Same(mcpMemo, DarlingDataReader.CollectionHealthMemoFor(mcpHost));
        Assert.NotSame(mcpMemo, DarlingDataReader.CollectionHealthMemoFor(webHost));

        /* And it is not the fleet's: two memos over two statements, which is the whole shape decision. */
        Assert.NotSame(
            (object)mcpMemo,
            DarlingFleetReader.CollectionHealthMemoFor(mcpHost));
    }

    /* ───────────────────────── source anchors ───────────────────────── */

    /// <summary>
    /// The tool reaches the 7-day statement ONLY through the memo, and reads its <c>now</c> ONCE. Both are
    /// source-anchored because neither is visible to a behavioural test: a second direct call site would
    /// silently restore one statement per call, and two <c>DateTime.UtcNow</c> reads would put the published
    /// age on a different clock from the window it describes — both compile, both read correctly, and the
    /// fleet twin's own pin exists for the first of them.
    /// </summary>
    [Fact]
    public void TheTool_ReadsCollectionHealth_OnlyThroughTheMemo_OnOneClock()
    {
        var tool = RepoFile.ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");

        Assert.Contains(
            "var nowUtc = DateTime.UtcNow;\n            var (rows, collectionHealthAgeSeconds) = await DarlingDataReader.GetCollectionHealthMemoizedAsync(\n                postgres, resolved.ServerId, nowUtc.AddDays(-7), nowUtc, cancellationToken);",
            tool,
            StringComparison.Ordinal);

        /* No tool body calls the direct read — the memoized entry point is the only route in. */
        Assert.Empty(Regex.Matches(tool, @"DarlingDataReader\.GetCollectionHealthAsync\("));

        /* The age rides the payload, trailing, so nothing an existing consumer indexes by position moved. */
        Assert.Contains("collection_health_age_seconds = collectionHealthAgeSeconds", tool, StringComparison.Ordinal);
        Assert.True(
            tool.IndexOf("collection_health_age_seconds = collectionHealthAgeSeconds", StringComparison.Ordinal)
            > tool.IndexOf("collectors = result,", StringComparison.Ordinal),
            "collection_health_age_seconds no longer trails the collectors array");
    }

    /// <summary>
    /// The memoized reader routes the statement's reader through the memo's scan seam and nowhere else, and
    /// the statement text and its deadline are untouched by this change — the plan was never the problem, and
    /// neither was the role's cap. The fleet twin pins its own half the same way.
    /// </summary>
    [Fact]
    public void TheReader_RunsTheStatement_OnlyInsideTheMemosSeam()
    {
        var reader = RepoFile.ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs");

        Assert.Contains(
            "await CollectionHealthMemoFor(postgres).GetAsync(",
            reader,
            StringComparison.Ordinal);

        /* Two call sites of the statement's reader: the memo's scan lambda, and the method's own declaration.
           Anything else is a route around the memo. */
        var callSites = Regex.Matches(reader, @"GetCollectionHealthAsync\(postgres,")
            .Select(m => m.Index)
            .ToList();
        Assert.Single(callSites);
        var lineStart = reader.LastIndexOf('\n', callSites[0]) + 1;
        var line = reader[lineStart..reader.IndexOf('\n', callSites[0])];
        Assert.Contains("GetCollectionHealthAsync(postgres, scanServerId, scanWindowStart, scanToken)", line, StringComparison.Ordinal);

        Assert.Contains(
            "await using var command = postgres.CreateCommand(CollectionHealthSql);\n        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;",
            reader,
            StringComparison.Ordinal);
        Assert.Contains("FROM v_collection_log\n            WHERE server_id = $1\n            AND   collection_time >= $2", reader, StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) end-to-end: five <c>get_collection_health</c> calls raced against one real data
/// source — the shape the #3856 band met once and will meet again — cost the store ONE statement, and the age
/// travels onto the payload. Plants nothing: the assertions are about the memo the tool routed through and the
/// field on the response, neither of which depends on what the store holds, so there is no sentinel row to
/// scope to or clean up.
/// </summary>
[Collection("live-postgres")]
public sealed class PerServerCollectionHealthMemoLivePostgresTests
{
    [Fact]
    public async Task FiveRacedCollectionHealthCalls_CostOneStatement()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live per-server memo test.");

        var ct = TestContext.Current.CancellationToken;
        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var memo = DarlingDataReader.CollectionHealthMemoFor(postgres);
        Assert.Equal(0, memo.ScansStarted);

        var now = DateTime.UtcNow;
        var raced = await Task.WhenAll(Enumerable.Range(0, 5)
            .Select(_ => DarlingDataReader.GetCollectionHealthMemoizedAsync(
                postgres, 1, now.AddDays(-7), now, ct))
            .ToArray());

        Assert.Equal(1, memo.ScansStarted);
        Assert.All(raced, r => Assert.Equal(0, r.AgeSeconds));
        Assert.All(raced, r => Assert.Same(raced[0].Rows, r.Rows));

        /* A sixth call a second later is a hit on the same statement, and its age is the reading's. */
        var hit = await DarlingDataReader.GetCollectionHealthMemoizedAsync(
            postgres, 1, now.AddDays(-7), now.AddSeconds(1), ct);
        Assert.Equal(1, memo.ScansStarted);
        Assert.Equal(1, hit.AgeSeconds);
    }
}
