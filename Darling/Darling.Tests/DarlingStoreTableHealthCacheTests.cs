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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableHealthReader;

namespace Darling.Tests;

/// <summary>
/// #8924: the store health read is held in process for <see cref="DarlingStoreTableHealthCache.Lifetime"/>, so a repeat costs the store
/// nothing. Every test here counts the store reads through the cache's own read delegates and moves time by stepping the cache's clock:
/// none of them opens a connection, waits on a timer or measures how long anything took. What they show is how many times the store is
/// read and what the answer says about its age. They do not show the seconds a read takes on a store with thousands of chunks.
/// </summary>
public sealed class DarlingStoreTableHealthCacheTests
{
    private static readonly DateTime Start = new(2026, 11, 17, 15, 0, 0, DateTimeKind.Utc);

    private static StoreTableHealthRow TableRow(string name, bool candidate = false) =>
        new(
            "collect", name, KindTable, 2_000_000_000, 1_999_999_900, 60, 40, 1234, 0, 0, 1_500_000_000, 0.8, 1, 1, null,
            500, 100, null, null, null, null, false, null, candidate, 10, 5, null, 50.0, 1000.0, 0, null, 0, 0, 100, 10, 0, 0, null, 0, 0, 0,
            null, StatsSourceReset, 24, false, 0, 0, 0, null);

    private static StoreIndexHealthRow IndexRow(string index) =>
        new("collect", "store_things", KindTable, index, false, 1, 0, 1000, 1000, 0, 0.5, false, false, true, false, 0, false,
            null, StatsSourceReset, null, null, 0, "below_size_floor");

    private static StoreIndexCoverageRow CoverageRow(string index) =>
        new("collect", "parts", index, true, 2, 2, 0, [], 0, []);

    /// <summary>A data source that is never opened: the cache keys on it and hands it to reads that ignore it.</summary>
    private static NpgsqlDataSource NeverOpened() => NpgsqlDataSource.Create("Host=127.0.0.1;Database=never_opened");

    /// <summary>
    /// A cache over three scripted store reads that count their calls, on a clock the test sets. Each table read answers five tables
    /// (t1 to t5, t2 and t4 rewrite candidates) in a new list, so a second store read is a different list from the held one.
    /// <see cref="BeforeTables"/> runs inside a table read, given the read's number: a test holds a read open with it or fails it.
    /// </summary>
    private sealed class CountedStore
    {
        public DateTime Now = Start;
        public int Tables;
        public int Indexes;
        public int Coverage;
        public int AutovacuumSettings;
        public Func<int, Task>? BeforeTables;

        public CountedStore()
        {
            Cache = new DarlingStoreTableHealthCache(
                () => Now,
                async (_, _) =>
                {
                    var call = Interlocked.Increment(ref Tables);
                    if (BeforeTables is { } hook)
                    {
                        await hook(call);
                    }

                    return [TableRow("t1"), TableRow("t2", candidate: true), TableRow("t3"), TableRow("t4", candidate: true), TableRow("t5")];
                },
                (_, _) =>
                {
                    Interlocked.Increment(ref Indexes);
                    return Task.FromResult(new List<StoreIndexHealthRow> { IndexRow("ix_1"), IndexRow("ix_2"), IndexRow("ix_3") });
                },
                (_, _) =>
                {
                    Interlocked.Increment(ref Coverage);
                    return Task.FromResult(new List<StoreIndexCoverageRow> { CoverageRow("ix_parts_1"), CoverageRow("ix_parts_2") });
                },
                (_, _) =>
                {
                    Interlocked.Increment(ref AutovacuumSettings);
                    return Task.FromResult(Settings(true, 3, 1, 60, -1, 200, 2, 0, -1, 65536, 20, 8192, 4, 1));
                });
        }

        public DarlingStoreTableHealthCache Cache { get; }

        public (int Tables, int Indexes, int Coverage) Reads => (Volatile.Read(ref Tables), Volatile.Read(ref Indexes), Volatile.Read(ref Coverage));
    }

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Fact]
    public async Task ASecondReadInsideTheLifetime_RunsNoStoreRead_AndCarriesTheFirstReadsTimeAndItsAge()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();

        var first = await store.Cache.GetTablesAsync(source);
        Assert.Equal(1, store.Reads.Tables);
        Assert.Equal(Start, first.ReadAtUtc);
        Assert.Equal(0, first.AgeSeconds(store.Cache.UtcNow()));

        store.Now = Start.AddSeconds(90);
        var second = await store.Cache.GetTablesAsync(source);

        Assert.Equal(1, store.Reads.Tables);
        Assert.Same(first.Rows, second.Rows);
        Assert.Equal(Start, second.ReadAtUtc);
        Assert.Equal(90, second.AgeSeconds(store.Cache.UtcNow()));
    }

    [Fact]
    public async Task ReadsThatArriveWhileOneIsRunning_ShareThatOneStoreRead()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        var running = Signal();
        var release = Signal();
        store.BeforeTables = _ =>
        {
            running.TrySetResult();
            return release.Task;
        };

        var first = store.Cache.GetTablesAsync(source);
        await running.Task;
        var second = store.Cache.GetTablesAsync(source);
        var third = store.Cache.GetTablesAsync(source);
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);
        Assert.False(third.IsCompleted);

        release.SetResult();
        var answers = await Task.WhenAll(first, second, third);

        Assert.Equal(1, store.Reads.Tables);
        Assert.Same(answers[0].Rows, answers[1].Rows);
        Assert.Same(answers[0].Rows, answers[2].Rows);
        Assert.Equal(5, answers[0].Rows.Count);
    }

    [Fact]
    public async Task AReadIsHeldUntilTheLifetimeHasPassed_AndTheNextCallGetsItAtOnceWhileANewOneRunsBehind()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        var first = await store.Cache.GetTablesAsync(source);

        store.Now = Start + DarlingStoreTableHealthCache.Lifetime - TimeSpan.FromSeconds(1);
        var inside = await store.Cache.GetTablesAsync(source);
        Assert.Equal(1, store.Reads.Tables);
        Assert.Same(first.Rows, inside.Rows);
        Assert.Equal(299, inside.AgeSeconds(store.Cache.UtcNow()));

        /* #9224: past the lifetime the call gets the held read with its own time, and a new read runs behind it. */
        store.Now = Start + DarlingStoreTableHealthCache.Lifetime;
        var after = await store.Cache.GetTablesAsync(source);
        Assert.Same(first.Rows, after.Rows);
        Assert.Equal(Start, after.ReadAtUtc);
        Assert.Equal(300, after.AgeSeconds(store.Cache.UtcNow()));

        await store.Cache.WhenIdleAsync();
        Assert.Equal(2, store.Reads.Tables);
        var next = await store.Cache.GetTablesAsync(source);
        Assert.Equal(2, store.Reads.Tables);
        Assert.NotSame(first.Rows, next.Rows);
        Assert.Equal(store.Now, next.ReadAtUtc);
        Assert.Equal(0, next.AgeSeconds(store.Cache.UtcNow()));
    }

    [Fact]
    public async Task AReadThatRunsPastItsLimit_IsNotHeld_SoTheNextCallReadsTheStoreAgain()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        store.BeforeTables = call => call == 1
            ? Task.FromException(new StoreTableHealthTimeoutException(new TimeoutException("scripted")))
            : Task.CompletedTask;

        await Assert.ThrowsAsync<StoreTableHealthTimeoutException>(() => store.Cache.GetTablesAsync(source));
        Assert.Equal(1, store.Reads.Tables);

        var next = await store.Cache.GetTablesAsync(source);
        Assert.Equal(2, store.Reads.Tables);
        Assert.Equal(5, next.Rows.Count);
    }

    [Fact]
    public async Task AFreshRead_GoesToTheStore_AndIsTheOneHeldAfterwards()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        var first = await store.Cache.GetTablesAsync(source);
        await store.Cache.GetIndexesAsync(source);

        store.Now = Start.AddSeconds(30);
        var fresh = await store.Cache.GetTablesAsync(source, fresh: true);
        Assert.Equal(2, store.Reads.Tables);
        Assert.NotSame(first.Rows, fresh.Rows);
        Assert.Equal(Start.AddSeconds(30), fresh.ReadAtUtc);

        store.Now = Start.AddSeconds(60);
        var held = await store.Cache.GetTablesAsync(source);
        Assert.Equal(2, store.Reads.Tables);
        Assert.Same(fresh.Rows, held.Rows);
        Assert.Equal(30, held.AgeSeconds(store.Cache.UtcNow()));

        /* A new read of the tables leaves the held index rows alone. */
        await store.Cache.GetIndexesAsync(source);
        Assert.Equal(1, store.Reads.Indexes);
    }

    [Fact]
    public async Task AFreshReadTakenWhileAnOlderOneIsStillRunning_IsTheOneHeld_NotTheOlderOneThatEndsLater()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        var running = Signal();
        var release = Signal();
        store.BeforeTables = call =>
        {
            if (call != 1)
            {
                return Task.CompletedTask;
            }

            running.TrySetResult();
            return release.Task;
        };

        var older = store.Cache.GetTablesAsync(source);
        await running.Task;
        var fresh = await store.Cache.GetTablesAsync(source, fresh: true);
        Assert.Equal(2, store.Reads.Tables);

        release.SetResult();
        var olderAnswer = await older;
        Assert.NotSame(fresh.Rows, olderAnswer.Rows);

        var held = await store.Cache.GetTablesAsync(source);
        Assert.Equal(2, store.Reads.Tables);
        Assert.Same(fresh.Rows, held.Rows);
    }

    [Fact]
    public async Task EachPartIsHeldOnItsOwn()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();

        await store.Cache.GetTablesAsync(source);
        Assert.Equal((1, 0, 0), store.Reads);
        await store.Cache.GetIndexesAsync(source);
        Assert.Equal((1, 1, 0), store.Reads);
        await store.Cache.GetIndexCoverageAsync(source);
        Assert.Equal((1, 1, 1), store.Reads);

        store.Now = Start.AddMinutes(4);
        var tables = await store.Cache.GetTablesAsync(source);
        var indexes = await store.Cache.GetIndexesAsync(source);
        var coverage = await store.Cache.GetIndexCoverageAsync(source);
        Assert.Equal((1, 1, 1), store.Reads);
        Assert.Equal(5, tables.Rows.Count);
        Assert.Equal(3, indexes.Rows.Count);
        Assert.Equal(2, coverage.Rows.Count);
        Assert.Equal(240, indexes.AgeSeconds(store.Cache.UtcNow()));
        Assert.Equal(240, coverage.AgeSeconds(store.Cache.UtcNow()));
    }

    [Fact]
    public async Task ACallerThatHasAlreadyLeft_StartsNoStoreRead()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        using var left = new CancellationTokenSource();
        await left.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.Cache.GetTablesAsync(source, cancellationToken: left.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.Cache.GetIndexesAsync(source, cancellationToken: left.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.Cache.GetIndexCoverageAsync(source, cancellationToken: left.Token));

        Assert.Equal((0, 0, 0), store.Reads);
    }

    [Fact]
    public async Task ACallerThatLeavesMidRead_EndsOnlyItsOwnWait_AndTheReadIsHeldForTheOthers()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        var running = Signal();
        var release = Signal();
        store.BeforeTables = _ =>
        {
            running.TrySetResult();
            return release.Task;
        };
        using var leaving = new CancellationTokenSource();

        var left = store.Cache.GetTablesAsync(source, cancellationToken: leaving.Token);
        await running.Task;
        var stayed = store.Cache.GetTablesAsync(source);
        await leaving.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => left);

        release.SetResult();
        var answer = await stayed;
        var again = await store.Cache.GetTablesAsync(source);

        Assert.Equal(1, store.Reads.Tables);
        Assert.Same(answer.Rows, again.Rows);
    }

    [Fact]
    public async Task TheProcessKeepsOneCachePerDataSource()
    {
        await using var one = NeverOpened();
        await using var other = NeverOpened();

        Assert.Same(DarlingStoreTableHealthCache.For(one), DarlingStoreTableHealthCache.For(one));
        Assert.NotSame(DarlingStoreTableHealthCache.For(one), DarlingStoreTableHealthCache.For(other));
    }

    private static async Task<JsonDocument> Tool(
        CountedStore store, NpgsqlDataSource source, int top = 50, bool candidatesOnly = false, string? section = null, int offset = 0) =>
        JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(
            source, top, candidatesOnly, section, offset, responseByteBudget: null, CancellationToken.None, cache: store.Cache));

    private static string[] TableNames(JsonDocument reply) =>
        reply.RootElement.GetProperty("tables").EnumerateArray().Select(t => t.GetProperty("table_name").GetString()!).ToArray();

    /// <summary>#8950: the tables part of the reply carries the store's autovacuum settings with the write budget, read once and held
    /// like the other parts, and counts the tables behind and the tables repeatedly cancelled.</summary>
    [Fact]
    public async Task TheToolsReply_CarriesTheAutovacuumSettingsAndBudget_HeldLikeTheOtherParts()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();

        using var first = await Tool(store, source);
        store.Now = Start.AddMinutes(2);
        using var second = await Tool(store, source, top: 2);

        Assert.Equal(1, Volatile.Read(ref store.AutovacuumSettings));
        var settings = second.RootElement.GetProperty("autovacuum_settings");
        Assert.Equal(Start, settings.GetProperty("as_of").GetDateTime());
        Assert.Equal(120, settings.GetProperty("age_seconds").GetDouble());
        Assert.True(settings.GetProperty("autovacuum_on").GetBoolean());
        Assert.Equal(200, settings.GetProperty("cost_limit").GetInt32());
        Assert.Equal("vacuum_cost_limit", settings.GetProperty("cost_limit_setting").GetString());
        Assert.Equal(39.1, settings.GetProperty("write_budget_mb_per_s").GetDouble());
        Assert.Equal(4, settings.GetProperty("cancels_24h").GetInt64());
        Assert.Equal(1, settings.GetProperty("cancels_unresolved").GetInt64());
        Assert.Equal(0, second.RootElement.GetProperty("tables_behind").GetInt32());
        Assert.Equal(0, second.RootElement.GetProperty("tables_repeatedly_cancelled").GetInt32());
        Assert.False(second.RootElement.TryGetProperty("autovacuum_settings_left_out_note", out _));
    }

    [Fact]
    public async Task TheTool_CutsTheHeldRowsToTop_SoCallsThatDifferOnlyInTopShareOneStoreRead()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();

        using var one = await Tool(store, source, top: 1, section: "tables");
        Assert.Equal(["t1"], TableNames(one));
        Assert.Equal(1, one.RootElement.GetProperty("tables_returned").GetInt32());
        Assert.True(one.RootElement.GetProperty("truncated").GetBoolean());
        Assert.True(one.RootElement.GetProperty("has_more").GetBoolean());
        /* The header counts are of every held row, not of the page. */
        Assert.Equal(5, one.RootElement.GetProperty("store_tables").GetInt32());
        Assert.Equal(2, one.RootElement.GetProperty("rewrite_candidates").GetInt32());

        using var five = await Tool(store, source, top: 5, section: "tables");
        Assert.Equal(["t1", "t2", "t3", "t4", "t5"], TableNames(five));
        Assert.False(five.RootElement.GetProperty("has_more").GetBoolean());

        Assert.Equal((1, 0, 0), store.Reads);
    }

    [Fact]
    public async Task TheTool_AppliesOffsetCandidatesOnlyAndSectionToTheHeldRows_WithoutAnotherStoreRead()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();

        using var all = await Tool(store, source);
        Assert.Equal((1, 1, 1), store.Reads);
        Assert.Equal(5, all.RootElement.GetProperty("tables_returned").GetInt32());
        Assert.Equal(3, all.RootElement.GetProperty("indexes_returned").GetInt32());
        Assert.Equal(2, all.RootElement.GetProperty("partition_index_coverage_returned").GetInt32());

        using var paged = await Tool(store, source, top: 5, section: "tables", offset: 3);
        Assert.Equal(["t4", "t5"], TableNames(paged));

        using var candidates = await Tool(store, source, candidatesOnly: true, section: "tables");
        Assert.Equal(["t2", "t4"], TableNames(candidates));

        using var indexes = await Tool(store, source, top: 2, section: "indexes");
        Assert.Equal(2, indexes.RootElement.GetProperty("indexes_returned").GetInt32());
        Assert.Equal(3, indexes.RootElement.GetProperty("indexes_total").GetInt32());
        Assert.False(indexes.RootElement.TryGetProperty("tables", out _));

        Assert.Equal((1, 1, 1), store.Reads);
    }

    [Fact]
    public async Task TheToolsReply_SaysWhenTheStoreAnsweredEachPart_AndHowOldTheHeldAnswerIs()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();

        using var first = await Tool(store, source);
        Assert.Equal(Start, first.RootElement.GetProperty("tables_as_of").GetDateTime().ToUniversalTime());
        Assert.Equal(0, first.RootElement.GetProperty("tables_age_seconds").GetInt64());
        Assert.Equal(0, first.RootElement.GetProperty("indexes_age_seconds").GetInt64());
        Assert.Equal(0, first.RootElement.GetProperty("partition_index_coverage_age_seconds").GetInt64());

        store.Now = Start.AddSeconds(120);
        using var held = await Tool(store, source);
        Assert.Equal((1, 1, 1), store.Reads);
        foreach (var part in new[] { "tables", "indexes", "partition_index_coverage" })
        {
            Assert.Equal(Start, held.RootElement.GetProperty(part + "_as_of").GetDateTime().ToUniversalTime());
            Assert.Equal(120, held.RootElement.GetProperty(part + "_age_seconds").GetInt64());
        }

        Assert.Equal(300, held.RootElement.GetProperty("held_for_seconds").GetInt32());
        Assert.Equal(DarlingMcpStoreTableHealthTools.AsOfNote, held.RootElement.GetProperty("as_of_note").GetString());

        /* #9224: past the lifetime the reply is the held read, with its own time and its age, and a new read runs behind it. The reply after
           that carries the new read's time and no age. */
        store.Now = Start.AddSeconds(301);
        using var stale = await Tool(store, source);
        Assert.Equal(Start, stale.RootElement.GetProperty("tables_as_of").GetDateTime().ToUniversalTime());
        Assert.Equal(301, stale.RootElement.GetProperty("tables_age_seconds").GetInt64());
        await store.Cache.WhenIdleAsync();
        Assert.Equal((2, 2, 2), store.Reads);
        using var reread = await Tool(store, source);
        Assert.Equal((2, 2, 2), store.Reads);
        Assert.Equal(Start.AddSeconds(301), reread.RootElement.GetProperty("tables_as_of").GetDateTime().ToUniversalTime());
        Assert.Equal(0, reread.RootElement.GetProperty("tables_age_seconds").GetInt64());
    }

    [Fact]
    public async Task ATablePartThatTimedOut_GivesNoReadTime_AndTheNextCallReadsOnlyThatPartAgain()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        store.BeforeTables = call => call == 1
            ? Task.FromException(new StoreTableHealthTimeoutException(new TimeoutException("scripted")))
            : Task.CompletedTask;

        using var first = await Tool(store, source);
        Assert.True(first.RootElement.TryGetProperty("tables_left_out_note", out _));
        Assert.False(first.RootElement.TryGetProperty("tables_as_of", out _));
        Assert.False(first.RootElement.TryGetProperty("tables_age_seconds", out _));
        Assert.Equal(Start, first.RootElement.GetProperty("indexes_as_of").GetDateTime().ToUniversalTime());

        store.Now = Start.AddSeconds(10);
        using var second = await Tool(store, source);
        Assert.Equal((2, 1, 1), store.Reads);
        Assert.Equal(5, second.RootElement.GetProperty("tables_returned").GetInt32());
        Assert.Equal(Start.AddSeconds(10), second.RootElement.GetProperty("tables_as_of").GetDateTime().ToUniversalTime());
        Assert.Equal(0, second.RootElement.GetProperty("tables_age_seconds").GetInt64());
        Assert.Equal(10, second.RootElement.GetProperty("indexes_age_seconds").GetInt64());
    }

    /// <summary>#9224: the service takes one pass of every part when it starts, so the first MCP call finds them held and reads nothing.</summary>
    [Fact]
    public async Task AfterTheStartPass_AnMcpCallReadsNothingFromTheStore()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();

        await store.Cache.WarmAsync(source);
        Assert.Equal((1, 1, 1), store.Reads);
        Assert.Equal(1, Volatile.Read(ref store.AutovacuumSettings));

        store.Now = Start.AddSeconds(40);
        using var reply = await Tool(store, source);

        Assert.Equal((1, 1, 1), store.Reads);
        Assert.Equal(1, Volatile.Read(ref store.AutovacuumSettings));
        Assert.Equal(5, reply.RootElement.GetProperty("tables_returned").GetInt32());
        Assert.Equal(Start, reply.RootElement.GetProperty("tables_as_of").GetDateTime().ToUniversalTime());
        Assert.Equal(40, reply.RootElement.GetProperty("tables_age_seconds").GetInt64());
        Assert.Equal(40, reply.RootElement.GetProperty("indexes_age_seconds").GetInt64());
        Assert.Equal(40, reply.RootElement.GetProperty("partition_index_coverage_age_seconds").GetInt64());
        Assert.Equal(40, reply.RootElement.GetProperty("autovacuum_settings").GetProperty("age_seconds").GetDouble());
    }

    /// <summary>#9224: a call that arrives while the start pass is reading a part waits for that read, so the store is read once.</summary>
    [Fact]
    public async Task ACallMadeDuringTheStartPass_AwaitsIt_AndTheStoreIsReadOnce()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        var running = Signal();
        var release = Signal();
        store.BeforeTables = _ =>
        {
            running.TrySetResult();
            return release.Task;
        };

        var warm = store.Cache.WarmAsync(source);
        await running.Task;
        var call = Tool(store, source);
        Assert.False(warm.IsCompleted);
        Assert.False(call.IsCompleted);

        release.SetResult();
        await warm;
        using var reply = await call;

        Assert.Equal((1, 1, 1), store.Reads);
        Assert.Equal(1, Volatile.Read(ref store.AutovacuumSettings));
        Assert.Equal(5, reply.RootElement.GetProperty("tables_returned").GetInt32());
    }

    /// <summary>#9224: past the lifetime a call gets the held part at once with its old time, and many concurrent calls start one refresh.</summary>
    [Fact]
    public async Task PastTheLifetime_CallsGetTheHeldPartAtOnce_AndConcurrentCallsStartOneRefresh()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        var first = await store.Cache.GetTablesAsync(source);
        var running = Signal();
        var release = Signal();
        store.BeforeTables = _ =>
        {
            running.TrySetResult();
            return release.Task;
        };

        store.Now = Start + DarlingStoreTableHealthCache.Lifetime + TimeSpan.FromSeconds(1);
        var calls = Enumerable.Range(0, 8).Select(_ => Task.Run(() => store.Cache.GetTablesAsync(source))).ToArray();
        /* Every call is answered while the refresh is still held open: none of them waits for it. */
        var answers = await Task.WhenAll(calls);
        await running.Task;

        Assert.All(answers, a =>
        {
            Assert.Same(first.Rows, a.Rows);
            Assert.Equal(Start, a.ReadAtUtc);
            Assert.Equal(301, a.AgeSeconds(store.Cache.UtcNow()));
        });
        Assert.Equal(2, store.Reads.Tables);

        using var reply = await Tool(store, source, section: "tables");
        Assert.Equal(Start, reply.RootElement.GetProperty("tables_as_of").GetDateTime().ToUniversalTime());
        Assert.Equal(301, reply.RootElement.GetProperty("tables_age_seconds").GetInt64());
        Assert.Equal(2, store.Reads.Tables);

        release.SetResult();
        await store.Cache.WhenIdleAsync();
        Assert.Equal(2, store.Reads.Tables);
        var after = await store.Cache.GetTablesAsync(source);
        Assert.NotSame(first.Rows, after.Rows);
        Assert.Equal(store.Now, after.ReadAtUtc);
        Assert.Equal(2, store.Reads.Tables);
    }

    /// <summary>#9224: a refresh that fails keeps the held part, says so once, and is tried again by the first call a lifetime after it.</summary>
    [Fact]
    public async Task AFailedRefresh_KeepsTheHeldPart_ReportsOnce_AndTheNextCallPastTheLifetimeTriesAgain()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        var failures = new List<string>();
        store.Cache.OnReadFailed = (part, _) =>
        {
            lock (failures)
            {
                failures.Add(part);
            }
        };
        var first = await store.Cache.GetTablesAsync(source);
        store.BeforeTables = call => call == 2 ? Task.FromException(new InvalidOperationException("scripted")) : Task.CompletedTask;

        store.Now = Start + DarlingStoreTableHealthCache.Lifetime + TimeSpan.FromSeconds(1);
        var held = await store.Cache.GetTablesAsync(source);
        await store.Cache.WhenIdleAsync();
        Assert.Same(first.Rows, held.Rows);
        Assert.Equal(2, store.Reads.Tables);
        Assert.Equal(["tables"], failures);

        /* Inside a lifetime of the failed attempt no other refresh starts, and the held part is still served with its own time. */
        store.Now += TimeSpan.FromSeconds(30);
        var again = await store.Cache.GetTablesAsync(source);
        await store.Cache.WhenIdleAsync();
        Assert.Same(first.Rows, again.Rows);
        Assert.Equal(Start, again.ReadAtUtc);
        Assert.Equal(2, store.Reads.Tables);

        store.Now += DarlingStoreTableHealthCache.Lifetime;
        var retrying = await store.Cache.GetTablesAsync(source);
        Assert.Same(first.Rows, retrying.Rows);
        await store.Cache.WhenIdleAsync();
        Assert.Equal(3, store.Reads.Tables);
        Assert.Equal(["tables"], failures);

        var renewed = await store.Cache.GetTablesAsync(source);
        Assert.NotSame(first.Rows, renewed.Rows);
        Assert.Equal(store.Now, renewed.ReadAtUtc);
    }

    /// <summary>#9224: a part that fails in the start pass is reported and does not stop the others, and the first caller reads it.</summary>
    [Fact]
    public async Task AStartPassPartThatFails_IsReported_TheOthersAreHeld_AndTheFirstCallReadsIt()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        var failures = new List<string>();
        store.Cache.OnReadFailed = (part, _) => failures.Add(part);
        store.BeforeTables = call => call == 1 ? Task.FromException(new InvalidOperationException("scripted")) : Task.CompletedTask;

        await store.Cache.WarmAsync(source);
        Assert.Equal(["tables"], failures);
        Assert.Equal((1, 1, 1), store.Reads);

        var table = await store.Cache.GetTablesAsync(source);
        Assert.Equal(2, store.Reads.Tables);
        Assert.Equal(5, table.Rows.Count);
        await store.Cache.GetIndexesAsync(source);
        Assert.Equal(1, store.Reads.Indexes);
    }

    /// <summary>#9224: the host's pass waits for the worker to say the store is migrated, then reads every part once.</summary>
    [Fact]
    public async Task TheWarmGate_StartsNoPassBeforeTheStoreIsMigrated_ThenReadsEveryPartOnce()
    {
        var store = new CountedStore();
        await using var source = NeverOpened();
        var gate = new StoreHealthWarmGate();

        var pass = gate.WarmWhenReady(source, NullLogger.Instance, cache: store.Cache);
        Assert.False(pass.IsCompleted);
        Assert.Equal((0, 0, 0), store.Reads);

        gate.MarkStoreReady();
        await pass;
        Assert.Equal((1, 1, 1), store.Reads);
        Assert.Equal(1, Volatile.Read(ref store.AutovacuumSettings));

        /* A host that asks after the store is ready starts its pass at once. */
        var late = new CountedStore();
        await gate.WarmWhenReady(source, NullLogger.Instance, cache: late.Cache);
        Assert.Equal((1, 1, 1), late.Reads);
    }
}
