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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5158: the host's memory of the statement plans it has already committed. These pin the cache's lifecycle
/// (a plan is a hit only after a commit), the <c>query_stats</c> identity (a recompile of the same handle misses),
/// the over-cap pairing, the case-insensitive eviction of the digests the store reports absent, the one-hour
/// prune, the knob, and the split of an oversized fetch.
/// </summary>
public sealed class PlanDigestCacheTests
{
    private static readonly DateTime Now = new(2031, 5, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Created = new(2031, 5, 1, 9, 0, 0, DateTimeKind.Unspecified);

    private static QueryStatsPlanKey Key(
        byte handle = 0x0A, int start = 0, int end = -1, DateTime? created = null, long generation = 1, string? db = "app")
        => new(7, db, new byte[] { 0x06, 0x00, handle }, start, end, created ?? Created, generation);

    [Fact]
    public void ThePlanKeyComparesItsHandleByValue()
    {
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        cache.AddPending(Key(), "AB", 10, Now);
        cache.ConfirmPending(new[] { Key() }, Now);

        /* A distinct byte[] with the same bytes: a reference compare would miss here, every run. */
        Assert.True(cache.TryGet(Key(), Now, out var hit));
        Assert.Equal("AB", hit.Digest);
    }

    [Theory]
    [InlineData("created")]
    [InlineData("generation")]
    [InlineData("handle")]
    [InlineData("start")]
    [InlineData("end")]
    [InlineData("database")]
    public void AnyChangeToThePlanIdentityMisses(string changed)
    {
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        cache.AddPending(Key(), "AB", 10, Now);
        cache.ConfirmPending(new[] { Key() }, Now);

        var other = changed switch
        {
            "created" => Key(created: Created.AddMinutes(5)),
            "generation" => Key(generation: 2),
            "handle" => Key(handle: 0x0B),
            "start" => Key(start: 4),
            "end" => Key(end: 90),
            _ => Key(db: "other"),
        };

        /* creation_time and plan_generation_num: a statement-level recompile keeps plan_handle and the offsets. */
        Assert.False(cache.TryGet(other, Now, out _));
    }

    [Fact]
    public void APendingEntryIsNotAHitUntilConfirmed()
    {
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        cache.AddPending(Key(), "AB", 10, Now);

        Assert.False(cache.TryGet(Key(), Now, out _));

        cache.ConfirmPending(new[] { Key() }, Now);
        Assert.True(cache.TryGet(Key(), Now, out _));
    }

    [Fact]
    public void ADiscardedPendingEntryNeverBecomesAHit_AndAConfirmedOneIsKept()
    {
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        cache.AddPending(Key(handle: 1), "AA", 1, Now);
        cache.AddPending(Key(handle: 2), "BB", 2, Now);
        cache.ConfirmPending(new[] { Key(handle: 2) }, Now);

        cache.DiscardPending(new[] { Key(handle: 1), Key(handle: 2) });
        cache.ConfirmPending(new[] { Key(handle: 1) }, Now);

        Assert.False(cache.TryGet(Key(handle: 1), Now, out _));
        Assert.True(cache.TryGet(Key(handle: 2), Now, out _));
    }

    [Fact]
    public void AnOverCapEntryCarriesItsBytesWithANullDigest()
    {
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        cache.AddPending(Key(), null, 900_000, Now);
        cache.ConfirmPending(new[] { Key() }, Now);

        Assert.True(cache.TryGet(Key(), Now, out var hit));
        Assert.Null(hit.Digest);
        Assert.Equal(900_000, hit.Bytes);
    }

    [Fact]
    public void EvictingAbsentDigestsIgnoresCase()
    {
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        cache.AddPending(Key(handle: 1), "abcdef0123", 1, Now);
        cache.AddPending(Key(handle: 2), "FFEE", 2, Now);
        cache.ConfirmPending(new[] { Key(handle: 1), Key(handle: 2) }, Now);

        /* PayloadDimensionWriter.FlushAsync reports upper-case hex. */
        Assert.Equal(1, cache.Evict(new[] { "ABCDEF0123" }));

        Assert.False(cache.TryGet(Key(handle: 1), Now, out _));
        Assert.True(cache.TryGet(Key(handle: 2), Now, out _));
    }

    [Fact]
    public void PruneForgetsOnlyIdentitiesUnseenForAnHour()
    {
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        cache.AddPending(Key(handle: 1), "AA", 1, Now.AddMinutes(-90));
        cache.AddPending(Key(handle: 2), "BB", 2, Now.AddMinutes(-90));
        cache.ConfirmPending(new[] { Key(handle: 1), Key(handle: 2) }, Now.AddMinutes(-90));

        /* Seeing an identity refreshes it. */
        Assert.True(cache.TryGet(Key(handle: 2), Now.AddMinutes(-10), out _));

        Assert.Equal(1, cache.Prune(Now - TimeSpan.FromHours(1)));
        Assert.False(cache.TryGet(Key(handle: 1), Now, out _));
        Assert.True(cache.TryGet(Key(handle: 2), Now, out _));
        Assert.Equal(TimeSpan.FromHours(1), DarlingCollectorRunner.QueryStatsPlanCacheMaxAge);
    }

    [Fact]
    public void ARowKeyNeedsAUsablePlanHandle()
    {
        var row = new QueryStatsCollector.Row { PlanHandle = "0x06000500AB", DatabaseName = "app", CreationTime = Created, PlanGenerationNum = 3, StatementEndOffset = -1 };
        var key = QueryStatsPlanKey.TryCreate(1, row);
        Assert.NotNull(key);
        Assert.Equal(new byte[] { 0x06, 0x00, 0x05, 0x00, 0xAB }, key!.Value.PlanHandle);

        Assert.Null(QueryStatsPlanKey.TryCreate(1, new QueryStatsCollector.Row { PlanHandle = null }));
        Assert.Null(QueryStatsPlanKey.TryCreate(1, new QueryStatsCollector.Row { PlanHandle = "0x" }));
        Assert.Null(QueryStatsPlanKey.TryCreate(1, new QueryStatsCollector.Row { PlanHandle = "0xZZ" }));
    }

    [Fact]
    public void TheFetchIsSplitAboveTheKeyBound()
    {
        var context = new CollectorContext
        {
            ServerId = 1, ServerName = "s", CollectionTime = Now, Deltas = new NoDeltas(),
            Target = new CollectorTargetInfo(), CapturePlanXml = true, DeferPlanXmlFetch = true,
        };

        QueryStatsCollector.PlanFetchKey[] KeysOf(int n) =>
            Enumerable.Range(0, n).Select(i => new QueryStatsCollector.PlanFetchKey(new byte[] { 0x06, (byte)(i % 250) }, i, -1)).ToArray();

        Assert.Equal(1000, QueryStatsCollector.MaxPlanFetchKeys);
        Assert.NotEmpty(QueryStatsCollector.BuildPlanFetchQuery(context, KeysOf(1000)).Text);
        Assert.Throws<InvalidOperationException>(() => QueryStatsCollector.BuildPlanFetchQuery(context, KeysOf(1001)));
    }

    [Theory]
    [InlineData(true, true, false, true)]
    [InlineData(false, true, false, false)]   // knob off: today's inline capture
    [InlineData(true, false, false, false)]   // capture off
    [InlineData(true, true, true, false)]     // Azure SQL DB reads per database, no second query
    public void OnlyQueryStatsDefers_AndOnlyWithTheKnobOnAndPlansCaptured(bool knob, bool capture, bool azure, bool expected)
    {
        var runner = (DarlingCollectorRunner)System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(typeof(DarlingCollectorRunner));
        typeof(DarlingCollectorRunner)
            .GetField("_queryStatsDeferredPlanFetch", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .SetValue(runner, (Func<bool>)(() => knob));
        /* procedure_stats reads its own knob and the plan switch; both off here, so it never defers. */
        typeof(DarlingCollectorRunner)
            .GetField("_capturePlans", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .SetValue(runner, (Func<bool>)(() => capture));
        typeof(DarlingCollectorRunner)
            .GetField("_procedureStatsDeferredPlanFetch", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .SetValue(runner, (Func<string?>)(() => "off"));

        var target = new CollectorTargetInfo { IsAzureSqlDb = azure };
        Assert.Equal(expected, runner.ShouldDeferPlanFetchFor("query_stats", capture, target));
        Assert.False(runner.ShouldDeferPlanFetchFor("procedure_stats", capture, target));
        Assert.False(runner.ShouldDeferPlanFetchFor("query_store", capture, target));
    }

    [Fact]
    public void WithTheKnobOffTheMainQueryIsTodaysInlineCaptureSql()
    {
        CollectorContext Make(bool defer) => new()
        {
            ServerId = 1, ServerName = "s", CollectionTime = Now, Deltas = new NoDeltas(),
            Target = new CollectorTargetInfo(), CapturePlanXml = true, DeferPlanXmlFetch = defer,
        };

        var inline = QueryStatsCollector.Instance.BuildQuery(Make(defer: false)).Text;
        Assert.Contains("dm_exec_text_query_plan", inline, StringComparison.Ordinal);
        Assert.Contains("query_plan_xml_bytes", inline, StringComparison.Ordinal);

        /* The byte-for-byte text is pinned by QueryStatsSqlGoldenTests; the runner's knob-off path sets no
           DeferPlanXmlFetch, so it builds exactly this. */
        var deferred = QueryStatsCollector.Instance.BuildQuery(Make(defer: true)).Text;
        Assert.DoesNotContain("dm_exec_text_query_plan", deferred, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRunnerConfirmsOnlyAfterTheCommitAndDiscardsOnFailure()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs")
            .ReplaceLineEndings("\n");

        var write = source.IndexOf("rowsWritten = await WriteBatchAsync(pgConnection, definition, rows, server, collectionTime, context, cancellationToken);", StringComparison.Ordinal);
        var committedSet = source.IndexOf("committed = true;", write, StringComparison.Ordinal);
        var confirm = source.IndexOf("planCache.ConfirmPending(", StringComparison.Ordinal);
        var discard = source.IndexOf("planCache.DiscardPending(", StringComparison.Ordinal);

        Assert.True(write > 0 && committedSet > write, "the commit flag is set only after WriteBatchAsync returns");
        Assert.True(confirm > committedSet, "ConfirmPending runs after the commit");
        Assert.True(discard > confirm, "DiscardPending covers the failed write");
        Assert.Equal(1, Count(source, "ConfirmPending("));
    }

    private static int Count(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + 1, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }
}
