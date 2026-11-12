/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7419 part 2: query_stats reads a statement's text in a second query, and only for a kept row whose text the host has
/// not already committed. These drive the whole host side of that without a target: the hit-or-miss split, the target
/// read through a data reader, the apply and the cache lifecycle, and the write.
/// </summary>
public sealed class QueryStatsDeferredTextFetchTests
{
    private static readonly DateTime Now = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Created = new(2026, 11, 2, 11, 0, 0, DateTimeKind.Utc);

    private static QueryStatsCollector.Row RowFor(int n, int start = 0, int end = -1, DateTime? created = null) => new()
    {
        QueryHash = "0xABCD",
        PlanHandle = "0x06" + n.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
        DatabaseName = "app",
        CreationTime = created ?? Created,
        PlanGenerationNum = 1,
        StatementStartOffset = start,
        StatementEndOffset = end,
    };

    private static CollectorContext Context(bool deferText) => new()
    {
        ServerId = 7,
        ServerName = "test-server",
        CollectionTime = Now,
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo(),
        DeferTextFetch = deferText,
    };

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return 0;
        }
    }

    private static async Task<Dictionary<int, string?>> ReadAsync(HashSet<int> unjudged, params (int Ord, string? Text)[] rows)
    {
        using var table = new DataTable();
        table.Columns.Add("ord", typeof(int));
        table.Columns.Add("query_text", typeof(string));
        foreach (var (ord, text) in rows)
        {
            table.Rows.Add(ord, (object?)text ?? DBNull.Value);
        }

        await using var reader = table.CreateDataReader();
        return await QueryStatsCollector.ReadTextFetchAsync(reader, new SensitiveStatements.Session(), CancellationToken.None, unjudged);
    }

    /// <summary>
    /// The first of the issue's two tests. A row whose statement the host already committed is not a miss, so no text read
    /// is built for it, and the write stores the digest and no text.
    /// </summary>
    [Fact]
    public void AKnownDigest_SendsNoTextRead_AndTheWriteStoresTheDigestAlone()
    {
        var row = RowFor(1);
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        var key = QueryStatsPlanKey.TryCreate(7, row)!.Value;
        var digest = new string('A', 64);
        cache.AddPending(key, digest, 20, Now);
        cache.ConfirmPending(new[] { key }, Now);

        var misses = DarlingCollectorRunner.PartitionDeferredQueryStatsText(7, new[] { row }, cache, Now);

        Assert.Empty(misses);
        Assert.Equal(digest, row.KnownTextDigest);
        Assert.Null(row.QueryText);

        var writer = new StatementScrubRecordingWriter();
        QueryStatsCollector.Instance.WritePayload(row, writer, Context(deferText: true));
        Assert.Equal((null, digest), writer.Payloads[0]);
    }

    /// <summary>
    /// The second. A statement the host has never committed is a miss, its text is read in the same run and the write stores
    /// the text itself. After the commit the next run finds it known.
    /// </summary>
    [Fact]
    public async Task ANewStatement_GetsItsTextInTheSameRun_ThenIsKnownAfterTheCommit()
    {
        var row = RowFor(2, start: 40, end: 120);
        var cache = new PlanDigestCache<QueryStatsPlanKey>();

        var misses = DarlingCollectorRunner.PartitionDeferredQueryStatsText(7, new[] { row }, cache, Now);
        Assert.Single(misses);

        var query = QueryStatsCollector.BuildTextFetchQuery(Context(deferText: true), misses.ConvertAll(m => m.Key.ToFetchKey()));
        Assert.Contains("(0, 0x06" + 2.ToString("X8", System.Globalization.CultureInfo.InvariantCulture) + ", 40, 120)", query.Text, StringComparison.Ordinal);

        var unjudged = new HashSet<int>();
        var fetched = await ReadAsync(unjudged, (0, "SELECT 1 FROM dbo.t;"));
        var pending = new List<QueryStatsPlanKey>();
        var applied = DarlingCollectorRunner.ApplyFetchedQueryStatsText(misses, fetched, unjudged, cache, Now, pending);

        Assert.Equal(1, applied);
        Assert.Equal("SELECT 1 FROM dbo.t;", row.QueryText);
        Assert.Null(row.KnownTextDigest);

        var writer = new StatementScrubRecordingWriter();
        QueryStatsCollector.Instance.WritePayload(row, writer, Context(deferText: true));
        Assert.Equal(("SELECT 1 FROM dbo.t;", (string?)null), writer.Payloads[0]);

        /* Pending is not known until the batch commits. */
        Assert.Single(DarlingCollectorRunner.PartitionDeferredQueryStatsText(7, new[] { RowFor(2, 40, 120) }, cache, Now));
        cache.ConfirmPending(pending, Now);
        var next = RowFor(2, 40, 120);
        Assert.Empty(DarlingCollectorRunner.PartitionDeferredQueryStatsText(7, new[] { next }, cache, Now));
        Assert.NotNull(next.KnownTextDigest);
    }

    /// <summary>
    /// A text that did not come back is stored as no text, never as an empty string, and is read again next run: nothing
    /// is cached for it. The same holds for a text the statement filter could not judge, which is stored as the marker.
    /// </summary>
    [Fact]
    public async Task ATextThatDidNotComeBack_IsStoredWithNoText_AndIsAMissAgainNextRun()
    {
        var gone = RowFor(3);
        var empty = RowFor(4);
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        var misses = DarlingCollectorRunner.PartitionDeferredQueryStatsText(7, new[] { gone, empty }, cache, Now);

        var unjudged = new HashSet<int>();
        var fetched = await ReadAsync(unjudged, (0, null), (1, string.Empty));
        var pending = new List<QueryStatsPlanKey>();
        var applied = DarlingCollectorRunner.ApplyFetchedQueryStatsText(misses, fetched, unjudged, cache, Now, pending);

        Assert.Equal(0, applied);
        Assert.Null(gone.QueryText);
        Assert.Null(empty.QueryText);
        Assert.Empty(pending);
        Assert.Equal(0, cache.Count);

        var writer = new StatementScrubRecordingWriter();
        QueryStatsCollector.Instance.WritePayload(gone, writer, Context(deferText: true));
        Assert.Equal(((string?)null, (string?)null), writer.Payloads[0]);

        Assert.Equal(2, DarlingCollectorRunner.PartitionDeferredQueryStatsText(7, new[] { gone, empty }, cache, Now).Count);

        /* Unjudged: the row keeps the marker the filter returned, and the cache learns nothing. */
        var marker = RowFor(5);
        var unjudgedMisses = DarlingCollectorRunner.PartitionDeferredQueryStatsText(7, new[] { marker }, cache, Now);
        var flagged = new HashSet<int> { 0 };
        var applied2 = DarlingCollectorRunner.ApplyFetchedQueryStatsText(
            unjudgedMisses, new Dictionary<int, string?> { [0] = "[withheld]" }, flagged, cache, Now, pending);
        Assert.Equal(1, applied2);
        Assert.Equal("[withheld]", marker.QueryText);
        Assert.Empty(pending);
        Assert.Equal(0, cache.Count);
    }

    /// <summary>
    /// The key is the plan, never the query hash. Two statements with one query_hash and different texts are two misses, a
    /// statement that recompiled (a new creation time, as an ALTER of a module presents) is a miss under its old plan handle,
    /// and a row with no usable plan handle is left with no text rather than given another row's.
    /// </summary>
    [Fact]
    public void TheKeyIsThePlan_SoTheSameHashOrAChangedStatementNeverTakesAnotherTextsDigest()
    {
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        var known = RowFor(6);
        var knownKey = QueryStatsPlanKey.TryCreate(7, known)!.Value;
        cache.AddPending(knownKey, new string('B', 64), 10, Now);
        cache.ConfirmPending(new[] { knownKey }, Now);

        var sameHashOtherLiteral = RowFor(7);
        var sameHandleAfterAlter = RowFor(6, created: Created.AddMinutes(30));
        var otherStatementInTheBatch = RowFor(6, start: 80, end: 200);
        var noHandle = RowFor(8);
        noHandle.PlanHandle = null;

        var misses = DarlingCollectorRunner.PartitionDeferredQueryStatsText(
            7, new[] { known, sameHashOtherLiteral, sameHandleAfterAlter, otherStatementInTheBatch, noHandle }, cache, Now);

        Assert.Equal(3, misses.Count);
        Assert.Equal(new string('B', 64), known.KnownTextDigest);
        Assert.Null(sameHashOtherLiteral.KnownTextDigest);
        Assert.Null(sameHandleAfterAlter.KnownTextDigest);
        Assert.Null(otherStatementInTheBatch.KnownTextDigest);
        Assert.Null(noHandle.KnownTextDigest);
        Assert.DoesNotContain(misses, m => ReferenceEquals(m.Row, noHandle));
    }

    /// <summary>The candidate query drops the text only when the host asks, and the text read slices by the same offsets.</summary>
    [Fact]
    public void TheCandidateQueryReturnsNoTextOnlyWhenTheHostDefersIt()
    {
        var inline = QueryStatsCollector.Instance.BuildQuery(Context(deferText: false)).Text;
        var deferred = QueryStatsCollector.Instance.BuildQuery(Context(deferText: true)).Text;

        Assert.Contains("SUBSTRING", inline, StringComparison.Ordinal);
        Assert.DoesNotContain("SUBSTRING", deferred, StringComparison.Ordinal);

        /* The apply stays: the own-statement filter and the host object name read it. */
        Assert.Contains("OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st", deferred, StringComparison.Ordinal);
        Assert.Contains("NOT LIKE N'%PerformanceMonitorLite%'", deferred, StringComparison.Ordinal);

        var fetch = QueryStatsCollector.BuildTextFetchQuery(
            Context(deferText: true), new[] { new QueryStatsCollector.PlanFetchKey(new byte[] { 0x06, 0xAB }, 0, -1) }).Text;
        Assert.Contains("sys.dm_exec_sql_text(k.plan_handle)", fetch, StringComparison.Ordinal);
        Assert.Contains("(0, 0x06AB, 0, -1)", fetch, StringComparison.Ordinal);

        Assert.Throws<InvalidOperationException>(() => QueryStatsCollector.BuildTextFetchQuery(
            Context(deferText: false), new[] { new QueryStatsCollector.PlanFetchKey(new byte[] { 1 }, 0, -1) }));
        Assert.Throws<InvalidOperationException>(() => QueryStatsCollector.BuildTextFetchQuery(
            Context(deferText: true), Array.Empty<QueryStatsCollector.PlanFetchKey>()));
    }

    [Fact]
    public void AzureSqlDatabaseKeepsTheTextInTheMainQuery()
    {
        Assert.True(DarlingCollectorRunner.ShouldDeferTextFetchFor("query_stats", new CollectorTargetInfo()));
        Assert.False(DarlingCollectorRunner.ShouldDeferTextFetchFor("query_stats", new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(DarlingCollectorRunner.ShouldDeferTextFetchFor("procedure_stats", new CollectorTargetInfo()));
    }
}
