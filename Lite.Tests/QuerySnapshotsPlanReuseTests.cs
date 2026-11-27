/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9529: query_snapshots renders at most <see cref="QuerySnapshotsCollector.MaxPlansPerRun"/> plans of each
/// kind per run and does not ask the target again for an estimated plan the host still holds. These drive the
/// definition's own BuildQuery and ReadAsync with the host's <see cref="QuerySnapshotPlanCache"/>: what the query
/// binds, what a held row is stored with, what a row past the cap is stored with, and what the run's note counts.
/// </summary>
public sealed class QuerySnapshotsPlanReuseTests
{
    private const string PlanKey = "0x06000100AA|10|-1|0x1122334455667788";
    private const string OtherPlanKey = "0x06000100BB|0|-1|0x8877665544332211";
    private const string PlanXml = "<ShowPlanXML><held/></ShowPlanXML>";

    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();
    private static readonly DateTime s_start = new(2026, 11, 25, 2, 0, 0, DateTimeKind.Utc);

    private static CollectorContext Context(QuerySnapshotPlanCache? cache, DateTime collectionTime, int serverId = 7) => new()
    {
        ServerId = serverId,
        ServerName = "test-server",
        CollectionTime = collectionTime,
        Deltas = s_deltas,
        SnapshotPlanCache = cache,
    };

    /// <summary>One query row: the 35 payload columns and the three plan columns that follow them.</summary>
    private static object[] QueryRow(int sessionId, object queryPlan, int planState, object planKey, bool livePlanOverCap = false)
    {
        var row = new object[38];
        Array.Fill(row, DBNull.Value);
        row[0] = sessionId;
        row[3] = "SELECT 1";
        row[4] = queryPlan;
        row[6] = "running";
        row[35] = planState;
        row[36] = livePlanOverCap;
        row[37] = planKey;
        return row;
    }

    private static long? Measured(CollectorContext context, string label)
    {
        foreach (var measurement in context.Measurements)
        {
            if (string.Equals(measurement.Label, label, StringComparison.Ordinal))
            {
                return measurement.Value;
            }
        }

        return null;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BuildQuery_WithNoCache_BindsNoPlanKeys(bool azure)
    {
        var context = new CollectorContext
        {
            ServerId = 7,
            ServerName = "test-server",
            CollectionTime = s_start,
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
        };

        var query = QuerySnapshotsCollector.Instance.BuildQuery(context);

        Assert.Empty(query.Parameters);
        Assert.Null(context.SnapshotKnownPlans);
    }

    [Fact]
    public void BuildQuery_WithACache_BindsEveryPlanKeyParameter_SoTheBatchTextNeverChanges()
    {
        var cache = new QuerySnapshotPlanCache();

        var empty = Context(cache, s_start);
        var emptyQuery = QuerySnapshotsCollector.Instance.BuildQuery(empty);

        cache.Store(7, PlanKey, PlanXml, s_start);
        cache.Store(7, OtherPlanKey, "<ShowPlanXML><other/></ShowPlanXML>", s_start);
        var holding = Context(cache, s_start.AddMinutes(1));
        var holdingQuery = QuerySnapshotsCollector.Instance.BuildQuery(holding);

        /* One text whatever is held: the target keeps one cached batch, not one per set of keys. */
        Assert.Equal(emptyQuery.Text, holdingQuery.Text);

        Assert.Equal(QuerySnapshotPlanCache.MaxKnownPlansPerServer, emptyQuery.Parameters.Count);
        Assert.All(emptyQuery.Parameters, p => Assert.Null(p.Value));
        Assert.Empty(empty.SnapshotKnownPlans!);

        Assert.Equal(QuerySnapshotPlanCache.MaxKnownPlansPerServer, holdingQuery.Parameters.Count);
        Assert.All(holdingQuery.Parameters, p => Assert.Equal(CollectorParameterType.NVarChar260, p.Type));
        var bound = holdingQuery.Parameters.Where(p => p.Value is not null).Select(p => (string)p.Value!).OrderBy(k => k, StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { PlanKey, OtherPlanKey }.OrderBy(k => k, StringComparer.Ordinal), bound);
        Assert.Equal(2, holding.SnapshotKnownPlans!.Count);
    }

    [Fact]
    public async Task ReadAsync_AFetchedPlan_IsHeldForLaterRuns_UntilItsLifetimeEnds()
    {
        var cache = new QuerySnapshotPlanCache();
        var context = Context(cache, s_start);
        QuerySnapshotsCollector.Instance.BuildQuery(context);

        using var reader = new FakeCollectorDataReader(QueryRow(61, PlanXml, planState: 0, PlanKey));
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(PlanXml, Assert.Single(rows).QueryPlan);
        Assert.Null(Measured(context, QuerySnapshotsCollector.PlansReusedMeasurement));
        Assert.Null(Measured(context, QuerySnapshotsCollector.PlansOverCapMeasurement));
        Assert.Null(Measured(context, QuerySnapshotsCollector.LivePlansOverCapMeasurement));

        var lastMinute = s_start + QuerySnapshotPlanCache.PlanLifetime - TimeSpan.FromMinutes(1);
        Assert.Equal(PlanXml, cache.Known(7, lastMinute)[PlanKey]);
        Assert.Empty(cache.Known(7, s_start + QuerySnapshotPlanCache.PlanLifetime));
        Assert.Equal(0, cache.TotalChars);
    }

    [Fact]
    public async Task ReadAsync_AHeldRow_IsStoredWithTheHostsCopyOfItsPlan_AndTheRunCountsIt()
    {
        var cache = new QuerySnapshotPlanCache();
        cache.Store(7, PlanKey, PlanXml, s_start);

        var context = Context(cache, s_start.AddMinutes(1));
        var query = QuerySnapshotsCollector.Instance.BuildQuery(context);
        Assert.Contains(query.Parameters, p => (string?)p.Value == PlanKey);

        /* The target rendered nothing for the held row: its plan column is NULL and plan_state says why. */
        using var reader = new FakeCollectorDataReader(
            QueryRow(61, DBNull.Value, planState: 1, PlanKey),
            QueryRow(62, "<ShowPlanXML><fresh/></ShowPlanXML>", planState: 0, OtherPlanKey));
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(PlanXml, rows[0].QueryPlan);
        Assert.Equal("<ShowPlanXML><fresh/></ShowPlanXML>", rows[1].QueryPlan);
        Assert.Equal(1, Measured(context, QuerySnapshotsCollector.PlansReusedMeasurement));
        Assert.Null(Measured(context, QuerySnapshotsCollector.PlansOverCapMeasurement));

        /* The stored row carries the plan itself, in the plan column a reader already opens. */
        var writer = new RecordingCollectorRowWriter();
        QuerySnapshotsCollector.Instance.WritePayload(rows[0], writer, context);
        Assert.Equal(35, writer.Values.Count);
        Assert.Equal(PlanXml, writer.Values[4]);
    }

    [Fact]
    public async Task ReadAsync_AHeldRow_IsFilledFromTheSetTheQueryNamed_WhateverTheCacheHoldsByThen()
    {
        var cache = new QuerySnapshotPlanCache();
        cache.Store(7, PlanKey, PlanXml, s_start);

        var context = Context(cache, s_start.AddMinutes(1));
        QuerySnapshotsCollector.Instance.BuildQuery(context);

        /* Another run replaces the plan between this run's query and its read. */
        cache.Store(7, PlanKey, "<ShowPlanXML><replaced/></ShowPlanXML>", s_start.AddMinutes(1));

        using var reader = new FakeCollectorDataReader(QueryRow(61, DBNull.Value, planState: 1, PlanKey));
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(PlanXml, Assert.Single(rows).QueryPlan);
    }

    [Fact]
    public async Task ReadAsync_ARowPastTheCap_IsStoredWithNoPlan_AndTheRunCountsIt()
    {
        var cache = new QuerySnapshotPlanCache();
        var context = Context(cache, s_start);
        QuerySnapshotsCollector.Instance.BuildQuery(context);

        using var reader = new FakeCollectorDataReader(
            QueryRow(61, PlanXml, planState: 0, PlanKey),
            QueryRow(62, DBNull.Value, planState: 2, OtherPlanKey, livePlanOverCap: true),
            QueryRow(63, DBNull.Value, planState: 2, "0x06000100CC|0|-1|0x0000000000000001", livePlanOverCap: true),
            /* A request with no plan handle has no plan and is not a skip. */
            QueryRow(64, DBNull.Value, planState: 3, DBNull.Value));
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(4, rows.Count);
        Assert.Null(rows[1].QueryPlan);
        Assert.Null(rows[2].QueryPlan);
        Assert.Null(rows[3].QueryPlan);
        Assert.Equal(2, Measured(context, QuerySnapshotsCollector.PlansOverCapMeasurement));
        Assert.Equal(2, Measured(context, QuerySnapshotsCollector.LivePlansOverCapMeasurement));

        /* Only the fetched plan is held: a skipped row is asked for again on the next run. */
        Assert.Equal(new[] { PlanKey }, cache.Known(7, s_start.AddMinutes(1)).Keys);
    }

    [Fact]
    public async Task ReadAsync_WithNoCache_ReadsThePlanColumns_AndHoldsNothing()
    {
        var context = Context(cache: null, s_start);
        QuerySnapshotsCollector.Instance.BuildQuery(context);

        using var reader = new FakeCollectorDataReader(
            QueryRow(61, PlanXml, planState: 0, PlanKey),
            QueryRow(62, DBNull.Value, planState: 2, OtherPlanKey));
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(PlanXml, rows[0].QueryPlan);
        Assert.Null(rows[1].QueryPlan);
        Assert.Equal(1, Measured(context, QuerySnapshotsCollector.PlansOverCapMeasurement));
    }

    [Fact]
    public async Task ReadAsync_RequestsThatShareAPlanKey_AllCarryThePlanItsFirstRequestFetched_AndTheRunCountsThem()
    {
        var cache = new QuerySnapshotPlanCache();
        var context = Context(cache, s_start);
        QuerySnapshotsCollector.Instance.BuildQuery(context);

        using var reader = new FakeCollectorDataReader(
            QueryRow(61, PlanXml, planState: 0, PlanKey),
            QueryRow(62, DBNull.Value, planState: 4, PlanKey),
            QueryRow(63, DBNull.Value, planState: 4, PlanKey),
            /* A key past the cap: neither its first request nor the one that shares it was rendered. */
            QueryRow(64, DBNull.Value, planState: 2, OtherPlanKey),
            QueryRow(65, DBNull.Value, planState: 4, OtherPlanKey));
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(5, rows.Count);
        Assert.Equal(PlanXml, rows[0].QueryPlan);
        Assert.Equal(PlanXml, rows[1].QueryPlan);
        Assert.Equal(PlanXml, rows[2].QueryPlan);
        Assert.Null(rows[3].QueryPlan);
        Assert.Null(rows[4].QueryPlan);
        Assert.Equal(2, Measured(context, QuerySnapshotsCollector.PlansSharedMeasurement));
        Assert.Equal(2, Measured(context, QuerySnapshotsCollector.PlansOverCapMeasurement));
        Assert.Null(Measured(context, QuerySnapshotsCollector.PlansReusedMeasurement));

        /* One fetch per key: only the fetched key is held, once. */
        Assert.Equal(new[] { PlanKey }, cache.Known(7, s_start.AddMinutes(1)).Keys);
    }

    [Fact]
    public async Task ReadAsync_ARequestThatSharesAPlanKey_IsFilledWhereverItsFirstRequestSitsInTheResult()
    {
        var context = Context(cache: null, s_start);
        QuerySnapshotsCollector.Instance.BuildQuery(context);

        /* No host cache (Darling's on-demand live read), and the sharing row arrives before the row that fetched. */
        using var reader = new FakeCollectorDataReader(
            QueryRow(62, DBNull.Value, planState: 4, PlanKey),
            QueryRow(61, PlanXml, planState: 0, PlanKey));
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(62, rows[0].SessionId);
        Assert.Equal(PlanXml, rows[0].QueryPlan);
        Assert.Equal(PlanXml, rows[1].QueryPlan);
        Assert.Equal(1, Measured(context, QuerySnapshotsCollector.PlansSharedMeasurement));
    }

    [Fact]
    public async Task ReadAsync_ARequestThatSharesAPlanKeyTheTargetRenderedNothingFor_IsStoredWithNoPlan_AndIsNotCounted()
    {
        var context = Context(new QuerySnapshotPlanCache(), s_start);
        QuerySnapshotsCollector.Instance.BuildQuery(context);

        /* The first request was inside the cap and the target returned no plan for it. */
        using var reader = new FakeCollectorDataReader(
            QueryRow(61, DBNull.Value, planState: 0, PlanKey),
            QueryRow(62, DBNull.Value, planState: 4, PlanKey));
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Null(rows[0].QueryPlan);
        Assert.Null(rows[1].QueryPlan);
        Assert.Null(Measured(context, QuerySnapshotsCollector.PlansSharedMeasurement));
        Assert.Null(Measured(context, QuerySnapshotsCollector.PlansOverCapMeasurement));
    }

    [Fact]
    public void Cache_HoldsEachServersPlansApart()
    {
        var cache = new QuerySnapshotPlanCache();
        cache.Store(7, PlanKey, PlanXml, s_start);

        Assert.Empty(cache.Known(8, s_start));
        Assert.Single(cache.Known(7, s_start));
    }

    [Fact]
    public void Cache_AtTheServerLimit_DropsThePlanSeenLongestAgo()
    {
        var cache = new QuerySnapshotPlanCache();
        for (var i = 0; i < QuerySnapshotPlanCache.MaxKnownPlansPerServer; i++)
        {
            cache.Store(7, "key-" + i, "<p/>", s_start);
        }

        /* Every plan but the first is used by the next run, so the first is the one seen longest ago. */
        for (var i = 1; i < QuerySnapshotPlanCache.MaxKnownPlansPerServer; i++)
        {
            cache.Touch(7, "key-" + i, s_start.AddMinutes(1));
        }

        cache.Store(7, "key-new", "<p/>", s_start.AddMinutes(1));

        var held = cache.Known(7, s_start.AddMinutes(1));
        Assert.Equal(QuerySnapshotPlanCache.MaxKnownPlansPerServer, held.Count);
        Assert.False(held.ContainsKey("key-0"));
        Assert.True(held.ContainsKey("key-1"));
        Assert.True(held.ContainsKey("key-new"));
    }

    [Fact]
    public void Cache_StaysInsideTheServerCharacterLimit_AndDoesNotHoldAPlanLargerThanIt()
    {
        var cache = new QuerySnapshotPlanCache();
        var half = new string('x', (int)(QuerySnapshotPlanCache.MaxServerChars / 2));

        cache.Store(7, "a", half, s_start);
        cache.Store(7, "b", half, s_start.AddSeconds(1));
        Assert.Equal(QuerySnapshotPlanCache.MaxServerChars, cache.TotalChars);

        /* A third plan does not fit beside both, so the one seen longest ago leaves. */
        cache.Store(7, "c", half, s_start.AddSeconds(2));
        var held = cache.Known(7, s_start.AddSeconds(3));
        Assert.Equal(new[] { "b", "c" }, held.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(QuerySnapshotPlanCache.MaxServerChars, cache.TotalChars);

        /* A plan over the whole limit is not held at all, and the run after it fetches it again. */
        cache.Store(8, "huge", new string('x', (int)QuerySnapshotPlanCache.MaxServerChars + 1), s_start);
        Assert.Empty(cache.Known(8, s_start.AddSeconds(3)));
        Assert.Equal(QuerySnapshotPlanCache.MaxServerChars, cache.TotalChars);
    }

    [Fact]
    public void Cache_APlanLargerThanTheServerLimit_LeavesWhatTheServerHoldsAsItWas()
    {
        var cache = new QuerySnapshotPlanCache();
        cache.Store(7, "a", PlanXml, s_start);
        cache.Store(7, "b", PlanXml, s_start.AddSeconds(1));
        var charsBefore = cache.TotalChars;

        /* A plan that cannot be held at all makes no room: the plans the server already holds stay. */
        cache.Store(7, "huge", new string('x', (int)QuerySnapshotPlanCache.MaxServerChars + 1), s_start.AddSeconds(2));

        var held = cache.Known(7, s_start.AddSeconds(3));
        Assert.Equal(new[] { "a", "b" }, held.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(PlanXml, held["a"]);
        Assert.Equal(PlanXml, held["b"]);
        Assert.Equal(charsBefore, cache.TotalChars);
    }

    [Fact]
    public void Cache_DropsAServersPlans_OnceNoRunHasAskedForThemForALifetime()
    {
        var cache = new QuerySnapshotPlanCache();
        cache.Store(7, PlanKey, PlanXml, s_start);
        Assert.Equal(PlanXml.Length, cache.TotalChars);

        /* Server 7 is never collected again. Another server's run past the lifetime clears what it left. */
        cache.Known(8, s_start + QuerySnapshotPlanCache.PlanLifetime);

        Assert.Equal(0, cache.TotalChars);
    }
}
