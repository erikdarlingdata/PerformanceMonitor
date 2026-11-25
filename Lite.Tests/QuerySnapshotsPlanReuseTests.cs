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
    public void BuildSnapshotQuery_RendersAnEstimatedPlanOnlyForTheFirstRankedRows(bool azure)
    {
        var sql = QuerySnapshotsCollector.BuildSnapshotQuery(supportsLiveQueryPlan: false, isAzureSqlDatabase: azure);

        Assert.Equal(20, QuerySnapshotsCollector.MaxPlansPerRun);
        Assert.Contains("CASE WHEN der.plan_state = 0 AND der.plan_rank <= 20 THEN der.plan_handle END,", sql, StringComparison.Ordinal);
        Assert.Contains(
            ") AS tqp\n    WHERE der.plan_state = 0\n    AND   der.plan_rank <= 20\n) AS deqp",
            sql.ReplaceLineEndings("\n"),
            StringComparison.Ordinal);
        Assert.Contains("plan_state = CASE WHEN der.plan_state = 0 AND der.plan_rank > 20 THEN 2 ELSE der.plan_state END,", sql, StringComparison.Ordinal);
        /* The live-read form holds nothing, so no row is ever marked held and nothing is bound. */
        Assert.Contains("WHEN 1 = 0", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@pm_held_plan_", sql, StringComparison.Ordinal);
        Assert.Contains("live_plan_over_cap = CONVERT(bit, 0),", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSnapshotQuery_RendersALivePlanOnlyForTheFirstRankedRows()
    {
        var sql = QuerySnapshotsCollector.BuildSnapshotQuery(supportsLiveQueryPlan: true, isAzureSqlDatabase: false);

        /* Both halves are needed. The NULL session id keeps the function from rendering a plan, and the WHERE is
           what leaves the row without one: inside an APPLY the function answers a NULL session id with the plan
           of the last session it was asked for (measured on SQL Server 2022 and 2025). */
        Assert.Contains(
            "FROM sys.dm_exec_query_statistics_xml(CASE WHEN der.plan_handle IS NOT NULL AND der.live_plan_rank <= 20 THEN der.session_id END) AS qsx\n"
            + "    WHERE der.plan_handle IS NOT NULL\n"
            + "    AND   der.live_plan_rank <= 20\n"
            + ") AS deqs",
            sql.ReplaceLineEndings("\n"),
            StringComparison.Ordinal);
        Assert.Contains(
            "live_plan_over_cap = CONVERT(bit, CASE WHEN der.plan_handle IS NOT NULL AND der.live_plan_rank > 20 THEN 1 ELSE 0 END),",
            sql,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BuildSnapshotQuery_KeepsRecompileOnEveryStatement(bool azure)
    {
        var sql = QuerySnapshotsCollector.BuildSnapshotQuery(supportsLiveQueryPlan: false, isAzureSqlDatabase: azure);

        Assert.Contains("OPTION(MAXDOP 1, RECOMPILE);", sql, StringComparison.Ordinal);
        if (azure)
        {
            /* The staging statement ranks the rows and compares the bound plan keys, so it carries its own hint. */
            var staging = sql[..sql.IndexOf("SELECT /* PerformanceMonitorLite */", StringComparison.Ordinal)];
            Assert.Contains("INTO #req", staging, StringComparison.Ordinal);
            Assert.Contains("OPTION(RECOMPILE);", staging, StringComparison.Ordinal);
        }
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
        Assert.DoesNotContain("@pm_held_plan_", query.Text, StringComparison.Ordinal);
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
        Assert.Contains("WHEN pk.plan_key IN (@pm_held_plan_0, @pm_held_plan_1,", holdingQuery.Text, StringComparison.Ordinal);
        Assert.Contains("@pm_held_plan_31)", holdingQuery.Text, StringComparison.Ordinal);

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
    public void BuildQuery_PutsTheExclusionInsideTheRankedSet_OnABoxedTarget()
    {
        var context = new CollectorContext
        {
            ServerId = 7,
            ServerName = "test-server",
            CollectionTime = s_start,
            Deltas = s_deltas,
            ExcludedDatabases = new[] { "SO" },
        };

        var sql = QuerySnapshotsCollector.Instance.BuildQuery(context).Text;

        /* An excluded database's request is left out before the rows are ranked, so it cannot take a plan from a
           stored request, and no plan is rendered for it. */
        var exclusion = sql.IndexOf("AND d.name NOT IN (@excl_db_0)", StringComparison.Ordinal);
        var rankedSetEnd = sql.IndexOf(") AS der", StringComparison.Ordinal);
        Assert.True(exclusion > 0, "the exclusion clause is in the query");
        Assert.True(exclusion < rankedSetEnd, "the exclusion clause sits inside the ranked request set");
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
