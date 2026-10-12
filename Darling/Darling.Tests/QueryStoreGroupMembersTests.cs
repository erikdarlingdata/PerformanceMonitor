/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582: the safety factor on the <c>pg_stats.n_distinct</c> group-member guess, with no store. A sampled <c>n_distinct</c> runs low for a
/// long-tailed column (1.7x low for <c>module_name</c> in the field), and a low guess picks the single scan where it can pass the temp-file
/// limit, so each <c>n_distinct</c>-derived count is multiplied by <see cref="QueryStoreGroupMembers.NDistinctSafetyFactor"/>; the server
/// count is exact and is not.
/// </summary>
public sealed class QueryStoreGroupMembersTests
{
    private static QueryStoreGroupMembers.MemberCount Estimate(double? value) => new(value, true);

    private static QueryStoreGroupMembers.MemberCount Exact(double? value) => new(value, false);

    private static string Panel(string groupBy) =>
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"topN\":10,"
        + "\"groupBy\":[\"" + groupBy + "\"],\"viz\":\"line\"}";

    [Fact]
    public void TheFactor_IsFour()
    {
        Assert.Equal(4d, QueryStoreGroupMembers.NDistinctSafetyFactor);
    }

    [Fact]
    public void AnNDistinctDimension_GetsTheFactor()
    {
        Assert.Equal(2_552L, QueryStoreGroupMembers.Combine([Estimate(638d)]));
    }

    [Fact]
    public void TheServerDimension_GetsNoFactor()
    {
        Assert.Equal(43L, QueryStoreGroupMembers.Combine([Exact(43d)]));
    }

    [Fact]
    public void TwoNDistinctDimensions_GetTheFactorEach()
    {
        /* 638 modules x 132 databases, each x4: 16 x the raw product, not 4 x. */
        Assert.Equal(638L * 132L * 16L, QueryStoreGroupMembers.Combine([Estimate(638d), Estimate(132d)]));
    }

    [Fact]
    public void AServerAndAnNDistinctDimension_FactorOnlyTheNDistinctOne()
    {
        Assert.Equal(3L * 200L, QueryStoreGroupMembers.Combine([Exact(3d), Estimate(50d)]));
    }

    [Fact]
    public void AnUnknownDimension_KeepsTheWholeCountUnknown()
    {
        Assert.Null(QueryStoreGroupMembers.Combine([Estimate(null)]));
        Assert.Null(QueryStoreGroupMembers.Combine([Exact(3d), Estimate(null)]));
        Assert.Null(QueryStoreGroupMembers.Combine([Estimate(0.5d)]));
    }

    [Fact]
    public void TheFactor_MovesAGuessAcrossTheSingleScanBound()
    {
        /* 24 hourly buckets: a raw guess of 10,000 members becomes 40,000 = 960,000 base rows (still one scan); a raw 10,417
           becomes 41,668 = 1,000,032 rows, over MaxSingleScanBaseRows (two scans). Without the factor both would be single. */
        Assert.True(24 * QueryStoreGroupMembers.Combine([Estimate(10_000d)])!.Value <= ComposeLimits.MaxSingleScanBaseRows);
        Assert.True(24 * QueryStoreGroupMembers.Combine([Estimate(10_417d)])!.Value > ComposeLimits.MaxSingleScanBaseRows);
    }

    [Fact]
    public async Task TheResolver_KeepsServerExact_AndQueryHashUnknown_WithoutTouchingAStore()
    {
        var ct = TestContext.Current.CancellationToken;

        /* Neither dimension reads pg_stats, so no connection is needed. ResolveAsync catches every non-cancel fault and returns null,
           so a null result alone cannot tell "unknown by design" from "touched the null connection and failed": the capturing
           logger can. A resolver that touched the connection logs a Debug line for the fault it swallowed. */
        var quiet = new CapturingTestLogger();
        using (ReadScope.Open(quiet))
        {
            Assert.Equal(43L, await QueryStoreGroupMembers.ResolveAsync(null!, QueryStoreRankedHarness.Parse(Panel("server")), 43, ct));
            Assert.Null(await QueryStoreGroupMembers.ResolveAsync(null!, QueryStoreRankedHarness.Parse(Panel("query_hash")), 43, ct));
        }

        Assert.True(quiet.Lines.Count == 0, "the resolver swallowed a fault, so it touched the store: " + quiet.Joined);

        /* Control: a dimension that does read pg_stats fails on the null connection, and that swallowed fault does reach the logger. */
        var loud = new CapturingTestLogger();
        using (ReadScope.Open(loud))
        {
            Assert.Null(await QueryStoreGroupMembers.ResolveAsync(null!, QueryStoreRankedHarness.Parse(Panel("database_name")), 43, ct));
        }

        Assert.Equal(1, loud.CountAtLevel(LogLevel.Debug));
    }
}
