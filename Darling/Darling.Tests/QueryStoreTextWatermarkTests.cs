/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2150: the per-database watermark for the query-text fetch — the sibling of
/// <see cref="QueryStorePlanWatermarkTests"/>, pinning the same conservative-zero rules on a second
/// catalog.
///
/// <para><b>Why the text fetch exists.</b> The runtime payload selected <c>query_sql_text</c>
/// (<c>nvarchar(max)</c>) inside a <c>TOP ... WITH TIES ... ORDER BY last_execution_time</c>. A Top-N Sort
/// carries every output column through the sort and reads ALL of its input before emitting a row, so
/// choosing the rows to ship materialized text for the entire qualifying set. With #2210's plan XML
/// already gone and that column as the only difference, time-to-first-row measured 4.67s against 0.45s at
/// 1,505 rows and 5.02s against 0.57s at 4,037. Neither the row cap nor the client byte budget bounds it:
/// <c>TOP (500)</c> measured the same as <c>TOP (50000)</c>, and wall time was flat from a 4 MB to a
/// 256 MB budget, because the server is finished before the client sees a byte.</para>
///
/// <para>Every zero below is the same deliberate choice: an absent, malformed, expired or future-stamped
/// watermark means "fetch everything", because a first run, a restarted host and a broken store are
/// indistinguishable from here and all three must refetch rather than skip.</para>
/// </summary>
public sealed class QueryStoreTextWatermarkTests
{
    private const string Db = "SO";

    private static DateTime Now => new(2026, 8, 16, 12, 0, 0, DateTimeKind.Utc);

    private static Dictionary<string, string> StateWith(long queryId, DateTime stampedAt) =>
        new() { [QueryStoreTextState.KeyFor(Db)] = QueryStoreTextState.Format(queryId, stampedAt) };

    [Fact]
    public void AFreshWatermarkRoundTrips()
    {
        Assert.Equal(900, QueryStoreTextState.Resolve(StateWith(900, Now), Db, Now));
        Assert.Equal(Now, QueryStoreTextState.ResolveStamp(StateWith(900, Now), Db));
    }

    /// <summary>
    /// Past the refresh horizon the watermark expires to 0 — a full re-walk.
    ///
    /// <para>Not decoration: <c>query_id</c> is monotonic in FIRST-SEEN order, not in "we have stored it",
    /// so a Query Store reset renumbers ids from the start and every text would arrive below a standing
    /// watermark. Without a bounded horizon that suppresses text forever.</para>
    /// </summary>
    [Fact]
    public void PastTheRefreshHorizonItRefetchesEverything()
    {
        var state = StateWith(900, Now);

        Assert.Equal(900, QueryStoreTextState.Resolve(state, Db, Now + QueryStoreTextState.RefreshAfter - TimeSpan.FromMinutes(1)));
        Assert.Equal(0, QueryStoreTextState.Resolve(state, Db, Now + QueryStoreTextState.RefreshAfter));
    }

    /// <summary>
    /// A future stamp is refused, or a backwards clock would pin the watermark for as long as the skew
    /// lasts.
    /// </summary>
    [Fact]
    public void AFutureStampIsRefused()
        => Assert.Equal(0, QueryStoreTextState.Resolve(StateWith(900, Now), Db, Now.AddHours(-1)));

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("garbage")]
    [InlineData("900")]
    [InlineData(":123")]
    [InlineData("900:")]
    [InlineData("-5:123")]
    [InlineData("900:notanumber")]
    public void AMalformedWatermarkRefetchesEverything(string raw)
    {
        var state = new Dictionary<string, string> { [QueryStoreTextState.KeyFor(Db)] = raw };

        Assert.Equal(0, QueryStoreTextState.Resolve(state, Db, Now));
        Assert.Null(QueryStoreTextState.ResolveStamp(state, Db));
    }

    [Fact]
    public void AnAbsentDatabaseRefetchesEverything()
    {
        Assert.Equal(0, QueryStoreTextState.Resolve(StateWith(900, Now), "somewhere-else", Now));
        Assert.Equal(0, QueryStoreTextState.Resolve(new Dictionary<string, string>(), Db, Now));
        Assert.Equal(0, QueryStoreTextState.Resolve(null!, Db, Now));
    }

    [Fact]
    public void TheWatermarkAdvancesToTheHighestLandedId()
    {
        var advance = QueryStoreTextState.AdvanceWatermark(100, new long[] { 101, 102, 103 });

        Assert.Equal(103, advance.Watermark);
        Assert.True(advance.ArrivedInQueryIdOrder);
    }

    /// <summary>
    /// Out-of-order arrival HOLDS the watermark, because the ordering is the whole safety argument: a
    /// budget cut is only a suffix if the ids arrived sorted, and advancing past a gap would strand
    /// unstored text behind a strict comparison permanently.
    /// </summary>
    [Fact]
    public void OutOfOrderArrivalHoldsTheWatermark()
    {
        var advance = QueryStoreTextState.AdvanceWatermark(100, new long[] { 101, 99, 102 });

        Assert.Equal(100, advance.Watermark);
        Assert.False(advance.ArrivedInQueryIdOrder);
    }

    /// <summary>
    /// A quiet pass is a quiet pass, not a reset. Lowering the watermark because nothing new arrived would
    /// refetch the catalog on every idle cycle.
    /// </summary>
    [Fact]
    public void AQuietPassNeverLowersTheWatermark()
    {
        Assert.Equal(100, QueryStoreTextState.AdvanceWatermark(100, Array.Empty<long>()).Watermark);
        Assert.Equal(100, QueryStoreTextState.AdvanceWatermark(100, null!).Watermark);
        Assert.Equal(100, QueryStoreTextState.AdvanceWatermark(100, new long[] { 5, 6 }).Watermark);
        Assert.True(QueryStoreTextState.AdvanceWatermark(100, Array.Empty<long>()).ArrivedInQueryIdOrder);
    }

    /// <summary>
    /// The stamp survives an advance, which is what makes the refresh horizon reachable at all: re-stamping
    /// on every advance would push it out forever on any database that keeps seeing new statements — which
    /// is exactly where a Query Store reset would hurt most.
    /// </summary>
    [Fact]
    public void AnAdvanceCanCarryTheOriginalStampForward()
    {
        var originalStamp = Now.AddHours(-6);
        var carried = QueryStoreTextState.Format(950, originalStamp);
        var state = new Dictionary<string, string> { [QueryStoreTextState.KeyFor(Db)] = carried };

        Assert.Equal(950, QueryStoreTextState.Resolve(state, Db, Now));
        Assert.Equal(originalStamp, QueryStoreTextState.ResolveStamp(state, Db));
        /* Six hours in, the horizon is still six hours closer than a re-stamp would have left it. */
        Assert.Equal(0, QueryStoreTextState.Resolve(state, Db, originalStamp + QueryStoreTextState.RefreshAfter));
    }

    /// <summary>
    /// Text and plan watermarks live under DIFFERENT collector names. They walk different catalogs at
    /// different rates, and sharing state would let one side's reset drop the other's watermark for no
    /// reason.
    /// </summary>
    [Fact]
    public void TheTextWatermarkIsStoredSeparatelyFromThePlanWatermark()
    {
        Assert.NotEqual(QueryStorePlanXmlState.StateCollectorName, QueryStoreTextState.StateCollectorName);
        Assert.NotEqual(QueryStorePlanXmlState.WatermarkKeyPrefix, QueryStoreTextState.WatermarkKeyPrefix);
        Assert.NotEqual(QueryStorePlanXmlState.KeyFor(Db), QueryStoreTextState.KeyFor(Db));
    }
}
