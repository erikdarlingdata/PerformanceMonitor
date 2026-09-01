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
/// Pins for <see cref="QueryStoreOpenIntervalState"/> (#2312) — the per-database stamp that decides
/// which cycles re-read the OPEN Query Store interval. The decision table matters because every wrong
/// answer is silent in a different direction: a false "skip" starves the current hour's view, a false
/// "include" quietly keeps paying the 40–110 s open-interval bill this state exists to cut.
/// </summary>
public sealed class QueryStoreOpenIntervalStateTests
{
    private static readonly DateTime Now = new(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);

    private static Dictionary<string, string> StateWith(string databaseName, string raw) =>
        new(StringComparer.Ordinal) { [QueryStoreOpenIntervalState.KeyFor(databaseName)] = raw };

    private static string StampedAt(DateTime utc) => QueryStoreOpenIntervalState.Format(utc);

    /// <summary>
    /// Include is the CONSERVATIVE default: a first run, a restarted host, a broken store and a
    /// clock-skewed stamp all behave exactly like today's collector rather than silently going stale.
    /// </summary>
    [Fact]
    public void AbsentMalformedAndFutureStampsAllInclude()
    {
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(null, "SO", Now));
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(
            new Dictionary<string, string>(StringComparer.Ordinal), "SO", Now));
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(StateWith("SO", ""), "SO", Now));
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(StateWith("SO", "not-a-number"), "SO", Now));
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(StateWith("SO", "-5"), "SO", Now));
        /* Numeric but beyond FromUnixTimeSeconds's year-9999 ceiling (review catch): parses as a long,
           so it must fall to the ArgumentOutOfRangeException guard, not throw through it. */
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(StateWith("SO", "999999999999999"), "SO", Now));
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(StateWith("SO", long.MaxValue.ToString()), "SO", Now));
        /* Future stamp = the clock moved backwards; honoring it would pin the snapshot stale for as
           long as the skew lasts. */
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(
            StateWith("SO", StampedAt(Now.AddMinutes(10))), "SO", Now));
    }

    /// <summary>The refresh boundary, both sides, inclusive at exactly RefreshEvery.</summary>
    [Fact]
    public void FreshStampSkips_StaleStampIncludes()
    {
        Assert.False(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(
            StateWith("SO", StampedAt(Now.AddMinutes(-5))), "SO", Now));
        Assert.False(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(
            StateWith("SO", StampedAt(Now - QueryStoreOpenIntervalState.RefreshEvery + TimeSpan.FromSeconds(1))), "SO", Now));
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(
            StateWith("SO", StampedAt(Now - QueryStoreOpenIntervalState.RefreshEvery)), "SO", Now));
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(
            StateWith("SO", StampedAt(Now.AddHours(-2))), "SO", Now));
    }

    /// <summary>Databases are independent: one database's fresh stamp must not skip another's refresh.</summary>
    [Fact]
    public void StampsArePerDatabase()
    {
        var state = StateWith("A", StampedAt(Now.AddMinutes(-1)));

        Assert.False(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(state, "A", Now));
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(state, "B", Now));
    }

    /// <summary>
    /// The owner seam, pinned from both ends like the plan and text watermarks: the definition declares
    /// no state keys, and this state's owner name differs from the collector's — a row written under
    /// "query_store" would never be read back, so the skip would silently never apply and collection
    /// would quietly keep paying full price.
    /// </summary>
    [Fact]
    public void OwnerIsItsOwnStateCollectorName()
    {
        Assert.Empty(QueryStoreCollector.Instance.StateKeys);
        Assert.NotEqual(QueryStoreOpenIntervalState.StateCollectorName, QueryStoreCollector.Instance.Name);
        Assert.Equal("query_store_open_interval", QueryStoreOpenIntervalState.StateCollectorName);
        Assert.Equal("qsowm:", QueryStoreOpenIntervalState.WatermarkKeyPrefix);
        Assert.Equal("qsowm:SO", QueryStoreOpenIntervalState.KeyFor("SO"));
    }

    /// <summary>Format/parse round-trip at second granularity, and the 30-minute horizon stays a recorded decision (#2759).</summary>
    [Fact]
    public void FormatRoundTripsAndTheHorizonIsPinned()
    {
        var stamp = QueryStoreOpenIntervalState.Format(Now);
        Assert.False(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(StateWith("SO", stamp), "SO", Now.AddMinutes(29)));
        Assert.True(QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(StateWith("SO", stamp), "SO", Now.AddMinutes(30)));
        Assert.Equal(TimeSpan.FromMinutes(30), QueryStoreOpenIntervalState.RefreshEvery);
    }
}
