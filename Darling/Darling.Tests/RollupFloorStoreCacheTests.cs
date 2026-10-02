/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4957, with no database: the pure halves of the per-store rollup floor cache — which store a data source belongs
/// to, which views a cycle measures inline versus in the background, and how a cycle's result is written back.
/// The end-to-end behavior is in <c>RollupFloorCacheLiveTests</c>.
/// </summary>
public sealed class RollupFloorStoreCacheTests
{
    private const string View = TimescaleSupport.QueryStoreStatsIntervalHourlyView;
    private static readonly DateTime Now = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Floor = Now.AddDays(-30);

    private static string KeyOf(string connectionString)
    {
        using var dataSource = NpgsqlDataSource.Create(connectionString);
        return TimescaleSupport.RollupStoreKey(dataSource);
    }

    [Fact]
    public void RollupStoreKey_IsTheSameForAnyUserPasswordOrSessionOption_OnOneHostPortAndDatabase()
    {
        var first = KeyOf("Host=Store-A;Port=5432;Username=owner;Password=one;Database=darling");
        var second = KeyOf("Host=store-a;Port=5432;Username=mcp;Password=two;Database=darling;Options=-c TimeZone=UTC");

        Assert.Equal(first, second);
    }

    [Theory]
    [InlineData("Host=store-b;Port=5432;Username=owner;Database=darling")]
    [InlineData("Host=store-a;Port=5433;Username=owner;Database=darling")]
    [InlineData("Host=store-a;Port=5432;Username=owner;Database=darling_other")]
    public void RollupStoreKey_DiffersWhenTheHostPortOrDatabaseDiffers(string other)
    {
        Assert.NotEqual(KeyOf("Host=store-a;Port=5432;Username=owner;Database=darling"), KeyOf(other));
    }

    private static TimescaleSupport.RollupFloorCacheEntry Entry(DateTime measuredAt, long oid = 7)
        => new("chunk_1", "hyper_1", Floor, measuredAt, Ceiling: null, DatabaseOid: oid);

    private static TimescaleSupport.RollupFloorPlan Plan(
        TimescaleSupport.RollupFloorCacheEntry? entry, long oidNow = 7, string chunkNow = "chunk_1", string hypertableNow = "hyper_1")
    {
        var cached = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal);
        if (entry is { } e)
        {
            cached[View] = e;
        }

        var oldestNow = new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal)
        {
            [View] = new(chunkNow, hypertableNow, oidNow),
        };

        return TimescaleSupport.PlanRollupFloorMeasurements(cached, oldestNow, RollupAvailability.All, Now);
    }

    [Fact]
    public void Plan_AFloorPastTheHourWithItsIdentityUnchanged_IsDueInTheBackground_NotInline()
    {
        var plan = Plan(Entry(Now - TimescaleSupport.RollupFloorMaxReuse));

        Assert.Empty(plan.Inline);
        Assert.Contains(View, plan.Background);
    }

    [Fact]
    public void Plan_AFreshFloorWithItsIdentityUnchanged_IsNeitherInlineNorDue()
    {
        var plan = Plan(Entry(Now - TimeSpan.FromMinutes(5)));

        Assert.Empty(plan.Inline);
        Assert.Empty(plan.Background);
    }

    [Fact]
    public void Plan_NoEntryYet_IsInline()
    {
        var plan = Plan(entry: null);

        Assert.Contains(View, plan.Inline);
        Assert.Empty(plan.Background);
    }

    [Fact]
    public void Plan_ADifferentDatabaseOid_IsInline_EvenWhenTheEntryIsPastTheHour()
    {
        var plan = Plan(Entry(Now - TimescaleSupport.RollupFloorMaxReuse - TimeSpan.FromMinutes(5), oid: 7), oidNow: 8);

        Assert.Contains(View, plan.Inline);
        Assert.Empty(plan.Background);
    }

    /* The second threshold. Between the reuse window and twice the window a floor is served from the cache and
       re-measured once in the background (the tests above). At twice the window it is measured inline again, so no
       floor is served older than that, whether or not callers kept arriving. */
    [Fact]
    public void ServeCap_IsTwiceTheReuseWindow()
    {
        Assert.Equal(TimescaleSupport.RollupFloorMaxReuse * 2, TimescaleSupport.RollupFloorMaxServeAge);
        Assert.True(TimescaleSupport.RollupFloorMaxServeAge > TimescaleSupport.RollupFloorMaxReuse);
    }

    [Fact]
    public void Plan_AFloorJustUnderTheServeCap_WithItsIdentityUnchanged_IsStillServed_AndDueInTheBackground()
    {
        var plan = Plan(Entry(Now - TimescaleSupport.RollupFloorMaxServeAge + TimeSpan.FromMinutes(1)));

        Assert.Empty(plan.Inline);
        Assert.Contains(View, plan.Background);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(60 * 24 * 30)]
    public void Plan_AFloorAtOrPastTheServeCap_WithItsIdentityUnchanged_IsInline_NotServedFromTheCache(int minutesPast)
    {
        var plan = Plan(Entry(Now - TimescaleSupport.RollupFloorMaxServeAge - TimeSpan.FromMinutes(minutesPast)));

        Assert.Contains(View, plan.Inline);
        Assert.Empty(plan.Background);
    }

    [Theory]
    [InlineData(5)]
    [InlineData(61)]
    [InlineData(121)]
    [InlineData(60 * 24 * 30)]
    public void Plan_AChangedChunkHypertableOrDatabase_IsInline_AtAnyAge(int ageMinutes)
    {
        var measuredAt = Now - TimeSpan.FromMinutes(ageMinutes);

        foreach (var plan in new[]
        {
            Plan(Entry(measuredAt), chunkNow: "chunk_2"),
            Plan(Entry(measuredAt), hypertableNow: "hyper_2"),
            Plan(Entry(measuredAt), oidNow: 8),
        })
        {
            Assert.Contains(View, plan.Inline);
            Assert.Empty(plan.Background);
        }
    }

    [Fact]
    public void Plan_RollupFloorsToMeasure_IsTheUnionOfBoth()
    {
        var cached = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal)
        {
            [View] = Entry(Now - TimescaleSupport.RollupFloorMaxReuse),
        };
        var oldestNow = new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal)
        {
            [View] = new("chunk_1", "hyper_1", 7),
        };

        Assert.Contains(View, TimescaleSupport.RollupFloorsToMeasure(cached, oldestNow, RollupAvailability.All, Now));
    }

    [Fact]
    public void Apply_ReplacesWhatThisCycleMeasured_DropsAnEmptyView_AndLeavesTheRestAsTheCacheHoldsThemNow()
    {
        var other = TimescaleSupport.QueryStoreStatsHourlyView;
        var newerThanTheCycleSnapshot = Entry(Now);
        var cache = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal)
        {
            [View] = Entry(Now.AddHours(-2)),
            [other] = newerThanTheCycleSnapshot,
        };
        var measured = new Dictionary<string, DateTime?>(StringComparer.Ordinal) { [View] = null };
        var newEntries = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal)
        {
            [other] = Entry(Now.AddHours(-5)),
        };

        TimescaleSupport.ApplyRollupFloorMeasurements(cache, measured, newEntries, RollupAvailability.All);

        Assert.False(cache.ContainsKey(View));
        Assert.Equal(newerThanTheCycleSnapshot, cache[other]);
    }

    [Fact]
    public void Apply_DropsAViewWhoseRollupIsGone()
    {
        var cache = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal)
        {
            [View] = Entry(Now),
        };

        TimescaleSupport.ApplyRollupFloorMeasurements(
            cache,
            new Dictionary<string, DateTime?>(StringComparer.Ordinal),
            new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal),
            RollupAvailability.None);

        Assert.Empty(cache);
    }
}
