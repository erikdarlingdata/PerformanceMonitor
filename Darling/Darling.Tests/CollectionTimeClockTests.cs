/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3936: <see cref="CollectionTimeClock"/> is the write-side half of the fix — the reader-side half
/// (<c>collection_id DESC</c> tiebreaks) is pinned per-reader in <c>DarlingFleetReaderTests</c>,
/// <c>DarlingMcpPlanCacheSchedulerToolsTests</c>, <c>ViewerCpuSchedulerTests</c>,
/// <c>ViewerW2aTests</c> and <c>PgFactCollectorTests</c> (Lite's twins in <c>LocalDataServiceTests</c> /
/// <c>DuckDbFactCollectorTests</c>-adjacent coverage). Every (server, collector) key here is a distinct,
/// never-reused sentinel — the clock's backing dictionary is process-static and never cleared, so two tests
/// sharing a key would observe each other's stamps.
/// </summary>
public sealed class CollectionTimeClockTests
{
    [Fact]
    public void NextStrictlyAfter_ReturnsTheInputUnchanged_OnTheFirstCallForAKey()
    {
        var now = new DateTime(2026, 9, 23, 6, 0, 0, DateTimeKind.Utc);
        var result = CollectionTimeClock.NextStrictlyAfter(-393601, "cpu_scheduler_stats", now);
        Assert.Equal(now, result);
    }

    [Fact]
    public void NextStrictlyAfter_ReturnsTheInputUnchanged_WhenItIsAlreadyLaterThanTheLastStamp()
    {
        const int serverId = -393602;
        const string collector = "cpu_scheduler_stats";
        var first = new DateTime(2026, 9, 23, 6, 0, 0, DateTimeKind.Utc);
        var second = first.AddMinutes(1);

        CollectionTimeClock.NextStrictlyAfter(serverId, collector, first);
        var result = CollectionTimeClock.NextStrictlyAfter(serverId, collector, second);

        Assert.Equal(second, result);
    }

    /// <summary>
    /// The reported shape (#3936): a retried run, an orphaned config-reload body, or plain clock-resolution
    /// coincidence hands the SAME instant to two runs of the SAME (server, collector) pair. Without the nudge
    /// both would stamp their row with an IDENTICAL collection_time, making "the newest reading" ambiguous.
    /// </summary>
    [Fact]
    public void NextStrictlyAfter_NudgesForwardByOneStoredMicrosecond_WhenItWouldCollideWithTheLastStamp()
    {
        const int serverId = -393603;
        const string collector = "cpu_scheduler_stats";
        var collidingInstant = new DateTime(2026, 9, 23, 6, 0, 0, DateTimeKind.Utc);

        var firstRun = CollectionTimeClock.NextStrictlyAfter(serverId, collector, collidingInstant);
        var secondRun = CollectionTimeClock.NextStrictlyAfter(serverId, collector, collidingInstant);

        Assert.Equal(collidingInstant, firstRun);
        Assert.Equal(collidingInstant.AddMicroseconds(1), secondRun);
        Assert.True(secondRun > firstRun, "two runs stamped with the same instant must not store the same collection_time");
    }

    [Fact]
    public void NextStrictlyAfter_KeepsNudgingForward_AcrossRepeatedCollisions()
    {
        const int serverId = -393604;
        const string collector = "cpu_scheduler_stats";
        var collidingInstant = new DateTime(2026, 9, 23, 6, 0, 0, DateTimeKind.Utc);

        var run1 = CollectionTimeClock.NextStrictlyAfter(serverId, collector, collidingInstant);
        var run2 = CollectionTimeClock.NextStrictlyAfter(serverId, collector, collidingInstant);
        var run3 = CollectionTimeClock.NextStrictlyAfter(serverId, collector, collidingInstant);

        Assert.Equal(collidingInstant, run1);
        Assert.Equal(collidingInstant.AddMicroseconds(1), run2);
        Assert.Equal(collidingInstant.AddMicroseconds(2), run3);
    }

    /// <summary>A clock that appears to move backwards (NTP step, VM migration) must not un-advance the
    /// stamp either — the same "strictly after the last one" guarantee, from the other direction.</summary>
    [Fact]
    public void NextStrictlyAfter_NudgesForward_WhenTheClockAppearsToGoBackwards()
    {
        const int serverId = -393605;
        const string collector = "cpu_scheduler_stats";
        var later = new DateTime(2026, 9, 23, 6, 0, 0, DateTimeKind.Utc);
        var earlier = later.AddMinutes(-5);

        CollectionTimeClock.NextStrictlyAfter(serverId, collector, later);
        var result = CollectionTimeClock.NextStrictlyAfter(serverId, collector, earlier);

        Assert.Equal(later.AddMicroseconds(1), result);
    }

    [Fact]
    public void NextStrictlyAfter_TracksEachServerCollectorPairIndependently()
    {
        var now = new DateTime(2026, 9, 23, 6, 0, 0, DateTimeKind.Utc);

        /* Same instant, two different keys — neither the server id nor the collector name may leak into
           the other's history. */
        var serverA = CollectionTimeClock.NextStrictlyAfter(-393606, "cpu_scheduler_stats", now);
        var serverB = CollectionTimeClock.NextStrictlyAfter(-393607, "cpu_scheduler_stats", now);
        var collectorA = CollectionTimeClock.NextStrictlyAfter(-393608, "cpu_scheduler_stats", now);
        var collectorB = CollectionTimeClock.NextStrictlyAfter(-393608, "memory_stats", now);

        Assert.Equal(now, serverA);
        Assert.Equal(now, serverB);
        Assert.Equal(now, collectorA);
        Assert.Equal(now, collectorB);
    }

    /// <summary>
    /// #3936: PostgreSQL and DuckDB keep collection_time to the microsecond and truncate below it, so two runs
    /// whose instants differ only in ticks under one microsecond would still store ONE collection_time. The clock
    /// floors to the stored microsecond first, so the second run steps a whole microsecond past the first.
    /// </summary>
    [Fact]
    public void NextStrictlyAfter_SeparatesTwoInstantsThatShareAStoredMicrosecond()
    {
        const int serverId = -393611;
        const string collector = "cpu_scheduler_stats";
        var microsecond = new DateTime(2026, 9, 23, 6, 0, 0, DateTimeKind.Utc);
        var firstRun = CollectionTimeClock.NextStrictlyAfter(serverId, collector, microsecond.AddTicks(3));
        var secondRun = CollectionTimeClock.NextStrictlyAfter(serverId, collector, microsecond.AddTicks(7));

        Assert.Equal(microsecond, firstRun);
        Assert.Equal(microsecond.AddMicroseconds(1), secondRun);
        Assert.Equal(0, firstRun.Ticks % TimeSpan.TicksPerMicrosecond);
        Assert.Equal(0, secondRun.Ticks % TimeSpan.TicksPerMicrosecond);
    }

    [Theory]
    [InlineData(DateTimeKind.Utc)]
    [InlineData(DateTimeKind.Unspecified)]
    public void NextStrictlyAfter_PreservesTheSuppliedKind(DateTimeKind kind)
    {
        var serverId = kind == DateTimeKind.Utc ? -393609 : -393610;
        var now = new DateTime(2026, 9, 23, 6, 0, 0, kind);

        var result = CollectionTimeClock.NextStrictlyAfter(serverId, "cpu_scheduler_stats", now);

        Assert.Equal(kind, result.Kind);
    }
}
