// Copyright (c) 2026 Darling Data, LLC. Licensed under the MIT License. See LICENSE in the repository root.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.ComposeStampLiveSupport;
using static Darling.Tests.QueryStoreComposeStampLiveTests;

namespace Darling.Tests;

/// <summary>
/// #7762: the compose rollup builds an hour at <see cref="QueryStoreComposeStamp.BuildLag"/> (90 minutes) and its row trigger marks a row of an
/// earlier hour at once, one hour sooner than V173's did. Four behaviours on a migrated scratch store: a row written after its hour ended is marked
/// and the next build reads it (a row written in its own hour is not marked); the plan reads an hour at its start plus the lag and not a minute
/// before (the constant and the plan's own text agree); the build tick takes the newest due hour and, with nothing due, writes nothing.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class QueryStoreComposeStampLagLiveTests
{
    [Fact]
    public async Task ARowWrittenAfterItsHourEnded_IsMarkedAndTheNextBuildReadsIt_ARowInItsOwnHourIsNot()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        /* The seed ends one hour later than the default, so it holds rows of the store's current hour (the writer's own hour). */
        var thisHour = ThisHour();
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, at: thisHour.AddHours(1));
        var bodySucceeded = false;
        try
        {
            Assert.Equal(string.Empty, await StaleListAsync(connection, ct));
            var previous = thisHour.AddHours(-1);

            /* Written now, in the hour after its own: marked. */
            await InsertRowAsync(connection, 1, previous.AddMinutes(10), 9_300_001, ct);
            Assert.Equal($"1@{previous:yyyy-MM-dd HH}", await StaleListAsync(connection, ct));

            /* Written in its own hour: not marked (the tail reads it until its hour is built). */
            await InsertRowAsync(connection, 2, thisHour.AddMinutes(5), 9_300_002, ct);
            Assert.Equal($"1@{previous:yyyy-MM-dd HH}", await StaleListAsync(connection, ct));

            Assert.True(await MismatchesAsync(connection, previous, previous.AddHours(1), ct) > 0, "the rollup does not hold the late row yet");
            var tick = await TickAtAsync(source, ClockWithin(hourNow), ct);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(1, tick.BuiltStale);
            Assert.Equal(string.Empty, await StaleListAsync(connection, ct));
            Assert.Equal(0, await MismatchesAsync(connection, previous, previous.AddHours(1), ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheBuild_DoesNotReadAnHourBeforeItsStartPlusTheLag_AndReadsItAtThatInstant()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            /* The plan's cut is its own text (PlanSql), the test's is the constant: one minute either side of the constant decides it. */
            var hour = hourNow.AddHours(-5);
            var atLag = hour + QueryStoreComposeStamp.BuildLag;
            var early = await QueryStoreComposeStamp.PlanAsync(connection, atLag.AddMinutes(-1), RetentionDays, ct, atLag.AddMinutes(-1));
            Assert.NotEmpty(early.Builds);
            Assert.Equal(hour.AddHours(-1), early.Builds.First().Hour);
            var onTime = await QueryStoreComposeStamp.PlanAsync(connection, atLag, RetentionDays, ct, atLag);
            Assert.Equal(hour, onTime.Builds.First().Hour);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheBuildTick_BuildsTheNewestDueHour_AndWithNothingDueWritesNoRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            var clock = ClockWithin(hourNow);
            var first = await TickAtAsync(source, clock, ct);
            Assert.Equal(0, first.Failed);
            Assert.True(first.Built > 0, "an eligible hour is built");
            Assert.Equal(1, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_hours WHERE hour = TIMESTAMP '{At(NewestDueHour(clock))}'", ct));

            /* Everything due is built: the next tick builds nothing and writes no rollup, validity or hours row. */
            Assert.Equal(BuiltHours - first.Built, await BuildAllAsync(source, ct, clock));
            string[] tables = ["query_store_compose_stamp", "query_store_compose_stamp_hours", "query_store_compose_stamp_built"];
            var before = new long[tables.Length];
            for (var t = 0; t < tables.Length; t++)
            {
                before[t] = await CountAsync(connection, "SELECT count(*) FROM collect." + tables[t], ct);
            }

            var idle = await TickAtAsync(source, clock, ct);
            Assert.Equal(0, idle.Built);
            Assert.Equal(0, idle.Failed);
            for (var t = 0; t < tables.Length; t++)
            {
                Assert.Equal(before[t], await CountAsync(connection, "SELECT count(*) FROM collect." + tables[t], ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }
}
