/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The panel notes say how often a collector runs from its effective cadence, not from the shipped default: a schedule override
/// changes the words. Pure: no window, no store.
/// </summary>
public sealed class CollectorCadenceTextTests
{
    [Theory]
    [InlineData(60, "hourly", "hour")]
    [InlineData(1, "every minute", "minute")]
    [InlineData(5, "every 5 minutes", "5 minutes")]
    [InlineData(30, "every 30 minutes", "30 minutes")]
    [InlineData(120, "every 2 hours", "2 hours")]
    [InlineData(1440, "daily", "day")]
    [InlineData(4320, "every 3 days", "3 days")]
    [InlineData(90, "every 90 minutes", "90 minutes")]
    public void TheNoteNamesTheEffectiveCadence(int minutes, string every, string span)
    {
        Assert.Equal(every, CollectorCadenceText.Every(minutes));
        Assert.Equal(span, CollectorCadenceText.Span(minutes));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ANonRecurringCadence_IsNotStatedAsANumber(int minutes)
    {
        Assert.Equal("on its schedule", CollectorCadenceText.Every(minutes));
        Assert.Equal("collection interval", CollectorCadenceText.Span(minutes));
    }

    [Fact]
    public void AFleetOverrideOfThirtyMinutes_ChangesTheConfigChangesNoteFromHourly()
    {
        /* The same resolution the Viewer's panel notes run: pg_server_config ships at 60, an override wins. */
        var shipped = CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes("pg_server_config", 1, Array.Empty<ScheduleOverride>());
        var overridden = CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(
            "pg_server_config", 1, new[] { new ScheduleOverride(null, "pg_server_config", 30, null, null) });

        Assert.Equal("hourly", CollectorCadenceText.Every(shipped!.Value));
        Assert.Equal("every 30 minutes", CollectorCadenceText.Every(overridden!.Value));
    }

    private static CollectorScheduleRow Fleet(string collector, int minutes) => new(null, collector, minutes, null, true, null);

    [Fact]
    public async Task ThreeNotesAndThePicker_ShareOneScheduleRead_UntilItIsFiveMinutesOld()
    {
        var reads = 0;
        var now = new DateTime(2026, 10, 25, 12, 0, 0, DateTimeKind.Utc);
        var cache = new ViewerScheduleOverridesCache(
            () => { reads++; return Task.FromResult<IReadOnlyList<CollectorScheduleRow>>(new[] { Fleet("pg_server_config", 30) }); },
            () => now,
            _ => { });

        /* One Overview refresh: the config-changes note, the logging-audit note and the CPU note ask together. */
        var asks = await Task.WhenAll(cache.GetAsync(), cache.GetAsync(), cache.GetAsync());
        await cache.GetAsync();
        Assert.Equal(1, reads);
        Assert.All(asks, rows => Assert.Equal(30, ViewerTimeRangeWindow.SampleIntervalFor("pg_server_config", 1, rows)!.Value.TotalMinutes));

        now += ViewerScheduleOverridesCache.MaxAge + TimeSpan.FromSeconds(1);
        await cache.GetAsync();
        Assert.Equal(2, reads);
    }

    [Fact]
    public async Task AFailedScheduleRead_GivesNull_IsReported_AndIsTriedAgainOnTheNextAsk()
    {
        var reads = 0;
        var failures = 0;
        var cache = new ViewerScheduleOverridesCache(
            () => ++reads == 1
                ? Task.FromException<IReadOnlyList<CollectorScheduleRow>>(new InvalidOperationException("store down"))
                : Task.FromResult<IReadOnlyList<CollectorScheduleRow>>(Array.Empty<CollectorScheduleRow>()),
            () => DateTime.UtcNow,
            _ => failures++);

        Assert.Null(await cache.GetAsync());
        Assert.Equal(1, failures);
        /* Null means the shipped cadence: the picker and the notes still answer. */
        Assert.Equal(60, ViewerTimeRangeWindow.SampleIntervalFor("pg_server_config", 1, null)!.Value.TotalMinutes);

        Assert.NotNull(await cache.GetAsync());
        Assert.Equal(2, reads);
    }

    [Fact]
    public async Task SeveralCallersWaitingOnOneFailedRead_AllGetNull_AndTheFailureIsReportedOnce()
    {
        /* The Viewer asks from the UI thread, one continuation at a time; an exclusive scheduler gives the test the same order. */
        var oneAtATime = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        await Task.Factory.StartNew(async () =>
        {
            var reads = 0;
            var reported = new List<Exception>();
            var read = new TaskCompletionSource<IReadOnlyList<CollectorScheduleRow>>();
            var cache = new ViewerScheduleOverridesCache(
                () => ++reads == 1 ? read.Task : Task.FromResult<IReadOnlyList<CollectorScheduleRow>>(Array.Empty<CollectorScheduleRow>()),
                () => DateTime.UtcNow,
                reported.Add);

            /* One Overview refresh: three panel notes and the picker ask while the one read is still under way. */
            var asks = new[] { cache.GetAsync(), cache.GetAsync(), cache.GetAsync(), cache.GetAsync() };
            read.SetException(new InvalidOperationException("store down"));
            var results = await Task.WhenAll(asks);

            Assert.Equal(1, reads);
            Assert.All(results, Assert.Null);
            Assert.Single(reported);
            /* The failure was not kept: the next ask reads again. */
            Assert.NotNull(await cache.GetAsync());
            Assert.Equal(2, reads);
        }, CancellationToken.None, TaskCreationOptions.None, oneAtATime).Unwrap();
    }

    [Fact]
    public void TheMcpToolsCadence_FollowsTheServersOwnOverride_ThenTheFleetOne_ThenTheShippedDefault()
    {
        var overrides = new[]
        {
            new ScheduleOverride(null, "pg_server_config", 30, null, null),
            new ScheduleOverride(7, "pg_server_config", 120, null, null),
        };

        Assert.Equal(60, DarlingCollectorCadence.Minutes("pg_server_config", 1, Array.Empty<ScheduleOverride>(), 60));
        Assert.Equal(30, DarlingCollectorCadence.Minutes("pg_server_config", 1, overrides, 60));
        Assert.Equal(120, DarlingCollectorCadence.Minutes("pg_server_config", 7, overrides, 60));
        /* The tool's sentence is built from it: the words follow the schedule, not the shipped hour. */
        Assert.Equal("every 2 hours", CollectorCadenceText.Every(DarlingCollectorCadence.Minutes("pg_server_config", 7, overrides, 60)));
        /* A name the shipped schedule does not know keeps the caller's fallback. */
        Assert.Equal(60, DarlingCollectorCadence.Minutes("not_a_collector", 1, overrides, 60));
    }
}
