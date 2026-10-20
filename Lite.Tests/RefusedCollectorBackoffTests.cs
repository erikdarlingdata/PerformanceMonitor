/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6186: a collector the monitored server refuses with the same permission denial on every run used to be skipped for the rest
/// of the app session after its first refusal (so a permission granted later was never picked up without a restart). It now
/// backs off while the same refusal repeats, is still tried again after the wait, and goes back to the normal interval on its
/// first success or a changed answer. RecordCollectorResult and IsCollectorBackedOff only touch in-memory state, so the
/// service is constructed with null dependencies (the interval falls back to one minute).
/// </summary>
public class RefusedCollectorBackoffTests
{
    private const int ServerId = 61;
    private const string Collector = "tempdb_stats";
    private const string Denied = "The server refused the read: the user has no access to tempdb.";

    private static RemoteCollectorService CreateService() =>
        new(duckDb: null!, serverManager: null!, scheduleManager: null!);

    [Fact]
    public void ACollectorRefusedRunAfterRun_IsBackedOff_AndTheFirstSuccessRestoresTheInterval()
    {
        var service = CreateService();

        for (var run = 1; run <= RefusedCollectorBackoff.FreeRefusals; run++)
        {
            service.RecordCollectorResult(ServerId, Collector, "PERMISSIONS", Denied);
            Assert.False(service.IsCollectorBackedOff(ServerId, Collector), $"refusal {run} still runs at the normal interval");
        }

        service.RecordCollectorResult(ServerId, Collector, "PERMISSIONS", Denied);
        Assert.True(service.IsCollectorBackedOff(ServerId, Collector));

        /* Not stopped for good: once the wait has passed it is tried again, and the grant arriving ends the backoff. */
        service.GetRefusedBackoff(ServerId).SetNotBeforeForTests(Collector, DateTime.UtcNow.AddSeconds(-1));
        Assert.False(service.IsCollectorBackedOff(ServerId, Collector));

        service.RecordCollectorResult(ServerId, Collector, "SUCCESS");
        Assert.False(service.IsCollectorBackedOff(ServerId, Collector));
        Assert.Equal(0, service.GetRefusedBackoff(ServerId).Streak(Collector));
    }

    [Fact]
    public void ACollectorRefusedManyTimes_IsTriedFarFewerTimesThanItHasIntervals()
    {
        var service = CreateService();
        var backoff = service.GetRefusedBackoff(ServerId);
        const int intervals = 60;
        var tries = 0;
        var clock = DateTime.UtcNow;

        /* One minute per interval for an hour: a try happens only when the collector is not backed off, as the sweep decides. */
        for (var minute = 0; minute < intervals; minute++)
        {
            var now = clock.AddMinutes(minute);
            if (backoff.IsBackedOff(Collector, now, TimeSpan.FromMinutes(1)))
            {
                continue;
            }

            tries++;
            backoff.Record(Collector, refused: true, Denied, now, TimeSpan.FromMinutes(1));
        }

        Assert.True(tries < intervals / 4, $"{tries} tries in {intervals} one-minute intervals");
    }

    [Fact]
    public void ADifferentAnswerRestartsTheStreak()
    {
        var service = CreateService();
        for (var run = 0; run < 6; run++)
        {
            service.RecordCollectorResult(ServerId, Collector, "PERMISSIONS", Denied);
        }

        Assert.True(service.IsCollectorBackedOff(ServerId, Collector));

        service.RecordCollectorResult(ServerId, Collector, "PERMISSIONS", "A different refusal.");
        Assert.False(service.IsCollectorBackedOff(ServerId, Collector));
        Assert.Equal(1, service.GetRefusedBackoff(ServerId).Streak(Collector));

        service.RecordCollectorResult(ServerId, Collector, "PERMISSIONS", "A different refusal.");
        service.RecordCollectorResult(ServerId, Collector, "PERMISSIONS", "A different refusal.");
        service.RecordCollectorResult(ServerId, Collector, "ERROR", "boom");
        Assert.Equal(0, service.GetRefusedBackoff(ServerId).Streak(Collector));
    }

    [Fact]
    public void ARunThatSaysNothingAboutTheGrant_LeavesTheBackoffAlone()
    {
        var service = CreateService();
        for (var run = 0; run < 6; run++)
        {
            service.RecordCollectorResult(ServerId, Collector, "PERMISSIONS", Denied);
        }

        service.RecordCollectorResult(ServerId, Collector, "YIELDED", "lock timeout");
        Assert.True(service.IsCollectorBackedOff(ServerId, Collector));
    }

    [Fact]
    public void ARemovedServersBackoffIsForgotten()
    {
        var service = CreateService();
        for (var run = 0; run < 6; run++)
        {
            service.RecordCollectorResult(ServerId, Collector, "PERMISSIONS", Denied);
        }

        service.ClearHealthForServer(ServerId);
        Assert.False(service.IsCollectorBackedOff(ServerId, Collector));
    }
}
