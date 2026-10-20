/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6155: the post-restart settle window explains a server that went quiet around the restart. It does not explain one that
/// had already been offline for days before it. The second is classified offline, in its reason and in the staleness watch
/// item, by the rule <c>list_servers</c> uses, evaluated at the instant the service started.
/// </summary>
public sealed class FleetSweepOfflineBeforeRestartTests
{
    private static readonly DateTime Now = new(2026, 10, 19, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Started = Now.AddMinutes(-3);
    private static readonly DateTime Registered = Now.AddDays(-60);

    private static FleetSweepComposition Compose(params FleetSweepServerReading[] readings) =>
        FleetSweepEngine.Compose(
            Now,
            Now.AddHours(-1),
            alertsEnabled: true,
            serversExpected: readings.Length,
            readings,
            previousRun: null,
            Array.Empty<FleetSweepServerVerdict>(),
            Array.Empty<FleetSweepWatchItem>(),
            new FleetSweepInstrumentCounters(Started, 2, 0),
            DeadlockRateThresholds.Default);

    private static FleetSweepServerReading Quiet(int id, string name, DateTime? lastCollection) =>
        new(id, name, default, 0, null, lastCollection, Registered);

    /// <summary>A target last collected nine days before the restart is offline: its reason says so and names the restart, and its
    /// staleness watch item opens even inside the settle window. A target last collected a minute before the restart stays
    /// startup-transient, with no watch item.</summary>
    [Fact]
    public void ATargetOfflineBeforeTheRestart_IsClassifiedOffline_AndOneCollectedAMinuteBeforeIsStillStartupTransient()
    {
        var composition = Compose(
            Quiet(1, "offline-nine-days", Started.AddDays(-9)),
            Quiet(2, "restarted-over", Started.AddMinutes(-1)));

        var offline = composition.Verdicts.Single(v => v.ServerId == 1);
        Assert.Equal("No Data", offline.Band);
        Assert.Contains("already offline before the restart", offline.BandReason, StringComparison.Ordinal);
        Assert.Contains("classified offline, not startup-transient", offline.BandReason, StringComparison.Ordinal);
        Assert.Contains(Started.AddDays(-9).ToString("yyyy-MM-dd HH:mm", System.Globalization.CultureInfo.InvariantCulture), offline.BandReason, StringComparison.Ordinal);

        var transient = composition.Verdicts.Single(v => v.ServerId == 2);
        Assert.Contains("startup-transient", transient.BandReason, StringComparison.Ordinal);
        Assert.DoesNotContain("already offline", transient.BandReason, StringComparison.Ordinal);

        Assert.Contains(composition.WatchItems, w => w.ServerId == 1 && w.ItemKey == FleetSweepEngine.CollectionStaleItemKey);
        Assert.DoesNotContain(composition.WatchItems, w => w.ServerId == 2 && w.ItemKey == FleetSweepEngine.CollectionStaleItemKey);
    }

    /// <summary>A registered target whose collections have all aged out of retention was offline before the restart too; a target
    /// the store has never collected from is not known to be offline, so the settle window still explains it.</summary>
    [Fact]
    public void ATargetWithNoRetainedCollection_IsOffline_OnlyWhenItWasRegisteredLongBeforeTheRestart()
    {
        var composition = Compose(
            new FleetSweepServerReading(1, "aged-out", default, 0, null, null, Now.AddDays(-400)),
            new FleetSweepServerReading(2, "brand-new", default, 0, null, null, Started.AddMinutes(-1)));

        Assert.Contains("already offline before the restart", composition.Verdicts.Single(v => v.ServerId == 1).BandReason, StringComparison.Ordinal);
        Assert.Contains("startup-transient", composition.Verdicts.Single(v => v.ServerId == 2).BandReason, StringComparison.Ordinal);
    }

    /// <summary>A server that has data in the span is never offline before the restart, whatever its last collection says.</summary>
    [Fact]
    public void ATargetWithDataInTheSpan_IsNotOfflineBeforeTheRestart()
    {
        var reading = new FleetSweepServerReading(
            1, "has-data", new DailyHealthSignals { HasData = true, Window = TimeSpan.FromHours(1) }, 0, null, Started.AddDays(-9), Registered);

        Assert.False(reading.WasOfflineBeforeRestart(Started));
    }
}
