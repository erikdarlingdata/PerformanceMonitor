/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5642, ruling 6: the PostgreSQL alert families that fire from <c>DarlingWorker</c> carry the newest-sample age too,
/// computed once at fire time from the instant the family's own read already returned and persisted with the alert's
/// context. The families whose read returns an instant are High CPU (the counted sample), Deadlocks (the newest
/// report), Blocking (the newest capture) and Poison Wait (the newest collection). The Tier 0 trio and Long-Running
/// Query read no instant, so they carry none.
/// </summary>
public sealed class PgAlertSampleAgeTests
{
    private static readonly DateTime Now = new(2026, 10, 11, 15, 48, 49, DateTimeKind.Utc);

    [Fact]
    public void TheAgeIsComputedAtFireTime_FromTheInstant_AndAgreesWithTheEnginesStamp()
    {
        var instant = new DateTime(2026, 10, 11, 15, 43, 29, DateTimeKind.Unspecified);

        var pg = DarlingWorker.StampPgNewestSample(null, "CPU", instant, Now);
        var engine = AlertEngine.StampNewestSample(null, "CPU", instant, Now);

        Assert.NotNull(pg?.NewestSample);
        Assert.Equal(engine!.NewestSample, pg!.NewestSample);
        Assert.Equal(TimeSpan.FromSeconds(320), pg.NewestSample!.Age);
        Assert.Equal(DateTimeKind.Utc, pg.NewestSample.InstantUtc.Kind);
        Assert.Equal("Newest CPU sample: 15:43:29 UTC, 5m 20s old", AlertDetailText.SampleAgeLine(pg.NewestSample));
    }

    [Fact]
    public void ANullMinValueOrLocalInstant_GivesNoAge_AndNullStaysNull()
    {
        Assert.Null(DarlingWorker.StampPgNewestSample(null, "CPU", null, Now));
        Assert.Null(DarlingWorker.StampPgNewestSample(null, "CPU", DateTime.MinValue, Now));
        Assert.Null(DarlingWorker.StampPgNewestSample(null, "CPU", DateTime.SpecifyKind(Now.AddMinutes(-1), DateTimeKind.Local), Now));

        var kept = new AlertContext();
        Assert.Same(kept, DarlingWorker.StampPgNewestSample(kept, "CPU", null, Now));
        Assert.Null(kept.NewestSample);
    }

    [Fact]
    public void AFutureDatedSample_AgesAsZero()
    {
        var context = DarlingWorker.StampPgNewestSample(null, "deadlock", Now.AddMinutes(3), Now);
        Assert.Equal(TimeSpan.Zero, context!.NewestSample!.Age);
    }

    [Fact]
    public void TheNewestOfAFamilysInstants_IgnoresDefaults_AndIsNullWhenThereAreNone()
    {
        Assert.Null(DarlingWorker.NewestPgInstant([]));
        Assert.Null(DarlingWorker.NewestPgInstant([default, default]));
        Assert.Equal(Now, DarlingWorker.NewestPgInstant([Now.AddMinutes(-9), default, Now, Now.AddMinutes(-1)]));
    }

    /// <summary>
    /// High CPU's detail text is hand-built and the fire passes no Details, so the deliverer's one append is the only
    /// place the line enters: the text carries it once, and a text that already has it is left alone.
    /// </summary>
    [Fact]
    public void ACpuFiresHandBuiltText_GetsTheLineOnce()
    {
        var context = DarlingWorker.StampPgNewestSample(null, "CPU", Now.AddSeconds(-320), Now);
        const string handBuilt = "  Capacity: 91% of ceiling\n  Threshold: 80%\n  Sustained: 3 consecutive samples";

        var once = AlertDetailText.WithSampleAge(handBuilt, context);
        var twice = AlertDetailText.WithSampleAge(once, context);

        Assert.Equal(once, twice);
        Assert.Single(Regex.Matches(once!, "Newest CPU sample"));
        Assert.EndsWith("Newest CPU sample: 15:43:29 UTC, 5m 20s old", once, StringComparison.Ordinal);
    }

    /// <summary>
    /// Source shape, RED at the base commit (each of these four fires passed a context with no sample, or none at all):
    /// every family hands the stamp its own instant. High CPU's context was null.
    /// </summary>
    [Fact]
    public void EveryFamilyWhoseReadReturnsAnInstant_StampsItAtFireTime()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();
        Assert.Contains("Context: StampPgNewestSample(null, \"CPU\", reading.SampleTimeUtc, now)", source, StringComparison.Ordinal);
        Assert.Contains("\"deadlock\", NewestPgInstant(rows.Select(r => r.OccurredAtUtc)), now)", source, StringComparison.Ordinal);
        Assert.Contains("\"blocking capture\", NewestPgInstant(worstPerRoot.Select(r => r.CapturedAt)), now)", source, StringComparison.Ordinal);
        Assert.Contains("newestCollectionBySubject.TryGetValue(finding.Subject, out var newestWaitCollection)", source, StringComparison.Ordinal);
        Assert.Equal(4, Regex.Matches(source, @"StampPgNewestSample\(\s*(?:null|new AlertContext)").Count);
    }
}
