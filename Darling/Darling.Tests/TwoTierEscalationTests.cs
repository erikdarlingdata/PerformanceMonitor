/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5715 (part A): High CPU, Deadlocks Detected, Poison Wait and Volume Free Space opt into the Warning-to-Critical
/// escalation #5709 built for Runnable Queue (<see cref="TierEscalation"/>). Each alert is driven through the real
/// <see cref="AlertEngine"/> with the same sequences: a Warning then a Critical inside the cooldown (delivered at
/// once, flag set), Critical back to Warning (nothing), a repeated Critical (follows the cooldown), a cleared incident
/// (escalates again) and a Critical whose every channel failed (retried as an escalation). The helper's own rules are
/// pinned in <c>RunnableQueueEscalationTests</c>.
/// </summary>
public sealed class TwoTierEscalationTests
{
    private static readonly DateTime T0 = new(2026, 10, 13, 10, 0, 0, DateTimeKind.Utc);

    private enum Level { Clear, Warning, Critical }

    public static readonly object[][] Alerts =
    {
        new object[] { "High CPU" },
        new object[] { "Deadlocks Detected" },
        new object[] { "Poison Wait" },
        new object[] { "Volume Free Space" },
    };

    private static AlertEngineTests.Harness Armed(string metric)
    {
        var h = new AlertEngineTests.Harness { Now = T0 };
        switch (metric)
        {
            case "High CPU":
                h.Settings.CpuEnabled = true;
                break;
            case "Deadlocks Detected":
                h.Settings.DeadlockEnabled = true;
                h.Settings.DeadlockRateThresholds = new DeadlockRateThresholds(7.0, 31.0);
                break;
            case "Poison Wait":
                h.Settings.PoisonWaitEnabled = true;
                break;
            case "Volume Free Space":
                h.Settings.LowDiskEnabled = true;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(metric), metric, null);
        }

        return h;
    }

    /// <summary>One evaluation of <paramref name="metric"/> at <paramref name="at"/> showing <paramref name="level"/>;
    /// returns how many of THAT alert's outcomes it delivered. <paramref name="step"/> only has to grow from call to
    /// call: it makes a Critical deadlock count, a poison collection time or a free-space level newer than the last.</summary>
    private static async Task<int> StepAsync(string metric, AlertEngineTests.Harness h, AlertEngine engine, DateTime at, Level level, int step, int? cpuSamples = null)
    {
        h.Now = at;
        var before = h.Deliverer.Outcomes.Count(o => o.MetricName == metric);
        switch (metric)
        {
            case "High CPU":
            {
                /* #7085: the SQL Server gate opens on SqlCpuBreachSamples consecutive samples at or over the bar and clears only
                   after SqlCpuClearHold of sample time under it, so a Warning or Critical call feeds the opening count and a Clear
                   call feeds enough samples to span the hold (the hold in minutes plus the sample that completes it). Every
                   Snapshot carries a sample time one minute after the last. Extra fires in a call are held back by the incident
                   rule or the cooldown. A test that counts retries passes 1. */
                double? value = level switch { Level.Warning => 90, Level.Critical => 97, _ => 10 };
                var samples = cpuSamples ?? (level == Level.Clear ? (int)AlertEngine.SqlCpuClearHold.TotalMinutes + 1 : AlertEngine.SqlCpuBreachSamples);
                for (var i = 0; i < samples; i++)
                {
                    await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot(sqlCpu: value, totalCpu: value));
                }

                break;
            }
            case "Deadlocks Detected":
            {
                h.Adapter.Deadlocks.Clear();
                var rows = level switch { Level.Warning => 25, Level.Critical => 35 + step, _ => 0 };
                for (var i = 0; i < rows; i++)
                {
                    h.Adapter.Deadlocks.Add(AlertEngineTests.DeadlockRow());
                }

                await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
                break;
            }
            case "Poison Wait":
            {
                h.Adapter.PoisonWaits.Clear();
                var collected = T0.AddMinutes(step);
                h.Adapter.PoisonWaits.Add(level switch
                {
                    Level.Warning => new PoisonWaitAccumulation("THREADPOOL", 600_000, 300_000, 10, collected),
                    Level.Critical => new PoisonWaitAccumulation("THREADPOOL", 6_000_000, 3_000_000, 10, collected),
                    _ => new PoisonWaitAccumulation("THREADPOOL", 1_000, 1, 10, collected),
                });
                await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
                break;
            }
            case "Volume Free Space":
            {
                h.Adapter.Volumes.Clear();
                if (level != Level.Clear)
                {
                    /* 100 GB volume: 8 GB (8%) is a breach but not critically low; the Critical levels fall by more than the
                       1-point worsening step each time, so a later Critical is a worsening in its own right. */
                    var freeMb = level == Level.Warning ? 8192 : Math.Max(100, 2900 - (step * 1200));
                    h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = 102400, FreeMb = freeMb });
                }

                await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
                break;
            }
        }

        return h.Deliverer.Outcomes.Count(o => o.MetricName == metric) - before;
    }

    private static AlertOutcome Last(string metric, AlertEngineTests.Harness h) => h.Deliverer.Outcomes.Last(o => o.MetricName == metric);

    private static bool Flag(string metric, AlertEngineTests.Harness h) => Last(metric, h).Context!.EscalationBypassesCooldown;

    private static AlertDelivery FailedByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: "Teams: 429 Too Many Requests", AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    [Fact]
    public void TheSixOptedInMetrics_AreExactlyThese()
    {
        Assert.Equal(
            new[] { "Client Timeouts", "Deadlocks Detected", "High CPU", "Poison Wait", "Runnable Queue", "Volume Free Space" },
            TierEscalation.OptedInMetrics.OrderBy(m => m, StringComparer.Ordinal));

        foreach (var metric in new[] { "Blocking Detected", "Blocking Wait Time", "tempdb Space", "Persistent Version Store" })
        {
            Assert.False(TierEscalation.IsOptedIn(metric), metric);
        }
    }

    [Theory]
    [MemberData(nameof(Alerts))]
    public async Task AWarningThatWorsensToCritical_IsDeliveredAtOnce_WithTheFlag_AndRestartsTheClock(string metric)
    {
        var h = Armed(metric);
        var engine = h.Build();

        Assert.Equal(1, await StepAsync(metric, h, engine, T0, Level.Warning, 0));
        Assert.False(Flag(metric, h));

        Assert.Equal(1, await StepAsync(metric, h, engine, T0.AddMinutes(1), Level.Critical, 1));
        Assert.Equal(AlertSeverityLevel.Critical, Last(metric, h).Severity);
        Assert.Equal(AlertSeverityLevel.Critical, Last(metric, h).Context!.SeverityOverride);
        Assert.True(Flag(metric, h));

        /* The Critical restarted the clock. */
        Assert.Equal(0, await StepAsync(metric, h, engine, T0.AddMinutes(2), Level.Critical, 2));
    }

    [Theory]
    [MemberData(nameof(Alerts))]
    public async Task CriticalGoingBackToWarning_DeliversNothing(string metric)
    {
        var h = Armed(metric);
        var engine = h.Build();

        Assert.Equal(1, await StepAsync(metric, h, engine, T0, Level.Warning, 0));
        Assert.Equal(1, await StepAsync(metric, h, engine, T0.AddMinutes(1), Level.Critical, 1));
        Assert.Equal(0, await StepAsync(metric, h, engine, T0.AddMinutes(2), Level.Warning, 2));

        /* And a Critical again inside the cooldown is not a second escalation. */
        Assert.Equal(0, await StepAsync(metric, h, engine, T0.AddMinutes(3), Level.Critical, 3));
    }

    [Theory]
    [MemberData(nameof(Alerts))]
    public async Task ARepeatedCritical_FollowsTheNormalCooldown_AndCarriesNoFlag(string metric)
    {
        var h = Armed(metric);
        var engine = h.Build();

        Assert.Equal(1, await StepAsync(metric, h, engine, T0, Level.Warning, 0));
        Assert.Equal(1, await StepAsync(metric, h, engine, T0.AddMinutes(1), Level.Critical, 1));

        /* The cooldown counts from the Critical (T0 + 1 minute), not from the Warning. */
        var due = T0.AddMinutes(1).Add(TimeSpan.FromMinutes(h.Settings.CooldownMinutes));
        Assert.Equal(0, await StepAsync(metric, h, engine, due.AddSeconds(-30), Level.Critical, 2));
        if (metric == "High CPU")
        {
            /* #7085: High CPU holds an open incident to escalations only, so the repeat after the cooldown pages nothing and
               the Critical that escalated stays the last page. The cooldown no longer decides a High CPU repeat. */
            Assert.Equal(0, await StepAsync(metric, h, engine, due, Level.Critical, 3));
            Assert.Equal(0, await StepAsync(metric, h, engine, due.AddMinutes(h.Settings.CooldownMinutes), Level.Critical, 4));
            Assert.True(Flag(metric, h));
            return;
        }

        /* #7086: Deadlocks Detected pages once per episode, so a Critical that only repeats inside the same episode, cooldown
           or not, is not a second page. The others repeat on the cooldown. */
        if (metric == "Deadlocks Detected")
        {
            Assert.Equal(0, await StepAsync(metric, h, engine, due, Level.Critical, 3));
            return;
        }

        Assert.Equal(1, await StepAsync(metric, h, engine, due, Level.Critical, 3));
        Assert.False(Flag(metric, h));
    }

    [Theory]
    [MemberData(nameof(Alerts))]
    public async Task AClearedIncident_StartsOver_SoTheNextWarningThenCriticalEscalatesAgain(string metric)
    {
        var h = Armed(metric);
        var engine = h.Build();

        Assert.Equal(1, await StepAsync(metric, h, engine, T0, Level.Warning, 0));
        Assert.Equal(1, await StepAsync(metric, h, engine, T0.AddMinutes(1), Level.Critical, 1));
        await StepAsync(metric, h, engine, T0.AddMinutes(2), Level.Clear, 2);

        var later = T0.AddMinutes(1).Add(TimeSpan.FromMinutes(h.Settings.CooldownMinutes)).AddMinutes(1);
        Assert.Equal(1, await StepAsync(metric, h, engine, later, Level.Warning, 3));
        Assert.False(Flag(metric, h));
        Assert.Equal(1, await StepAsync(metric, h, engine, later.AddMinutes(1), Level.Critical, 4));
        Assert.Equal(AlertSeverityLevel.Critical, Last(metric, h).Severity);
        Assert.True(Flag(metric, h));
    }

    [Theory]
    [MemberData(nameof(Alerts))]
    public async Task AnEscalationWhoseEveryChannelFailed_IsRetriedAsAnEscalation_AtTheNextEvaluation(string metric)
    {
        var h = Armed(metric);
        var engine = h.Build();

        Assert.Equal(1, await StepAsync(metric, h, engine, T0, Level.Warning, 0));

        h.Deliverer.Report = _ => FailedByWebhook();
        Assert.Equal(1, await StepAsync(metric, h, engine, T0.AddMinutes(1), Level.Critical, 1, cpuSamples: 1));
        Assert.True(Flag(metric, h));

        /* Nothing was delivered at Critical, so the next evaluation is still the escalation and carries the flag. */
        h.Deliverer.Report = null;
        Assert.Equal(1, await StepAsync(metric, h, engine, T0.AddMinutes(2), Level.Critical, 2, cpuSamples: 1));
        Assert.Equal(AlertSeverityLevel.Critical, Last(metric, h).Severity);
        Assert.True(Flag(metric, h));

        /* The success recorded Critical: the next one waits for the cooldown. */
        Assert.Equal(0, await StepAsync(metric, h, engine, T0.AddMinutes(3), Level.Critical, 3, cpuSamples: 1));
    }

    [Theory]
    [MemberData(nameof(Alerts))]
    public async Task AnEscalationWhoseEveryChannelFailed_BacksOffLikeAnyFailedSend_AndADeliveredSendEndsTheStreak(string metric)
    {
        /* #5715 part A: Restore keeps the next evaluation an escalation, and an escalation used to skip the cooldown stamp the
           failure back-dated (#4752), so a Critical went out at every evaluation while every channel was down. While the
           family and key have an open failure streak, the escalation now waits for the back-dated stamp. */
        var h = Armed(metric);
        var engine = h.Build();

        Assert.Equal(1, await StepAsync(metric, h, engine, T0, Level.Warning, 0));
        Assert.False(Flag(metric, h));

        /* The Critical inside the cooldown: every channel fails (failure 1, retry in 1 minute). */
        h.Deliverer.Report = _ => FailedByWebhook();
        Assert.Equal(1, await StepAsync(metric, h, engine, T0.AddMinutes(1), Level.Critical, 1, cpuSamples: 1));
        Assert.True(Flag(metric, h));

        /* Before the retry delay: nothing, though the incident is still an escalation. */
        Assert.Equal(0, await StepAsync(metric, h, engine, T0.AddMinutes(1).AddSeconds(30), Level.Critical, 2, cpuSamples: 1));
        Assert.Equal(0, await StepAsync(metric, h, engine, T0.AddMinutes(1).AddSeconds(59), Level.Critical, 3, cpuSamples: 1));

        /* After the delay: the Critical goes out again with the flag, and fails again (failure 2, retry in 2 minutes). */
        Assert.Equal(1, await StepAsync(metric, h, engine, T0.AddMinutes(2), Level.Critical, 4, cpuSamples: 1));
        Assert.True(Flag(metric, h));
        Assert.Equal(0, await StepAsync(metric, h, engine, T0.AddMinutes(3), Level.Critical, 5, cpuSamples: 1));
        Assert.Equal(0, await StepAsync(metric, h, engine, T0.AddMinutes(3).AddSeconds(59), Level.Critical, 6, cpuSamples: 1));

        /* The second failure waited 2 minutes. This send is delivered, which ends the streak. */
        h.Deliverer.Report = null;
        Assert.Equal(1, await StepAsync(metric, h, engine, T0.AddMinutes(4), Level.Critical, 7, cpuSamples: 1));
        Assert.True(Flag(metric, h));
        Assert.Equal(0, await StepAsync(metric, h, engine, T0.AddMinutes(4).AddSeconds(30), Level.Critical, 8, cpuSamples: 1));

        /* A later incident's escalation goes at once again, inside the cooldown, with no wait. */
        await StepAsync(metric, h, engine, T0.AddMinutes(5), Level.Clear, 9);
        var later = T0.AddMinutes(4).Add(TimeSpan.FromMinutes(h.Settings.CooldownMinutes)).AddMinutes(1);
        Assert.Equal(1, await StepAsync(metric, h, engine, later, Level.Warning, 10));
        Assert.Equal(1, await StepAsync(metric, h, engine, later.AddMinutes(1), Level.Critical, 11));
        Assert.Equal(AlertSeverityLevel.Critical, Last(metric, h).Severity);
        Assert.True(Flag(metric, h));
    }

    [Theory]
    [MemberData(nameof(Alerts))]
    public async Task ACriticalThatOpensTheIncident_IsAnOrdinaryFirstFire_NotAnEscalation(string metric)
    {
        var h = Armed(metric);
        var engine = h.Build();

        Assert.Equal(1, await StepAsync(metric, h, engine, T0, Level.Critical, 0));
        Assert.Equal(AlertSeverityLevel.Critical, Last(metric, h).Severity);
        Assert.False(Flag(metric, h));
    }

    [Fact]
    public async Task AMutedWarning_HoldsARiseForTheCooldown_SoTheMutedCriticalIsWrittenAtOnce()
    {
        /* #7085: a muted High CPU fire is not a page, so it is not "delivered" and the muted Critical is not flagged as an escalation
           over it (the flag only lets a cooldown-held email through, and a muted row sends nothing). It still counts for one cooldown:
           the Critical rise is judged against it and is written at once. */
        var h = Armed("High CPU");
        h.Muted = true;
        var engine = h.Build();

        Assert.Equal(1, await StepAsync("High CPU", h, engine, T0, Level.Warning, 0));
        Assert.True(Last("High CPU", h).Muted);
        Assert.Equal(1, await StepAsync("High CPU", h, engine, T0.AddMinutes(1), Level.Critical, 1));
        Assert.True(Last("High CPU", h).Muted);
        Assert.False(Flag("High CPU", h));
    }

    [Fact]
    public async Task VolumeFreeSpace_AnEscalationPassesTheWorseningCheck_WhenTheFreePercentBarelyMoved()
    {
        var h = Armed("Volume Free Space");
        var engine = h.Build();

        /* 3.5% free of a 100 GB volume (3.5 GB) is a breach but not critically low. 2.95% is critically low yet only
           0.55 points lower, inside the 1-point worsening step: on its own the worsening check would hold it. */
        h.Now = T0;
        h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = 102400, FreeMb = 3584 });
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = T0.AddMinutes(1);
        h.Adapter.Volumes[0].FreeMb = 3021;
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[1].Severity);
        Assert.True(h.Deliverer.Outcomes[1].Context!.EscalationBypassesCooldown);
    }
}
