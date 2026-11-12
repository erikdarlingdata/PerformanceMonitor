/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5641 (seat decision): a Runnable Queue incident open at Warning delivers the first evaluation graded Critical at
/// once, past the cooldown, and that Critical restarts the cooldown clock. Critical going back to Warning delivers
/// nothing new, and repeated Critical evaluations follow the normal cooldown. The incident replay in
/// <c>RunnableQueueIncidentReplayTests</c> pins the 10-09 timeline; these pin the rule on its own, the downstream
/// legs (the shared cooldown and the repeat ceiling, the Darling viewer's tray cooldown) and the opt-in boundary.
/// </summary>
public sealed class RunnableQueueEscalationTests
{
    private static readonly DateTime T0 = new(2026, 10, 9, 15, 40, 0, DateTimeKind.Utc);

    /* ---------------- the helper ---------------- */

    [Fact]
    public void TierEscalation_IsAnEscalationOnlyWhenTheOpenIncidentWasDeliveredLower()
    {
        var gate = new TierEscalation();

        /* Nothing delivered yet: the first fire follows the ordinary cooldown, whatever its tier. */
        Assert.False(gate.IsEscalation("k", AlertSeverityLevel.Warning));
        Assert.False(gate.IsEscalation("k", AlertSeverityLevel.Critical));

        gate.Record("k", AlertSeverityLevel.Warning);
        Assert.False(gate.IsEscalation("k", AlertSeverityLevel.Warning));
        Assert.True(gate.IsEscalation("k", AlertSeverityLevel.Critical));

        /* Delivering the Critical records it: further Criticals are ordinary, and so is Critical back to Warning. */
        gate.Record("k", AlertSeverityLevel.Critical);
        Assert.False(gate.IsEscalation("k", AlertSeverityLevel.Critical));
        Assert.False(gate.IsEscalation("k", AlertSeverityLevel.Warning));

        /* A later Warning fire does not lower the record: Critical, Warning, Critical is not a second escalation. */
        gate.Record("k", AlertSeverityLevel.Warning);
        Assert.False(gate.IsEscalation("k", AlertSeverityLevel.Critical));

        /* Another server's incident is its own. */
        Assert.False(gate.IsEscalation("other", AlertSeverityLevel.Critical));

        /* Closing the incident starts the next one from nothing delivered. */
        gate.Close("k");
        Assert.False(gate.IsEscalation("k", AlertSeverityLevel.Critical));
        gate.Record("k", AlertSeverityLevel.Warning);
        Assert.True(gate.IsEscalation("k", AlertSeverityLevel.Critical));
    }

    [Fact]
    public void RunnableQueueIsAnOptedInAlert_AndTheOthersKeepTheOldBehaviour()
    {
        /* The full list is pinned in TwoTierEscalationTests (#5715 added High CPU, Deadlocks, Poison Wait and Volume Free Space). */
        Assert.True(TierEscalation.IsOptedIn(RunnableQueueEvaluator.MetricName));

        /* The alerts that keep the old behaviour: a worsening inside the window waits for the cooldown. */
        foreach (var metric in new[] { "Blocking Detected", "tempdb Space" })
        {
            Assert.False(TierEscalation.IsOptedIn(metric), metric);
        }

        Assert.False(TierEscalation.IsOptedIn(null));
    }

    [Fact]
    public void TheEngineAsksTheEscalationHelperAtTheSixOptedInFireSitesOnly()
    {
        var engine = System.IO.File.ReadAllText(RepoFile("PerformanceMonitor.Alerting/AlertEngine.cs"));

        /* Runnable Queue, Client Timeouts (#5640) and the four older two-tier alerts (#5715) are the six fire sites. */
        Assert.Equal(6, Count(engine, "IsEscalation("));
        Assert.Equal(1, Count(engine, "_runnableQueueEscalation.IsEscalation("));
        Assert.Equal(1, Count(engine, "_attentionEscalation.IsEscalation("));
        Assert.Equal(1, Count(engine, "_cpuEscalation.IsEscalation("));
        Assert.Equal(1, Count(engine, "_deadlockEscalation.IsEscalation("));
        Assert.Equal(1, Count(engine, "_poisonWaitEscalation.IsEscalation("));
        Assert.Equal(1, Count(engine, "_lowDiskEscalation.IsEscalation("));
        Assert.Equal(6, Count(engine, "EscalationBypassesCooldown ="));
    }

    /* ---------------- the engine ---------------- */

    private static (AlertEngineTests.Harness H, AlertEngine Engine) Engine()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.RunnableQueueEnabled = true;
        return (h, h.Build());
    }

    private static List<RunnableQueueSample> Samples(DateTime now, params double[] perMinuteAverages)
    {
        /* One scheduler-stats collection a minute, newest last, each at 4 schedulers. */
        var list = new List<RunnableQueueSample>();
        for (var i = 0; i < perMinuteAverages.Length; i++)
        {
            var at = now.AddMinutes(-(perMinuteAverages.Length - 1 - i)).AddSeconds(-5);
            list.Add(new RunnableQueueSample(at, perMinuteAverages[i], (long)(perMinuteAverages[i] * 4), 4));
        }

        return list;
    }

    private static async Task<int> EvaluateAsync(AlertEngineTests.Harness h, AlertEngine engine, DateTime at, params double[] averages)
    {
        h.Now = at;
        h.Adapter.RunnableQueue = Samples(at, averages);
        var before = h.Deliverer.Outcomes.Count(o => o.Hold is null);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        return h.Deliverer.Outcomes.Count(o => o.Hold is null) - before;
    }

    /// <summary>#7086: a Warning is held (a digest row, nothing sent), so the flows below count PAGES only, and the held
    /// row is checked where a test needs it.</summary>
    private static AlertOutcome LastHeld(AlertEngineTests.Harness h) =>
        h.Deliverer.Outcomes.Last(o => o.Hold is not null);

    [Fact]
    public async Task AWarningThatWorsensToCritical_IsDeliveredAtOnce_AndRestartsTheClock()
    {
        var (h, engine) = Engine();

        /* #7086: the Warning is held, not paged, and it leaves the page cooldown and the tier alone. */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0, 9, 9, 9));
        Assert.Equal(AlertSeverityLevel.Warning, LastHeld(h).Severity);

        /* A minute later, a fresh collection takes the average over the Critical bar: it pages at once. Nothing stamped the
           cooldown, so it is an ordinary first page and needs no bypass flag. */
        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(1), 9, 9, 9, 60));
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[^1].Severity);
        Assert.False(h.Deliverer.Outcomes[^1].Context!.EscalationBypassesCooldown);

        /* The clock restarted at the Critical: the next Critical evaluations are held by the normal cooldown. */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(2), 9, 9, 60, 60));
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(5), 60, 60, 60, 60, 60));
    }

    [Fact]
    public async Task RepeatedCritical_FollowsTheNormalCooldown_AndIsNotFlaggedAsAnEscalation()
    {
        var (h, engine) = Engine();

        Assert.Equal(0, await EvaluateAsync(h, engine, T0, 9, 9, 9));
        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(1), 9, 9, 9, 60));

        /* The default cooldown is counted from the Critical (T0 + 1 minute), not from the Warning. */
        var due = T0.AddMinutes(1).Add(TimeSpan.FromMinutes(h.Settings.CooldownMinutes));
        Assert.Equal(0, await EvaluateAsync(h, engine, due.AddSeconds(-30), 60, 60, 60, 60, 60));
        Assert.Equal(1, await EvaluateAsync(h, engine, due, 60, 60, 60, 60, 60, 60));
        Assert.False(h.Deliverer.Outcomes[^1].Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task CriticalGoingBackToWarning_DeliversNothingNew_AndANewCriticalAfterThatIsNotASecondEscalation()
    {
        var (h, engine) = Engine();

        Assert.Equal(0, await EvaluateAsync(h, engine, T0, 9, 9, 9));
        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(1), 9, 9, 9, 60));

        /* Back to Warning inside the cooldown: nothing. */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(2), 9, 9, 9, 9, 9));

        /* Critical again, still inside the cooldown: the incident already delivered its Critical, so it waits. */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(3), 9, 60, 60, 60, 60));
    }

    [Fact]
    public async Task ACriticalThatOpensTheIncident_IsAnOrdinaryFirstFire_NotAnEscalation()
    {
        var (h, engine) = Engine();

        Assert.Equal(1, await EvaluateAsync(h, engine, T0, 60, 60, 60));
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[^1].Severity);
        Assert.False(h.Deliverer.Outcomes[^1].Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task AClearedIncident_StartsOver_SoTheNextWarningThenCriticalEscalatesAgain()
    {
        var (h, engine) = Engine();

        Assert.Equal(0, await EvaluateAsync(h, engine, T0, 9, 9, 9));
        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(1), 9, 9, 9, 60));

        /* Under the Warning bar: cleared. */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(2), 0, 0, 0, 0, 0));

        /* A new incident inside the old cooldown: its Warning is held, as before... */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(3), 0, 0, 9, 9, 9));
        /* ...and after the cooldown a Warning is still held and the Critical that follows it pages once more. */
        var later = T0.AddMinutes(1).Add(TimeSpan.FromMinutes(h.Settings.CooldownMinutes)).AddMinutes(1);
        Assert.Equal(0, await EvaluateAsync(h, engine, later, 9, 9, 9));
        Assert.Equal(1, await EvaluateAsync(h, engine, later.AddMinutes(1), 9, 9, 9, 60));
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[^1].Severity);
    }

    [Fact]
    public async Task AMutedIncident_RecordsItsMutedCriticalAtOnceToo_SoTheHistoryRowIsNotLate()
    {
        var (h, engine) = Engine();
        h.Muted = true;

        /* The muted Warning is held and the mute is recorded on its row. */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0, 9, 9, 9));
        Assert.True(LastHeld(h).Muted);

        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(1), 9, 9, 9, 60));
        Assert.True(h.Deliverer.Outcomes[^1].Muted);
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[^1].Severity);
    }

    /* ---------------- the shared cooldown and the repeat ceiling ---------------- */

    private static AlertDelivery FailedByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: "Teams: 429 Too Many Requests", AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    [Fact]
    public async Task AnEscalationWhoseEveryChannelFailed_BacksOffLikeAnyFailedSend_AndADeliveredSendEndsTheStreak()
    {
        /* #5715 part A: the escalation skipped the cooldown stamp a failed send back-dates (#4752), so a Critical went out at
           every evaluation while every channel was down. While the family and key have an open streak it waits for the stamp. */
        var (h, engine) = Engine();

        Assert.Equal(0, await EvaluateAsync(h, engine, T0, 9, 9, 9));

        h.Deliverer.Report = _ => FailedByWebhook();
        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(1), 9, 9, 9, 60));      /* failure 1: retry in 1 minute */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(1).AddSeconds(30), 9, 9, 9, 60));
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(1).AddSeconds(59), 9, 9, 9, 60));

        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(2), 9, 9, 9, 60));      /* failure 2: retry in 2 minutes */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(3), 9, 9, 60, 60));
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(3).AddSeconds(59), 9, 60, 60, 60));

        h.Deliverer.Report = null;
        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(4), 60, 60, 60, 60));   /* delivered: the streak ends */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(4).AddSeconds(30), 60, 60, 60, 60, 60));

        /* A later incident's Critical goes out once the cooldown is up, after its held Warning. */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(5), 0, 0, 0, 0, 0));
        var later = T0.AddMinutes(4).Add(TimeSpan.FromMinutes(h.Settings.CooldownMinutes)).AddMinutes(1);
        Assert.Equal(0, await EvaluateAsync(h, engine, later, 9, 9, 9));
        Assert.Equal(1, await EvaluateAsync(h, engine, later.AddMinutes(1), 9, 9, 9, 60));
    }

    [Fact]
    public async Task TheSharedCooldown_LetsTheFlaggedEscalationThroughInsideTheWindow_AndTheStampRestartsIt()
    {
        var cooldown = new IncidentCooldown("", (_, _, _) => Task.FromResult<DateTime?>(null));
        var window = TimeSpan.FromMinutes(15);

        var warning = await cooldown.EvaluateAsync("7", "Runnable Queue", null, window);
        Assert.True(warning.ShouldSend);
        Assert.False(warning.Bypassed);
        cooldown.Stamp(warning);

        var held = await cooldown.EvaluateAsync("7", "Runnable Queue", null, window);
        Assert.False(held.ShouldSend);

        var escalation = await cooldown.EvaluateAsync("7", "Runnable Queue", null, window, bypassCooldown: true);
        Assert.True(escalation.ShouldSend);
        Assert.True(escalation.Bypassed);
        Assert.False(escalation.AnyFirstNotice);
        cooldown.Stamp(escalation);

        /* The window restarted at the escalation. */
        Assert.False((await cooldown.EvaluateAsync("7", "Runnable Queue", null, window)).ShouldSend);
    }

    [Fact]
    public void TheRepeatCeiling_NeverFoldsAnEscalation_ButAPlainRepeatStillFolds()
    {
        var budget = new RepeatDeliveryBudget();
        var window = TimeSpan.FromMinutes(15);

        IncidentCooldown.Decision At(int minute, bool bypassed) =>
            new(true, new[] { "7:Runnable Queue" }, T0.AddMinutes(minute), null, AnyFirstNotice: false, Bypassed: bypassed);

        /* The first carrier of the window. */
        Assert.True(budget.Evaluate("Runnable Queue", "SRV-A", At(0, false), window, aggregateRepeats: true).ShouldSend);

        /* A plain repeat inside the window is folded; the escalation is not. */
        Assert.False(budget.Evaluate("Runnable Queue", "SRV-A", At(1, false), window, aggregateRepeats: true).ShouldSend);
        Assert.True(budget.Evaluate("Runnable Queue", "SRV-A", At(1, true), window, aggregateRepeats: true).ShouldSend);
    }

    /* ---------------- the Darling viewer's tray cooldown ---------------- */

    private static ViewerAlertRow Row(DateTime at, AlertSeverityLevel tier, string metric = "Runnable Queue") => new()
    {
        AlertTime = at,
        ServerId = 7,
        ServerName = "Server7",
        MetricName = metric,
        CurrentValue = 20,
        ThresholdValue = 8,
        AlertSent = true,
        NotificationType = "tray",
        Muted = false,
        ContextJson = AlertContextSerializer.Serialize(new AlertContext { SeverityOverride = tier }),
    };

    [Fact]
    public void TheViewerToasts_ARunnableQueueCriticalAfterItsWarningInsideTheTrayCooldown()
    {
        var coordinator = new AlertToastCoordinator(TimeSpan.FromMinutes(20));
        var cooldown = TimeSpan.FromMinutes(5);

        Assert.Single(coordinator.SelectToasts(new[] { Row(T0, AlertSeverityLevel.Warning) }, T0.AddSeconds(5), cooldown));

        var critical = coordinator.SelectToasts(
            new[] { Row(T0.AddMinutes(1), AlertSeverityLevel.Critical) }, T0.AddMinutes(1).AddSeconds(5), cooldown);
        Assert.Single(critical);

        /* The Critical toast restarted the viewer's clock: a second Critical, and a Warning, wait. */
        Assert.Empty(coordinator.SelectToasts(
            new[] { Row(T0.AddMinutes(2), AlertSeverityLevel.Critical) }, T0.AddMinutes(2).AddSeconds(5), cooldown));
        Assert.Empty(coordinator.SelectToasts(
            new[] { Row(T0.AddMinutes(3), AlertSeverityLevel.Warning) }, T0.AddMinutes(3).AddSeconds(5), cooldown));
    }

    [Fact]
    public void TheViewerKeepsItsCooldownForEveryOtherAlert_EvenWhenTheRowIsCriticalAfterAWarning()
    {
        var coordinator = new AlertToastCoordinator(TimeSpan.FromMinutes(20));
        var cooldown = TimeSpan.FromMinutes(5);

        Assert.Single(coordinator.SelectToasts(new[] { Row(T0, AlertSeverityLevel.Warning, "Blocking Detected") }, T0.AddSeconds(5), cooldown));
        Assert.Empty(coordinator.SelectToasts(
            new[] { Row(T0.AddMinutes(1), AlertSeverityLevel.Critical, "Blocking Detected") }, T0.AddMinutes(1).AddSeconds(5), cooldown));
    }

    /* ---------------- helpers ---------------- */

    private static int Count(string text, string needle)
    {
        var count = 0;
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static string RepoFile(string relative, [System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = new System.IO.DirectoryInfo(System.IO.Path.GetDirectoryName(thisFile)!);
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        return System.IO.Path.Combine(dir!.FullName, relative);
    }
}
