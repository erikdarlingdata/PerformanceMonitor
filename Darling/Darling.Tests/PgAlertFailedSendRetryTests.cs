/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4795: a PostgreSQL alert whose every channel failed is tried again a minute later, not after the whole
/// cooldown. The six families (the findings, High CPU, Deadlocks, Blocking, Long-Running Query and Poison Wait)
/// stamp their cooldown before they deliver, and used to call the send that returns no result, so a failed send
/// looked like a delivered one. They now keep the delivery's answer and hand it to
/// <see cref="DarlingWorker.AfterPgFireCore"/>, the twin of <c>AlertEngine.AfterFire</c> (#4752).
///
/// <para>The arms are private and read a live store, so they cannot be driven here. What is pinned is the two
/// halves either side of that: the behaviour of what each arm calls after its send (the back-dated stamp, the
/// streak, the marker restores, all against the real gate where one applies), and the source of each arm, so a
/// send that keeps its result and reports it cannot be quietly turned back into one that discards it. The
/// source pins are sliced per arm so one arm's wiring cannot vouch for another's.</para>
/// </summary>
public sealed class PgAlertFailedSendRetryTests
{
    private static readonly DateTime FireTime = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Cooldown = TimeSpan.FromMinutes(5);

    private const string DeadlockFamily = AlertEngine.DeadlockWatermarkMetric;
    private const string PoisonKey = "7|" + PostgresAlertEvaluator.PoisonWaitMetric + "|LWLock:BufferMapping";

    /* The delivery shapes the channels really produce, as AlertEngineTests builds them for the SQL Server
       families: the deliverer answers these same values for a PostgreSQL alert. */
    private static AlertDelivery FailedByEmail() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.Failed, SendError: "SMTP: 535 authentication failed",
            WebhookOutcome: AlertChannelOutcome.NotAttempted, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private static AlertDelivery FailedByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: "Slack: 429 Too Many Requests", AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private static AlertDelivery DeliveredByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Delivered, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    /* A PARTIAL failure: the email failed, the webhook delivered. Sent is true and SendError is set, so a retry
       would send the alert a second time down the channel that worked. */
    private static AlertDelivery FailedByEmailButDeliveredByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.Failed, SendError: "SMTP: 535 authentication failed",
            WebhookOutcome: AlertChannelOutcome.Delivered, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private static AlertDelivery MutedNothingAttempted() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.NotAttempted, WebhookSendError: null, AnyChannelConfigured: true),
        muted: true, trayChannelPresent: false);

    /// <summary>Runs one fire's after-send step the way an arm does: the arm has already stamped its cooldown
    /// at <paramref name="now"/> before it delivered.</summary>
    private static bool Fire(
        FailedSendBackoff backoff, DarlingSelfAlertTests.CapturingLogger logger, string family,
        ConcurrentDictionary<string, DateTime> stamps, string key, DateTime now, AlertDelivery? delivery)
    {
        stamps[key] = now;
        return DarlingWorker.AfterPgFireCore(backoff, logger, family, stamps, key, now, Cooldown, delivery);
    }

    [Theory]
    [InlineData(PostgresAlertEvaluator.WraparoundMetric, "7|" + PostgresAlertEvaluator.WraparoundMetric + "|orders")]
    [InlineData(AlertEngine.CpuPersistenceMetric, "7")]
    [InlineData(AlertEngine.DeadlockWatermarkMetric, "7")]
    [InlineData(AlertEngine.BlockingWatermarkMetric, "7")]
    [InlineData(AlertEngine.LongRunningQueryWatermarkMetric, "7")]
    [InlineData(PostgresAlertEvaluator.PoisonWaitMetric, PoisonKey)]
    public void AFireWhoseEveryChannelFailed_OpensAgainAMinuteLater_NotAfterTheCooldown(string family, string key)
    {
        foreach (var failure in new[] { FailedByEmail(), FailedByWebhook() })
        {
            var backoff = new FailedSendBackoff();
            var stamps = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);

            var everyChannelFailed = Fire(backoff, new DarlingSelfAlertTests.CapturingLogger(), family, stamps, key, FireTime, failure);

            Assert.True(everyChannelFailed);

            /* The check every arm makes is now - last >= cooldown. */
            Assert.True(FireTime.AddSeconds(30) - stamps[key] < Cooldown, "still closed 30 seconds after the failed fire");
            Assert.True(FireTime.AddSeconds(61) - stamps[key] >= Cooldown, "open 61 seconds after the failed fire");
        }
    }

    [Fact]
    public void ConsecutiveFailures_DoubleTheWait_AndNeverPassTheCooldown()
    {
        var backoff = new FailedSendBackoff();
        var logger = new DarlingSelfAlertTests.CapturingLogger();
        var stamps = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);

        var now = FireTime;
        foreach (var expectedMinutes in new[] { 1, 2, 4, 5, 5 })
        {
            Assert.True(Fire(backoff, logger, DeadlockFamily, stamps, "7", now, FailedByWebhook()));

            /* stamp + cooldown is the moment the arm's own check opens again. */
            Assert.Equal(now.AddMinutes(expectedMinutes), stamps["7"] + Cooldown);
            now = stamps["7"] + Cooldown;
        }
    }

    [Fact]
    public void ADeliveredAlert_AfterAFailedOne_WaitsTheFullCooldown_AndTheNextFailureStartsAtAMinute()
    {
        var backoff = new FailedSendBackoff();
        var logger = new DarlingSelfAlertTests.CapturingLogger();
        var stamps = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);

        Assert.True(Fire(backoff, logger, DeadlockFamily, stamps, "7", FireTime, FailedByWebhook()));
        var retryTime = stamps["7"] + Cooldown;

        /* The retry is delivered: the stamp stays where the arm put it, so the alert waits the whole cooldown. */
        Assert.False(Fire(backoff, logger, DeadlockFamily, stamps, "7", retryTime, DeliveredByWebhook()));
        Assert.Equal(retryTime, stamps["7"]);
        Assert.True(retryTime.AddSeconds(299) - stamps["7"] < Cooldown);
        Assert.True(retryTime.AddSeconds(300) - stamps["7"] >= Cooldown);

        /* The delivery ended the streak: the next failure waits a minute again, not the two the streak would
           have earned. */
        var laterFire = retryTime + Cooldown;
        Assert.True(Fire(backoff, logger, DeadlockFamily, stamps, "7", laterFire, FailedByWebhook()));
        Assert.Equal(laterFire.AddMinutes(1), stamps["7"] + Cooldown);
    }

    [Fact]
    public void APartialFailure_AMutedAlert_ADeliveredOne_AndNoReport_AreNotRetriedEarly()
    {
        var shapes = new (string Name, AlertDelivery? Delivery)[]
        {
            ("partial failure", FailedByEmailButDeliveredByWebhook()),
            ("muted", MutedNothingAttempted()),
            ("delivered", DeliveredByWebhook()),
            ("unreported", null),
        };

        foreach (var (name, delivery) in shapes)
        {
            var backoff = new FailedSendBackoff();
            var logger = new DarlingSelfAlertTests.CapturingLogger();
            var stamps = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);

            var everyChannelFailed = Fire(backoff, logger, DeadlockFamily, stamps, "7", FireTime, delivery);

            Assert.False(everyChannelFailed, name);
            Assert.Equal(FireTime, stamps["7"]);
            Assert.Equal(0, backoff.TrackedCount);
            Assert.Empty(logger.Entries);
        }
    }

    [Fact]
    public void TheRetryLine_NamesTheFamilyTheKeyTheFailureCountAndTheDelay()
    {
        var backoff = new FailedSendBackoff();
        var logger = new DarlingSelfAlertTests.CapturingLogger();
        var stamps = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);

        Fire(backoff, logger, DeadlockFamily, stamps, "7", FireTime, FailedByWebhook());
        Fire(backoff, logger, DeadlockFamily, stamps, "7", stamps["7"] + Cooldown, FailedByWebhook());

        Assert.Equal(2, logger.Entries.Count);
        Assert.All(logger.Entries, e => Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Information, e.Level));
        Assert.Contains(DeadlockFamily, logger.Entries[0].Message, StringComparison.Ordinal);
        Assert.Contains(" on 7 ", logger.Entries[0].Message, StringComparison.Ordinal);
        Assert.Contains("(failure 1)", logger.Entries[0].Message, StringComparison.Ordinal);
        Assert.Contains("00:01:00", logger.Entries[0].Message, StringComparison.Ordinal);
        Assert.Contains("(failure 2)", logger.Entries[1].Message, StringComparison.Ordinal);
        Assert.Contains("00:02:00", logger.Entries[1].Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheStreaks_AreKeptPerFamilyAndKey()
    {
        var backoff = new FailedSendBackoff();
        var logger = new DarlingSelfAlertTests.CapturingLogger();
        var stamps = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);

        /* Two failures for one server's deadlocks: the third would wait four minutes. */
        Fire(backoff, logger, DeadlockFamily, stamps, "7", FireTime, FailedByWebhook());
        Fire(backoff, logger, DeadlockFamily, stamps, "7", FireTime.AddMinutes(1), FailedByWebhook());

        /* Another server, and another family on the same server, each start at a minute. */
        Fire(backoff, logger, DeadlockFamily, stamps, "8", FireTime.AddMinutes(2), FailedByWebhook());
        Assert.Equal(FireTime.AddMinutes(3), stamps["8"] + Cooldown);

        var blockingStamps = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);
        Fire(backoff, logger, AlertEngine.BlockingWatermarkMetric, blockingStamps, "7", FireTime.AddMinutes(2), FailedByWebhook());
        Assert.Equal(FireTime.AddMinutes(3), blockingStamps["7"] + Cooldown);
    }

    [Theory]
    [InlineData(0, 3)]
    [InlineData(2, 5)]
    public void AFailedDeadlockOrBlockingFire_PutsTheWatermarkBackToItsPreFireValue_SoTheRetryFiresAtTheSameCount(
        int preFireWatermark, int count)
    {
        const int threshold = 1;

        var fired = RollingCountAlertGate.Evaluate(count, threshold, preFireWatermark, cooldownElapsed: true, suppressed: false);
        Assert.True(fired.Fire);
        Assert.Equal(count, fired.Watermark);

        /* Every channel failed: the watermark the gate advanced (and the arm saved) goes back. */
        var restored = DarlingWorker.PgUnannouncedWatermark(preFireWatermark, count);
        Assert.Equal(preFireWatermark, restored);

        /* 30 seconds later the cooldown is still closed, and the count is not consumed by waiting. */
        var waiting = RollingCountAlertGate.Evaluate(count, threshold, restored, cooldownElapsed: false, suppressed: false);
        Assert.False(waiting.Fire);
        Assert.Equal(restored, waiting.Watermark);

        /* At 61 seconds the cooldown has opened and the same count fires again. */
        var retry = RollingCountAlertGate.Evaluate(count, threshold, restored, cooldownElapsed: true, suppressed: false);
        Assert.True(retry.Fire);
        Assert.Equal(count, retry.Watermark);
    }

    [Theory]
    [InlineData(0, 3)]
    [InlineData(2, 5)]
    public void ADeliveredDeadlockOrBlockingFire_KeepsTheAdvancedWatermark_SoTheSameCountDoesNotFireAgain(
        int preFireWatermark, int count)
    {
        var fired = RollingCountAlertGate.Evaluate(count, threshold: 1, preFireWatermark, cooldownElapsed: true, suppressed: false);
        Assert.True(fired.Fire);

        /* Nothing puts the watermark back after a delivery, so the same count stays reported once the
           cooldown has passed. */
        var later = RollingCountAlertGate.Evaluate(count, threshold: 1, fired.Watermark, cooldownElapsed: true, suppressed: false);
        Assert.False(later.Fire);
    }

    [Fact]
    public void AFailedPoisonWaitFire_PutsBackThePriorCollectionTime_SoTheRetryCountsTheSameCollectionAsFresh()
    {
        var times = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);
        var earlier = FireTime.AddMinutes(-10);
        var newest = FireTime.AddMinutes(-1);

        /* The arm's own guard: only a collection newer than the one last fired on is a fresh observation. */
        bool HasFreshCollection() => !times.TryGetValue(PoisonKey, out var last) || newest > last;

        /* A prior collection time exists: the arm recorded the newest before it delivered. */
        times[PoisonKey] = earlier;
        Assert.True(HasFreshCollection());
        times[PoisonKey] = newest;
        Assert.False(HasFreshCollection());

        DarlingWorker.RestorePgPoisonCollectionTime(times, PoisonKey, earlier);
        Assert.Equal(earlier, times[PoisonKey]);
        Assert.True(HasFreshCollection());

        /* The failed fire was the first one for the subject: there was no entry, so there is none again. */
        times[PoisonKey] = newest;
        DarlingWorker.RestorePgPoisonCollectionTime(times, PoisonKey, null);
        Assert.False(times.ContainsKey(PoisonKey));
        Assert.True(HasFreshCollection());
    }

    /* ---------------- source pins: each arm keeps the send's answer and reports it ---------------- */

    private static string Worker() =>
        RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

    private static string Slice(string source, string startMarker, string endMarker)
    {
        var start = source.IndexOf(startMarker, StringComparison.Ordinal);
        Assert.True(start >= 0, $"marker not found: {startMarker}");
        var end = source.IndexOf(endMarker, start + startMarker.Length, StringComparison.Ordinal);
        Assert.True(end > start, $"end marker not found after start: {endMarker}");
        return source[start..end];
    }

    [Theory]
    [InlineData(
        "private async Task EvaluatePostgresAlertsAsync(",
        "await EvaluatePgDeadlocksAsync(runtime, snapshot, config, cancellationToken);",
        "AfterPgFire(finding.MetricName, _lastPostgresAlert, cooldownKey, now, cooldown, delivery)")]
    [InlineData(
        "private async Task EvaluatePgCpuAsync(",
        "private async Task EvaluatePgDeadlocksAsync(",
        "AfterPgFire(metricName, _lastPgCpuAlert, key, now, cooldown, delivery)")]
    [InlineData(
        "private async Task EvaluatePgDeadlocksAsync(",
        "private async Task EvaluatePgBlockingAsync(",
        "AfterPgFire(metricName, _lastPgDeadlockAlert, key, now, cooldown, delivery)")]
    [InlineData(
        "private async Task EvaluatePgBlockingAsync(",
        "private const int PgLongRunningQueryRecencyMinutes",
        "AfterPgFire(metricName, _lastPgBlockingAlert, key, now, cooldown, delivery)")]
    [InlineData(
        "private async Task EvaluatePgLongRunningQueryAsync(",
        "internal static AlertIncident BuildPgLongRunningQueryIncident(",
        "AfterPgFire(metricName, _lastPgLongRunningQueryAlert, key, now, cooldown, delivery)")]
    [InlineData(
        "private async Task EvaluatePgPoisonWaitAsync(",
        "private async Task NotifyPgResolutionAsync(",
        "AfterPgFire(finding.MetricName, _lastPgPoisonWaitAlert, cooldownKey, now, cooldown, delivery)")]
    public void EachPostgresArm_KeepsTheSendsAnswer_AndReportsItRightAfterAgainstItsOwnCooldownStamps(
        string startMarker, string endMarker, string expectedAfterFireCall)
    {
        var arm = Slice(Worker(), startMarker, endMarker);

        Assert.DoesNotContain("_alertDeliverer.DeliverAsync(", arm, StringComparison.Ordinal);

        var send = arm.IndexOf("var delivery = await _alertDeliverer.DeliverAndReportAsync(", StringComparison.Ordinal);
        Assert.True(send >= 0, "the arm no longer keeps the result of the reporting send");

        var report = arm.IndexOf(expectedAfterFireCall, StringComparison.Ordinal);
        Assert.True(report > send, $"the arm no longer reports the send's answer with: {expectedAfterFireCall}");
    }

    [Theory]
    [InlineData(
        "private async Task EvaluatePgDeadlocksAsync(",
        "private async Task EvaluatePgBlockingAsync(",
        "_lastAlertedPgDeadlockCount")]
    [InlineData(
        "private async Task EvaluatePgBlockingAsync(",
        "private const int PgLongRunningQueryRecencyMinutes",
        "_lastAlertedPgBlockingCount")]
    public void TheCountArms_PutTheWatermarkBackAndSaveIt_WhenEveryChannelFailed(
        string startMarker, string endMarker, string watermarkField)
    {
        var arm = Slice(Worker(), startMarker, endMarker);

        var report = arm.IndexOf("if (AfterPgFire(", StringComparison.Ordinal);
        Assert.True(report >= 0, "the arm does not act on the report of a failed send");

        var restore = arm.IndexOf("var unannouncedWatermark = PgUnannouncedWatermark(watermark, count);", StringComparison.Ordinal);
        Assert.True(restore > report, "the arm does not compute the pre-fire watermark after a failed send");

        var inMemory = arm.IndexOf($"{watermarkField}[key] = unannouncedWatermark;", StringComparison.Ordinal);
        Assert.True(inMemory > restore, "the arm does not put the in-memory watermark back");

        var save = arm.IndexOf("SaveEdgeTriggerWatermarkAsync(key, metricName, unannouncedWatermark)", StringComparison.Ordinal);
        Assert.True(save > inMemory, "the arm does not save the restored watermark after putting it back in memory");
    }

    [Fact]
    public void PoisonWait_PutsTheCollectionTimeBack_WhenEveryChannelFailed()
    {
        var arm = Slice(Worker(), "private async Task EvaluatePgPoisonWaitAsync(", "private async Task NotifyPgResolutionAsync(");

        var prior = arm.IndexOf("var hadPriorCollection = _lastPgPoisonWaitCollectionTime.TryGetValue(cooldownKey, out var lastCollection);", StringComparison.Ordinal);
        Assert.True(prior >= 0, "the arm no longer remembers whether a collection time was recorded before the fire");

        var report = arm.IndexOf("if (AfterPgFire(", StringComparison.Ordinal);
        Assert.True(report > prior, "the arm does not act on the report of a failed send");

        var restore = arm.IndexOf(
            "RestorePgPoisonCollectionTime(_lastPgPoisonWaitCollectionTime, cooldownKey, hadPriorCollection ? lastCollection : null);",
            StringComparison.Ordinal);
        Assert.True(restore > report, "the arm does not put the prior collection time back after a failed send");
    }

    [Fact]
    public void DarlingWorker_HasNoDiscardingSend_AndExactlySixReportingSends()
    {
        /* Census: a seventh PostgreSQL arm has to choose a send and wire the same report, and an arm turned
           back to the discarding send fails the per-arm pins above and this count. */
        var worker = Worker();

        Assert.DoesNotContain("_alertDeliverer.DeliverAsync(", worker, StringComparison.Ordinal);
        Assert.Equal(6, CountOccurrences(worker, "_alertDeliverer.DeliverAndReportAsync("));

        /* The definition is "bool AfterPgFire(" and "AfterPgFireCore(" does not match; the rest are the six calls. */
        Assert.Equal(6, CountOccurrences(worker, "AfterPgFire(") - CountOccurrences(worker, "bool AfterPgFire("));
    }

    private static int CountOccurrences(string source, string value)
    {
        var count = 0;
        var at = 0;
        while ((at = source.IndexOf(value, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += value.Length;
        }

        return count;
    }
}
