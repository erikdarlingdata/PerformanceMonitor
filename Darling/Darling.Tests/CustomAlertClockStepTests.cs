/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4732: a custom alert rule waits on two times that are stamped ahead of the clock and compared to it raw. The per-rule
/// cadence (<c>NextDueAt</c>, saved with the rule's state, so it survives a restart) is <c>now + interval</c> when a pass
/// runs; the failed-send retry is <c>now + delay</c>, never more than <c>FailedSendRetryCap</c>. A wall clock that steps
/// backwards after either is stamped leaves it further ahead than it was ever written, and left raw it skipped the rule
/// (or held the alert back) until the clock caught up. Each now counts as due when it is more than the longest lead its
/// writer stamps ahead of the clock, and is honoured up to that lead. These run on a clock the test owns; the cadence
/// decision is driven through <see cref="CustomAlertEvaluator.CadenceIsWaiting"/> and the retry through the
/// evaluator's decision half (<see cref="CustomAlertEvaluator.ApplyValueAsync"/>), so no store is read or written.
/// </summary>
public sealed class CustomAlertClockStepTests
{
    private const int ServerId = 7;
    private const long RuleId = 4732;
    private const string DisplayName = "alpha-01";

    private static readonly TimeSpan Cap = TimeSpan.FromMinutes(15);

    // Warning at >= 25, fires on the first breach and resolves on the first clear.
    private const string DefinitionJson =
        "{\"metric\":{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"hours\":0.25}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}," +
        "\"hysteresis\":{\"breachSamples\":1,\"clearSamples\":1}," +
        "\"scope\":{\"mode\":\"servers\",\"servers\":[\"alpha-01\"]}}";

    private static readonly DateTime Now = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Minute = TimeSpan.FromSeconds(60);

    // ---- The per-rule cadence -------------------------------------------------------------------------------

    [Fact]
    public void ARuleThatHasNeverRun_IsNotWaiting()
    {
        Assert.False(CustomAlertEvaluator.CadenceIsWaiting(null, Now, Minute));
    }

    [Fact]
    public void ARuleStampedDueAnHourAhead_OnASixtySecondCadence_EvaluatesNow_AndOneDueInThirtySecondsWaits()
    {
        // The clock stepped back: the saved due time is an hour ahead, though it was written as now + 60 seconds.
        Assert.False(CustomAlertEvaluator.CadenceIsWaiting(Now.AddHours(1), Now, Minute), "a rule stamped an hour ahead must evaluate now");

        // The normal wait: 30 of the 60 seconds are left.
        Assert.True(CustomAlertEvaluator.CadenceIsWaiting(Now.AddSeconds(30), Now, Minute), "a rule with 30 seconds left still waits");
    }

    [Fact]
    public void TheClampIsStrictlyMoreThanOneInterval_SoAFullIntervalAheadStillWaits()
    {
        // now + interval is what a pass writes, so that exact lead is a normal wait...
        Assert.True(CustomAlertEvaluator.CadenceIsWaiting(Now + Minute, Now, Minute));

        // ...and one tick more can only be a step backwards.
        Assert.False(CustomAlertEvaluator.CadenceIsWaiting(Now + Minute + TimeSpan.FromTicks(1), Now, Minute));
    }

    [Theory]
    [InlineData(60, 0, false)]      // due this instant
    [InlineData(60, -1, false)]     // due a second ago
    [InlineData(60, -3600, false)]  // overdue by an hour (the service was stopped)
    [InlineData(60, 1, true)]
    [InlineData(60, 59, true)]
    [InlineData(300, 299, true)]    // a slower rule waits out its own, longer interval
    [InlineData(300, 3600, false)]
    [InlineData(1, 3600, false)]
    public void TheDecisionIsMadeAgainstTheRulesOwnInterval(int intervalSeconds, int dueInSeconds, bool waiting)
    {
        Assert.Equal(
            waiting,
            CustomAlertEvaluator.CadenceIsWaiting(Now.AddSeconds(dueInSeconds), Now, TimeSpan.FromSeconds(intervalSeconds)));
    }

    [Fact]
    public void TheEvaluationPass_DecidesThroughTheClamp_WithTheIntervalItStampsTheNextDueTimeWith()
    {
        var source = ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/CustomAlertEvaluator.cs");

        var interval = "var intervalSeconds = def.EvaluationIntervalSeconds ?? _defaultIntervalSeconds;";
        var check = "CadenceIsWaiting(state.NextDueAt, now, TimeSpan.FromSeconds(intervalSeconds))";
        var stamp = "var nextDue = now.AddSeconds(intervalSeconds);";
        Assert.Contains(check, source, StringComparison.Ordinal);
        Assert.Contains(stamp, source, StringComparison.Ordinal);

        // The interval is read before the check, and it is the one the stamp below the check uses.
        Assert.True(
            source.IndexOf(interval, StringComparison.Ordinal) < source.IndexOf(check, StringComparison.Ordinal)
            && source.IndexOf(check, StringComparison.Ordinal) < source.IndexOf(stamp, StringComparison.Ordinal),
            "the interval must be read first, then checked, then used to stamp the next due time");

        // No raw comparison of the saved due time with the clock is left in the pass.
        Assert.DoesNotContain("now < due", source, StringComparison.Ordinal);
        Assert.Contains("nextDueAt is DateTime due && now < CollectorCadence.ClampDue(due, now, interval)", source, StringComparison.Ordinal);
    }

    // ---- The failed-send retry -------------------------------------------------------------------------------

    [Fact]
    public async Task ARetryStampedFarPastTheCap_IsDueNow_AndAnAlertIsNotHeldUntilTheClockCatchesUp()
    {
        await using var rig = new Rig();
        rig.Deliverer.Report = _ => FailedByWebhook();

        // The rising edge fails: the retry is stamped a minute ahead.
        await rig.PassAsync(Now, 30);
        Assert.Single(rig.Deliverer.Outcomes);

        // The clock steps back an hour: the stamp is now 61 minutes ahead of it, far past the 15 minute cap.
        await rig.PassAsync(Now.AddHours(-1), 30);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task ARetryInsideTheCap_StillWaits_UpToTheCapExactly()
    {
        await using var rig = new Rig();
        rig.Deliverer.Report = _ => FailedByWebhook();
        await rig.PassAsync(Now, 30);
        Assert.Single(rig.Deliverer.Outcomes);

        // 11 minutes ahead (the stamp is at Now + 1 minute): inside the cap, so it is a wait, not a step.
        await rig.PassAsync(Now.AddMinutes(-10), 30);
        Assert.Single(rig.Deliverer.Outcomes);

        // Exactly the cap ahead: still the longest wait the writer can record, and it still waits.
        await rig.PassAsync(Now.AddMinutes(1) - Cap, 30);
        Assert.Single(rig.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ARetryOneSecondPastTheCapAhead_IsDueNow()
    {
        await using var rig = new Rig();
        rig.Deliverer.Report = _ => FailedByWebhook();
        await rig.PassAsync(Now, 30);
        Assert.Single(rig.Deliverer.Outcomes);

        await rig.PassAsync(Now.AddMinutes(1) - Cap - TimeSpan.FromSeconds(1), 30);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AFullCapWait_IsNotCutShort_WhenTheStreakHasReachedTheCap()
    {
        await using var rig = new Rig();
        rig.Deliverer.Report = _ => FailedByWebhook();
        await rig.PassAsync(Now, 30);

        // 1, 2, 4, 8 minutes, then the 15 minute cap: each retry goes out when it is due.
        var at = Now;
        var sent = 1;
        foreach (var minutes in new[] { 1, 2, 4, 8, 15 })
        {
            at = at.AddMinutes(minutes);
            await rig.PassAsync(at, 30);
            Assert.Equal(++sent, rig.Deliverer.Outcomes.Count);
        }

        // The last stamp is a full cap ahead. A pass just short of it is a legitimate wait and sends nothing.
        await rig.PassAsync(at.AddMinutes(14).AddSeconds(59), 30);
        Assert.Equal(sent, rig.Deliverer.Outcomes.Count);
        await rig.PassAsync(at.AddMinutes(15), 30);
        Assert.Equal(sent + 1, rig.Deliverer.Outcomes.Count);
    }

    [Fact]
    public void TheRetryCheck_IsClampedByTheCapTheWriterUses()
    {
        var source = ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/CustomAlertEvaluator.cs");

        Assert.DoesNotContain("now >= retryAtUtc", source, StringComparison.Ordinal);
        Assert.Contains("now >= CollectorCadence.ClampDue(retryAtUtc, now, FailedSendRetryCap)", source, StringComparison.Ordinal);

        // The clamp's interval is the cap the wait is recorded under.
        Assert.Contains("_failedSends.RecordFailure(family, key, now, FailedSendRetryCap, out var failures)", source, StringComparison.Ordinal);
        Assert.Contains("private static readonly TimeSpan FailedSendRetryCap = TimeSpan.FromMinutes(15);", source, StringComparison.Ordinal);
    }

    private sealed class StagedDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        /// <summary>What the channels did with a delivery: the answer <see cref="DeliverAndReportAsync"/> gives.</summary>
        public Func<AlertOutcome, AlertDelivery?>? Report { get; set; }

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }

        public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            await DeliverAsync(outcome, cancellationToken);
            return Report?.Invoke(outcome);
        }
    }

    /// <summary>One evaluator over a deliverer we control, plus the (rule, server) state carried from pass to pass the way
    /// the store would carry it. The data source is never opened by a fire.</summary>
    private sealed class Rig : IAsyncDisposable
    {
        private readonly NpgsqlDataSource _dataSource =
            NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=1");

        public StagedDeliverer Deliverer { get; } = new();
        public CapturingTestLogger Log { get; } = new();
        public CustomAlertRule Row { get; } = new(RuleId, "clock step rule", DefinitionJson, null, true, 1, Now, Now, null);
        public CustomAlertRuleDefinition Definition { get; }
        public CustomAlertEvaluator Evaluator { get; }
        public CustomAlertRuleState State { get; set; } = CustomAlertRuleState.Fresh(1);

        public Rig()
        {
            var (definition, error) = CustomAlertRuleDefinition.TryParse(DefinitionJson);
            Assert.True(definition is not null, error);
            Definition = definition!;

            Evaluator = new CustomAlertEvaluator(
                new CustomAlertRuleStore(_dataSource), new CustomAlertStateStore(_dataSource), _dataSource, Deliverer,
                isAlertMuted: _ => false, alertsEnabled: static () => true, new PgAlertHistoryStore(_dataSource),
                defaultIntervalSeconds: 0, cacheTtl: TimeSpan.Zero, Log);
        }

        /// <summary>One sweep of the rule against this server: the value observed at <paramref name="at"/>.</summary>
        public async Task PassAsync(DateTime at, double value) =>
            State = await Evaluator.ApplyValueAsync(
                Row, Definition, ServerId, DisplayName, at, at.AddSeconds(30), value, State,
                TestContext.Current.CancellationToken);

        public ValueTask DisposeAsync() => _dataSource.DisposeAsync();
    }

    private static AlertDelivery FailedByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: "Slack: 429 Too Many Requests", AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    /* Locate the repo from this file: no build-output copying. */
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
