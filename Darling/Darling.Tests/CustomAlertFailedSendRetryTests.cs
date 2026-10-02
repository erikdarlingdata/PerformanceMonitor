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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4795: a custom alert rule whose alert no channel delivered is sent again while its condition holds. A rule
/// has no cooldown, so before this the evaluator recorded the rising edge as delivered whether or not the send
/// worked, and the alert was lost until the condition cleared and came back. These drive the evaluator's
/// decision half (<see cref="CustomAlertEvaluator.ApplyValueAsync"/>) directly, one pass at a time on a clock the
/// test owns, over a deliverer that answers a staged <see cref="AlertDelivery"/>; no store is read or written.
/// </summary>
public sealed class CustomAlertFailedSendRetryTests
{
    private const int ServerId = 7;
    private const long RuleId = 4795;
    private const string DisplayName = "alpha-01";

    // Warning at >= 25, Critical at >= 40, fires on the first breach and resolves on the first clear.
    private const string DefinitionJson =
        "{\"metric\":{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"hours\":0.25}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}," +
        "\"hysteresis\":{\"breachSamples\":1,\"clearSamples\":1}," +
        "\"scope\":{\"mode\":\"servers\",\"servers\":[\"alpha-01\"]}}";

    private static readonly DateTime T0 = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);

    private sealed class StagedDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        /// <summary>What the channels did with a delivery: the answer <see cref="DeliverAndReportAsync"/> gives.
        /// Null (the default) is "unreported", which reads as delivered.</summary>
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

    /// <summary>One evaluator over a deliverer we control, plus the (rule, server) state carried from pass to pass
    /// the way the store would carry it. The data source is never opened by a fire; a resolve writes its history
    /// row through it and swallows the refusal, so a resolve is counted from its log line.</summary>
    private sealed class Rig : IAsyncDisposable
    {
        private readonly NpgsqlDataSource _dataSource =
            NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=1");

        public StagedDeliverer Deliverer { get; } = new();
        public CapturingTestLogger Log { get; } = new();
        public bool Muted { get; set; }
        public CustomAlertRule Row { get; } = new(
            RuleId, "retry rule", DefinitionJson, null, true, 1, T0, T0, null);
        public CustomAlertRuleDefinition Definition { get; }
        public CustomAlertEvaluator Evaluator { get; }
        public CustomAlertRuleState State { get; set; } = CustomAlertRuleState.Fresh(1);

        public Rig(CustomAlertRuleState? carriedState = null)
        {
            var (definition, error) = CustomAlertRuleDefinition.TryParse(DefinitionJson);
            Assert.True(definition is not null, error);
            Definition = definition!;
            if (carriedState is not null)
            {
                State = carriedState;
            }

            Evaluator = new CustomAlertEvaluator(
                new CustomAlertRuleStore(_dataSource), new CustomAlertStateStore(_dataSource), _dataSource, Deliverer,
                isAlertMuted: _ => Muted, alertsEnabled: static () => true, new PgAlertHistoryStore(_dataSource),
                defaultIntervalSeconds: 0, cacheTtl: TimeSpan.Zero, Log);
        }

        /// <summary>One sweep of the rule against this server: the value observed at <paramref name="at"/>.</summary>
        public async Task PassAsync(DateTime at, double value) =>
            State = await Evaluator.ApplyValueAsync(
                Row, Definition, ServerId, DisplayName, at, at.AddSeconds(30), value, State,
                TestContext.Current.CancellationToken);

        public int Resolves => Log.Lines.Count(l => l.Contains("ALERT RESOLVED", StringComparison.Ordinal));

        public int RetryLines => Log.Lines.Count(l => l.StartsWith("Information:", StringComparison.Ordinal)
            && l.Contains("no channel delivered", StringComparison.Ordinal));

        public ValueTask DisposeAsync() => _dataSource.DisposeAsync();
    }

    private static AlertDelivery FailedByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: "Slack: 429 Too Many Requests", AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

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

    private static AlertDelivery DeliveredByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Delivered, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    [Fact]
    public async Task EveryChannelFailsOnTheRisingEdge_SavesNoSeverity_AndSendsAgainAfterAMinute_ThenTwo()
    {
        await using var rig = new Rig();
        rig.Deliverer.Report = _ => FailedByWebhook();

        // The rising edge: sent once, every channel failed, so the incident is open but not delivered.
        await rig.PassAsync(T0, 30);
        Assert.Single(rig.Deliverer.Outcomes);
        Assert.True(rig.State.Persistence.Firing);
        Assert.Null(rig.State.FiredSeverity);

        // First failure: the retry waits a minute.
        await rig.PassAsync(T0.AddSeconds(59), 30);
        Assert.Single(rig.Deliverer.Outcomes);
        await rig.PassAsync(T0.AddSeconds(60), 30);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
        Assert.Null(rig.State.FiredSeverity);

        // Second failure in a row: two minutes.
        await rig.PassAsync(T0.AddSeconds(60 + 119), 30);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
        await rig.PassAsync(T0.AddSeconds(60 + 120), 30);
        Assert.Equal(3, rig.Deliverer.Outcomes.Count);
        Assert.Null(rig.State.FiredSeverity);

        // Every send is the same fire, at the severity the value has now: the same incident key each time.
        Assert.All(rig.Deliverer.Outcomes, o =>
        {
            Assert.Equal(CustomAlertEvaluator.MetricNameFor(RuleId), o.MetricName);
            Assert.Equal(AlertSeverityLevel.Warning, o.Severity);
        });

        // One Information line per failed send: rule id, server, how many failures in a row, and the wait.
        Assert.Equal(3, rig.RetryLines);
        var last = rig.Log.Lines[^1];
        Assert.Contains("4795", last, StringComparison.Ordinal);
        Assert.Contains(DisplayName, last, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADeliveredRetry_RecordsTheSeverity_AndTheNextPassSendsNothing()
    {
        await using var rig = new Rig();
        rig.Deliverer.Report = _ => FailedByWebhook();
        await rig.PassAsync(T0, 30);
        Assert.Null(rig.State.FiredSeverity);

        // The channel works again by the time the retry is due.
        rig.Deliverer.Report = _ => DeliveredByWebhook();
        await rig.PassAsync(T0.AddSeconds(60), 30);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
        Assert.Equal("Warning", rig.State.FiredSeverity);

        // Delivered: nothing more goes out while the condition holds, however long it holds.
        await rig.PassAsync(T0.AddSeconds(61), 30);
        await rig.PassAsync(T0.AddSeconds(120), 30);
        await rig.PassAsync(T0.AddSeconds(600), 30);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task TheWaitDoublesUpToFifteenMinutes_AndNoLonger()
    {
        await using var rig = new Rig();
        rig.Deliverer.Report = _ => FailedByWebhook();
        await rig.PassAsync(T0, 30);

        var at = T0;
        var sent = 1;
        foreach (var minutes in new[] { 1, 2, 4, 8, 15, 15 })
        {
            await rig.PassAsync(at.AddMinutes(minutes).AddSeconds(-1), 30);
            Assert.Equal(sent, rig.Deliverer.Outcomes.Count);
            at = at.AddMinutes(minutes);
            await rig.PassAsync(at, 30);
            Assert.Equal(++sent, rig.Deliverer.Outcomes.Count);
        }

    }

    [Fact]
    public async Task NoResolveGoesOut_WhenTheConditionClearsAfterAFailedSend_AndTheNextRisingEdgeStartsAtAMinute()
    {
        await using var rig = new Rig();
        rig.Deliverer.Report = _ => FailedByWebhook();
        await rig.PassAsync(T0, 30);
        Assert.Single(rig.Deliverer.Outcomes);

        // The condition clears before any retry got through: the alert never arrived, so no "resolved" goes out.
        await rig.PassAsync(T0.AddSeconds(30), 10);
        Assert.Equal(0, rig.Resolves);
        Assert.False(rig.State.Persistence.Firing);
        Assert.Null(rig.State.FiredSeverity);

        // It comes back: a new incident, a fresh streak. Its retry waits a minute, not the two the earlier
        // failure would have made it.
        await rig.PassAsync(T0.AddSeconds(40), 30);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
        await rig.PassAsync(T0.AddSeconds(40 + 59), 30);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
        await rig.PassAsync(T0.AddSeconds(40 + 60), 30);
        Assert.Equal(3, rig.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AResolveGoesOut_ForAnIncidentWhoseRetryWasDelivered()
    {
        await using var rig = new Rig();
        rig.Deliverer.Report = _ => FailedByWebhook();
        await rig.PassAsync(T0, 30);
        rig.Deliverer.Report = _ => DeliveredByWebhook();
        await rig.PassAsync(T0.AddSeconds(60), 30);
        Assert.Equal("Warning", rig.State.FiredSeverity);

        await rig.PassAsync(T0.AddSeconds(90), 10);
        Assert.Equal(1, rig.Resolves);
        Assert.Null(rig.State.FiredSeverity);
    }

    [Fact]
    public async Task ARestart_SendsTheUndeliveredFireOnItsFirstDuePass()
    {
        var before = new Rig();
        await using (before)
        {
            before.Deliverer.Report = _ => FailedByWebhook();
            await before.PassAsync(T0, 30);
            Assert.True(before.State.Persistence.Firing);
            Assert.Null(before.State.FiredSeverity);
        }

        // A new evaluator over the saved state of an incident that was opened and never delivered (firing, no
        // delivered severity): its retry table is empty, so the first pass sends the fire at once.
        await using var after = new Rig(before.State with { FiredSeverity = null });
        await after.PassAsync(T0.AddSeconds(5), 30);
        Assert.Single(after.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Warning, after.Deliverer.Outcomes[0].Severity);
        Assert.Equal("Warning", after.State.FiredSeverity);

        await after.PassAsync(T0.AddSeconds(65), 30);
        Assert.Single(after.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ARetryAfterARestart_SendsTheSeverityTheValueHasNow()
    {
        var before = new Rig();
        await using (before)
        {
            before.Deliverer.Report = _ => FailedByWebhook();
            await before.PassAsync(T0, 30);
        }

        await using var after = new Rig(before.State with { FiredSeverity = null });
        await after.PassAsync(T0.AddSeconds(5), 50);
        Assert.Equal(AlertSeverityLevel.Critical, Assert.Single(after.Deliverer.Outcomes).Severity);
        Assert.Equal("Critical", after.State.FiredSeverity);
    }

    [Fact]
    public async Task AFailedSeverityChange_KeepsTheOldBand_AndIsSentAgainWhenDue()
    {
        await using var rig = new Rig();

        // Delivered at Warning.
        await rig.PassAsync(T0, 30);
        Assert.Equal("Warning", rig.State.FiredSeverity);

        // The value climbs into Critical, and every channel fails on the change: the band the operator was
        // last told about stays Warning, so the next pass still sees the change.
        rig.Deliverer.Report = _ => FailedByWebhook();
        await rig.PassAsync(T0.AddSeconds(30), 50);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
        Assert.Equal(AlertSeverityLevel.Critical, rig.Deliverer.Outcomes[1].Severity);
        Assert.Equal("Warning", rig.State.FiredSeverity);

        // Not due for a minute after the failed send.
        await rig.PassAsync(T0.AddSeconds(30 + 59), 50);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);

        // Due: sent again, and this one is delivered.
        rig.Deliverer.Report = _ => DeliveredByWebhook();
        await rig.PassAsync(T0.AddSeconds(30 + 60), 50);
        Assert.Equal(3, rig.Deliverer.Outcomes.Count);
        Assert.Equal(AlertSeverityLevel.Critical, rig.Deliverer.Outcomes[2].Severity);
        Assert.Equal("Critical", rig.State.FiredSeverity);

        await rig.PassAsync(T0.AddSeconds(30 + 61), 50);
        Assert.Equal(3, rig.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task APartialFailure_RecordsTheSeverityAtOnce_AndIsNotSentAgain()
    {
        await using var rig = new Rig();

        // One channel delivered and another did not: a retry would send it twice down the channel that worked.
        rig.Deliverer.Report = _ => FailedByEmailButDeliveredByWebhook();
        await rig.PassAsync(T0, 30);
        Assert.Equal("Warning", rig.State.FiredSeverity);

        await rig.PassAsync(T0.AddSeconds(61), 30);
        await rig.PassAsync(T0.AddSeconds(200), 30);
        await rig.PassAsync(T0.AddSeconds(1000), 30);
        Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal(0, rig.RetryLines);
    }

    [Fact]
    public async Task AMutedFire_RecordsTheSeverityAtOnce_AndIsNotSentAgain()
    {
        await using var rig = new Rig { Muted = true };
        rig.Deliverer.Report = outcome => outcome.Muted ? MutedNothingAttempted() : null;

        await rig.PassAsync(T0, 30);
        Assert.True(Assert.Single(rig.Deliverer.Outcomes).Muted);
        Assert.Equal("Warning", rig.State.FiredSeverity);

        await rig.PassAsync(T0.AddSeconds(61), 30);
        await rig.PassAsync(T0.AddSeconds(1000), 30);
        Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal(0, rig.RetryLines);
    }
}
