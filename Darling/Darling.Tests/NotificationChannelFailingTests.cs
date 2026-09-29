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
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Notification Channel Failing self-alert (#4750). A webhook channel counts its failures in a row
/// and logs the first few, but nothing else reported them, so a channel could fail for weeks while another
/// one delivered every alert. The evaluator now reads those counts through one seam and raises ONE alert per
/// channel when a count first reaches three, none while it stays there, and one recovery row when it is back
/// at 0. The text names the channel and the count and never an error, because a webhook error can carry the
/// endpoint's URL and the URL is the credential.
/// </summary>
public sealed class NotificationChannelFailingTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private const string FailingName = "Notification Channel Failing";
    private const string RecoveredName = "Notification Channel Recovered";

    /// <summary>One evaluator over recording fakes, with the failure counts the seam returns held in a
    /// dictionary the test edits between passes.</summary>
    private sealed class Rig
    {
        public DarlingSelfAlertTests.FakeSettings Settings { get; } = new();
        public DarlingSelfAlertTests.RecordingDeliverer Deliverer { get; } = new();
        public DarlingSelfAlertTests.FakeHistoryStore History { get; } = new();

        public Dictionary<string, int> Counts { get; } = new(StringComparer.Ordinal)
        {
            [NotificationRouter.TeamsChannel] = 0,
            [NotificationRouter.SlackChannel] = 0,
            [NotificationRouter.GenericChannel] = 0,
            [NotificationRouter.PagerDutyChannel] = 0,
        };

        public DateTime Now { get; set; } = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);

        public DarlingSelfAlertEvaluator Build(bool wireSeam = true) => new(
            Settings, Deliverer, History, _ => false,
            utcNow: () => Now,
            webhookChannelFailures: wireSeam
                ? () => Counts.Select(kv => new WebhookChannelFailureCount(kv.Key, kv.Value)).ToArray()
                : null);
    }

    [Fact]
    public async Task ASlackChannelAtThreeFailures_RaisesOneAlert_ForSlackOnly()
    {
        var rig = new Rig();
        rig.Counts[NotificationRouter.SlackChannel] = 3;
        rig.Counts[NotificationRouter.GenericChannel] = 2;
        var e = rig.Build();

        await e.ApplyNotificationChannelsAsync(Ct);

        var fired = Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal(FailingName, fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Contains("Slack", fired.ServerKey, StringComparison.Ordinal);
        Assert.Contains("Slack", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("3 times in a row", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("Slack", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Equal(3, fired.NumericCurrentValue);
        Assert.Equal(WebhookAlertService.FailingChannelThreshold, fired.NumericThresholdValue);

        /* The other channels are not named: Generic sat at two, under the bar, and the rest at 0. */
        foreach (var other in new[] { "Teams", "Generic", "PagerDuty" })
        {
            Assert.DoesNotContain(other, fired.DetailText, StringComparison.Ordinal);
            Assert.DoesNotContain(other, fired.ShortMessage, StringComparison.Ordinal);
        }

        Assert.Empty(rig.History.Records);
    }

    [Fact]
    public async Task TheCountStayingAtOrAboveThree_DoesNotRaiseItAgain()
    {
        var rig = new Rig();
        rig.Counts[NotificationRouter.SlackChannel] = 3;
        var e = rig.Build();

        await e.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(rig.Deliverer.Outcomes);

        /* The same count, a higher one, the log's every-fiftieth mark, and a day later: an edge does not
           re-fire on a cooldown, however long the channel stays broken. */
        await e.ApplyNotificationChannelsAsync(Ct);
        rig.Counts[NotificationRouter.SlackChannel] = 4;
        await e.ApplyNotificationChannelsAsync(Ct);
        rig.Counts[NotificationRouter.SlackChannel] = 50;
        rig.Now = rig.Now.AddDays(1);
        await e.ApplyNotificationChannelsAsync(Ct);

        Assert.Single(rig.Deliverer.Outcomes);
        Assert.Empty(rig.History.Records);
    }

    [Fact]
    public async Task TheCountBackAtZero_ClearsWithARecoveredRow_ThenTheNextRunRaisesAgain()
    {
        var rig = new Rig();
        rig.Counts[NotificationRouter.SlackChannel] = 3;
        var e = rig.Build();
        await e.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(rig.Deliverer.Outcomes);

        rig.Counts[NotificationRouter.SlackChannel] = 0;
        await e.ApplyNotificationChannelsAsync(Ct);

        var recovered = Assert.Single(rig.History.Records);
        Assert.Equal(RecoveredName, recovered.MetricName);
        Assert.Contains("Slack", recovered.DetailText, StringComparison.Ordinal);
        Assert.Contains("Slack", recovered.ServerId, StringComparison.Ordinal);
        Assert.Single(rig.Deliverer.Outcomes);

        /* Steady at 0 writes nothing more. */
        await e.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(rig.History.Records);

        /* A NEW run of failures is a new edge. */
        rig.Counts[NotificationRouter.SlackChannel] = 3;
        await e.ApplyNotificationChannelsAsync(Ct);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task CountsUnderThree_NeverRaise_AndAnAnnouncedChannelHoldsUntilItIsBackAtZero()
    {
        var rig = new Rig();
        var e = rig.Build();

        foreach (var under in new[] { 0, 1, 2 })
        {
            rig.Counts[NotificationRouter.PagerDutyChannel] = under;
            await e.ApplyNotificationChannelsAsync(Ct);
        }

        Assert.Empty(rig.Deliverer.Outcomes);
        Assert.Empty(rig.History.Records);

        rig.Counts[NotificationRouter.PagerDutyChannel] = 3;
        await e.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(rig.Deliverer.Outcomes);

        /* Delivered and failed again between two passes: a 2 is neither a recovery nor a new failure. */
        rig.Counts[NotificationRouter.PagerDutyChannel] = 2;
        await e.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(rig.Deliverer.Outcomes);
        Assert.Empty(rig.History.Records);

        rig.Counts[NotificationRouter.PagerDutyChannel] = 0;
        await e.ApplyNotificationChannelsAsync(Ct);
        Assert.Equal(RecoveredName, Assert.Single(rig.History.Records).MetricName);
    }

    [Fact]
    public async Task TwoFailingChannels_AreEachRaisedOnceUnderTheirOwnKey()
    {
        var rig = new Rig();
        rig.Counts[NotificationRouter.SlackChannel] = 3;
        rig.Counts[NotificationRouter.PagerDutyChannel] = 40;
        var e = rig.Build();

        await e.ApplyNotificationChannelsAsync(Ct);
        await e.ApplyNotificationChannelsAsync(Ct);

        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
        Assert.Equal(2, rig.Deliverer.Outcomes.Select(o => o.ServerKey).Distinct(StringComparer.Ordinal).Count());
        Assert.Contains(rig.Deliverer.Outcomes, o => o.DetailText!.Contains("PagerDuty", StringComparison.Ordinal)
            && o.DetailText.Contains("40 times in a row", StringComparison.Ordinal));
        Assert.Contains(rig.Deliverer.Outcomes, o => o.DetailText!.Contains("Slack", StringComparison.Ordinal)
            && o.DetailText.Contains("3 times in a row", StringComparison.Ordinal));

        /* One recovering leaves the other announced. */
        rig.Counts[NotificationRouter.SlackChannel] = 0;
        await e.ApplyNotificationChannelsAsync(Ct);
        var recovered = Assert.Single(rig.History.Records);
        Assert.Contains("Slack", recovered.ServerId, StringComparison.Ordinal);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task WithTheMasterAlertsSwitchOff_NothingIsRaisedOrRecorded()
    {
        var rig = new Rig();
        rig.Settings.AlertsEnabled = false;
        rig.Counts[NotificationRouter.SlackChannel] = 9;
        var e = rig.Build();

        await e.ApplyNotificationChannelsAsync(Ct);

        Assert.Empty(rig.Deliverer.Outcomes);
        Assert.Empty(rig.History.Records);
    }

    [Fact]
    public async Task AnEvaluatorBuiltWithoutTheSeam_JudgesNoChannel()
    {
        var rig = new Rig();
        rig.Counts[NotificationRouter.SlackChannel] = 9;
        var e = rig.Build(wireSeam: false);

        await e.EvaluateNotificationChannelsAsync(Ct);

        Assert.Empty(rig.Deliverer.Outcomes);
        Assert.Empty(rig.History.Records);
    }

    [Fact]
    public async Task ASeamThatThrows_IsLoggedAndSwallowed_SoTheSweepLoopKeepsRunning()
    {
        var rig = new Rig();
        var e = new DarlingSelfAlertEvaluator(
            rig.Settings, rig.Deliverer, rig.History, _ => false,
            webhookChannelFailures: () => throw new InvalidOperationException("seam boom"));

        await e.EvaluateNotificationChannelsAsync(Ct);

        Assert.Empty(rig.Deliverer.Outcomes);
    }

    /// <summary>
    /// The URL rule, end to end. A real <see cref="WebhookAlertService"/> fails three Slack posts against an
    /// endpoint that answers 500 and echoes a webhook URL in its body, which is exactly what lands in the
    /// service's last error. The alert built from those counts must carry neither that error nor any part of
    /// the URL. The seam hands over counts only, so this holds by construction; the test pins that the
    /// construction stays that way.
    /// </summary>
    [Fact]
    public async Task TheAlertCarriesNoErrorText_EvenWhenTheChannelsLastErrorHoldsAWebhookUrl()
    {
        const string secretUrl = "https://hooks.slack.com/services/T0000/B0000/SecretTokenAbc123";
        using var slack = new CapturingWebhookEndpoint(statusCode: 500, responseBody: "invalid_token " + secretUrl);
        var webhooks = new WebhookAlertService(
            new FakeSlackSettings { SlackWebhookUrl = slack.Url },
            DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);

        /* Distinct servers, so no per-server cooldown can stand between a failing channel and its next post. */
        foreach (var server in new[] { "SQL01", "SQL02", "SQL03" })
        {
            var sent = await webhooks.TrySendWebhookAlertsAsync("High CPU", server, "97%", "90%");
            Assert.Equal(AlertChannelOutcome.Failed, sent.Outcome);
        }

        var health = webhooks.GetSlackHealth();
        Assert.Equal(3, health.ConsecutiveFailures);
        Assert.Contains("SecretTokenAbc123", health.LastError, StringComparison.Ordinal);

        var rig = new Rig();
        var e = new DarlingSelfAlertEvaluator(
            rig.Settings, rig.Deliverer, rig.History, _ => false,
            webhookChannelFailures: webhooks.GetChannelFailureCounts);

        await e.ApplyNotificationChannelsAsync(Ct);

        var fired = Assert.Single(rig.Deliverer.Outcomes);
        Assert.Contains("Slack", fired.DetailText, StringComparison.Ordinal);
        var everyField = string.Join(
            "\n", fired.ServerKey, fired.ServerName, fired.MetricName, fired.CurrentValue, fired.ThresholdValue,
            fired.DetailText, fired.ShortMessage, fired.DisplayName);
        Assert.DoesNotContain("SecretTokenAbc123", everyField, StringComparison.Ordinal);
        Assert.DoesNotContain("hooks.slack.com", everyField, StringComparison.Ordinal);
        Assert.DoesNotContain("invalid_token", everyField, StringComparison.Ordinal);
        Assert.DoesNotContain("HTTP 500", everyField, StringComparison.Ordinal);
        Assert.DoesNotContain(slack.Url, everyField, StringComparison.Ordinal);
        Assert.DoesNotContain(health.LastError!, everyField, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheWebhookService_ReportsEveryChannelsCount_InTheRouterChannelNames()
    {
        using var slack = new CapturingWebhookEndpoint(statusCode: 500);
        var webhooks = new WebhookAlertService(
            new FakeSlackSettings { SlackWebhookUrl = slack.Url },
            DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);

        Assert.All(webhooks.GetChannelFailureCounts(), c => Assert.Equal(0, c.ConsecutiveFailures));

        await webhooks.TrySendWebhookAlertsAsync("High CPU", "SQL01", "97%", "90%");
        await webhooks.TrySendWebhookAlertsAsync("High CPU", "SQL02", "97%", "90%");

        var counts = webhooks.GetChannelFailureCounts().ToDictionary(c => c.Channel, c => c.ConsecutiveFailures);
        Assert.Equal(
            new[]
            {
                NotificationRouter.TeamsChannel, NotificationRouter.SlackChannel,
                NotificationRouter.GenericChannel, NotificationRouter.PagerDutyChannel,
            }.OrderBy(n => n, StringComparer.Ordinal),
            counts.Keys.OrderBy(n => n, StringComparer.Ordinal));
        Assert.Equal(2, counts[NotificationRouter.SlackChannel]);
        Assert.Equal(webhooks.GetSlackHealth().ConsecutiveFailures, counts[NotificationRouter.SlackChannel]);
        Assert.Equal(0, counts[NotificationRouter.TeamsChannel]);
        Assert.Equal(0, counts[NotificationRouter.GenericChannel]);
        Assert.Equal(0, counts[NotificationRouter.PagerDutyChannel]);
    }

    /// <summary>The metric names are webhook automation keys and the history grids classify by name, so both
    /// are pinned as strings, and the resolution must read as one.</summary>
    [Fact]
    public void TheTwoNames_AreStable_AndClassifyAsACountAlertAndAResolution()
    {
        Assert.Equal(FailingName, DarlingSelfAlertEvaluator.NotificationChannelFailingMetric);
        Assert.Equal(RecoveredName, DarlingSelfAlertEvaluator.NotificationChannelRecoveredMetric);

        Assert.False(AlertMetricClassifier.IsResolution(FailingName));
        Assert.True(AlertMetricClassifier.IsWarning(FailingName));
        Assert.False(AlertMetricClassifier.IsCritical(FailingName));
        Assert.Equal("3", AlertMetricClassifier.FormatHistoryValue(FailingName, 3));
        Assert.False(AlertMetricClassifier.IsStateOnly(FailingName));

        Assert.True(AlertMetricClassifier.IsResolution(RecoveredName));
    }

    [Fact]
    public void TheFamily_IsSelfMonitor_AndItsResolutionFoldsOntoTheFiringTriagePage()
    {
        Assert.Equal(AlertFamily.SelfMonitor, AlertFamily.Of(FailingName));
        Assert.Contains(
            DarlingTriageEndpoint.ResolutionAliases,
            a => a.Alias == RecoveredName && a.Canonical == FailingName);

        var firing = DarlingTriageEndpoint.SectionsFor(FailingName);
        Assert.NotSame(DarlingTriageEndpoint.DefaultSections, firing);
        Assert.All(firing, s => Assert.True(s.FleetLevel));
        Assert.Same(firing, DarlingTriageEndpoint.SectionsFor(RecoveredName));
    }

    /// <summary>The wiring is in a large method a unit test cannot run, so it is pinned from source: the
    /// worker hands the evaluator the counts of the webhook service the deliverer sends through, and asks it to
    /// judge them on the sweep loop.</summary>
    [Fact]
    public void TheWorker_WiresTheSeamToTheDeliverersWebhookService_AndEvaluatesItOnTheSweep()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        Assert.Contains("webhookChannelFailures: webhookAlertService.GetChannelFailureCounts", worker, StringComparison.Ordinal);
        Assert.Contains("_selfAlerts.EvaluateNotificationChannelsAsync(stoppingToken)", worker, StringComparison.Ordinal);
    }

    /// <summary>A Slack-only <see cref="IAlertSettings"/>: every other channel unconfigured, so a post can only
    /// go to the endpoint under test.</summary>
    private sealed class FakeSlackSettings : IAlertSettings
    {
        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 25;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "";
        public string SmtpRecipients => "";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;
        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => !string.IsNullOrWhiteSpace(SlackWebhookUrl);
        public string SlackWebhookUrl { get; init; } = "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled => false;
        public string GenericWebhookUrl => "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled => false;
        public string PagerDutyRoutingKey => "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity => 1.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 5;
        public string TriageBaseUrl => "";
    }
}
