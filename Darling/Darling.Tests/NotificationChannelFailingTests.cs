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

        /// <summary>Channels the seam reports with no destination left (#4750): disabled, or their URL removed.</summary>
        public HashSet<string> Unconfigured { get; } = new(StringComparer.Ordinal);

        public DateTime Now { get; set; } = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);

        public DarlingSelfAlertEvaluator Build(bool wireSeam = true) => new(
            Settings, Deliverer, History, _ => false,
            utcNow: () => Now,
            webhookChannelFailures: wireSeam
                ? () => Counts.Select(kv => new WebhookChannelFailureCount(kv.Key, kv.Value, !Unconfigured.Contains(kv.Key))).ToArray()
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

    private const string TurnedOffTail = ": the Slack webhook channel was turned off (no destination is configured for it)";

    /// <summary>The same resolution for a route's destination, which names the route (<see cref="SlackRoute"/> is route 1).</summary>
    private const string RouteTurnedOffTail = ": the Slack (route 1) webhook channel was turned off (no destination is configured for it)";

    private static int s_serverSequence;

    /// <summary>Fails the Slack channel <paramref name="times"/> times against the endpoint the settings name.
    /// Every post is from a distinct server, so no per-server cooldown can stand between a failing channel and
    /// its next post.</summary>
    private static async Task FailSlackAsync(WebhookAlertService webhooks, int times)
    {
        for (var i = 0; i < times; i++)
        {
            var server = "SQL" + Interlocked.Increment(ref s_serverSequence);
            var sent = await webhooks.TrySendWebhookAlertsAsync("High CPU", server, "97%", "90%");
            Assert.Equal(AlertChannelOutcome.Failed, sent.Outcome);
        }
    }

    /// <summary>A real webhook service and an evaluator reading its counts, with a Slack endpoint that fails.
    /// The settings are the test's to change, which is how an operator turns a channel off.</summary>
    private sealed class LiveRig : IDisposable
    {
        private readonly CapturingWebhookEndpoint _slack = new(statusCode: 500);

        public LiveRig(bool slackViaParentSettings = true)
        {
            Slack = new FakeSlackSettings
            {
                SlackWebhookEnabled = slackViaParentSettings,
                SlackWebhookUrl = slackViaParentSettings ? _slack.Url : "",
            };
            Webhooks = new WebhookAlertService(Slack, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);
            Evaluator = new DarlingSelfAlertEvaluator(
                Rig.Settings, Rig.Deliverer, Rig.History, _ => false,
                webhookChannelFailures: Webhooks.GetChannelFailureCounts);
        }

        public Rig Rig { get; } = new();
        public FakeSlackSettings Slack { get; }
        public WebhookAlertService Webhooks { get; }
        public DarlingSelfAlertEvaluator Evaluator { get; }
        public string SlackUrl => _slack.Url;

        public void Dispose() => _slack.Dispose();
    }

    private static NotificationRoute SlackRoute(string url, bool enabled) =>
        new(RouteId: 1, MetricMatch: "High CPU", TeamsUrl: "", SlackUrl: url, GenericUrl: "", PagerDutyRoutingKey: "",
            SmtpRecipients: "", Enabled: enabled);

    /// <summary>
    /// Turning a failing channel off is the expected response to the alert, so it has to close it. A channel
    /// with no destination can neither fail nor deliver, so its count used to sit at the last value until the
    /// process restarted and the alert stayed open. Here the operator removes the URL.
    /// </summary>
    [Fact]
    public async Task AFailingChannelWhoseUrlIsRemoved_ResolvesAsTurnedOff_WithTheTurnedOffText()
    {
        using var live = new LiveRig();
        await FailSlackAsync(live.Webhooks, 3);
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Equal(FailingName, Assert.Single(live.Rig.Deliverer.Outcomes).MetricName);

        live.Slack.SlackWebhookUrl = "";
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);

        var resolved = Assert.Single(live.Rig.History.Records);
        Assert.Equal(RecoveredName, resolved.MetricName);
        Assert.EndsWith(TurnedOffTail, resolved.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("delivering again", resolved.DetailText, StringComparison.Ordinal);
        Assert.Contains("Slack", resolved.ServerId, StringComparison.Ordinal);

        /* Once: the next look finds a channel with no destination and a count of 0, and says nothing. */
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(live.Rig.History.Records);
        Assert.Single(live.Rig.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AFailingChannelThatIsDisabled_ResolvesTheSameWay()
    {
        using var live = new LiveRig();
        await FailSlackAsync(live.Webhooks, 4);
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(live.Rig.Deliverer.Outcomes);

        live.Slack.SlackWebhookEnabled = false;
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);

        var resolved = Assert.Single(live.Rig.History.Records);
        Assert.Equal(RecoveredName, resolved.MetricName);
        Assert.EndsWith(TurnedOffTail, resolved.DetailText, StringComparison.Ordinal);
        Assert.Single(live.Rig.Deliverer.Outcomes);
    }

    /// <summary>The two resolutions say which of the two happened, never a combined phrase.</summary>
    [Fact]
    public async Task AChannelThatDeliversAgain_ResolvesWithTheDeliveredText_NotTheTurnedOffText()
    {
        var rig = new Rig();
        rig.Counts[NotificationRouter.SlackChannel] = 3;
        var e = rig.Build();
        await e.ApplyNotificationChannelsAsync(Ct);

        rig.Counts[NotificationRouter.SlackChannel] = 0;
        await e.ApplyNotificationChannelsAsync(Ct);

        var resolved = Assert.Single(rig.History.Records);
        Assert.Equal(RecoveredName, resolved.MetricName);
        Assert.EndsWith(": the Slack webhook channel is delivering again (failures in a row back to 0)", resolved.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("turned off", resolved.DetailText, StringComparison.Ordinal);
    }

    /// <summary>A channel with no destination never raises the alert, however high its count: there is
    /// nothing for it to have failed at.</summary>
    [Fact]
    public async Task AChannelWithNoDestination_NeverRaisesFailing_HoweverHighItsCount()
    {
        var rig = new Rig();
        rig.Unconfigured.Add(NotificationRouter.SlackChannel);
        rig.Counts[NotificationRouter.SlackChannel] = 40;
        var e = rig.Build();

        await e.ApplyNotificationChannelsAsync(Ct);
        await e.ApplyNotificationChannelsAsync(Ct);

        Assert.Empty(rig.Deliverer.Outcomes);
        Assert.Empty(rig.History.Records);
    }

    /// <summary>The count reset is what keeps a channel the operator turns back on from raising Failing at
    /// once on the count it had when it was turned off.</summary>
    [Fact]
    public async Task AChannelTurnedBackOn_StartsFromZero_NoFailingUntilThreeNewFailures()
    {
        using var live = new LiveRig();
        await FailSlackAsync(live.Webhooks, 3);
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(live.Rig.Deliverer.Outcomes);

        live.Slack.SlackWebhookUrl = "";
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(live.Rig.History.Records);
        Assert.Equal(0, live.Webhooks.GetSlackHealth().ConsecutiveFailures);

        live.Slack.SlackWebhookUrl = live.SlackUrl;
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(live.Rig.Deliverer.Outcomes);

        await FailSlackAsync(live.Webhooks, 2);
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(live.Rig.Deliverer.Outcomes);

        await FailSlackAsync(live.Webhooks, 1);
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Equal(2, live.Rig.Deliverer.Outcomes.Count);
        Assert.Contains("3 times in a row", live.Rig.Deliverer.Outcomes[1].DetailText, StringComparison.Ordinal);
    }

    /// <summary>A route can carry a channel the parent settings do not (an only-pages-page setup), so a channel
    /// whose parent settings are empty but which an enabled route still carries is not turned off. Disabling
    /// that route is what turns it off, and the resolution names the route whose destination it was.</summary>
    [Fact]
    public async Task AChannelWithEmptyParentSettings_ThatAnEnabledRouteCarries_IsNotTurnedOff()
    {
        using var live = new LiveRig(slackViaParentSettings: false);
        live.Slack.NotificationRoutes = new[] { SlackRoute(live.SlackUrl, enabled: true) };
        await FailSlackAsync(live.Webhooks, 3);
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(live.Rig.Deliverer.Outcomes);

        var slack = live.Webhooks.GetChannelFailureCounts().Single(c => c.Channel == NotificationRouter.SlackChannel && c.RouteId == 1);
        Assert.True(slack.Configured);
        Assert.Equal(3, slack.ConsecutiveFailures);

        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Empty(live.Rig.History.Records);

        live.Slack.NotificationRoutes = new[] { SlackRoute(live.SlackUrl, enabled: false) };
        await live.Evaluator.ApplyNotificationChannelsAsync(Ct);

        var resolved = Assert.Single(live.Rig.History.Records);
        Assert.EndsWith(RouteTurnedOffTail, resolved.DetailText, StringComparison.Ordinal);
    }

    /// <summary>The service's own read: a channel with no destination comes back with its current count and
    /// Configured false, and the count is then reset (with the last error, as a delivery would).</summary>
    [Fact]
    public async Task TheWebhookService_ReportsWhichChannelsAreConfigured_AndClearsTheCountOfAnUnconfiguredOne()
    {
        using var live = new LiveRig();
        await FailSlackAsync(live.Webhooks, 2);

        var configured = live.Webhooks.GetChannelFailureCounts().ToDictionary(c => c.Channel);
        Assert.True(configured[NotificationRouter.SlackChannel].Configured);
        Assert.Equal(2, configured[NotificationRouter.SlackChannel].ConsecutiveFailures);
        foreach (var other in new[] { NotificationRouter.TeamsChannel, NotificationRouter.GenericChannel, NotificationRouter.PagerDutyChannel })
        {
            Assert.False(configured[other].Configured);
        }

        live.Slack.SlackWebhookEnabled = false;
        var first = live.Webhooks.GetChannelFailureCounts().Single(c => c.Channel == NotificationRouter.SlackChannel);
        Assert.False(first.Configured);
        Assert.Equal(2, first.ConsecutiveFailures);

        var second = live.Webhooks.GetChannelFailureCounts().Single(c => c.Channel == NotificationRouter.SlackChannel);
        Assert.False(second.Configured);
        Assert.Equal(0, second.ConsecutiveFailures);
        Assert.Equal((0, (string?)null), live.Webhooks.GetSlackHealth());
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
        /* #6231: the stored text is the status only; the endpoint's body never reaches it. */
        Assert.Equal("HTTP 500 Internal Server Error", health.LastError);
        Assert.DoesNotContain("SecretTokenAbc123", health.LastError, StringComparison.Ordinal);

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

    /// <summary>
    /// Two routes point Slack at two destinations, and one of them is broken. Each destination has its own count,
    /// so a delivery to the healthy destination is not a recovery of the broken one: its count climbs to the
    /// threshold, the alert is raised, and the text names the route that is failing. Turning that route off closes
    /// the alert the way turning a channel off does.
    /// </summary>
    [Fact]
    public async Task TwoRoutesOnOneChannel_TheBrokenDestinationKeepsCounting_WhileTheOtherDelivers()
    {
        using var broken = new CapturingWebhookEndpoint(statusCode: 500);
        using var healthy = new CapturingWebhookEndpoint(statusCode: 200);
        var brokenRoute = new NotificationRoute(
            RouteId: 1, MetricMatch: "High CPU", TeamsUrl: "", SlackUrl: broken.Url, GenericUrl: "", PagerDutyRoutingKey: "",
            SmtpRecipients: "", Enabled: true);
        var healthyRoute = new NotificationRoute(
            RouteId: 2, MetricMatch: "Spare Metric", TeamsUrl: "", SlackUrl: healthy.Url, GenericUrl: "", PagerDutyRoutingKey: "",
            SmtpRecipients: "", Enabled: true);
        var settings = new FakeSlackSettings
        {
            SlackWebhookEnabled = false,
            SlackWebhookUrl = "",
            NotificationRoutes = new[] { brokenRoute, healthyRoute },
        };
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);
        var rig = new Rig();
        var e = new DarlingSelfAlertEvaluator(
            rig.Settings, rig.Deliverer, rig.History, _ => false,
            webhookChannelFailures: webhooks.GetChannelFailureCounts);

        /* A failure on route 1 and a delivery on route 2, three times over. Distinct servers, by name and id, so neither the
           per-server cooldown a delivery stamps nor anything else stands between a destination and its next post. */
        for (var i = 0; i < 3; i++)
        {
            var server = "SQL" + Interlocked.Increment(ref s_serverSequence);
            Assert.Equal(AlertChannelOutcome.Failed, (await webhooks.TrySendWebhookAlertsAsync("High CPU", server, "97%", "90%", serverId: server)).Outcome);
            Assert.Equal(AlertChannelOutcome.Delivered, (await webhooks.TrySendWebhookAlertsAsync("Spare Metric", server, "1", "0", serverId: server)).Outcome);
        }

        var slack = webhooks.GetChannelFailureCounts().Where(c => c.Channel == NotificationRouter.SlackChannel).ToList();
        Assert.Equal(3, slack.Max(c => c.ConsecutiveFailures));
        Assert.Contains(slack, c => c.ConsecutiveFailures == 0 && c.Configured);

        await e.ApplyNotificationChannelsAsync(Ct);

        var fired = Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal(FailingName, fired.MetricName);
        Assert.Contains("Slack", fired.ServerKey, StringComparison.Ordinal);
        Assert.Contains("Slack", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("route 1", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("route 1", fired.ShortMessage, StringComparison.Ordinal);
        Assert.DoesNotContain("route 2", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain(broken.Url, fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("3 times in a row", fired.DetailText, StringComparison.Ordinal);

        /* Another delivery on route 2 recovers nothing: route 1 is still broken. */
        var later = "SQL" + Interlocked.Increment(ref s_serverSequence);
        Assert.Equal(AlertChannelOutcome.Delivered, (await webhooks.TrySendWebhookAlertsAsync("Spare Metric", later, "1", "0", serverId: later)).Outcome);
        await e.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(rig.Deliverer.Outcomes);
        Assert.Empty(rig.History.Records);

        /* Disabling the failing route closes its alert as turned off, and names the route. */
        settings.NotificationRoutes = new[] { brokenRoute with { Enabled = false }, healthyRoute };
        await e.ApplyNotificationChannelsAsync(Ct);

        var resolved = Assert.Single(rig.History.Records);
        Assert.Equal(RecoveredName, resolved.MetricName);
        Assert.Contains("route 1", resolved.DetailText, StringComparison.Ordinal);
        Assert.Contains("turned off", resolved.DetailText, StringComparison.Ordinal);
        Assert.Single(rig.Deliverer.Outcomes);
    }

    /// <summary>A resolution's facts, for the send path that is handed a destination and not a route.</summary>
    private static AlertResolutionFacts ResolutionFacts(string serverName) => new(
        "CPU Resolved", "High CPU", serverName, new DateTime(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc),
        new DateTime(2026, 9, 29, 12, 5, 0, DateTimeKind.Utc), "97%", "CPU has returned to normal", TriageUrl: null);

    /// <summary>Posts one Slack resolution to <paramref name="destination"/> from a distinct server, so no cooldown
    /// stands in the way, and returns the send's error text, null when it delivered.</summary>
    private static Task<string?> ResolveSlackAsync(WebhookAlertService webhooks, string destination)
    {
        var server = "SQL" + Interlocked.Increment(ref s_serverSequence);
        return webhooks.TrySendResolutionAsync(NotificationRouter.SlackChannel, destination, ResolutionFacts(server), server, genericDedupKey: null, Ct);
    }

    /// <summary>
    /// An alert that fired through a route is resolved after the operator turns the route off, and the resolution goes
    /// to the route's destination. Its outcome is that route's, enabled or not: the settings row's own destination is a
    /// different endpoint, so a failure there must not raise the settings row's count, and a delivery there must not
    /// reset it. The disabled route's entry is reported once, as turned off, with the count it reached.
    /// </summary>
    [Fact]
    public async Task AResolutionToADisabledRoutesDestination_CountsAgainstThatRoute_NotTheSettingsRow()
    {
        using var row = new CapturingWebhookEndpoint(statusCode: 500);
        using var routeBroken = new CapturingWebhookEndpoint(statusCode: 500);
        using var routeHealthy = new CapturingWebhookEndpoint(statusCode: 200);
        var settings = new FakeSlackSettings
        {
            SlackWebhookEnabled = true,
            SlackWebhookUrl = row.Url,
            NotificationRoutes = new[]
            {
                SlackRoute(routeBroken.Url, enabled: false),
                new NotificationRoute(RouteId: 2, MetricMatch: "Spare Metric", TeamsUrl: "", SlackUrl: routeHealthy.Url, GenericUrl: "",
                    PagerDutyRoutingKey: "", SmtpRecipients: "", Enabled: false),
            },
        };
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);

        /* The settings row's own destination is failing: two so far, under the bar. The routes are disabled, so the
           alerts resolve to the settings row. */
        await FailSlackAsync(webhooks, 2);
        Assert.Equal(2, row.Bodies.Count);

        for (var i = 0; i < 3; i++)
        {
            Assert.NotNull(await ResolveSlackAsync(webhooks, routeBroken.Url));
        }

        Assert.Equal(3, routeBroken.Bodies.Count);

        var counts = webhooks.GetChannelFailureCounts();
        var settingsRow = counts.Single(c => c.Channel == NotificationRouter.SlackChannel && c.RouteId is null);
        Assert.True(settingsRow.Configured);
        Assert.Equal(2, settingsRow.ConsecutiveFailures);
        var route = counts.Single(c => c.Channel == NotificationRouter.SlackChannel && c.RouteId == 1);
        Assert.Equal(3, route.ConsecutiveFailures);
        Assert.False(route.Configured);

        /* A delivery to the other disabled route's destination is that route's, not the settings row's recovery. */
        Assert.Null(await ResolveSlackAsync(webhooks, routeHealthy.Url));
        Assert.Single(routeHealthy.Bodies);
        Assert.Equal(2, webhooks.GetChannelFailureCounts().Single(c => c.Channel == NotificationRouter.SlackChannel && c.RouteId is null).ConsecutiveFailures);
        Assert.Equal(2, webhooks.GetSlackHealth().ConsecutiveFailures);
    }

    /// <summary>
    /// A resolution to a destination that neither the settings row nor any route names (a route's URL that was edited
    /// away between the fire and the clear) belongs to no configured channel. It is still sent, and its outcome is
    /// counted nowhere: not against the settings row, whose own destination it is not, and under no route's entry.
    /// </summary>
    [Fact]
    public async Task AResolutionToADestinationNoChannelNames_IsSentAndCountedNowhere()
    {
        using var row = new CapturingWebhookEndpoint(statusCode: 500);
        using var strayBroken = new CapturingWebhookEndpoint(statusCode: 500);
        using var strayHealthy = new CapturingWebhookEndpoint(statusCode: 200);
        var settings = new FakeSlackSettings
        {
            SlackWebhookEnabled = true,
            SlackWebhookUrl = row.Url,
            NotificationRoutes = new[] { SlackRoute("http://192.0.2.10/edited", enabled: true) },
        };
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);

        /* The settings row's own destination is failing: two so far, under the bar. "Spare Metric" matches no route. */
        for (var i = 0; i < 2; i++)
        {
            var server = "SQL" + Interlocked.Increment(ref s_serverSequence);
            Assert.Equal(AlertChannelOutcome.Failed, (await webhooks.TrySendWebhookAlertsAsync("Spare Metric", server, "1", "0", serverId: server)).Outcome);
        }

        for (var i = 0; i < 3; i++)
        {
            Assert.NotNull(await ResolveSlackAsync(webhooks, strayBroken.Url));
        }

        Assert.Null(await ResolveSlackAsync(webhooks, strayHealthy.Url));
        Assert.Equal(3, strayBroken.Bodies.Count);
        Assert.Single(strayHealthy.Bodies);

        var counts = webhooks.GetChannelFailureCounts();
        Assert.Equal(4, counts.Count);
        Assert.All(counts, c => Assert.Null(c.RouteId));
        var slack = counts.Single(c => c.Channel == NotificationRouter.SlackChannel);
        Assert.True(slack.Configured);
        Assert.Equal(2, slack.ConsecutiveFailures);
        Assert.Equal(2, webhooks.GetSlackHealth().ConsecutiveFailures);
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
        public bool SlackWebhookEnabled { get; set; } = true;
        public string SlackWebhookUrl { get; set; } = "";
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
        public IReadOnlyList<NotificationRoute> NotificationRoutes { get; set; } = Array.Empty<NotificationRoute>();
    }
}
