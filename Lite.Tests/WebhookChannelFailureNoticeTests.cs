/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins Lite's signal for a webhook channel that keeps failing (#4750). A channel can fail for weeks while
/// another one delivers every alert, and Lite has no self-alert evaluator, so the signal is a tray notice on
/// the status timer that runs the connection checks: one when a channel reaches three failures in a row, one
/// when it delivers again, none in between. The edge is the SHARED <see cref="WebhookChannelFailurePolicy"/>
/// (mirrored in Darling.Tests, the <see cref="ConnectionAlertPolicyTests"/> discipline), and the wording is
/// <see cref="WebhookChannelTrayNotice"/>, which names the channel and the count and never an error.
/// </summary>
public sealed class WebhookChannelFailureNoticeTests
{
    /// <summary>Feeds a channel's count history through the policy the way the window does: the caller's
    /// memory is set by a Failing notice and cleared by a Recovered one.</summary>
    private static List<WebhookChannelNotice> Replay(params int[] counts) =>
        ReplayLooks(counts.Select(c => (c, true)).ToArray());

    /// <summary>The same, over looks that carry whether the channel still has a destination: the caller's
    /// memory is cleared by either resolution.</summary>
    private static List<WebhookChannelNotice> ReplayLooks(params (int Count, bool Configured)[] looks)
    {
        var failing = false;
        var notices = new List<WebhookChannelNotice>();
        foreach (var (count, configured) in looks)
        {
            var notice = WebhookChannelFailurePolicy.Decide(failing, count, configured);
            if (notice == WebhookChannelNotice.Failing)
            {
                failing = true;
            }
            else if (notice is WebhookChannelNotice.Recovered or WebhookChannelNotice.TurnedOff)
            {
                failing = false;
            }

            notices.Add(notice);
        }

        return notices;
    }

    /// <summary>The window's loop for one channel: read the service, decide, remember. Returns what a look
    /// announced, with the wording Lite would show.</summary>
    private static (WebhookChannelNotice Notice, (string Title, string Message)? Text) Look(
        WebhookAlertService webhooks, ref bool failing)
    {
        var slack = webhooks.GetChannelFailureCounts().Single(c => c.Channel == NotificationRouter.SlackChannel);
        var notice = WebhookChannelFailurePolicy.Decide(failing, slack.ConsecutiveFailures, slack.Configured);
        if (notice != WebhookChannelNotice.None)
        {
            failing = notice == WebhookChannelNotice.Failing;
        }

        return (notice, WebhookChannelTrayNotice.For(notice, slack.Channel, slack.ConsecutiveFailures));
    }

    /// <summary>Fails the Slack channel by handing it a URL that is not one: the send throws, and the service
    /// counts it exactly as it counts a rejected post. Distinct servers, so no cooldown stands in the way.</summary>
    private static async Task FailSlackAsync(WebhookAlertService webhooks, int times)
    {
        for (var i = 0; i < times; i++)
        {
            var server = "SQL" + Interlocked.Increment(ref s_serverSequence);
            var sent = await webhooks.TrySendWebhookAlertsAsync("High CPU", server, "97%", "90%");
            Assert.Equal(AlertChannelOutcome.Failed, sent.Outcome);
        }
    }

    private static int s_serverSequence;

    /// <summary>Slack-only settings the test can change, which is how the user turns a channel off. The routes are
    /// empty unless a test sets them.</summary>
    private sealed class SlackSettings : IAlertSettings
    {
        public bool SlackWebhookEnabled { get; set; } = true;
        public string SlackWebhookUrl { get; set; } = "not-a-webhook-url";
        public IReadOnlyList<NotificationRoute> NotificationRoutes { get; set; } = Array.Empty<NotificationRoute>();

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
        public double AnalysisNotifySeverity => 0.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 10;
        public string TriageBaseUrl => "";
    }

    private static WebhookAlertService Service(SlackSettings settings) =>
        new(settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());

    [Fact]
    public async Task AFailingChannelWhoseUrlIsRemoved_IsToldOnceAsTurnedOff_WithTheTurnedOffText()
    {
        var settings = new SlackSettings();
        var webhooks = Service(settings);
        var failing = false;

        await FailSlackAsync(webhooks, 3);
        Assert.Equal(WebhookChannelNotice.Failing, Look(webhooks, ref failing).Notice);

        settings.SlackWebhookUrl = "";
        var (notice, text) = Look(webhooks, ref failing);

        Assert.Equal(WebhookChannelNotice.TurnedOff, notice);
        Assert.NotNull(text);
        Assert.Equal("Notification Channel Turned Off", text!.Value.Title);
        Assert.Contains("Slack", text.Value.Message, StringComparison.Ordinal);
        Assert.Contains("turned off (no destination is configured for it)", text.Value.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("delivering again", text.Value.Message, StringComparison.Ordinal);
        Assert.False(failing);

        Assert.Equal(WebhookChannelNotice.None, Look(webhooks, ref failing).Notice);
    }

    [Fact]
    public async Task AFailingChannelThatIsDisabled_IsToldTheSameWay()
    {
        var settings = new SlackSettings();
        var webhooks = Service(settings);
        var failing = false;

        await FailSlackAsync(webhooks, 5);
        Assert.Equal(WebhookChannelNotice.Failing, Look(webhooks, ref failing).Notice);

        settings.SlackWebhookEnabled = false;
        var (notice, text) = Look(webhooks, ref failing);

        Assert.Equal(WebhookChannelNotice.TurnedOff, notice);
        Assert.Equal("Notification Channel Turned Off", text!.Value.Title);
        Assert.Equal(WebhookChannelNotice.None, Look(webhooks, ref failing).Notice);
    }

    [Fact]
    public void AChannelThatDeliversAgain_IsToldSo_NotThatItWasTurnedOff()
    {
        var recovered = WebhookChannelTrayNotice.For(WebhookChannelNotice.Recovered, "Slack", 0);

        Assert.Equal("Notification Channel Recovered", recovered!.Value.Title);
        Assert.Equal("The Slack webhook is delivering again.", recovered.Value.Message);
        Assert.DoesNotContain("turned off", recovered.Value.Message, StringComparison.Ordinal);
        Assert.Equal(WebhookChannelNotice.Recovered, ReplayLooks((3, true), (0, true))[1]);
    }

    /// <summary>The reset is what keeps a channel the user turns back on from being announced at once on the
    /// count it had when it was turned off.</summary>
    [Fact]
    public async Task AChannelTurnedBackOn_StartsFromZero_NoFailingUntilThreeNewFailures()
    {
        var settings = new SlackSettings();
        var webhooks = Service(settings);
        var failing = false;

        await FailSlackAsync(webhooks, 3);
        Assert.Equal(WebhookChannelNotice.Failing, Look(webhooks, ref failing).Notice);
        var url = settings.SlackWebhookUrl;
        settings.SlackWebhookUrl = "";
        Assert.Equal(WebhookChannelNotice.TurnedOff, Look(webhooks, ref failing).Notice);
        Assert.Equal(0, webhooks.GetSlackHealth().ConsecutiveFailures);

        settings.SlackWebhookUrl = url;
        Assert.Equal(WebhookChannelNotice.None, Look(webhooks, ref failing).Notice);

        await FailSlackAsync(webhooks, 2);
        Assert.Equal(WebhookChannelNotice.None, Look(webhooks, ref failing).Notice);

        await FailSlackAsync(webhooks, 1);
        Assert.Equal(WebhookChannelNotice.Failing, Look(webhooks, ref failing).Notice);
    }

    [Fact]
    public async Task TheServiceReportsWhichChannelsAreConfigured_AndClearsTheCountOfAnUnconfiguredOne()
    {
        var settings = new SlackSettings();
        var webhooks = Service(settings);
        await FailSlackAsync(webhooks, 2);

        var before = webhooks.GetChannelFailureCounts().ToDictionary(c => c.Channel);
        Assert.True(before[NotificationRouter.SlackChannel].Configured);
        Assert.Equal(2, before[NotificationRouter.SlackChannel].ConsecutiveFailures);
        Assert.False(before[NotificationRouter.TeamsChannel].Configured);
        Assert.False(before[NotificationRouter.GenericChannel].Configured);
        Assert.False(before[NotificationRouter.PagerDutyChannel].Configured);

        settings.SlackWebhookEnabled = false;
        var first = webhooks.GetChannelFailureCounts().Single(c => c.Channel == NotificationRouter.SlackChannel);
        Assert.False(first.Configured);
        Assert.Equal(2, first.ConsecutiveFailures);
        var second = webhooks.GetChannelFailureCounts().Single(c => c.Channel == NotificationRouter.SlackChannel);
        Assert.Equal(0, second.ConsecutiveFailures);
    }

    [Theory]
    [InlineData(false, 0, true, WebhookChannelNotice.None)]
    [InlineData(false, 2, true, WebhookChannelNotice.None)]
    [InlineData(false, 3, true, WebhookChannelNotice.Failing)]
    [InlineData(false, 99, true, WebhookChannelNotice.Failing)]
    [InlineData(true, 99, true, WebhookChannelNotice.None)]
    [InlineData(true, 2, true, WebhookChannelNotice.None)]
    [InlineData(true, 0, true, WebhookChannelNotice.Recovered)]
    [InlineData(false, 0, false, WebhookChannelNotice.None)]
    [InlineData(false, 3, false, WebhookChannelNotice.None)]
    [InlineData(false, 99, false, WebhookChannelNotice.None)]
    [InlineData(true, 99, false, WebhookChannelNotice.TurnedOff)]
    [InlineData(true, 3, false, WebhookChannelNotice.TurnedOff)]
    [InlineData(true, 0, false, WebhookChannelNotice.TurnedOff)]
    public void EachState_CountAndConfiguration_HasOneAnswer(bool wasFailing, int count, bool configured, WebhookChannelNotice expected)
    {
        Assert.Equal(expected, WebhookChannelFailurePolicy.Decide(wasFailing, count, configured));
    }

    [Fact]
    public void ACountHistory_NoticesAtThreeAndAtRecovery_AndNothingInBetween()
    {
        var notices = Replay(0, 1, 2, 3, 4, 5, 50, 0, 0);

        Assert.Equal(
            new[]
            {
                WebhookChannelNotice.None, WebhookChannelNotice.None, WebhookChannelNotice.None,
                WebhookChannelNotice.Failing, WebhookChannelNotice.None, WebhookChannelNotice.None,
                WebhookChannelNotice.None, WebhookChannelNotice.Recovered, WebhookChannelNotice.None,
            },
            notices);
    }

    [Fact]
    public void ACountThatNeverReachesThree_IsNeverAnnounced()
    {
        Assert.All(Replay(0, 1, 2, 1, 0, 2, 2, 0), n => Assert.Equal(WebhookChannelNotice.None, n));
    }

    [Fact]
    public void AChannelAlreadyPastThreeAtTheFirstLook_IsAnnouncedOnce()
    {
        var notices = Replay(7, 8, 9);

        Assert.Equal(
            new[] { WebhookChannelNotice.Failing, WebhookChannelNotice.None, WebhookChannelNotice.None },
            notices);
    }

    [Fact]
    public void AnAnnouncedChannelThatDeliveredAndFailedAgainBetweenLooks_HoldsUntilItIsBackAtZero()
    {
        /* 5 then 2: the channel delivered (count reset to 0) and failed twice more before the next look. That
           is neither a recovery nor a new failure, so nothing is announced until a look finds it at 0. */
        var notices = Replay(3, 5, 2, 0, 3);

        Assert.Equal(
            new[]
            {
                WebhookChannelNotice.Failing, WebhookChannelNotice.None, WebhookChannelNotice.None,
                WebhookChannelNotice.Recovered, WebhookChannelNotice.Failing,
            },
            notices);
    }

    [Fact]
    public void TheTrayNotice_NamesTheChannelAndTheCount_AndSaysNothingWhenThereIsNothingToSay()
    {
        var failing = WebhookChannelTrayNotice.For(WebhookChannelNotice.Failing, "Slack", 3);
        Assert.NotNull(failing);
        Assert.Equal("Notification Channel Failing", failing!.Value.Title);
        Assert.Contains("Slack", failing.Value.Message, StringComparison.Ordinal);
        Assert.Contains("3 times in a row", failing.Value.Message, StringComparison.Ordinal);

        var recovered = WebhookChannelTrayNotice.For(WebhookChannelNotice.Recovered, "Slack", 0);
        Assert.NotNull(recovered);
        Assert.Equal("Notification Channel Recovered", recovered!.Value.Title);
        Assert.Contains("Slack", recovered.Value.Message, StringComparison.Ordinal);

        var turnedOff = WebhookChannelTrayNotice.For(WebhookChannelNotice.TurnedOff, "Slack", 0);
        Assert.NotNull(turnedOff);
        Assert.Equal("Notification Channel Turned Off", turnedOff!.Value.Title);
        Assert.Equal("The Slack webhook was turned off (no destination is configured for it).", turnedOff.Value.Message);

        Assert.Null(WebhookChannelTrayNotice.For(WebhookChannelNotice.None, "Slack", 3));
    }

    [Fact]
    public void TheTrayNotice_TakesNoErrorText_SoNoWebhookUrlCanReachIt()
    {
        /* The builder's whole input is the notice kind, the channel name and the count: there is no parameter
           an error string could arrive through. */
        var parameters = typeof(WebhookChannelTrayNotice)
            .GetMethod("For", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public
                | System.Reflection.BindingFlags.Static)!
            .GetParameters();

        Assert.Equal(
            new[] { typeof(WebhookChannelNotice), typeof(string), typeof(int) },
            parameters.Select(p => p.ParameterType).ToArray());
    }

    /// <summary>The window wires the check onto the status timer that runs the connection checks, reads the
    /// counts from the webhook service it built, and hands them with its memory to <see cref="WebhookChannelCheck"/>,
    /// which decides with the shared policy. A WPF window cannot be driven from a unit test, so the wiring is pinned
    /// from source; the deciding is pinned by behaviour below.</summary>
    [Fact]
    public void TheStatusTimer_RunsTheChannelCheck_RightAfterTheConnectionCheck()
    {
        var window = ReadRepoFile(Path.Combine("Lite", "MainWindow.xaml.cs"));
        var check = ReadRepoFile(Path.Combine("Lite", "Services", "WebhookChannelCheck.cs"));

        Assert.Contains(
            "CheckConnectionsAndNotify();\r\n            CheckWebhookChannelsAndNotify();",
            window.Replace("\r\n", "\n").Replace("\n", "\r\n"), StringComparison.Ordinal);
        Assert.Contains("_webhookAlertService.GetChannelFailureCounts()", window, StringComparison.Ordinal);
        Assert.Contains("WebhookChannelCheck.Look(_webhookAlertService.GetChannelFailureCounts(), _webhookChannelFailing)", window, StringComparison.Ordinal);
        Assert.Contains("WebhookChannelFailurePolicy.Decide(wasFailing, destination.ConsecutiveFailures, destination.Configured)", check, StringComparison.Ordinal);
        Assert.Contains("WebhookChannelTrayNotice.For(", window, StringComparison.Ordinal);
    }

    /// <summary>
    /// The window's loop, as the helper it calls: three Slack destinations (the settings row's and two routes') are
    /// remembered apart by Key. The failing route is announced once with its Label, the healthy destinations say
    /// nothing, a later look at the same state says nothing again, and the route's destination going away closes it as
    /// turned off, naming the route, and forgets it.
    /// </summary>
    [Fact]
    public void TheWindowsLook_RemembersEachDestinationByKey_AndNamesTheRoute()
    {
        var memory = new Dictionary<string, bool>(StringComparer.Ordinal);
        WebhookChannelFailureCount SettingsRow(int count) => new(NotificationRouter.SlackChannel, count, Configured: true);
        WebhookChannelFailureCount Route(int id, int count, bool configured = true) => new(NotificationRouter.SlackChannel, count, configured, RouteId: id);

        var first = WebhookChannelCheck.Look(new[] { SettingsRow(0), Route(1, 3), Route(2, 0) }, memory);

        var failing = Assert.Single(first);
        Assert.Equal(WebhookChannelNotice.Failing, failing.Notice);
        Assert.Equal("Slack (route 1)", failing.Label);
        Assert.Equal(3, failing.ConsecutiveFailures);
        Assert.True(memory["Slack:route:1"]);
        Assert.False(memory.ContainsKey("Slack"));
        Assert.False(memory.ContainsKey("Slack:route:2"));

        /* The same state, further along: an edge is not announced twice, and a delivery on route 2 is not route 1's recovery. */
        Assert.Empty(WebhookChannelCheck.Look(new[] { SettingsRow(0), Route(1, 5), Route(2, 0) }, memory));
        Assert.True(memory["Slack:route:1"]);

        /* Route 1 is disabled: the service reports its destination once more, unconfigured, and the look closes it as turned off. */
        var closed = WebhookChannelCheck.Look(new[] { SettingsRow(0), Route(1, 5, configured: false), Route(2, 0) }, memory);

        var turnedOff = Assert.Single(closed);
        Assert.Equal(WebhookChannelNotice.TurnedOff, turnedOff.Notice);
        Assert.Equal("Slack (route 1)", turnedOff.Label);
        Assert.False(memory.ContainsKey("Slack:route:1"));

        /* Forgotten: the destination is no longer listed, and nothing is said about it again. */
        Assert.Empty(WebhookChannelCheck.Look(new[] { SettingsRow(0), Route(2, 0) }, memory));
        Assert.Empty(memory);
    }

    /// <summary>A destination that recovers is told so and forgotten, and the settings row's own destination is keyed
    /// by the channel name alone, as it was before routes had entries of their own.</summary>
    [Fact]
    public void TheWindowsLook_ClosesARecoveredDestination_AndKeysTheSettingsRowByChannel()
    {
        var memory = new Dictionary<string, bool>(StringComparer.Ordinal);

        var failing = Assert.Single(WebhookChannelCheck.Look(new[] { new WebhookChannelFailureCount(NotificationRouter.SlackChannel, 3, Configured: true) }, memory));
        Assert.Equal(WebhookChannelNotice.Failing, failing.Notice);
        Assert.Equal("Slack", failing.Label);
        Assert.True(memory["Slack"]);

        var recovered = Assert.Single(WebhookChannelCheck.Look(new[] { new WebhookChannelFailureCount(NotificationRouter.SlackChannel, 0, Configured: true) }, memory));
        Assert.Equal(WebhookChannelNotice.Recovered, recovered.Notice);
        Assert.Equal("Slack", recovered.Label);
        Assert.Empty(memory);
    }

    /// <summary>
    /// Lite shares the service, so two routes that point Slack at two destinations are counted apart here too: a
    /// delivery to the healthy destination is not a recovery of the broken one, whose count climbs to the threshold
    /// and is announced.
    /// </summary>
    [Fact]
    public async Task TwoRoutesOnOneChannel_TheBrokenDestinationKeepsCounting_WhileTheOtherDelivers()
    {
        using var healthy = new OkEndpoint();
        var settings = new SlackSettings
        {
            SlackWebhookEnabled = false,
            SlackWebhookUrl = "",
            NotificationRoutes = new[]
            {
                new NotificationRoute(RouteId: 1, MetricMatch: "High CPU", TeamsUrl: "", SlackUrl: "not-a-webhook-url", GenericUrl: "",
                    PagerDutyRoutingKey: "", SmtpRecipients: "", Enabled: true),
                new NotificationRoute(RouteId: 2, MetricMatch: "Spare Metric", TeamsUrl: "", SlackUrl: healthy.Url, GenericUrl: "",
                    PagerDutyRoutingKey: "", SmtpRecipients: "", Enabled: true),
            },
        };
        var webhooks = Service(settings);

        /* A failure on route 1 and a delivery on route 2, three times over. Distinct servers, by name and id, so neither the
           per-server cooldown a delivery stamps nor anything else stands between a destination and its next post. */
        for (var i = 0; i < 3; i++)
        {
            var server = "SQL" + Interlocked.Increment(ref s_serverSequence);
            Assert.Equal(AlertChannelOutcome.Failed, (await webhooks.TrySendWebhookAlertsAsync("High CPU", server, "97%", "90%", serverId: server)).Outcome);
            Assert.Equal(AlertChannelOutcome.Delivered, (await webhooks.TrySendWebhookAlertsAsync("Spare Metric", server, "1", "0", serverId: server)).Outcome);
        }

        var slack = webhooks.GetChannelFailureCounts().Where(c => c.Channel == NotificationRouter.SlackChannel).ToList();
        var broken = slack.OrderByDescending(c => c.ConsecutiveFailures).First();
        Assert.Equal(3, broken.ConsecutiveFailures);
        Assert.True(broken.Configured);
        Assert.Contains(slack, c => c.ConsecutiveFailures == 0 && c.Configured);
        Assert.Equal(WebhookChannelNotice.Failing, WebhookChannelFailurePolicy.Decide(false, broken.ConsecutiveFailures, broken.Configured));
    }

    /// <summary>A loopback endpoint that answers every post with 200, so a route pointed at it delivers.</summary>
    private sealed class OkEndpoint : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();

        public OkEndpoint()
        {
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _ = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    await DrainRequestAsync(stream);
                    await stream.WriteAsync(Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"), _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
        }

        /// <summary>Reads the headers and the Content-Length body, so the whole request is accepted before the reply.</summary>
        private static async Task DrainRequestAsync(NetworkStream stream)
        {
            var buffer = new byte[16 * 1024];
            var received = new List<byte>();
            int headerEnd;
            while ((headerEnd = Encoding.ASCII.GetString(received.ToArray()).IndexOf("\r\n\r\n", StringComparison.Ordinal)) < 0)
            {
                var read = await stream.ReadAsync(buffer);
                if (read == 0) return;
                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }

            var length = 0;
            foreach (var line in Encoding.ASCII.GetString(received.ToArray(), 0, headerEnd).Split("\r\n"))
            {
                if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
                {
                    length = int.Parse(line["Content-Length:".Length..].Trim(), System.Globalization.CultureInfo.InvariantCulture);
                }
            }

            while (received.Count - (headerEnd + 4) < length)
            {
                var read = await stream.ReadAsync(buffer);
                if (read == 0) return;
                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            _stop.Dispose();
        }
    }

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
