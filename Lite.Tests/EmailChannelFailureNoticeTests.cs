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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins Lite's tray notice for an SMTP relay that keeps refusing alert email (#8245). The webhook destinations got this
/// signal in #4750: three failures in a row are announced once, and the next delivery closes the announcement. Email
/// failed the same way and announced nothing, because the status timer read only the webhook counts. The email core now
/// counts its sends on the same ledger the timer reads, as the Email entry, and the notice text names email rather than
/// a webhook. The failures here are the send refusing a recipient list before any connection, and the recovery is a
/// delivery to a loopback relay, so nothing waits on a network.
/// </summary>
public sealed class EmailChannelFailureNoticeTests
{
    /// <summary>SMTP-only settings the test can change, which is how the user turns the channel off or fixes it.</summary>
    private sealed class EmailSettings : IAlertSettings
    {
        public bool SmtpEnabled { get; set; } = true;
        public string SmtpServer { get; set; } = "127.0.0.1";
        public int SmtpPort { get; set; } = 25;
        public string SmtpRecipients { get; set; } = Refused;
        public IReadOnlyList<NotificationRoute> NotificationRoutes => Array.Empty<NotificationRoute>();

        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "monitor@example.invalid";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;
        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
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

    /// <summary>A recipient list the send refuses before it connects anywhere (two addresses joined by a semicolon), so a
    /// failure costs no network and no wait.</summary>
    private const string Refused = "ops@example.invalid;second@example.invalid";

    private const string Accepted = "ops@example.invalid";

    private sealed class NoHistory : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;
        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }

    /// <summary>A loopback relay that accepts every message, for the recovery.</summary>
    private sealed class LoopbackSmtp : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _accepting;

        public LoopbackSmtp()
        {
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _accepting = Task.Run(AcceptAsync);
        }

        public int Port { get; }

        public int Accepted => Volatile.Read(ref _accepted);

        private int _accepted;

        private async Task AcceptAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    using var reader = new StreamReader(stream, Encoding.UTF8, false, 8192, leaveOpen: true);
                    using var writer = new StreamWriter(stream, new UTF8Encoding(false), 8192, leaveOpen: true) { AutoFlush = true, NewLine = "\r\n" };
                    await writer.WriteLineAsync("220 localhost ESMTP test");
                    var inData = false;
                    string? line;
                    while ((line = await reader.ReadLineAsync(_stop.Token)) is not null)
                    {
                        if (inData)
                        {
                            if (line == ".")
                            {
                                inData = false;
                                Interlocked.Increment(ref _accepted);
                                await writer.WriteLineAsync("250 OK queued");
                            }

                            continue;
                        }

                        if (line.StartsWith("DATA", StringComparison.OrdinalIgnoreCase))
                        {
                            inData = true;
                            await writer.WriteLineAsync("354 End data with <CRLF>.<CRLF>");
                        }
                        else if (line.StartsWith("QUIT", StringComparison.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("221 Bye");
                            break;
                        }
                        else
                        {
                            await writer.WriteLineAsync("250 localhost");
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (IOException) { }
            catch (SocketException) { }
            catch (ObjectDisposedException) { }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            try
            {
                _accepting.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException) { }

            _stop.Dispose();
        }
    }

    /// <summary>The real webhook service and email core on SMTP-only settings, and the status timer's memory.</summary>
    private sealed class Rig
    {
        public EmailSettings Settings { get; } = new();
        public WebhookAlertService Webhooks { get; }
        public EmailSendCore Core { get; }
        public Dictionary<string, bool> Announced { get; } = new(StringComparer.Ordinal);

        public Rig()
        {
            Webhooks = new WebhookAlertService(Settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());
            Core = new EmailSendCore(Settings, new NoHistory(), Webhooks, EmailAlertService.Branding, new AppLoggerAdapter<EmailSendCore>());
        }

        /// <summary>One alert email from a distinct server, so no cooldown stands between two sends.</summary>
        public async Task<AlertChannelOutcome> SendAsync()
        {
            var server = "SQL" + Interlocked.Increment(ref s_serverSequence);
            var result = await Core.TrySendAsync("High CPU", server, "97%", "90%", serverId: server, context: null, attemptChannels: true);
            return result.EmailOutcome;
        }

        public async Task FailAsync(int times)
        {
            for (var i = 0; i < times; i++)
            {
                Assert.Equal(AlertChannelOutcome.Failed, await SendAsync());
            }
        }

        /// <summary>The status timer's look: every announcement this observation makes, with the text the tray would show.</summary>
        public List<(WebhookChannelCheck.Announcement Announcement, (string Title, string Message)? Text)> Look() =>
            WebhookChannelCheck.Look(Webhooks.GetChannelFailureCounts(), Announced)
                .Select(a => (a, WebhookChannelTrayNotice.For(a.Notice, a.Label, a.ConsecutiveFailures)))
                .ToList();
    }

    private static int s_serverSequence;

    [Fact]
    public async Task ThreeRefusedEmails_AreAnnouncedOnceAsEmailFailing_AndADeliveryAnnouncesTheRecovery()
    {
        using var relay = new LoopbackSmtp();
        var rig = new Rig();

        await rig.FailAsync(2);
        Assert.Empty(rig.Look());

        await rig.FailAsync(1);
        var (failing, failingText) = Assert.Single(rig.Look());
        Assert.Equal(WebhookChannelNotice.Failing, failing.Notice);
        Assert.Equal(NotificationRouter.EmailChannel, failing.Label);
        Assert.Equal(3, failing.ConsecutiveFailures);
        Assert.Equal("Notification Channel Failing", failingText!.Value.Title);
        Assert.Contains("email", failingText.Value.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("3 times in a row", failingText.Value.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("webhook", failingText.Value.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("example.invalid", failingText.Value.Message, StringComparison.Ordinal);

        /* Still failing: nothing more is said, however far the count climbs. */
        await rig.FailAsync(2);
        Assert.Empty(rig.Look());

        rig.Settings.SmtpRecipients = Accepted;
        rig.Settings.SmtpPort = relay.Port;
        Assert.Equal(AlertChannelOutcome.Delivered, await rig.SendAsync());
        Assert.Equal(1, relay.Accepted);

        var (recovered, recoveredText) = Assert.Single(rig.Look());
        Assert.Equal(WebhookChannelNotice.Recovered, recovered.Notice);
        Assert.Equal(NotificationRouter.EmailChannel, recovered.Label);
        Assert.Equal("Notification Channel Recovered", recoveredText!.Value.Title);
        Assert.Equal("Alert email is delivering again.", recoveredText.Value.Message);

        Assert.Empty(rig.Look());
    }

    [Fact]
    public async Task TurningSmtpOffWhileFailing_ClosesTheAnnouncement_AndTheCountStartsOverWhenItIsBackOn()
    {
        var rig = new Rig();

        await rig.FailAsync(3);
        Assert.Equal(WebhookChannelNotice.Failing, Assert.Single(rig.Look()).Announcement.Notice);

        rig.Settings.SmtpEnabled = false;
        var (turnedOff, text) = Assert.Single(rig.Look());
        Assert.Equal(WebhookChannelNotice.TurnedOff, turnedOff.Notice);
        Assert.Equal(NotificationRouter.EmailChannel, turnedOff.Label);
        Assert.Equal("Notification Channel Turned Off", text!.Value.Title);
        Assert.Contains("email", text.Value.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("webhook", text.Value.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(rig.Look());

        rig.Settings.SmtpEnabled = true;
        Assert.Equal(0, rig.Webhooks.GetChannelFailureCounts().Single(c => c.Channel == NotificationRouter.EmailChannel).ConsecutiveFailures);
        await rig.FailAsync(2);
        Assert.Empty(rig.Look());
        await rig.FailAsync(1);
        Assert.Equal(WebhookChannelNotice.Failing, Assert.Single(rig.Look()).Announcement.Notice);
    }
}
