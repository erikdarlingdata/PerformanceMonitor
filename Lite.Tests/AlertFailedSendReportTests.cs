/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Threading;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4752, Lite's half, end to end: <see cref="LiteAlertDeliverer.DeliverAndReportAsync"/> over the real
/// <c>EmailAlertService</c> and webhook sender, posting to loopback endpoints that answer with a fixed status.
/// The seam-level pins in <c>LiteAlertForwardingTests</c> use hand-built deliveries; these pin that the deliveries
/// Lite's real send produces are the ones the engine's retry reads: every configured channel failing is "not
/// sent", one channel delivering is "sent", and a Lite with no email or webhook configured is neither.
/// </summary>
public sealed class AlertFailedSendReportTests
{
    [Fact]
    public async Task EveryConfiguredChannelFailing_IsReportedAsNotSent()
    {
        using var endpoint = new StatusEndpoint(500);
        using var rig = new Rig(new AlertWebhookCancelTests.GenericWebhookSettings(endpoint.Url));

        var delivery = await rig.Deliverer.DeliverAndReportAsync(Outcome());

        Assert.NotNull(delivery);
        Assert.True(FailedSendBackoff.EveryChannelFailed(delivery));
        Assert.False(delivery.Sent);
        Assert.NotNull(delivery.SendError);
        Assert.Equal(1, endpoint.Posts);

        /* The report is the row's own disposition, not a second opinion about it. */
        Assert.Equal(delivery, Assert.Single(rig.History.Records).Delivery);
    }

    [Fact]
    public async Task OneChannelDelivering_AndAnotherFailing_IsReportedAsSent()
    {
        using var delivering = new StatusEndpoint(200);
        using var failing = new StatusEndpoint(500);
        using var rig = new Rig(new AlertWebhookCancelTests.GenericWebhookSettings(delivering.Url, teamsUrl: failing.Url));

        var delivery = await rig.Deliverer.DeliverAndReportAsync(Outcome());

        Assert.NotNull(delivery);
        Assert.True(delivery.Sent);
        Assert.False(FailedSendBackoff.EveryChannelFailed(delivery));
        Assert.Equal(1, delivering.Posts);
        Assert.Equal(1, failing.Posts);
    }

    [Fact]
    public async Task NoExternalChannelConfigured_IsNotAFailedSend()
    {
        using var rig = new Rig(new AlertWebhookCancelTests.GenericWebhookSettings(""));

        var delivery = await rig.Deliverer.DeliverAndReportAsync(Outcome());

        Assert.NotNull(delivery);
        Assert.False(FailedSendBackoff.EveryChannelFailed(delivery));
        Assert.Null(delivery.SendError);
        Assert.Single(rig.History.Records);
    }

    [Fact]
    public async Task ACancelledCaller_LeavesAsTheCancellation_AndWritesNoHistoryRow()
    {
        using var endpoint = new StatusEndpoint(500);
        using var rig = new Rig(new AlertWebhookCancelTests.GenericWebhookSettings(endpoint.Url));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => rig.Deliverer.DeliverAndReportAsync(Outcome(), cts.Token));

        Assert.Empty(rig.History.Records);
    }

    private static AlertOutcome Outcome() =>
        new("1", "SQL01", "High CPU", "97%", "90%", null, "detail", 97, 90, false, null,
            "Total CPU at 97% (threshold: 90%)");

    /// <summary>The deliverer over the real email and webhook services and a history store that keeps its rows.</summary>
    private sealed class Rig : IDisposable
    {
        private readonly string _configDir;

        public Rig(IAlertSettings settings)
        {
            _configDir = Path.Combine(Path.GetTempPath(), "pmlite-failedsend-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_configDir);

            History = new AlertWebhookCancelTests.CapturingHistoryStore();
            var webhooks = new WebhookAlertService(
                settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());
            var emailAlerts = new EmailAlertService(
                settings, History, webhooks, new AppLoggerAdapter<EmailAlertService>());
            Deliverer = new LiteAlertDeliverer(
                emailAlerts,
                new MuteRuleService(new AlertWebhookCancelTests.NoMuteRules(), new AppLoggerAdapter<MuteRuleService>()),
                new ServerManager(_configDir),
                () => null,
                Dispatcher.CurrentDispatcher);
        }

        public LiteAlertDeliverer Deliverer { get; }

        public AlertWebhookCancelTests.CapturingHistoryStore History { get; }

        public void Dispose() => Directory.Delete(_configDir, recursive: true);
    }

    /* A loopback listener that reads each request through its body and answers with a fixed status, which is what
       an endpoint that is up but refusing (an HTTP 429, a 500) looks like from the client's side. TcpListener
       rather than HttpListener because the latter wants a URL ACL on Windows. */
    private sealed class StatusEndpoint : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _accepting;
        private readonly int _status;
        private int _posts;

        public StatusEndpoint(int status)
        {
            _status = status;
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _accepting = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        public int Posts => Volatile.Read(ref _posts);

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    _ = Task.Run(() => AnswerAsync(client));
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
        }

        private async Task AnswerAsync(TcpClient client)
        {
            try
            {
                using (client)
                {
                    var stream = client.GetStream();
                    var received = new StringBuilder();
                    var buffer = new byte[4096];
                    var headerEnd = -1;
                    var bodyBytes = 0;

                    /* Headers first, then as many body bytes as Content-Length promises, so the client has
                       finished writing before the answer arrives. */
                    while (headerEnd < 0)
                    {
                        var read = await stream.ReadAsync(buffer, _stop.Token);
                        if (read == 0)
                        {
                            return;
                        }

                        received.Append(Encoding.ASCII.GetString(buffer, 0, read));
                        headerEnd = received.ToString().IndexOf("\r\n\r\n", StringComparison.Ordinal);
                        if (headerEnd >= 0)
                        {
                            bodyBytes = received.Length - (headerEnd + 4);
                        }
                    }

                    var length = 0;
                    foreach (var line in received.ToString(0, headerEnd).Split("\r\n"))
                    {
                        if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
                        {
                            length = int.Parse(line["Content-Length:".Length..].Trim(), System.Globalization.CultureInfo.InvariantCulture);
                        }
                    }

                    while (bodyBytes < length)
                    {
                        var read = await stream.ReadAsync(buffer, _stop.Token);
                        if (read == 0)
                        {
                            return;
                        }

                        bodyBytes += read;
                    }

                    Interlocked.Increment(ref _posts);
                    var response = Encoding.ASCII.GetBytes(
                        $"HTTP/1.1 {_status} Answer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    await stream.WriteAsync(response, _stop.Token);
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (IOException) { /* the client gave up */ }
            catch (ObjectDisposedException) { /* Dispose */ }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();

            try
            {
                _accepting.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException) { /* the loop's own stop */ }

            _stop.Dispose();
        }
    }
}
