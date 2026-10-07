/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5366: one webhook delivery reads its URLs, headers and proxies from one snapshot. A saved setting that changes while
/// the delivery is in flight does not change where the later channels of that delivery go.
/// </summary>
public sealed class WebhookDeliverySnapshotTests
{
    [Fact]
    public async Task AProxyChangedWhileTheFirstChannelPosts_DoesNotMoveTheNextChannelOfTheSameDelivery()
    {
        var config = new DarlingConfig();
        using var teamsProxy = new LoopbackProxy();
        using var slackProxy = new LoopbackProxy();
        config.Webhooks.TeamsUrl = "http://teams.example.test/hook";
        config.Webhooks.TeamsProxy = teamsProxy.Url;
        config.Webhooks.SlackUrl = "http://slack.example.test/hook";
        config.Webhooks.SlackProxy = slackProxy.Url;

        /* The Teams post is in flight when the saved Slack proxy changes to an address nothing listens on. */
        teamsProxy.OnRequest = () => config.Webhooks.SlackProxy = "http://127.0.0.1:1";

        var service = new WebhookAlertService(
            new DarlingAlertSettings(config), DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);

        var result = await service.TrySendWebhookAlertsAsync(
            "Deadlocks Detected", "server-alpha", "3", "1", cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(1, teamsProxy.Requests);
        Assert.Equal(1, slackProxy.Requests);
        Assert.Equal(AlertChannelOutcome.Delivered, result.Outcome);
    }

    [Fact]
    public void ASnapshot_KeepsTheProxiesAndHeadersItWasTakenWith()
    {
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = "http://teams.example.test/hook";
        config.Webhooks.TeamsProxy = "http://127.0.0.1:2001";
        config.Webhooks.SlackProxy = "http://127.0.0.1:2002";
        config.Webhooks.GenericUrl = "http://generic.example.test/hook";
        config.Webhooks.GenericProxy = "http://127.0.0.1:2003";
        config.Webhooks.GenericHeaders = "{\"X-Fake-Key\":\"not-real-value\"}";
        config.Webhooks.PagerDutyProxy = "http://127.0.0.1:2004";
        var settings = new DarlingAlertSettings(config);

        var snapshot = ((IAlertSettings)settings).SnapshotForDelivery();

        config.Webhooks.TeamsProxy = "http://127.0.0.1:3001";
        config.Webhooks.SlackProxy = "http://127.0.0.1:3002";
        config.Webhooks.GenericProxy = "http://127.0.0.1:3003";
        config.Webhooks.GenericHeaders = "{\"X-Fake-Key\":\"changed\"}";
        config.Webhooks.PagerDutyProxy = "http://127.0.0.1:3004";

        Assert.Equal("http://127.0.0.1:2001", snapshot.TeamsProxyAddress);
        Assert.Equal("http://127.0.0.1:2002", snapshot.SlackProxyAddress);
        Assert.Equal("http://127.0.0.1:2003", snapshot.GenericWebhookProxyAddress);
        Assert.Equal("http://127.0.0.1:2004", snapshot.PagerDutyProxyAddress);
        Assert.Contains("not-real-value", snapshot.GenericWebhookHeadersJson, StringComparison.Ordinal);
        Assert.Equal("http://127.0.0.1:3001", settings.TeamsProxyAddress);
    }

    /// <summary>A loopback "proxy" that answers every request 200 and can run a callback while the request is in flight.</summary>
    private sealed class LoopbackProxy : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _accepting;
        private int _requests;

        public LoopbackProxy()
        {
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";
            _accepting = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        public int Requests => Volatile.Read(ref _requests);

        public Action? OnRequest { get; set; }

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    var received = new StringBuilder();
                    var buffer = new byte[16 * 1024];
                    while (!received.ToString().Contains("\r\n\r\n", StringComparison.Ordinal))
                    {
                        var read = await stream.ReadAsync(buffer, _stop.Token);
                        if (read == 0)
                        {
                            break;
                        }

                        received.Append(Encoding.ASCII.GetString(buffer, 0, read));
                    }

                    Interlocked.Increment(ref _requests);
                    OnRequest?.Invoke();

                    var response = Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    await stream.WriteAsync(response, _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
            catch (System.IO.IOException) { /* client went away */ }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            try
            {
                _accepting.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException) { /* the cancellation above */ }

            _stop.Dispose();
        }
    }
}
