/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4752: a webhook post is bounded by its own timeout and by the caller's token. Before, each of the four
/// channels awaited a post of up to 30 seconds with no way to cancel it, so one endpoint that accepted the
/// connection and never answered held the whole alert delivery for that long, once per channel, and a service
/// stop waited it out. These tests post to a loopback endpoint that accepts and never answers.
/// </summary>
public sealed class WebhookPostTimeoutTests
{
    /* Well above anything the bounded paths need (a 200 ms timeout, an immediate cancel) and well below the
       30 seconds the unbounded post took, so a pass cannot be the client's own timeout ending the call. */
    private static readonly TimeSpan PromptBound = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The hung-endpoint case at the post itself: the timeout ends the call, the result is an error naming
    /// the timeout, and it comes back at the bound rather than at the client's 30 seconds.
    /// </summary>
    [Fact]
    public async Task AHungEndpoint_EndsThePostAsATimeoutError_AtTheBound()
    {
        using var endpoint = new HungWebhookEndpoint();
        var stopwatch = Stopwatch.StartNew();

        var error = await WebhookAlertService
            .PostWebhookAsync(endpoint.Url, "{}", proxyAddress: null, headers: null, TimeSpan.FromMilliseconds(200), CancellationToken.None)
            .WaitAsync(PromptBound);

        stopwatch.Stop();
        Assert.NotNull(error);
        Assert.Contains("timed out", error, StringComparison.Ordinal);
        Assert.True(stopwatch.Elapsed < PromptBound, $"the post took {stopwatch.Elapsed}");
    }

    /// <summary>
    /// A cancelled CALLER token is not a timeout: it propagates as the cancellation it is, and it does so
    /// while the post is still hanging. The timeout here is long, so a pass is the token's doing.
    /// </summary>
    [Fact]
    public async Task ACallersCancel_EndsAHangingPost_AsACancellation()
    {
        using var endpoint = new HungWebhookEndpoint();
        using var cts = new CancellationTokenSource();

        var post = WebhookAlertService.PostWebhookAsync(
            endpoint.Url, "{}", proxyAddress: null, headers: null, TimeSpan.FromSeconds(60), cts.Token);

        await endpoint.Connected.WaitAsync(PromptBound);
        Assert.False(post.IsCompleted);

        var stopwatch = Stopwatch.StartNew();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => post.WaitAsync(PromptBound));
        Assert.True(stopwatch.Elapsed < PromptBound, $"the cancel took {stopwatch.Elapsed}");
    }

    /// <summary>
    /// A caller's cancel is a stop request, not a failed delivery. A token that is already cancelled reaches
    /// the channel's post, and the cancellation comes back out of the fan-out as the cancellation it is:
    /// there is no result that could record the channel as failed, and the channel's failure count does not
    /// move, because the endpoint was never at fault.
    /// </summary>
    [Fact]
    public async Task AnAlreadyCancelledToken_ThrowsTheCancellation_WithTheChannelsFailureCountUnchanged()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        var webhooks = GenericOnlyService(endpoint.Url);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => webhooks
            .TrySendWebhookAlertsAsync("High CPU", "SQL01", "97%", "90%", cancellationToken: cts.Token)
            .WaitAsync(PromptBound));

        Assert.Equal((0, (string?)null), webhooks.GetGenericHealth());
        Assert.Empty(endpoint.Bodies);
    }

    /// <summary>
    /// The same for a cancel that arrives while the post is in flight, which is a service stopping in the
    /// middle of a delivery: the cancellation reaches the caller promptly, and the channel's failure count
    /// and last error stay as they were.
    /// </summary>
    [Fact]
    public async Task ACancelDuringAPost_ThrowsTheCancellation_WithTheChannelsFailureCountUnchanged()
    {
        using var endpoint = new HungWebhookEndpoint();
        var webhooks = GenericOnlyService(endpoint.Url);
        using var cts = new CancellationTokenSource();

        var fanout = webhooks.TrySendWebhookAlertsAsync(
            "High CPU", "SQL01", "97%", "90%", cancellationToken: cts.Token);

        await endpoint.Connected.WaitAsync(PromptBound);
        Assert.False(fanout.IsCompleted);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fanout.WaitAsync(PromptBound));

        Assert.Equal((0, (string?)null), webhooks.GetGenericHealth());
    }

    /// <summary>
    /// The deliverer's shutdown path, reached through a real post. <see cref="DarlingAlertDeliverer.DeliverAndReportAsync"/>
    /// lets the caller's own cancellation through and writes no history row for it, because a delivery the
    /// service abandoned is not a delivery that failed. The channel's send used to swallow the cancel into a
    /// Failed outcome, so the deliverer never saw it and wrote a "failed" row for a stop.
    /// </summary>
    [Fact]
    public async Task ACancelInTheMiddleOfAPost_LeavesTheDelivererWithTheCancellation_AndWritesNoHistoryRow()
    {
        using var endpoint = new HungWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = endpoint.Url;
        var settings = new DarlingAlertSettings(config);
        var history = new CapturingHistoryStore();
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);
        using var cts = new CancellationTokenSource();

        var delivery = deliverer.DeliverAndReportAsync(
            new AlertOutcome(
                "sql01", "SQL01", "High CPU", "97%", "90%",
                Context: null, DetailText: null, NumericCurrentValue: 97, NumericThresholdValue: 90,
                Muted: false, Severity: AlertSeverityLevel.Warning),
            cts.Token);

        await endpoint.Connected.WaitAsync(PromptBound);
        Assert.False(delivery.IsCompleted);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => delivery.WaitAsync(PromptBound));

        Assert.Empty(history.Records);
        Assert.Equal((0, (string?)null), webhooks.GetGenericHealth());
    }

    /// <summary>
    /// The public path is bounded too: with no token at all, the fan-out's post to an endpoint that never
    /// answers gives up on its own at <see cref="WebhookAlertService.WebhookPostTimeout"/> and records the
    /// channel failed with the timeout as the reason. Takes the timeout's full length, so it is the one slow
    /// test here.
    /// </summary>
    [Fact]
    public async Task AHungEndpoint_GivesUpAtTheWebhookPostTimeout_ThroughTheFanout()
    {
        using var endpoint = new HungWebhookEndpoint();
        var webhooks = GenericOnlyService(endpoint.Url);
        var stopwatch = Stopwatch.StartNew();

        var result = await webhooks
            .TrySendWebhookAlertsAsync("High CPU", "SQL01", "97%", "90%")
            .WaitAsync(WebhookAlertService.WebhookPostTimeout + TimeSpan.FromSeconds(20));

        stopwatch.Stop();
        Assert.Equal(AlertChannelOutcome.Failed, result.Outcome);
        Assert.Equal(AlertChannelOutcome.Failed, result.ChannelOutcomes![NotificationRouter.GenericChannel]);
        Assert.Contains("timed out", result.SendError, StringComparison.Ordinal);
        Assert.True(
            stopwatch.Elapsed < WebhookAlertService.WebhookPostTimeout + TimeSpan.FromSeconds(15),
            $"the fan-out took {stopwatch.Elapsed}");
    }

    /// <summary>
    /// The bound does not break the happy path: an endpoint that answers gets its post, through the timeout
    /// overload and through the fan-out.
    /// </summary>
    [Fact]
    public async Task AnEndpointThatAnswers_StillReceivesThePost()
    {
        using var endpoint = new CapturingWebhookEndpoint();

        var error = await WebhookAlertService.PostWebhookAsync(
            endpoint.Url, "{\"probe\":1}", proxyAddress: null, headers: null, TimeSpan.FromSeconds(10), CancellationToken.None);

        Assert.Null(error);
        Assert.Equal("{\"probe\":1}", Assert.Single(endpoint.Bodies));

        var webhooks = GenericOnlyService(endpoint.Url);
        var result = await webhooks.TrySendWebhookAlertsAsync("High CPU", "SQL01", "97%", "90%");

        Assert.Equal(AlertChannelOutcome.Delivered, result.Outcome);
        Assert.Equal(AlertChannelOutcome.Delivered, result.ChannelOutcomes![NotificationRouter.GenericChannel]);
        Assert.Equal(2, endpoint.Bodies.Count);
    }

    private static WebhookAlertService GenericOnlyService(string url)
    {
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = url;
        return new WebhookAlertService(
            new DarlingAlertSettings(config), DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);
    }

    /* A loopback listener that accepts every connection and never reads or answers, which is what a
       firewalled or wedged endpoint looks like from the client's side. TcpListener rather than HttpListener
       for the reason CapturingWebhookEndpoint gives: no URL ACL. Connected completes on the first accept, so
       a test can cancel while the post is genuinely in flight and not before it has connected. */
    private sealed class HungWebhookEndpoint : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _stop = new();
        private readonly List<TcpClient> _held = new();
        private readonly TaskCompletionSource _connected = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Task _accepting;

        public HungWebhookEndpoint()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _accepting = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        public Task Connected => _connected.Task;

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    lock (_held)
                    {
                        _held.Add(client);
                    }

                    _connected.TrySetResult();
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
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

            lock (_held)
            {
                foreach (var client in _held)
                {
                    client.Dispose();
                }
            }

            _stop.Dispose();
        }
    }
}
