using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7086, Lite's half: <see cref="AlertOutcome.Hold"/> through <see cref="LiteAlertDeliverer"/>. A held fire is one digest
/// row, no toast and no post. It leaves the send core's cooldown alone, so a real page for the same alert right after it
/// goes out, and a mute rule wins over the hold.
/// </summary>
public sealed class AlertHoldDeliveryTests
{
    private const string Reason = "Held for the digest: the condition has stood 5 of 30 minutes.";

    private static AlertOutcome Outcome(bool muted = false, AlertHold? hold = null) =>
        new("1", "SQL01", "High CPU", "97%", "90%", null, "detail", 97, 90, muted, null,
            "Total CPU at 97% (threshold: 90%)", Hold: hold);

    [Fact]
    public async Task AHeldFire_WritesOneDigestRow_NoToastNoPost_AndARealPageRightAfterItGoesOut()
    {
        var configDir = Path.Combine(Path.GetTempPath(), "pmlite-hold-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(configDir);

        try
        {
            using var endpoint = new CountingEndpoint();
            var settings = new AlertWebhookCancelTests.GenericWebhookSettings(endpoint.Url);
            var history = new AlertWebhookCancelTests.CapturingHistoryStore();
            var webhooks = new WebhookAlertService(
                settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());
            var emailAlerts = new EmailAlertService(
                settings, history, webhooks, new AppLoggerAdapter<EmailAlertService>());
            var toasts = new List<string>();
            var deliverer = new LiteAlertDeliverer(
                (title, _, _, _, _) => toasts.Add(title),
                (metric, server, current, threshold, id, context, numCur, numThr, muted, detail, mode, token) =>
                    emailAlerts.TrySendAlertEmailAsync(
                        metric, server, current, threshold, id, context, numCur, numThr, muted, detail,
                        deliveryMode: mode, cancellationToken: token),
                _ => null,
                (metric, server, current, threshold, id, context, numCur, numThr, detail, hold, token) =>
                    emailAlerts.RecordHeldAlertAsync(
                        metric, server, current, threshold, id, context, numCur, numThr, detail, hold, token));
            var ct = TestContext.Current.CancellationToken;

            var held = await deliverer.DeliverAndReportAsync(Outcome(hold: new AlertHold(Reason)), ct);

            Assert.NotNull(held);
            Assert.False(held!.Sent);
            Assert.Equal(AlertDelivery.ChannelDigest, held.Channel);
            Assert.Empty(toasts);
            Assert.Equal(0, endpoint.Requests);
            var row = Assert.Single(history.Records);
            Assert.False(row.AlertSent);
            Assert.False(row.Muted);
            Assert.Equal(AlertDelivery.ChannelDigest, row.NotificationType);
            var routing = AlertContextSerializer.TryReadRouting(row.ContextJson);
            Assert.NotNull(routing);
            Assert.Equal("digest", routing!.Route);
            Assert.Equal(Reason, routing.Reason);

            // The held fire did not stamp the cooldown: the same alert, not held, toasts and posts at once.
            var page = await deliverer.DeliverAndReportAsync(Outcome(), ct);

            Assert.NotNull(page);
            Assert.True(page!.Sent);
            Assert.Single(toasts);
            Assert.Equal(1, endpoint.Requests);
            Assert.Equal(2, history.Records.Count);
            Assert.True(history.Records[1].AlertSent);
        }
        finally
        {
            Directory.Delete(configDir, recursive: true);
        }
    }

    [Fact]
    public async Task AMutedAndHeldFire_TakesTheMutedRoad()
    {
        var toasts = new List<string>();
        var sends = new List<bool>();
        var held = 0;
        var deliverer = new LiteAlertDeliverer(
            (title, _, _, _, _) => toasts.Add(title),
            (_, _, _, _, _, _, _, _, muted, _, _, _) =>
            {
                sends.Add(muted);
                return Task.FromResult<AlertDelivery?>(null);
            },
            _ => null,
            (_, _, _, _, _, _, _, _, _, _, _) =>
            {
                held++;
                return Task.FromResult<AlertDelivery?>(AlertDelivery.RoutedToDigest());
            });

        await deliverer.DeliverAsync(Outcome(muted: true, hold: new AlertHold(Reason)), TestContext.Current.CancellationToken);

        Assert.Equal(0, held);
        Assert.Equal(new[] { true }, sends);
    }

    /// <summary>A loopback endpoint that answers every POST with HTTP 200 and counts them.</summary>
    private sealed class CountingEndpoint : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _accepting;
        private int _requests;

        public CountingEndpoint()
        {
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _accepting = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        public int Requests => Volatile.Read(ref _requests);

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    var buffer = new byte[16 * 1024];
                    var seen = new StringBuilder();
                    while (!seen.ToString().Contains("\r\n\r\n", StringComparison.Ordinal))
                    {
                        var read = await stream.ReadAsync(buffer, _stop.Token);
                        if (read == 0)
                            break;
                        seen.Append(Encoding.ASCII.GetString(buffer, 0, read));
                    }

                    Interlocked.Increment(ref _requests);
                    var response = Encoding.ASCII.GetBytes(
                        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    await stream.WriteAsync(response, _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
            catch (IOException) { /* client went away */ }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            try { _accepting.Wait(TimeSpan.FromSeconds(2)); }
            catch (AggregateException) { /* the loop's own shutdown */ }
            _stop.Dispose();
        }
    }
}
