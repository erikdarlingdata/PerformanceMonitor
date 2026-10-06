/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
/// #5366: the generic webhook's configured headers go only to the generic URL itself or a path under it (same
/// scheme, host and port, and the path equal or continuing at a "/"), and are never sent across a redirect. A route
/// can point the generic channel at another URL; that URL gets the body and not the headers. The sender is shared,
/// so Lite and Darling follow the same rule.
/// </summary>
public sealed class GenericWebhookHeadersOriginTests
{
    private const string HeadersJson = "{\"X-Fake-Key\":\"not-real-value\"}";

    [Theory]
    [InlineData("https://example.test/a", "https://example.test/a", true)]
    [InlineData("HTTPS://EXAMPLE.TEST:443/a", "https://example.test/a", true)]
    [InlineData("https://example.test/alerts/team-a", "https://example.test/alerts", true)]
    [InlineData("https://example.test/alerts/team-a", "https://example.test/alerts/", true)]
    [InlineData("https://example.test/alerts?x=1", "https://example.test/alerts?y=2", true)]
    [InlineData("https://example.test/", "https://example.test", true)]
    [InlineData("https://example.test", "https://example.test/", true)]
    [InlineData("https://example.test/anything", "https://example.test", false)]
    [InlineData("https://example.test/anything", "https://example.test/", false)]
    [InlineData("https://example.test/alerts%2Fteam-a", "https://example.test/alerts", false)]
    [InlineData("https://example.test/alerts/team%2fa", "https://example.test/alerts", false)]
    [InlineData("https://example.test/alerts/team%5Ca", "https://example.test/alerts", false)]
    [InlineData("https://example.test/alerts%5cteam-a", "https://example.test/alerts", false)]
    [InlineData("https://example.test/a%2Fb", "https://example.test/a%2Fb", false)]
    [InlineData("https://example.test/alerts2", "https://example.test/alerts", false)]
    [InlineData("https://example.test/other", "https://example.test/alerts", false)]
    [InlineData("https://example.test/Alerts", "https://example.test/alerts", false)]
    [InlineData("https://example.test/alerts/../other", "https://example.test/alerts", false)]
    [InlineData("https://example.test", "https://example.test/alerts", false)]
    [InlineData("http://example.test/a", "https://example.test/a", false)]
    [InlineData("https://other.example.test/a", "https://example.test/a", false)]
    [InlineData("https://example.test:8443/a", "https://example.test/a", false)]
    [InlineData("", "https://example.test/a", false)]
    [InlineData("not a url", "not a url", false)]
    public void HeadersApplyTo_ComparesOriginAndPath(string route, string generic, bool expected) =>
        Assert.Equal(expected, WebhookAlertService.HeadersApplyTo(route, generic));

    [Fact]
    public async Task TheConfiguredEndpoint_ReceivesTheHeaders()
    {
        using var configured = new Capture();

        await SendAsync(configured.Url, routedUrl: null);

        Assert.Equal(1, configured.Requests);
        Assert.Contains("X-Fake-Key: not-real-value", configured.LastRequestHead, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ARouteToAnotherPathOnTheSameEndpoint_ReceivesTheHeaders()
    {
        using var configured = new Capture();

        await SendAsync(configured.Url, routedUrl: configured.Url + "/routed");

        Assert.Equal(1, configured.Requests);
        Assert.Contains("X-Fake-Key: not-real-value", configured.LastRequestHead, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ARouteToAnotherEndpoint_ReceivesTheBodyAndNoHeaders()
    {
        using var configured = new Capture();
        using var routed = new Capture();

        await SendAsync(configured.Url, routedUrl: routed.Url);

        Assert.Equal(0, configured.Requests);
        Assert.Equal(1, routed.Requests);
        Assert.DoesNotContain("X-Fake-Key", routed.LastRequestHead, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ARouteToADifferentPathOnTheSameHost_ReceivesTheBodyAndNoHeaders()
    {
        using var shared = new Capture();

        await SendAsync(shared.Url + "/alerts", routedUrl: shared.Url + "/other");

        Assert.Equal(1, shared.Requests);
        Assert.DoesNotContain("X-Fake-Key", shared.LastRequestHead, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ARedirectAnswerToAHeaderSend_IsAFailedSend_AndTheTargetIsNotCalled()
    {
        using var target = new Capture();
        using var configured = new Capture(redirectTo: target.Url + "/final");
        var service = NewService(configured.Url, Array.Empty<NotificationRoute>());

        var result = await service.TrySendWebhookAlertsAsync("High CPU", "example-server", "97%", "90%", serverId: "1");

        Assert.Equal(1, configured.Requests);
        Assert.Equal(0, target.Requests);
        Assert.NotNull(result.SendError);
        Assert.Contains("redirect", result.SendError, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(target.Url, result.SendError, StringComparison.Ordinal);
        Assert.DoesNotContain("not-real-value", result.SendError, StringComparison.Ordinal);
    }

    private static async Task SendAsync(string configuredUrl, string? routedUrl)
    {
        var routes = routedUrl is null
            ? Array.Empty<NotificationRoute>()
            : new[] { new NotificationRoute(1, "High CPU", "", "", routedUrl, "", "", true) };
        var service = NewService(configuredUrl, routes);

        await service.TrySendWebhookAlertsAsync("High CPU", "example-server", "97%", "90%", serverId: "1");
    }

    private static WebhookAlertService NewService(string configuredUrl, IReadOnlyList<NotificationRoute> routes) =>
        new(new Settings(configuredUrl, routes), EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());

    private sealed class Settings : IAlertSettings
    {
        private readonly string _genericUrl;

        public Settings(string genericUrl, IReadOnlyList<NotificationRoute> routes)
        {
            _genericUrl = genericUrl;
            NotificationRoutes = routes;
        }

        public IReadOnlyList<NotificationRoute> NotificationRoutes { get; }

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

        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
        public string SlackProxyAddress => "";

        public bool GenericWebhookEnabled => true;
        public string GenericWebhookUrl => _genericUrl;
        public string GenericWebhookHeadersJson => HeadersJson;
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

    /// <summary>A loopback endpoint that answers every POST 200 and keeps the last request's head.</summary>
    private sealed class Capture : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _accepting;
        private int _requests;
        private volatile string _lastHead = "";

        private readonly string? _redirectTo;

        public Capture(string? redirectTo = null)
        {
            _redirectTo = redirectTo;
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";
            _accepting = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        public int Requests => Volatile.Read(ref _requests);

        public string LastRequestHead => _lastHead;

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    var head = await ReadRequestAsync(stream, _stop.Token);
                    _lastHead = head;
                    Interlocked.Increment(ref _requests);

                    var response = Encoding.ASCII.GetBytes(_redirectTo is null
                        ? "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        : $"HTTP/1.1 302 Found\r\nLocation: {_redirectTo}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    await stream.WriteAsync(response, _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
        }

        private static async Task<string> ReadRequestAsync(NetworkStream stream, CancellationToken token)
        {
            var received = new List<byte>();
            var buffer = new byte[16 * 1024];
            var bodyStart = -1;
            var contentLength = 0;
            var head = "";

            while (true)
            {
                if (bodyStart < 0)
                {
                    var end = IndexOfHeaderEnd(received);
                    if (end >= 0)
                    {
                        bodyStart = end + 4;
                        head = Encoding.ASCII.GetString(received.ToArray(), 0, end);
                        foreach (var line in head.Split("\r\n", StringSplitOptions.RemoveEmptyEntries))
                        {
                            if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase) &&
                                int.TryParse(line.AsSpan("Content-Length:".Length).Trim(), out var length))
                            {
                                contentLength = length;
                            }
                        }
                    }
                }

                if (bodyStart >= 0 && received.Count - bodyStart >= contentLength)
                {
                    return head;
                }

                var read = await stream.ReadAsync(buffer, token);
                if (read == 0)
                {
                    return head;
                }

                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }
        }

        private static int IndexOfHeaderEnd(List<byte> bytes)
        {
            for (var i = 0; i + 3 < bytes.Count; i++)
            {
                if (bytes[i] == (byte)'\r' && bytes[i + 1] == (byte)'\n' &&
                    bytes[i + 2] == (byte)'\r' && bytes[i + 3] == (byte)'\n')
                {
                    return i;
                }
            }

            return -1;
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
