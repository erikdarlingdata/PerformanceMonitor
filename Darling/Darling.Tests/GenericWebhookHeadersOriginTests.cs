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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5366: the generic webhook's configured headers go only to the generic URL itself or a path under it (same
/// scheme, host and port, and the path equal or continuing at a "/"), and are never sent across a redirect. The
/// sender is shared with Lite, which pins the same rule.
/// </summary>
public sealed class GenericWebhookHeadersOriginTests
{
    private static readonly Dictionary<string, string> Headers = new() { ["X-Fake-Key"] = "not-real-value" };

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
    public async Task ASendWithHeaders_ToAnEndpointThatAnswersOk_CarriesTheHeaders()
    {
        using var endpoint = new LoopbackEndpoint(redirectTo: null);

        var error = await Post(endpoint.Url, Headers);

        Assert.Null(error);
        Assert.Contains("X-Fake-Key: not-real-value", endpoint.LastRequestHead, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ARedirectAnswerToASendWithHeaders_IsAFailedSend_AndTheTargetIsNotCalled()
    {
        using var target = new LoopbackEndpoint(redirectTo: null);
        using var endpoint = new LoopbackEndpoint(redirectTo: target.Url + "/final");

        var error = await Post(endpoint.Url, Headers);

        Assert.Equal(1, endpoint.Requests);
        Assert.Equal(0, target.Requests);
        Assert.NotNull(error);
        Assert.Contains("redirect", error, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(target.Url, error, StringComparison.Ordinal);
        Assert.DoesNotContain("not-real-value", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARedirectAnswerToASendWithoutHeaders_IsStillFollowed()
    {
        using var target = new LoopbackEndpoint(redirectTo: null);
        using var endpoint = new LoopbackEndpoint(redirectTo: target.Url + "/final");

        var error = await Post(endpoint.Url, new Dictionary<string, string>());

        Assert.Null(error);
        Assert.Equal(1, target.Requests);
    }

    private static Task<string?> Post(string url, IReadOnlyDictionary<string, string> headers) =>
        WebhookAlertService.PostWebhookAsync(
            url, "{}", proxyAddress: null, headers, TimeSpan.FromSeconds(5), CancellationToken.None);

    /// <summary>A loopback endpoint that answers every POST 200 (or a 302 to a target) and keeps the last head.</summary>
    private sealed class LoopbackEndpoint : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _accepting;
        private readonly string? _redirectTo;
        private int _requests;
        private volatile string _lastHead = "";

        public LoopbackEndpoint(string? redirectTo)
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
                    var received = new List<byte>();
                    var buffer = new byte[16 * 1024];
                    var head = "";

                    while (true)
                    {
                        var read = await stream.ReadAsync(buffer, _stop.Token);
                        if (read == 0)
                        {
                            break;
                        }

                        received.AddRange(new ArraySegment<byte>(buffer, 0, read));
                        head = Encoding.ASCII.GetString(received.ToArray());
                        if (head.Contains("\r\n\r\n", StringComparison.Ordinal))
                        {
                            break;
                        }
                    }

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
