/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6231: a webhook URL's shape is refused at save with the same sentence in every writer, and a webhook post does not connect
/// to a link-local address wherever the address came from. Loopback and private addresses stay allowed.
/// </summary>
public sealed class WebhookUrlGuardTests
{
    [Theory]
    [InlineData("169.254.169.254", true)]
    [InlineData("169.254.0.1", true)]
    [InlineData("fe80::1", true)]
    [InlineData("febf::1", true)]
    [InlineData("::ffff:169.254.169.254", true)]
    [InlineData("127.0.0.1", false)]
    [InlineData("::1", false)]
    [InlineData("10.1.2.3", false)]
    [InlineData("192.168.1.5", false)]
    [InlineData("169.253.1.1", false)]
    [InlineData("fec0::1", false)]
    [InlineData("2001:db8::1", false)]
    public void LinkLocalIsRefused_LoopbackAndPrivateAreNot(string address, bool linkLocal) =>
        Assert.Equal(linkLocal, WebhookConnectGuard.IsLinkLocal(IPAddress.Parse(address)));

    [Fact]
    public async Task AWebhookPost_ToALinkLocalAddress_IsRefusedAtConnect_ButALoopbackReceiverIsReached()
    {
        using var handler = new SocketsHttpHandler { ConnectCallback = WebhookConnectGuard.ConnectAsync };
        using var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) };

        var refused = await Assert.ThrowsAsync<HttpRequestException>(
            () => client.GetAsync("http://169.254.169.254/latest", TestContext.Current.CancellationToken));
        Assert.Contains(WebhookConnectGuard.RefusalText, refused.ToString(), StringComparison.Ordinal);

        using var receiver = new CapturingWebhookEndpoint();
        using var posted = await client.PostAsync(receiver.Url, new StringContent("{}"), TestContext.Current.CancellationToken);
        Assert.True(posted.IsSuccessStatusCode);
        Assert.Single(receiver.Bodies);
    }

    [Theory]
    [InlineData("https://hooks.example.com/services/T0/B0/xyz", null)]
    [InlineData("http://127.0.0.1:8080/hook", null)]
    [InlineData("ftp://hooks.example.com/x", "The generic webhook URL must be an absolute http or https URL with a host. (The value is not echoed.)")]
    [InlineData("hooks.example.com/x", "The generic webhook URL must be an absolute http or https URL with a host. (The value is not echoed.)")]
    [InlineData("http:///x", "The generic webhook URL must be an absolute http or https URL with a host. (The value is not echoed.)")]
    [InlineData("https://user:pass@hooks.example.com/x", "The generic webhook URL must not carry a user name or password; put any token in the path. (The value is not echoed.)")]
    public void TheRouteWriter_RefusesAUrlThatCannotBeSentTo_WithoutEchoingIt(string url, string? refusal)
    {
        var row = new NotificationRouteDraft { MetricMatch = "Blocking", GenericUrl = url };
        void Resolve() => NotificationRouteWriter.ResolveRoute(
            row, null, new WebhookProxies("", "", "", ""), 1, (text, _) => "sealed:" + text, null);

        if (refusal is null)
        {
            Resolve();
            Assert.Equal("sealed:" + url, row.GenericUrl);
            return;
        }

        var ex = Assert.Throws<NotificationRouteRefusedException>(Resolve);
        Assert.Equal(refusal, ex.Message);
        Assert.DoesNotContain(url, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRouteWriter_DoesNotHoldAPagerDutyRoutingKeyToAUrlShape()
    {
        var row = new NotificationRouteDraft { MetricMatch = "Blocking", PagerDutyRoutingKey = "R0UT1NGK3Y" };
        NotificationRouteWriter.ResolveRoute(row, null, new WebhookProxies("", "", "", ""), 1, (text, _) => "sealed:" + text, null);
        Assert.Equal("sealed:R0UT1NGK3Y", row.PagerDutyRoutingKey);
    }
}
