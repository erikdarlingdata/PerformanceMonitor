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
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
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

    /* #6392: the proxied handler connects to its proxy through the same guard, so a proxy at a link-local address is refused while a proxy on
       loopback (or a private address) still carries the request. Both redirect modes build the handler the same way. */
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AProxiedWebhookPost_RefusesALinkLocalProxy_ButAProxyOnLoopbackStillCarriesIt(bool followRedirects)
    {
        using (var refusedHandler = WebhookAlertService.NewProxyHandler("http://169.254.169.254:3128", followRedirects))
        using (var refusedClient = new HttpClient(refusedHandler) { Timeout = TimeSpan.FromSeconds(10) })
        {
            var refused = await Assert.ThrowsAsync<HttpRequestException>(
                () => refusedClient.PostAsync("http://hooks.example.test/hook", new StringContent("{}"), TestContext.Current.CancellationToken));
            Assert.Contains(WebhookConnectGuard.RefusalText, refused.ToString(), StringComparison.Ordinal);
        }

        using var proxy = new CapturingWebhookEndpoint();
        using var handler = WebhookAlertService.NewProxyHandler(proxy.Url, followRedirects);
        using var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) };
        using var posted = await client.PostAsync("http://hooks.example.test/hook", new StringContent("{}"), TestContext.Current.CancellationToken);
        Assert.True(posted.IsSuccessStatusCode);
        Assert.Single(proxy.Bodies);
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

    [Theory]
    [InlineData("fd00:ec2::254", true)]
    [InlineData("168.63.129.16", true)]
    [InlineData("100.100.100.200", true)]
    [InlineData("::ffff:168.63.129.16", true)]
    [InlineData("::ffff:100.100.100.200", true)]
    [InlineData("169.254.169.254", true)]
    [InlineData("fe80::1", true)]
    [InlineData("fd00:ec2::255", false)]
    [InlineData("168.63.129.17", false)]
    [InlineData("100.100.100.201", false)]
    [InlineData("127.0.0.1", false)]
    [InlineData("::1", false)]
    [InlineData("10.1.2.3", false)]
    [InlineData("172.16.0.9", false)]
    [InlineData("192.168.1.5", false)]
    [InlineData("fd12:3456::1", false)]
    public void CloudInstanceAddressesAreRefused_PrivateAndLoopbackAreNot(string address, bool refused) =>
        Assert.Equal(refused, WebhookConnectGuard.IsRefused(IPAddress.Parse(address)));

    [Theory]
    [InlineData("http://168.63.129.16/machine")]
    [InlineData("http://100.100.100.200/latest/meta-data")]
    [InlineData("http://[fd00:ec2::254]/latest/meta-data")]
    [InlineData("http://[::ffff:168.63.129.16]/machine")]
    public async Task AWebhookPost_ToACloudInstanceAddress_IsRefusedAtConnect(string url)
    {
        using var client = new HttpClient(WebhookConnectGuard.NewHandler()) { Timeout = TimeSpan.FromSeconds(10) };
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        var refused = await Assert.ThrowsAsync<HttpRequestException>(() => client.GetAsync(url, cts.Token));
        Assert.Contains(WebhookConnectGuard.RefusalText, refused.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("64:ff9b::a9fe:a9fe", true)]
    [InlineData("64:ff9b::169.254.169.254", true)]
    [InlineData("::169.254.169.254", true)]
    [InlineData("::a9fe:a9fe", true)]
    [InlineData("64:ff9b::a83f:8110", true)]
    [InlineData("::168.63.129.16", true)]
    [InlineData("64:ff9b::a00:1", false)]
    [InlineData("::10.1.2.3", false)]
    [InlineData("64:ff9b::7f00:1", false)]
    [InlineData("::1", false)]
    [InlineData("::", false)]
    [InlineData("64:ff9b:1::a9fe:a9fe", false)]
    [InlineData("2001:db8::a9fe:a9fe", false)]
    public void ANat64OrIpv4CompatibleForm_IsCheckedAsItsIpv4(string address, bool refused) =>
        Assert.Equal(refused, WebhookConnectGuard.IsRefused(IPAddress.Parse(address)));

    [Theory]
    [InlineData("http://169.254.169.254/latest", WebhookUrlShape.Problem.BlockedAddress)]
    [InlineData("http://169.254.169.254./latest", WebhookUrlShape.Problem.BlockedAddress)]
    [InlineData("https://[::ffff:169.254.169.254]/x", WebhookUrlShape.Problem.BlockedAddress)]
    [InlineData("https://[64:ff9b::a9fe:a9fe]/x", WebhookUrlShape.Problem.BlockedAddress)]
    [InlineData("https://[::169.254.169.254]/x", WebhookUrlShape.Problem.BlockedAddress)]
    [InlineData("http://168.63.129.16./machine", WebhookUrlShape.Problem.BlockedAddress)]
    [InlineData("http://127.0.0.1./hook", WebhookUrlShape.Problem.None)]
    [InlineData("http://10.1.2.3/hook", WebhookUrlShape.Problem.None)]
    [InlineData("https://hooks.example.com./services/x", WebhookUrlShape.Problem.None)]
    public void TheSaveCheck_ReadsATrailingDotAndTheTranslatedForms_AsTheirIpv4Address(string url, WebhookUrlShape.Problem expected) =>
        Assert.Equal(expected, WebhookUrlShape.TryParse(url, out _));

    [Fact]
    public async Task TheHeartbeatClient_RefusesALinkLocalTarget_AndStillReachesAPrivateReceiver()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        var refused = await Assert.ThrowsAsync<HttpRequestException>(
            () => DarlingHeartbeat.SharedClient.GetAsync("http://169.254.169.254/latest/meta-data", cts.Token));
        Assert.Contains(WebhookConnectGuard.RefusalText, refused.ToString(), StringComparison.Ordinal);

        using var receiver = new CapturingWebhookEndpoint();
        var (ok, reason) = await DarlingHeartbeat.PingAsync(
            DarlingHeartbeat.SharedClient, new Uri(receiver.Url), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.True(ok, reason);
    }

    [Fact]
    public async Task ACancelledConnect_DisposesTheSocketItWasHanded()
    {
        using var cts = new CancellationTokenSource();
        TrackedSocket? made = null;
        Socket Make(AddressFamily family)
        {
            made = new TrackedSocket(family);
            cts.Cancel(); /* the cancel arrives while the connect is about to start, so it is deterministic */
            return made;
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await WebhookConnectGuard.ConnectAsync(new DnsEndPoint("127.0.0.1", 9), Make, cts.Token));

        Assert.NotNull(made);
        Assert.True(made.WasDisposed);
    }

    [Fact]
    public async Task AFailedConnect_DisposesTheSocketItWasHanded()
    {
        int closedPort;
        using (var probe = new TcpListener(IPAddress.Loopback, 0))
        {
            probe.Start();
            closedPort = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
        }

        TrackedSocket? made = null;
        Socket Make(AddressFamily family) => made = new TrackedSocket(family);

        await Assert.ThrowsAsync<HttpRequestException>(async () =>
            await WebhookConnectGuard.ConnectAsync(new DnsEndPoint("127.0.0.1", closedPort), Make, TestContext.Current.CancellationToken));

        Assert.NotNull(made);
        Assert.True(made.WasDisposed);
    }

    [Fact]
    public async Task ASuccessfulConnect_KeepsTheSocketOpenForTheStream()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        TrackedSocket? made = null;
        Socket Make(AddressFamily family) => made = new TrackedSocket(family);

        await using var stream = await WebhookConnectGuard.ConnectAsync(
            new DnsEndPoint("127.0.0.1", port), Make, TestContext.Current.CancellationToken);

        Assert.NotNull(made);
        Assert.False(made.WasDisposed);
    }

    private sealed class TrackedSocket(AddressFamily family) : Socket(family, SocketType.Stream, ProtocolType.Tcp)
    {
        public bool WasDisposed { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                WasDisposed = true;
            }

            base.Dispose(disposing);
        }
    }
}
