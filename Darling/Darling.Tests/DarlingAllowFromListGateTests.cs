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
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5288: a live HTTP proof that BOTH hosts admit every range of an <c>allowFrom</c> LIST and refuse an address in
/// none of them with a 403, through the SAME <c>ConfigurePipeline</c> methods production calls (the harness shape
/// is <see cref="DarlingWebHostGateLiveTests"/>'s and <see cref="DarlingMcpHostGateLiveTests"/>'s: a real
/// pipeline over <see cref="TestServer"/>, with <c>RemoteIpAddress</c> set per request through
/// <see cref="TestServer.SendAsync"/>).
///
/// <para>The list does not start as a hand-built object. It is a JSON ARRAY in a darling.json text, and each
/// test walks the production route to the gate: <see cref="DarlingConfig.Parse"/> (the converter joins the
/// array), the host's own bind resolver (the ladder must let the list through), <c>CidrAllowList.Parse</c> (what
/// <c>TryStartServerAsync</c> does after the ladder), then <c>ConfigurePipeline</c>.</para>
/// </summary>
public sealed class DarlingAllowFromListGateTests
{
    private const string ListenIp = "192.168.1.205";

    /// <summary>Two disjoint ranges, written as the array form the issue asks for.</summary>
    private const string AllowFromJson = @"[""192.168.1.0/24"", ""10.8.0.0/16""]";

    private const string McpToken = "correct-mcp-token-value";
    private const string WebToken = "correct-web-token-value";

    /// <summary>In the first range, in the SECOND range (the one a first-entry-only check would miss), the
    /// second range as an IPv4-mapped IPv6 address (how an IPv4 client reaches a dual-stack listener), and
    /// loopback (exempt from the list, never from the credential).</summary>
    private static readonly IPAddress[] Admitted =
    {
        IPAddress.Parse("192.168.1.50"),
        IPAddress.Parse("192.168.1.1"),
        IPAddress.Parse("10.8.3.4"),
        IPAddress.Parse("10.8.255.254"),
        IPAddress.Parse("::ffff:10.8.3.4"),
        IPAddress.Loopback,
    };

    /// <summary>In neither range: next to each, elsewhere private, public, and a native IPv6 address.</summary>
    private static readonly IPAddress[] Refused =
    {
        IPAddress.Parse("10.9.0.1"),
        IPAddress.Parse("192.168.2.1"),
        IPAddress.Parse("192.168.0.255"),
        IPAddress.Parse("172.16.0.1"),
        IPAddress.Parse("203.0.113.9"),
        IPAddress.Parse("2001:db8::1"),
    };

    private static DarlingConfig LoadConfig(string section, string token)
        => DarlingConfig.Parse(
            @"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ], """ + section
            + @""": { ""enabled"": true, ""network"": { ""listen"": """ + ListenIp + @""", ""allowFrom"": " + AllowFromJson
            + @", ""token"": """ + token + @""" } } }");

    private static void AssertStatus(int expected, int actual, IPAddress remote)
        => Assert.True(expected == actual, $"a request from {remote} should answer {expected}, not {actual}");

    /// <summary>
    /// The builder both hosts start from. <paramref name="frameworkSwitchOn"/> turns the framework's forwarded-headers
    /// switch on THROUGH the builder's own configuration, as a command-line argument, which the framework reads the way
    /// it reads the <c>ASPNETCORE_FORWARDEDHEADERS_ENABLED</c> environment variable. It never goes through the process
    /// environment: a process-wide variable would reach every test class running in parallel with this one.
    /// </summary>
    private static WebApplicationBuilder CreateBuilder(bool frameworkSwitchOn)
        => WebApplication.CreateBuilder(new WebApplicationOptions
        {
            Args = frameworkSwitchOn ? new[] { "--ForwardedHeaders_Enabled=true" } : Array.Empty<string>(),
        });

    /// <summary>The one value a forwarded-header request names that is not the peer's own address.</summary>
    private const string NamedByTheHeader = "203.0.113.77";

    /* ---- MCP ---- */

    private static async Task<TestServer> BuildMcpServer(CidrAllowList allowedCidr, bool frameworkSwitchOn = false)
    {
        var builder = CreateBuilder(frameworkSwitchOn);

        // The production line, on the line after the builder exists, exactly where both hosts run it.
        DarlingMcpHostService.PinForwardedHeadersOff(builder.Services);

        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        // A pool the gates never open: a request is refused, or reaches tools/list, before any tool body runs.
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        builder.Services.AddSingleton(new DarlingAnalysisService(
            postgres, planFetcher: null, logger: NullLogger.Instance, baselineCache: new BaselineCache()));
        builder.Services.AddSingleton<ILogger>(NullLogger.Instance);

        DarlingMcpHostService.ConfigureMcpServices(builder.Services, DarlingPeerDirectory.Snapshot.Empty);

        var app = builder.Build();

        var host = new DarlingMcpHostService(
            NullLogger<DarlingMcpHostService>.Instance,
            new McpRuntimeState(),
            new MonitoredServerRegistryState());

        host.ConfigurePipeline(
            app,
            networkMode: true,
            networkListenIp: IPAddress.Parse(ListenIp),
            allowedCidr: allowedCidr,
            bearerToken: McpToken);

        await app.StartAsync();
        return app.GetTestServer();
    }

    /// <summary>A <c>tools/list</c> JSON-RPC POST on <c>/</c> with the RIGHT bearer token from <paramref name="remote"/>:
    /// 200 means the request got past every gate, 403 means the CIDR gate refused it.</summary>
    private static async Task<int> McpToolsListAsync(TestServer server, IPAddress remote, string? forwardedFor = null)
    {
        const string requestBody = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}";
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = ListenIp;
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(requestBody);
            c.Request.Body = new MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = remote;
            c.Request.Headers.Authorization = $"Bearer {McpToken}";
            if (forwardedFor is not null)
            {
                c.Request.Headers["X-Forwarded-For"] = forwardedFor;
            }
        });

        using var reader = new StreamReader(ctx.Response.Body);
        await reader.ReadToEndAsync();
        return ctx.Response.StatusCode;
    }

    [Fact]
    public async Task Mcp_List_AdmitsEachRange_403Outside()
    {
        var config = LoadConfig("mcp", McpToken);
        Assert.Equal("192.168.1.0/24,10.8.0.0/16", config.Mcp.Network!.AllowFrom);

        var bind = DarlingMcpHostService.ResolveMcpBind(config.Mcp, managed: true, inContainer: false);
        Assert.Equal(DarlingMcpHostService.McpBindMode.NetworkAndLoopback, bind.Mode);
        Assert.Equal(DarlingMcpHostService.McpBindReason.NetworkExposed, bind.Reason);

        using var server = await BuildMcpServer(CidrAllowList.Parse(config.Mcp.Network.AllowFrom!));

        foreach (var remote in Admitted)
        {
            AssertStatus(StatusCodes.Status200OK, await McpToolsListAsync(server, remote), remote);
        }

        foreach (var remote in Refused)
        {
            AssertStatus(StatusCodes.Status403Forbidden, await McpToolsListAsync(server, remote), remote);
        }
    }

    /// <summary>
    /// #5288: the allowFrom check judges the connection's own peer address, never an address a request header names,
    /// even with the framework's forwarded-headers switch turned ON in the host's configuration (the host pins the
    /// handling off, <see cref="DarlingMcpHostService.PinForwardedHeadersOff"/>). A peer outside the list that names an
    /// in-list address, or loopback, in <c>X-Forwarded-For</c> is still answered 403; and an in-list peer that names an
    /// outside address still gets through, because the header is not read in either direction.
    /// </summary>
    [Theory]
    [InlineData("192.168.1.50")]
    [InlineData("127.0.0.1")]
    public async Task Mcp_ForwardedFor_IsIgnored_EvenWithTheFrameworkSwitchOn(string forwardedFor)
    {
        using var server = await BuildMcpServer(CidrAllowList.Parse("192.168.1.0/24,10.8.0.0/16"), frameworkSwitchOn: true);

        var outside = IPAddress.Parse("203.0.113.50");
        AssertStatus(StatusCodes.Status403Forbidden, await McpToolsListAsync(server, outside, forwardedFor), outside);

        var inList = IPAddress.Parse("192.168.1.50");
        AssertStatus(StatusCodes.Status200OK, await McpToolsListAsync(server, inList, NamedByTheHeader), inList);
    }

    /* ---- web ---- */

    private static async Task<TestServer> BuildWebServer(CidrAllowList allowedCidr, bool frameworkSwitchOn = false)
    {
        var builder = CreateBuilder(frameworkSwitchOn);

        // The production line, on the line after the builder exists, exactly where both hosts run it.
        DarlingMcpHostService.PinForwardedHeadersOff(builder.Services);

        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        // A pool the gates never open (they run entirely ahead of DarlingWebEndpoints.MapAll's routes).
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);

        // ConfigurePipeline calls app.UseResponseCompression(), which resolves its options from DI.
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);

        var app = builder.Build();

        var host = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance,
            new WebRuntimeState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache());

        host.ConfigurePipeline(
            app,
            postgres,
            networkMode: true,
            networkListenIp: IPAddress.Parse(ListenIp),
            allowedCidr: allowedCidr,
            accessToken: WebToken,
            oidcClient: null);

        await app.StartAsync();
        return app.GetTestServer();
    }

    /// <summary>A <c>GET /?token=</c> with the RIGHT token from <paramref name="remote"/>: 302 means the gate
    /// exchanged the token for a session cookie (past every gate), 403 means the CIDR gate refused it first.</summary>
    private static async Task<int> WebTokenRequestAsync(TestServer server, IPAddress remote, string? forwardedFor = null)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "GET";
            c.Request.Path = "/";
            c.Request.QueryString = new QueryString("?token=" + Uri.EscapeDataString(WebToken));
            c.Request.Headers.Host = ListenIp;
            c.Connection.RemoteIpAddress = remote;
            if (forwardedFor is not null)
            {
                c.Request.Headers["X-Forwarded-For"] = forwardedFor;
            }
        });

        return ctx.Response.StatusCode;
    }

    [Fact]
    public async Task Web_List_AdmitsEachRange_403Outside()
    {
        var config = LoadConfig("web", WebToken);
        Assert.Equal("192.168.1.0/24,10.8.0.0/16", config.Web.Network!.AllowFrom);

        var bind = DarlingWebHostService.ResolveWebBind(config.Web, managed: true, inContainer: false);
        Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, bind.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.NetworkExposed, bind.Reason);

        using var server = await BuildWebServer(CidrAllowList.Parse(config.Web.Network.AllowFrom!));

        foreach (var remote in Admitted)
        {
            AssertStatus(StatusCodes.Status302Found, await WebTokenRequestAsync(server, remote), remote);
        }

        foreach (var remote in Refused)
        {
            AssertStatus(StatusCodes.Status403Forbidden, await WebTokenRequestAsync(server, remote), remote);
        }
    }

    /// <summary>
    /// #5288: the web twin of <see cref="Mcp_ForwardedFor_IsIgnored_EvenWithTheFrameworkSwitchOn"/>. The allowFrom
    /// check judges the connection's own peer address, never one a request header names, with the framework's
    /// forwarded-headers switch turned ON: an outside peer that names an in-list address, or loopback, is still
    /// answered 403, and an in-list peer that names an outside address still has its token exchanged for a cookie.
    /// </summary>
    [Theory]
    [InlineData("192.168.1.50")]
    [InlineData("127.0.0.1")]
    public async Task Web_ForwardedFor_IsIgnored_EvenWithTheFrameworkSwitchOn(string forwardedFor)
    {
        using var server = await BuildWebServer(CidrAllowList.Parse("192.168.1.0/24,10.8.0.0/16"), frameworkSwitchOn: true);

        var outside = IPAddress.Parse("203.0.113.50");
        AssertStatus(StatusCodes.Status403Forbidden, await WebTokenRequestAsync(server, outside, forwardedFor), outside);

        var inList = IPAddress.Parse("192.168.1.50");
        AssertStatus(StatusCodes.Status302Found, await WebTokenRequestAsync(server, inList, NamedByTheHeader), inList);
    }

    /// <summary>
    /// The premise of the two tests above, pinned so they cannot pass for the wrong reason: on the very builder they
    /// use, the framework's switch really does turn forwarded-header handling on, and
    /// <see cref="DarlingMcpHostService.PinForwardedHeadersOff"/> is what turns it back off. If a framework change
    /// ever stopped the switch from taking effect, the first assertion fails here instead of the tests above going
    /// quietly green.
    /// </summary>
    [Fact]
    public void FrameworkSwitch_TurnsForwardedHeadersOn_AndThePinTurnsThemOffAgain()
    {
        var unpinned = CreateBuilder(frameworkSwitchOn: true);
        using var unpinnedApp = unpinned.Build();
        Assert.NotEqual(
            Microsoft.AspNetCore.HttpOverrides.ForwardedHeaders.None,
            unpinnedApp.Services.GetRequiredService<IOptions<ForwardedHeadersOptions>>().Value.ForwardedHeaders);

        var pinned = CreateBuilder(frameworkSwitchOn: true);
        DarlingMcpHostService.PinForwardedHeadersOff(pinned.Services);
        using var pinnedApp = pinned.Build();
        Assert.Equal(
            Microsoft.AspNetCore.HttpOverrides.ForwardedHeaders.None,
            pinnedApp.Services.GetRequiredService<IOptions<ForwardedHeadersOptions>>().Value.ForwardedHeaders);
    }
}
