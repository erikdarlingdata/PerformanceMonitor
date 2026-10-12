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
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #5240 edit routes through the REAL web pipeline (<c>DarlingWebHostService.ConfigurePipeline</c>, as
/// <see cref="DarlingWebHostGateLiveTests"/> does), with session cookies minted under a key the test chose: an
/// anonymous caller is 401, a read-only sign-in is 403 on the unsafe edit method and on the by-id admin read (the
/// host's group gate lets that GET through, so the route refuses it itself), an editing sign-in reaches the route, and
/// only PATCH is routed on the edit path. The store pool is never opened: every request that reaches the route is
/// one the route refuses before it touches the store.
/// </summary>
public sealed class ServerEditGateLiveTests
{
    private const string ListenIp = "192.168.1.205";
    private const string Token = "correct-token-value";
    private static readonly IPAddress InCidrRemote = IPAddress.Parse("192.168.1.50");
    private static readonly byte[] Key = RandomNumberGenerator.GetBytes(32);

    private static string Cookie(string? subject, WebOidcRole? role) =>
        DarlingWebHostService.SessionCookieName + "=" + DarlingWebHostService.BuildSessionCookieValue(
            Key, DateTimeOffset.UtcNow.AddHours(1), subject is null ? null : DarlingWebSeat.EncodeCookieSubject(subject, role!.Value));

    private static async Task<TestServer> BuildServer()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        var app = builder.Build();
        var host = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance, new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache());
        host.ConfigurePipeline(
            app, postgres, networkMode: true, networkListenIp: IPAddress.Parse(ListenIp), allowedCidr: IPNetwork.Parse("192.168.1.0/24"),
            accessToken: Token, oidcClient: null, publicBaseUrlHost: null, sessionSigningKeyForTests: Key);
        await app.StartAsync();
        return app.GetTestServer();
    }

    private static async Task<(int Status, string Body)> SendAsync(TestServer server, string method, string path, string? cookie, string? body = null)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = method;
            c.Request.Path = path;
            c.Request.Headers.Host = ListenIp;
            c.Connection.RemoteIpAddress = InCidrRemote;
            if (cookie is not null)
            {
                c.Request.Headers["Cookie"] = cookie;
            }

            if (body is not null)
            {
                c.Request.ContentType = "application/json";
                c.Request.Body = new System.IO.MemoryStream(Encoding.UTF8.GetBytes(body));
            }
        });
        return (ctx.Response.StatusCode, await new System.IO.StreamReader(ctx.Response.Body).ReadToEndAsync());
    }

    private const string Edit = "{\"display_name\":\"x\",\"password\":\"Zq9-fake-secret-must-never-appear-7XK\"}";

    [Theory]
    [InlineData("PATCH", "/api/servers/41")]
    [InlineData("GET", "/api/admin/servers/41")]
    public async Task AnAnonymousCaller_IsRefused401Json_OnBothEditRoutes(string method, string path)
    {
        using var server = await BuildServer();
        var (status, body) = await SendAsync(server, method, path, cookie: null, method == "PATCH" ? Edit : null);
        Assert.Equal(StatusCodes.Status401Unauthorized, status);
        Assert.Contains("\"error\"", body, StringComparison.Ordinal);
        Assert.DoesNotContain("Zq9", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReadOnlySignIn_IsRefused403_OnPatch_BeforeTheRoute_WithoutEchoingTheBody()
    {
        using var server = await BuildServer();
        var (status, body) = await SendAsync(server, "PATCH", "/api/servers/41", Cookie("bob", WebOidcRole.Viewer), Edit);
        Assert.Equal(StatusCodes.Status403Forbidden, status);
        Assert.Contains("read-only", body, StringComparison.Ordinal);
        Assert.DoesNotContain("Zq9", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReadOnlySignIn_IsRefused403_OnTheByIdAdminRead_BeforeTheStore()
    {
        using var server = await BuildServer();
        var (status, body) = await SendAsync(server, "GET", "/api/admin/servers/41", Cookie("bob", WebOidcRole.Viewer));
        Assert.Equal(StatusCodes.Status403Forbidden, status);
        Assert.Contains("read-only", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnEditingSignIn_ReachesThePatchRoute_WhichRefusesAMissingTokenWith400()
    {
        using var server = await BuildServer();
        var (status, body) = await SendAsync(server, "PATCH", "/api/servers/41", Cookie("alice", WebOidcRole.Admin), Edit);
        Assert.Equal(StatusCodes.Status400BadRequest, status);
        Assert.Contains("expected_modified_at", body, StringComparison.Ordinal);
        Assert.DoesNotContain("Zq9", body, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("POST")]
    [InlineData("PUT")]
    [InlineData("DELETE")]
    public async Task OnlyPatchIsRoutedOnTheEditPath_ForAnEditingSignIn(string method)
    {
        using var server = await BuildServer();
        var (status, _) = await SendAsync(server, method, "/api/servers/41", Cookie("alice", WebOidcRole.Admin), Edit);
        Assert.Equal(StatusCodes.Status405MethodNotAllowed, status);
    }

    [Theory]
    [InlineData("POST")]
    [InlineData("PUT")]
    [InlineData("PATCH")]
    [InlineData("DELETE")]
    public async Task EveryUnsafeMethod_OnTheAdminReadPath_IsRefusedForAReadOnlySignIn_AndNotRoutedForAnEditingOne(string method)
    {
        using var server = await BuildServer();
        Assert.Equal(StatusCodes.Status403Forbidden, (await SendAsync(server, method, "/api/admin/servers/41", Cookie("bob", WebOidcRole.Viewer), "{}")).Status);
        Assert.Equal(StatusCodes.Status405MethodNotAllowed, (await SendAsync(server, method, "/api/admin/servers/41", Cookie("alice", WebOidcRole.Admin), "{}")).Status);
    }
}
