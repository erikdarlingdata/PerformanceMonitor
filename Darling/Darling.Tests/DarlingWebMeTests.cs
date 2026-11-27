/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json.Nodes;
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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>#8737: <c>GET /api/me</c> describes only the calling session: its sign-in method, seat, identity and expiry.</summary>
public sealed class DarlingWebMeTests
{
    private static readonly string[] Keys = ["method", "seat", "subject", "display_name", "granted_by", "role_mapping", "token_name", "group_name", "expires_at", "personal_tokens"];
    private static readonly byte[] Key = Enumerable.Range(0, 32).Select(i => (byte)(i * 5 + 1)).ToArray();
    private const string ScopedName = "finance";
    private static readonly string OwnerKey = new('k', DarlingWebSeat.OwnerKeyLength);

    private static void AssertShape(JsonObject me, string method, string seat, string? subject = null, string? displayName = null,
        string? grantedBy = null, string? tokenName = null, string? groupName = null, string? expiresAt = null, bool? roleMapping = null, bool personalTokens = false)
    {
        Assert.Equal(Keys.OrderBy(k => k, StringComparer.Ordinal), me.Select(p => p.Key).OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(method, (string?)me["method"]);
        Assert.Equal(seat, (string?)me["seat"]);
        Assert.Equal(subject, (string?)me["subject"]);
        Assert.Equal(displayName, (string?)me["display_name"]);
        Assert.Equal(grantedBy, (string?)me["granted_by"]);
        Assert.Equal(roleMapping, (bool?)me["role_mapping"]);
        Assert.Equal(tokenName, (string?)me["token_name"]);
        Assert.Equal(groupName, (string?)me["group_name"]);
        Assert.Equal(expiresAt, (string?)me["expires_at"]);
        Assert.Equal(personalTokens, (bool?)me["personal_tokens"]);
    }

    [Fact]
    public void PersonalTokens_TrueForOidcWhenServed()
    {
        Assert.True((bool?)DarlingWebMe.Describe(new DarlingWebSeat("bob@example.test", false, OwnerKey: "k"), null, true)["personal_tokens"]);
        Assert.True((bool?)DarlingWebMe.Describe(new DarlingWebSeat("carol@example.test", true, OwnerKey: "k"), null, true)["personal_tokens"]);
    }

    [Fact]
    public void PersonalTokens_TrueForSharedAdminWhenServed()
    {
        Assert.True((bool?)DarlingWebMe.Describe(DarlingWebSeat.SharedToken, null, true)["personal_tokens"]);
    }

    [Fact]
    public void PersonalTokens_FalseForReadOnlyNonOidc()
    {
        Assert.False((bool?)DarlingWebMe.Describe(DarlingWebSeat.ReadsOnly, null, true)["personal_tokens"]);
        Assert.False((bool?)DarlingWebMe.Describe(new DarlingWebSeat(ScopedName, false, TokenName: ScopedName, AllowedTagIds: [41]), null, true)["personal_tokens"]);
    }

    [Fact]
    public void PersonalTokens_FalseWhenNotServed()
    {
        Assert.False((bool?)DarlingWebMe.Describe(new DarlingWebSeat("carol@example.test", true, OwnerKey: "k"), null, false)["personal_tokens"]);
        Assert.False((bool?)DarlingWebMe.Describe(DarlingWebSeat.SharedToken, null, false)["personal_tokens"]);
    }

    [Fact]
    public void AnOidcAdmin_CarriesSubjectNameGrantAndExpiry()
    {
        var expiry = new DateTimeOffset(2026, 11, 14, 10, 30, 15, TimeSpan.FromHours(2));
        var me = DarlingWebMe.Describe(new DarlingWebSeat("alice@example.test", true, OwnerKey: "k", DisplayName: "Alice A", GrantedBy: "Admins", SessionExpiresAt: expiry), null, false);

        AssertShape(me, "oidc", "admin", "alice@example.test", "Alice A", "Admins", expiresAt: "2026-11-14T08:30:15.0000000Z");
    }

    [Fact]
    public void AnOidcViewer_IsReadOnly()
    {
        var me = DarlingWebMe.Describe(new DarlingWebSeat("bob@example.test", false, DisplayName: "Bob", GrantedBy: "Viewers"), null, false);

        AssertShape(me, "oidc", "read_only", "bob@example.test", "Bob", "Viewers");
    }

    [Fact]
    public void AnOidcAdmin_WithNoGrant_HasNullNameAndGrant()
    {
        var me = DarlingWebMe.Describe(new DarlingWebSeat("carol@example.test", true), null, false);

        AssertShape(me, "oidc", "admin", "carol@example.test");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AnOidcSession_ReportsWhetherTheHostHasARoleMapping(bool mapped)
    {
        var me = DarlingWebMe.Describe(new DarlingWebSeat("bob@example.test", false), mapped, false);

        AssertShape(me, "oidc", "read_only", "bob@example.test", roleMapping: mapped);
    }

    [Fact]
    public void AnOidcSession_OnAHostWithNoOidc_HasANullRoleMapping()
    {
        AssertShape(DarlingWebMe.Describe(new DarlingWebSeat("bob@example.test", false), null, false), "oidc", "read_only", "bob@example.test");
    }

    [Fact]
    public void ANonOidcSession_HasANullRoleMapping_WhateverTheHostHas()
    {
        AssertShape(DarlingWebMe.Describe(DarlingWebSeat.SharedToken, true, false), "shared_token", "admin");
        AssertShape(DarlingWebMe.Describe(new DarlingWebSeat(ScopedName, false, TokenName: ScopedName, AllowedTagIds: [41]), false, false), "named_token", "read_only", tokenName: ScopedName);
    }

    [Fact]
    public void TheSharedToken_IsAnAdminWithNoIdentity()
    {
        AssertShape(DarlingWebMe.Describe(DarlingWebSeat.SharedToken, null, false), "shared_token", "admin");
    }

    [Fact]
    public void ReadsOnly_WithNoSession_IsReadOnlyWithNoIdentity()
    {
        AssertShape(DarlingWebMe.Describe(DarlingWebSeat.ReadsOnly, null, false), "reads_only", "read_only");
    }

    [Fact]
    public void AReadOnlySeat_WithASession_IsStillTheSharedToken()
    {
        var expiry = new DateTimeOffset(2026, 11, 14, 10, 30, 15, TimeSpan.Zero);

        var me = DarlingWebMe.Describe(DarlingWebSeat.ReadsOnly with { SessionExpiresAt = expiry }, null, false);

        AssertShape(me, "shared_token", "read_only", expiresAt: "2026-11-14T10:30:15.0000000Z");
    }

    [Fact]
    public void ANamedTokenScopedReader_NamesTheToken_AndNoSubject()
    {
        var me = DarlingWebMe.Describe(new DarlingWebSeat(ScopedName, false, TokenName: ScopedName, AllowedTagIds: [41]), null, false);

        AssertShape(me, "named_token", "read_only", tokenName: ScopedName);
    }

    [Fact]
    public void AManagedGroupSeat_NamesTheGroup_AndNoSubjectOrTokenName()
    {
        var me = DarlingWebMe.Describe(new DarlingWebSeat("Ops", false, TokenName: "Ops", AllowedTagIds: [7], GroupId: Guid.NewGuid(), GroupGeneration: Guid.NewGuid()), null, false);

        AssertShape(me, "managed_group", "read_only", groupName: "Ops");
    }

    private static async Task<(TestServer Server, WebApplication App, NpgsqlDataSource Postgres)> StartAsync(DarlingWebTokenSet? webTokens = null, DarlingWebOidcClient? oidcClient = null, string? accessToken = null)
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
        host.ConfigurePipeline(app, postgres, networkMode: false, networkListenIp: null, allowedCidr: IPNetwork.Parse("127.0.0.1/32"),
            accessToken: accessToken ?? new string('t', 40), oidcClient: oidcClient, requireTokenWhenLoopbackOnly: true,
            sessionSigningKeyForTests: Key, webTokens: webTokens);
        await app.StartAsync(TestContext.Current.CancellationToken);
        return (app.GetTestServer(), app, postgres);
    }

    private static async Task<(int Status, string Body)> GetAsync(TestServer server, string path, string? subject, DateTimeOffset? expiry = null, bool cookie = true)
    {
        var value = DarlingWebHostService.BuildSessionCookieValue(Key, expiry ?? DateTimeOffset.UtcNow.AddHours(1), subject);
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "GET";
            c.Request.Path = path;
            c.Request.Headers.Host = "localhost";
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
            if (cookie)
            {
                c.Request.Headers["Cookie"] = DarlingWebHostService.SessionCookieName + "=" + value;
            }
        }, TestContext.Current.CancellationToken);
        var body = await new StreamReader(ctx.Response.Body).ReadToEndAsync(TestContext.Current.CancellationToken);
        return (ctx.Response.StatusCode, body);
    }

    [Fact]
    public async Task OnTheStore_AnOidcViewerCookie_IsDescribed_WithItsExpiry()
    {
        var (server, app, postgres) = await StartAsync();
        await using var _app = app;
        await using var _pg = postgres;
        var expiry = DateTimeOffset.UtcNow.AddHours(3);
        var subject = DarlingWebSeat.EncodeCookieSubject("viewer@example.test", WebOidcRole.Viewer, OwnerKey, "Vera Viewer", "Analysts");

        var (status, body) = await GetAsync(server, "/api/me", subject, expiry);

        Assert.Equal(StatusCodes.Status200OK, status);
        var seatExpiry = DateTimeOffset.FromUnixTimeSeconds(expiry.ToUnixTimeSeconds());
        AssertShape(JsonNode.Parse(body)!.AsObject(), "oidc", "read_only", "viewer@example.test", "Vera Viewer", "Analysts",
            expiresAt: seatExpiry.UtcDateTime.ToString("O", CultureInfo.InvariantCulture));
    }

    private static DarlingWebOidcClient OidcClient(string[] adminRoles, string[] viewerRoles) => new(new DarlingWebOidcClient.ResolvedOptions(
        "https://idp.example.test", "client-id", null, "openid", null, "groups", adminRoles, viewerRoles));

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OnTheStore_AViewerWithNoOwnerKey_IsToldWhetherThisHostHasARoleMapping(bool mapped)
    {
        var client = OidcClient([], mapped ? ["Viewers"] : []);
        var (server, app, postgres) = await StartAsync(oidcClient: client);
        await using var _app = app;
        await using var _pg = postgres;
        var o1 = DarlingWebSeat.EncodeCookieSubject("viewer@example.test", WebOidcRole.Viewer);

        var (status, body) = await GetAsync(server, "/api/me", o1);

        Assert.Equal(StatusCodes.Status200OK, status);
        var me = JsonNode.Parse(body)!.AsObject();
        Assert.Equal("oidc", (string?)me["method"]);
        Assert.Equal("read_only", (string?)me["seat"]);
        Assert.Null(me["granted_by"]);
        Assert.Equal(mapped, (bool?)me["role_mapping"]);
    }

    [Fact]
    public async Task OnTheStore_AShortSharedToken_SignsInReadOnly_AsTheSharedToken_WithASessionEnd()
    {
        const string shortToken = "tiny";
        var (server, app, postgres) = await StartAsync(accessToken: shortToken);
        await using var _app = app;
        await using var _pg = postgres;
        var signIn = await DarlingWebHostGateLiveTests.PostSignIn(server, "localhost", IPAddress.Loopback, shortToken);
        Assert.Equal(StatusCodes.Status302Found, signIn.Response.StatusCode);
        var setCookie = Assert.Single(signIn.Response.Headers.SetCookie) ?? string.Empty;
        var cookie = setCookie[..setCookie.IndexOf(';')];

        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "GET";
            c.Request.Path = "/api/me";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers["Cookie"] = cookie;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        }, TestContext.Current.CancellationToken);
        var body = await new StreamReader(ctx.Response.Body).ReadToEndAsync(TestContext.Current.CancellationToken);

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        var me = JsonNode.Parse(body)!.AsObject();
        Assert.Equal("shared_token", (string?)me["method"]);
        Assert.Equal("read_only", (string?)me["seat"]);
        Assert.NotNull((string?)me["expires_at"]);
    }

    [Fact]
    public async Task OnTheStore_SessionStillLeavesTheNameOut()
    {
        var (server, app, postgres) = await StartAsync();
        await using var _app = app;
        await using var _pg = postgres;
        var subject = DarlingWebSeat.EncodeCookieSubject("viewer@example.test", WebOidcRole.Viewer, OwnerKey, "Vera Viewer", "Analysts");

        var (status, body) = await GetAsync(server, "/api/session", subject);

        Assert.Equal(StatusCodes.Status200OK, status);
        var session = JsonNode.Parse(body)!.AsObject();
        Assert.False(session.ContainsKey("subject"));
        Assert.False(session.ContainsKey("owner_key"));
        Assert.DoesNotContain("viewer@example.test", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnTheStore_NoCookie_Is401()
    {
        var (server, app, postgres) = await StartAsync();
        await using var _app = app;
        await using var _pg = postgres;

        var (status, _) = await GetAsync(server, "/api/me", null, cookie: false);

        Assert.Equal(StatusCodes.Status401Unauthorized, status);
    }

    [Fact]
    public async Task OnTheStore_AScopedReader_IsAdmitted()
    {
        var set = DarlingWebNamedTokenTests.SetFor(DarlingWebNamedTokenTests.ScopedEntry(ScopedName, DarlingWebNamedTokenTests.ReaderToken, 41));
        var (server, app, postgres) = await StartAsync(set);
        await using var _app = app;
        await using var _pg = postgres;

        var (status, body) = await GetAsync(server, "/api/me", DarlingWebSeat.EncodeTokenCookieSubject(ScopedName));

        Assert.Equal(StatusCodes.Status200OK, status);
        var me = JsonNode.Parse(body)!.AsObject();
        Assert.Equal("named_token", (string?)me["method"]);
        Assert.Equal(ScopedName, (string?)me["token_name"]);
        Assert.Equal("read_only", (string?)me["seat"]);
    }
}
