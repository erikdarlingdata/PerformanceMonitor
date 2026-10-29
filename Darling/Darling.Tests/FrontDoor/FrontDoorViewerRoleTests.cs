/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests.FrontDoor;

/// <summary>
/// A real store's dashboard (the production sign-in gate and endpoint map) on a free loopback port, for the front door to sign in to
/// (#6840). Its pool points at a closed port: every answer these tests compare is decided by the seat before any store read.
/// </summary>
internal sealed class RealStorePeer : IAsyncDisposable
{
    internal const string Token = "real-store-web-token-0123456789abcdef";
    private static readonly byte[] SigningKey = System.Security.Cryptography.RandomNumberGenerator.GetBytes(32);

    private readonly WebApplication _app;
    private readonly NpgsqlDataSource _postgres;
    private readonly HttpClient _client = new(new SocketsHttpHandler { UseCookies = false, AllowAutoRedirect = false });

    private RealStorePeer(WebApplication app, NpgsqlDataSource postgres, Uri baseUri)
    {
        _app = app;
        _postgres = postgres;
        BaseUri = baseUri;
    }

    internal Uri BaseUri { get; }

    internal static async Task<RealStorePeer> StartAsync()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options => options.Listen(IPAddress.Loopback, 0));
        var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=postgres;Username=darling;Timeout=2");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        var app = builder.Build();

        var host = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance, new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState());
        host.ConfigurePipeline(app, postgres, networkMode: false, networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"), accessToken: Token, oidcClient: null,
            requireTokenWhenLoopbackOnly: true, sessionSigningKeyForTests: SigningKey);
        await app.StartAsync(TestContext.Current.CancellationToken);
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new RealStorePeer(app, postgres, new Uri(address.Replace("[::]", "127.0.0.1", StringComparison.Ordinal) + "/"));
    }

    /// <summary>A store-mode request, straight to the store: an OIDC viewer's session, or the shared token's, plus an optional role header.</summary>
    internal async Task<(HttpStatusCode Status, string Body, bool Applied)> GetAsync(string path, bool viewer, string? roleHeader, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(BaseUri, path.TrimStart('/')));
        var subject = viewer ? DarlingWebSeat.EncodeCookieSubject("viewer@example.test", WebOidcRole.Viewer) : null;
        request.Headers.TryAddWithoutValidation("Cookie", DarlingWebHostService.SessionCookieName + "="
            + DarlingWebHostService.BuildSessionCookieValue(SigningKey, DateTimeOffset.UtcNow.AddHours(1), subject));
        if (roleHeader is not null)
        {
            request.Headers.TryAddWithoutValidation(DarlingWebSeat.FrontDoorRoleHeader, roleHeader);
        }

        using var response = await _client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        return (response.StatusCode, body, response.Headers.Contains(DarlingWebSeat.FrontDoorRoleAppliedHeader));
    }

    public async ValueTask DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
        await _postgres.DisposeAsync();
    }
}

/// <summary>
/// #6840: a person whose role cannot edit gets, through the front door, the answer the store gives a viewer. The front door's session at
/// the store is the shared token's, so without the role header the store answered a front-door viewer as an editor: the Admin server
/// form's login name and TLS settings, the investigation export, and the edit flags.
/// </summary>
public sealed class FrontDoorViewerRoleTests
{
    private static async Task<FrontDoorWebHarness> DoorInFrontOf(RealStorePeer store, CapturingTestLogger? logger = null)
    {
        var secrets = new WebFakeSecrets();
        secrets.SetWebToken("east", RealStorePeer.Token);
        var directory = new WebFakeDirectory(new[]
        {
            (new FrontDoorStore("east", new Uri("http://127.0.0.1:1/"), store.BaseUri, null), new[] { "sql-east-1" }),
        });
        return await FrontDoorWebHarness.StartAsync(directory, secrets, logger: logger);
    }

    /// <summary>The two reads a store refuses a viewer, and two that report a viewer's flags.</summary>
    [Theory(Timeout = 60_000)]
    [InlineData("/api/admin/servers/3", HttpStatusCode.Forbidden)]
    [InlineData("/api/investigation-export", HttpStatusCode.Forbidden)]
    [InlineData("/api/active-queries/live/available", HttpStatusCode.OK)]
    [InlineData("/api/analyze-now/available", HttpStatusCode.OK)]
    public async Task AFrontDoorViewer_GetsTheAnswerTheStoreGivesAViewer(string path, HttpStatusCode viewerStatus)
    {
        await using var store = await RealStorePeer.StartAsync();
        await using var door = await DoorInFrontOf(store);

        var (storeStatus, storeBody, _) = await store.GetAsync(path, viewer: true, null, TestContext.Current.CancellationToken);
        Assert.Equal(viewerStatus, storeStatus);

        var (response, body) = await door.SendAsync(door.Request(HttpMethod.Get, path, FrontDoorSeat.Viewer, store: "east"), TestContext.Current.CancellationToken);
        Assert.Equal(storeStatus, response.StatusCode);
        Assert.Equal(storeBody, body);
        if (viewerStatus == HttpStatusCode.OK)
        {
            Assert.Contains("\"can_edit\":false", body, StringComparison.Ordinal);
        }
    }

    [Fact(Timeout = 60_000)]
    public async Task TheRoleHeader_LowersAnEditorsStoreSession_ForThatOneRequest()
    {
        await using var store = await RealStorePeer.StartAsync();

        var (lowered, loweredBody, applied) = await store.GetAsync("/api/session", viewer: false, DarlingWebSeat.FrontDoorViewerRole, TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, lowered);
        Assert.Contains("\"can_edit\":false", loweredBody, StringComparison.Ordinal);
        Assert.True(applied);

        var (refused, _, _) = await store.GetAsync("/api/admin/servers/3", viewer: false, DarlingWebSeat.FrontDoorViewerRole, TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.Forbidden, refused);

        /* The same session with no header is the editor again: the role is decided per request, never stored with the session. */
        var (_, editorBody, editorApplied) = await store.GetAsync("/api/session", viewer: false, null, TestContext.Current.CancellationToken);
        Assert.Contains("\"can_edit\":true", editorBody, StringComparison.Ordinal);
        Assert.False(editorApplied);
    }

    [Theory(Timeout = 60_000)]
    [InlineData("admin")]
    [InlineData("Viewer")]
    [InlineData("viewer, admin")]
    [InlineData("")]
    public async Task AnyOtherRoleValue_ChangesNothing(string value)
    {
        await using var store = await RealStorePeer.StartAsync();

        var (_, editorBody, editorApplied) = await store.GetAsync("/api/session", viewer: false, value, TestContext.Current.CancellationToken);
        Assert.Contains("\"can_edit\":true", editorBody, StringComparison.Ordinal);
        Assert.False(editorApplied);

        var (_, viewerBody, _) = await store.GetAsync("/api/session", viewer: true, value, TestContext.Current.CancellationToken);
        Assert.Contains("\"can_edit\":false", viewerBody, StringComparison.Ordinal);
    }

    [Fact(Timeout = 60_000)]
    public async Task AStoreThatDoesNotConfirmTheRole_IsRefusedForAViewer_With502NamingIt_ButServesAnEditor()
    {
        await using var peer = await FakeWebPeer.StartAsync("use1");
        peer.OlderBuild = true;
        await using var door = await FrontDoorWebHarness.StartAsync(new[] { (peer, new[] { "sql-east-1" }) });

        var (viewer, viewerBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", FrontDoorSeat.Viewer, store: "use1"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.BadGateway, viewer.StatusCode);
        Assert.Contains("use1", viewerBody, StringComparison.Ordinal);
        Assert.DoesNotContain("\"peer\"", viewerBody, StringComparison.Ordinal);

        var (editor, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", FrontDoorSeat.Admin, store: "use1"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, editor.StatusCode);
    }

    /// <summary>
    /// A store answers some requests before its sign-in gate runs (its Host check, a rate limit, a throttle), so those answers carry
    /// no role confirmation and no data. The viewer gets the store's own refusal, not the 502 for a store that is out of date.
    /// </summary>
    [Theory(Timeout = 60_000)]
    [InlineData(429)]
    [InlineData(400)]
    public async Task AStoresRefusalWithoutTheRoleConfirmation_ReachesAViewerAsTheStoresOwnStatus(int status)
    {
        await using var peer = await FakeWebPeer.StartAsync("use1");
        peer.OlderBuild = true;
        if (status == 429)
        {
            peer.RateLimitSeconds = 7;
        }
        else
        {
            peer.AnswerStatus = status;
        }

        await using var door = await FrontDoorWebHarness.StartAsync(new[] { (peer, new[] { "sql-east-1" }) });

        var (viewer, viewerBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", FrontDoorSeat.Viewer, store: "use1"), TestContext.Current.CancellationToken);
        Assert.Equal((HttpStatusCode)status, viewer.StatusCode);
        Assert.DoesNotContain("did not confirm", viewerBody, StringComparison.Ordinal);
    }

    [Fact(Timeout = 60_000)]
    public async Task TheRoleHeader_RidesEveryViewerRequest_IncludingComposeRun_AndNoEditorsRequest()
    {
        await using var peer = await FakeWebPeer.StartAsync("use1");
        await using var door = await FrontDoorWebHarness.StartAsync(new[] { (peer, new[] { "sql-east-1" }) });

        await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", FrontDoorSeat.Viewer, store: "use1"), TestContext.Current.CancellationToken);
        var (compose, _) = await door.SendAsync(door.Request(HttpMethod.Post, "/api/compose/run", FrontDoorSeat.Viewer, store: "use1", body: "{}"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, compose.StatusCode);

        /* A browser's own copy of the header never reaches the store: a viewer who sends another value is still sent as a viewer. */
        var viewerRequest = door.Request(HttpMethod.Get, "/api/fleet", FrontDoorSeat.Viewer, store: "use1");
        viewerRequest.Headers.TryAddWithoutValidation(DarlingWebSeat.FrontDoorRoleHeader, "admin");
        await door.SendAsync(viewerRequest, TestContext.Current.CancellationToken);

        await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", FrontDoorSeat.Admin, store: "use1"), TestContext.Current.CancellationToken);
        await door.SendAsync(door.Request(HttpMethod.Post, "/api/mute-rules", FrontDoorSeat.Admin, store: "use1", body: "{}"), TestContext.Current.CancellationToken);

        var seen = peer.Api.ToArray();
        Assert.Equal(5, seen.Length);
        Assert.All(seen.Take(3), s => Assert.Equal(DarlingWebSeat.FrontDoorViewerRole, s.Headers[DarlingWebSeat.FrontDoorRoleHeader]));
        Assert.All(seen.Skip(3), s => Assert.False(s.Headers.ContainsKey(DarlingWebSeat.FrontDoorRoleHeader)));
    }

    /// <summary>Finding 2: an answer that is not JSON never opens as a page on the front door's address.</summary>
    [Theory(Timeout = 60_000)]
    [InlineData("text/html; charset=utf-8")]
    [InlineData("application/javascript")]
    [InlineData("image/svg+xml")]
    public async Task AStoresAnswerThatIsNotJson_IsSentAsADownload(string contentType)
    {
        await using var peer = await FakeWebPeer.StartAsync("use1");
        peer.AnswerContentType = contentType;
        await using var door = await FrontDoorWebHarness.StartAsync(new[] { (peer, new[] { "sql-east-1" }) });

        var (response, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", FrontDoorSeat.Admin, store: "use1"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("attachment", response.Content.Headers.ContentDisposition?.DispositionType);
        Assert.Equal("nosniff", string.Join(",", response.Headers.GetValues("X-Content-Type-Options").Distinct()));
    }

    [Fact(Timeout = 60_000)]
    public async Task AStoresJsonAnswer_PassesAsItIs()
    {
        await using var peer = await FakeWebPeer.StartAsync("use1");
        await using var door = await FrontDoorWebHarness.StartAsync(new[] { (peer, new[] { "sql-east-1" }) });

        var (response, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", FrontDoorSeat.Admin, store: "use1"), TestContext.Current.CancellationToken);
        Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);
        Assert.Null(response.Content.Headers.ContentDisposition);
    }

    /// <summary>Finding 3: a line break in the address (decoded from %0D%0A) never starts a new line in the front door's log.</summary>
    [Fact(Timeout = 60_000)]
    public async Task ALineBreakInTheAddress_NeverReachesTheLogAsALineBreak()
    {
        await using var peer = await FakeWebPeer.StartAsync("use1");
        peer.AlwaysUnauthorized = true;
        var logger = new CapturingTestLogger();
        var secrets = new WebFakeSecrets();
        secrets.SetWebToken("use1", peer.Token);
        var directory = new WebFakeDirectory(new[] { (new FrontDoorStore("use1", new Uri("http://127.0.0.1:1/"), peer.BaseUri, null), new[] { "sql-east-1" }) });
        await using var door = await FrontDoorWebHarness.StartAsync(directory, secrets, logger: logger);

        var (response, _) = await door.SendAsync(door.Request(
            HttpMethod.Get, "/api/fleet%0D%0A2026-10-28 store use1 /api/x answered 200", FrontDoorSeat.Admin, store: "use1"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);

        var line = Assert.Single(logger.Lines, l => l.Contains("/api/fleet", StringComparison.Ordinal));
        Assert.DoesNotContain('\r', line);
        Assert.DoesNotContain('\n', line);
    }
}
