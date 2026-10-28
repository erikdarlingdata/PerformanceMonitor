/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests.FrontDoor;

/// <summary>Which seat a test request carries a session cookie for. A signed-out request carries none.</summary>
internal enum FrontDoorSeat { Admin, Viewer, SignedOut, NamedAdmin }

/// <summary>The stores and their servers a test front door knows, with the directory's resolve rule: exact names before partial ones,
/// one store routes and two or more are refused.</summary>
internal sealed class WebFakeDirectory : IFrontDoorDirectory
{
    private readonly List<FrontDoorStoreState> _states = new();

    internal WebFakeDirectory(IEnumerable<(FrontDoorStore Store, string[] Servers)> stores)
    {
        foreach (var (store, servers) in stores)
        {
            _states.Add(new FrontDoorStoreState(
                store, FrontDoorStoreHealth.Ok,
                servers.Select((name, i) => new RegisteredServer(i + 1, name, null)).ToList(),
                new DateTime(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc), null));
        }
    }

    internal int RefreshCalls { get; private set; }

    public IReadOnlyList<FrontDoorStoreState> Stores => _states;

    public FrontDoorStore? Find(string name)
        => _states.FirstOrDefault(s => string.Equals(s.Store.Name, name, StringComparison.OrdinalIgnoreCase))?.Store;

    public FrontDoorRoute ResolveServer(string serverName)
    {
        foreach (var tier in new[] { "exact", "partial" })
        {
            var hits = _states.Where(s => ServerNameMatcher.Match(s.Servers, serverName).MatchedBy == tier).Select(s => s.Store.Name).ToList();
            if (hits.Count == 1)
            {
                return new FrontDoorRoute.ToStore(hits[0]);
            }

            if (hits.Count > 1)
            {
                return new FrontDoorRoute.Refused($"'{serverName}' matches servers in {string.Join(", ", hits)}.", Ambiguous: true, Stores: hits);
            }
        }

        return new FrontDoorRoute.Refused($"No store has a server named '{serverName}'.");
    }

    public Task RefreshOnMissAsync(CancellationToken ct)
    {
        RefreshCalls++;
        return Task.CompletedTask;
    }

    public void Hold(string store, TimeSpan span, string why)
    {
    }
}

internal sealed class WebFakeSecrets : IFrontDoorSecrets
{
    private readonly Dictionary<string, string> _web = new(StringComparer.OrdinalIgnoreCase);

    internal void SetWebToken(string store, string token) => _web[store] = token;

    public string? McpToken(string store, bool agentRole) => null;

    public string? WebToken(string store) => _web.TryGetValue(store, out var token) ? token : null;
}

internal sealed class ManualTime : TimeProvider
{
    private DateTimeOffset _now = new(2026, 10, 28, 12, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow() => _now;

    internal void Advance(TimeSpan span) => _now += span;
}

/// <summary>The production web pipeline (sign-in gate, role gate, Host guard, headers) over a front door, in process, with the web proxy
/// where the dashboard's endpoint map would be, in front of fake store peers on real sockets.</summary>
internal sealed class FrontDoorWebHarness : IAsyncDisposable
{
    internal const string FrontDoorToken = "front-door-own-web-token-0123456789";
    private static readonly byte[] SigningKey = System.Security.Cryptography.RandomNumberGenerator.GetBytes(32);

    private readonly WebApplication _app;

    private FrontDoorWebHarness(WebApplication app, HttpClient client, WebFakeDirectory directory, WebFakeSecrets secrets, DarlingFrontDoorWebSession session)
    {
        _app = app;
        Client = client;
        Directory = directory;
        Secrets = secrets;
        Session = session;
    }

    internal HttpClient Client { get; }

    internal WebFakeDirectory Directory { get; }

    internal WebFakeSecrets Secrets { get; }

    internal DarlingFrontDoorWebSession Session { get; }

    internal static Task<FrontDoorWebHarness> StartAsync(
        IReadOnlyList<(FakeWebPeer Peer, string[] Servers)> peers, TimeSpan? peerTimeout = null, TimeProvider? time = null,
        CancellationToken ct = default, ILogger? logger = null)
    {
        var secrets = new WebFakeSecrets();
        var directory = new WebFakeDirectory(peers.Select(p =>
        {
            secrets.SetWebToken(p.Peer.Name, p.Peer.Token);
            return (new FrontDoorStore(p.Peer.Name, new Uri("http://127.0.0.1:1/"), p.Peer.BaseUri, null), p.Servers);
        }));
        return StartAsync(directory, secrets, peerTimeout, time, ct, logger);
    }

    internal static async Task<FrontDoorWebHarness> StartAsync(
        WebFakeDirectory directory, WebFakeSecrets secrets, TimeSpan? peerTimeout = null, TimeProvider? time = null,
        CancellationToken ct = default, ILogger? logger = null)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        var app = builder.Build();

        var session = new DarlingFrontDoorWebSession(secrets, time);
        var proxy = new DarlingFrontDoorWebProxy(directory, session, peerTimeout ?? TimeSpan.FromSeconds(30), logger ?? NullLogger.Instance, time);

        app.Use(next => context =>
        {
            context.Connection.RemoteIpAddress ??= IPAddress.Loopback;
            return next(context);
        });

        var host = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance, new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState());
        host.ConfigurePipeline(app, null, networkMode: false, networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"), accessToken: FrontDoorToken, oidcClient: null,
            requireTokenWhenLoopbackOnly: true, sessionSigningKeyForTests: SigningKey, frontDoorProxy: proxy);
        await app.StartAsync(ct == default ? TestContext.Current.CancellationToken : ct);
        return new FrontDoorWebHarness(app, app.GetTestServer().CreateClient(), directory, secrets, session);
    }

    /// <summary>The Cookie header value for a seat: the shared-token seat edits, an OIDC viewer reads.</summary>
    internal static string? CookieFor(FrontDoorSeat seat) => seat switch
    {
        FrontDoorSeat.Admin => DarlingWebHostService.SessionCookieName + "="
            + DarlingWebHostService.BuildSessionCookieValue(SigningKey, DateTimeOffset.UtcNow.AddHours(1), null),
        FrontDoorSeat.NamedAdmin => DarlingWebHostService.SessionCookieName + "="
            + DarlingWebHostService.BuildSessionCookieValue(
                SigningKey, DateTimeOffset.UtcNow.AddHours(1), DarlingWebSeat.EncodeCookieSubject("alice@example.test", WebOidcRole.Admin)),
        FrontDoorSeat.Viewer => DarlingWebHostService.SessionCookieName + "="
            + DarlingWebHostService.BuildSessionCookieValue(
                SigningKey, DateTimeOffset.UtcNow.AddHours(1), DarlingWebSeat.EncodeCookieSubject("viewer@example.test", WebOidcRole.Viewer)),
        _ => null,
    };

    internal HttpRequestMessage Request(
        HttpMethod method, string url, FrontDoorSeat seat = FrontDoorSeat.Admin, string? store = null, string? body = null,
        string contentType = "application/json")
    {
        var request = new HttpRequestMessage(method, url);
        if (CookieFor(seat) is { } cookie)
        {
            request.Headers.TryAddWithoutValidation("Cookie", cookie);
        }

        if (store is not null)
        {
            request.Headers.TryAddWithoutValidation(DarlingFrontDoorWebProxy.StoreHeader, store);
        }

        if (body is not null)
        {
            request.Content = new StringContent(body, Encoding.UTF8, contentType);
        }

        return request;
    }

    internal async Task<(HttpResponseMessage Response, string Body)> SendAsync(HttpRequestMessage request, CancellationToken ct = default)
    {
        var response = await Client.SendAsync(request, ct == default ? TestContext.Current.CancellationToken : ct);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return (response, body);
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _app.DisposeAsync();
    }
}

/// <summary>
/// #6840 part P4: the front door's web proxy through the real web pipeline, in front of fake store peers. Each test pins one guard: the
/// store's own session and headers stay on the store's side, the front door's session and role gate decide before anything is sent, and
/// a slow, rate-limiting or forgetful store is answered as a store, not as the person's own session.
/// </summary>
public sealed class FrontDoorWebProxyTests
{
    private static readonly string[] UseServers = { "sql-east-1", "sql-shared" };
    private static readonly string[] UsuServers = { "sql-west-1", "sql-shared" };

    private static async Task<(FakeWebPeer A, FakeWebPeer B, FrontDoorWebHarness Door)> TwoStoresAsync(
        CancellationToken ct, TimeSpan? peerTimeout = null, TimeProvider? time = null)
    {
        var a = await FakeWebPeer.StartAsync("use1");
        var b = await FakeWebPeer.StartAsync("usw2");
        var door = await FrontDoorWebHarness.StartAsync(new[] { (a, UseServers), (b, UsuServers) }, peerTimeout, time, ct);
        return (a, b, door);
    }

    private static JsonNode Json(string body) => JsonNode.Parse(body)!;

    [Fact(Timeout = 60_000)]
    public async Task TheStoresSetCookieAndSecurityHeaders_NeverReachTheBrowser()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var (response, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("use1", (string?)Json(body)["peer"]);
        Assert.StartsWith("application/json", response.Content.Headers.ContentType?.ToString());
        Assert.False(response.Headers.Contains("Set-Cookie"), "the store's Set-Cookie reached the browser");
        Assert.False(response.Headers.Contains("X-Peer-Marker"), "a header of the store's own reached the browser");
        Assert.DoesNotContain(FakeWebPeer.PeerCsp, response.Headers.TryGetValues("Content-Security-Policy", out var csp) ? string.Join(";", csp) : string.Empty);
        Assert.Single(a.Api);
    }

    [Fact(Timeout = 60_000)]
    public async Task TheFrontDoorsCookieAndAuthorization_NeverReachAStore_AndTheStoresOwnSessionDoes()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var request = door.Request(HttpMethod.Get, "/api/fleet", store: "use1");
        request.Headers.TryAddWithoutValidation("Authorization", "Bearer front-door-secret");
        request.Headers.TryAddWithoutValidation("Origin", "https://front-door.example.test");
        request.Headers.TryAddWithoutValidation("Referer", "https://front-door.example.test/#/fleet");
        request.Headers.TryAddWithoutValidation("X-Forwarded-For", "203.0.113.9");
        request.Headers.TryAddWithoutValidation("X-Forwarded-Host", "front-door.example.test");
        request.Headers.TryAddWithoutValidation("Accept", "application/json");
        var (response, _) = await door.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var seen = Assert.Single(a.Api);
        Assert.False(seen.Headers.ContainsKey("Authorization"));
        Assert.False(seen.Headers.ContainsKey("Origin"));
        Assert.False(seen.Headers.ContainsKey("Referer"));
        Assert.False(seen.Headers.ContainsKey("X-Forwarded-For"));
        Assert.False(seen.Headers.ContainsKey("X-Forwarded-Host"));
        Assert.False(seen.Headers.ContainsKey(DarlingFrontDoorWebProxy.StoreHeader));
        Assert.Equal("application/json", seen.Headers["Accept"]);

        /* The Cookie header is the store's own session and nothing else. */
        var cookie = seen.Headers["Cookie"];
        Assert.StartsWith(FakeWebPeer.SessionCookie + "=sess-", cookie);
        Assert.DoesNotContain(";", cookie);
        Assert.DoesNotContain(FrontDoorWebHarness.CookieFor(FrontDoorSeat.Admin)!, cookie);

        /* Host is the store's own address, not the front door's. */
        Assert.Equal(a.BaseUri.Authority, seen.Headers["Host"]);
        Assert.Empty(b.Api);
        Assert.Empty(b.Logins);
    }

    [Fact(Timeout = 60_000)]
    public async Task AViewersPost_IsRefusedWith403_AndTheStoreReceivesNothing()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var (response, _) = await door.SendAsync(door.Request(
            HttpMethod.Post, "/api/mute-rules", FrontDoorSeat.Viewer, store: "use1", body: "{\"name\":\"x\"}"));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Empty(a.Api);
        Assert.Empty(a.Logins);

        /* A viewer's read still goes through, so the refusal above is the role and not the route. */
        var (read, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", FrontDoorSeat.Viewer, store: "use1"));
        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
    }

    [Fact(Timeout = 60_000)]
    public async Task AnUnknownStoreName_Is400_AndNothingIsSent()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var (response, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "nowhere"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var json = Json(body);
        Assert.Equal("unknown_store", (string?)json["status"]);
        Assert.Equal(new[] { "use1", "usw2" }, json["stores"]!.AsArray().Select(n => (string?)n).ToArray());
        Assert.Empty(a.Api);
        Assert.Empty(b.Api);
        Assert.Empty(a.Logins);
        Assert.Empty(b.Logins);
    }

    [Theory(Timeout = 60_000)]
    [InlineData("/api/ping")]
    [InlineData("/api/session")]
    [InlineData("/api/front-door")]
    [InlineData("/api/front-door/locate?server=sql-east-1")]
    [InlineData("/api/fleet")]
    public async Task ASignedOutRequest_ToEachLocalRoute_Is401(string url)
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var (response, _) = await door.SendAsync(door.Request(HttpMethod.Get, url, FrontDoorSeat.SignedOut, store: "use1"));

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Empty(a.Api);
        Assert.Empty(a.Logins);
    }

    [Theory(Timeout = 60_000)]
    [InlineData("text/plain")]
    [InlineData("application/x-www-form-urlencoded")]
    public async Task ANonJsonWrite_IsRefused_AndNothingIsSent(string contentType)
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var (response, _) = await door.SendAsync(door.Request(
            HttpMethod.Post, "/api/mute-rules", store: "use1", body: "name=x", contentType: contentType));

        Assert.Equal(HttpStatusCode.UnsupportedMediaType, response.StatusCode);
        Assert.Empty(a.Api);
        Assert.Empty(a.Logins);

        var (json, _) = await door.SendAsync(door.Request(HttpMethod.Post, "/api/mute-rules", store: "use1", body: "{\"name\":\"x\"}"));
        Assert.Equal(HttpStatusCode.OK, json.StatusCode);
        Assert.Equal("{\"name\":\"x\"}", Assert.Single(a.Api).Body);
    }

    [Fact(Timeout = 60_000)]
    public async Task AForwardedChange_LogsOneLineWithWhoStoreMethodPathAndStatus_AndNoSecretOrQuery()
    {
        var logger = new DarlingSelfAlertTests.CapturingLogger();
        var a = await FakeWebPeer.StartAsync("use1");
        var b = await FakeWebPeer.StartAsync("usw2");
        await using var _a = a;
        await using var _b = b;
        await using var door = await FrontDoorWebHarness.StartAsync(
            new[] { (a, UseServers), (b, UsuServers) }, ct: TestContext.Current.CancellationToken, logger: logger);

        var get = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet?x=1", FrontDoorSeat.NamedAdmin, store: "use1"));
        Assert.Equal(HttpStatusCode.OK, get.Response.StatusCode);
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("change", StringComparison.Ordinal));

        var (post, _) = await door.SendAsync(door.Request(
            HttpMethod.Post, "/api/mute-rules?note=secretquery", FrontDoorSeat.NamedAdmin, store: "use1", body: "{\"name\":\"body-text-1234\"}"));
        Assert.Equal(HttpStatusCode.OK, post.StatusCode);

        var line = Assert.Single(logger.Entries, e => e.Level == LogLevel.Information).Message;
        Assert.Contains("alice@example.test", line);
        Assert.Contains("admin", line);
        Assert.Contains("use1", line);
        Assert.Contains("POST", line);
        Assert.Contains("/api/mute-rules", line);
        Assert.Contains("200", line);
        Assert.DoesNotContain("secretquery", line, StringComparison.Ordinal);
        Assert.DoesNotContain("body-text-1234", line, StringComparison.Ordinal);
        Assert.DoesNotContain(a.Token, line, StringComparison.Ordinal);
        Assert.DoesNotContain("Cookie", line, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(DarlingWebHostService.SessionCookieName, line, StringComparison.Ordinal);
    }

    [Fact(Timeout = 60_000)]
    public async Task AnAmbiguousServerName_Is409_AndNothingIsSent()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        /* sql-shared is registered, exactly, in both stores. */
        var (read, readBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet?server=sql-shared"));
        var (write, _) = await door.SendAsync(door.Request(HttpMethod.Put, "/api/server-tags/1?server=sql-shared", body: "{}"));
        var (locate, locateBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/front-door/locate?server=sql-shared"));

        Assert.Equal(HttpStatusCode.Conflict, read.StatusCode);
        Assert.Equal("ambiguous", (string?)Json(readBody)["status"]);
        Assert.Equal(new[] { "use1", "usw2" }, Json(readBody)["stores"]!.AsArray().Select(n => (string?)n).ToArray());
        Assert.Equal(HttpStatusCode.Conflict, write.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, locate.StatusCode);
        Assert.Equal("ambiguous", (string?)Json(locateBody)["status"]);
        Assert.Empty(a.Api);
        Assert.Empty(b.Api);
        Assert.Empty(a.Logins);
        Assert.Empty(b.Logins);
    }

    [Fact(Timeout = 60_000)]
    public async Task ARateLimitedStore_PassesItsStatusAndRetryAfterThrough()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;
        a.RateLimitSeconds = 7;

        var (response, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));

        Assert.Equal((HttpStatusCode)429, response.StatusCode);
        Assert.Equal("7", Assert.Single(response.Headers.GetValues("Retry-After")));
        Assert.Equal("slow down", (string?)Json(body)["error"]);
    }

    [Fact(Timeout = 60_000)]
    public async Task ASlowStore_Gives504_NamingTheStore()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken, peerTimeout: TimeSpan.FromSeconds(1));
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;
        a.Delay = TimeSpan.FromSeconds(20);

        /* The store waits 20 s and the front door gives it 1 s, so a 504 can only come from the timeout cutting the call short. The
           test's own 60 s timeout is the hang guard, and no assertion reads a clock. */
        var (response, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));

        Assert.Equal(HttpStatusCode.GatewayTimeout, response.StatusCode);
        Assert.Equal("use1", (string?)Json(body)["store"]);
        Assert.Contains("use1", (string?)Json(body)["message"]);
    }

    [Fact(Timeout = 60_000)]
    public async Task OneUnauthorizedFromAStore_LeadsToOneNewSignIn_AndOneRetryWithTheSameBody()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var (first, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));
        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal(1, a.SignIns);

        /* The store forgets its sessions (a restart): the next change meets a 401, signs in once more and is sent once more. */
        a.UnauthorizedNext = 1;
        var (write, _) = await door.SendAsync(door.Request(HttpMethod.Post, "/api/mute-rules", store: "use1", body: "{\"name\":\"keep-me\"}"));

        Assert.Equal(HttpStatusCode.OK, write.StatusCode);
        Assert.Equal(2, a.SignIns);
        var writes = a.Api.Where(r => r.Method == "POST").ToList();
        Assert.Equal(2, writes.Count);
        Assert.All(writes, w => Assert.Equal("{\"name\":\"keep-me\"}", w.Body));
    }

    [Fact(Timeout = 60_000)]
    public async Task AStoreThatKeepsAnswering401_IsNotLoopedOn_AndIsNotPassedOnAsThePersonsOwn401()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;
        a.AlwaysUnauthorized = true;

        var (response, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));

        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
        Assert.Equal("use1", (string?)Json(body)["store"]);
        Assert.Equal(2, a.Api.Count);
        Assert.Equal(2, a.SignIns);
    }

    [Fact(Timeout = 60_000)]
    public async Task AClosedRequest_CancelsTheCallToTheStore()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;
        a.Delay = TimeSpan.FromSeconds(30);

        using var closed = new CancellationTokenSource();
        var call = door.Client.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"), closed.Token);
        await a.ApiStarted.Task.WaitAsync(TimeSpan.FromSeconds(20), TestContext.Current.CancellationToken);

        closed.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call);

        var done = await Task.WhenAny(a.ApiAborted.Task, Task.Delay(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Same(a.ApiAborted.Task, done);
    }

    [Fact(Timeout = 60_000)]
    public async Task AStoreThatRefusesTheSignIn_IsHeldFor60Seconds_AndAnswered502NamingIt()
    {
        var time = new ManualTime();
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken, time: time);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;
        door.Secrets.SetWebToken("use1", "not-the-token");

        var (first, firstBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));
        var (second, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));

        Assert.Equal(HttpStatusCode.BadGateway, first.StatusCode);
        Assert.Contains("use1", (string?)Json(firstBody)["message"]);
        Assert.DoesNotContain("not-the-token", firstBody);
        Assert.Equal(HttpStatusCode.BadGateway, second.StatusCode);
        Assert.Single(a.Logins);
        Assert.Empty(a.Api);

        /* The other store is untouched by it. */
        var (other, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "usw2"));
        Assert.Equal(HttpStatusCode.OK, other.StatusCode);

        /* After the hold the front door tries again, and a corrected token signs in. */
        time.Advance(TimeSpan.FromSeconds(61));
        door.Secrets.SetWebToken("use1", a.Token);
        var (third, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));
        Assert.Equal(HttpStatusCode.OK, third.StatusCode);
        Assert.Equal(2, a.Logins.Count);
    }

    [Fact(Timeout = 60_000)]
    public async Task ASignInRateLimit_HoldsTheStoreForItsRetryAfter_WhenThatIsLongerThanTheDefault()
    {
        var time = new ManualTime();
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken, time: time);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;
        a.SignInRateLimitSeconds = 300;

        var (first, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));
        time.Advance(TimeSpan.FromSeconds(120));
        var (second, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));

        Assert.Equal(HttpStatusCode.BadGateway, first.StatusCode);
        Assert.Equal(HttpStatusCode.BadGateway, second.StatusCode);
        Assert.Single(a.Logins);
    }

    [Fact(Timeout = 60_000)]
    public async Task ConcurrentFirstRequests_SignInOnce_AndShareTheSession()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var calls = Enumerable.Range(0, 8)
            .Select(_ => door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1")))
            .ToList();
        var results = await Task.WhenAll(calls);

        Assert.All(results, r => Assert.Equal(HttpStatusCode.OK, r.Response.StatusCode));
        Assert.Single(a.Logins);
        Assert.Equal(8, a.Api.Count);
    }

    [Fact(Timeout = 60_000)]
    public async Task TheTargetStore_IsTheHeader_ThenTheServerName_ThenTheOnlyStore()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        /* The header wins over a server name that lives in another store. */
        await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet?server=sql-east-1", store: "usw2"));
        Assert.Single(b.Api);
        Assert.Empty(a.Api);

        /* A server name alone is found through the directory. */
        var (byServer, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet?server=sql-east-1"));
        Assert.Equal(HttpStatusCode.OK, byServer.StatusCode);
        Assert.Equal("?server=sql-east-1", Assert.Single(a.Api).Query);

        /* Nothing to go on, with two stores: 400 and the list. */
        var (none, noneBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet"));
        Assert.Equal(HttpStatusCode.BadRequest, none.StatusCode);
        Assert.Equal("store_required", (string?)Json(noneBody)["status"]);
        Assert.Equal(new[] { "use1", "usw2" }, Json(noneBody)["stores"]!.AsArray().Select(n => (string?)n).ToArray());

        /* A change needs the header or an exact name: a partial name is refused. */
        var (partial, _) = await door.SendAsync(door.Request(HttpMethod.Put, "/api/server-tags/1?server=east", body: "{}"));
        Assert.Equal(HttpStatusCode.BadRequest, partial.StatusCode);
        var (exact, _) = await door.SendAsync(door.Request(HttpMethod.Put, "/api/server-tags/1?server=sql-east-1", body: "{}"));
        Assert.Equal(HttpStatusCode.OK, exact.StatusCode);
        Assert.Equal(2, a.Api.Count);
        Assert.Single(b.Api);
    }

    [Fact(Timeout = 60_000)]
    public async Task WithOneStore_AReadNeedsNoHeader_ButAChangeStillDoes()
    {
        var only = await FakeWebPeer.StartAsync("only");
        await using var _only = only;
        await using var door = await FrontDoorWebHarness.StartAsync(new[] { (only, new[] { "sql-1" }) }, ct: TestContext.Current.CancellationToken);

        var (read, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet"));
        var (ghost, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet?server=not-a-server"));
        var (write, writeBody) = await door.SendAsync(door.Request(HttpMethod.Post, "/api/mute-rules", body: "{}"));

        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
        Assert.Equal(HttpStatusCode.OK, ghost.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, write.StatusCode);
        Assert.Equal("store_required", (string?)Json(writeBody)["status"]);
        Assert.Equal(2, only.Api.Count);
        Assert.True(door.Directory.RefreshCalls >= 1, "a name no store knows asks the directory to refresh once");
    }

    [Fact(Timeout = 60_000)]
    public async Task TheLocalRoutes_AnswerTheirShapes_AndOnlyReadEachStoresOwnSession()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var (session, sessionBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/session"));
        var (viewer, viewerBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/session", FrontDoorSeat.Viewer));
        var (stores, storesBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/front-door"));
        var (located, locatedBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/front-door/locate?server=sql-west-1"));
        var (missing, missingBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/front-door/locate?server=ghost"));
        var (ping, pingBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/ping"));
        var (post, _) = await door.SendAsync(door.Request(HttpMethod.Post, "/api/front-door", body: "{}"));
        var (under, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/front-door/anything-else"));

        Assert.Equal(HttpStatusCode.OK, session.StatusCode);
        Assert.True((bool)Json(sessionBody)["front_door"]!);
        Assert.False((bool)Json(sessionBody)["can_own"]!);
        Assert.True((bool)Json(sessionBody)["can_edit"]!);
        Assert.False((bool)Json(viewerBody)["can_edit"]!);
        Assert.True((bool)Json(viewerBody)["front_door"]!);

        Assert.Equal(HttpStatusCode.OK, stores.StatusCode);
        var list = Json(storesBody);
        Assert.True((bool)list["front_door"]!);
        var first = list["stores"]!.AsArray()[0]!;
        Assert.Equal("use1", (string?)first["name"]);
        Assert.Equal("ok", (string?)first["health"]);
        Assert.Equal(2, (int)first["server_count"]!);
        Assert.Equal("2026-10-28T12:00:00Z", (string?)first["last_ok_utc"]);

        Assert.Equal(HttpStatusCode.OK, located.StatusCode);
        Assert.Equal("usw2", (string?)Json(locatedBody)["store"]);
        Assert.Equal("sql-west-1", (string?)Json(locatedBody)["server_name"]);
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
        Assert.Equal("not_found", (string?)Json(missingBody)["status"]);
        Assert.Equal(HttpStatusCode.OK, ping.StatusCode);
        Assert.Equal("ok", (string?)Json(pingBody)["status"]);
        Assert.Equal(HttpStatusCode.MethodNotAllowed, post.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, under.StatusCode);

        /* The only thing a local route asks a store is its own /api/session, for admin_writes_off. */
        Assert.All(a.Api, seen => Assert.Equal("/api/session", seen.Path));
        Assert.All(b.Api, seen => Assert.Equal("/api/session", seen.Path));
    }

    [Fact(Timeout = 60_000)]
    public async Task AStoreRunningWithAdminWritesOff_IsReportedSoInTheStoreList_AndAStoreWithThemOnIsNot()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;
        b.AdminWritesOff = true;

        var (response, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/front-door"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var list = Json(body)["stores"]!.AsArray();
        Assert.False((bool)list[0]!["admin_writes_off"]!);
        Assert.True((bool)list[1]!["admin_writes_off"]!);
        Assert.False((bool)list[0]!["managed_store"]!);
        Assert.True((bool)list[1]!["managed_store"]!);
        /* The front door's own /api/session still carries no store's flag: the page takes it from the store in use. */
        var (_, sessionBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/session"));
        Assert.False((bool)Json(sessionBody)["admin_writes_off"]!);
    }

    [Fact(Timeout = 60_000)]
    public async Task AStoreThatCannotBeReadForItsSession_HasNoFlagInTheStoreList_AndTheOthersStillDo()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;
        a.AnswerStatus = 500;
        b.AdminWritesOff = true;

        var (response, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/front-door"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var list = Json(body)["stores"]!.AsArray();
        Assert.Equal("use1", (string?)list[0]!["name"]);
        Assert.Null(list[0]!["admin_writes_off"]);
        Assert.Null(list[0]!["managed_store"]);
        Assert.True((bool)list[1]!["admin_writes_off"]!);
    }

    [Fact(Timeout = 60_000)]
    public async Task TheSignInRoutesAndTheStaticPages_AreServedByTheFrontDoor_NeverForwarded()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var login = await door.Client.GetAsync("/auth/login", TestContext.Current.CancellationToken);
        var page = await door.SendAsync(door.Request(HttpMethod.Get, "/index.html"));

        Assert.NotEqual(HttpStatusCode.BadGateway, login.StatusCode);
        Assert.Equal(HttpStatusCode.OK, login.StatusCode);
        Assert.Equal(HttpStatusCode.OK, page.Response.StatusCode);
        Assert.Empty(a.Api);
        Assert.Empty(a.Logins);
        Assert.Empty(b.Api);
        Assert.Empty(b.Logins);
    }

    [Fact(Timeout = 60_000)]
    public async Task AStoreWithNoWebAddress_Is502_NamingIt()
    {
        var directory = new WebFakeDirectory(new[] { (new FrontDoorStore("mcp-only", new Uri("http://127.0.0.1:1/"), null, null), new[] { "sql-1" }) });
        await using var door = await FrontDoorWebHarness.StartAsync(directory, new WebFakeSecrets(), ct: TestContext.Current.CancellationToken);

        var (response, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "mcp-only"));

        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
        Assert.Equal("mcp-only", (string?)Json(body)["store"]);
        Assert.Equal("store_unavailable", (string?)Json(body)["status"]);
    }

    [Fact(Timeout = 60_000)]
    public async Task AStoreThatAnswersARedirect_Is502_AndNoLocationReachesTheBrowser()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;
        a.RedirectTo = "https://elsewhere.example.test/api/fleet";

        var (response, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/fleet", store: "use1"));

        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
        Assert.False(response.Headers.Contains("Location"), "a store's redirect reached the browser");
        Assert.Equal("use1", (string?)Json(body)["store"]);
        Assert.Contains("redirect", (string?)Json(body)["message"]);
    }

    [Fact(Timeout = 60_000)]
    public async Task AChangeBodyOverTheLimit_Is413_AndNothingIsSent()
    {
        var (a, b, door) = await TwoStoresAsync(TestContext.Current.CancellationToken);
        await using var _a = a;
        await using var _b = b;
        await using var _door = door;

        var tooLarge = "{\"note\":\"" + new string('x', DarlingFrontDoorWebProxy.MaxBodyBytes) + "\"}";
        var (response, _) = await door.SendAsync(door.Request(HttpMethod.Post, "/api/mute-rules", store: "use1", body: tooLarge));

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Empty(a.Api);
        Assert.Empty(a.Logins);
    }

    [Theory]
    [InlineData("/api/../admin/purge")]
    [InlineData("/api/%2e%2e/admin/purge")]
    [InlineData("//elsewhere.example.test/api/fleet")]
    [InlineData("/auth/logout")]
    [InlineData("/index.html")]
    public void APathOutsideTheStoresApiTree_IsRefused_BeforeAnythingIsSent(string path)
    {
        var webUrl = new Uri("http://127.0.0.1:5000/");

        Assert.Throws<FrontDoorPeerUnavailableException>(() => DarlingFrontDoorWebProxy.PeerUri(webUrl, path, null));
        Assert.Throws<FrontDoorPeerUnavailableException>(() => DarlingFrontDoorWebProxy.PeerUri(new Uri("http://127.0.0.1:5000/darling/"), path, null));
    }

    [Fact]
    public void AnApiPath_StaysUnderTheStoresConfiguredBase()
    {
        var target = DarlingFrontDoorWebProxy.PeerUri(new Uri("https://store.example.test:8443/darling/"), "/api/fleet", "?server=a%20b");

        Assert.Equal("https://store.example.test:8443/darling/api/fleet?server=a%20b", target.AbsoluteUri);
    }
}
