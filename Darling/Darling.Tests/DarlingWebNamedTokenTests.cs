/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7735: named web dashboard tokens with a role (<c>web.network.tokens</c>), the twin of <c>mcp.tokens</c>. The live cases drive the
/// REAL <c>DarlingWebHostService.ConfigurePipeline</c> over <see cref="TestServer"/>, the way <see cref="DarlingWebHostGateLiveTests"/>
/// does: a reader token signs in and reads, is refused every write route by the one write gate, an operator token is not, the shared
/// token keeps its edit seat beside them, and a token removed from the set ends its sessions on the next request. The unit cases cover
/// the start check (names, roles, values, duplicates, the MCP token rule) and the constant-time match.
/// </summary>
public sealed class DarlingWebNamedTokenTests
{
    private const string ListenIp = "192.168.1.205";
    private const string AllowedCidr = "192.168.1.0/24";
    private const string SharedToken = "shared-token-value-0123456789";
    private const string ReaderToken = "reader-token-value-abcdefghijk-1";
    private const string OperatorToken = "operator-token-value-lmnopqrst-2";
    private static readonly IPAddress InCidrRemote = IPAddress.Parse("192.168.1.50");

    /// <summary>One signing key for every server a test builds, so a cookie minted by one server is valid on another and the
    /// revocation cases can present it there; production keeps a per-process random key.</summary>
    private static readonly byte[] SigningKey = Enumerable.Range(0, 32).Select(i => (byte)(i * 7 + 3)).ToArray();

    private static WebTokenEntry Entry(string? name, string? token, string? role) => new() { Name = name, Token = token, Role = role };

    private static WebNetworkConfig Network(params WebTokenEntry?[] entries) => new() { Tokens = entries.ToList()! };

    private static List<string> Check(WebNetworkConfig network, out List<DarlingWebTokenSet.Entry> usable, string shared = SharedToken, IReadOnlyList<string>? mcp = null) =>
        DarlingWebTokenSet.Check(network, shared, mcp, DarlingWebTokenSet.ResolveEntry, out usable);

    private static DarlingWebTokenSet SetFor(params WebTokenEntry[] entries) =>
        DarlingWebTokenSet.Resolve(Network(entries), SharedToken, null, null);

    private static DarlingWebTokenSet ReaderAndOperator() =>
        SetFor(Entry("readers", ReaderToken, "reader"), Entry("ops", OperatorToken, "operator"));

    /* ---------------------------------------------------------------- the start check ---------------------------------------------------------------- */

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("has space")]
    [InlineData("a/b")]
    public void Check_RefusesABlankOrInvalidName(string? name)
    {
        var problems = Check(Network(Entry(name, ReaderToken, "reader")), out var usable);

        Assert.Empty(usable);
        var problem = Assert.Single(problems);
        Assert.Contains("web.network.tokens[0] has no valid name", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_RefusesANameOver64Characters()
    {
        var problems = Check(Network(Entry(new string('a', 65), ReaderToken, "reader")), out var usable);

        Assert.Empty(usable);
        Assert.Contains(problems, p => p.Contains("has no valid name", StringComparison.Ordinal));
    }

    /// <summary>A token's name becomes its <c>updated_by</c> stamp and its <c>person:</c> journal actor, so a name a built-in writer already
    /// stamps as itself (web, mcp, cli, viewer) or an mcp.tokens placeholder (invalid, unclaimed) is refused in any case, and the finding
    /// names the entry.</summary>
    [Theory]
    [InlineData("web")]
    [InlineData("MCP")]
    [InlineData("unclaimed")]
    [InlineData("Invalid")]
    [InlineData("cli")]
    [InlineData("Viewer")]
    public void Check_RefusesAReservedName_ABuiltInStampOrAnMcpPlaceholder(string name)
    {
        var problems = Check(Network(Entry(name, ReaderToken, "operator")), out var usable);

        Assert.Empty(usable);
        var problem = Assert.Single(problems);
        Assert.Contains("web.network.tokens[0] is named " + name + ", which is reserved", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("web-ops")]
    [InlineData("mcp.readers")]
    [InlineData("unclaimed2")]
    public void Check_ServesANameThatOnlyContainsAReservedWord(string name)
    {
        var problems = Check(Network(Entry(name, ReaderToken, "reader")), out var usable);

        Assert.Empty(problems);
        Assert.Equal(name, Assert.Single(usable).Name);
    }

    [Theory]
    [InlineData("reader", "reader", false)]
    [InlineData("Reader", "reader", false)]
    [InlineData("operator", "operator", true)]
    [InlineData(" OPERATOR ", "operator", true)]
    public void Check_ServesTheTwoSeats_UnderTheMcpWords(string role, string normalized, bool canEdit)
    {
        var problems = Check(Network(Entry("t", ReaderToken, role)), out var usable);

        Assert.Empty(problems);
        var entry = Assert.Single(usable);
        Assert.Equal(normalized, entry.Role);
        Assert.Equal(canEdit, entry.CanEdit);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("agent")]
    [InlineData("editor")]
    [InlineData("viewer")]
    [InlineData("admin")]
    public void Check_RefusesARoleThatIsNotAWebSeat(string? role)
    {
        var problems = Check(Network(Entry("t", ReaderToken, role)), out var usable);

        Assert.Empty(usable);
        var problem = Assert.Single(problems);
        Assert.Contains("web.network.tokens[0] (t) has no valid role", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_RefusesAnEntryWithNoValue_AndOneUnderTheFloor()
    {
        var problems = Check(Network(Entry("blank", null, "reader"), Entry("short", "aaaaaaaaaaaaaaaaaaaa", "reader")), out var usable);

        Assert.Empty(usable);
        Assert.Equal(2, problems.Count);
        Assert.Contains("web.network.tokens[0] (blank)'s token could not be read", problems[0], StringComparison.Ordinal);
        Assert.Contains("web.network.tokens[1] (short)'s token", problems[1], StringComparison.Ordinal);
        Assert.DoesNotContain("aaaaaaaaaaaaaaaaaaaa", problems[1], StringComparison.Ordinal);
    }

    [Fact]
    public void Check_RefusesDuplicateNames_NeitherServed_AndKeepsTheOthers()
    {
        var problems = Check(Network(Entry("readers", ReaderToken, "reader"), Entry("readers", OperatorToken, "operator"), Entry("ops", "third-token-value-0123456789-3", "operator")), out var usable);

        Assert.Equal("ops", Assert.Single(usable).Name);
        var problem = Assert.Single(problems);
        Assert.Equal("Two web.network.tokens entries are named readers. Neither is served: each name must be unique.", problem);
    }

    [Fact]
    public void Check_RefusesDuplicateValues_NoneServed()
    {
        var problems = Check(Network(Entry("a", ReaderToken, "reader"), Entry("b", ReaderToken, "operator")), out var usable);

        Assert.Empty(usable);
        var problem = Assert.Single(problems);
        Assert.StartsWith("These web.network.tokens entries share a token value with another web token: a, b.", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_RefusesAValueTheSharedTokenHas()
    {
        var problems = Check(Network(Entry("same", SharedToken, "reader")), out var usable);

        Assert.Empty(usable);
        Assert.Contains(problems, p => p.StartsWith("These web.network.tokens entries share a token value with another web token: same.", StringComparison.Ordinal));
        Assert.All(problems, p => Assert.DoesNotContain(SharedToken, p, StringComparison.Ordinal));
    }

    [Fact]
    public void Check_RefusesAValueAnMcpTokenHas()
    {
        var problems = Check(Network(Entry("agentish", ReaderToken, "reader"), Entry("ops", OperatorToken, "operator")), out var usable, mcp: new[] { "some-other-mcp-token-0123456789", ReaderToken });

        Assert.Equal("ops", Assert.Single(usable).Name);
        var problem = Assert.Single(problems);
        Assert.StartsWith("These web.network.tokens entries have the value of an MCP token: agentish.", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_WithNoTokensBlock_FindsNothing()
    {
        Assert.Empty(Check(new WebNetworkConfig(), out var usable));
        Assert.Empty(usable);
        Assert.Empty(DarlingWebTokenSet.Check(null, SharedToken, null, DarlingWebTokenSet.ResolveEntry, out _));
    }

    [Fact]
    public void Match_ComparesEveryToken_AndNeverStopsEarly()
    {
        var compares = 0;
        var set = new DarlingWebTokenSet(ReaderAndOperator().Named)
        {
            Compare = (presented, expected) => { compares++; return string.Equals(presented, expected, StringComparison.Ordinal); },
        };

        Assert.Equal("readers", set.Match(ReaderToken)?.Name);
        Assert.Equal(2, compares);
        compares = 0;
        Assert.Null(set.Match("nothing-like-it"));
        Assert.Equal(2, compares);
        Assert.Null(set.Match(""));
        Assert.Null(set.Find("nobody"));
        Assert.True(set.Find("ops")!.CanEdit);
    }

    [Fact]
    public void Resolve_LogsEachProblemAtCritical_AndNeverAValue()
    {
        var logger = new CapturingTestLogger();
        var set = DarlingWebTokenSet.Resolve(Network(Entry("readers", ReaderToken, "reader"), Entry("bad", OperatorToken, "agent")), SharedToken, null, logger);

        Assert.Equal("readers", Assert.Single(set.Named).Name);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Critical));
        Assert.Contains("(bad) has no valid role", logger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain(OperatorToken, logger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void UnknownKeys_NamesAMistypedKeyInsideATokenEntry()
    {
        const string json = """
            { "web": { "enabled": true, "network": { "encryptedToken": "x", "tokens": [ { "name": "readers", "token": "t", "rol": "reader" } ] } } }
            """;

        Assert.Contains("web.network.tokens[0].rol", DarlingUnknownKeys.Find(json));
    }

    [Fact]
    public async Task ValidateConfig_ReportsTheNamedWebTokenFindings_WithoutAValue()
    {
        var config = new DarlingConfig
        {
            Web = new WebConfig { Network = new WebNetworkConfig { Token = SharedToken, Tokens = new List<WebTokenEntry> { Entry("a", ReaderToken, "reader"), Entry("a", OperatorToken, "operator") } } },
        };
        var output = new StringWriter();

        await DarlingCliCommands.ReportAgentDispatchAsync(config, output, CancellationToken.None);

        var text = output.ToString();
        Assert.Contains("WARNING: Two web.network.tokens entries are named a.", text, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, text, StringComparison.Ordinal);
        Assert.DoesNotContain(OperatorToken, text, StringComparison.Ordinal);
    }

    /* ---------------------------------------------------------------- the live pipeline ---------------------------------------------------------------- */

    /// <summary>The real pipeline, as <see cref="DarlingWebHostGateLiveTests"/> builds it, in network mode with the shared token and
    /// <paramref name="webTokens"/>. The signing key is fixed so a second server reads the first one's cookie.</summary>
    private static async Task<TestServer> BuildServer(DarlingWebTokenSet? webTokens, CapturingTestLogger? hostLogger = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        var app = builder.Build();

        var host = new DarlingWebHostService(
            hostLogger is null ? Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingWebHostService>.Instance : new HostLogger(hostLogger),
            new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache());

        host.ConfigurePipeline(
            app, postgres, networkMode: true,
            networkListenIp: IPAddress.Parse(ListenIp), allowedCidr: IPNetwork.Parse(AllowedCidr),
            accessToken: SharedToken, oidcClient: null,
            sessionSigningKeyForTests: SigningKey,
            webTokens: webTokens);

        await app.StartAsync();
        return app.GetTestServer();
    }

    private sealed class HostLogger(CapturingTestLogger inner) : ILogger<DarlingWebHostService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => inner.Log(logLevel, eventId, state, exception, formatter);
    }

    /// <summary>Signs in through the form POST and returns the session cookie (<c>name=value</c>) the 302 set.</summary>
    private static async Task<string> SignIn(TestServer server, string token)
    {
        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, token);
        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        var setCookie = Assert.Single(ctx.Response.Headers.SetCookie) ?? string.Empty;
        return setCookie[..setCookie.IndexOf(';')];
    }

    private static async Task<(HttpContext Context, string Body)> Send(TestServer server, string method, string path, string? cookie)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = method;
            c.Request.Path = path;
            c.Request.Headers.Host = ListenIp;
            c.Connection.RemoteIpAddress = InCidrRemote;
            if (cookie is not null) c.Request.Headers["Cookie"] = cookie;
        });
        return (ctx, await new StreamReader(ctx.Response.Body).ReadToEndAsync());
    }

    [Fact]
    public async Task AReaderToken_SignsIn_AndReadsTheSession_AsAReadOnlySeat()
    {
        var hostLogger = new CapturingTestLogger();
        using var server = await BuildServer(ReaderAndOperator(), hostLogger);
        var cookie = await SignIn(server, ReaderToken);

        var (ctx, body) = await Send(server, "GET", "/api/session", cookie);

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Contains("\"can_edit\":false", body, StringComparison.Ordinal);
        Assert.Contains("\"can_own\":false", body, StringComparison.Ordinal);
        Assert.Contains("\"signed_in_admin\":false", body, StringComparison.Ordinal);
        // The audit line names the token and its role, never the value.
        Assert.Contains("Web dashboard sign-in: token readers as reader (read-only)", hostLogger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, hostLogger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReaderToken_PassesTheGate_ForAPageRead()
    {
        using var server = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(server, ReaderToken);

        // A path nothing maps answers 404 once the gate lets the request through; the sign-in form would be 200 text/html.
        var (ctx, _) = await Send(server, "GET", "/some/unmapped/page", cookie);

        Assert.Equal(StatusCodes.Status404NotFound, ctx.Response.StatusCode);
    }

    /// <summary>Every write route the web host maps, by method and path. The gate refuses them by METHOD, before routing, so this list
    /// is the proof that the enumerated surface is covered, not the mechanism: a mapped route that is not here is still refused.</summary>
    public static TheoryData<string, string> WriteRoutes => new()
    {
        // Admin: servers, people, agent changes
        { "POST", "/api/servers" }, { "PATCH", "/api/servers/3" }, { "PUT", "/api/servers/3/actions/kill_session" },
        { "PUT", "/api/people/add" }, { "PUT", "/api/agent-changes/switch" }, { "PUT", "/api/agent-changes/7/undo" },
        // Alerting: rules, settings, routes, mute rules, dismiss
        { "POST", "/api/alerts" }, { "PUT", "/api/alerts/3" }, { "DELETE", "/api/alerts/3" }, { "POST", "/api/alerts/validate" }, { "POST", "/api/alerts/test" },
        { "PUT", "/api/alert-settings/daily-report" }, { "PUT", "/api/alert-settings/database-protection" },
        { "PUT", "/api/threshold-overrides" }, { "PUT", "/api/environment-profiles" },
        { "POST", "/api/notification-routes" }, { "PUT", "/api/notification-routes/2" }, { "DELETE", "/api/notification-routes/2" },
        { "POST", "/api/mute-rules" }, { "PATCH", "/api/mute-rules/abc" }, { "PUT", "/api/mute-rules/abc/enabled" }, { "DELETE", "/api/mute-rules/abc" },
        { "POST", "/api/alert-history/dismiss" },
        // Composer saves and known patterns
        { "POST", "/api/views" }, { "PUT", "/api/views/3" }, { "DELETE", "/api/views/3" },
        { "POST", "/api/known-patterns" }, { "PUT", "/api/known-patterns/3" }, { "DELETE", "/api/known-patterns/3" },
        // Tags
        { "POST", "/api/server-tags" }, { "PATCH", "/api/server-tags/3" }, { "DELETE", "/api/server-tags/3" }, { "POST", "/api/server-tags/3/servers" }, { "DELETE", "/api/server-tags/3/servers" },
        { "POST", "/api/query-tags" }, { "PATCH", "/api/query-tags/3" }, { "DELETE", "/api/query-tags/3" }, { "POST", "/api/query-tags/3/assignments" }, { "DELETE", "/api/query-tags/3/assignments" },
        { "POST", "/api/query-tag-rules" }, { "PATCH", "/api/query-tag-rules/3" }, { "DELETE", "/api/query-tag-rules/3" },
        // Actions on a monitored server
        { "POST", "/api/collectors/run" }, { "POST", "/api/analyze-now" }, { "POST", "/api/active-queries/live" },
        // Investigations
        { "PUT", "/api/investigation-notes" }, { "PUT", "/api/investigation-notes/3/approve" }, { "PUT", "/api/investigation-notes/3/reject" },
        { "PUT", "/api/investigation-requests/r1/close" }, { "PUT", "/api/investigation-practice-requests/r1" }, { "PUT", "/api/investigation-feedback/r1" },
        // The proposal presses: a token seat has no owner key, so the #7109 exception does not open for it
        { "POST", "/api/proposals/5f3a6d2e-9b1c-4e7f-8a2d-1c3b5e7f9a0b/first-press" }, { "POST", "/api/proposals/5f3a6d2e-9b1c-4e7f-8a2d-1c3b5e7f9a0b/confirm" },
        // A write route that does not exist yet is born gated
        { "POST", "/api/anything/new" }, { "PATCH", "/api/compose/run" },
    };

    [Theory]
    [MemberData(nameof(WriteRoutes))]
    public async Task AReaderToken_IsRefusedEveryWriteRoute_ByTheOneGate(string method, string path)
    {
        using var server = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(server, ReaderToken);

        var (ctx, body) = await Send(server, method, path, cookie);

        Assert.Equal(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);
        Assert.Contains(DarlingWebEndpoints.ReadOnlyAccountText, body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReaderToken_StillRunsComposedPanels_TheOneUnsafeMethodRead()
    {
        using var server = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(server, ReaderToken);

        var (ctx, _) = await Send(server, "POST", "/api/compose/run", cookie);

        // Past the gate: whatever the handler answers an empty body, it is not the gate's 403.
        Assert.NotEqual(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);
    }

    [Fact]
    public async Task AnOperatorToken_SignsInToTheEditSeat_AndIsNotRefusedAWrite()
    {
        var hostLogger = new CapturingTestLogger();
        using var server = await BuildServer(ReaderAndOperator(), hostLogger);
        var cookie = await SignIn(server, OperatorToken);

        var (session, body) = await Send(server, "GET", "/api/session", cookie);
        Assert.Equal(StatusCodes.Status200OK, session.Response.StatusCode);
        Assert.Contains("\"can_edit\":true", body, StringComparison.Ordinal);
        Assert.Contains("\"signed_in_admin\":true", body, StringComparison.Ordinal);
        Assert.Contains("\"can_own\":false", body, StringComparison.Ordinal);

        // A write to a path nothing maps: the gate lets it through (404 from routing), where the reader seat gets 403.
        var (write, _) = await Send(server, "POST", "/api/anything/new", cookie);
        Assert.Equal(StatusCodes.Status404NotFound, write.Response.StatusCode);
        Assert.Contains("Web dashboard sign-in: token ops as operator (edit)", hostLogger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheSharedToken_KeepsItsEditSeat_BesideNamedTokens()
    {
        using var server = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(server, SharedToken);

        var (session, body) = await Send(server, "GET", "/api/session", cookie);
        Assert.Contains("\"can_edit\":true", body, StringComparison.Ordinal);
        var (write, _) = await Send(server, "POST", "/api/anything/new", cookie);
        Assert.Equal(StatusCodes.Status404NotFound, write.Response.StatusCode);
    }

    [Fact]
    public async Task AWrongToken_GetsTheSignInFormAgain_AndNoCookie()
    {
        using var server = await BuildServer(ReaderAndOperator());

        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, "not-any-of-them-0123456789");

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", ctx.Response.ContentType);
        Assert.Equal(0, ctx.Response.Headers.SetCookie.Count);
    }

    [Fact]
    public async Task ARemovedToken_EndsItsSession_OnTheNextRequest()
    {
        using var before = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(before, ReaderToken);
        var (still, _) = await Send(before, "GET", "/api/session", cookie);
        Assert.Equal(StatusCodes.Status200OK, still.Response.StatusCode);

        // The same signing key, the readers entry gone: the cookie is cryptographically valid and names a token that no longer exists.
        using var after = await BuildServer(SetFor(Entry("ops", OperatorToken, "operator")));

        var (api, apiBody) = await Send(after, "GET", "/api/session", cookie);
        Assert.Equal(StatusCodes.Status401Unauthorized, api.Response.StatusCode);
        Assert.Contains("session expired", apiBody, StringComparison.Ordinal);

        var (page, _) = await Send(after, "GET", "/", cookie);
        Assert.Equal(StatusCodes.Status200OK, page.Response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", page.Response.ContentType);
    }

    [Fact]
    public async Task ATokenCookie_IsRefusedWhereNoNamedTokenIsServed()
    {
        using var before = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(before, OperatorToken);

        using var after = await BuildServer(webTokens: null);

        var (api, _) = await Send(after, "GET", "/api/session", cookie);
        Assert.Equal(StatusCodes.Status401Unauthorized, api.Response.StatusCode);
        var (write, _) = await Send(after, "POST", "/api/anything/new", cookie);
        Assert.Equal(StatusCodes.Status401Unauthorized, write.Response.StatusCode);
    }

    [Fact]
    public async Task ARoleChange_AppliesToAnExistingSession_BecauseTheCookieCarriesOnlyTheName()
    {
        using var before = await BuildServer(SetFor(Entry("team", ReaderToken, "reader")));
        var cookie = await SignIn(before, ReaderToken);
        var (_, readerBody) = await Send(before, "GET", "/api/session", cookie);
        Assert.Contains("\"can_edit\":false", readerBody, StringComparison.Ordinal);

        using var after = await BuildServer(SetFor(Entry("team", ReaderToken, "operator")));
        var (_, operatorBody) = await Send(after, "GET", "/api/session", cookie);
        Assert.Contains("\"can_edit\":true", operatorBody, StringComparison.Ordinal);

        using var demoted = await BuildServer(SetFor(Entry("team", ReaderToken, "reader")));
        var (write, _) = await Send(demoted, "POST", "/api/views", cookie);
        Assert.Equal(StatusCodes.Status403Forbidden, write.Response.StatusCode);
    }

    [Fact]
    public void TheSeatModel_DecodesATokenCookie_ReadOnlyWithItsName_AndRefusesAnEmptyName()
    {
        Assert.True(DarlingWebSeat.TryResolveSeat(DarlingWebSeat.EncodeTokenCookieSubject("readers"), out var seat));
        Assert.Equal("readers", seat.Subject);
        Assert.Equal("readers", seat.TokenName);
        Assert.False(seat.CanEdit);
        Assert.Null(seat.OwnerKey);

        Assert.False(DarlingWebSeat.TryResolveSeat("t1:", out _));
        // A token seat never presses a proposal: the exception is for a seat with an owner key.
        Assert.False(DarlingWebSeat.IsRequestAllowed(seat, "POST", "/api/proposals/5f3a6d2e-9b1c-4e7f-8a2d-1c3b5e7f9a0b/confirm"));
    }
}
