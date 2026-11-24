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
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the one scratch database this fixture mints through ScratchPostgres is dropped with the fixture, and every fact signs in as
   its own person, so no class here touches another test's rows or the shared database. */

/// <summary>One scratch store migrated to the latest rung, shared by the personal-token route tests: each fact signs in as its own person (an owner
/// key made from its own label), so none reads another's rows.</summary>
public sealed class PersonalTokenWebStore : IAsyncLifetime
{
    private ScratchPostgres? _scratch;

    public string? ConnectionString => _scratch?.ConnectionString;

    public async ValueTask InitializeAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return;
        }

        _scratch = await ScratchPostgres.CreateAsync(baseConnectionString, CancellationToken.None);
        await using var connection = new NpgsqlConnection(_scratch.ConnectionString);
        await connection.OpenAsync(CancellationToken.None);
        await PgMigrations.MigrateAsync(connection, CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        if (_scratch is not null)
        {
            NpgsqlConnection.ClearAllPools();
            await _scratch.DisposeAsync();
        }
    }
}

/* #1776 own-store: this class runs on the fixture's own scratch database (and one more it mints), never the shared one. */

/// <summary>
/// #8737: <c>/api/mcp-tokens</c> through the real web pipeline (the sign-in gate, the write gate and the endpoint map) on a live store, with the
/// session cookies the OIDC sign-in mints. A read-only OIDC seat may make and revoke its own tokens, an admin may revoke anyone's, the journal names
/// the person, and the MCP snapshot holds each change before the answer leaves.
/// </summary>
public sealed class PersonalTokenWebRouteLiveTests : IClassFixture<PersonalTokenWebStore>
{
    private static readonly byte[] SessionKey = RandomNumberGenerator.GetBytes(32);
    private const string Json = "application/json";

    private readonly PersonalTokenWebStore _store;

    public PersonalTokenWebRouteLiveTests(PersonalTokenWebStore store) => _store = store;

    private enum Seat { Viewer, Admin, SharedToken, ViewerNoKey }

    private sealed record Person(string Subject, string OwnerKey, Seat Seat);

    private static Person PersonFor(string label, Seat seat = Seat.Viewer) =>
        new(label + "@example.test", DarlingWebSeat.ComputeOwnerKey("https://idp.example.test", label)!, seat);

    private static string CookieFor(Person who)
    {
        var subject = who.Seat switch
        {
            Seat.Viewer => DarlingWebSeat.EncodeCookieSubject(who.Subject, WebOidcRole.Viewer, who.OwnerKey, who.Subject, "Everyone"),
            Seat.Admin => DarlingWebSeat.EncodeCookieSubject(who.Subject, WebOidcRole.Admin, who.OwnerKey, who.Subject, "Admins"),
            Seat.ViewerNoKey => DarlingWebSeat.EncodeCookieSubject(who.Subject, WebOidcRole.Viewer),
            _ => null,
        };
        return DarlingWebHostService.SessionCookieName + "=" + DarlingWebHostService.BuildSessionCookieValue(SessionKey, DateTimeOffset.UtcNow.AddHours(1), subject);
    }

    private sealed record Reply(int Status, string Text, HttpContext Context)
    {
        public JsonNode? Body => string.IsNullOrWhiteSpace(Text) ? null : JsonNode.Parse(Text);

        public string? Error => (string?)Body?["error"];
    }

    private sealed class Rig : IAsyncDisposable
    {
        private readonly WebApplication _app;
        private readonly TestServer _server;
        private readonly bool _network;

        private Rig(WebApplication app, TestServer server, NpgsqlDataSource owner, DarlingPersonalTokenSnapshot snapshot, bool network)
        {
            _app = app;
            _server = server;
            Owner = owner;
            Snapshot = snapshot;
            _network = network;
        }

        public NpgsqlDataSource Owner { get; }

        public DarlingPersonalTokenSnapshot Snapshot { get; }

        public static async Task<Rig> StartAsync(string connectionString, PersonalTokenSettings? settings, bool publishOwner = true, bool network = false,
            CapturingTestLogger? log = null)
        {
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
                WebRootPath = "wwwroot",
            });
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            var owner = NpgsqlDataSource.Create(connectionString);
            builder.Services.AddSingleton(owner);
            DarlingWebHostService.ConfigureResponseCompression(builder.Services);
            var app = builder.Build();

            var ownerSource = new StoreOwnerSource();
            if (publishOwner)
            {
                ownerSource.Publish(owner);
            }

            var snapshot = new DarlingPersonalTokenSnapshot();
            var host = new DarlingWebHostService(
                log is null ? NullLogger<DarlingWebHostService>.Instance : new HostLogger(log), new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(),
                new BaselineCache(), ownerSource: ownerSource)
            {
                PersonalSnapshot = snapshot,
            };
            host.ConfigurePipeline(app, owner, networkMode: network,
                networkListenIp: network ? IPAddress.Parse("10.1.2.3") : null,
                allowedCidr: IPNetwork.Parse(network ? "10.1.2.0/24" : "127.0.0.1/32"),
                accessToken: Convert.ToHexString(RandomNumberGenerator.GetBytes(24)), oidcClient: null, requireTokenWhenLoopbackOnly: !network,
                sessionSigningKeyForTests: SessionKey, personalTokens: settings);
            await app.StartAsync(TestContext.Current.CancellationToken);
            return new Rig(app, app.GetTestServer(), owner, snapshot, network);
        }

        public async Task<Reply> SendAsync(string method, string path, Person? who, string? contentType = Json, string? body = null, string host = "localhost",
            CancellationToken? abort = null)
        {
            var context = await _server.SendAsync(c =>
            {
                c.Request.Method = method;
                var question = path.IndexOf('?', StringComparison.Ordinal);
                c.Request.Path = question < 0 ? path : path[..question];
                if (question >= 0)
                {
                    c.Request.QueryString = new QueryString(path[question..]);
                }

                c.Request.Headers.Host = _network && host == "localhost" ? "10.1.2.3" : host;
                c.Connection.RemoteIpAddress = _network ? IPAddress.Parse("10.1.2.9") : IPAddress.Loopback;
                if (who is not null)
                {
                    c.Request.Headers["Cookie"] = CookieFor(who);
                }

                if (contentType is not null)
                {
                    c.Request.ContentType = contentType;
                }

                c.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(body ?? string.Empty));
            }, abort ?? TestContext.Current.CancellationToken);
            using var reader = new StreamReader(context.Response.Body);
            return new Reply(context.Response.StatusCode, await reader.ReadToEndAsync(TestContext.Current.CancellationToken), context);
        }

        public Task<Reply> CreateAsync(Person who, string name, string role = "reader", int days = 7) =>
            SendAsync("POST", "/api/mcp-tokens", who, body: Create(name, role, days));

        public async Task<string?> ScalarAsync(string sql)
        {
            await using var command = Owner.CreateCommand(sql);
            return Convert.ToString(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken), CultureInfo.InvariantCulture);
        }

        public async ValueTask DisposeAsync()
        {
            await _app.DisposeAsync();
            Snapshot.Dispose();
            await Owner.DisposeAsync();
        }
    }

    private sealed class HostLogger(CapturingTestLogger inner) : ILogger<DarlingWebHostService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => inner.Log(logLevel, eventId, state, exception, formatter);
    }

    private static string Create(string name, string role = "reader", int days = 7) =>
        new JsonObject { ["name"] = name, ["lifetime_days"] = days, ["role"] = role }.ToJsonString();

    private static PersonalTokenSettings Settings(string adminRole = McpTokenEntry.OperatorRole, int days = 30, int cap = 5) => new(days, cap, adminRole, null);

    private async Task<Rig> StartAsync(PersonalTokenSettings? settings, bool publishOwner = true, bool network = false)
    {
        Assert.SkipWhen(_store.ConnectionString is null, "Set DARLING_TEST_PG to run the personal token route live tests.");
        return await Rig.StartAsync(_store.ConnectionString!, settings, publishOwner, network);
    }

    [Fact]
    public async Task Create_ReadOnlySeat_ReaderAllowed()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("create-reader");

        var reply = await rig.CreateAsync(vera, "laptop");

        Assert.Equal(201, reply.Status);
        var value = (string)reply.Body!["token"]!;
        Assert.Equal(65, value.Length);
        Assert.StartsWith("dpt1_", value, StringComparison.Ordinal);
        var record = reply.Body["record"]!;
        Assert.Equal("laptop", (string?)record["name"]);
        Assert.Equal("reader", (string?)record["role"]);
        Assert.Equal("live", (string?)record["state"]);
        Assert.Null(record["subject"]);
        /* R6: the snapshot holds the token before the caller has its value. */
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(value, DateTimeOffset.UtcNow).Outcome);
    }

    private static string PaddedCreate(string name, int padBytes) =>
        new JsonObject { ["name"] = name, ["lifetime_days"] = 7, ["role"] = "reader", ["pad"] = new string('x', padBytes) }.ToJsonString();

    [Fact]
    public async Task Create_BodyOverTheCap_400BadRequest_NoRowMade()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("create-big-body");

        var reply = await rig.SendAsync("POST", "/api/mcp-tokens", vera, body: PaddedCreate("laptop", 10_000));

        Assert.Equal(400, reply.Status);
        Assert.Equal("bad_request", reply.Error);
        Assert.Equal("0", await rig.ScalarAsync($"SELECT count(*) FROM config.mcp_personal_token WHERE owner_key = '{vera.OwnerKey}'"));
    }

    [Fact]
    public async Task Create_BodyNearTheCap_StillCreated()
    {
        await using var rig = await StartAsync(Settings());
        var padding = DarlingWebEndpoints.MaxCreateBodyBytes - Create("laptop").Length - 40;

        var reply = await rig.SendAsync("POST", "/api/mcp-tokens", PersonFor("create-edge-body"), body: PaddedCreate("laptop", padding));

        Assert.True(Encoding.UTF8.GetByteCount(PaddedCreate("laptop", padding)) <= DarlingWebEndpoints.MaxCreateBodyBytes);
        Assert.Equal(201, reply.Status);
    }

    [Fact]
    public async Task Create_ReadOnlySeat_OperatorRefused_role_not_allowed()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("create-viewer-operator");

        var reply = await rig.CreateAsync(vera, "ops", role: "operator");

        Assert.Equal(400, reply.Status);
        Assert.Equal("role_not_allowed", reply.Error);
        Assert.Empty((await PersonalTokenStore.ListAsync(rig.Owner, vera.OwnerKey, DateTimeOffset.UtcNow, TestContext.Current.CancellationToken)).Rows);
    }

    [Fact]
    public async Task Create_Admin_OperatorAllowed()
    {
        await using var rig = await StartAsync(Settings());
        var ada = PersonFor("create-admin-operator", Seat.Admin);

        var reply = await rig.CreateAsync(ada, "ops", role: "operator");

        Assert.Equal(201, reply.Status);
        Assert.Equal("operator", (string?)reply.Body!["record"]!["role"]);
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check((string)reply.Body["token"]!, DateTimeOffset.UtcNow).Outcome);
    }

    [Fact]
    public async Task Create_Admin_AdminRoleReader_OperatorRefused()
    {
        await using var rig = await StartAsync(Settings(McpTokenEntry.ReaderRole));
        var ada = PersonFor("create-admin-reader-only", Seat.Admin);

        var refused = await rig.CreateAsync(ada, "ops", role: "operator");
        var allowed = await rig.CreateAsync(ada, "plain", role: "reader");

        Assert.Equal(400, refused.Status);
        Assert.Equal("role_not_allowed", refused.Error);
        Assert.Equal(201, allowed.Status);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(31)]
    [InlineData(-5)]
    public async Task Create_LifetimeOutsideRange_bad_lifetime(int days)
    {
        await using var rig = await StartAsync(Settings(days: 30));

        var reply = await rig.CreateAsync(PersonFor("create-lifetime"), "short", days: days);

        Assert.Equal(400, reply.Status);
        Assert.Equal("bad_lifetime", reply.Error);
    }

    [Fact]
    public async Task Create_LifetimeAtMax_Allowed_AndOverMaxRefused()
    {
        await using var rig = await StartAsync(Settings(days: 14));
        var vera = PersonFor("create-lifetime-max");

        Assert.Equal(201, (await rig.CreateAsync(vera, "at-max", days: 14)).Status);
        Assert.Equal("bad_lifetime", (await rig.CreateAsync(vera, "over", days: 15)).Error);
        Assert.Equal("bad_lifetime", (await rig.SendAsync("POST", "/api/mcp-tokens", vera, body: """{"name":"frac","lifetime_days":1.5,"role":"reader"}""")).Error);
        Assert.Equal("bad_lifetime", (await rig.SendAsync("POST", "/api/mcp-tokens", vera, body: """{"name":"none","role":"reader"}""")).Error);
    }

    [Fact]
    public async Task Create_BadName_bad_name()
    {
        await using var rig = await StartAsync(Settings());

        var reply = await rig.CreateAsync(PersonFor("create-bad-name"), "not valid!");

        Assert.Equal(400, reply.Status);
        Assert.Equal("bad_name", reply.Error);
    }

    [Theory]
    [InlineData("web")]
    [InlineData("MCP")]
    [InlineData("cli")]
    [InlineData("viewer")]
    public async Task Create_BuiltInStampName_bad_name(string name)
    {
        await using var rig = await StartAsync(Settings());

        var reply = await rig.CreateAsync(PersonFor("create-stamp-" + name.ToLowerInvariant()), name);

        Assert.Equal(400, reply.Status);
        Assert.Equal("bad_name", reply.Error);
    }

    [Theory]
    [InlineData("a/b")]
    [InlineData("a b")]
    [InlineData("")]
    public async Task Create_InvalidAlphabet_bad_name(string name)
    {
        await using var rig = await StartAsync(Settings());

        var reply = await rig.CreateAsync(PersonFor("create-alphabet"), name);

        Assert.Equal(400, reply.Status);
        Assert.Equal("bad_name", reply.Error);
    }

    [Fact]
    public async Task Create_NameLength_64Allowed_65Refused_AndANonStringNameRefused()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("create-name-length");

        Assert.Equal("bad_name", (await rig.CreateAsync(vera, new string('n', 65))).Error);
        Assert.Equal("bad_name", (await rig.SendAsync("POST", "/api/mcp-tokens", vera, body: """{"name":7,"lifetime_days":7,"role":"reader"}""")).Error);
        Assert.Equal(201, (await rig.CreateAsync(vera, new string('n', 64))).Status);
    }

    [Fact]
    public async Task Create_NamesCompareOrdinal_CaseDiffersIsAnotherName_AndARepeatIs409()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("create-name-taken");

        Assert.Equal(201, (await rig.CreateAsync(vera, "CI")).Status);
        Assert.Equal(201, (await rig.CreateAsync(vera, "ci")).Status);
        var repeat = await rig.CreateAsync(vera, "CI");

        Assert.Equal(409, repeat.Status);
        Assert.Equal("name_taken", repeat.Error);
        /* Another person may use the same name. */
        Assert.Equal(201, (await rig.CreateAsync(PersonFor("create-name-taken-other"), "CI")).Status);
    }

    [Fact]
    public async Task Create_AtCap_409()
    {
        await using var rig = await StartAsync(Settings(cap: 2));
        var vera = PersonFor("create-at-cap");

        Assert.Equal(201, (await rig.CreateAsync(vera, "one")).Status);
        Assert.Equal(201, (await rig.CreateAsync(vera, "two")).Status);
        var third = await rig.CreateAsync(vera, "three");

        Assert.Equal(409, third.Status);
        Assert.Equal("at_cap", third.Error);
    }

    /// <summary>I2: create then revoke frees the live slot, so a loop of the two is held by the 24-hour ceiling: 429 too_many_recent.</summary>
    [Fact]
    public async Task Create_TooManyRecent_429()
    {
        await using var rig = await StartAsync(Settings(cap: 1));
        var vera = PersonFor("create-too-many-recent");
        for (var i = 0; i < PersonalTokenStore.RecentCreatesPerSlot; i++)
        {
            var made = await rig.CreateAsync(vera, "loop-" + i.ToString(CultureInfo.InvariantCulture));
            Assert.Equal(201, made.Status);
            Assert.Equal(204, (await rig.SendAsync("POST", "/api/mcp-tokens/" + (string)made.Body!["record"]!["id"]! + "/revoke", vera)).Status);
        }

        var refused = await rig.CreateAsync(vera, "one-more");

        Assert.Equal(429, refused.Status);
        Assert.Equal("too_many_recent", refused.Error);
        Assert.False(string.IsNullOrWhiteSpace((string?)refused.Body!["message"]));
        Assert.DoesNotContain((await PersonalTokenStore.ListAsync(rig.Owner, vera.OwnerKey, DateTimeOffset.UtcNow, TestContext.Current.CancellationToken)).Rows,
            r => r.TokenName == "one-more");
    }

    /// <summary>I2 (#3699): <c>?all=1</c> answers at most <see cref="PersonalTokenStore.ListLimit"/> rows and says when it cut, on a store of its own.</summary>
    [Fact]
    public async Task List_All_PastTheLimit_SetsTruncated()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the personal token route live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var insert = new NpgsqlCommand(string.Format(CultureInfo.InvariantCulture,
                "INSERT INTO config.mcp_personal_token (token_id, secret_sha256, owner_key, subject, token_name, role, created_at, expires_at) "
                + "SELECT lpad(to_hex(i), 16, '0'), sha256(i::text::bytea), 'bulk', 's', 'n' || i, 'reader', now() - make_interval(secs => {0} - i), now() + interval '1 day' "
                + "FROM generate_series(0, {0}) AS i", PersonalTokenStore.ListLimit), connection);
            await insert.ExecuteNonQueryAsync(ct);
        }

        await using var rig = await Rig.StartAsync(scratch.ConnectionString, Settings());
        var ada = PersonFor("list-all-truncated", Seat.Admin);

        var cut = await rig.SendAsync("GET", "/api/mcp-tokens?all=1", ada);
        var own = await rig.SendAsync("GET", "/api/mcp-tokens", ada);

        Assert.Equal(200, cut.Status);
        Assert.True((bool?)cut.Body!["truncated"]);
        Assert.Equal(PersonalTokenStore.ListLimit, cut.Body["tokens"]!.AsArray().Count);
        Assert.False((bool?)own.Body!["truncated"]);
        Assert.Empty(own.Body["tokens"]!.AsArray());
    }

    [Fact]
    public async Task Create_SharedTokenAdmin_oidc_only()
    {
        await using var rig = await StartAsync(Settings());

        var reply = await rig.CreateAsync(new Person("shared", "unused", Seat.SharedToken), "shared-one");

        Assert.Equal(403, reply.Status);
        Assert.Equal("oidc_only", reply.Error);
    }

    [Fact]
    public async Task Create_ViewerSignInWithNoOwnerKey_IsRefusedByTheWriteGate()
    {
        await using var rig = await StartAsync(Settings());

        var reply = await rig.CreateAsync(PersonFor("create-no-key", Seat.ViewerNoKey), "nokey");

        Assert.Equal(403, reply.Status);
    }

    [Fact]
    public async Task Create_FeatureOff_404()
    {
        await using var rig = await StartAsync(settings: null);
        var ada = PersonFor("feature-off", Seat.Admin);

        var create = await rig.CreateAsync(ada, "laptop");
        var list = await rig.SendAsync("GET", "/api/mcp-tokens", ada);
        var revoke = await rig.SendAsync("POST", "/api/mcp-tokens/0123456789abcdef/revoke", ada);
        var me = await rig.SendAsync("GET", "/api/me", ada);

        foreach (var reply in new[] { create, list, revoke })
        {
            Assert.Equal(404, reply.Status);
            Assert.Equal("personal_tokens_unavailable", reply.Error);
        }

        Assert.False((bool?)me.Body!["personal_tokens"]);
        Assert.Empty((await PersonalTokenStore.ListAsync(rig.Owner, ada.OwnerKey, DateTimeOffset.UtcNow, TestContext.Current.CancellationToken)).Rows);
    }

    [Fact]
    public async Task Me_PersonalTokensIsTrue_ForAnOidcViewer_WhenServed()
    {
        await using var rig = await StartAsync(Settings());

        var me = await rig.SendAsync("GET", "/api/me", PersonFor("me-served"));

        Assert.True((bool?)me.Body!["personal_tokens"]);
    }

    [Fact]
    public async Task Create_NoJsonContentType_415()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("create-content-type");

        var text = await rig.SendAsync("POST", "/api/mcp-tokens", vera, contentType: "text/plain", body: Create("laptop"));
        var form = await rig.SendAsync("POST", "/api/mcp-tokens", vera, contentType: "application/x-www-form-urlencoded", body: "name=laptop");
        var none = await rig.SendAsync("POST", "/api/mcp-tokens", vera, contentType: null, body: Create("laptop"));

        Assert.Equal(415, text.Status);
        Assert.Equal(415, form.Status);
        Assert.Equal(415, none.Status);
        Assert.Empty((await PersonalTokenStore.ListAsync(rig.Owner, vera.OwnerKey, DateTimeOffset.UtcNow, TestContext.Current.CancellationToken)).Rows);
    }

    [Fact]
    public async Task Create_BodyThatIsNotAJsonObject_400()
    {
        await using var rig = await StartAsync(Settings());

        foreach (var body in new[] { "not json", "[1]", string.Empty })
        {
            var reply = await rig.SendAsync("POST", "/api/mcp-tokens", PersonFor("create-bad-body"), body: body);
            Assert.Equal(400, reply.Status);
            Assert.Equal("bad_request", reply.Error);
        }
    }

    [Fact]
    public async Task Create_OwnerNotPublished_503()
    {
        await using var rig = await StartAsync(Settings(), publishOwner: false);
        var vera = PersonFor("owner-not-published");

        var create = await rig.CreateAsync(vera, "laptop");
        var list = await rig.SendAsync("GET", "/api/mcp-tokens", vera);
        var revoke = await rig.SendAsync("POST", "/api/mcp-tokens/0123456789abcdef/revoke", vera);

        Assert.Equal(503, create.Status);
        Assert.Equal(503, list.Status);
        Assert.Equal(503, revoke.Status);
        Assert.Equal("store_unavailable", create.Error);
    }

    /// <summary>E1: a store one rung behind has no table. Both reads and writes answer 503, never 500.</summary>
    [Fact]
    public async Task Routes_StoreOneRungBehind_503()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the personal token route live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var drop = new NpgsqlCommand("DROP TABLE config.mcp_personal_token; DROP FUNCTION config.mcp_personal_token_owner_only()", connection);
            await drop.ExecuteNonQueryAsync(ct);
        }

        var log = new CapturingTestLogger();
        await using var rig = await Rig.StartAsync(scratch.ConnectionString, Settings(), log: log);
        var ada = PersonFor("one-rung-behind", Seat.Admin);

        var list = await rig.SendAsync("GET", "/api/mcp-tokens", ada);
        var all = await rig.SendAsync("GET", "/api/mcp-tokens?all=1", ada);
        var create = await rig.CreateAsync(ada, "laptop");
        var revoke = await rig.SendAsync("POST", "/api/mcp-tokens/0123456789abcdef/revoke", ada);

        Assert.Equal(503, list.Status);
        Assert.Equal(503, all.Status);
        Assert.Equal(503, create.Status);
        Assert.Equal(503, revoke.Status);
        Assert.Equal("store_unavailable", create.Error);
        /* M7: four requests on a store below the rung log one Information line, with no stack, and no Warning. */
        var missing = log.Lines.Where(l => l.Contains("mcp_personal_token", StringComparison.Ordinal)).ToList();
        Assert.Single(missing);
        Assert.StartsWith("Information:", missing[0], StringComparison.Ordinal);
        Assert.Empty(log.ExceptionsAtLevel(LogLevel.Information));
        Assert.True(log.CountAtLevel(LogLevel.Warning) == 0, log.Joined);
    }

    [Fact]
    public async Task List_ShowsOnlyTheCallersRows_AndTheRulesTheFormNeeds()
    {
        await using var rig = await StartAsync(Settings(days: 21));
        var vera = PersonFor("list-own");
        var wes = PersonFor("list-other");
        Assert.Equal(201, (await rig.CreateAsync(vera, "mine")).Status);
        Assert.Equal(201, (await rig.CreateAsync(wes, "theirs")).Status);

        var reply = await rig.SendAsync("GET", "/api/mcp-tokens", vera);

        Assert.Equal(200, reply.Status);
        var tokens = reply.Body!["tokens"]!.AsArray();
        var row = Assert.Single(tokens);
        Assert.Equal("mine", (string?)row!["name"]);
        Assert.Null(row["subject"]);
        Assert.Equal(21, (int?)reply.Body["max_lifetime_days"]);
        Assert.Equal(new[] { "reader" }, reply.Body["allowed_roles"]!.AsArray().Select(n => (string?)n).ToArray());
    }

    [Fact]
    public async Task List_AllowedRoles_FollowTheSeatAndAdminRole()
    {
        await using var operatorRig = await StartAsync(Settings());
        await using var readerRig = await StartAsync(Settings(McpTokenEntry.ReaderRole));

        var admin = await operatorRig.SendAsync("GET", "/api/mcp-tokens", PersonFor("roles-admin", Seat.Admin));
        var adminReader = await readerRig.SendAsync("GET", "/api/mcp-tokens", PersonFor("roles-admin", Seat.Admin));
        var shared = await operatorRig.SendAsync("GET", "/api/mcp-tokens", new Person("shared", "unused", Seat.SharedToken));

        Assert.Equal(new[] { "operator", "reader" }, admin.Body!["allowed_roles"]!.AsArray().Select(n => (string?)n).ToArray());
        Assert.Equal(new[] { "reader" }, adminReader.Body!["allowed_roles"]!.AsArray().Select(n => (string?)n).ToArray());
        /* A non-OIDC admin has no tokens of its own and may make none. */
        Assert.Equal(200, shared.Status);
        Assert.Empty(shared.Body!["tokens"]!.AsArray());
        Assert.Empty(shared.Body["allowed_roles"]!.AsArray());
    }

    [Fact]
    public async Task List_NeverCarriesValueOrHash()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("list-no-leak");
        var ada = PersonFor("list-no-leak-admin", Seat.Admin);
        var created = await rig.CreateAsync(vera, "laptop");
        var value = (string)created.Body!["token"]!;
        Assert.True(PersonalTokenFormat.TryParse(value, out _, out var hash));
        /* #9211: the secret is base64url and can hold '_', so take it after the separator that follows the id, as PersonalTokenFormat.TryParse does. */
        var secret = value[(value.IndexOf('_', PersonalTokenFormat.Prefix.Length) + 1)..];

        var own = await rig.SendAsync("GET", "/api/mcp-tokens", vera);
        var all = await rig.SendAsync("GET", "/api/mcp-tokens?all=1", ada);

        foreach (var text in new[] { own.Text, all.Text, created.Body["record"]!.ToJsonString() })
        {
            Assert.DoesNotContain(value, text, StringComparison.Ordinal);
            Assert.DoesNotContain(secret, text, StringComparison.Ordinal);
            Assert.DoesNotContain(Convert.ToHexString(hash), text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(Convert.ToBase64String(hash).TrimEnd('='), text, StringComparison.Ordinal);
            Assert.DoesNotContain("secret", text, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public async Task List_All_NonAdmin_403()
    {
        await using var rig = await StartAsync(Settings());

        var reply = await rig.SendAsync("GET", "/api/mcp-tokens?all=1", PersonFor("list-all-viewer"));

        Assert.Equal(403, reply.Status);
    }

    [Fact]
    public async Task List_All_Admin_IncludesSubject()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("list-all-vera");
        var ada = PersonFor("list-all-ada", Seat.Admin);
        Assert.Equal(201, (await rig.CreateAsync(vera, "from-vera")).Status);

        var reply = await rig.SendAsync("GET", "/api/mcp-tokens?all=1", ada);

        Assert.Equal(200, reply.Status);
        var row = reply.Body!["tokens"]!.AsArray().Single(n => (string?)n!["name"] == "from-vera")!;
        Assert.Equal(vera.Subject, (string?)row["subject"]);
        /* The shared token is an admin too. */
        var shared = await rig.SendAsync("GET", "/api/mcp-tokens?all=1", new Person("shared", "unused", Seat.SharedToken));
        Assert.Equal(200, shared.Status);
        Assert.Contains(shared.Body!["tokens"]!.AsArray(), n => (string?)n!["name"] == "from-vera");
    }

    [Fact]
    public async Task List_StatesAreLiveExpiredAndRevoked()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("list-states");
        var live = await rig.CreateAsync(vera, "live-one");
        var revoked = await rig.CreateAsync(vera, "revoked-one");
        var expired = await rig.CreateAsync(vera, "expired-one");
        Assert.Equal(204, (await rig.SendAsync("POST", "/api/mcp-tokens/" + (string)revoked.Body!["record"]!["id"]! + "/revoke", vera)).Status);
        await using (var command = rig.Owner.CreateCommand(
            "UPDATE config.mcp_personal_token SET created_at = now() - interval '3 days', expires_at = now() - interval '1 day' WHERE token_id = $1"))
        {
            command.Parameters.AddWithValue((string)expired.Body!["record"]!["id"]!);
            await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var reply = await rig.SendAsync("GET", "/api/mcp-tokens", vera);

        var states = reply.Body!["tokens"]!.AsArray().ToDictionary(n => (string)n!["name"]!, n => (string)n!["state"]!);
        Assert.Equal("live", states["live-one"]);
        Assert.Equal("revoked", states["revoked-one"]);
        Assert.Equal("expired", states["expired-one"]);
        Assert.Equal(PersonalTokenStore.RevokedByOwnerReason, (string?)reply.Body["tokens"]!.AsArray().Single(n => (string?)n!["name"] == "revoked-one")!["revoke_reason"]);
        Assert.NotNull(live.Body);
    }

    [Fact]
    public async Task Revoke_Own_204_ThenMcpCheckDead()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("revoke-own");
        var created = await rig.CreateAsync(vera, "laptop");
        var value = (string)created.Body!["token"]!;
        var id = (string)created.Body["record"]!["id"]!;
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(value, DateTimeOffset.UtcNow).Outcome);

        var reply = await rig.SendAsync("POST", "/api/mcp-tokens/" + id + "/revoke", vera);

        Assert.Equal(204, reply.Status);
        Assert.Equal(string.Empty, reply.Text);
        /* No tick: the route refreshed the snapshot before it answered. */
        Assert.Equal(PersonalTokenOutcome.Dead, rig.Snapshot.Check(value, DateTimeOffset.UtcNow).Outcome);
        Assert.Equal(404, (await rig.SendAsync("POST", "/api/mcp-tokens/" + id + "/revoke", vera)).Status);
    }

    /// <summary>
    /// I3: a revoke that has committed reaches the MCP snapshot even when the caller goes away before the refresh. The snapshot's refresh gate is
    /// held by a refresh of a second store whose table the test has locked, so the route's own refresh waits behind it; once the revoke is
    /// committed the request is aborted, then the lock is let go. The route's refresh still runs, and the token is Dead.
    /// </summary>
    [Fact]
    public async Task Revoke_RequestAbortedAfterTheCommit_TheSnapshotStillSeesItDead()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the personal token route live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("revoke-aborted");
        var created = await rig.CreateAsync(vera, "laptop");
        var value = (string)created.Body!["token"]!;
        var id = (string)created.Body["record"]!["id"]!;

        await using var other = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var migrate = new NpgsqlConnection(other.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        await using var holder = new NpgsqlConnection(other.ConnectionString);
        await holder.OpenAsync(ct);
        await using var lockTransaction = await holder.BeginTransactionAsync(ct);
        await using (var lockTable = new NpgsqlCommand("LOCK TABLE config.mcp_personal_token IN ACCESS EXCLUSIVE MODE", holder, lockTransaction))
        {
            await lockTable.ExecuteNonQueryAsync(ct);
        }

        await using var otherSource = NpgsqlDataSource.Create(other.ConnectionString);
        var otherOwner = new StoreOwnerSource();
        otherOwner.Publish(otherSource);
        var blocking = rig.Snapshot.RefreshAsync(otherOwner, NullLogger.Instance, CancellationToken.None);
        /* Read from another session: inside the lock's transaction pg_stat_activity is one cached snapshot. */
        var otherDatabase = new NpgsqlConnectionStringBuilder(other.ConnectionString).Database;
        await WaitUntilAsync(async () => await rig.ScalarAsync(
            $"SELECT count(*) FROM pg_stat_activity WHERE datname = '{otherDatabase}' AND wait_event_type = 'Lock'") != "0", ct);

        using var abort = new CancellationTokenSource();
        var revoke = rig.SendAsync("POST", "/api/mcp-tokens/" + id + "/revoke", vera, abort: abort.Token);
        await WaitUntilAsync(async () => await rig.ScalarAsync($"SELECT count(*) FROM config.mcp_personal_token WHERE token_id = '{id}' AND revoked_at IS NOT NULL") == "1", ct);
        await abort.CancelAsync();
        await lockTransaction.RollbackAsync(ct);
        await blocking;
        /* The test host answers an aborted request with an IOException. */
        await Assert.ThrowsAnyAsync<IOException>(() => revoke);

        await WaitUntilAsync(() => Task.FromResult(rig.Snapshot.Check(value, DateTimeOffset.UtcNow).Outcome == PersonalTokenOutcome.Dead), ct);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "the condition did not hold within 15 seconds");
            await Task.Delay(50, ct);
        }
    }

    [Fact]
    public async Task Revoke_OtherPersons_NonAdmin_404()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("revoke-other-vera");
        var wes = PersonFor("revoke-other-wes");
        var created = await rig.CreateAsync(vera, "laptop");
        var id = (string)created.Body!["record"]!["id"]!;

        var refused = await rig.SendAsync("POST", "/api/mcp-tokens/" + id + "/revoke", wes);
        var absent = await rig.SendAsync("POST", "/api/mcp-tokens/ffffffffffffffff/revoke", wes);

        Assert.Equal(404, refused.Status);
        /* One answer for another person's token and for one that does not exist: nothing shows that the id is real. */
        Assert.Equal(absent.Status, refused.Status);
        Assert.Equal(absent.Text, refused.Text);
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check((string)created.Body["token"]!, DateTimeOffset.UtcNow).Outcome);
    }

    [Fact]
    public async Task Revoke_OtherPersons_Admin_204()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("revoke-admin-vera");
        var ada = PersonFor("revoke-admin-ada", Seat.Admin);
        var created = await rig.CreateAsync(vera, "laptop");
        var id = (string)created.Body!["record"]!["id"]!;

        var reply = await rig.SendAsync("POST", "/api/mcp-tokens/" + id + "/revoke", ada);

        Assert.Equal(204, reply.Status);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.Snapshot.Check((string)created.Body["token"]!, DateTimeOffset.UtcNow).Outcome);
        Assert.Equal(PersonalTokenStore.RevokedByAdminReason, await rig.ScalarAsync($"SELECT revoke_reason FROM config.mcp_personal_token WHERE token_id = '{id}'"));
        Assert.Equal("admin:" + ada.Subject, await rig.ScalarAsync($"SELECT revoked_by FROM config.mcp_personal_token WHERE token_id = '{id}'"));
    }

    /// <summary>M2: an admin revoking their own token is its owner revoking it: the reason and who come from whose row it is, not the seat.</summary>
    [Fact]
    public async Task Revoke_AdminsOwnToken_IsRevokedByOwner()
    {
        await using var rig = await StartAsync(Settings());
        var ada = PersonFor("revoke-admin-own", Seat.Admin);
        var created = await rig.CreateAsync(ada, "laptop");
        var id = (string)created.Body!["record"]!["id"]!;

        Assert.Equal(204, (await rig.SendAsync("POST", "/api/mcp-tokens/" + id + "/revoke", ada)).Status);

        Assert.Equal(PersonalTokenStore.RevokedByOwnerReason, await rig.ScalarAsync($"SELECT revoke_reason FROM config.mcp_personal_token WHERE token_id = '{id}'"));
        Assert.Equal("person:" + ada.Subject, await rig.ScalarAsync($"SELECT revoked_by FROM config.mcp_personal_token WHERE token_id = '{id}'"));
    }

    [Fact]
    public async Task Revoke_SharedTokenAdmin_204()
    {
        await using var rig = await StartAsync(Settings());
        var created = await rig.CreateAsync(PersonFor("revoke-shared-vera"), "laptop");
        var id = (string)created.Body!["record"]!["id"]!;

        var reply = await rig.SendAsync("POST", "/api/mcp-tokens/" + id + "/revoke", new Person("shared", "unused", Seat.SharedToken));

        Assert.Equal(204, reply.Status);
        Assert.Equal("person:web", await rig.ScalarAsync(
            $"SELECT actor FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked' AND detail->>'token_id' = '{id}'"));
        Assert.Equal("admin:web", await rig.ScalarAsync($"SELECT revoked_by FROM config.mcp_personal_token WHERE token_id = '{id}'"));
    }

    [Theory]
    [InlineData("0123456789ABCDEF")]
    [InlineData("0123456789abcde")]
    [InlineData("not-a-token-id!!")]
    public async Task Revoke_IdThatIsNotATokenId_404(string id)
    {
        await using var rig = await StartAsync(Settings());

        var reply = await rig.SendAsync("POST", "/api/mcp-tokens/" + id + "/revoke", PersonFor("revoke-bad-id", Seat.Admin));

        Assert.Equal(404, reply.Status);
    }

    [Fact]
    public async Task Revoke_CrossSite_Refused()
    {
        await using var rig = await StartAsync(Settings(), network: true);
        var vera = PersonFor("revoke-cross-site");
        var created = await rig.CreateAsync(vera, "laptop");
        Assert.Equal(201, created.Status);
        var path = "/api/mcp-tokens/" + (string)created.Body!["record"]!["id"]! + "/revoke";

        var wrongHost = await rig.SendAsync("POST", path, vera, host: "evil.example");
        var notJson = await rig.SendAsync("POST", path, vera, contentType: "text/plain");
        var noType = await rig.SendAsync("POST", path, vera, contentType: null);
        var otherWrite = await rig.SendAsync("POST", "/api/custom-views", vera, body: "{}");

        Assert.Equal(400, wrongHost.Status);
        Assert.Equal(415, notJson.Status);
        Assert.Equal(415, noType.Status);
        Assert.Equal(403, otherWrite.Status);
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check((string)created.Body["token"]!, DateTimeOffset.UtcNow).Outcome);
    }

    [Fact]
    public async Task JournalRow_ActorIsPersonSubject()
    {
        await using var rig = await StartAsync(Settings());
        var vera = PersonFor("journal-actor");
        var created = await rig.CreateAsync(vera, "laptop");
        var id = (string)created.Body!["record"]!["id"]!;
        Assert.Equal(204, (await rig.SendAsync("POST", "/api/mcp-tokens/" + id + "/revoke", vera)).Status);

        var actors = await rig.ScalarAsync(
            "SELECT string_agg(event || '=' || actor, ',' ORDER BY event) FROM config.investigation_journal "
            + $"WHERE event LIKE 'mcp_personal_token_%' AND detail->>'token_id' = '{id}'");

        Assert.Equal($"mcp_personal_token_created=person:{vera.Subject},mcp_personal_token_revoked=person:{vera.Subject}", actors);
        Assert.Equal($"person:{vera.Subject}", await rig.ScalarAsync($"SELECT revoked_by FROM config.mcp_personal_token WHERE token_id = '{id}'"));
        Assert.DoesNotContain((string)created.Body["token"]!,
            await rig.ScalarAsync($"SELECT string_agg(detail::text, ',') FROM config.investigation_journal WHERE detail->>'token_id' = '{id}'") ?? string.Empty,
            StringComparison.Ordinal);
    }
}
