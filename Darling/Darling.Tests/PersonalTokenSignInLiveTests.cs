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
using System.Net.Http;
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

/* #1776 own-store: this class runs on the PersonalTokenWebStore fixture's own scratch database (and one more it mints for the missing-table
   case), never the shared one, and every fact signs in as its own person. */

/// <summary>
/// #8737: the OIDC sign-in drops a person's personal tokens when it drops the seat (a refused sign-in loses them all, a read-only seat or an
/// adminRole lowered to reader loses the operator ones), best-effort and never changing the sign-in's outcome, and the start sweep applies lowered
/// settings once the owner connection is there. The callback runs through the real web pipeline with a fake identity provider.
/// </summary>
public sealed class PersonalTokenSignInLiveTests : IClassFixture<PersonalTokenWebStore>
{
    private const string Issuer = "https://idp.example.test";
    private const string ClientId = "client-id";
    private static readonly byte[] SessionKey = RandomNumberGenerator.GetBytes(32);

    private readonly PersonalTokenWebStore _store;

    public PersonalTokenSignInLiveTests(PersonalTokenWebStore store) => _store = store;

    private static string OwnerKeyFor(string label) => DarlingWebSeat.ComputeOwnerKey(Issuer, label)!;

    private static PersonalTokenSettings Settings(string adminRole = McpTokenEntry.OperatorRole, int days = 30) => new(days, 5, adminRole, null);

    private sealed class CapturingLogger : ILogger<DarlingWebHostService>
    {
        public List<(LogLevel Level, string Message)> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Lines)
            {
                Lines.Add((logLevel, formatter(state, exception) + (exception is null ? string.Empty : " | " + exception.Message)));
            }
        }
    }

    /// <summary>The identity provider: discovery, and a token endpoint that answers with whatever ID token the test set.</summary>
    private sealed class FakeIdp : HttpMessageHandler
    {
        public string IdToken { get; set; } = "unset";

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;
            var json = path.EndsWith("openid-configuration", StringComparison.Ordinal)
                ? new JsonObject { ["issuer"] = Issuer, ["authorization_endpoint"] = Issuer + "/authorize", ["token_endpoint"] = Issuer + "/token" }
                : new JsonObject { ["id_token"] = IdToken };
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(json.ToJsonString(), Encoding.UTF8, "application/json"),
            });
        }
    }

    private static string Base64Url(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    /// <summary>An unsigned ID token (this host checks claims, not the signature) for one person with the given group claim.</summary>
    private static string IdTokenFor(string label, string? group, string audience = ClientId, bool withSub = true)
    {
        var payload = new JsonObject
        {
            ["iss"] = Issuer,
            ["aud"] = audience,
            ["exp"] = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds(),
            ["nonce"] = "nonce-1",
            ["preferred_username"] = label + "@example.test",
        };
        if (withSub)
        {
            payload["sub"] = label;
        }

        if (group is not null)
        {
            payload["groups"] = new JsonArray(group);
        }

        var header = Base64Url(Encoding.UTF8.GetBytes("""{"alg":"none"}"""));
        return header + "." + Base64Url(Encoding.UTF8.GetBytes(payload.ToJsonString())) + ".sig";
    }

    private sealed record Callback(int Status, bool SessionCookieSet);

    private sealed class Rig : IAsyncDisposable
    {
        private readonly WebApplication _app;
        private readonly TestServer _server;
        private readonly DarlingWebOidcClient _oidc;

        private Rig(WebApplication app, TestServer server, NpgsqlDataSource owner, DarlingPersonalTokenSnapshot snapshot, StoreOwnerSource ownerSource,
            FakeIdp idp, DarlingWebOidcClient oidc, CapturingLogger logger, DarlingWebHostService host)
        {
            Host = host;
            _app = app;
            _server = server;
            Owner = owner;
            Snapshot = snapshot;
            OwnerSource = ownerSource;
            Idp = idp;
            _oidc = oidc;
            Logger = logger;
        }

        public NpgsqlDataSource Owner { get; }

        public DarlingPersonalTokenSnapshot Snapshot { get; }

        public StoreOwnerSource OwnerSource { get; }

        public FakeIdp Idp { get; }

        public CapturingLogger Logger { get; }

        public DarlingWebHostService Host { get; }

        /// <param name="publishedOwner">The data source the host reads as the owner connection when it is not the store's own (a dead one).</param>
        public static async Task<Rig> StartAsync(string connectionString, PersonalTokenSettings? settings, NpgsqlDataSource? publishedOwner = null)
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
            ownerSource.Publish(publishedOwner ?? owner);
            var snapshot = new DarlingPersonalTokenSnapshot();
            var logger = new CapturingLogger();
            var idp = new FakeIdp();
            var oidc = new DarlingWebOidcClient(new DarlingWebOidcClient.ResolvedOptions(
                Issuer, ClientId, null, "openid", null, "groups", new[] { "admins" }, new[] { "everyone" }), idp);
            var host = new DarlingWebHostService(
                logger, new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache(), ownerSource: ownerSource)
            {
                PersonalSnapshot = snapshot,
            };
            host.ConfigurePipeline(app, owner, networkMode: true, networkListenIp: IPAddress.Parse("10.1.2.3"),
                allowedCidr: IPNetwork.Parse("10.1.2.0/24"), accessToken: Convert.ToHexString(RandomNumberGenerator.GetBytes(24)),
                oidcClient: oidc, requireTokenWhenLoopbackOnly: false, sessionSigningKeyForTests: SessionKey, personalTokens: settings);
            await app.StartAsync(TestContext.Current.CancellationToken);
            return new Rig(app, app.GetTestServer(), owner, snapshot, ownerSource, idp, oidc, logger, host);
        }

        /// <summary>One OIDC callback carrying a valid transaction, with the identity provider answering the given ID token.</summary>
        public async Task<Callback> SignInAsync(string idToken, CancellationToken requestAborted = default)
        {
            Idp.IdToken = idToken;
            var transaction = new DarlingWebOidc.OidcTransaction("state-1", "nonce-1", "verifier-1", "http://10.1.2.3/auth/oidc/callback", "/");
            var cookie = DarlingWebOidc.TransactionCookieName + "="
                + DarlingWebOidc.BuildTransactionCookie(DarlingWebOidc.DeriveTransactionKey(SessionKey), transaction, DateTimeOffset.UtcNow.AddMinutes(5));
            var context = await _server.SendAsync(c =>
            {
                c.Request.Method = "GET";
                c.Request.Path = "/auth/oidc/callback";
                c.Request.QueryString = new QueryString("?code=code-1&state=state-1");
                c.Request.Headers.Host = "10.1.2.3";
                c.Request.Headers["Cookie"] = cookie;
                c.Connection.RemoteIpAddress = IPAddress.Parse("10.1.2.9");
            }, requestAborted);
            var cookies = context.Response.Headers.SetCookie.ToString();
            return new Callback(context.Response.StatusCode, cookies.Contains(DarlingWebHostService.SessionCookieName + "=", StringComparison.Ordinal)
                && !cookies.Contains(DarlingWebHostService.SessionCookieName + "=;", StringComparison.Ordinal));
        }

        /// <summary>Makes a live token for the owner key straight through the store and loads the snapshot, so it answers Live.</summary>
        public async Task<(string Value, string Id)> MakeTokenAsync(string ownerKey, string name, string role)
        {
            var minted = PersonalTokenFormat.Mint();
            var outcome = await PersonalTokenStore.InsertAsync(Owner, minted.TokenId, minted.SecretSha256, ownerKey, "subject-" + ownerKey, name, role,
                DateTimeOffset.UtcNow, 30, 5, "test", TestContext.Current.CancellationToken);
            Assert.Equal(PersonalTokenCreateOutcome.Created, outcome);
            await Snapshot.RefreshAsync(OwnerSource, Logger, TestContext.Current.CancellationToken);
            return (minted.Value, minted.TokenId);
        }

        public PersonalTokenOutcome StateOf(string value) => Snapshot.Check(value, DateTimeOffset.UtcNow).Outcome;

        public async Task<string?> ScalarAsync(string sql)
        {
            await using var command = Owner.CreateCommand(sql);
            return Convert.ToString(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken), System.Globalization.CultureInfo.InvariantCulture);
        }

        public async ValueTask DisposeAsync()
        {
            await _app.DisposeAsync();
            _oidc.Dispose();
            Snapshot.Dispose();
            await Owner.DisposeAsync();
        }
    }

    private async Task<Rig> StartAsync(PersonalTokenSettings? settings, NpgsqlDataSource? publishedOwner = null)
    {
        Assert.SkipWhen(_store.ConnectionString is null, "Set DARLING_TEST_PG to run the personal token sign-in live tests.");
        return await Rig.StartAsync(_store.ConnectionString!, settings, publishedOwner);
    }

    [Fact]
    public async Task ViewerSignIn_RevokesOperatorOnly()
    {
        await using var rig = await StartAsync(Settings());
        var key = OwnerKeyFor("viewer-signin");
        var op = await rig.MakeTokenAsync(key, "ops", "operator");
        var reader = await rig.MakeTokenAsync(key, "plain", "reader");

        var callback = await rig.SignInAsync(IdTokenFor("viewer-signin", "everyone"));

        Assert.Equal(200, callback.Status);
        Assert.True(callback.SessionCookieSet);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(op.Value));
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(reader.Value));
        Assert.Equal("1", await rig.ScalarAsync(
            $"SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked' AND actor = 'sign-in' AND detail->>'reason' = '{PersonalTokenStore.SeatLoweredReason}' AND detail::text LIKE '%{op.Id}%'"));
    }

    [Fact]
    public async Task RefusedSignIn_RevokesAll_StillRefused403()
    {
        await using var rig = await StartAsync(Settings());
        var key = OwnerKeyFor("refused-signin");
        var op = await rig.MakeTokenAsync(key, "ops", "operator");
        var reader = await rig.MakeTokenAsync(key, "plain", "reader");

        var callback = await rig.SignInAsync(IdTokenFor("refused-signin", "nobody"));

        Assert.Equal(403, callback.Status);
        Assert.False(callback.SessionCookieSet);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(op.Value));
        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(reader.Value));
        Assert.Equal("2", await rig.ScalarAsync(
            $"SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked' AND actor = 'sign-in' AND detail->>'reason' = '{PersonalTokenStore.SignInRefusedReason}'"
            + $" AND (detail::text LIKE '%{op.Id}%' OR detail::text LIKE '%{reader.Id}%')"));
    }

    [Fact]
    public async Task AdminSignIn_AdminRoleReader_RevokesOperator()
    {
        await using var rig = await StartAsync(Settings(McpTokenEntry.ReaderRole));
        var key = OwnerKeyFor("admin-reader-signin");
        var op = await rig.MakeTokenAsync(key, "ops", "operator");
        var reader = await rig.MakeTokenAsync(key, "plain", "reader");

        var callback = await rig.SignInAsync(IdTokenFor("admin-reader-signin", "admins"));

        Assert.Equal(200, callback.Status);
        Assert.True(callback.SessionCookieSet);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(op.Value));
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(reader.Value));
        Assert.Equal("1", await rig.ScalarAsync(
            $"SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked' AND actor = 'sign-in' AND detail->>'reason' = '{PersonalTokenStore.OperatorsDisabledReason}' AND detail::text LIKE '%{op.Id}%'"));
    }

    [Fact]
    public async Task AdminSignIn_Operator_RevokesNothing()
    {
        await using var rig = await StartAsync(Settings());
        var key = OwnerKeyFor("admin-operator-signin");
        var op = await rig.MakeTokenAsync(key, "ops", "operator");

        var callback = await rig.SignInAsync(IdTokenFor("admin-operator-signin", "admins"));

        Assert.Equal(200, callback.Status);
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(op.Value));
    }

    [Fact]
    public async Task SignInWithoutSub_NoOwnerKey_RevokesNothing()
    {
        await using var rig = await StartAsync(Settings());
        var op = await rig.MakeTokenAsync(OwnerKeyFor("no-sub-signin"), "ops", "operator");

        /* The subject resolves from preferred_username, so the sign-in is allowed, but with no sub there is no owner key to revoke for. */
        var callback = await rig.SignInAsync(IdTokenFor("no-sub-signin", "everyone", withSub: false));

        Assert.Equal(200, callback.Status);
        Assert.True(callback.SessionCookieSet);
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(op.Value));
    }

    [Fact]
    public async Task EarlyRefusal_BadIdToken_RevokesNothing()
    {
        await using var rig = await StartAsync(Settings());
        var op = await rig.MakeTokenAsync(OwnerKeyFor("bad-token-signin"), "ops", "operator");

        var callback = await rig.SignInAsync(IdTokenFor("bad-token-signin", "nobody", audience: "someone-else"));

        Assert.Equal(403, callback.Status);
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(op.Value));
    }

    [Fact]
    public async Task StoreDown_SignInStillSucceeds_WarningLogged()
    {
        await using var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Database=none;Timeout=2;Command Timeout=2;Pooling=false");
        await using var rig = await StartAsync(Settings(), publishedOwner: dead);

        var viewer = await rig.SignInAsync(IdTokenFor("store-down-viewer", "everyone"));
        var refused = await rig.SignInAsync(IdTokenFor("store-down-refused", "nobody"));

        Assert.Equal(200, viewer.Status);
        Assert.True(viewer.SessionCookieSet);
        Assert.Equal(403, refused.Status);
        Assert.False(refused.SessionCookieSet);
        Assert.Contains(rig.Logger.Lines, line => line.Level == LogLevel.Warning
            && line.Message.Contains("personal token", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task StoreWriteFails_AtSignIn_TokenStaysLive_ThenTheDrainRevokesIt()
    {
        await using var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Database=none;Timeout=2;Command Timeout=2;Pooling=false");
        await using var rig = await StartAsync(Settings());
        var key = OwnerKeyFor("drain-after-throw");
        var op = await rig.MakeTokenAsync(key, "ops", "operator");
        var reader = await rig.MakeTokenAsync(key, "plain", "reader");
        rig.OwnerSource.Publish(dead);

        var callback = await rig.SignInAsync(IdTokenFor("drain-after-throw", "everyone"));

        Assert.Equal(200, callback.Status);
        Assert.True(callback.SessionCookieSet);
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(op.Value));
        Assert.Equal(1, rig.Host.PendingPersonalRevokes.Count);
        Assert.Equal(1, rig.Logger.Lines.Count(line => line.Level == LogLevel.Warning && line.Message.Contains("queued", StringComparison.Ordinal)));

        /* The store is still down at the next tick: the entry stays, and the tick says nothing at Warning. */
        await rig.Host.DrainPersonalRevokesAsync(Settings(), TestContext.Current.CancellationToken);
        Assert.Equal(1, rig.Host.PendingPersonalRevokes.Count);
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(op.Value));

        rig.OwnerSource.Publish(rig.Owner);
        await rig.Host.DrainPersonalRevokesAsync(Settings(), TestContext.Current.CancellationToken);

        Assert.Equal(0, rig.Host.PendingPersonalRevokes.Count);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(op.Value));
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(reader.Value));
        Assert.Equal(1, rig.Logger.Lines.Count(line => line.Level == LogLevel.Warning && line.Message.Contains("queued", StringComparison.Ordinal)));
        Assert.Equal("1", await rig.ScalarAsync(
            $"SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked' AND actor = 'sign-in' AND detail->>'reason' = '{PersonalTokenStore.SeatLoweredReason}' AND detail::text LIKE '%{op.Id}%'"));
    }

    [Fact]
    public async Task NoOwnerSourceAtSignIn_TheDrainAfterPublishRevokes()
    {
        await using var rig = await StartAsync(Settings());
        var op = await rig.MakeTokenAsync(OwnerKeyFor("drain-after-publish"), "ops", "operator");
        rig.OwnerSource.Publish(null);

        var callback = await rig.SignInAsync(IdTokenFor("drain-after-publish", "everyone"));

        Assert.Equal(200, callback.Status);
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(op.Value));
        Assert.Equal(1, rig.Host.PendingPersonalRevokes.Count);

        /* Nothing to drain into yet. */
        await rig.Host.DrainPersonalRevokesAsync(Settings(), TestContext.Current.CancellationToken);
        Assert.Equal(1, rig.Host.PendingPersonalRevokes.Count);

        rig.OwnerSource.Publish(rig.Owner);
        await rig.Host.DrainPersonalRevokesAsync(Settings(), TestContext.Current.CancellationToken);

        Assert.Equal(0, rig.Host.PendingPersonalRevokes.Count);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(op.Value));
    }

    [Fact]
    public async Task Drain_WhileTheStoreFails_StopsAtTheFirstFailure_AndKeepsEveryEntry()
    {
        await using var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Database=none;Timeout=2;Command Timeout=2;Pooling=false");
        await using var rig = await StartAsync(Settings());
        rig.OwnerSource.Publish(null);
        foreach (var label in new[] { "stop-a", "stop-b", "stop-c", "stop-d" })
        {
            await rig.SignInAsync(IdTokenFor(label, "everyone"));
        }

        Assert.Equal(4, rig.Host.PendingPersonalRevokes.Count);
        rig.OwnerSource.Publish(dead);

        await rig.Host.DrainPersonalRevokesAsync(Settings(), TestContext.Current.CancellationToken);

        /* One store attempt (one "still pending" line, which is logged per failed attempt), and nothing left the queue. */
        Assert.Equal(1, rig.Logger.Lines.Count(line => line.Message.Contains("still pending", StringComparison.Ordinal)));
        Assert.Equal(4, rig.Host.PendingPersonalRevokes.Count);
    }

    private static DarlingPersonalTokenSnapshot SnapshotThatCannotRefresh() =>
        new(_ => throw new InvalidOperationException("the refresh failed"), (_, _) => Task.CompletedTask);

    [Fact]
    public async Task Drain_ThatRevokedRows_KeepsTheEntryUntilTheSnapshotRefreshSucceeds()
    {
        await using var rig = await StartAsync(Settings());
        var key = OwnerKeyFor("kept-until-refresh");
        var op = await rig.MakeTokenAsync(key, "ops", "operator");
        var pending = new PersonalTokenPendingRevokes();
        pending.Add(key, operatorOnly: true, PersonalTokenStore.SeatLoweredReason);
        using var failing = SnapshotThatCannotRefresh();

        await DarlingPersonalTokenSignIn.DrainPendingAsync(Settings(), rig.OwnerSource, failing, pending, rig.Logger, TestContext.Current.CancellationToken);

        /* The rows are revoked in the store, but the snapshot has not seen it, so the queue still refuses the token. */
        Assert.Equal("1", await rig.ScalarAsync($"SELECT count(*) FROM config.mcp_personal_token WHERE token_id = '{op.Id}' AND revoked_at IS NOT NULL"));
        Assert.Equal(1, pending.Count);
        Assert.True(pending.Refuses(key, "operator"));

        await DarlingPersonalTokenSignIn.DrainPendingAsync(Settings(), rig.OwnerSource, rig.Snapshot, pending, rig.Logger, TestContext.Current.CancellationToken);

        Assert.Equal(0, pending.Count);
        /* The snapshot the failing one stood in for reloads on its own tick, and then shows the revoke. */
        await rig.Snapshot.RefreshAsync(rig.OwnerSource, rig.Logger, TestContext.Current.CancellationToken);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(op.Value));
    }

    [Fact]
    public async Task Drain_ThatRevokedNoRows_CompletesTheEntryAtOnce_EvenWhenTheRefreshWouldFail()
    {
        await using var rig = await StartAsync(Settings());
        var pending = new PersonalTokenPendingRevokes();
        pending.Add(OwnerKeyFor("nothing-to-revoke"), operatorOnly: true, PersonalTokenStore.SeatLoweredReason);
        using var failing = SnapshotThatCannotRefresh();

        await DarlingPersonalTokenSignIn.DrainPendingAsync(Settings(), rig.OwnerSource, failing, pending, rig.Logger, TestContext.Current.CancellationToken);

        Assert.Equal(0, pending.Count);
    }

    [Fact]
    public async Task LaterSignIn_ThatLeavesNothingToRevoke_ClearsTheOwnersPendingEntry()
    {
        await using var rig = await StartAsync(Settings());
        var op = await rig.MakeTokenAsync(OwnerKeyFor("moot-entry"), "ops", "operator");
        rig.OwnerSource.Publish(null);
        await rig.SignInAsync(IdTokenFor("moot-entry", "everyone"));
        Assert.Equal(1, rig.Host.PendingPersonalRevokes.Count);

        /* Back at the admin seat (adminRole operator): the seat is not lowered, so the stale entry is dropped. */
        await rig.SignInAsync(IdTokenFor("moot-entry", "admins"));
        Assert.Equal(0, rig.Host.PendingPersonalRevokes.Count);

        rig.OwnerSource.Publish(rig.Owner);
        await rig.Host.DrainPersonalRevokesAsync(Settings(), TestContext.Current.CancellationToken);
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(op.Value));
    }

    [Fact]
    public async Task LaterNarrowerSignIn_KeepsTheBroaderPendingEntry()
    {
        await using var rig = await StartAsync(Settings());
        rig.OwnerSource.Publish(null);
        await rig.SignInAsync(IdTokenFor("keep-broad", "nobody"));
        await rig.SignInAsync(IdTokenFor("keep-broad", "everyone"));

        var entry = Assert.Single(rig.Host.PendingPersonalRevokes.Snapshot());
        Assert.False(entry.OperatorOnly);
        Assert.Equal(PersonalTokenStore.SignInRefusedReason, entry.Reason);
    }

    [Fact]
    public async Task SupervisorTick_WithTheWebListenerNotRunning_StillDrainsAQueuedRevoke()
    {
        await using var rig = await StartAsync(Settings());
        var op = await rig.MakeTokenAsync(OwnerKeyFor("tick-no-web"), "ops", "operator");
        rig.OwnerSource.Publish(null);
        await rig.SignInAsync(IdTokenFor("tick-no-web", "everyone"));
        Assert.Equal(1, rig.Host.PendingPersonalRevokes.Count);
        rig.OwnerSource.Publish(rig.Owner);

        await rig.Host.RunPersonalTickAsync(Settings(), webRunning: false, TestContext.Current.CancellationToken);

        Assert.Equal(0, rig.Host.PendingPersonalRevokes.Count);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(op.Value));
        Assert.False(rig.Host.PersonalStartSweepDone);
    }

    [Fact]
    public async Task LaterSignIn_OfEqualBreadth_ReplacesTheQueuedReason_AndTheJournalCarriesIt()
    {
        await using var rig = await StartAsync(Settings());
        var op = await rig.MakeTokenAsync(OwnerKeyFor("later-reason"), "ops", "operator");
        var owner = OwnerKeyFor("later-reason");
        rig.OwnerSource.Publish(null);

        rig.Host.PendingPersonalRevokes.Add(owner, operatorOnly: true, PersonalTokenStore.SeatLoweredReason);
        rig.Host.PendingPersonalRevokes.Add(owner, operatorOnly: true, PersonalTokenStore.OperatorsDisabledReason);

        var entry = Assert.Single(rig.Host.PendingPersonalRevokes.Snapshot());
        Assert.Equal(PersonalTokenStore.OperatorsDisabledReason, entry.Reason);

        rig.OwnerSource.Publish(rig.Owner);
        await rig.Host.DrainPersonalRevokesAsync(Settings(), TestContext.Current.CancellationToken);

        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(op.Value));
        Assert.Equal("1", await rig.ScalarAsync(
            $"SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked' AND detail->>'reason' = '{PersonalTokenStore.OperatorsDisabledReason}' AND detail::text LIKE '%{op.Id}%'"));
    }

    [Fact]
    public async Task Drain_WithTheFeatureOff_KeepsTheQueue()
    {
        await using var rig = await StartAsync(Settings());
        var op = await rig.MakeTokenAsync(OwnerKeyFor("drain-feature-off"), "ops", "operator");
        rig.OwnerSource.Publish(null);
        await rig.SignInAsync(IdTokenFor("drain-feature-off", "everyone"));
        rig.OwnerSource.Publish(rig.Owner);

        await rig.Host.DrainPersonalRevokesAsync(null, TestContext.Current.CancellationToken);

        Assert.Equal(1, rig.Host.PendingPersonalRevokes.Count);
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(op.Value));
    }

    [Fact]
    public void PendingRevokes_TwoEntriesForOneOwner_MergeToTheBroaderOne()
    {
        var pending = new PersonalTokenPendingRevokes();

        Assert.Equal(PersonalTokenPendingAdd.Queued, pending.Add("k1", true, PersonalTokenStore.SeatLoweredReason));
        Assert.Equal(PersonalTokenPendingAdd.Merged, pending.Add("k1", false, PersonalTokenStore.SignInRefusedReason));
        var broad = Assert.Single(pending.Snapshot());
        Assert.False(broad.OperatorOnly);
        Assert.Equal(PersonalTokenStore.SignInRefusedReason, broad.Reason);

        /* A later narrower entry does not take it back. */
        Assert.Equal(PersonalTokenPendingAdd.Merged, pending.Add("k1", true, PersonalTokenStore.SeatLoweredReason));
        var still = Assert.Single(pending.Snapshot());
        Assert.False(still.OperatorOnly);
        Assert.Equal(PersonalTokenStore.SignInRefusedReason, still.Reason);

        /* A broader entry that arrives while the narrow one is being revoked survives the narrow one's completion. */
        pending.Add("k2", true, PersonalTokenStore.SeatLoweredReason);
        var narrow = pending.Snapshot().Single(e => e.OwnerKey == "k2");
        pending.Add("k2", false, PersonalTokenStore.SignInRefusedReason);
        pending.Complete(narrow);
        var left = pending.Snapshot().Single(e => e.OwnerKey == "k2");
        Assert.False(left.OperatorOnly);
        pending.Complete(left);
        Assert.DoesNotContain(pending.Snapshot(), e => e.OwnerKey == "k2");
    }

    [Fact]
    public void PendingRevokes_TheBoundHolds_NewOwnersDropped_AMergeForAnExistingOwnerStillWorks()
    {
        var pending = new PersonalTokenPendingRevokes(maxOwners: 3);

        Assert.Equal(PersonalTokenPendingAdd.Queued, pending.Add("a", true, "r"));
        Assert.Equal(PersonalTokenPendingAdd.Queued, pending.Add("b", true, "r"));
        Assert.Equal(PersonalTokenPendingAdd.Queued, pending.Add("c", true, "r"));
        Assert.Equal(PersonalTokenPendingAdd.DroppedFirst, pending.Add("d", true, "r"));
        Assert.Equal(PersonalTokenPendingAdd.Dropped, pending.Add("e", true, "r"));
        Assert.Equal(3, pending.Count);
        Assert.DoesNotContain(pending.Snapshot(), e => e.OwnerKey is "d" or "e");

        Assert.Equal(PersonalTokenPendingAdd.Merged, pending.Add("a", false, "wider"));
        Assert.False(pending.Snapshot().Single(e => e.OwnerKey == "a").OperatorOnly);

        /* Room again after a drain: the next drop reports first again. */
        pending.Complete(pending.Snapshot().Single(e => e.OwnerKey == "b"));
        Assert.Equal(PersonalTokenPendingAdd.Queued, pending.Add("d", true, "r"));
        Assert.Equal(PersonalTokenPendingAdd.DroppedFirst, pending.Add("f", true, "r"));
    }

    [Fact]
    public async Task Revoke_AtSignIn_McpCheckDeadImmediately()
    {
        await using var rig = await StartAsync(Settings());
        var op = await rig.MakeTokenAsync(OwnerKeyFor("immediate-signin"), "ops", "operator");
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(op.Value));

        /* No refresh by the test: the answer is already out when the callback returns, so the snapshot the MCP gate reads is dead by then. */
        await rig.SignInAsync(IdTokenFor("immediate-signin", "everyone"));

        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(op.Value));
    }

    [Fact]
    public async Task StartSweep_ReaderOnly_RevokesOperator()
    {
        await using var rig = await StartAsync(Settings(McpTokenEntry.ReaderRole));
        var key = OwnerKeyFor("sweep-reader");
        var op = await rig.MakeTokenAsync(key, "ops", "operator");
        var reader = await rig.MakeTokenAsync(key, "plain", "reader");

        var done = await DarlingPersonalTokenSignIn.ApplySettingsAtStartAsync(
            Settings(McpTokenEntry.ReaderRole), rig.OwnerSource, rig.Snapshot, rig.Logger, TestContext.Current.CancellationToken);

        Assert.True(done);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.StateOf(op.Value));
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(reader.Value));
        Assert.Equal("1", await rig.ScalarAsync(
            $"SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked' AND actor = 'config' AND detail::text LIKE '%{op.Id}%'"));
    }

    [Fact]
    public async Task StartSweep_LowerLifetime_CutsExpiry()
    {
        await using var rig = await StartAsync(Settings(days: 5));
        var made = await rig.MakeTokenAsync(OwnerKeyFor("sweep-lifetime"), "plain", "reader");

        var done = await DarlingPersonalTokenSignIn.ApplySettingsAtStartAsync(
            Settings(days: 5), rig.OwnerSource, rig.Snapshot, rig.Logger, TestContext.Current.CancellationToken);

        Assert.True(done);
        var expiry = DateTimeOffset.Parse(
            (await rig.ScalarAsync($"SELECT expires_at FROM config.mcp_personal_token WHERE token_id = '{made.Id}'"))!, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AssumeUniversal);
        Assert.True(expiry < DateTimeOffset.UtcNow.AddDays(5).AddMinutes(1), "the expiry is cut to five days from creation");
        Assert.Equal("1", await rig.ScalarAsync(
            $"SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_expiry_cut' AND actor = 'config' AND detail::text LIKE '%{made.Id}%'"));
        Assert.Equal(PersonalTokenOutcome.Live, rig.StateOf(made.Value));
    }

    [Fact]
    public async Task StartSweep_WaitsForOwnerSource_ThenRunsOnce()
    {
        Assert.SkipWhen(_store.ConnectionString is null, "Set DARLING_TEST_PG to run the personal token sign-in live tests.");
        var settings = Settings(McpTokenEntry.ReaderRole);
        await using var owner = NpgsqlDataSource.Create(_store.ConnectionString!);
        var ownerSource = new StoreOwnerSource();
        using var snapshot = new DarlingPersonalTokenSnapshot();
        var logger = new CapturingLogger();
        var host = new DarlingWebHostService(
            logger, new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache(), ownerSource: ownerSource)
        {
            PersonalSnapshot = snapshot,
        };
        var ct = TestContext.Current.CancellationToken;

        async Task<string> MakeOperatorAsync(string name)
        {
            var minted = PersonalTokenFormat.Mint();
            await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, OwnerKeyFor("sweep-once"), "subject-sweep-once", name, "operator",
                DateTimeOffset.UtcNow, 30, 5, "test", ct);
            await snapshot.RefreshAsync(ownerSource, logger, ct);
            return minted.Value;
        }

        await host.RunPersonalStartSweepAsync(settings, ct);
        Assert.False(host.PersonalStartSweepDone, "no owner connection yet, so the sweep has not run");

        ownerSource.Publish(owner);
        var first = await MakeOperatorAsync("first");
        await host.RunPersonalStartSweepAsync(settings, ct);
        Assert.True(host.PersonalStartSweepDone);
        Assert.Equal(PersonalTokenOutcome.Dead, snapshot.Check(first, DateTimeOffset.UtcNow).Outcome);

        var second = await MakeOperatorAsync("second");
        await host.RunPersonalStartSweepAsync(settings, ct);
        Assert.Equal(PersonalTokenOutcome.Live, snapshot.Check(second, DateTimeOffset.UtcNow).Outcome);
    }

    [Fact]
    public async Task StartSweep_MissingTable_CountsAsNotYet()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the personal token sign-in live tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, CancellationToken.None);
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var drop = owner.CreateCommand("DROP TABLE IF EXISTS config.mcp_personal_token CASCADE"))
        {
            await drop.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var ownerSource = new StoreOwnerSource();
        ownerSource.Publish(owner);
        using var snapshot = new DarlingPersonalTokenSnapshot();

        var done = await DarlingPersonalTokenSignIn.ApplySettingsAtStartAsync(
            Settings(), ownerSource, snapshot, NullLogger.Instance, TestContext.Current.CancellationToken);

        Assert.False(done);
        NpgsqlConnection.ClearAllPools();
    }
}
