/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using Core = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// The web dashboard's server add and edit refuse Windows authentication over an unverified certificate unless
/// <c>web.allowWindowsAuthOverUnverifiedTls</c> is on, as the MCP tools do under their own setting. Driven through
/// the real <c>POST /api/servers</c> and <c>PATCH /api/servers/{id}</c> routes. The refused add runs the route's own
/// default wiring over a store nothing listens behind, so a refusal that reached the store would fail the test; the
/// other cases stand a stub store and probe behind the same routes.
/// </summary>
public sealed class WebServerWindowsAuthScreenTests
{
    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";
    private const string WebKey = "web.allowWindowsAuthOverUnverifiedTls";
    private const string McpKey = "mcp.allowWindowsAuthOverUnverifiedTls";
    private static readonly DateTime Stamp = new(2026, 10, 5, 12, 0, 0, 123, DateTimeKind.Unspecified);

    private sealed class CountingDefinitions : Core.IServerDefinitions
    {
        public int Inserts { get; private set; }

        public Task<List<Core.RegisteredTarget>> LoadTargetsAsync(CancellationToken cancellationToken) => Task.FromResult(new List<Core.RegisteredTarget>());

        public Task<int?> ScopedAgentRouteAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<int?>(null);

        public Task<List<string>> LoadStorageKeysAsync(CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<string?> ReadStorageKeyAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<string?>(null);

        public Task<int> InsertAsync(Core.ParsedServerEntry entry, string? encryptedPassword, string? actualStorageKey, Core.HeldCheck? heldCheck, CancellationToken cancellationToken)
        {
            Inserts++;
            return Task.FromResult(7);
        }
    }

    private sealed class CountingEditStore(Core.ServerEditRow row) : Core.IServerEditStore
    {
        public int Writes { get; private set; }

        public Task<Core.ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<Core.ServerEditRow?>(row);

        public Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<Core.ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<Core.EditColumnValue> sets, string? newStorageKey, string? actualStorageKey, Core.HeldCheck? heldCheck, CancellationToken cancellationToken)
        {
            Writes++;
            return Task.FromResult(new Core.ServerEditWrite(Core.ServerEditWriteKind.Written, Stamp.AddSeconds(1)));
        }
    }

    private sealed class Web : IAsyncDisposable
    {
        public required WebApplication App { get; init; }

        public required HttpClient Client { get; init; }

        public required System.Runtime.CompilerServices.StrongBox<int> ProbeCount { get; init; }

        public int Probes => ProbeCount.Value;

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await App.DisposeAsync();
        }
    }

    private static Core.ServerEditRow Row(string auth, bool trust, string encrypt = "Mandatory") => new(
        ServerId: 41, Name: "alpha-01", Host: "alpha-01.example.test", Port: 0, Database: null, ReadOnlyIntent: false, Engine: "sqlserver",
        Auth: auth, Username: auth == "sql" ? "monitor" : null, EncryptMode: encrypt, TrustServerCertificate: trust,
        MultiSubnetFailover: false, MonthlyCostUsd: 10m, ModifiedAt: Stamp.AddTicks(7));

    /// <summary>The routes over the add and edit cores with a stub store and probe, the screen set as the host sets it from the setting.</summary>
    private static async Task<Web> StartAsync(
        bool allow, CountingDefinitions? definitions = null, CountingEditStore? editStore = null, bool realAddWiring = false)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        var probes = new System.Runtime.CompilerServices.StrongBox<int>(0);
        var source = NpgsqlDataSource.Create(DeadStore);
        app.Use(async (context, next) =>
        {
            context.Items[DarlingWebSeat.HttpContextItemKey] = new DarlingWebSeat("alice", true);
            await next(context);
        });

        Core.ServerProbe probe = (_, _) =>
        {
            probes.Value++;
            return Task.FromResult(new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null));
        };
        var screen = DarlingUnverifiedWindowsAuthScreen.ForWeb(allow);
        var logger = new CapturingTestLogger();

        Func<string, Task<string>>? add = realAddWiring
            ? null
            : body => Core.AddServersAsync(definitions ?? new CountingDefinitions(), body, probe, CancellationToken.None, ring: TestKeyRings.Healthy, webScreen: screen);
        Func<int, string, Task<string>>? edit = editStore is null
            ? null
            : (id, body) => Core.EditServerCoreAsync(editStore, id, body, probe, TestKeyRings.Healthy, null, CancellationToken.None, webScreen: screen);
        DarlingWebEndpoints.MapServers(app, source, logger, add, null, edit, null, null, allowWindowsAuthOverUnverifiedTls: allow);
        await app.StartAsync(TestContext.Current.CancellationToken);
        return new Web { App = app, Client = app.GetTestClient(), ProbeCount = probes };
    }

    private static string Windows(string extra) => "[{\"host\":\"winhost.example.test\",\"auth\":\"Windows\"" + extra + "}]";

    private static string SqlTrust() =>
        "[{\"host\":\"sql-01.example.test\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"Synth-Web-Pw-1\",\"trust_server_certificate\":true}]";

    private static async Task<(int Status, JsonNode Body)> PostAsync(Web web, string body)
    {
        using var response = await web.Client.PostAsync("/api/servers", new StringContent(body, Encoding.UTF8, "application/json"), TestContext.Current.CancellationToken);
        var text = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return ((int)response.StatusCode, JsonNode.Parse(text)!);
    }

    private static async Task<(int Status, JsonNode Body)> PatchAsync(Web web, string body)
    {
        /* The web edit requires the opaque modified_at token an earlier read returned. */
        var token = Core.ModifiedAtToken(Row("integrated", trust: false).ModifiedAt);
        body = "{\"expected_modified_at\":\"" + token + "\"," + body.TrimStart().Substring(1);
        using var request = new HttpRequestMessage(HttpMethod.Patch, "/api/servers/41") { Content = new StringContent(body, Encoding.UTF8, "application/json") };
        using var response = await web.Client.SendAsync(request, TestContext.Current.CancellationToken);
        var text = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return ((int)response.StatusCode, JsonNode.Parse(text)!);
    }

    private static string AddStatus(JsonNode body) => body["results"]![0]!["status"]!.GetValue<string>();

    private static string AddDetail(JsonNode body) => body["results"]![0]!["detail"]!.GetValue<string>();

    /* ---------------- add ---------------- */

    [Theory]
    [InlineData(",\"trust_server_certificate\":true")]
    [InlineData(",\"encrypt_mode\":\"Optional\"")]
    public async Task Add_WindowsAuth_OverAnUnverifiedConnection_IsRefused_BeforeTheStoreOrAProbe(string extra)
    {
        /* The route's own default wiring, over a store nothing listens behind: only a refusal ahead of the store answers cleanly. */
        await using var web = await StartAsync(allow: false, realAddWiring: true);

        var (status, body) = await PostAsync(web, Windows(extra));

        Assert.Equal(200, status);
        Assert.Equal("invalid", AddStatus(body));
        Assert.Equal(0, body["added"]!.GetValue<int>());
        Assert.Contains(WebKey, AddDetail(body), StringComparison.Ordinal);
        Assert.DoesNotContain(McpKey, AddDetail(body), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Add_WindowsAuth_Refused_SavesNothing_AndMakesNoProbe()
    {
        var definitions = new CountingDefinitions();
        await using var web = await StartAsync(allow: false, definitions);

        var (_, body) = await PostAsync(web, Windows(",\"trust_server_certificate\":true"));

        Assert.Equal("invalid", AddStatus(body));
        Assert.Equal(0, definitions.Inserts);
        Assert.Equal(0, web.Probes);
    }

    [Fact]
    public async Task Add_SqlAuth_WithTrustServerCertificate_IsAllowed()
    {
        var definitions = new CountingDefinitions();
        await using var web = await StartAsync(allow: false, definitions);

        var (_, body) = await PostAsync(web, SqlTrust());

        Assert.Equal("added", AddStatus(body));
        Assert.Equal(1, definitions.Inserts);
    }

    [Fact]
    public async Task Add_WindowsAuth_OverAVerifiedConnection_IsAllowed()
    {
        var definitions = new CountingDefinitions();
        await using var web = await StartAsync(allow: false, definitions);

        var (_, body) = await PostAsync(web, Windows(",\"trust_server_certificate\":false,\"encrypt_mode\":\"Mandatory\""));

        Assert.Equal("added", AddStatus(body));
        Assert.Equal(1, definitions.Inserts);
    }

    [Fact]
    public async Task Add_WindowsAuth_OverAnUnverifiedConnection_IsAllowed_WhenTheSettingIsOn()
    {
        var definitions = new CountingDefinitions();
        await using var web = await StartAsync(allow: true, definitions);

        var (_, body) = await PostAsync(web, Windows(",\"trust_server_certificate\":true"));

        Assert.Equal("added", AddStatus(body));
        Assert.Equal(1, definitions.Inserts);
    }

    [Fact]
    public async Task Add_RouteWiring_PassesTheSetting_ToTheDefaultCore()
    {
        /* With the setting on, the default wiring gets past the screen and reaches the store, which here is unreachable: the answer is
           not the refusal. The same body with the setting off is refused in Add_WindowsAuth_OverAnUnverifiedConnection_IsRefused_BeforeTheStoreOrAProbe. */
        await using var web = await StartAsync(allow: true, realAddWiring: true);

        using var response = await web.Client.PostAsync(
            "/api/servers", new StringContent(Windows(",\"trust_server_certificate\":true"), Encoding.UTF8, "application/json"), TestContext.Current.CancellationToken);
        var text = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        Assert.DoesNotContain(WebKey, text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("{\"web\":{\"enabled\":true}}", true)]
    [InlineData("{\"web\":{\"allowWindowsAuthOverUnverifiedTls\":false}}", true)]
    [InlineData("{\"web\":{\"allowWindowsAuthOverUnverifiedTls\":true}}", false)]
    public async Task HostPipeline_CarriesTheParsedSetting_ToThePostRoute(string json, bool refused)
    {
        /* Through the host's own ConfigurePipeline -> MapAll -> MapServers, with the setting read from parsed darling.json text and the
           route's default wiring over a store nothing listens behind: off answers the refusal row naming the web key; on gets past the
           screen to the unreachable store, so the refusal text is absent. */
        var config = DarlingConfig.Parse(json);
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var postgres = NpgsqlDataSource.Create(DeadStore);
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        await using var app = builder.Build();
        var host = new DarlingWebHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingWebHostService>.Instance,
            new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache());
        const string hostName = "192.168.1.205";
        var remote = System.Net.IPAddress.Parse("192.168.1.50");
        host.ConfigurePipeline(
            app, postgres, networkMode: true, networkListenIp: System.Net.IPAddress.Parse(hostName),
            allowedCidr: System.Net.IPNetwork.Parse("192.168.1.0/24"), accessToken: "correct-token-value", oidcClient: null,
            allowWindowsAuthOverUnverifiedTls: config.Web?.AllowWindowsAuthOverUnverifiedTls ?? false);
        await app.StartAsync(TestContext.Current.CancellationToken);
        var server = app.GetTestServer();

        /* The shared-token seat edits: sign in with the token, then send the add with the session cookie it mints. */
        var exchange = await DarlingWebHostGateLiveTests.PostSignIn(server, hostName, remote, "correct-token-value");
        var cookie = exchange.Response.Headers.SetCookie.ToString().Split(';')[0];
        var payload = Encoding.UTF8.GetBytes(Windows(",\"trust_server_certificate\":true"));
        var context = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/api/servers";
            c.Request.Headers.Host = hostName;
            c.Request.Headers["Cookie"] = cookie;
            c.Request.ContentType = "application/json";
            c.Request.ContentLength = payload.Length;
            c.Request.Body = new System.IO.MemoryStream(payload);
            c.Connection.RemoteIpAddress = remote;
        });
        using var reader = new System.IO.StreamReader(context.Response.Body);
        var text = await reader.ReadToEndAsync(TestContext.Current.CancellationToken);
        var response = context.Response;

        Assert.True(refused == text.Contains(WebKey, StringComparison.Ordinal), response.StatusCode + " " + text);
        if (refused)
        {
            Assert.Equal(200, response.StatusCode);
            Assert.Equal("invalid", AddStatus(JsonNode.Parse(text)!));
        }
    }

    /* ---------------- edit ---------------- */

    [Fact]
    public async Task Edit_ChangingTheHost_OfAWindowsServerThatTrustsTheCertificate_IsRefused()
    {
        var store = new CountingEditStore(Row("integrated", trust: true));
        await using var web = await StartAsync(allow: false, editStore: store);

        var (status, body) = await PatchAsync(web, "{\"host\":\"other.example.test\"}");

        Assert.Equal("invalid", body["status"]!.GetValue<string>());
        Assert.Equal(400, status);
        var message = body.ToJsonString();
        Assert.Contains("Not saved:", message, StringComparison.Ordinal);
        Assert.Contains(WebKey, message, StringComparison.Ordinal);
        Assert.Equal(0, store.Writes);
        Assert.Equal(0, web.Probes);
    }

    [Fact]
    public async Task Edit_ChangingOnlyTheDisplayName_OfTheSameServer_IsAllowed()
    {
        var store = new CountingEditStore(Row("integrated", trust: true));
        await using var web = await StartAsync(allow: false, editStore: store);

        var (_, body) = await PatchAsync(web, "{\"display_name\":\"Orders\"}");

        Assert.Equal("updated", body["status"]!.GetValue<string>());
        Assert.Equal(1, store.Writes);
    }

    [Fact]
    public async Task Edit_ChangingMonthlyCost_OfTheSameServer_IsAllowed()
    {
        var store = new CountingEditStore(Row("integrated", trust: true, encrypt: "Optional"));
        await using var web = await StartAsync(allow: false, editStore: store);

        var (_, body) = await PatchAsync(web, "{\"monthly_cost_usd\":120}");

        Assert.Equal("updated", body["status"]!.GetValue<string>());
        Assert.Equal(1, store.Writes);
    }

    [Theory]
    [InlineData("{\"trust_server_certificate\":true}")]
    [InlineData("{\"encrypt_mode\":\"Optional\"}")]
    public async Task Edit_MakingAVerifiedWindowsServerUnverified_IsRefused(string changes)
    {
        var store = new CountingEditStore(Row("integrated", trust: false));
        await using var web = await StartAsync(allow: false, editStore: store);

        var (status, body) = await PatchAsync(web, changes);

        Assert.Equal("invalid", body["status"]!.GetValue<string>());
        Assert.Equal(400, status);
        Assert.Equal(0, store.Writes);
        Assert.Equal(0, web.Probes);
    }

    [Fact]
    public async Task Edit_SwitchingASqlServerToWindows_WhenItTrustsTheCertificate_IsRefused()
    {
        var store = new CountingEditStore(Row("sql", trust: true));
        await using var web = await StartAsync(allow: false, editStore: store);

        var (_, body) = await PatchAsync(web, "{\"auth\":\"Windows\"}");

        Assert.Equal("invalid", body["status"]!.GetValue<string>());
        Assert.Equal(0, store.Writes);
    }

    [Fact]
    public async Task Edit_OfASqlServer_ThatTrustsTheCertificate_IsAllowed()
    {
        var store = new CountingEditStore(Row("sql", trust: true));
        await using var web = await StartAsync(allow: false, editStore: store);

        var (_, body) = await PatchAsync(web, "{\"host\":\"other.example.test\",\"password\":\"Synth-Web-Pw-2\"}");

        Assert.Equal("updated", body["status"]!.GetValue<string>());
        Assert.Equal(1, store.Writes);
    }

    [Fact]
    public async Task Edit_ChangingTheHost_OfAnUnverifiedWindowsServer_IsAllowed_WhenTheSettingIsOn()
    {
        var store = new CountingEditStore(Row("integrated", trust: true));
        await using var web = await StartAsync(allow: true, editStore: store);

        var (_, body) = await PatchAsync(web, "{\"host\":\"other.example.test\"}");

        Assert.Equal("updated", body["status"]!.GetValue<string>());
        Assert.Equal(1, store.Writes);
        Assert.Equal(1, web.Probes);
    }

    [Fact]
    public async Task Edit_ChangingOnlyTheQueryStoreReplica_OfAnUnverifiedWindowsServer_IsRefused()
    {
        /* The replica is reached with the primary's Windows sign-in and certificate settings, so changing it is judged like a change of host. */
        var store = new CountingEditStore(Row("integrated", trust: true));
        await using var web = await StartAsync(allow: false, editStore: store);

        var (status, body) = await PatchAsync(web, "{\"query_store_replica\":\"replica-a.example.test\"}");

        Assert.Equal("invalid", body["status"]!.GetValue<string>());
        Assert.Equal(400, status);
        var message = body.ToJsonString();
        Assert.Contains("Not saved:", message, StringComparison.Ordinal);
        Assert.Contains(WebKey, message, StringComparison.Ordinal);
        Assert.Equal(0, store.Writes);
        Assert.Equal(0, web.Probes);
    }

    [Fact]
    public async Task Edit_ChangingOnlyTheQueryStoreReplica_OfAnUnverifiedWindowsServer_IsAllowed_WhenTheSettingIsOn()
    {
        var store = new CountingEditStore(Row("integrated", trust: true));
        await using var web = await StartAsync(allow: true, editStore: store);

        var (_, body) = await PatchAsync(web, "{\"query_store_replica\":\"replica-a.example.test\"}");

        Assert.Equal("updated", body["status"]!.GetValue<string>());
        Assert.Equal(1, store.Writes);
        Assert.Equal(1, web.Probes);
    }

    /* The edit route's own default wiring (EditServerByIdAsync with the screen) has no test here: that core reads the row from a
       PostgreSQL store before anything else, and the route offers no seam for a stub store behind its default edit; the tests above
       hand the route their own closure. The add route's default wiring is tested above with the setting off. */

    /* ---------------- MCP keeps its own key ---------------- */

    [Fact]
    public async Task Mcp_Refusal_StillNamesTheMcpKey()
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);

        var answer = await Core.AddServersAsync(
            dead, Windows(",\"trust_server_certificate\":true"), (_, _) => throw new InvalidOperationException("no probe"), CancellationToken.None,
            policy: DarlingMcpAddServersPolicy.Default);

        var detail = AddDetail(JsonNode.Parse(answer)!);
        Assert.Contains(McpKey, detail, StringComparison.Ordinal);
        Assert.DoesNotContain(WebKey, detail, StringComparison.Ordinal);
    }

    /* ---------------- config ---------------- */

    [Fact]
    public void TheSetting_IsOffWhenTheKeyIsMissing_AndReadWhenSet()
    {
        Assert.False(DarlingConfig.Parse("{\"web\":{\"enabled\":true}}").Web!.AllowWindowsAuthOverUnverifiedTls);
        Assert.False(DarlingConfig.Parse("{}").Web?.AllowWindowsAuthOverUnverifiedTls ?? false);
        Assert.True(DarlingConfig.Parse("{\"web\":{\"allowWindowsAuthOverUnverifiedTls\":true}}").Web!.AllowWindowsAuthOverUnverifiedTls);
    }
}
