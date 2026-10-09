/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using Core = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// A failed connection test answers an MCP or web client with one fixed sentence that names the host and port the
/// caller sent and carries nothing the driver said; the driver's text goes to the service log only, with the typed
/// password removed. The probe is a stand-in that fails with a distinctive message, so a test sees whether that
/// message reaches the reply (it must not) and the log (it must).
///
/// <para>Callers of the probe's error text, and what each now does: the add and edit cores (the MCP
/// <c>add_servers</c> / <c>edit_server</c> tools and the web <c>POST /api/servers</c> / <c>PATCH /api/servers/{id}</c>
/// routes, which hand the body to those cores) give the fixed sentence; the <c>--add-server</c> verb, typed at the
/// service host's own command line, and the <c>--test-connection</c> verb and the <c>test_connect</c> command the
/// desktop viewer sends keep the driver's text, because their reader is the person administering the service.</para>
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class ConnectionTestReplyTests : IDisposable
{
    private const string DriverText = "Distinctive-Driver-Text: login failed for user 'svc_probe' from 10.9.8.7 (certificate CN=hidden-name)";
    private const string Password = "Synth-Reply-Pw-1";

    private readonly DarlingOwnedSet _ownedBefore = DarlingOwnedSecrets.Current;

    public ConnectionTestReplyTests() => DarlingOwnedSecrets.Set(DarlingOwnedSet.Empty);

    public void Dispose() => DarlingOwnedSecrets.Set(_ownedBefore);

    private static readonly DateTime Stamp = new(2026, 10, 5, 12, 0, 0, 123, DateTimeKind.Unspecified);

    private static readonly Core.ServerProbe Failing = (_, _) => Task.FromResult(
        new ConnectionProbeResult(false, 0, 0, null, false, false, false, false, DriverText + " password=" + Password));

    private static string AddBody() =>
        JsonSerializer.Serialize(new[] { new { host = "sql-test-01", port = 1444, auth = "SQL", username = "monitor", password = Password } });

    private sealed class EmptyDefinitions : Core.IServerDefinitions
    {
        public Task<List<string>> LoadStorageKeysAsync(CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<string?> ReadStorageKeyAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<string?>(null);

        public Task<int> InsertAsync(Core.ParsedServerEntry entry, string? encryptedPassword, string? actualStorageKey, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("a server that could not be reached must not be written");
    }

    private sealed class OneRowStore : Core.IServerEditStore
    {
        public Core.ServerEditRow Row { get; } = new(
            ServerId: 41, Name: "alpha-01", Host: "alpha-01.example.test", Port: 0, Database: null, ReadOnlyIntent: false, Engine: "sqlserver",
            Auth: "sql", Username: "monitor", EncryptMode: "Mandatory", TrustServerCertificate: false,
            MultiSubnetFailover: false, MonthlyCostUsd: 10m, ModifiedAt: Stamp.AddTicks(7));

        public Task<Core.ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<Core.ServerEditRow?>(Row);

        public Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<Core.ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<Core.EditColumnValue> sets, string? newStorageKey, string? actualStorageKey, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("a server that could not be reached must not be saved");
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));

        public string Joined => string.Join("\n", Lines);
    }

    private static string DetailOf(string answer) => JsonNode.Parse(answer)!["results"]![0]!["detail"]!.GetValue<string>();

    private static void AssertFixedReply(string reply, string target)
    {
        Assert.Contains("Could not connect to " + target + ". The service log has the details.", reply, StringComparison.Ordinal);
        Assert.DoesNotContain("Distinctive-Driver-Text", reply, StringComparison.Ordinal);
        Assert.DoesNotContain("svc_probe", reply, StringComparison.Ordinal);
        Assert.DoesNotContain("10.9.8.7", reply, StringComparison.Ordinal);
        Assert.DoesNotContain("hidden-name", reply, StringComparison.Ordinal);
        Assert.DoesNotContain(Password, reply, StringComparison.Ordinal);
    }

    private static void AssertDriverTextLoggedWithoutThePassword(CapturingLogger log)
    {
        Assert.Contains(log.Lines, l => l.Contains("Distinctive-Driver-Text", StringComparison.Ordinal)
            && l.Contains("sql-test-01,1444", StringComparison.Ordinal));
        Assert.DoesNotContain(Password, log.Joined, StringComparison.Ordinal);
        Assert.Contains("[redacted]", log.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task McpAdd_FailedProbe_RepliesWithTheFixedSentence_AndLogsTheDriverText()
    {
        var log = new CapturingLogger();

        var answer = await Core.AddServersAsync(new EmptyDefinitions(), AddBody(), Failing, CancellationToken.None, ring: TestKeyRings.Healthy, logger: log);

        Assert.Equal("connection_failed", JsonNode.Parse(answer)!["results"]![0]!["status"]!.GetValue<string>());
        AssertFixedReply(answer, "sql-test-01,1444");
        AssertDriverTextLoggedWithoutThePassword(log);
    }

    [Fact]
    public async Task McpAdd_FailedProbe_WithNoDriverText_StillNamesTheHostAndPort()
    {
        var probe = (Core.ServerProbe)((_, _) => Task.FromResult(new ConnectionProbeResult(false, 0, 0, null, false, false, false, false, null)));

        var answer = await Core.AddServersAsync(new EmptyDefinitions(), AddBody(), probe, CancellationToken.None, ring: TestKeyRings.Healthy);

        Assert.Equal("Could not connect to sql-test-01,1444. The service log has the details.", DetailOf(answer));
    }

    [Fact]
    public async Task TheHostCommandLineAdd_KeepsTheDriverText_ForThePersonAtTheServiceHost()
    {
        var answer = await Core.AddServersAsync(
            new EmptyDefinitions(), AddBody(), Failing, CancellationToken.None, allowSecretReferences: true, ring: TestKeyRings.Healthy, revealConnectError: true);

        var detail = DetailOf(answer);
        Assert.Contains("Distinctive-Driver-Text", detail, StringComparison.Ordinal);
        Assert.DoesNotContain(Password, detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task McpEdit_FailedProbe_RepliesWithTheFixedSentence_AndLogsTheDriverText()
    {
        var log = new CapturingLogger();
        var body = "{\"host\":\"beta-02.example.test\",\"password\":\"" + Password + "\"}";

        var answer = await Core.EditServerCoreAsync(new OneRowStore(), 41, body, Failing, TestKeyRings.Healthy, log, CancellationToken.None);

        Assert.Equal("connection_failed", JsonNode.Parse(answer)!["status"]!.GetValue<string>());
        var message = JsonNode.Parse(answer)!["message"]!.GetValue<string>();
        AssertFixedReply(message, "beta-02.example.test");
        Assert.Contains("Nothing was saved.", message, StringComparison.Ordinal);
        Assert.Contains(log.Lines, l => l.Contains("Distinctive-Driver-Text", StringComparison.Ordinal) && l.Contains("beta-02.example.test", StringComparison.Ordinal));
        Assert.DoesNotContain(Password, log.Joined, StringComparison.Ordinal);
    }

    private sealed class Rig : IAsyncDisposable
    {
        public required WebApplication App { get; init; }

        public required HttpClient Client { get; init; }

        public required CapturingLogger Probe { get; init; }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await App.DisposeAsync();
        }
    }

    /// <summary>The web routes over the real add and edit cores with the failing probe, so what the browser is
    /// answered is what the cores say.</summary>
    private static async Task<Rig> StartWebAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        var routeLog = new CapturingLogger();
        var probeLog = new CapturingLogger();
        var source = NpgsqlDataSource.Create("Host=localhost;Database=never_opened;Username=nobody");

        app.Use(async (context, next) =>
        {
            context.Items[DarlingWebSeat.HttpContextItemKey] = new DarlingWebSeat("alice", true);
            await next(context);
        });
        DarlingWebEndpoints.MapServers(
            app, source, routeLog,
            body => Core.AddServersAsync(new EmptyDefinitions(), body, Failing, CancellationToken.None, ring: TestKeyRings.Healthy, logger: probeLog),
            null,
            (id, body) => Core.EditServerCoreAsync(new OneRowStore(), id, body, Failing, TestKeyRings.Healthy, null, CancellationToken.None, probeLog),
            null, null);
        await app.StartAsync(TestContext.Current.CancellationToken);
        return new Rig { App = app, Client = app.GetTestClient(), Probe = probeLog };
    }

    [Fact]
    public async Task WebAdd_FailedProbe_ReplyCarriesNoDriverText_AndTheLogHasIt()
    {
        await using var rig = await StartWebAsync();
        var ct = TestContext.Current.CancellationToken;

        using var response = await rig.Client.PostAsync("/api/servers", new StringContent(AddBody(), Encoding.UTF8, "application/json"), ct);
        var reply = await response.Content.ReadAsStringAsync(ct);

        AssertFixedReply(DetailOf(reply), "sql-test-01,1444");
        AssertFixedReply(reply, "sql-test-01,1444");
        Assert.Contains(rig.Probe.Lines, l => l.Contains("Distinctive-Driver-Text", StringComparison.Ordinal));
        Assert.DoesNotContain(Password, rig.Probe.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WebEdit_FailedProbe_ReplyCarriesNoDriverText_AndTheLogHasIt()
    {
        await using var rig = await StartWebAsync();
        var ct = TestContext.Current.CancellationToken;
        using var request = new HttpRequestMessage(HttpMethod.Patch, "/api/servers/41")
        {
            Content = new StringContent(
                "{\"host\":\"beta-02.example.test\",\"password\":\"" + Password + "\",\"expected_modified_at\":\"" + Core.ModifiedAtToken(new OneRowStore().Row.ModifiedAt) + "\"}",
                Encoding.UTF8, "application/json"),
        };

        using var response = await rig.Client.SendAsync(request, ct);
        var reply = await response.Content.ReadAsStringAsync(ct);

        AssertFixedReply(reply, "beta-02.example.test");
        Assert.Contains(rig.Probe.Lines, l => l.Contains("Distinctive-Driver-Text", StringComparison.Ordinal));
        Assert.DoesNotContain(Password, rig.Probe.Joined, StringComparison.Ordinal);
    }

    /// <summary>The web routes' own wiring (the real store, the real probe) passes the logger the driver text goes
    /// to; the stand-ins above cannot see that, so the source is read.</summary>
    [Fact]
    public void TheWebRoutesPassTheServiceLogger_ToBothCores()
    {
        var text = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs");
        Assert.Contains("DarlingMcpServerAdminTools.AddServers(postgres, body, logger)", text, StringComparison.Ordinal);
        Assert.Contains("CancellationToken.None, probeLogger: logger)", text, StringComparison.Ordinal);
    }
}
