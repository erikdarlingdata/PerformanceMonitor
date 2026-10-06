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
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using Core = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// The password a request carries is the password itself. The web add and edit routes, the MCP add and edit tools and
/// the <c>test_connect</c> command all ask one question of it (<see cref="DarlingSecretSource.RequestReferenceRefusal"/>),
/// and it is the resolver's own question, so what is refused is exactly what would be dereferenced.
///
/// <para>An edit that sends the password blank keeps whatever is stored, and a test of a stored value only runs at the
/// address that stored it. Each request path is exercised through the real core, with a probe that counts its calls.</para>
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class ServerPasswordFromRequestTests : IDisposable
{
    private const string Typed = "Typed-Pw-Q7-7XK";
    private const string StoredReference = "env:SYNTH_STORED_PASSWORD_REF";
    private static readonly DateTime Stamp = new(2026, 10, 5, 12, 0, 0, 123, DateTimeKind.Unspecified);

    private readonly DarlingOwnedSet _ownedBefore = DarlingOwnedSecrets.Current;

    public ServerPasswordFromRequestTests() => DarlingOwnedSecrets.Set(DarlingOwnedSet.Empty);

    public void Dispose() => DarlingOwnedSecrets.Set(_ownedBefore);

    /* Every shape the resolver dereferences: the prefix at the very start, any text after it (the name or path is trimmed). */
    public static TheoryData<string> References => new()
    {
        "env:X",
        "env:SOME_VARIABLE",
        "env:  SPACED_NAME  ",
        "file:C:\\x",
        "file:C:\\secrets\\sql.txt",
        "file:/run/secrets/sql_password",
        "file:   C:\\spaced path\\x   ",
    };

    private static string RefusalSentence => DarlingSecretSource.RequestReferenceRefusalText;

    /* ---------------- the one predicate ---------------- */

    [Theory]
    [MemberData(nameof(References))]
    public void TheRefusal_IsExactlyWhatTheResolverWouldDereference(string reference)
    {
        Assert.True(DarlingSecretSource.IsReference(reference));
        Assert.Equal(RefusalSentence, DarlingSecretSource.RequestReferenceRefusal(reference));
        Assert.DoesNotContain(reference.Trim(), RefusalSentence, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("hunter2")]
    [InlineData("pa-env:-ss")]
    [InlineData("my file:pw")]
    [InlineData("xenv:X")]
    [InlineData(" env:X")]
    [InlineData("ENV:X")]
    [InlineData("File:C:\\x")]
    [InlineData("")]
    [InlineData("   ")]
    public void ALiteral_IsNotRefused_BecauseTheResolverWouldNotDereferenceIt(string literal)
    {
        Assert.False(DarlingSecretSource.IsReference(literal));
        Assert.Null(DarlingSecretSource.RequestReferenceRefusal(literal));
        Assert.Equal(literal, DarlingSecretSource.Resolve(literal, "test.setting"));
    }

    [Fact]
    public void TheNullPassword_IsNotRefused() => Assert.Null(DarlingSecretSource.RequestReferenceRefusal(null));

    /* ---------------- MCP add_servers (and the web add: the route hands the body to this same core) ---------------- */

    private static string AddBody(string password, string auth = "SQL") =>
        JsonSerializer.Serialize(new[] { new { host = "sql01", auth, username = "monitor", password } });

    private sealed class UntouchedDefinitions : Core.IServerDefinitions
    {
        public Task<List<string>> LoadStorageKeysAsync(CancellationToken cancellationToken) =>
            throw new InvalidOperationException("the store must not be opened for a refused request");

        public Task<string?> ReadStorageKeyAsync(int serverId, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("the store must not be read for a refused request");

        public Task<int> InsertAsync(Core.ParsedServerEntry entry, string? encryptedPassword, string? actualStorageKey, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("nothing may be written for a refused request");
    }

    private sealed class CountingProbe
    {
        public int Calls { get; private set; }

        public MonitoredServer? Last { get; private set; }

        public Core.ServerProbe Probe => (server, _) =>
        {
            Calls++;
            Last = server;
            return Task.FromResult(new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null));
        };
    }

    [Theory]
    [MemberData(nameof(References))]
    public async Task McpAdd_RefusesAReference_WithTheSentence_NoEcho_NoProbe_NoStoreAccess(string reference)
    {
        foreach (var auth in new[] { "SQL", "ServicePrincipal" })
        {
            var probe = new CountingProbe();

            var answer = await Core.AddServersAsync(new UntouchedDefinitions(), AddBody(reference, auth), probe.Probe, CancellationToken.None);

            var node = JsonNode.Parse(answer)!;
            Assert.Equal(1, node["failed"]!.GetValue<int>());
            Assert.Equal(0, node["added"]!.GetValue<int>());
            var result = node["results"]![0]!;
            Assert.Equal("invalid", result["status"]!.GetValue<string>());
            Assert.Equal(RefusalSentence, result["detail"]!.GetValue<string>());
            Assert.DoesNotContain(reference.Trim(), answer, StringComparison.Ordinal);
            Assert.Equal(0, probe.Calls);
        }
    }

    [Fact]
    public void McpAdd_RefusesTheReference_OnEveryPlatform_BeforeTheLiteralRule()
    {
        foreach (var isWindows in new[] { true, false })
        {
            var (entries, invalid, whole) = Core.ParseRequest(AddBody("file:C:\\x"), isWindows);

            Assert.Null(whole);
            Assert.Empty(entries);
            Assert.Equal(RefusalSentence, Assert.Single(invalid).Detail);
        }
    }

    [Fact]
    public void McpAdd_AcceptsAPassword_ThatOnlyContainsAPrefixInTheMiddle()
    {
        var (entries, invalid, _) = Core.ParseRequest(AddBody("pa-env:-ss"), isWindows: true);

        Assert.Empty(invalid);
        Assert.Equal("pa-env:-ss", Assert.Single(entries).PlaintextPassword);
    }

    [Fact]
    public void TheHostCommandLineAdd_StillAcceptsAReference_ThatIsNotDarlingsOwn()
    {
        var (entries, invalid, _) = Core.ParseRequest(AddBody("env:SOME_OTHER_VAR"), isWindows: false, allowSecretReferences: true);

        Assert.Empty(invalid);
        Assert.Equal("env:SOME_OTHER_VAR", Assert.Single(entries).PlaintextPassword);
    }

    [Fact]
    public void ARequestAdd_OfALiteral_OffWindows_PointsAtTheConfigurationFile_NotAtAReference()
    {
        var (_, invalid, _) = Core.ParseRequest(AddBody(Typed), isWindows: false);

        var detail = Assert.Single(invalid).Detail;
        Assert.Contains("configuration file", detail, StringComparison.Ordinal);
        Assert.DoesNotContain(Typed, detail, StringComparison.Ordinal);
    }

    /* ---------------- MCP edit_server (and the web edit: the route hands the body to this same core) ---------------- */

    private static Core.ServerEditRow Row(string host = "alpha-01.example.test", string auth = "sql", string? database = null) => new(
        ServerId: 41, Name: "alpha-01", Host: host, Port: 0, Database: database, ReadOnlyIntent: false, Engine: "sqlserver",
        Auth: auth, Username: auth == "integrated" ? null : "monitor", EncryptMode: "Mandatory", TrustServerCertificate: false,
        MultiSubnetFailover: false, MonthlyCostUsd: 10m, ModifiedAt: Stamp.AddTicks(7));

    /* A store whose row holds a REFERENCE in the secret slot, as an installation seeded from the configuration file does. The
       secret column is the one thing this fake keeps: a write that does not name it leaves the reference byte for byte. */
    private sealed class StoreHoldingAReference : Core.IServerEditStore
    {
        public Core.ServerEditRow Row { get; set; } = ServerPasswordFromRequestTests.Row();

        public string? StoredSecret { get; private set; } = StoredReference;

        public int Writes { get; private set; }

        public Task<Core.ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<Core.ServerEditRow?>(Row);

        public Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<Core.ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<Core.EditColumnValue> sets, string? newStorageKey, string? actualStorageKey, CancellationToken cancellationToken)
        {
            Writes++;
            if (sets.FirstOrDefault(s => s.Column == "encrypted_password") is { } secret)
            {
                StoredSecret = (string?)secret.Value;
            }

            return Task.FromResult(new Core.ServerEditWrite(Core.ServerEditWriteKind.Written, Stamp.AddSeconds(1)));
        }
    }

    private static Task<string> Edit(StoreHoldingAReference store, string changes, CountingProbe probe) =>
        Core.EditServerCoreAsync(store, 41, changes, probe.Probe, isWindows: true, logger: null, CancellationToken.None);

    private static string Status(string answer) => JsonNode.Parse(answer)!["status"]!.GetValue<string>();

    [Theory]
    [MemberData(nameof(References))]
    public async Task McpEdit_RefusesATypedReference_WithTheSentence_NoEcho_NoProbe_NoWrite(string reference)
    {
        foreach (var body in new[]
                 {
                     "{\"host\":\"beta.example.test\",\"password\":" + JsonSerializer.Serialize(reference) + "}",
                     "{\"display_name\":\"Renamed\",\"password\":" + JsonSerializer.Serialize(reference) + "}",
                 })
        {
            var store = new StoreHoldingAReference();
            var probe = new CountingProbe();

            var answer = await Edit(store, body, probe);

            Assert.Equal("invalid", Status(answer));
            Assert.Equal(RefusalSentence, JsonNode.Parse(answer)!["message"]!.GetValue<string>());
            Assert.DoesNotContain(reference.Trim(), answer, StringComparison.Ordinal);
            Assert.Equal(0, probe.Calls);
            Assert.Equal(0, store.Writes);
            Assert.Equal(StoredReference, store.StoredSecret);
        }
    }

    [Fact]
    public async Task McpEdit_AcceptsATypedPassword_ThatOnlyContainsAPrefixInTheMiddle()
    {
        var store = new StoreHoldingAReference();
        var probe = new CountingProbe();

        var answer = await Edit(store, "{\"display_name\":\"Renamed\",\"password\":\"pa-env:-ss\"}", probe);

        Assert.Equal("updated", Status(answer));
        Assert.Equal("pa-env:-ss", probe.Last!.Password);
    }

    [Theory]
    [InlineData("{\"display_name\":\"Renamed\"}")]
    [InlineData("{\"display_name\":\"Renamed\",\"password\":\"\"}")]
    [InlineData("{\"monthly_cost_usd\":99,\"password\":\"\"}")]
    public async Task McpEdit_WithTheStoredSecretAReference_AbsentOrBlankKeepsItByteForByte(string body)
    {
        var store = new StoreHoldingAReference();
        var probe = new CountingProbe();

        var answer = await Edit(store, body, probe);

        Assert.Equal("updated", Status(answer));
        Assert.Equal(1, store.Writes);
        Assert.Equal(StoredReference, store.StoredSecret);
        Assert.Equal(0, probe.Calls);
    }

    [Fact]
    public async Task McpEdit_ATypedLiteral_ReplacesTheStoredReference()
    {
        var store = new StoreHoldingAReference();
        var probe = new CountingProbe();

        var answer = await Edit(store, "{\"display_name\":\"Renamed\",\"password\":\"" + Typed + "\"}", probe);

        Assert.Equal("updated", Status(answer));
        Assert.NotEqual(StoredReference, store.StoredSecret);
        Assert.Equal(Typed, DarlingSecrets.Unprotect(store.StoredSecret!));
        Assert.Equal(Typed, probe.Last!.Password);
        Assert.DoesNotContain(Typed, answer, StringComparison.Ordinal);
    }

    /* The stored reference is never read back by this surface, so a test that has no typed password cannot use it: any change to
       how the server is reached answers with the plain ask, and nothing is probed. */
    [Theory]
    [InlineData("{\"host\":\"elsewhere.example.test\"}")]
    [InlineData("{\"host\":\"elsewhere.example.test\",\"password\":\"\"}")]
    [InlineData("{\"host\":\"alpha-01.example.test\\\\OTHERINSTANCE\"}")]
    [InlineData("{\"database\":\"Other\"}")]
    [InlineData("{\"username\":\"someone_else\"}")]
    public async Task McpEdit_AChangeOfHowTheServerIsReached_WithNoTypedPassword_AsksForIt_AndProbesNothing(string body)
    {
        var store = new StoreHoldingAReference();
        var probe = new CountingProbe();

        var answer = await Edit(store, body, probe);

        Assert.Equal("invalid", Status(answer));
        Assert.Equal(Core.EditPasswordNeededText, JsonNode.Parse(answer)!["message"]!.GetValue<string>());
        Assert.Equal(0, probe.Calls);
        Assert.Equal(0, store.Writes);
        Assert.Equal(StoredReference, store.StoredSecret);
    }

    /* ---------------- the web routes, with the real cores behind them ---------------- */

    private sealed class Web : IAsyncDisposable
    {
        public required WebApplication App { get; init; }

        public required HttpClient Client { get; init; }

        public required NpgsqlDataSource Source { get; init; }

        public required CountingProbe Probe { get; init; }

        public required StoreHoldingAReference Store { get; init; }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await App.DisposeAsync();
            await Source.DisposeAsync();
        }
    }

    private static async Task<Web> StartWebAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        var source = NpgsqlDataSource.Create("Host=localhost;Database=never_opened;Username=nobody");
        var probe = new CountingProbe();
        var store = new StoreHoldingAReference();

        app.Use(async (context, next) =>
        {
            context.Items[DarlingWebSeat.HttpContextItemKey] = new DarlingWebSeat("alice", true);
            await next(context);
        });
        DarlingWebEndpoints.MapServers(
            app, source, new CapturingTestLogger(),
            addServers: body => Core.AddServersAsync(new UntouchedDefinitions(), body, probe.Probe, CancellationToken.None),
            editServer: (id, body) => Core.EditServerCoreAsync(store, id, body, probe.Probe, isWindows: true, logger: null, CancellationToken.None));
        await app.StartAsync(TestContext.Current.CancellationToken);
        return new Web { App = app, Client = app.GetTestClient(), Source = source, Probe = probe, Store = store };
    }

    private static async Task<(HttpStatusCode Status, string Body)> SendAsync(Web web, HttpMethod method, string path, string body)
    {
        var ct = TestContext.Current.CancellationToken;
        using var request = new HttpRequestMessage(method, path) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
        using var response = await web.Client.SendAsync(request, ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(ct));
    }

    [Theory]
    [MemberData(nameof(References))]
    public async Task WebAdd_RefusesAReference_WithTheSentence_NoEcho_NoProbe(string reference)
    {
        await using var web = await StartWebAsync();

        var (status, body) = await SendAsync(web, HttpMethod.Post, "/api/servers", AddBody(reference));

        Assert.Contains(RefusalSentence, body, StringComparison.Ordinal);
        Assert.DoesNotContain(reference.Trim(), body, StringComparison.Ordinal);
        Assert.Equal("invalid", JsonNode.Parse(body)!["results"]![0]!["status"]!.GetValue<string>());
        Assert.True(status is HttpStatusCode.OK or HttpStatusCode.BadRequest);
        Assert.Equal(0, web.Probe.Calls);
    }

    [Theory]
    [MemberData(nameof(References))]
    public async Task WebEdit_RefusesAReference_WithTheSentence_NoEcho_NoProbe_NoWrite(string reference)
    {
        await using var web = await StartWebAsync();
        var json = "{\"display_name\":\"Renamed\",\"password\":" + JsonSerializer.Serialize(reference)
                   + ",\"expected_modified_at\":\"" + Core.ModifiedAtToken(Stamp.AddTicks(7)) + "\"}";

        var (_, body) = await SendAsync(web, HttpMethod.Patch, "/api/servers/41", json);

        Assert.Contains(RefusalSentence, body, StringComparison.Ordinal);
        Assert.DoesNotContain(reference.Trim(), body, StringComparison.Ordinal);
        Assert.Equal(0, web.Probe.Calls);
        Assert.Equal(0, web.Store.Writes);
        Assert.Equal(StoredReference, web.Store.StoredSecret);
    }

    [Fact]
    public async Task WebEdit_ABlankPassword_KeepsTheStoredReference()
    {
        await using var web = await StartWebAsync();
        var json = "{\"display_name\":\"Renamed\",\"password\":\"\",\"expected_modified_at\":\"" + Core.ModifiedAtToken(Stamp.AddTicks(7)) + "\"}";

        var (status, body) = await SendAsync(web, HttpMethod.Patch, "/api/servers/41", json);

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("updated", Status(body));
        Assert.Equal(StoredReference, web.Store.StoredSecret);
        Assert.Equal(0, web.Probe.Calls);
    }

    /* ---------------- the test_connect command ---------------- */

    private static MonitoredServer Tested(string host, string? encrypted, int port = 0, string? password = null) => new()
    {
        Name = "alpha-01", Host = host, Port = port, Auth = "sql", Username = "monitor", EncryptedPassword = encrypted, Password = password,
    };

    /// <summary>The stored row the tested server (<see cref="Tested"/>) matches on every setting.</summary>
    private static ServerConnectionSettings StoredRow(string host, int port = 0) =>
        new(host, port, "sqlserver", null, false, "sql", "monitor", "Mandatory", false, false);

    private sealed class StoredAddresses
    {
        public List<ServerConnectionSettings> Rows { get; } = [];

        public List<string> Asked { get; } = [];

        public Task<IReadOnlyList<ServerConnectionSettings>> ReadAsync(string reference, CancellationToken cancellationToken)
        {
            Asked.Add(reference);
            return Task.FromResult<IReadOnlyList<ServerConnectionSettings>>(Rows.ToList());
        }
    }

    [Fact]
    public async Task TestConnect_AStoredReference_IsResolvedOnlyWhereHostPortAndInstanceAllMatch()
    {
        var stored = new StoredAddresses();
        stored.Rows.Add(StoredRow("alpha-01.example.test\\INST1"));
        var ct = CancellationToken.None;

        Assert.Null(await DarlingCommandExecutor.TestConnectReferenceRefusalAsync(
            Tested("alpha-01.example.test\\INST1", StoredReference), stored.ReadAsync, ct));

        foreach (var (host, port) in new[]
                 {
                     ("elsewhere.example.test\\INST1", 0),
                     ("alpha-01.example.test\\INST2", 0),
                     ("alpha-01.example.test", 0),
                     ("alpha-01.example.test\\INST1", 1434),
                     ("ALPHA-01.example.test\\INST1", 0),
                 })
        {
            Assert.Equal(
                DarlingCommandExecutor.TestConnectPasswordNeededText,
                await DarlingCommandExecutor.TestConnectReferenceRefusalAsync(Tested(host, StoredReference, port), stored.ReadAsync, ct));
        }
    }

    /// <summary>The tested server flipped on one connection setting away from <see cref="StoredRow"/>.</summary>
    private static MonitoredServer TestedWith(string setting)
    {
        var server = Tested("alpha-01.example.test", StoredReference);
        switch (setting)
        {
            case "engine": server.Engine = "postgresql"; break;
            case "encryptMode": server.EncryptMode = "Optional"; break;
            case "trustServerCertificate": server.TrustServerCertificate = true; break;
            case "auth": server.Auth = "serviceprincipal"; break;
            case "username": server.Username = "someone-else"; break;
            case "database": server.Database = "other_db"; break;
            case "readOnlyIntent": server.ReadOnlyIntent = true; break;
            case "multiSubnetFailover": server.MultiSubnetFailover = true; break;
            case "host": server.Host = "elsewhere.example.test"; break;
            case "port": server.Port = 1434; break;
            case "none": break;
            default: throw new ArgumentOutOfRangeException(nameof(setting), setting, null);
        }

        return server;
    }

    [Theory]
    [InlineData("engine")]
    [InlineData("encryptMode")]
    [InlineData("trustServerCertificate")]
    [InlineData("auth")]
    [InlineData("username")]
    [InlineData("database")]
    [InlineData("readOnlyIntent")]
    [InlineData("multiSubnetFailover")]
    [InlineData("host")]
    [InlineData("port")]
    public async Task TestConnect_AStoredReference_IsRefusedWhenAnySingleConnectionSettingDiffersFromTheStoredServer(string setting)
    {
        var stored = new StoredAddresses();
        stored.Rows.Add(StoredRow("alpha-01.example.test"));

        var refusal = await DarlingCommandExecutor.TestConnectReferenceRefusalAsync(TestedWith(setting), stored.ReadAsync, CancellationToken.None);

        Assert.Equal(DarlingCommandExecutor.TestConnectPasswordNeededText, refusal);
        Assert.Contains("different connection settings", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain(StoredReference, refusal, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TestConnect_AStoredReference_IsResolvedWhenEveryConnectionSettingMatches_IgnoringCaseWhereTheEditCoreDoes()
    {
        var stored = new StoredAddresses();
        stored.Rows.Add(StoredRow("alpha-01.example.test"));
        var server = TestedWith("none");
        server.Auth = "SQL";
        server.EncryptMode = "MANDATORY";
        server.Engine = "SqlServer";

        Assert.Null(await DarlingCommandExecutor.TestConnectReferenceRefusalAsync(TestedWith("none"), stored.ReadAsync, CancellationToken.None));
        Assert.Null(await DarlingCommandExecutor.TestConnectReferenceRefusalAsync(server, stored.ReadAsync, CancellationToken.None));
    }


    [Fact]
    public async Task TestConnect_AReferenceNoStoredServerHolds_IsRefused()
    {
        var stored = new StoredAddresses();

        var refusal = await DarlingCommandExecutor.TestConnectReferenceRefusalAsync(
            Tested("alpha-01.example.test", "file:C:\\x"), stored.ReadAsync, CancellationToken.None);

        Assert.Equal(DarlingCommandExecutor.TestConnectPasswordNeededText, refusal);
        Assert.DoesNotContain("C:\\x", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TestConnect_AReferenceInThePlainPasswordSlot_IsAlwaysRefused_WithoutAskingTheStore()
    {
        var stored = new StoredAddresses();
        stored.Rows.Add(StoredRow("alpha-01.example.test"));

        var refusal = await DarlingCommandExecutor.TestConnectReferenceRefusalAsync(
            Tested("alpha-01.example.test", null, password: "env:X"), stored.ReadAsync, CancellationToken.None);

        Assert.Equal(RefusalSentence, refusal);
        Assert.Empty(stored.Asked);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("Dpapi-Blob-Base64+/==")]
    public async Task TestConnect_ADpapiBlobOrNoPassword_IsNotAskedAbout_AndTheStoreIsNotRead(string? encrypted)
    {
        var stored = new StoredAddresses();

        Assert.Null(await DarlingCommandExecutor.TestConnectReferenceRefusalAsync(
            Tested("anywhere.example.test", encrypted), stored.ReadAsync, CancellationToken.None));
        Assert.Empty(stored.Asked);
    }

    /* ---------------- the configuration file still takes references ---------------- */

    [Fact]
    public void TheConfigurationFile_StillAcceptsAReference_AndMarksItDeclaredByTheFile()
    {
        var config = DarlingConfig.Parse(
            "{ \"servers\": [ { \"host\": \"sql01\", \"auth\": \"sql\", \"username\": \"monitor\", \"encryptedPassword\": \"env:SOME_FILE_DECLARED_REF\" } ] }");

        var server = Assert.Single(config.Servers);
        Assert.Equal("env:SOME_FILE_DECLARED_REF", server.EncryptedPassword);
        Assert.True(server.EncryptedPasswordDeclaredByFile);
    }
}
