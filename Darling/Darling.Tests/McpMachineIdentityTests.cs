/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using Core = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #6285: add_servers and edit_server over MCP recognise one machine reached under two names. The host text cannot tell a short
/// name from its full domain name, or a name from its address; the name the server reports for itself, read on the connection
/// test, is compared with the name collected for each registration. The text match (#6282) stays beside it.
/// A stubbed probe stands in for the connection and a stubbed store for the registrations; the live store lookup and the
/// SQL Server read are in <c>McpMachineIdentityLiveTests</c> and <c>McpMachineIdentitySqlServerLiveTests</c>.
/// </summary>
public sealed class McpMachineIdentityTests
{
    private const int HeldId = 50;
    private const int RouteId = 9;

    private static Core.RegisteredTarget Held(string host, string? identity, int serverId = HeldId, bool removed = false) =>
        new(serverId, host, removed ? null : host, removed ? null : "sqlserver", 0, "prod-orders", removed, identity);

    private static Core.ServerProbe Reports(string? identity) => (_, _) => Task.FromResult(
        new ConnectionProbeResult(true, 16, 3, "Enterprise", false, false, false, true, null, MachineIdentity: identity));

    private static readonly Core.ServerProbe Unreachable = (_, _) => Task.FromResult(
        new ConnectionProbeResult(false, 0, 0, null, false, false, false, false, "No such host is known."));

    /// <summary>A definitions table that holds the given registrations, scopes an agent route to <see cref="HeldId"/> only,
    /// and records every insert.</summary>
    private sealed class Definitions(List<Core.RegisteredTarget> held, int routedId = HeldId) : Core.IServerDefinitions
    {
        public List<string> Inserted { get; } = [];

        public Task<List<Core.RegisteredTarget>> LoadTargetsAsync(CancellationToken cancellationToken) => Task.FromResult(held);

        public Task<int?> ScopedAgentRouteAsync(int serverId, CancellationToken cancellationToken) =>
            Task.FromResult<int?>(serverId == routedId ? RouteId : null);

        public Task<List<string>> LoadStorageKeysAsync(CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<string?> ReadStorageKeyAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<string?>(null);

        public Task<int> InsertAsync(Core.ParsedServerEntry entry, string? encryptedPassword, string? actualStorageKey, Core.HeldCheck? heldCheck, CancellationToken cancellationToken)
        {
            Inserted.Add(entry.ProbeConfig.Host);
            return Task.FromResult(1);
        }
    }

    private static async Task<(string Status, string Detail)> AddAsync(Definitions definitions, string host, Core.ServerProbe probe)
    {
        var answer = JsonNode.Parse(await Core.AddServersAsync(definitions, JsonSerializer.Serialize(new[] { new { host } }), probe, CancellationToken.None,
            ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default))!;
        var row = answer["results"]!.AsArray()[0]!;
        return (row["status"]!.GetValue<string>(), row["detail"]!.GetValue<string>());
    }

    [Theory]
    [InlineData("sqlprod01.corp.example.test", "SQLPROD01")]
    [InlineData("10.0.0.5", "sqlprod01")]
    [InlineData("10.0.0.5,1444", "SqlProd01")]
    [InlineData("sqlprod01.corp.example.test\\MSSQLSERVER", " sqlprod01 ")]
    public async Task AnotherNameForAHeldMachine_IsRefused_LikeTheTextMatch(string host, string reported)
    {
        var definitions = new Definitions([Held("sqlprod01", "sqlprod01")]);

        var (status, detail) = await AddAsync(definitions, host, Reports(reported));

        Assert.Equal("collides", status);
        Assert.Contains("prod-orders", detail, StringComparison.Ordinal);
        Assert.Contains($"route {RouteId}", detail, StringComparison.Ordinal);
        Assert.Contains("would sit outside that route", detail, StringComparison.Ordinal);
        Assert.Empty(definitions.Inserted);
    }

    [Fact]
    public async Task ADifferentInstanceOnTheSameMachine_IsNotTheHeldMachine()
    {
        var definitions = new Definitions([Held("sqlprod01", "sqlprod01")]);

        var (status, _) = await AddAsync(definitions, "sqlprod01.corp.example.test\\REPORTS", Reports("SQLPROD01\\REPORTS"));

        Assert.Equal("added", status);
        Assert.Equal(["sqlprod01.corp.example.test\\REPORTS"], definitions.Inserted);
    }

    [Fact]
    public async Task ARegistrationWithNoCollectedIdentity_IsMatchedByItsTextAlone()
    {
        var definitions = new Definitions([Held("sqlprod01", null)]);

        /* The server says it is sqlprod01, but nothing was collected for the held registration: another name goes through. */
        var (other, _) = await AddAsync(definitions, "sqlprod01.corp.example.test", Reports("SQLPROD01"));
        Assert.Equal("added", other);

        /* Its own spelling is still refused, by the #6282 rule that needs no identity. */
        var (same, _) = await AddAsync(definitions, "SQLPROD01.", Reports("SQLPROD01"));
        Assert.Equal("collides", same);
    }

    [Fact]
    public async Task AFailedConnectionTest_FallsBackToTheTextMatch()
    {
        var definitions = new Definitions([Held("sqlprod01", "sqlprod01")]);

        /* No identity was read, so another name is not recognised: it is the connection failure it was before. */
        var (other, _) = await AddAsync(definitions, "sqlprod01.corp.example.test", Unreachable);
        Assert.Equal("connection_failed", other);

        /* The text match answers without a connection. */
        var (same, _) = await AddAsync(definitions, "tcp:sqlprod01,1433", Unreachable);
        Assert.Equal("collides", same);
        Assert.Empty(definitions.Inserted);
    }

    [Fact]
    public async Task AServerThatReportsNoIdentity_IsHeldToTheTextMatch()
    {
        var definitions = new Definitions([Held("sqlprod01", "sqlprod01")]);

        var (status, _) = await AddAsync(definitions, "sqlprod01.corp.example.test", Reports(null));

        Assert.Equal("added", status);
    }

    [Fact]
    public async Task ARemovedRegistrationsCollectedIdentity_HoldsItsMachineToo()
    {
        var definitions = new Definitions([Held("sqlprod01:orders", "sqlprod01", removed: true)]);

        var (status, detail) = await AddAsync(definitions, "10.0.0.5", Reports("sqlprod01"));

        Assert.Equal("collides", status);
        Assert.Contains("a removed registration", detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WithoutARouteScopedToTheHeldRegistration_AnotherNameIsAdded()
    {
        var definitions = new Definitions([Held("sqlprod01", "sqlprod01", serverId: HeldId + 1)]);

        var (status, _) = await AddAsync(definitions, "10.0.0.5", Reports("sqlprod01"));

        Assert.Equal("added", status);
    }

    [Theory]
    [InlineData(null, 3)]
    [InlineData("   ", 3)]
    [InlineData("SQLPROD01", 5)]
    public void NoIdentityIsComparable_WhenTheServerNamedNoMachine(string? reported, int edition) =>
        Assert.Null(Core.MachineIdentityOf(reported, edition));

    [Fact]
    public void TheIdentity_IsFoldedToLowerCase_AndKeepsTheInstance()
    {
        Assert.Equal("sqlprod01", Core.MachineIdentityOf(" SQLPROD01 ", 3));
        Assert.Equal("sqlprod01\\reports", Core.MachineIdentityOf("SQLPROD01\\REPORTS", 3));
    }

    /* ---------------- edit_server ---------------- */

    private sealed class OneRowStore : Core.IServerEditStore
    {
        public Core.ServerEditRow Row { get; } = new(
            ServerId: 41, Name: "alpha-01", Host: "alpha-01.example.test", Port: 0, Database: null, ReadOnlyIntent: false, Engine: "sqlserver",
            Auth: "windows", Username: null, EncryptMode: "Mandatory", TrustServerCertificate: false,
            MultiSubnetFailover: false, MonthlyCostUsd: 10m, ModifiedAt: new DateTime(2026, 10, 5, 12, 0, 0, DateTimeKind.Unspecified));

        public Task<Core.ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<Core.ServerEditRow?>(Row);

        public Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<Core.ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<Core.EditColumnValue> sets, string? newStorageKey, string? actualStorageKey, Core.HeldCheck? heldCheck, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("the edit was refused, so nothing is written");
    }

    private static async Task<(string Status, string Message)> EditToAsync(string host, Core.ServerProbe probe, List<Core.RegisteredTarget> held)
    {
        var (changes, error) = Core.ParseEditChanges(JsonSerializer.Serialize(new { host }));
        Assert.Null(error);
        var answer = JsonNode.Parse(await Core.EditServerCoreAsync(new OneRowStore(), 41, changes!, probe, TestKeyRings.Healthy, null, CancellationToken.None,
            policy: DarlingMcpAddServersPolicy.Default,
            agentRouteCovering: (id, _) => Task.FromResult<int?>(id == HeldId ? RouteId : null),
            loadTargets: _ => Task.FromResult(held)))!;
        return (answer["status"]!.GetValue<string>(), answer["message"]!.GetValue<string>());
    }

    [Fact]
    public async Task AnEditThatMovesARegistrationToAnotherNameOfAHeldMachine_IsRefused()
    {
        var (status, message) = await EditToAsync("10.0.0.5", Reports("SQLPROD01"), [Held("sqlprod01", "sqlprod01")]);

        Assert.Equal("refused", status);
        Assert.Contains("prod-orders", message, StringComparison.Ordinal);
        Assert.Contains($"route {RouteId}", message, StringComparison.Ordinal);
        Assert.Contains("moved there, would sit outside that route", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnEditToAnotherNameOfAHeldMachine_WithNoIdentityRead_IsNotTheIdentityRefusal()
    {
        /* The connection test failed: the edit answers as it did before, the text rule being the only one applied. */
        var (status, _) = await EditToAsync("10.0.0.5", Unreachable, [Held("sqlprod01", "sqlprod01")]);

        Assert.Equal("connection_failed", status);
    }

    /* ---------------- round 1 of the security review (M2, L1) and PostgreSQL (#6339) ---------------- */

    private static int IdOf(string host) => PerformanceMonitor.Common.ServerIdHelper.GetDeterministicHashCode(new MonitoredServer { Host = host }.StorageName);

    [Fact]
    public async Task AddingAnotherSpellingOfARemovedCoveredKey_IsRefused_AndTheExactKeyReusesItsId()
    {
        /* The removed registration was stored as SQLPROD01 (its id is that spelling's hash) and a route is scoped to that id. */
        var removed = Held("SQLPROD01", null, serverId: IdOf("SQLPROD01"), removed: true);

        /* Another spelling is a new id outside the route: refused (the storage-name skip used to wave it through). */
        var lower = new Definitions([removed], routedId: removed.ServerId);
        var (status, detail) = await AddAsync(lower, "sqlprod01", Reports("unrelated"));
        Assert.Equal("collides", status);
        Assert.Contains("a removed registration", detail, StringComparison.Ordinal);
        Assert.Empty(lower.Inserted);

        /* The exact key makes the same id, so the route that covers it follows the new row. */
        var exact = new Definitions([removed], routedId: removed.ServerId);
        Assert.Equal("added", (await AddAsync(exact, "SQLPROD01", Reports("unrelated"))).Status);
    }

    [Fact]
    public async Task AnEditToARemovedCoveredKey_IsRefused_BecauseAnEditKeepsItsOwnId()
    {
        var removed = Held("10.0.0.5", null, serverId: IdOf("10.0.0.5"), removed: true);
        var held = new List<Core.RegisteredTarget> { removed };

        var (changes, error) = Core.ParseEditChanges(JsonSerializer.Serialize(new { host = "10.0.0.5" }));
        Assert.Null(error);
        var answer = JsonNode.Parse(await Core.EditServerCoreAsync(new OneRowStore(), 41, changes!, Reports(null), TestKeyRings.Healthy, null, CancellationToken.None,
            policy: DarlingMcpAddServersPolicy.Default,
            agentRouteCovering: (id, _) => Task.FromResult<int?>(id == removed.ServerId ? RouteId : null),
            loadTargets: _ => Task.FromResult(held)))!;

        Assert.Equal("refused", answer["status"]!.GetValue<string>());
        Assert.Contains("a removed registration", answer["message"]!.GetValue<string>(), StringComparison.Ordinal);
    }

    private static async Task<JsonNode> EditSubnetAsync(string probedIdentity, List<Core.RegisteredTarget> held)
    {
        var (changes, error) = Core.ParseEditChanges(JsonSerializer.Serialize(new { multi_subnet_failover = true }));
        Assert.Null(error);
        return JsonNode.Parse(await Core.EditServerCoreAsync(new OneRowStore(), 41, changes!, Reports(probedIdentity), TestKeyRings.Healthy, null, CancellationToken.None,
            policy: DarlingMcpAddServersPolicy.Default,
            agentRouteCovering: (id, _) => Task.FromResult<int?>(id == HeldId ? RouteId : null),
            loadTargets: _ => Task.FromResult(held)))!;
    }

    [Fact]
    public async Task AnEditThatChangesOnlyMultiSubnetFailover_IsHeldToTheIdentityTheServerReports()
    {
        var answer = await EditSubnetAsync("sqlprod01", [Held("sqlprod01", "sqlprod01")]);

        Assert.Equal("refused", answer["status"]!.GetValue<string>());
        Assert.Contains($"route {RouteId}", answer["message"]!.GetValue<string>(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnEditThatChangesOnlyMultiSubnetFailover_IsNotRefusedForAMachineItAlreadyWas_OrByHostText()
    {
        /* The registration already reports the held identity (it sits beside the covered one from before the route): the machine
           did not change, so nothing is refused and the edit reaches the write. */
        await Assert.ThrowsAsync<InvalidOperationException>(() => EditSubnetAsync("sqlprod01",
            [Held("sqlprod01", "sqlprod01"), Held("alpha-01", "sqlprod01", serverId: 41)]));

        /* A held registration with the same host text but no identity: the address did not move, so the text rule is not applied. */
        await Assert.ThrowsAsync<InvalidOperationException>(() => EditSubnetAsync("another", [Held("alpha-01.example.test", null)]));
    }

    /* PostgreSQL (#6339): the cluster's system_identifier and port, as the connect probe reads them. */

    private static Core.RegisteredTarget HeldPostgres(string host, int port, string? identity) =>
        new(HeldId, host + ":pg" + (port == 5432 ? "" : ":" + port), host, "postgres", port, "pg-prod", false, identity);

    private static Task<string?> PostgresAddRefusalAsync(Core.RegisteredTarget held, string host, int port, string? probedIdentity)
    {
        var config = new MonitoredServer { Host = host, Engine = "postgres", Port = port };
        var entry = new Core.ParsedServerEntry(0, host, config.StorageName, config, null);
        return Core.TargetHeldRefusalAsync([held], entry, (id, _) => Task.FromResult<int?>(id == HeldId ? RouteId : null), CancellationToken.None,
            probedIdentity: Core.MachineIdentityOf(probedIdentity, 0));
    }

    [Fact]
    public async Task OnePostgresClusterUnderTwoHostNames_IsRefused()
    {
        var refusal = await PostgresAddRefusalAsync(HeldPostgres("db1.corp.example.test", 5432, "7699170741796808760:5432"), "10.2.3.4", 5432, "7699170741796808760:5432");

        Assert.NotNull(refusal);
        Assert.Contains("the server reports the same name for itself", refusal, StringComparison.Ordinal);
        Assert.Contains($"route {RouteId}", refusal, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("7699170741796808760:5433")]
    [InlineData("1234567890123456789:5432")]
    public async Task TwoPostgresClustersOnOneHost_AreNotOneMachine(string reported)
    {
        /* Held: the cluster on 5432. Added: another port or another cluster, on a host name that is not the same text. */
        Assert.Null(await PostgresAddRefusalAsync(HeldPostgres("db1.corp.example.test", 5432, "7699170741796808760:5432"), "10.2.3.4", 5433, reported));
    }

    [Fact]
    public async Task APostgresRegistrationWithNoIdentity_IsMatchedByItsTextAlone()
    {
        /* The role may not call pg_control_system(): nothing was stored, so another name goes through, and the same host text is still held. */
        Assert.Null(await PostgresAddRefusalAsync(HeldPostgres("db1", 5432, null), "10.2.3.4", 5432, "7699170741796808760:5432"));
        Assert.NotNull(await PostgresAddRefusalAsync(HeldPostgres("db1", 5432, null), "DB1", 5432, null));
    }

    /* ---------------- round 2 of the security review (N1): a PostgreSQL login that cannot read the identity ---------------- */

    private static Core.ServerProbe PostgresReports(string? identity, bool aurora = false) => (_, _) => Task.FromResult(
        new ConnectionProbeResult(true, 0, 0, null, false, false, false, false, null, Engine: CollectorTargetEngine.PostgreSql, PostgresMajorVersion: 16,
            IsAurora: aurora, MachineIdentity: identity));

    private static async Task<(string Status, string Detail)> AddPostgresAsync(Definitions definitions, string host, Core.ServerProbe probe)
    {
        var answer = JsonNode.Parse(await Core.AddServersAsync(definitions, JsonSerializer.Serialize(new[] { new { host, engine = "postgres", port = 5432, auth = "sql", username = "monitor", password = "unused", encrypt_mode = "Optional" } }), probe,
            CancellationToken.None, ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default))!;
        var row = answer["results"]!.AsArray()[0]!;
        return (row["status"]!.GetValue<string>(), row["detail"]!.GetValue<string>());
    }

    [Fact]
    public async Task APostgresProbeThatReadsNoIdentity_IsRefusedWhileACoveredPostgresRegistrationHoldsOne_AndTheMessageNamesTheGrant()
    {
        var definitions = new Definitions([HeldPostgres("db1.corp.example.test", 5432, "7699170741796808760:5432")]);

        var (status, detail) = await AddPostgresAsync(definitions, "10.2.3.4", PostgresReports(null));

        Assert.Equal("collides", status);
        Assert.Contains("pg_control_system()", detail, StringComparison.Ordinal);
        Assert.Contains("GRANT EXECUTE", detail, StringComparison.Ordinal);
        Assert.Contains($"route {RouteId}", detail, StringComparison.Ordinal);
        Assert.Empty(definitions.Inserted);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    public async Task APostgresProbeThatReadsNoIdentity_IsNotRefused_WhenNothingHeldHasOne_OrTheTargetIsAurora(bool aurora, bool heldHasIdentity)
    {
        /* No stored identity to be unsure about, or an Aurora cluster, which gives none by design: the text match decides, as before. */
        var definitions = new Definitions([HeldPostgres("db1.corp.example.test", 5432, heldHasIdentity ? "7699170741796808760:5432" : null)]);

        var (status, _) = await AddPostgresAsync(definitions, "10.2.3.4", PostgresReports(null, aurora));

        Assert.Equal("added", status);
        Assert.Equal(["10.2.3.4"], definitions.Inserted);
    }

    [Fact]
    public async Task ASqlServerProbeThatReadsNoIdentity_IsNotHeldAgainstAPostgresRegistrationsIdentity()
    {
        var definitions = new Definitions([HeldPostgres("db1.corp.example.test", 5432, "7699170741796808760:5432")]);

        var (status, _) = await AddAsync(definitions, "10.2.3.4", Reports(null));

        Assert.Equal("added", status);
    }
}
