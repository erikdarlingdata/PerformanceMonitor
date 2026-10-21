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
    private sealed class Definitions(List<Core.RegisteredTarget> held) : Core.IServerDefinitions
    {
        public List<string> Inserted { get; } = [];

        public Task<List<Core.RegisteredTarget>> LoadTargetsAsync(CancellationToken cancellationToken) => Task.FromResult(held);

        public Task<int?> ScopedAgentRouteAsync(int serverId, CancellationToken cancellationToken) =>
            Task.FromResult<int?>(serverId == HeldId ? RouteId : null);

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
}
