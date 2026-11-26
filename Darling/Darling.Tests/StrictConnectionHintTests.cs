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
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;
using Core = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #9674: a connection test that failed with encrypt_mode Strict says what Strict needs, because the driver's text alone does not.
/// The line is not added for a login failure or another server refusal, a cancellation, any other mode, or a PostgreSQL target. The wiring tests dial a
/// closed local port, so the failure is the driver's own and no server is involved.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class StrictConnectionHintTests : IDisposable
{
    private readonly DarlingOwnedSet _ownedBefore = DarlingOwnedSecrets.Current;

    public StrictConnectionHintTests() => DarlingOwnedSecrets.Set(DarlingOwnedSet.Empty);

    public void Dispose() => DarlingOwnedSecrets.Set(_ownedBefore);

    [Theory]
    [InlineData("Strict", null, true)]
    /* darling.json is hand-edited and the connection builders read the mode trimmed and case-insensitively, so these are Strict too. */
    [InlineData("strict", null, true)]
    [InlineData(" STRICT ", null, true)]
    [InlineData("strict", 18456, false)]
    [InlineData("Strict", 0, true)]
    [InlineData("Strict", -1, true)]
    [InlineData("Strict", 18456, false)]
    [InlineData("Strict", 18452, false)]
    [InlineData("Strict", 18470, false)]
    [InlineData("Strict", 18486, false)]
    [InlineData("Strict", 18487, false)]
    [InlineData("Strict", 18488, false)]
    [InlineData("Strict", 4060, false)]
    [InlineData("Strict", 40532, false)]
    [InlineData("Strict", 40615, false)]
    [InlineData("Mandatory", null, false)]
    [InlineData("Optional", null, false)]
    [InlineData("", null, false)]
    [InlineData(null, null, false)]
    public void Hint_AppliesToAStrictTransportFailureOnly(string? mode, int? number, bool expected)
    {
        Assert.Equal(expected, StrictEncryptionHint.For(mode, number) is not null);
    }

    [Fact]
    public void Append_PutsTheHintOnTheLineAfterTheDriverText()
    {
        var text = StrictEncryptionHint.Append("The driver said no.", "Strict", null);

        Assert.StartsWith("The driver said no." + Environment.NewLine, text, StringComparison.Ordinal);
        Assert.EndsWith(StrictEncryptionHint.Text, text, StringComparison.Ordinal);
        Assert.Equal("The driver said no.", StrictEncryptionHint.Append("The driver said no.", "Mandatory", null));
    }

    private static ConnectionProbeResult Failed(int? number, bool nativeLoad = false) =>
        new(false, 0, 0, null, false, false, false, false, "Driver-Text-Only",
            SqlClientNativeLoadFailed: nativeLoad, SqlErrorNumber: number);

    private static MonitoredServer Server(string mode, string engine = "sqlserver") =>
        new() { Name = "alpha", Host = "alpha.example.test", EncryptMode = mode, Engine = engine };

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ConnectFailureReply_StrictTransportFailure_CarriesTheLine_ForBothReplyShapes(bool reveal)
    {
        var reply = Core.ConnectFailureReply(Server("Strict"), Failed(null), null, reveal);

        Assert.Contains(StrictEncryptionHint.Text, reply, StringComparison.Ordinal);
        Assert.Equal(reveal, reply.Contains("Driver-Text-Only", StringComparison.Ordinal));
    }

    [Fact]
    public void ConnectFailureReply_StrictLoginFailure_HasNoLine()
    {
        Assert.DoesNotContain(StrictEncryptionHint.Text, Core.ConnectFailureReply(Server("Strict"), Failed(18456), null, true), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Mandatory")]
    [InlineData("Optional")]
    public void ConnectFailureReply_OtherModes_HaveNoLine(string mode)
    {
        Assert.DoesNotContain(StrictEncryptionHint.Text, Core.ConnectFailureReply(Server(mode), Failed(null), null, true), StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectFailureReply_NativeLoadFailure_HasNoLine()
    {
        Assert.DoesNotContain(StrictEncryptionHint.Text, Core.ConnectFailureReply(Server("Strict"), Failed(null, nativeLoad: true), null, true), StringComparison.Ordinal);
    }

    /// <summary>Strict does not apply to a PostgreSQL target, whatever its stored mode says.</summary>
    [Fact]
    public void ConnectFailureReply_PostgresTarget_HasNoLine()
    {
        Assert.DoesNotContain(StrictEncryptionHint.Text, Core.ConnectFailureReply(Server("Strict", "postgres"), Failed(null), null, true), StringComparison.Ordinal);
    }

    /// <summary>A port that nothing listens on: bind to get a free one, then release it.</summary>
    private static int ClosedLocalPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class EmptyDefinitions : Core.IServerDefinitions
    {
        public Task<List<Core.RegisteredTarget>> LoadTargetsAsync(CancellationToken cancellationToken) => Task.FromResult(new List<Core.RegisteredTarget>());

        public Task<int?> ScopedAgentRouteAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<int?>(null);

        public Task<List<string>> LoadStorageKeysAsync(CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<string?> ReadStorageKeyAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<string?>(null);

        public Task<int> InsertAsync(Core.ParsedServerEntry entry, string? encryptedPassword, string? actualStorageKey, Core.HeldCheck? heldCheck, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("a server that could not be reached must not be written");
    }

    private static async Task<string> AddAgainstClosedPortAsync(string mode)
    {
        var body = JsonSerializer.Serialize(new[]
        {
            new { host = "127.0.0.1," + ClosedLocalPort(), auth = "SQL", username = "monitor", password = "Synth-Strict-Pw-1", encrypt_mode = mode },
        });
        return await Core.AddServersAsync(
            new EmptyDefinitions(), body, (config, token) => DarlingServerConnector.ProbeAsync(config, null, token),
            CancellationToken.None, ring: TestKeyRings.Healthy);
    }

    [Fact]
    public async Task AddServers_StrictEntryOnAClosedPort_FailsWithTheLine()
    {
        var answer = await AddAgainstClosedPortAsync("Strict");

        Assert.Contains("connection_failed", answer, StringComparison.Ordinal);
        Assert.Contains(StrictEncryptionHint.Text, answer, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddServers_MandatoryEntryOnAClosedPort_FailsWithoutTheLine()
    {
        var answer = await AddAgainstClosedPortAsync("Mandatory");

        Assert.Contains("connection_failed", answer, StringComparison.Ordinal);
        Assert.DoesNotContain(StrictEncryptionHint.Text, answer, StringComparison.Ordinal);
    }

    /// <summary>
    /// A Strict entry whose env: secret is unset fails inside the probe, before any connection is made. TLS did not cause it, so
    /// neither the add reply nor the test reply tells the operator to drop to Mandatory.
    /// </summary>
    [Fact]
    public async Task StrictEntryWithAnUnsetEnvSecret_FailsBeforeConnecting_AndGetsNoLine()
    {
        const string variable = "UNSET_VAR_9674";
        Environment.SetEnvironmentVariable(variable, null);
        var server = new MonitoredServer
        {
            Name = "unset-secret", Host = "127.0.0.1," + ClosedLocalPort(), Auth = "sql", Username = "monitor", Password = "env:" + variable, EncryptMode = "Strict",
        };

        var probe = await DarlingServerConnector.ProbeAsync(server, null, CancellationToken.None);

        Assert.False(probe.Success);
        Assert.DoesNotContain(StrictEncryptionHint.Text, Core.ConnectFailureReply(server, probe, null, true), StringComparison.Ordinal);
        Assert.DoesNotContain(StrictEncryptionHint.Text, Core.ConnectFailureReply(server, probe, null, false), StringComparison.Ordinal);
        Assert.DoesNotContain(StrictEncryptionHint.Text, DarlingCommandExecutor.MapProbeResult(probe, server).ResultJson, StringComparison.Ordinal);
    }

    private static MonitoredServer ClosedPortServer(string mode) => new()
    {
        Name = "closed-port", Host = "127.0.0.1," + ClosedLocalPort(), Auth = "sql", Username = "monitor", Password = "Synth-Strict-Pw-1", EncryptMode = mode,
    };

    private static async Task<string> TestConnectionVerbAsync(string mode)
    {
        var output = new StringWriter();
        var ok = await DarlingCliCommands.ProbeTargetsAsync(
            new[] { ClosedPortServer(mode) }, output, null,
            (server, token) => DarlingServerConnector.ProbeAsync(server, null, token),
            windowsHost: false, "console", CancellationToken.None);
        Assert.False(ok);
        Assert.Contains("[FAIL] closed-port", output.ToString(), StringComparison.Ordinal);
        return output.ToString();
    }

    [Fact]
    public async Task TestConnectionVerb_StrictEntryOnAClosedPort_FailsWithTheLine()
    {
        Assert.Contains(StrictEncryptionHint.Text, await TestConnectionVerbAsync("Strict"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TestConnectionVerb_MandatoryEntryOnAClosedPort_FailsWithoutTheLine()
    {
        Assert.DoesNotContain(StrictEncryptionHint.Text, await TestConnectionVerbAsync("Mandatory"), StringComparison.Ordinal);
    }

    private static async Task<(string Status, string Error)> TestConnectCommandAsync(string mode)
    {
        var server = ClosedPortServer(mode);
        var probe = await DarlingServerConnector.ProbeAsync(server, null, CancellationToken.None);
        var (status, json) = DarlingCommandExecutor.MapProbeResult(probe, server);
        using var document = JsonDocument.Parse(json);
        Assert.False(document.RootElement.GetProperty("success").GetBoolean());
        return (status, document.RootElement.GetProperty("error").GetString()!);
    }

    [Fact]
    public async Task TestConnectCommand_StrictEntryOnAClosedPort_FailsWithTheLine_AndKeepsItsStatusWord()
    {
        var (status, error) = await TestConnectCommandAsync("Strict");

        Assert.Equal("connection_failed", status);
        Assert.Contains(StrictEncryptionHint.Text, error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TestConnectCommand_MandatoryEntryOnAClosedPort_FailsWithoutTheLine()
    {
        var (status, error) = await TestConnectCommandAsync("Mandatory");

        Assert.Equal("connection_failed", status);
        Assert.DoesNotContain(StrictEncryptionHint.Text, error, StringComparison.Ordinal);
    }

    [Fact]
    public void TestConnectCommand_StrictPostgresTarget_AndNativeLoadFailure_HaveNoLine()
    {
        var (_, postgres) = DarlingCommandExecutor.MapProbeResult(Failed(null), Server("Strict", "postgres"));
        var (_, native) = DarlingCommandExecutor.MapProbeResult(Failed(null, nativeLoad: true), Server("Strict"));

        Assert.DoesNotContain(StrictEncryptionHint.Text, postgres, StringComparison.Ordinal);
        Assert.DoesNotContain(StrictEncryptionHint.Text, native, StringComparison.Ordinal);
    }
}
