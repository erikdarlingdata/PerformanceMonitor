/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
/// The line is not added for a login failure, a cancellation, any other mode, or a PostgreSQL target. The wiring tests dial a
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
    [InlineData("Strict", 0, true)]
    [InlineData("Strict", -1, true)]
    [InlineData("Strict", 18456, false)]
    [InlineData("Strict", 18452, false)]
    [InlineData("Strict", 18470, false)]
    [InlineData("Strict", 18486, false)]
    [InlineData("Strict", 18487, false)]
    [InlineData("Strict", 18488, false)]
    [InlineData("Mandatory", null, false)]
    [InlineData("Optional", null, false)]
    [InlineData("", null, false)]
    [InlineData(null, null, false)]
    public void Hint_AppliesToAStrictNonLoginFailureOnly(string? mode, int? number, bool expected)
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
}
