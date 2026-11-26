/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9674: a Lite connection test that failed with encrypt_mode Strict says what Strict needs. <c>--add-server</c>, the
/// <c>add_servers</c> and <c>edit_server</c> tools, and the Add Server window all take their failure text from
/// <see cref="ServerEntryRules.TestConnectionAsync"/> and <see cref="ServerEntryRules.FailureText"/>. The wiring tests dial a
/// closed local port, so the failure is the driver's own and no server is involved. The line is for a transport failure only: not for a
/// failure that is not a SqlException (a secret that could not be read), and not for an error the server sent. SqlClient has no public
/// constructor for a server-sent error and this suite has no factory for one, so that branch of <see cref="ServerEntryRules.IsTransportFailure"/>
/// is covered by the Darling suite, which builds real ones against the same rule.
/// </summary>
public sealed class StrictConnectionHintTests
{
    /// <summary>A port that nothing listens on: bind to get a free one, then release it.</summary>
    private static int ClosedLocalPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static ServerConnection Entry(string mode) => new()
    {
        ServerName = "127.0.0.1," + ClosedLocalPort(),
        DisplayName = "closed-port",
        AuthenticationType = AuthenticationTypes.SqlServer,
        EncryptMode = mode,
        TrustServerCertificate = true,
    };

    [Fact]
    public async Task TestConnection_StrictEntryOnAClosedPort_FailsWithTheLine()
    {
        var failure = await ServerEntryRules.TestConnectionAsync(Entry("Strict"), "monitor", "Synth-Pw-1", CancellationToken.None);

        Assert.NotNull(failure);
        Assert.EndsWith(StrictEncryptionHint.Text, failure, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Mandatory")]
    [InlineData("Optional")]
    public async Task TestConnection_OtherModesOnAClosedPort_FailWithoutTheLine(string mode)
    {
        var failure = await ServerEntryRules.TestConnectionAsync(Entry(mode), "monitor", "Synth-Pw-1", CancellationToken.None);

        Assert.NotNull(failure);
        Assert.DoesNotContain(StrictEncryptionHint.Text, failure, StringComparison.Ordinal);
    }

    [Fact]
    public void FailureText_StrictCancellation_HasNoLine()
    {
        var text = ServerEntryRules.FailureText(new OperationCanceledException("Stopped."), "Strict");

        Assert.DoesNotContain(StrictEncryptionHint.Text, text, StringComparison.Ordinal);
    }

    /// <summary>
    /// A Strict entry whose connection could not be built (a missing secret throws before any connection is made) fails with
    /// something other than a SqlException. TLS did not cause that, so no line.
    /// </summary>
    [Fact]
    public void FailureText_StrictNonSqlFailure_HasNoLine()
    {
        var ex = new InvalidOperationException("Broke.");

        Assert.False(ServerEntryRules.IsTransportFailure(ex));
        Assert.Equal("Broke.", ServerEntryRules.FailureText(ex, "Strict"));
        Assert.Equal("Broke.", ServerEntryRules.FailureText(ex, "Mandatory"));
    }

    /// <summary>Lite connects with Strict only for the exact stored word: "strict" builds Mandatory, so it gets no Strict line. The failure is the closed port's real SqlException.</summary>
    [Theory]
    [InlineData("Strict", true)]
    [InlineData("strict", false)]
    [InlineData("Mandatory", false)]
    public async Task FailureText_TheLineFollowsTheModeLiteConnectsWith(string mode, bool expectLine)
    {
        var failure = await ServerEntryRules.TestConnectionAsync(Entry(mode), "monitor", "Synth-Pw-1", CancellationToken.None);

        Assert.NotNull(failure);
        Assert.Equal(expectLine, failure.Contains(StrictEncryptionHint.Text, StringComparison.Ordinal));
    }
}
