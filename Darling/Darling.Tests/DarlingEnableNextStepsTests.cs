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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6474: after <c>--enable-web</c> / <c>--enable-mcp</c> turn an endpoint on, the verb prints the address to open or the
/// <c>claude mcp add</c> command, and the verb that shows the token. The verbs need a live store to run end to end, so these
/// drive the step they call, <c>WriteEnableNextSteps</c>, with a config read from a temp darling.json and the port the store
/// row reports.
/// </summary>
public sealed class DarlingEnableNextStepsTests
{
    private const string Store = "Host=127.0.0.1;Port=5432;Username=darling;Password=x;Database=darlingtest";

    private static string Run(bool mcp, string surfaceExtra, int port, bool managed = false)
    {
        var root = Directory.CreateTempSubdirectory("darling-enable-steps-");
        try
        {
            var surface = mcp ? "mcp" : "web";
            var path = Path.Combine(root.FullName, "darling.json");
            File.WriteAllText(path,
                ("{ 'postgres': { 'managed': " + (managed ? "true" : "false") + ", 'connectionString': '" + Store + "' }, 'servers': [ { 'host': 'SQL2022' } ], '"
                 + surface + "': { 'enabled': true, 'port': 1" + surfaceExtra + " } }").Replace('\'', '"'));
            var output = new StringWriter();
            DarlingCliCommands.WriteEnableNextSteps(mcp, DarlingConfig.Load(path), port, output);
            return output.ToString();
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void EnableWeb_PrintsTheAddressOnTheStorePort_AndTheVerbThatShowsTheToken()
    {
        var text = Run(mcp: false, ", 'network': { 'token': 'env:SOME_TOKEN' }", port: 5199);

        Assert.Contains("http://localhost:5199/", text, StringComparison.Ordinal);
        Assert.Contains("--print-web-token", text, StringComparison.Ordinal);
        Assert.DoesNotContain("SOME_TOKEN", text, StringComparison.Ordinal);
    }

    [Fact]
    public void EnableWeb_WithNoToken_PrintsTheAddressAndNoTokenVerb()
    {
        var text = Run(mcp: false, ", 'allowTokenlessLoopback': true", port: 5153);

        Assert.Contains("http://localhost:5153/", text, StringComparison.Ordinal);
        Assert.DoesNotContain("--print-web-token", text, StringComparison.Ordinal);
    }

    [Fact]
    public void EnableMcp_PrintsTheClaudeCommandWithThePlaceholder_AndTheVerbThatShowsTheToken()
    {
        var text = Run(mcp: true, ", 'network': { 'token': 'env:SOME_TOKEN' }", port: 5151);

        Assert.Contains(
            "claude mcp add --transport http --scope user sql-monitor-darling http://localhost:5151/ --header \"Authorization: Bearer <token>\"",
            text, StringComparison.Ordinal);
        Assert.Contains("--print-mcp-token", text, StringComparison.Ordinal);
        Assert.DoesNotContain("SOME_TOKEN", text, StringComparison.Ordinal);
    }

    /// <summary>The Viewer's Settings button copies <c>ClaudeMcpAddCommand(port)</c>; the verb must print that exact string.</summary>
    [Theory]
    [InlineData(5151)]
    [InlineData(6200)]
    public void EnableMcp_PrintsTheSameCommandTheViewerCopies(int port)
    {
        var text = Run(mcp: true, ", 'network': { 'token': 'env:SOME_TOKEN' }", port);

        Assert.Contains(
            DarlingEndpointClientText.ClaudeMcpAddCommand(port.ToString(CultureInfo.InvariantCulture)),
            text, StringComparison.Ordinal);
    }

    /// <summary>No token and no opt-out: the service does not start the endpoint, <c>EnableTokenNote</c> has said so, and an address
    /// that refuses the connection (or a command that cannot work) is not a next step. Fails on the code that printed it anyway.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void EnableVerb_WhenTheServiceWillNotStartTheEndpoint_PrintsNoNextStep(bool mcp)
    {
        Assert.Equal(string.Empty, Run(mcp, "", port: 5151));
    }

    /// <summary>A named-token (<c>mcp.tokens</c>) deployment starts without a legacy token: the command is printed, the legacy
    /// verb is not offered, and the no-token sentence is not printed.</summary>
    [Fact]
    public void EnableMcp_WithOnlyNamedTokens_PrintsTheCommand_AndPointsAtTheTokensList()
    {
        var extra = ", 'tokens': [ { 'name': 'ops-lead', 'token': 'env:SOME_TOKEN', 'role': 'operator' } ]";
        var text = Run(mcp: true, extra, port: 5151);

        Assert.Contains("claude mcp add", text, StringComparison.Ordinal);
        Assert.Contains("mcp.tokens", text, StringComparison.Ordinal);
        Assert.DoesNotContain("--print-mcp-token", text, StringComparison.Ordinal);
        Assert.DoesNotContain("sets no MCP token", text, StringComparison.Ordinal);
        Assert.DoesNotContain("SOME_TOKEN", text, StringComparison.Ordinal);
        Assert.Null(DarlingCliCommands.EnableTokenNote(true, LoadConfig(true, extra)));
    }

    /// <summary>A TLS network listener on a wildcard listen is ONE listener, HTTPS for loopback too (#5288 review F11): the loopback
    /// address is printed over https. A TLS listener on a specific LAN IP also binds plain loopback, so that stays http.</summary>
    [Theory]
    [InlineData(false, "0.0.0.0", "https://localhost:5153/")]
    [InlineData(false, "192.168.1.205", "http://localhost:5153/")]
    [InlineData(true, "0.0.0.0", "https://localhost:5153/")]
    [InlineData(true, "192.168.1.205", "http://localhost:5153/")]
    public void EnableVerb_NamesTheSchemeTheLoopbackListenerSpeaks(bool mcp, string listen, string expectedAddress)
    {
        var extra = ", 'network': { 'listen': '" + listen + "', 'allowFrom': '192.168.1.0/24', 'token': 'env:SOME_TOKEN', "
                    + "'tls': { 'pfxPath': 'c:/x.pfx' } }";
        var text = Run(mcp, extra, port: 5153, managed: true);

        Assert.Contains(expectedAddress, text, StringComparison.Ordinal);
    }

    /// <summary>Opt-out on, no token: the MCP sentence says the setting is on and drops the header; the web address is still printed.</summary>
    [Fact]
    public void EnableMcp_WithTheOptOutAndNoToken_SaysToLeaveTheHeaderOff()
    {
        var text = Run(mcp: true, ", 'allowTokenlessLoopback': true", port: 5151);

        Assert.Contains("mcp.allowTokenlessLoopback is on, so leave the --header part off", text, StringComparison.Ordinal);
    }

    private static DarlingConfig LoadConfig(bool mcp, string surfaceExtra)
    {
        var root = Directory.CreateTempSubdirectory("darling-enable-steps-");
        try
        {
            var path = Path.Combine(root.FullName, "darling.json");
            File.WriteAllText(path,
                ("{ 'postgres': { 'managed': false, 'connectionString': '" + Store + "' }, 'servers': [ { 'host': 'SQL2022' } ], '"
                 + (mcp ? "mcp" : "web") + "': { 'enabled': true, 'port': 1" + surfaceExtra + " } }").Replace('\'', '"'));
            return DarlingConfig.Load(path);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }
}
