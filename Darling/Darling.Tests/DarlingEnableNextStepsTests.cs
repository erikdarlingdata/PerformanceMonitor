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

    private static string Run(bool mcp, string surfaceExtra, int port)
    {
        var root = Directory.CreateTempSubdirectory("darling-enable-steps-");
        try
        {
            var surface = mcp ? "mcp" : "web";
            var path = Path.Combine(root.FullName, "darling.json");
            File.WriteAllText(path,
                ("{ 'postgres': { 'managed': false, 'connectionString': '" + Store + "' }, 'servers': [ { 'host': 'SQL2022' } ], '"
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
}
