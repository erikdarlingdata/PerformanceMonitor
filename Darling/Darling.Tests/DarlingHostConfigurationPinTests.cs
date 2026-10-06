/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5288: the two hosts (the MCP server and the web dashboard) take their security-relevant settings from code, not from
/// whatever the service environment or an appsettings file adds. These tests pin the glue each host owns, which a
/// test cannot drive through <c>TryStartServerAsync</c> (a network-mode start needs a managed store): that each host
/// runs <see cref="DarlingMcpHostService.PinForwardedHeadersOff"/> on its own builder, and (added with the listener
/// pins below) that each host's listener layout starts from an empty Kestrel configuration. The behavior behind each
/// pin is proven where the pipeline is built:
/// <see cref="DarlingAllowFromListGateTests"/> for forwarded headers and the live listener test in this class for
/// configured endpoints.
///
/// <para>Source pins read the code with comments and literals blanked
/// (<see cref="CSharpSourceWalker.StripCommentsAndStrings"/>), the way
/// <c>DarlingWebFailureHandlingTests.BothWebHosts_PinEnvironmentNameToProduction_OnTheirOwnCreateBuilderCall</c>
/// reads the same two files, so prose in a comment can neither satisfy nor break one.</para>
/// </summary>
public sealed class DarlingHostConfigurationPinTests
{
    private static string HostCode(string fileName)
        => CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", fileName));

    /// <summary>
    /// Each host pins forwarded-header handling off on the statement right after its <c>WebApplication.CreateBuilder</c>
    /// call, so no service is registered between the builder and the pin, and each host builds exactly one builder,
    /// so a second one cannot start without it. The behavior (an <c>X-Forwarded-For</c> naming an allowed address does
    /// not change who the gates judge, even with the framework switch on) is proven in
    /// <c>DarlingAllowFromListGateTests</c> through the same helper.
    /// </summary>
    [Theory]
    [InlineData("DarlingMcpHostService.cs", "PinForwardedHeadersOff(builder.Services);")]
    [InlineData("DarlingWebHostService.cs", "DarlingMcpHostService.PinForwardedHeadersOff(builder.Services);")]
    public void EachHost_PinsForwardedHeadersOff_OnTheStatementAfterItsOwnCreateBuilder(string fileName, string pinCall)
    {
        var code = HostCode(fileName);

        Assert.Single(Regex.Matches(code, @"WebApplication\.CreateBuilder\("));

        var pinned = new Regex(@"WebApplication\.CreateBuilder\([^;]*?\}\);\s*" + Regex.Escape(pinCall));
        Assert.True(
            pinned.IsMatch(code),
            $"{fileName}: the statement after WebApplication.CreateBuilder(...) must be {pinCall}");
    }
}
