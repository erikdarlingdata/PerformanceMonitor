/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5288: the two hosts (the MCP server and the web dashboard) take their security-relevant settings from code, not from
/// whatever the service environment or an appsettings file adds. These tests pin the glue each host owns, which a
/// test cannot drive through <c>TryStartServerAsync</c> (a network-mode start needs a managed store): that each host
/// runs <see cref="DarlingMcpHostService.PinForwardedHeadersOff"/> on its own builder, and that each host's listener
/// layout starts from an empty Kestrel configuration. The behavior behind each pin is proven where the pipeline is
/// built: <see cref="DarlingAllowFromListGateTests"/> for forwarded headers and the live listener test in this class
/// for configured endpoints.
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

    /* ---- the listener layout starts from an empty Kestrel configuration ---- */

    private const string EmptyConfigurationLine = "options.Configure(new ConfigurationBuilder().Build());";

    /// <summary>
    /// The MCP host's listener layout binds exactly the listeners <c>ConfigureListeners</c> adds, and no endpoint
    /// written in the <c>Kestrel:Endpoints</c> section of configuration (an appsettings file, or a
    /// <c>Kestrel__Endpoints__*</c> variable in the service environment). Over REAL Kestrel on loopback with an
    /// ephemeral port, the way <c>DarlingMcpTlsLiveTests</c> starts its listener: an endpoint is written into the
    /// builder's configuration on a second free port, the host's own <c>ConfigureListeners</c> binds, and the server's
    /// bound addresses are exactly the one the code asked for.
    /// </summary>
    [Fact]
    public async Task McpListeners_BindOnlyTheCodeDefinedEndpoint_NotOnesWrittenInConfiguration()
    {
        var configuredPort = FreeLoopbackPort();

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = Environments.Production });
        builder.Logging.ClearProviders();
        builder.Configuration["Kestrel:Endpoints:Extra:Url"] = $"http://127.0.0.1:{configuredPort}";
        builder.WebHost.ConfigureKestrel(options =>
            DarlingMcpHostService.ConfigureListeners(
                options, networkMode: true, IPAddress.Loopback, effectivePort: 0, certificate: null));

        await using var app = builder.Build();
        await app.StartAsync();
        try
        {
            var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.ToArray();

            var bound = Assert.Single(addresses);
            Assert.NotEqual(configuredPort, new Uri(bound).Port);
        }
        finally
        {
            await app.StopAsync();
        }
    }

    /// <summary>
    /// The web host's Kestrel callback is inline in <c>TryStartServerAsync</c> (a network-mode start needs a managed
    /// store, so no test starts it), so what is pinned from source is the same line the MCP host's
    /// <c>ConfigureListeners</c> runs and proves above: it appears once per host and is the first statement of the
    /// listener layout, ahead of every <c>options.Listen</c>.
    /// </summary>
    [Theory]
    [InlineData(
        "DarlingMcpHostService.cs",
        @"static void ConfigureListeners\([^)]*\)\s*\{\s*ArgumentNullException\.ThrowIfNull\(options\);\s*ArgumentNullException\.ThrowIfNull\(primaryBind\);\s*")]
    [InlineData("DarlingWebHostService.cs", @"\.ConfigureKestrel\(\s*options\s*=>\s*\{\s*")]
    public void EachHost_ListenerLayout_StartsFromAnEmptyKestrelConfiguration(string fileName, string whatPrecedesIt)
    {
        var code = HostCode(fileName);

        Assert.Single(Regex.Matches(code, Regex.Escape(EmptyConfigurationLine)));
        Assert.True(
            Regex.IsMatch(code, whatPrecedesIt + Regex.Escape(EmptyConfigurationLine)),
            $"{fileName}: {EmptyConfigurationLine} must be the first statement of the listener layout");

        var configure = code.IndexOf(EmptyConfigurationLine, StringComparison.Ordinal);
        var firstListen = code.IndexOf("options.Listen", StringComparison.Ordinal);
        Assert.True(
            firstListen > configure,
            $"{fileName}: no listener may be added before the Kestrel configuration is emptied");
    }

    private static int FreeLoopbackPort()
    {
        using var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        return ((IPEndPoint)probe.LocalEndpoint).Port;
    }
}
