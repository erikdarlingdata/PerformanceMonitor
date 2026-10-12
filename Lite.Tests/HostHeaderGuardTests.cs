/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1648 Lite side. Lite's MCP host had the same shape as Darling's — loopback-only, tokenless, no Host-header
/// check — so a browser on the machine that loads attacker content could be DNS-rebound to
/// <c>127.0.0.1:{port}</c> and call every tool same-origin (the <c>application/json</c> content type does not
/// force a CORS preflight under a rebind, because the browser treats the request as same-origin). Severity here
/// is informational: single-user desktop app, no service account, no fleet credentials. It is fixed in the same
/// change anyway, running the SAME <see cref="HostHeaderGuard"/> Darling's two hosts run, because a guard that
/// exists in one app's copy and not the other's is exactly the drift this codebase keeps paying for.
///
/// <para>Mirrors <c>Darling.Tests/HostHeaderGuardTests.cs</c>: the pure decision, plus a parse of the REAL host
/// source pinning that the middleware is actually installed first — the defect was a wiring omission, so the
/// decision test alone would have passed on the vulnerable build.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class HostHeaderGuardTests
{
    /* ---------------- the pure decision (Lite is always loopback-only: no listen IP) ---------------- */

    [Theory]
    [InlineData("localhost", true)]
    [InlineData("LOCALHOST", true)]
    [InlineData("127.0.0.1", true)]
    [InlineData("127.0.0.53", true)]
    [InlineData("::1", true)]
    [InlineData("::ffff:127.0.0.1", true)]
    [InlineData("", true)]                    // absent Host (HTTP/1.0, some health probes)
    [InlineData("evil.com", false)]           // the attack: a domain re-resolved to 127.0.0.1
    [InlineData("attacker.internal", false)]
    [InlineData("192.168.1.205", false)]      // Lite binds no LAN address, so a LAN IP is never an allowed Host
    public void IsAllowedHost_LoopbackOnly_AllowsOnlyLoopbackHosts(string host, bool expected)
        => Assert.Equal(expected, HostHeaderGuard.IsAllowedHost(host, networkListenIp: null));

    /// <summary>The network-mode rows exist so Lite's copy of the guard cannot silently diverge from Darling's
    /// if Lite ever grows a LAN bind — the shared function is the same one both apps call.</summary>
    [Theory]
    [InlineData("192.168.1.205", true)]
    [InlineData("192.168.1.206", false)]
    [InlineData("localhost", true)]
    [InlineData("evil.com", false)]
    public void IsAllowedHost_WithAListenIp_AlsoAllowsThatAddress(string host, bool expected)
        => Assert.Equal(expected, HostHeaderGuard.IsAllowedHost(host, IPAddress.Parse("192.168.1.205")));

    /* ---------------- the wiring: the guard is installed, first, ahead of MapMcp ---------------- */

    private static string ReadMcpHostSource()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "McpHostService.cs");
        Assert.True(File.Exists(path), "McpHostService.cs was not copied beside the test binary — check the csproj None/Link item.");

        var source = File.ReadAllText(path);
        /* Guard the guard: pin the anchors the assertions key off, so a restructured host fails loudly
           instead of passing vacuously on a parse that matched nothing. */
        Assert.Contains("_app = builder.Build();", source, StringComparison.Ordinal);
        Assert.Contains("_app.MapMcp();", source, StringComparison.Ordinal);
        return source;
    }

    [Fact]
    public void McpHost_InstallsTheHostHeaderGuard_AsItsFirstMiddleware_BeforeMapMcp()
    {
        var source = ReadMcpHostSource();
        var afterBuild = source[(source.IndexOf("_app = builder.Build();", StringComparison.Ordinal) + 1)..];

        var firstUse = afterBuild.IndexOf("_app.Use(", StringComparison.Ordinal);
        Assert.True(firstUse >= 0, "no middleware is registered after builder.Build() — the Host-header guard is missing (#1648)");

        var mapMcp = afterBuild.IndexOf("_app.MapMcp();", StringComparison.Ordinal);
        Assert.True(firstUse < mapMcp, "the Host-header guard must be registered before MapMcp maps the MCP endpoints");

        /* The first Use's body only — a second Use cannot satisfy this by accident. */
        var body = afterBuild[firstUse..];
        var nextUse = body.IndexOf("_app.Use(", 1, StringComparison.Ordinal);
        var first = nextUse > 0 ? body[..nextUse] : body[..(mapMcp - firstUse)];

        Assert.Contains("HostHeaderGuard.IsAllowedHost(context.Request.Host.Host", first, StringComparison.Ordinal);
        Assert.Contains("Status400BadRequest", first, StringComparison.Ordinal);
    }
}
