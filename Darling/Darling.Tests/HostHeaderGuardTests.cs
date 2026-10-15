/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1648: the DNS-rebinding guard on Darling's two Kestrel hosts. Two halves, because the defect had two halves.
///
/// <para><b>The decision</b> — <see cref="HostHeaderGuard.IsAllowedHost"/>, now shared with Lite. The web host's
/// own matrix (<see cref="DarlingWebAuthTests"/>) still calls <c>DarlingWebHostService.IsAllowedHost</c> and is
/// deliberately left untouched: it passing unchanged IS the proof that lifting the body into
/// <see cref="HostHeaderGuard"/> (via <c>DarlingHostBinding</c>) kept that host byte-for-byte. What is added
/// here is the forwarders-agree check, so a future edit cannot make one host's guard diverge from the other's.</para>
///
/// <para><b>The wiring</b> — the actual bug. The MCP host's decision function was never the problem; the
/// middleware simply was not installed, in either mode, so a pure-function test would have passed on the
/// vulnerable build. These tests parse the REAL host sources (copied beside the test binary by the csproj) and
/// assert the guard is the FIRST <c>_app.Use</c> after <c>builder.Build()</c> and sits AHEAD of the
/// <c>if (networkMode)</c> gates — i.e. it runs in loopback mode too, which is the mode that was exposed.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class HostHeaderGuardTests
{
    /* ---------------- the pure decision, both modes ---------------- */

    /// <summary>Network mode: the configured listen IP is a legitimate Host, loopback still is, nothing else.</summary>
    [Theory]
    [InlineData("localhost", true)]
    [InlineData("LOCALHOST", true)]
    [InlineData("127.0.0.1", true)]
    [InlineData("127.0.0.53", true)]          // the whole 127/8 loopback range
    [InlineData("::1", true)]
    [InlineData("::ffff:127.0.0.1", true)]    // IPv4-mapped loopback
    [InlineData("192.168.1.205", true)]       // the configured listen IP: the real operator, direct
    [InlineData("", true)]                    // absent Host (HTTP/1.0, some health probes)
    [InlineData("evil.com", false)]           // the rebound foreign hostname
    [InlineData("192.168.1.206", false)]      // a LAN IP we do NOT bind
    [InlineData("darling.internal", false)]
    public void IsAllowedHost_NetworkMode_AllowsLoopbackAndTheListenIp_RejectsEverythingElse(string host, bool expected)
        => Assert.Equal(expected, HostHeaderGuard.IsAllowedHost(host, IPAddress.Parse("192.168.1.205")));

    /// <summary>
    /// WILDCARD listen (#2569) — the mode the compose distribution ships for BOTH surfaces, and the one that
    /// refused every real LAN request. Comparing a Host against <c>0.0.0.0</c> can only match the literal
    /// string "0.0.0.0", which no client sends, so a browser at the box's actual address got a 400 before any
    /// auth ran. Measured through a real Kestrel pipeline, not just this function.
    ///
    /// <para>The rule is "any IP literal", because a wildcard names no single address to compare against. The
    /// tempting alternative — enumerate this machine's interfaces at bind time — does NOT fix the case that
    /// motivated the bug: inside a container the local interfaces are the container's, while the address the
    /// browser used is the host's published one, which the container cannot see. NAT and port forwarding
    /// break it the same way, silently, as a 400.</para>
    ///
    /// <para>The rebinding defence is unchanged, and the hostname rows below are the proof: a rebind arrives
    /// with a HOSTNAME by construction (the victim loaded http://evil.com, so the browser sends that Host and
    /// treats the reply as same-origin), and a hostname still never passes.</para>
    /// </summary>
    [Theory]
    [InlineData("192.168.1.205", true)]        // the LAN client this bug refused
    [InlineData("10.0.0.7", true)]             // another subnet: with a wildcard we bind that too
    [InlineData("172.18.0.3", true)]           // the container case, where enumeration would have failed
    [InlineData("localhost", true)]
    [InlineData("127.0.0.1", true)]
    [InlineData("::1", true)]
    [InlineData("", true)]
    [InlineData("evil.com", false)]                    // THE attack, still refused
    [InlineData("darling.corp.local", false)]          // a friendly hostname is still a hostname
    [InlineData("192.168.1.205.evil.com", false)]      // an IP-shaped prefix on an attacker domain
    [InlineData("localhost.evil.com", false)]          // and on the loopback name
    public void IsAllowedHost_WildcardListen_AllowsAnyIpLiteral_ButNeverAHostname(string host, bool expected)
        => Assert.Equal(expected, HostHeaderGuard.IsAllowedHost(host, IPAddress.Any));

    /// <summary>The IPv6 wildcard behaves the same as the IPv4 one — it is the same "bind everything" intent,
    /// and a rule that covered only <c>0.0.0.0</c> would leave <c>::</c> refusing every request.</summary>
    [Theory]
    [InlineData("192.168.1.205", true)]
    [InlineData("2001:db8::1", true)]
    [InlineData("evil.com", false)]
    public void IsAllowedHost_IPv6WildcardListen_MatchesTheIPv4Wildcard(string host, bool expected)
        => Assert.Equal(expected, HostHeaderGuard.IsAllowedHost(host, IPAddress.IPv6Any));

    /// <summary>A SPECIFIC listen IP is unchanged by #2569 — the wildcard arm must not leak into it, or the
    /// guard would accept any IP literal everywhere and the "an address we actually bind" rule would be
    /// gone.</summary>
    [Theory]
    [InlineData("192.168.1.205", true)]
    [InlineData("192.168.1.206", false)]
    [InlineData("10.0.0.7", false)]
    public void IsAllowedHost_SpecificListen_StillRejectsOtherIpLiterals(string host, bool expected)
        => Assert.Equal(expected, HostHeaderGuard.IsAllowedHost(host, IPAddress.Parse("192.168.1.205")));

    /// <summary>Loopback-only mode (null listen IP) — the default, and the mode the MCP hosts were exposed in.</summary>
    [Theory]
    [InlineData("localhost", true)]
    [InlineData("127.0.0.1", true)]
    [InlineData("::1", true)]
    [InlineData("::ffff:127.0.0.1", true)]
    [InlineData("", true)]
    [InlineData("evil.com", false)]           // the attack: a domain re-resolved to 127.0.0.1
    [InlineData("attacker.internal", false)]
    [InlineData("192.168.1.205", false)]      // no listen IP configured, so a LAN IP is NOT an allowed Host
    public void IsAllowedHost_LoopbackOnlyMode_AllowsOnlyLoopback(string host, bool expected)
        => Assert.Equal(expected, HostHeaderGuard.IsAllowedHost(host, networkListenIp: null));

    /// <summary>
    /// The web host's forwarder must stay identical to the shared decision — that is the whole point of lifting
    /// it out. Compared across the full matrix rather than spot-checked, in both modes.
    /// </summary>
    [Fact]
    public void DarlingWebHostServiceForwarder_AgreesWithTheSharedGuard_AcrossTheWholeMatrix()
    {
        string[] hosts = ["localhost", "LOCALHOST", "127.0.0.1", "::1", "::ffff:127.0.0.1", "192.168.1.205", "", "evil.com", "10.0.0.5"];
        IPAddress?[] listens = [null, IPAddress.Parse("192.168.1.205")];

        foreach (var listen in listens)
        {
            foreach (var host in hosts)
            {
                Assert.Equal(
                    HostHeaderGuard.IsAllowedHost(host, listen),
                    DarlingWebHostService.IsAllowedHost(host, listen));
            }
        }
    }

    /* ---------------- the wiring: the MCP transport stays stateless while /core is mapped ---------------- */

    private static string ReadHostSource(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", fileName);
        Assert.True(File.Exists(path), $"{fileName} was not copied beside the test binary — check the csproj None/Link item.");

        var source = File.ReadAllText(path);
        /* Guard the guard: if the host were renamed or restructured past recognition, the assertions below
           could pass vacuously on an empty/mismatched parse. Pin the anchors they all key off. Both hosts'
           gates are extracted (#4128 parts a and b) into a ConfigurePipeline(WebApplication app, ...) method
           that calls app.Use, not _app.Use — "app.Use(" covers both shapes. */
        Assert.Contains("_app = builder.Build();", source, StringComparison.Ordinal);
        Assert.Contains("app.Use(", source, StringComparison.Ordinal);
        return source;
    }

    /// <summary>
    /// #3898 D7: /core is a real subset only in Stateless mode, where <c>ConfigureSessionOptions</c> runs on every
    /// request. A stateful session is found by its id alone, not by route, so a session opened on / could call
    /// any tool on /core. While /core is mapped, the transport stays stateless.
    /// </summary>
    [Fact]
    public void McpHost_KeepsTheTransportStatelessWhileCoreIsMapped()
    {
        var source = ReadHostSource("DarlingMcpHostService.cs");
        Assert.True(source.Contains("app.MapMcp(\"/core\");", StringComparison.Ordinal), "/core is no longer mapped — this test needs rewriting");

        var transport = source.IndexOf(".WithHttpTransport(options =>", StringComparison.Ordinal);
        var stateless = source.IndexOf("options.Stateless = true;", StringComparison.Ordinal);
        var configure = source.IndexOf("options.ConfigureSessionOptions =", StringComparison.Ordinal);

        Assert.True(transport >= 0, "WithHttpTransport(options => ...) is gone — this test needs rewriting");
        Assert.True(
            stateless > transport && configure > stateless,
            "options.Stateless = true must be set in the same WithHttpTransport block as ConfigureSessionOptions");
        Assert.False(source.Contains("Stateless = false", StringComparison.Ordinal), "the MCP transport must stay stateless while /core is mapped");
    }
}
