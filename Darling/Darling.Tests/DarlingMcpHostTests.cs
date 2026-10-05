/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Net;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;
using Host = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpHostService;

namespace Darling.Tests;

/// <summary>
/// The MCP host's PURE network-endpoint decisions (darling-network-endpoints, Phase 2): the effective-bind
/// resolver <see cref="Host.ResolveMcpBind"/> ((Mode, Reason) matrix, no logger), the loopback-listener
/// collision guard, the in-app CIDR check (loopback-exempt), and the constant-time bearer-token check. The
/// config parse, token resolution, and Validate() behavior are pinned by <see cref="DarlingConfigTests"/>
/// (Phase 1); the live round-trip (a token+CIDR client reaches the tools, off-CIDR/bad-token is refused) is
/// validated on DARLING01.
/// </summary>
public sealed class DarlingMcpHostTests
{
    private static McpConfig Mcp(
        string? listen = null, string? allowFrom = null, string? token = null, string? encryptedToken = null)
        => new()
        {
            Enabled = true,
            Network = (listen is null && allowFrom is null && token is null && encryptedToken is null)
                ? null
                : new McpNetworkConfig
                {
                    Listen = listen,
                    AllowFrom = allowFrom,
                    Token = token,
                    EncryptedToken = encryptedToken,
                },
        };

    /* ---- ResolveMcpBind: NetworkAndLoopback only when exposed + managed + token + valid allowFrom ---- */

    [Theory]
    [InlineData("192.168.1.205", "192.168.1.0/24")]  // a specific LAN IPv4
    [InlineData("0.0.0.0", "192.168.1.0/24")]         // IPv4 wildcard (single bind, no loopback collision — see ShouldAddLoopbackListeners)
    [InlineData("::", "2001:db8::/32")]               // IPv6 wildcard
    [InlineData("2001:db8::5", "2001:db8::/32")]      // a specific IPv6
    public void ResolveMcpBind_Exposed_Managed_TokenAndAllowFrom_IsNetworkAndLoopback(string listen, string allowFrom)
    {
        var decision = Host.ResolveMcpBind(Mcp(listen, allowFrom, token: "s3cr3t"), managed: true);

        Assert.Equal(Host.McpBindMode.NetworkAndLoopback, decision.Mode);
        Assert.Equal(Host.McpBindReason.NetworkExposed, decision.Reason);
    }

    [Fact]
    public void ResolveMcpBind_Exposed_EncryptedTokenCountsAsPresent()
    {
        /* Presence check only — no DPAPI decryption in the pure resolver; encryptedToken alone satisfies it. */
        var decision = Host.ResolveMcpBind(
            Mcp("192.168.1.205", "192.168.1.0/24", encryptedToken: "some-dpapi-blob"), managed: true);

        Assert.Equal(Host.McpBindMode.NetworkAndLoopback, decision.Mode);
        Assert.Equal(Host.McpBindReason.NetworkExposed, decision.Reason);
    }

    [Fact]
    public void ResolveMcpBind_Exposed_Managed_NoToken_IsLoopbackOnly_TokenMissing()
    {
        var decision = Host.ResolveMcpBind(Mcp("192.168.1.205", "192.168.1.0/24" /* no token */), managed: true);

        Assert.Equal(Host.McpBindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(Host.McpBindReason.TokenMissing, decision.Reason);
    }

    [Theory]
    [InlineData(null)]              // allowFrom missing
    [InlineData("")]
    [InlineData("not-a-cidr")]      // not a CIDR at all
    [InlineData("192.168.1.0/33")]  // impossible IPv4 prefix length
    [InlineData("192.168.1.0")]     // an address with no prefix
    public void ResolveMcpBind_Exposed_Managed_Token_BadAllowFrom_IsLoopbackOnly_AllowFromInvalid(string? allowFrom)
    {
        var decision = Host.ResolveMcpBind(Mcp("192.168.1.205", allowFrom, token: "s3cr3t"), managed: true);

        Assert.Equal(Host.McpBindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(Host.McpBindReason.AllowFromInvalid, decision.Reason);
    }

    [Theory]
    [InlineData("localhost")]  // a name, not an IP (a plausible "I meant loopback" typo)
    [InlineData("myhost")]     // a hostname
    [InlineData("db.lan")]
    [InlineData("*")]          // the postgres listen wildcard, not a Kestrel-bindable IP
    public void ResolveMcpBind_Exposed_Managed_NonIpListen_IsLoopbackOnly_ListenInvalid(string listen)
    {
        /* A non-IP "exposed" listen must DEGRADE, not throw and take the whole host down (D-validate) — the
           host's IPAddress.Parse would otherwise FormatException into the generic catch and exit MCP entirely. */
        var decision = Host.ResolveMcpBind(Mcp(listen, "192.168.1.0/24", token: "s3cr3t"), managed: true);

        Assert.Equal(Host.McpBindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(Host.McpBindReason.ListenInvalid, decision.Reason);
    }

    [Theory]
    [InlineData("192.168.1.205", "2001:db8::/32")]  // IPv4 listen, IPv6 CIDR
    [InlineData("2001:db8::5", "192.168.1.0/24")]   // IPv6 listen, IPv4 CIDR
    public void ResolveMcpBind_Exposed_Managed_AllowFromFamilyMismatch_IsLoopbackOnly_AllowFromInvalid(string listen, string allowFrom)
    {
        /* A family mismatch binds one family while the CIDR check rejects the other -> every client 403s
           (fail-closed but silently non-functional); degrade with a reason instead (mirrors the store's D4). */
        var decision = Host.ResolveMcpBind(Mcp(listen, allowFrom, token: "s3cr3t"), managed: true);

        Assert.Equal(Host.McpBindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(Host.McpBindReason.AllowFromInvalid, decision.Reason);
    }

    [Fact]
    public void ResolveMcpBind_Exposed_Byo_IsLoopbackOnly_ManagedModeRequired()
    {
        /* A fully-formed exposure but managed=false: the network path never runs in BYO (D-BYO). */
        var decision = Host.ResolveMcpBind(Mcp("192.168.1.205", "192.168.1.0/24", token: "s3cr3t"), managed: false);

        Assert.Equal(Host.McpBindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(Host.McpBindReason.ManagedModeRequired, decision.Reason);
    }

    [Fact]
    public void ResolveMcpBind_Exposed_Byo_MissingToken_StillManagedModeRequired_NotTokenMissing()
    {
        /* BYO dominates a missing token/allowFrom so the operator sees the actionable "managed only" notice. */
        var decision = Host.ResolveMcpBind(Mcp("192.168.1.205" /* no token, no allowFrom */), managed: false);

        Assert.Equal(Host.McpBindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(Host.McpBindReason.ManagedModeRequired, decision.Reason);
    }

    [Fact]
    public void ResolveMcpBind_Byo_ConfiguredButNotExposed_IsManagedModeRequired()
    {
        /* Any network.* set in BYO is ignored -> the warning fires even when the listen is not exposed. */
        var decision = Host.ResolveMcpBind(Mcp(allowFrom: "192.168.1.0/24"), managed: false);

        Assert.Equal(Host.McpBindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(Host.McpBindReason.ManagedModeRequired, decision.Reason);
    }

    [Fact]
    public void ResolveMcpBind_LoopbackListen_IsLoopbackByDefault_NoCollision()
    {
        /* 127.0.0.1 must resolve to the loopback single-bind path, never a network bind. */
        var decision = Host.ResolveMcpBind(
            Mcp("127.0.0.1", "192.168.1.0/24", token: "s3cr3t"), managed: true);

        Assert.Equal(Host.McpBindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(Host.McpBindReason.LoopbackByDefault, decision.Reason);
    }

    [Fact]
    public void ResolveMcpBind_NoNetworkBlock_IsLoopbackByDefault()
    {
        var decision = Host.ResolveMcpBind(Mcp(), managed: true);

        Assert.Equal(Host.McpBindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(Host.McpBindReason.LoopbackByDefault, decision.Reason);
    }

    [Fact]
    public void ResolveMcpBind_Managed_LoopbackListenConfigured_NoByoWarning()
    {
        /* Managed + a non-exposed listen = the secure default, silent (no BYO warning — that is BYO-only). */
        var decision = Host.ResolveMcpBind(Mcp("127.0.0.1"), managed: true);

        Assert.Equal(Host.McpBindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(Host.McpBindReason.LoopbackByDefault, decision.Reason);
    }

    /* ---- ShouldAddLoopbackListeners: skip the loopback binds for loopback/wildcard listens (collision) ---- */

    [Theory]
    [InlineData("192.168.1.205", true)]  // a specific LAN IP -> add loopback so local "localhost" clients still reach it
    [InlineData("2001:db8::5", true)]
    [InlineData("127.0.0.1", false)]     // already loopback
    [InlineData("127.0.0.5", false)]     // anywhere in 127.0.0.0/8
    [InlineData("::1", false)]           // IPv6 loopback
    [InlineData("0.0.0.0", false)]       // IPv4 wildcard covers 127.0.0.1 -> explicit loopback would collide
    [InlineData("::", false)]            // IPv6 wildcard covers ::1
    public void ShouldAddLoopbackListeners_SkipsLoopbackAndWildcards(string listen, bool expected)
        => Assert.Equal(expected, Host.ShouldAddLoopbackListeners(IPAddress.Parse(listen)));

    /* ---- IsRemoteAddressAllowed: inside the CIDR OR loopback (always) ---- */

    [Theory]
    [InlineData("192.168.1.50", true)]
    [InlineData("192.168.1.255", true)]
    [InlineData("192.168.2.1", false)]
    [InlineData("10.0.0.5", false)]
    [InlineData("127.0.0.1", true)]                // loopback always allowed (Round-4 #2)
    [InlineData("127.0.0.9", true)]                // 127.0.0.0/8
    [InlineData("::1", true)]                      // IPv6 loopback always allowed
    [InlineData("::ffff:127.0.0.1", true)]         // IPv4-mapped loopback
    [InlineData("::ffff:192.168.1.50", true)]      // IPv4-mapped, inside the CIDR
    [InlineData("::ffff:10.0.0.5", false)]         // IPv4-mapped, outside the CIDR
    public void IsRemoteAddressAllowed_Ipv4Cidr(string remote, bool expected)
        => Assert.Equal(expected, Host.IsRemoteAddressAllowed(IPAddress.Parse(remote), IPNetwork.Parse("192.168.1.0/24")));

    [Theory]
    [InlineData("2001:db8::1", true)]
    [InlineData("2001:db8:0:0:0:0:0:abcd", true)]
    [InlineData("2001:dead::1", false)]
    [InlineData("::1", true)]                       // loopback exempt even under an IPv6 CIDR
    public void IsRemoteAddressAllowed_Ipv6Cidr(string remote, bool expected)
        => Assert.Equal(expected, Host.IsRemoteAddressAllowed(IPAddress.Parse(remote), IPNetwork.Parse("2001:db8::/32")));

    [Fact]
    public void IsRemoteAddressAllowed_NullRemote_FailsClosed()
        => Assert.False(Host.IsRemoteAddressAllowed(null, IPNetwork.Parse("192.168.1.0/24")));

    /* ---- IsBearerTokenAuthorized: constant-time; 401 on empty/missing/mismatch; no loopback notion ---- */

    [Theory]
    [InlineData("Bearer s3cr3t-token", true)]      // exact match
    [InlineData("bearer s3cr3t-token", true)]      // scheme is case-insensitive
    [InlineData("BEARER s3cr3t-token", true)]
    [InlineData("Bearer wrong-token", false)]      // mismatch
    [InlineData("Bearer ", false)]                 // empty token part
    [InlineData("", false)]                        // no header
    [InlineData(null, false)]                      // missing header
    [InlineData("s3cr3t-token", false)]            // no Bearer scheme
    [InlineData("Basic s3cr3t-token", false)]      // wrong scheme
    public void IsBearerTokenAuthorized_MatchesOnlyExactBearerToken(string? header, bool expected)
        => Assert.Equal(expected, Host.IsBearerTokenAuthorized(header, "s3cr3t-token"));

    [Fact]
    public void IsBearerTokenAuthorized_EmptyExpected_NeverAuthorizes()
    {
        /* A blank expected token must never authorize, even against a "Bearer " with a token. */
        Assert.False(Host.IsBearerTokenAuthorized("Bearer anything", ""));
        Assert.False(Host.IsBearerTokenAuthorized("Bearer ", ""));
    }

    [Theory]
    [InlineData("Bearer abc", "abc")]
    [InlineData("bearer abc", "abc")]
    [InlineData("Bearer   abc  ", "abc")]  // surrounding whitespace trimmed
    [InlineData("abc", null)]              // no scheme
    [InlineData("Bearer ", null)]          // blank token
    [InlineData("Bearer", null)]           // scheme without the required trailing space
    [InlineData("", null)]
    [InlineData(null, null)]
    public void ExtractBearerToken_ParsesTheSchemeStrictly(string? header, string? expected)
        => Assert.Equal(expected, Host.ExtractBearerToken(header));

    /* ---- reason -> severity mapping (MapBindReasonSeverity, which LogBindReason drives its emit off) ---- */

    /* [Fact], not [Theory] with InlineData: the internal McpBindReason enum cannot appear in a public test
       method's SIGNATURE (CS0051), so the cases live in the body (InternalsVisibleTo makes them reachable). */

    [Fact]
    public void MapBindReasonSeverity_DegradesAreCritical_ByoIsWarning()
    {
        Assert.Equal(LogLevel.Critical, Host.MapBindReasonSeverity(Host.McpBindReason.ListenInvalid));
        Assert.Equal(LogLevel.Critical, Host.MapBindReasonSeverity(Host.McpBindReason.TokenMissing));
        Assert.Equal(LogLevel.Critical, Host.MapBindReasonSeverity(Host.McpBindReason.AllowFromInvalid));
        Assert.Equal(LogLevel.Warning, Host.MapBindReasonSeverity(Host.McpBindReason.ManagedModeRequired));
    }

    [Fact]
    public void MapBindReasonSeverity_NonDegradeReasons_AreSilent()
    {
        Assert.Null(Host.MapBindReasonSeverity(Host.McpBindReason.NetworkExposed));     // announced at start with the real bind
        Assert.Null(Host.MapBindReasonSeverity(Host.McpBindReason.LoopbackByDefault));  // the silent, byte-for-byte-today path
    }

    /* ---- ResolveAllowedHostName (#5288, review F2): the one extra Host name, admitted in NETWORK mode only ---- */

    [Theory]
    [InlineData("mcp.corp.example", "mcp.corp.example")]
    [InlineData("  MCP.Corp.Example. ", "MCP.Corp.Example")]        // trimmed, one trailing dot stripped, case kept
    [InlineData("b\u00FCcher.example", "xn--bcher-kva.example")]    // a Unicode name is admitted as the punycode a client sends
    public void ResolveAllowedHostName_NetworkMode_IsTheNormalizedName_AndSilent(string configured, string expected)
    {
        var logger = new CapturingTestLogger();

        Assert.Equal(expected, Host.ResolveAllowedHostName(configured, networkMode: true, logger));
        Assert.Empty(logger.Lines);
    }

    [Theory]
    [InlineData("mcp.corp.example")]
    [InlineData("b\u00FCcher.example")]
    public void ResolveAllowedHostName_LoopbackOrDegradedMode_IsNull_EvenForAValidName(string configured)
    {
        /* networkMode is false in loopback-only mode AND after every degrade (an unreadable token, a refused
           certificate), so one answer covers all of them: that surface is tokenless, and the name exists for the
           network listener's clients. A valid name that is simply not used here is not worth a log line. */
        var logger = new CapturingTestLogger();

        Assert.Null(Host.ResolveAllowedHostName(configured, networkMode: false, logger));
        Assert.Empty(logger.Lines);
    }

    [Theory]
    [InlineData(true, null)]
    [InlineData(true, "")]
    [InlineData(true, "   ")]
    [InlineData(false, null)]
    [InlineData(false, "\t\r\n")]
    public void ResolveAllowedHostName_NotSet_IsNullAndSilent_InEveryMode(bool networkMode, string? configured)
    {
        var logger = new CapturingTestLogger();

        Assert.Null(Host.ResolveAllowedHostName(configured, networkMode, logger));
        Assert.Empty(logger.Lines);
    }

    [Theory]
    [InlineData(true, "https://mcp.corp.example")]
    [InlineData(false, "https://mcp.corp.example")]
    [InlineData(true, "mcp.corp.example:5152")]
    [InlineData(true, "*.corp.example")]
    [InlineData(true, "10.1.2.3")]
    [InlineData(false, "10.1.2.3")]
    [InlineData(true, "mcp.corp.example..")]
    [InlineData(true, "b\u00FCcher..example")]                      // an invalid internationalized name
    public void ResolveAllowedHostName_SetButRefused_LogsOneWarning_AdmitsNothing_InEveryMode(bool networkMode, string configured)
    {
        /* The warning is about the CONFIG VALUE, so it is written in loopback mode too: the operator who typed a URL
           where a name goes finds out at the first start, not only once the network listener is working. */
        var logger = new CapturingTestLogger();

        Assert.Null(Host.ResolveAllowedHostName(configured, networkMode, logger));

        var line = Assert.Single(logger.Lines);
        Assert.StartsWith("Warning: ", line);
        Assert.Contains("mcp.network.hostName", line);
        Assert.Contains($"'{configured}'", line);
    }

    [Fact]
    public void ResolveAllowedHostName_SetButRefused_EchoesNoLineBreak()
    {
        /* The value comes from darling.json, but a log file is split on newlines: a value carrying one must not be
           able to forge a second entry. */
        var logger = new CapturingTestLogger();

        Assert.Null(Host.ResolveAllowedHostName("evil\r\nCritical: forged", networkMode: true, logger));

        var line = Assert.Single(logger.Lines);
        Assert.DoesNotContain("\r", line);
        Assert.DoesNotContain("\n", line);
    }
}
