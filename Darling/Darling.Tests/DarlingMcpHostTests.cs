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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
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

    /* ---- TLS on the network listener (#5288): the host's own glue, pinned from the shipped source ----
       #1648's lesson: a pure-function test passes happily on a build where the decision never reaches the
       server. The listener a certificate produces is proven live by DarlingMcpTlsLiveTests; what only the host can
       do (adopt, bail, dispose, gate on network mode) cannot be driven from a test, because a network-mode start
       needs a managed store, so it is pinned where it lives. */

    private static string HostSource()
        => RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs");

    private static int CountOf(string source, string needle)
    {
        var count = 0;
        for (var at = source.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = source.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>The text of one member, braces balanced from its header (every brace in these bodies is paired).</summary>
    private static string BodyOf(string source, string header)
    {
        var start = source.IndexOf(header, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{header}' is gone - this pin needs rewriting");
        var depth = 0;
        for (var i = source.IndexOf('{', start); i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}' && --depth == 0)
            {
                return source[start..(i + 1)];
            }
        }

        Assert.Fail($"unbalanced braces after '{header}'");
        return "";
    }

    [Fact]
    public void McpHost_OneUseHttps_LoopbackListenersPlain()
    {
        var source = HostSource();

        /* Exactly one UseHttps call in the file: a second would mean a loopback listener acquired a certificate. */
        Assert.Equal(1, CountOf(source, "UseHttps("));

        /* ...and it sits on the NETWORK listener's own callback, handing Kestrel the leaf and the intermediates
           through the body both hosts share, ahead of both loopback listeners and the loopback-only server. */
        var network = source.IndexOf("options.Listen(primaryBind, effectivePort, listen =>", StringComparison.Ordinal);
        var https = source.IndexOf("listen.UseHttps(https => DarlingListenerTls.ConfigureHttps(https, certificate.Value));", StringComparison.Ordinal);
        var v4 = source.IndexOf("options.Listen(IPAddress.Loopback, effectivePort);", StringComparison.Ordinal);
        var v6 = source.IndexOf("options.Listen(IPAddress.IPv6Loopback, effectivePort);", StringComparison.Ordinal);
        var loopbackOnly = source.IndexOf("options.ListenLocalhost(effectivePort);", StringComparison.Ordinal);
        Assert.True(
            network > 0 && https > network && v4 > https && v6 > v4 && loopbackOnly > v6,
            "the network listener must carry the one UseHttps call, ahead of the plain loopback listeners");

        /* The host hands the listener exactly the certificate Resolve returned, and routes through ConfigureListeners. */
        Assert.Contains(
            "ConfigureListeners(options, networkMode, primaryBind, effectivePort, serverCertificate)", source, StringComparison.Ordinal);
        Assert.Contains("serverCertificate = tlsOutcome.Certificate;", source, StringComparison.Ordinal);
    }

    [Fact]
    public void McpHost_TlsRefusal_SetsNetworkModeFalse()
    {
        var source = HostSource();
        var tryStart = BodyOf(source, "private async Task<bool> TryStartServerAsync(");

        /* One Resolve call, with the MCP labels, the MCP state, the block, and the normalized name. */
        Assert.Equal(1, CountOf(source, "DarlingListenerTls.Resolve("));
        var call = tryStart.IndexOf("DarlingListenerTls.Resolve(", StringComparison.Ordinal);
        var callText = tryStart[call..tryStart.IndexOf(';', call)];
        Assert.Contains("ListenerTlsLabels.Mcp", callText, StringComparison.Ordinal);
        Assert.Contains("_mcpTlsCertState", callText, StringComparison.Ordinal);
        Assert.Contains("network.Tls", callText, StringComparison.Ordinal);
        Assert.Contains("NormalizedHostName(network.HostName)", callText, StringComparison.Ordinal);

        /* NETWORK mode only: the block that holds the call opens with if (networkMode) and has not closed by it. */
        var gate = tryStart.LastIndexOf("if (networkMode)", call, StringComparison.Ordinal);
        Assert.True(gate > 0, "the TLS call must sit under if (networkMode)");
        Assert.DoesNotContain("\n            }", tryStart[gate..call], StringComparison.Ordinal);

        /* A refusal decides the mode, and the F1 backstop refuses TLS-asked-for-but-no-certificate with a Critical. */
        Assert.Contains("networkMode = tlsOutcome.Expose;", tryStart, StringComparison.Ordinal);
        Assert.Matches(
            @"(?s)if \(tlsOutcome\.ExposesWithoutItsCertificate\)\s*\{\s*_logger\.LogCritical\(.*?tlsOutcome\.Shape\);\s*networkMode = false;\s*\}",
            tryStart);

        /* Ordered so the decision reaches everything that reads the final mode: after the token (a token that
           cannot be read already made the mode loopback-only, so no certificate is loaded for it), before the real
           bind address, before the Host-name decision, and before the pipeline. */
        var token = tryStart.IndexOf("config.Mcp.Network.ResolveToken(", StringComparison.Ordinal);
        var primaryBind = tryStart.IndexOf("var primaryBind = networkMode", StringComparison.Ordinal);
        var hostName = tryStart.IndexOf("var allowedHostName = ResolveAllowedHostName(", StringComparison.Ordinal);
        var pipeline = tryStart.IndexOf("ConfigurePipeline(_app,", StringComparison.Ordinal);
        Assert.True(
            token > 0 && call > token && primaryBind > call && hostName > primaryBind && pipeline > hostName,
            "TLS must be decided after the token and before primaryBind, the Host-name decision and the pipeline");
    }

    [Fact]
    public void McpHost_EveryBailPath_DisposesTheCertificate()
    {
        var source = HostSource();
        var tryStart = BodyOf(source, "private async Task<bool> TryStartServerAsync(");

        /* The host adopts what comes back at once, before the first bail path after the TLS call (the web pin's twin). */
        var call = tryStart.IndexOf("DarlingListenerTls.Resolve(", StringComparison.Ordinal);
        var adopted = tryStart.IndexOf("_serverCertificate = tlsOutcome.Certificate;", StringComparison.Ordinal);
        var firstBail = tryStart.IndexOf("PortUtilityService.IsTcpPortListeningAsync(", StringComparison.Ordinal);
        Assert.True(call > 0 && adopted > call && firstBail > adopted, "the host no longer adopts the certificate before its bail paths");
        Assert.Contains("tlsOutcome.ExposesWithoutItsCertificate", tryStart, StringComparison.Ordinal);

        /* Every way out after the adoption releases it: the port bail, the non-Windows bail, both store-credential
           bails, the shutdown-mid-start catch, and the generic catch. Each return false is preceded by the cleanup. */
        var bails = System.Text.RegularExpressions.Regex.Matches(tryStart, "return false;")
            .Where(match => match.Index > adopted)
            .ToList();
        Assert.True(bails.Count >= 6, $"expected at least six bail paths after the adoption, found {bails.Count}");
        foreach (var bail in bails)
        {
            Assert.EndsWith(
                "await DisposeFailedStartAsync();", tryStart[..bail.Index].TrimEnd(), StringComparison.Ordinal);
        }

        /* The cleanup itself, and the one release both exits share: dispose, forget, withdraw the published facts. */
        var release = BodyOf(source, "private void ReleaseServerCertificate()");
        Assert.Contains("_serverCertificate?.Dispose();", release, StringComparison.Ordinal);
        Assert.Contains("_serverCertificate = null;", release, StringComparison.Ordinal);
        Assert.Contains("_mcpTlsCertState.Clear();", release, StringComparison.Ordinal);
        Assert.Contains("ReleaseServerCertificate();", BodyOf(source, "private async Task DisposeFailedStartAsync()"), StringComparison.Ordinal);

        /* StopServerAsync releases BEFORE its early _app-is-null return, so no path skips it, and again after the
           app has stopped, so a handshake never reaches a key that was already removed. */
        var stop = BodyOf(source, "private async Task StopServerAsync(CancellationToken cancellationToken)");
        var nullGuard = stop.IndexOf("if (_app is null)", StringComparison.Ordinal);
        var earlyRelease = stop.IndexOf("ReleaseServerCertificate();", StringComparison.Ordinal);
        var earlyReturn = stop.IndexOf("return;", StringComparison.Ordinal);
        Assert.True(nullGuard >= 0 && earlyRelease > nullGuard && earlyRelease < earlyReturn, "the early return must release the certificate first");
        Assert.True(
            stop.LastIndexOf("ReleaseServerCertificate();", StringComparison.Ordinal) > stop.IndexOf("await _app.StopAsync(", StringComparison.Ordinal),
            "the certificate must also be released after the app has stopped");
    }

    /* ---- the constructor and the DI seam ---- */

    [Fact]
    public void Constructor_McpTlsCertState_IsTheLastParameter_AndOptional()
    {
        var parameters = typeof(Host).GetConstructors().Single().GetParameters();

        Assert.Equal("mcpTlsCertState", parameters[^1].Name);
        Assert.Equal(typeof(McpTlsCertificateState), parameters[^1].ParameterType);
        Assert.True(parameters[^1].IsOptional);
    }

    [Fact]
    public void Di_HandsTheHostTheRegisteredMcpTlsCertificateState_TheOneTheWorkerReads()
    {
        /* The self-alert worker takes the same registered McpTlsCertificateState singleton, so what the host publishes
           and clears is what the alert sweep reads. A host that kept its private default would publish into a state
           nobody reads and the alert would never fire. */
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<McpRuntimeState>();
        services.AddSingleton<MonitoredServerRegistryState>();
        var registered = new McpTlsCertificateState();
        services.AddSingleton(registered);
        services.AddHostedService<Host>();

        using var provider = services.BuildServiceProvider();
        var host = provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>().OfType<Host>().Single();

        var field = typeof(Host).GetField("_mcpTlsCertState", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(field);
        Assert.Same(registered, field!.GetValue(host));

        var program = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Program.cs");
        Assert.Contains("builder.Services.AddHostedService<DarlingMcpHostService>();", program, StringComparison.Ordinal);
    }

    /* ---- the start line (#5288 review F11) ---- */

    private const string StartOrigin = "the control plane (config.config_service)";

    /* The line this host logged before TLS existed, as its template, so "byte-identical" is measured against the old
       text rather than restated by hand. */
    private const string OldStartTemplate =
        "Starting MCP server on http://{Listen}:{Port} (LAN-exposed to {Cidr} behind a bearer token + in-app CIDR; loopback also bound) \u2014 "
        + "enabled/port from {Origin}; listen/allowFrom/token from darling.json mcp.network (file-only, restart-only)";

    [Theory]
    [InlineData("192.168.1.205", "192.168.1.0/24")]
    [InlineData("0.0.0.0", "192.168.1.0/24")]
    [InlineData("::", "2001:db8::/32")]
    [InlineData("127.0.0.1", "127.0.0.0/8")]
    public void DescribeNetworkStart_NoTls_IsByteIdenticalToTheOldLine(string listen, string allowFrom)
    {
        var cidr = CidrAllowList.Parse(allowFrom);
        var expected = OldStartTemplate
            .Replace("{Listen}", IPAddress.Parse(listen).ToString(), StringComparison.Ordinal)
            .Replace("{Port}", "5152", StringComparison.Ordinal)
            .Replace("{Cidr}", cidr.ToString(), StringComparison.Ordinal)
            .Replace("{Origin}", StartOrigin, StringComparison.Ordinal);

        Assert.Equal(expected, Host.DescribeNetworkStart(false, IPAddress.Parse(listen), 5152, cidr, StartOrigin));
    }

    [Fact]
    public void DescribeNetworkStart_Tls_SpecificListen_SaysHttps_AndLoopbackIsPlainHttp()
    {
        var line = Host.DescribeNetworkStart(
            true, IPAddress.Parse("192.168.1.205"), 5152, CidrAllowList.Parse("192.168.1.0/24"), StartOrigin);

        Assert.Equal(
            "Starting MCP server on https://192.168.1.205:5152 (LAN-exposed to 192.168.1.0/24 behind a bearer token + in-app CIDR; "
            + "loopback also bound over plain HTTP) \u2014 enabled/port from the control plane (config.config_service); "
            + "listen/allowFrom/token/tls from darling.json mcp.network (file-only, restart-only)",
            line);
    }

    [Theory]
    [InlineData("0.0.0.0", "192.168.1.0/24")]
    [InlineData("::", "2001:db8::/32")]
    [InlineData("127.0.0.1", "127.0.0.0/8")]
    public void DescribeNetworkStart_Tls_WildcardOrLoopbackListen_SaysTheOneListenerServesHttpsToLoopbackToo(string listen, string allowFrom)
    {
        /* No second set of loopback listeners exists here (ShouldAddLoopbackListeners declines), so the one listener
           is HTTPS for loopback as well, and saying "plain HTTP" would send a local client to the wrong scheme. */
        var line = Host.DescribeNetworkStart(
            true, IPAddress.Parse(listen), 5152, CidrAllowList.Parse(allowFrom), StartOrigin);

        Assert.StartsWith($"Starting MCP server on https://{IPAddress.Parse(listen)}:5152 (LAN-exposed to ", line, StringComparison.Ordinal);
        Assert.Contains("; the one listener serves HTTPS to loopback too) \u2014 ", line, StringComparison.Ordinal);
        Assert.DoesNotContain("plain HTTP", line, StringComparison.Ordinal);
        Assert.Contains("listen/allowFrom/token/tls from darling.json", line, StringComparison.Ordinal);
    }

    [Fact]
    public void McpHost_LogsTheStartLineThroughDescribeNetworkStart_WithTheAdoptedCertificate()
    {
        Assert.Contains(
            "DescribeNetworkStart(serverCertificate is not null, primaryBind, effectivePort, allowedCidr, origin)",
            HostSource(),
            StringComparison.Ordinal);
    }
}
