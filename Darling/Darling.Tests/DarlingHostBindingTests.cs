/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The SHARED host-binding helpers (#1562, darling-network-endpoints) both Kestrel hosts now delegate to:
/// the LAN-exposure ladder <see cref="DarlingHostBinding.ResolveBind"/>, the loopback-listener collision
/// guard, the in-app CIDR check (loopback-exempt), the constant-time token compare, and the reason→severity
/// map. These pins hold the ladder ONCE for both surfaces; the MCP host's own
/// <c>DarlingMcpHostTests</c> stay unmodified and prove the refactor kept its adapter byte-for-byte (the
/// nested-enum parity pin at the bottom guards the numeric cast that adapter relies on).
/// </summary>
public sealed class DarlingHostBindingTests
{
    /* ---- ResolveBind: NetworkAndLoopback only when exposed + managed + token + valid, family-matched allowFrom ---- */

    [Theory]
    [InlineData("192.168.1.205", "192.168.1.0/24")]  // a specific LAN IPv4
    [InlineData("0.0.0.0", "192.168.1.0/24")]         // IPv4 wildcard
    [InlineData("::", "2001:db8::/32")]               // IPv6 wildcard
    [InlineData("2001:db8::5", "2001:db8::/32")]      // a specific IPv6
    public void ResolveBind_Exposed_Managed_TokenAndAllowFrom_IsNetworkAndLoopback(string listen, string allowFrom)
    {
        var decision = DarlingHostBinding.ResolveBind(listen, allowFrom, tokenPresent: true, networkConfigured: true, managed: true);

        Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.NetworkExposed, decision.Reason);
    }

    [Fact]
    public void ResolveBind_Exposed_Managed_NoToken_IsLoopbackOnly_TokenMissing()
    {
        var decision = DarlingHostBinding.ResolveBind("192.168.1.205", "192.168.1.0/24", tokenPresent: false, networkConfigured: true, managed: true);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.TokenMissing, decision.Reason);
    }

    [Theory]
    [InlineData(null)]              // allowFrom missing
    [InlineData("")]
    [InlineData("not-a-cidr")]      // not a CIDR at all
    [InlineData("192.168.1.0/33")]  // impossible IPv4 prefix length
    [InlineData("192.168.1.0")]     // an address with no prefix
    public void ResolveBind_Exposed_Managed_Token_BadAllowFrom_IsLoopbackOnly_AllowFromInvalid(string? allowFrom)
    {
        var decision = DarlingHostBinding.ResolveBind("192.168.1.205", allowFrom, tokenPresent: true, networkConfigured: true, managed: true);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.AllowFromInvalid, decision.Reason);
    }

    [Theory]
    [InlineData("localhost")]  // a name, not an IP
    [InlineData("myhost")]
    [InlineData("*")]          // the postgres listen wildcard, not a Kestrel-bindable IP
    public void ResolveBind_Exposed_Managed_NonIpListen_IsLoopbackOnly_ListenInvalid(string listen)
    {
        var decision = DarlingHostBinding.ResolveBind(listen, "192.168.1.0/24", tokenPresent: true, networkConfigured: true, managed: true);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.ListenInvalid, decision.Reason);
    }

    [Theory]
    [InlineData("192.168.1.205", "2001:db8::/32")]  // IPv4 listen, IPv6 CIDR
    [InlineData("2001:db8::5", "192.168.1.0/24")]   // IPv6 listen, IPv4 CIDR
    public void ResolveBind_Exposed_Managed_AllowFromFamilyMismatch_IsLoopbackOnly_AllowFromInvalid(string listen, string allowFrom)
    {
        var decision = DarlingHostBinding.ResolveBind(listen, allowFrom, tokenPresent: true, networkConfigured: true, managed: true);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.AllowFromInvalid, decision.Reason);
    }

    [Fact]
    public void ResolveBind_Exposed_Byo_IsLoopbackOnly_ManagedModeRequired()
    {
        var decision = DarlingHostBinding.ResolveBind("192.168.1.205", "192.168.1.0/24", tokenPresent: true, networkConfigured: true, managed: false);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.ManagedModeRequired, decision.Reason);
    }

    [Fact]
    public void ResolveBind_Byo_ConfiguredButNotExposed_IsManagedModeRequired()
    {
        /* A loopback/partial network block in BYO is ignored -> the warning fires even when not exposed. */
        var decision = DarlingHostBinding.ResolveBind(listen: null, allowFrom: "192.168.1.0/24", tokenPresent: false, networkConfigured: true, managed: false);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.ManagedModeRequired, decision.Reason);
    }

    /* ── the #1804 container gate: a containerized BYO deployment (compose) is exposure-capable — the
       port mapping is the boundary the BYO reverse-proxy rule was standing in for, and a loopback bind
       would be dead through it. Every other precondition applies IDENTICALLY. ── */

    [Fact]
    public void ResolveBind_Exposed_ByoInContainer_IsNetworkExposed()
    {
        var decision = DarlingHostBinding.ResolveBind(
            "0.0.0.0", "10.0.0.0/8", tokenPresent: true, networkConfigured: true, managed: false, inContainer: true);

        Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.NetworkExposed, decision.Reason);
    }

    [Fact]
    public void ResolveBind_ByoInContainer_StillFailsClosed_WithoutTokenOrCidr()
    {
        /* The container gate relaxes ONLY the managed requirement — never the token or the CIDR. */
        var noToken = DarlingHostBinding.ResolveBind(
            "0.0.0.0", "10.0.0.0/8", tokenPresent: false, networkConfigured: true, managed: false, inContainer: true);
        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, noToken.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.TokenMissing, noToken.Reason);

        var badCidr = DarlingHostBinding.ResolveBind(
            "0.0.0.0", "not-a-cidr", tokenPresent: true, networkConfigured: true, managed: false, inContainer: true);
        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, badCidr.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.AllowFromInvalid, badCidr.Reason);
    }

    [Fact]
    public void ResolveBind_ByoInContainer_NotExposed_NoManagedModeNotice()
    {
        /* In a container the network block is HONORED, so a not-exposed block is just the default —
           the "network.* is ignored" notice would be a lie there. */
        var decision = DarlingHostBinding.ResolveBind(
            listen: null, allowFrom: "10.0.0.0/8", tokenPresent: false, networkConfigured: true, managed: false, inContainer: true);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.LoopbackByDefault, decision.Reason);
    }

    [Fact]
    public void ResolveBind_Exposed_ByoOutsideContainer_StaysManagedModeRequired()
    {
        /* The uncontained BYO rule is byte-for-byte unchanged — inContainer defaults to false. */
        var decision = DarlingHostBinding.ResolveBind(
            "192.168.1.205", "192.168.1.0/24", tokenPresent: true, networkConfigured: true, managed: false, inContainer: false);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.ManagedModeRequired, decision.Reason);
    }

    [Theory]
    [InlineData("127.0.0.1")]  // loopback resolves to the single-bind path, never a network bind
    [InlineData("127.0.0.5")]  // anywhere in 127.0.0.0/8
    public void ResolveBind_LoopbackListen_IsLoopbackByDefault(string listen)
    {
        var decision = DarlingHostBinding.ResolveBind(listen, "192.168.1.0/24", tokenPresent: true, networkConfigured: true, managed: true);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.LoopbackByDefault, decision.Reason);
    }

    [Fact]
    public void ResolveBind_NoNetworkBlock_IsLoopbackByDefault()
    {
        var decision = DarlingHostBinding.ResolveBind(listen: null, allowFrom: null, tokenPresent: false, networkConfigured: false, managed: true);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.LoopbackByDefault, decision.Reason);
    }

    /* ---- ShouldAddLoopbackListeners: skip the loopback binds for loopback/wildcard listens (collision) ---- */

    [Theory]
    [InlineData("192.168.1.205", true)]
    [InlineData("2001:db8::5", true)]
    [InlineData("127.0.0.1", false)]
    [InlineData("::1", false)]
    [InlineData("0.0.0.0", false)]
    [InlineData("::", false)]
    public void ShouldAddLoopbackListeners_SkipsLoopbackAndWildcards(string listen, bool expected)
        => Assert.Equal(expected, DarlingHostBinding.ShouldAddLoopbackListeners(IPAddress.Parse(listen)));

    /* ---- IsRemoteAddressAllowed: inside the CIDR OR loopback (always) ---- */

    [Theory]
    [InlineData("192.168.1.50", true)]
    [InlineData("192.168.2.1", false)]
    [InlineData("127.0.0.1", true)]                // loopback always allowed
    [InlineData("::1", true)]                      // IPv6 loopback always allowed
    [InlineData("::ffff:127.0.0.1", true)]         // IPv4-mapped loopback
    [InlineData("::ffff:192.168.1.50", true)]      // IPv4-mapped, inside the CIDR
    [InlineData("::ffff:10.0.0.5", false)]         // IPv4-mapped, outside the CIDR
    public void IsRemoteAddressAllowed_Ipv4Cidr(string remote, bool expected)
        => Assert.Equal(expected, DarlingHostBinding.IsRemoteAddressAllowed(IPAddress.Parse(remote), IPNetwork.Parse("192.168.1.0/24")));

    [Fact]
    public void IsRemoteAddressAllowed_NullRemote_FailsClosed()
        => Assert.False(DarlingHostBinding.IsRemoteAddressAllowed(null, IPNetwork.Parse("192.168.1.0/24")));

    /* ---- FixedTimeTokenEquals: only an exact match; empty/null never authorizes ---- */

    [Theory]
    [InlineData("s3cr3t", "s3cr3t", true)]
    [InlineData("wrong", "s3cr3t", false)]
    [InlineData("", "s3cr3t", false)]
    [InlineData("s3cr3t", "", false)]
    [InlineData(null, "s3cr3t", false)]
    [InlineData("s3cr3t", null, false)]
    [InlineData("s3cr3", "s3cr3t", false)]   // prefix, not equal
    public void FixedTimeTokenEquals_MatchesOnlyExact(string? presented, string? expected, bool result)
        => Assert.Equal(result, DarlingHostBinding.FixedTimeTokenEquals(presented, expected));

    /* ---- MapBindReasonSeverity: degrades are Critical, BYO is Warning, non-degrade is silent ---- */

    [Fact]
    public void MapBindReasonSeverity_DegradesAreCritical_ByoIsWarning_NonDegradeIsSilent()
    {
        Assert.Equal(LogLevel.Critical, DarlingHostBinding.MapBindReasonSeverity(DarlingHostBinding.BindReason.ListenInvalid));
        Assert.Equal(LogLevel.Critical, DarlingHostBinding.MapBindReasonSeverity(DarlingHostBinding.BindReason.TokenMissing));
        Assert.Equal(LogLevel.Critical, DarlingHostBinding.MapBindReasonSeverity(DarlingHostBinding.BindReason.AllowFromInvalid));
        Assert.Equal(LogLevel.Warning, DarlingHostBinding.MapBindReasonSeverity(DarlingHostBinding.BindReason.ManagedModeRequired));
        Assert.Null(DarlingHostBinding.MapBindReasonSeverity(DarlingHostBinding.BindReason.NetworkExposed));
        Assert.Null(DarlingHostBinding.MapBindReasonSeverity(DarlingHostBinding.BindReason.LoopbackByDefault));
    }

    /* ---- Refactor proof: the MCP host's nested enums mirror the shared ones 1:1 (same member order), so its
           ResolveMcpBind adapter's numeric cast (McpBindMode)(int)decision.Mode maps correctly. ---- */

    [Fact]
    public void NestedMcpEnums_MirrorSharedBindEnums_1To1_ForTheAdapterCast()
    {
        Assert.Equal((int)DarlingHostBinding.BindMode.LoopbackOnly, (int)DarlingMcpHostService.McpBindMode.LoopbackOnly);
        Assert.Equal((int)DarlingHostBinding.BindMode.NetworkAndLoopback, (int)DarlingMcpHostService.McpBindMode.NetworkAndLoopback);

        Assert.Equal((int)DarlingHostBinding.BindReason.LoopbackByDefault, (int)DarlingMcpHostService.McpBindReason.LoopbackByDefault);
        Assert.Equal((int)DarlingHostBinding.BindReason.NetworkExposed, (int)DarlingMcpHostService.McpBindReason.NetworkExposed);
        Assert.Equal((int)DarlingHostBinding.BindReason.ManagedModeRequired, (int)DarlingMcpHostService.McpBindReason.ManagedModeRequired);
        Assert.Equal((int)DarlingHostBinding.BindReason.ListenInvalid, (int)DarlingMcpHostService.McpBindReason.ListenInvalid);
        Assert.Equal((int)DarlingHostBinding.BindReason.TokenMissing, (int)DarlingMcpHostService.McpBindReason.TokenMissing);
        Assert.Equal((int)DarlingHostBinding.BindReason.AllowFromInvalid, (int)DarlingMcpHostService.McpBindReason.AllowFromInvalid);
    }

    /* ================================================================================================
       #2389: WHICH PLANE decided enabled/port. One mcp object in darling.json has two owners —
       enabled/port are a seed the store overrides forever after, network.* is file-only and restart-only —
       and before this the supervisor's `published?.Enabled ?? config.Mcp.Enabled` could not say which side
       it had taken. The resolution now carries its provenance, so the start line names it and a
       disagreement is reported at the point of override.
       ================================================================================================ */

    /// <summary>Nothing published yet (worker bootstrapping, or a store it never reached): the FILE values
    /// apply, unchanged — and are flagged as such, because the control plane can still contradict them.</summary>
    [Fact]
    public void ResolveEndpointToggle_Unpublished_TakesTheFileValues_AndSaysSo()
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle(published: null, fileEnabled: true, filePort: 5152);

        Assert.True(toggle.Enabled);
        Assert.Equal(5152, toggle.Port);
        Assert.Equal(DarlingHostBinding.EndpointToggleOrigin.File, toggle.Origin);

        /* Nothing is published, so there is nothing to disagree WITH — an unpublished read is not an override. */
        Assert.False(toggle.EnabledOverridden);
        Assert.False(toggle.PortOverridden);
        Assert.Null(DarlingHostBinding.DescribeToggleOverride(toggle, "mcp", "MCP", fileEnabled: true, filePort: 5152));
    }

    /// <summary>The published row wins — the pre-existing behavior, pinned so the diagnostic did not change it.</summary>
    [Theory]
    [InlineData(true, 5152, false, 5199)]
    [InlineData(false, 5199, true, 5152)]
    public void ResolveEndpointToggle_Published_AlwaysWins(bool storeEnabled, int storePort, bool fileEnabled, int filePort)
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle((storeEnabled, storePort), fileEnabled, filePort);

        Assert.Equal(storeEnabled, toggle.Enabled);
        Assert.Equal(storePort, toggle.Port);
        Assert.Equal(DarlingHostBinding.EndpointToggleOrigin.ControlPlane, toggle.Origin);
    }

    /// <summary>Agreeing planes are silent: the warning fires on the MISMATCH, not on every start.</summary>
    [Fact]
    public void DescribeToggleOverride_PlanesAgree_IsSilent()
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle((true, 5152), fileEnabled: true, filePort: 5152);

        Assert.False(toggle.EnabledOverridden);
        Assert.False(toggle.PortOverridden);
        Assert.Null(DarlingHostBinding.DescribeToggleOverride(toggle, "mcp", "MCP", fileEnabled: true, filePort: 5152));
    }

    /// <summary>
    /// The reported case (#2389): darling.json says enabled, the store says disabled, the store wins and the
    /// server is stopped five seconds after announcing a successful start. The message has to name BOTH sides
    /// with the key/column an operator can act on, say which one wins, and disclose the opposite ownership of
    /// the network block — otherwise the reader concludes their file edit worked.
    /// </summary>
    [Fact]
    public void DescribeToggleOverride_FileEnabledButStoreDisabled_NamesBothPlanesAndTheWinner()
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle((false, 5152), fileEnabled: true, filePort: 5152);
        var report = DarlingHostBinding.DescribeToggleOverride(toggle, "mcp", "MCP", fileEnabled: true, filePort: 5152);

        Assert.NotNull(report);
        Assert.Contains("true in darling.json (mcp.enabled)", report, StringComparison.Ordinal);
        Assert.Contains("false in config.config_service.mcp_enabled", report, StringComparison.Ordinal);
        Assert.Contains("CONTROL PLANE WINS", report, StringComparison.Ordinal);
        Assert.Contains("--enable-mcp/--disable-mcp", report, StringComparison.Ordinal);
        /* The other half of the confusion: network.* is file-authoritative and no store setting can move it. */
        Assert.Contains("mcp.network", report, StringComparison.Ordinal);
        Assert.Contains("cannot change where this endpoint binds", report, StringComparison.Ordinal);
        /* The endpoint IS down here, so the consequence names that state — and only in that state. */
        Assert.Contains("not what is keeping this endpoint down", report, StringComparison.Ordinal);
        /* Only the field that actually DIFFERS gets a clause — the ports agree here, so no port clause.
           (The closing advice still names mcp.port, which is why this pins the clause form, not the key.) */
        Assert.DoesNotContain("port is 5152 in darling.json", report, StringComparison.Ordinal);
    }

    /// <summary>The port has the identical shape (the issue's second half): a server on an unexpected port
    /// with no explanation. Both fields differing are reported together, in one line.</summary>
    [Fact]
    public void DescribeToggleOverride_ReportsThePortToo_AndBothFieldsAtOnce()
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle((true, 5199), fileEnabled: false, filePort: 5152);
        var report = DarlingHostBinding.DescribeToggleOverride(toggle, "mcp", "MCP", fileEnabled: false, filePort: 5152);

        Assert.NotNull(report);
        Assert.Contains("false in darling.json (mcp.enabled)", report, StringComparison.Ordinal);
        Assert.Contains("true in config.config_service.mcp_enabled", report, StringComparison.Ordinal);
        Assert.Contains("port is 5152 in darling.json (mcp.port) but 5199 in config.config_service.mcp_port", report, StringComparison.Ordinal);

        /* Review catch: the control plane turned this endpoint ON despite the file, so the closing consequence
           must not claim it is being kept off — a diagnostic that exists to stop operators being misled about
           the effective state cannot itself misstate it. */
        Assert.DoesNotContain("keeping this endpoint down", report, StringComparison.Ordinal);
        Assert.Contains("what this RUNNING endpoint is bound and gated by", report, StringComparison.Ordinal);
    }

    /// <summary>
    /// The likeliest real mismatch, and the one that reads least like a bug: both planes agree the endpoint
    /// runs and only the PORT differs, because someone moved it in the Viewer's Settings. It is still a
    /// reportable disagreement — the file value is dead and the firewall rule is named off it (#2414) — but the
    /// endpoint is up, so nothing in the message may say otherwise.
    /// </summary>
    [Fact]
    public void DescribeToggleOverride_PortOnlyMismatch_ReportsIt_WithoutClaimingTheEndpointIsDown()
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle((true, 5199), fileEnabled: true, filePort: 5152);
        var report = DarlingHostBinding.DescribeToggleOverride(toggle, "mcp", "MCP", fileEnabled: true, filePort: 5152);

        Assert.NotNull(report);
        Assert.True(toggle.PortOverridden);
        Assert.False(toggle.EnabledOverridden);
        Assert.Contains("port is 5152 in darling.json (mcp.port) but 5199 in config.config_service.mcp_port", report, StringComparison.Ordinal);
        /* The planes agree on enabled, so no enabled clause. */
        Assert.DoesNotContain("in darling.json (mcp.enabled)", report, StringComparison.Ordinal);
        Assert.DoesNotContain("keeping this endpoint down", report, StringComparison.Ordinal);
    }

    /// <summary>The web dashboard is the same defect on the same seam, so it shares the resolver: one
    /// <c>section</c> argument drives the file key, the store column and the CLI verb, which is what stops the
    /// two surfaces' wordings from drifting apart.</summary>
    [Fact]
    public void DescribeToggleOverride_WebSurface_NamesTheWebKeysAndVerbs()
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle((false, 5153), fileEnabled: true, filePort: 5153);
        var report = DarlingHostBinding.DescribeToggleOverride(toggle, "web", "Web dashboard", fileEnabled: true, filePort: 5153);

        Assert.NotNull(report);
        Assert.Contains("web.enabled", report, StringComparison.Ordinal);
        Assert.Contains("config.config_service.web_enabled", report, StringComparison.Ordinal);
        Assert.Contains("--enable-web/--disable-web", report, StringComparison.Ordinal);
        Assert.DoesNotContain("mcp", report, StringComparison.Ordinal);
    }

    /// <summary>The start line's provenance clause distinguishes the two planes, and the file case admits it
    /// is provisional — that line is the one the operator greps and stops reading.</summary>
    [Fact]
    public void DescribeToggleOrigin_DistinguishesThePlanes_AndFlagsTheFileCaseAsProvisional()
    {
        var fromStore = DarlingHostBinding.DescribeToggleOrigin(
            DarlingHostBinding.ResolveEndpointToggle((true, 5152), fileEnabled: true, filePort: 5152));
        var fromFile = DarlingHostBinding.DescribeToggleOrigin(
            DarlingHostBinding.ResolveEndpointToggle(published: null, fileEnabled: true, filePort: 5152));

        Assert.Contains("config.config_service", fromStore, StringComparison.Ordinal);
        Assert.Contains("darling.json", fromFile, StringComparison.Ordinal);
        Assert.Contains("PROVISIONAL", fromFile, StringComparison.Ordinal);
        Assert.NotEqual(fromStore, fromFile);
    }

    /* ---- #2414: which plane named the port a FIREWALL rule is scoped to ---- */

    /// <summary>
    /// The defect itself. The port lives inside the rule's DisplayName, so a rule named from darling.json on a
    /// box whose store has moved the port is not a mis-scoped rule, it is a DIFFERENT rule: the served port stays
    /// shut and an inbound allow rule sits on a port with no listener. The disclosure has to carry both numbers,
    /// name the column that won, and state that consequence — an operator staring at blocked LAN clients reads
    /// this line and has to be able to stop looking at the CIDR and the token.
    /// </summary>
    [Fact]
    public void DescribeFirewallPortAuthority_StoreDisagreesWithTheFile_NamesBothPortsAndTheCost()
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle((true, 5199), fileEnabled: true, filePort: 5152);
        var report = DarlingHostBinding.DescribeFirewallPortAuthority(toggle, "mcp", "MCP", filePort: 5152, storeUnavailableReason: null);

        Assert.Contains("port 5199", report, StringComparison.Ordinal);
        Assert.Contains("CONTROL PLANE", report, StringComparison.Ordinal);
        Assert.Contains("config.config_service.mcp_port", report, StringComparison.Ordinal);
        Assert.Contains("mcp.port = 5152", report, StringComparison.Ordinal);
        /* Both halves of the cost, because the second one is the security half and is the less obvious. */
        Assert.Contains("closed to the LAN", report, StringComparison.Ordinal);
        Assert.Contains("nothing is listening on", report, StringComparison.Ordinal);
    }

    /// <summary>When the planes agree the line is a CONFIRMATION, not silence: a verb that opens a port should
    /// say on whose authority it picked one every time, so the absence of the disagreement wording is evidence
    /// the authoritative read happened rather than evidence of nothing.</summary>
    [Fact]
    public void DescribeFirewallPortAuthority_PlanesAgree_ConfirmsTheReadWithoutCryingWolf()
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle((true, 5152), fileEnabled: true, filePort: 5152);
        var report = DarlingHostBinding.DescribeFirewallPortAuthority(toggle, "mcp", "MCP", filePort: 5152, storeUnavailableReason: null);

        Assert.Contains("port 5152", report, StringComparison.Ordinal);
        Assert.Contains("confirmed against the control plane", report, StringComparison.Ordinal);
        Assert.DoesNotContain("could NOT read", report, StringComparison.Ordinal);
        Assert.DoesNotContain("WRONG port", report, StringComparison.Ordinal);
    }

    /// <summary>
    /// The store-unreachable case, which is the one with a security cost if it goes quiet. --configure-firewall
    /// runs elevated at install time, before the store exists, so it CANNOT require one — but a silent fall back
    /// to darling.json is how the wrong rule got recreated on every run in the first place. So the line must name
    /// the port it used, the reason it could do no better, and the state in which that port is wrong.
    /// </summary>
    [Fact]
    public void DescribeFirewallPortAuthority_StoreUnreadable_SaysWhichPortItUsed_WhyAndWhenThatIsWrong()
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle(published: null, fileEnabled: true, filePort: 5152);
        var report = DarlingHostBinding.DescribeFirewallPortAuthority(
            toggle, "mcp", "MCP", filePort: 5152, storeUnavailableReason: "the store did not answer within 10 seconds");

        Assert.Contains("could NOT read the control plane", report, StringComparison.Ordinal);
        Assert.Contains("the store did not answer within 10 seconds", report, StringComparison.Ordinal);
        Assert.Contains("mcp.port = 5152", report, StringComparison.Ordinal);
        Assert.Contains("FIRST-RUN", report, StringComparison.Ordinal);
        Assert.Contains("WRONG port", report, StringComparison.Ordinal);
        /* And the way out, because this verb is the one an operator is told to re-run. */
        Assert.Contains("--configure-firewall", report, StringComparison.Ordinal);
    }

    /// <summary>A missing reason must not silently become an empty parenthesis that reads like "no problem".</summary>
    [Fact]
    public void DescribeFirewallPortAuthority_StoreUnreadableWithNoReasonGiven_StillAdmitsItDoesNotKnow()
    {
        var toggle = DarlingHostBinding.ResolveEndpointToggle(published: null, fileEnabled: true, filePort: 5152);
        var report = DarlingHostBinding.DescribeFirewallPortAuthority(toggle, "mcp", "MCP", filePort: 5152, storeUnavailableReason: null);

        Assert.Contains("reason unknown", report, StringComparison.Ordinal);
    }

    /// <summary>web is the byte-identical twin of mcp on this seam, and shares the resolver: one section
    /// argument drives the file key, the store column and the CLI verb, which is what keeps the two surfaces'
    /// wordings from drifting apart the way their ports did.</summary>
    [Fact]
    public void DescribeFirewallPortAuthority_WebSurface_NamesTheWebKeysAndVerbs()
    {
        var overridden = DarlingHostBinding.DescribeFirewallPortAuthority(
            DarlingHostBinding.ResolveEndpointToggle((true, 5188), fileEnabled: true, filePort: 5153),
            "web", "web dashboard", filePort: 5153, storeUnavailableReason: null);

        Assert.Contains("config.config_service.web_port", overridden, StringComparison.Ordinal);
        Assert.Contains("web.port = 5153", overridden, StringComparison.Ordinal);
        Assert.DoesNotContain("mcp", overridden, StringComparison.Ordinal);

        var fallback = DarlingHostBinding.DescribeFirewallPortAuthority(
            DarlingHostBinding.ResolveEndpointToggle(published: null, fileEnabled: true, filePort: 5153),
            "web", "web dashboard", filePort: 5153, storeUnavailableReason: "no credential");

        Assert.Contains("--enable-web", fallback, StringComparison.Ordinal);
        Assert.DoesNotContain("--enable-mcp", fallback, StringComparison.Ordinal);
    }
}
