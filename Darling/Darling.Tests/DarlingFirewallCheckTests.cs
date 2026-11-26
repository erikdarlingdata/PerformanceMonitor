/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #1771 — the service used to try to CREATE its own firewall rules from an unprivileged virtual account.
/// Every attempt failed "Access is denied" on a hardened box, and on a fresh networked install (the normal
/// deployment mode) the rule was never created at all, so remote clients were blocked outright.
///
/// <para>Rule management moved to the elevated install path; the runtime only VERIFIES. These pin both
/// halves: the read-only probe/verdict/remedy logic, the pure desired-state planner the elevated verb runs,
/// and the elevated verb itself, run against a recording shell so what it hands to PowerShell, and what it
/// prints and returns, are observed rather than read out of its source (#9788). The few seams that no test can
/// reach without an elevated host (the install and uninstall scripts, the hosts' start-up call sites) are
/// still pinned by reading their text below.</para>
/// </summary>
public class DarlingFirewallCheckTests
{
    /* ---- the probe: read-only, and shaped so "absent" is an ANSWER, not a failure ---- */

    [Fact]
    public void BuildProbeCommand_ReadsOnly_AndNeverWrites()
    {
        var command = DarlingFirewallCheck.BuildProbeCommand("PerformanceMonitor Darling MCP (port 5152)");

        Assert.Contains("Get-NetFirewallRule", command, StringComparison.Ordinal);
        Assert.Contains("-DisplayName 'PerformanceMonitor Darling MCP (port 5152)'", command, StringComparison.Ordinal);

        /* The whole point of #1771: the running service issues NO firewall write. */
        Assert.DoesNotContain("New-NetFirewallRule", command, StringComparison.Ordinal);
        Assert.DoesNotContain("Remove-NetFirewallRule", command, StringComparison.Ordinal);
        Assert.DoesNotContain("Set-NetFirewallRule", command, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProbeCommand_TreatsAnAbsentRuleAsAnAnswer_NotAFailure()
    {
        var command = DarlingFirewallCheck.BuildProbeCommand("PerformanceMonitor Darling Web (port 5153)");

        /* An exact DisplayName matching nothing is an ObjectNotFound ERROR from Get-NetFirewallRule (verified
           on Windows 11 26200), and a suppressed error still exits 1 — the same trap the disable builder
           documents. Without BOTH of these, every absent rule would read as a broken probe and warn. */
        Assert.Contains("-ErrorAction SilentlyContinue", command, StringComparison.Ordinal);
        Assert.Contains("exit 0", command, StringComparison.Ordinal);

        /* One DisplayName can match several rules (Windows stores one per profile), and a single match has no
           .Count without the array wrapper. */
        Assert.Contains("@(Get-NetFirewallRule", command, StringComparison.Ordinal);
        Assert.Contains(DarlingFirewallCheck.ProbeSentinel, command, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("rule'; whoami; '")]
    [InlineData("$(whoami)")]
    [InlineData("x'; New-NetFirewallRule -DisplayName 'pwn' -RemoteAddress Any; '")]
    public void BuildProbeCommand_KeepsAnyRuleNameInsideOneSingleQuotedLiteral(string hostile)
    {
        var command = DarlingFirewallCheck.BuildProbeCommand(hostile);

        /* Inside '…' PowerShell expands nothing, so the ONE way out is an unescaped quote: an odd total means
           the value broke out. Same property the write builders carry (#1646), enforced on the read path too. */
        Assert.True((command.Split('\'').Length - 1) % 2 == 0, $"unbalanced quoting: {command}");
        Assert.Contains(
            "-DisplayName '" + hostile.Replace("'", "''", StringComparison.Ordinal) + "'",
            command,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("DarlingFirewallProbe=0", false)]
    [InlineData("DarlingFirewallProbe=1", true)]
    [InlineData("DarlingFirewallProbe=3", true)]
    [InlineData("noise before\r\nDarlingFirewallProbe=2\r\nnoise after", true)]
    public void TryParseProbeOutput_ReadsTheCount(string output, bool expected)
        => Assert.Equal(expected, DarlingFirewallCheck.TryParseProbeOutput(output));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Get-NetFirewallRule : The term is not recognized")]
    [InlineData("DarlingFirewallProbe=")]
    [InlineData("DarlingFirewallProbe=x")]
    public void TryParseProbeOutput_IsNullWhenItCannotTell(string? output)
    {
        /* Null must NOT collapse into "absent": that would manufacture a "rule is MISSING" warning out of a
           broken probe. It becomes ProbeFailed, which says exactly what happened. */
        Assert.Null(DarlingFirewallCheck.TryParseProbeOutput(output));
        Assert.Equal(
            FirewallRuleVerdict.ProbeFailed,
            DarlingFirewallCheck.Classify(exposed: true, DarlingFirewallCheck.TryParseProbeOutput(output)));
    }

    /* ---- the verdict table: the loopback-vs-networked gate ---- */

    [Theory]
    [InlineData(true, true, FirewallRuleVerdict.ExposedRulePresent)]
    [InlineData(true, false, FirewallRuleVerdict.ExposedRuleMissing)]
    [InlineData(false, true, FirewallRuleVerdict.LoopbackStaleRule)]
    [InlineData(false, false, FirewallRuleVerdict.LoopbackNoRule)]
    public void Classify_MapsExposureAndPresence(bool exposed, bool present, FirewallRuleVerdict expected)
        => Assert.Equal(expected, DarlingFirewallCheck.Classify(exposed, present));

    [Fact]
    public void Classify_LoopbackWithNoRule_IsTheSilentHealthyDefault()
    {
        /* The constraint from #1771: a loopback-only install needs no rule and must produce NO warning. The
           verdict itself carries that (it is the one with no remedy and no log line). */
        var verdict = DarlingFirewallCheck.Classify(exposed: false, present: false);

        Assert.Equal(FirewallRuleVerdict.LoopbackNoRule, verdict);
        Assert.Null(DarlingFirewallCheck.BuildRemedyCommand(verdict, "PerformanceMonitor Darling MCP (port 5152)", 5152, null));
    }

    /* ---- the remedy: a WARN that names the exact command ---- */

    [Fact]
    public void BuildRemedyCommand_ForAMissingRule_IsTheExactScopedOpenCommand()
    {
        var remedy = DarlingFirewallCheck.BuildRemedyCommand(
            FirewallRuleVerdict.ExposedRuleMissing, "PerformanceMonitor Darling MCP (port 5152)", 5152, "192.168.1.0/24");

        Assert.NotNull(remedy);
        Assert.Contains("New-NetFirewallRule", remedy, StringComparison.Ordinal);
        Assert.Contains("-LocalPort 5152", remedy, StringComparison.Ordinal);
        Assert.Contains("-RemoteAddress '192.168.1.0/24'", remedy, StringComparison.Ordinal);

        /* Built by the SAME builder the elevated verb runs, so the printed command and the applied one cannot
           drift into being different rules. */
        Assert.Equal(
            DarlingManagedPostgres.BuildFirewallEnableCommand("PerformanceMonitor Darling MCP (port 5152)", 5152, "192.168.1.0/24"),
            remedy);
    }

    /* ---- #5288: allowFrom is a CIDR LIST; the builder quotes each range on its own ---- */

    [Fact]
    public void BuildFirewallEnableCommand_List_QuotesEachRange()
    {
        const string ruleName = "PerformanceMonitor Darling MCP (port 5152)";

        /* Exact text: one single-quoted literal per range, joined by "," with no spaces (PowerShell array syntax).
           Quoting only the first range, or a bare comma list inside one literal, would change this string. */
        Assert.Equal(
            $"Remove-NetFirewallRule -DisplayName '{ruleName}' -ErrorAction SilentlyContinue; " +
            $"New-NetFirewallRule -DisplayName '{ruleName}' -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5152 -RemoteAddress '10.8.0.0/16','192.168.1.5/32' | Out-Null",
            DarlingManagedPostgres.BuildFirewallEnableCommand(ruleName, 5152, "10.8.0.0/16,192.168.1.5/32"));

        Assert.Contains(
            "-RemoteAddress '10.8.0.0/16','192.168.1.0/24','2001:db8::/32' |",
            DarlingManagedPostgres.BuildFirewallEnableCommand(ruleName, 5152, "10.8.0.0/16,192.168.1.0/24,2001:db8::/32"),
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("10.0.0.0/8,x'; whoami; '")]
    [InlineData("10.0.0.0/8,10.8.0.0/16’; whoami; ’")]
    public void BuildFirewallEnableCommand_List_RefusesAnElementThatIsNotAnAddressAndPrefix(string remoteCidr)
    {
        /* The caller-side parse is the primary gate; this is the second layer, and it holds per element: one
           element holding a character outside an address and prefix length (0-9, A-F, ':', '.', '/') refuses the
           whole list, so nothing but address-shaped elements is ever quoted into the command. */
        Assert.Throws<ArgumentException>(
            () => DarlingManagedPostgres.BuildFirewallEnableCommand("PerformanceMonitor Darling MCP (port 5152)", 5152, remoteCidr));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(",")]
    [InlineData("10.8.0.0/16,")]
    [InlineData(",10.8.0.0/16")]
    [InlineData("10.8.0.0/16,,192.168.1.5/32")]
    [InlineData("10.8.0.0/16, ,192.168.1.5/32")]
    public void BuildFirewallEnableCommand_EmptyElement_Throws_RatherThanEmittingEmptyQuotes(string remoteCidr)
        => Assert.Throws<ArgumentException>(
            () => DarlingManagedPostgres.BuildFirewallEnableCommand("PerformanceMonitor Darling MCP (port 5152)", 5152, remoteCidr));

    [Fact]
    public void BuildRemedyCommand_ForAMissingRule_WithAList_IsTheBuildersText()
    {
        const string rule = "PerformanceMonitor Darling MCP (port 5152)";
        var remedy = DarlingFirewallCheck.BuildRemedyCommand(
            FirewallRuleVerdict.ExposedRuleMissing, rule, 5152, "10.8.0.0/16,192.168.1.0/24");

        Assert.Equal(DarlingManagedPostgres.BuildFirewallEnableCommand(rule, 5152, "10.8.0.0/16,192.168.1.0/24"), remedy);
        Assert.Contains("-RemoteAddress '10.8.0.0/16','192.168.1.0/24' |", remedy, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildRemedyCommand_ForAStaleRule_RemovesOnly()
    {
        var remedy = DarlingFirewallCheck.BuildRemedyCommand(
            FirewallRuleVerdict.LoopbackStaleRule, "PerformanceMonitor Darling Web (port 5153)", 5153, null);

        Assert.NotNull(remedy);
        Assert.Contains("Remove-NetFirewallRule", remedy, StringComparison.Ordinal);
        Assert.DoesNotContain("New-NetFirewallRule", remedy, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(FirewallRuleVerdict.ExposedRulePresent)]
    [InlineData(FirewallRuleVerdict.LoopbackNoRule)]
    [InlineData(FirewallRuleVerdict.ProbeFailed)]
    public void BuildRemedyCommand_IsNullWhenThereIsNothingToFix(FirewallRuleVerdict verdict)
        => Assert.Null(DarlingFirewallCheck.BuildRemedyCommand(verdict, "PerformanceMonitor Darling store (port 5641)", 5641, "10.0.0.0/8"));

    [Fact]
    public void BuildRemedyCommand_RefusesToBuildAnOpenCommandWithoutACidr()
    {
        /* An exposed-but-CIDR-less config is one the service fail-closes anyway. Emitting an open command with
           an empty -RemoteAddress would hand the operator a rule that opens the port to the world. */
        Assert.Null(DarlingFirewallCheck.BuildRemedyCommand(
            FirewallRuleVerdict.ExposedRuleMissing, "PerformanceMonitor Darling MCP (port 5152)", 5152, null));
        Assert.Null(DarlingFirewallCheck.BuildRemedyCommand(
            FirewallRuleVerdict.ExposedRuleMissing, "PerformanceMonitor Darling MCP (port 5152)", 5152, "   "));
    }

    /* ---- the per-surface sweep: a port change strands the OLD rule, and only a wildcard reaches it ---- */

    [Theory]
    [InlineData("PerformanceMonitor Darling MCP (port 5152)", "PerformanceMonitor Darling MCP (port *)")]
    [InlineData("PerformanceMonitor Darling Web (port 5153)", "PerformanceMonitor Darling Web (port *)")]
    [InlineData("PerformanceMonitor Darling store (port 5641)", "PerformanceMonitor Darling store (port *)")]
    public void SurfaceRuleWildcard_CoversEveryPortOfOneSurface(string ruleName, string expected)
        => Assert.Equal(expected, DarlingFirewallCheck.SurfaceRuleWildcard(ruleName));

    [Fact]
    public void SurfaceRuleWildcard_KeepsSurfacesApart()
    {
        /* The sweep must not reach across surfaces: clearing MCP's old ports must never touch the web rule. */
        var mcp = DarlingFirewallCheck.SurfaceRuleWildcard(DarlingMcpHostService.McpFirewallRuleName(5152));

        Assert.DoesNotContain("Web", mcp, StringComparison.Ordinal);
        Assert.DoesNotContain("store", mcp, StringComparison.Ordinal);
        Assert.NotEqual(mcp, DarlingFirewallCheck.SurfaceRuleWildcard(DarlingWebHostService.WebFirewallRuleName(5153)));
    }

    [Theory]
    [InlineData("PerformanceMonitor Darling MCP")]
    [InlineData("")]
    [InlineData("something else entirely")]
    public void SurfaceRuleWildcard_NeverInventsAWildcardFromAnUnrecognizedName(string ruleName)
    {
        /* Safety, not formality: a wildcard derived from a name with no port suffix would broaden the sweep to
           whatever else happened to share the prefix — rules this product does not own and must not delete. */
        Assert.Equal(ruleName, DarlingFirewallCheck.SurfaceRuleWildcard(ruleName));
        Assert.DoesNotContain("*", DarlingFirewallCheck.SurfaceRuleWildcard(ruleName), StringComparison.Ordinal);
    }

    [Fact]
    public void BuildFirewallSweepCommand_IsStandalone_AndReportsRealFailuresLoudly()
    {
        var cmd = DarlingManagedPostgres.BuildFirewallSweepCommand("PerformanceMonitor Darling MCP (port *)");

        Assert.Contains("Remove-NetFirewallRule -DisplayName 'PerformanceMonitor Darling MCP (port *)'", cmd, StringComparison.Ordinal);
        Assert.DoesNotContain("New-NetFirewallRule", cmd, StringComparison.Ordinal);

        /* SilentlyContinue would hide the error TEXT but still exit 1, so access denied would surface as
           "exit 1:" with an empty message — the trap BuildFirewallDisableCommand documents. */
        Assert.DoesNotContain("-ErrorAction SilentlyContinue", cmd, StringComparison.Ordinal);
        Assert.Contains("-ErrorAction Stop", cmd, StringComparison.Ordinal);
        Assert.Contains("ObjectNotFound", cmd, StringComparison.Ordinal);
        Assert.Contains("throw", cmd, StringComparison.Ordinal);

        /* It ends in `exit 0`, which makes it a COMPLETE command: concatenating it ahead of another would
           terminate the shell before that command ran. The verb must run it as its own step. */
        Assert.EndsWith("exit 0", cmd, StringComparison.Ordinal);
    }

    /* ---- no retry spam: report once per state, not once per supervisor tick ---- */

    [Fact]
    public void ShouldReport_OnlyWhenTheStateChanges()
    {
        const string rule = "PerformanceMonitor Darling MCP (port 5152)";

        /* First observation always reports. */
        Assert.True(DarlingFirewallCheck.ShouldReport(null, null, rule, FirewallRuleVerdict.ExposedRuleMissing));

        /* The supervisor re-attempts a failed start every 30s; the same verdict must stay quiet. */
        Assert.False(DarlingFirewallCheck.ShouldReport(rule, FirewallRuleVerdict.ExposedRuleMissing, rule, FirewallRuleVerdict.ExposedRuleMissing));

        /* A real transition reports again — an admin created the rule. */
        Assert.True(DarlingFirewallCheck.ShouldReport(rule, FirewallRuleVerdict.ExposedRuleMissing, rule, FirewallRuleVerdict.ExposedRulePresent));

        /* A port change makes a DIFFERENT rule; the new rule's state has never been stated. */
        Assert.True(DarlingFirewallCheck.ShouldReport(
            rule, FirewallRuleVerdict.ExposedRulePresent, "PerformanceMonitor Darling MCP (port 6000)", FirewallRuleVerdict.ExposedRulePresent));
    }

    /* ---- the elevated planner: desired state per surface, from darling.json alone ---- */

    [Fact]
    public void PlanFirewallRules_LoopbackOnlyManagedConfig_OpensNothing()
    {
        var plans = DarlingCliCommands.PlanFirewallRules(ManagedConfig());

        Assert.All(plans, p => Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, p.Action));

        /* Removal is idempotent, so the loopback default is a clean no-op that also sweeps up a rule left
           behind by an exposure that was turned off. */
        Assert.All(plans, p => Assert.Null(p.Cidr));
    }

    [Fact]
    public void PlanFirewallRules_ExposedMcp_OpensTheScopedRule()
    {
        var config = ManagedConfig();
        /* #2436: enabled AND exposed. mcp.enabled defaults false, and since #2436 a surface the effective
           toggle has switched off gets its rule removed rather than opened — so a fixture that means "this
           endpoint is serving the LAN" has to say both halves. */
        config.Mcp.Enabled = true;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var mcp = Assert.Single(PlansFor(config, "MCP"));

        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Open, mcp.Action);
        Assert.Equal(DarlingMcpHostService.McpFirewallRuleName(config.Mcp.Port), mcp.RuleName);
        Assert.Equal(config.Mcp.Port, mcp.Port);
        Assert.Equal("192.168.1.0/24", mcp.Cidr);

        /* The other two surfaces are untouched by an MCP exposure. */
        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, Assert.Single(PlansFor(config, "web dashboard")).Action);
        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, Assert.Single(PlansFor(config, "store")).Action);
    }

    [Fact]
    public void PlanFirewallRules_UsesTheParsersCanonicalCidr_NotTheRawString()
    {
        /* IPNetwork.TryParse MASKS host bits rather than rejecting them, so "validate then use the original"
           would open a rule for a CIDR the parser had already decided meant something else. */
        var config = ManagedConfig();
        config.Mcp.Enabled = true;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.77/24", Token = "t" };

        Assert.Equal("192.168.1.0/24", Assert.Single(PlansFor(config, "MCP")).Cidr);
    }

    [Theory]
    [InlineData(null, "t")]              /* exposed listen, NO allowFrom  -> the service fail-closes to loopback */
    [InlineData("not-a-cidr", "t")]      /* exposed listen, junk allowFrom -> fail-closed                        */
    [InlineData("2001:db8::/32", "t")]   /* address family disagrees with an IPv4 listen -> fail-closed          */
    [InlineData("192.168.1.0/24", null)] /* no token -> the endpoint refuses to expose                           */
    public void PlanFirewallRules_NeverOpensAPortTheServiceWillNotExpose(string? allowFrom, string? token)
    {
        /* THE gate. The planner asks the SAME resolver the running service fail-closes on, so a config the
           service degrades to loopback gets no open port — and the service's own start-up check reaches the
           same verdict, instead of flagging a rule the installer had just created. */
        var config = ManagedConfig();
        config.Mcp.Enabled = true;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = allowFrom, Token = token };

        var mcp = Assert.Single(PlansFor(config, "MCP"));
        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, mcp.Action);
        Assert.Null(mcp.Cidr);
        Assert.NotNull(mcp.Note); /* and it says WHY, rather than silently doing nothing */
    }

    [Fact]
    public void PlanFirewallRules_ByoMode_OpensNothingAndSkipsTheStore()
    {
        /* LAN exposure for MCP/web is managed-mode only, and in BYO the operator's own PostgreSQL governs the
           store's exposure — so the installer must not open ports for a store it does not run. */
        var config = ManagedConfig();
        config.Postgres!.Managed = false;
        config.Mcp.Enabled = true;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var all = DarlingCliCommands.PlanFirewallRules(config);

        Assert.DoesNotContain(all, p => p.Surface == "store");
        Assert.All(all, p => Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, p.Action));
        Assert.NotNull(Assert.Single(PlansFor(config, "MCP")).Note);
    }

    [Fact]
    public void PlanFirewallRules_CoversAllThreeSurfaces_WithDistinctPortSpecificRules()
    {
        /* The issue named 5152/5153, but the bundled store had the identical structural bug on its own port. */
        var all = DarlingCliCommands.PlanFirewallRules(ManagedConfig());

        Assert.Equal(3, all.Count);
        Assert.Equal(3, all.Select(p => p.RuleName).Distinct(StringComparer.Ordinal).Count());
        Assert.All(all, p => Assert.StartsWith(DarlingFirewallCheck.RuleNamePrefix, p.RuleName, StringComparison.Ordinal));
        Assert.All(all, p => Assert.Contains($"(port {p.Port})", p.RuleName, StringComparison.Ordinal));
    }

    /* ---- #2414: the plan's PORT follows the endpoint, not darling.json ---- */

    /// <summary>
    /// The defect, at the surface that owns every rule. The supervisor binds <c>config_service.mcp_port</c>;
    /// the planner used <c>config.Mcp.Port</c>. On a box where those differ, the elevated verb created
    /// "…MCP (port 5152)" for a server listening on 5199 — the served port left shut and an inbound allow rule
    /// standing on a port with nothing behind it — and re-created it on every subsequent run.
    /// </summary>
    [Fact]
    public void PlanFirewallRules_NamesTheRuleForTheStoresPort_NotTheFilesSeed()
    {
        var config = ManagedConfig();
        config.Mcp.Port = 5152;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var mcp = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, mcpStore: (true, 5199)), p => p.Surface == "MCP");

        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Open, mcp.Action);
        Assert.Equal(5199, mcp.Port);
        Assert.Equal("PerformanceMonitor Darling MCP (port 5199)", mcp.RuleName);
        Assert.Equal(DarlingMcpHostService.McpFirewallRuleName(5199), mcp.RuleName);

        /* The rule the service's own start-up check looks for, and the wildcard the sweep clears, are both
           derived from this name — so pinning the name pins that the verb and the runtime agree. */
        Assert.Equal("PerformanceMonitor Darling MCP (port *)", DarlingFirewallCheck.SurfaceRuleWildcard(mcp.RuleName));

        Assert.NotNull(mcp.PortNote);
        Assert.Contains("config.config_service.mcp_port", mcp.PortNote, StringComparison.Ordinal);
        Assert.Contains("mcp.port = 5152", mcp.PortNote, StringComparison.Ordinal);
    }

    /// <summary>web is the byte-identical twin: the same split, the same resolver, the same rule shape.</summary>
    [Fact]
    public void PlanFirewallRules_WebToo_NamesTheRuleForTheStoresPort()
    {
        var config = ManagedConfig();
        config.Web.Port = 5153;
        config.Web.Network = new WebNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var web = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, webStore: (true, 5188)), p => p.Surface == "web dashboard");

        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Open, web.Action);
        Assert.Equal(5188, web.Port);
        Assert.Equal(DarlingWebHostService.WebFirewallRuleName(5188), web.RuleName);
        Assert.NotNull(web.PortNote);
        Assert.Contains("config.config_service.web_port", web.PortNote, StringComparison.Ordinal);
    }

    /// <summary>
    /// The store surface has NO such split — <c>postgres.port</c> is file-only, with no config_service column
    /// able to disagree with it — so it keeps resolving from the file and must not grow a note claiming a
    /// provenance it does not have.
    /// </summary>
    [Fact]
    public void PlanFirewallRules_TheStoreSurfacesPortStaysFileResolved_AndCarriesNoPortNote()
    {
        var config = ManagedConfig();
        config.Postgres!.Port = 5641;
        config.Postgres.Network = new PostgresNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24" };

        var store = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, mcpStore: (true, 5199), webStore: (true, 5188)),
            p => p.Surface == "store");

        Assert.Equal(5641, store.Port);
        Assert.Null(store.PortNote);
    }

    /// <summary>
    /// The install-time path, and the one with a security cost if it stays quiet. The verb runs elevated before
    /// the store exists, so it falls back to darling.json's port — which is correct there, because the store row
    /// is SEEDED from that value — but the plan has to carry the admission, the reason, and the state in which
    /// the port it just used is the wrong one.
    /// </summary>
    [Fact]
    public void PlanFirewallRules_StoreUnreadable_UsesTheFilePort_AndSaysSoRatherThanFallingBackQuietly()
    {
        var config = ManagedConfig();
        config.Mcp.Enabled = true;
        config.Mcp.Port = 5152;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var mcp = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, storeUnavailableReason: "the store did not answer within 10 seconds"),
            p => p.Surface == "MCP");

        Assert.Equal(5152, mcp.Port);
        Assert.NotNull(mcp.PortNote);
        Assert.Contains("could NOT read the control plane", mcp.PortNote, StringComparison.Ordinal);
        Assert.Contains("the store did not answer within 10 seconds", mcp.PortNote, StringComparison.Ordinal);
        Assert.Contains("WRONG port", mcp.PortNote, StringComparison.Ordinal);
    }

    /// <summary>A Remove plan sweeps EVERY port of its surface, so it does not depend on having picked the
    /// right one — a port note there would be noise claiming a decision that was never made.</summary>
    [Fact]
    public void PlanFirewallRules_LoopbackPlansCarryNoPortNote()
        => Assert.All(
            DarlingCliCommands.PlanFirewallRules(ManagedConfig(), mcpStore: (true, 5199), webStore: (true, 5188)),
            p => Assert.Null(p.PortNote));

    /* ---- #2436: a rule follows the ENABLE flag too, and the sweep follows what the run can vouch for ---- */

    /// <summary>
    /// The residue #2432 disclosed. <c>mcp.network</c> describes HOW the endpoint would be exposed; whether it
    /// runs at all is <c>config_service.mcp_enabled</c>, and the supervisor stops the server outright when that
    /// is false. The planner read only the network block, so the elevated verb opened an inbound allow rule on
    /// a port with nothing behind it — the exact shape #2414 was filed about, one field over.
    /// <para>The concrete bug, rather than the principle: <c>--disable-mcp</c> sweeps the surface's rules, and
    /// the very next <c>--configure-firewall</c> re-created one. install-darling.ps1 runs that verb on every
    /// upgrade, so a deliberately disabled endpoint got its port re-opened by an upgrade.</para>
    /// </summary>
    [Fact]
    public void PlanFirewallRules_ControlPlaneHasTheSurfaceOff_RemovesTheRuleInsteadOfOpeningADeadPort()
    {
        var config = ManagedConfig();
        config.Mcp.Enabled = true;   /* the file's seed says yes... */
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        /* ...and the control plane, which is what the supervisor obeys, says no. */
        var mcp = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, mcpStore: (false, 5152)), p => p.Surface == "MCP");

        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, mcp.Action);
        Assert.Null(mcp.Cidr);

        /* And it says WHY, because "your exposure config is fine but no rule appeared" is otherwise an hour
           of looking at the CIDR. */
        Assert.NotNull(mcp.Note);
        Assert.Contains("config.config_service.mcp_enabled = false", mcp.Note, StringComparison.Ordinal);
        Assert.Contains("--enable-mcp", mcp.Note, StringComparison.Ordinal);

        /* A Remove always sweeps: "no rule on any port" needs no opinion about which port is live, and this is
           what makes a --disable-mcp stay done across the upgrade that re-runs this verb. */
        Assert.True(mcp.SweepOtherPorts);
    }

    /// <summary>The web dashboard is the byte-identical twin, as it is everywhere in this area.</summary>
    [Fact]
    public void PlanFirewallRules_ControlPlaneHasTheWebSurfaceOff_RemovesTheRuleToo()
    {
        var config = ManagedConfig();
        config.Web.Enabled = true;
        config.Web.Network = new WebNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var web = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, webStore: (false, 5153)), p => p.Surface == "web dashboard");

        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, web.Action);
        Assert.Contains("config.config_service.web_enabled = false", web.Note, StringComparison.Ordinal);
    }

    /// <summary>
    /// The install-time half of the same rule, and the reason it is not merely symmetry: at install time the
    /// store does not exist, so <c>mcp.enabled</c> IS the value <c>config_service.mcp_enabled</c> will be seeded
    /// with. A network block beside <c>enabled = false</c> therefore describes an endpoint that will not start
    /// on the first run either — opening its port was never "ready for later", it was ready for nothing.
    /// <para>Review's second finding, and the reason the note offers that as a READING rather than a fact: a
    /// File origin is not "fresh install". Every store-read failure — BYO, a missing credential, a timeout —
    /// collapses into it, so this branch is also reached on a long-lived box whose store is merely unreachable
    /// this minute, where mcp.enabled may be a seed nobody has touched since <c>--enable-mcp</c> wrote the
    /// store. Asserting the endpoint is off there would contradict the sweep-declined line the same run prints
    /// a few lines later, which says the off may be stale and the rule may be the live one — and that line is
    /// the one that is right.</para>
    /// </summary>
    [Fact]
    public void PlanFirewallRules_FileSeedSaysTheSurfaceIsOff_OpensNothing_AndOffersThatAsAReadingNotAFact()
    {
        var config = ManagedConfig();
        config.Mcp.Enabled = false;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var mcp = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, storeUnavailableReason: "the store did not answer within 10 seconds"),
            p => p.Surface == "MCP");

        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, mcp.Action);
        Assert.NotNull(mcp.Note);

        /* Which plane it went on and why it could not do better — the shape DescribeFirewallPortAuthority
           already applies to the port, now applied to the flag. */
        Assert.Contains("could NOT be read", mcp.Note, StringComparison.Ordinal);
        Assert.Contains("the store did not answer within 10 seconds", mcp.Note, StringComparison.Ordinal);
        Assert.Contains("mcp.enabled = false", mcp.Note, StringComparison.Ordinal);

        /* BOTH readings, so neither box's operator is misled: right at install time... */
        Assert.Contains("SEEDED", mcp.Note, StringComparison.Ordinal);
        /* ...and possibly stale on one that has run before. */
        Assert.Contains("may be stale", mcp.Note, StringComparison.Ordinal);

        /* No port note: this plan opened nothing, so it made no port decision to disclose. */
        Assert.Null(mcp.PortNote);

        /* And it does NOT sweep — see the test below, which is the reason. */
        Assert.False(mcp.SweepOtherPorts);
    }

    /// <summary>The confirmed half stays a flat statement, because there it IS one: the control plane answered
    /// and said off, so nothing is being inferred from a file the enable path never writes. Pinned so the two
    /// wordings cannot be collapsed into one hedged message that under-states a certain answer.</summary>
    [Fact]
    public void PlanFirewallRules_ControlPlaneAnsweredOff_SaysSoWithoutAStalenessCaveat()
    {
        var config = ManagedConfig();
        config.Mcp.Enabled = true;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var note = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, mcpStore: (false, 5152)), p => p.Surface == "MCP").Note;

        Assert.Contains("the CONTROL PLANE has it off", note, StringComparison.Ordinal);
        Assert.DoesNotContain("may be stale", note, StringComparison.Ordinal);
        Assert.DoesNotContain("could NOT be read", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// Caught in review on the first cut of this change, and it is the same outage arriving through the other
    /// field. Enabling MCP is a store-only write by design (#2389) — <c>--enable-mcp</c> and the Viewer never
    /// write back to darling.json — so on a long-lived box the file's <c>mcp.enabled = false</c> is not a
    /// statement about anything, it is just the seed nobody edited. Read that as "off" on a run that could not
    /// reach the control plane, and the plan becomes a Remove; let a Remove sweep unconditionally, on the
    /// reasoning that "no rule on any port" needs no knowledge of which port is live, and the wildcard deletes
    /// the rule for the port the endpoint IS serving on. install-darling.ps1 runs this verb with the service
    /// stopped, so that is the upgrade path, not a corner.
    /// <para>The fix is not "never sweep on a fallback": whether a surface is exposed at ALL is
    /// <c>mcp.network</c>, which is file-only and which the control plane can only ever switch OFF. So the
    /// sweep is withheld for exactly the surfaces the file exposes, and the loopback cases below keep it.</para>
    /// </summary>
    [Fact]
    public void PlanFirewallRules_StoreUnreadable_DoesNotSweepAwayARuleTheControlPlaneMayBeServingOn()
    {
        var config = ManagedConfig();
        config.Mcp.Enabled = false;   /* the untouched seed — --enable-mcp wrote the store, not this */
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var mcp = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, storeUnavailableReason: "the store did not answer within 10 seconds"),
            p => p.Surface == "MCP");

        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, mcp.Action);
        Assert.False(mcp.SweepOtherPorts);

        /* The web twin has the identical shape and the identical hole. */
        var webConfig = ManagedConfig();
        webConfig.Web.Enabled = false;
        webConfig.Web.Network = new WebNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        Assert.False(
            Assert.Single(
                DarlingCliCommands.PlanFirewallRules(webConfig, storeUnavailableReason: "the store did not answer within 10 seconds"),
                p => p.Surface == "web dashboard")
                .SweepOtherPorts);
    }

    /// <summary>
    /// The other side of that fix, and the reason it is not simply "never sweep on a fallback". A surface the
    /// file does not expose on the LAN is loopback-only whatever the store would have said — <c>mcp.network</c>
    /// has no config_service equivalent, by the #2389 design, precisely so that exposure requires touching the
    /// host. So no rule belongs on any port, that conclusion needs nothing this run could not read, and the
    /// installer keeps collecting rules left behind by an exposure that was removed from darling.json.
    /// </summary>
    [Fact]
    public void PlanFirewallRules_ALoopbackSurfaceAlwaysSweeps_BecauseNetworkExposureIsFileOnly()
    {
        var plans = DarlingCliCommands.PlanFirewallRules(
            ManagedConfig(), storeUnavailableReason: "the store did not answer within 10 seconds");

        Assert.All(plans, p => Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, p.Action));
        Assert.All(plans, p => Assert.True(p.SweepOtherPorts));
    }

    /// <summary>
    /// The other #2432 residue, and the sharper half of it. The sweep removes this surface's rules on ports
    /// other than the one being opened — that is how a rule stranded by a port change gets collected, and its
    /// entire justification is that the sweeper KNOWS which port is live. A run that fell back to darling.json's
    /// seed does not: install-darling.ps1 calls this verb with the service stopped, and on a managed install the
    /// store is a child of the service, so on an upgrade of a box whose port was moved in the Viewer the rule
    /// matching the wildcard is the WORKING one. Sweeping it closes a live LAN surface — the elevated verb an
    /// operator was told to run becomes the outage.
    /// </summary>
    [Fact]
    public void PlanFirewallRules_StoreUnreadable_DoesNotSweepTheSurfacesOtherPorts()
    {
        var config = ManagedConfig();
        config.Mcp.Enabled = true;
        config.Mcp.Port = 5152;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var mcp = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, storeUnavailableReason: "the store did not answer within 10 seconds"),
            p => p.Surface == "MCP");

        /* The rule for the file's port is still created — that is the fresh-install case, where the file cannot
           be wrong — and the enable command removes its own exact DisplayName first, so declining the sweep
           costs nothing in idempotence. */
        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Open, mcp.Action);
        Assert.False(mcp.SweepOtherPorts);
    }

    /// <summary>
    /// And when the control plane DID answer, the sweep is authoritative again: the port is known, so every
    /// other rule on this surface really is stranded and collecting it is the #2414 fix working. Withholding it
    /// permanently would trade one residue for the one #2432 was filed to remove.
    /// </summary>
    [Fact]
    public void PlanFirewallRules_ControlPlaneAnswered_SweepsTheSurfacesOtherPorts()
    {
        var config = ManagedConfig();
        config.Mcp.Enabled = true;
        config.Mcp.Port = 5152;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };

        var mcp = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, mcpStore: (true, 5199)), p => p.Surface == "MCP");

        Assert.Equal(5199, mcp.Port);
        Assert.True(mcp.SweepOtherPorts);
    }

    /// <summary>
    /// The store surface never withholds the sweep, and that is not an oversight to be made consistent later:
    /// <c>postgres.port</c> is file-only, with no config_service column able to disagree with it, so this
    /// surface's port is never a guess even when the store cannot be read.
    /// </summary>
    [Fact]
    public void PlanFirewallRules_TheStoreSurfaceAlwaysSweeps_BecauseItsPortIsNeverAGuess()
    {
        var config = ManagedConfig();
        config.Postgres!.Network = new PostgresNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24" };

        var store = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(config, storeUnavailableReason: "the store did not answer within 10 seconds"),
            p => p.Surface == "store");

        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Open, store.Action);
        Assert.True(store.SweepOtherPorts);
    }

    /* ---- drift guards: the seams a rename or a deleted call would break silently ---- */

    [Fact]
    public void EveryRuleNameBuilder_CarriesTheSharedPrefix_ThatUninstallSweepsBy()
    {
        Assert.StartsWith(DarlingFirewallCheck.RuleNamePrefix, DarlingManagedPostgres.StoreFirewallRuleName(5641), StringComparison.Ordinal);
        Assert.StartsWith(DarlingFirewallCheck.RuleNamePrefix, DarlingMcpHostService.McpFirewallRuleName(5152), StringComparison.Ordinal);
        Assert.StartsWith(DarlingFirewallCheck.RuleNamePrefix, DarlingWebHostService.WebFirewallRuleName(5153), StringComparison.Ordinal);
    }

    [Fact]
    public void UninstallScript_SweepsByTheSharedPrefix()
    {
        /* Uninstall removes rules by "<prefix>*" so a rule from a PREVIOUS port is cleared too. That literal
           lives in PowerShell, where no compiler checks it against the C# builders — this is that check. */
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "uninstall-darling.ps1"));

        Assert.Contains($"Remove-NetFirewallRule -DisplayName '{DarlingFirewallCheck.RuleNamePrefix}*'", script, StringComparison.Ordinal);
    }

    [Fact]
    public void InstallScript_ActuallyInvokesTheFirewallVerb()
    {
        /* If the installer stops calling the verb, nothing creates the rules and #1771 is back — with the
           runtime no longer even attempting it either.
           This pins the INVOCATION, not a mention: the script also names --configure-firewall in its help
           block and in the "re-run this by hand" error message, so a `Contains("--configure-firewall")` stays
           green with the real call deleted. A mutation run caught exactly that, which is why the assertion is
           the exact invocation form plus proof the wrapper is called somewhere other than its own definition. */
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "install-darling.ps1"));

        Assert.Contains("& $serviceExe --configure-firewall", script, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2436: the THIRD call, and the one whose absence is silent. The pre-start reconcile runs with the
    /// service stopped, and on a managed install the store is a child of the service — so on an UPGRADE the
    /// verb has nothing to ask and falls back to darling.json's seed, which is the wrong port on a box whose
    /// port was later moved in the Viewer. This call is what asks the store once it can answer.
    /// <para>Pinned by its GUARD as well as its presence. Running it unconditionally would fire it during a
    /// fresh install's ~2-minute first start, where the store cannot answer either and the file port is
    /// correct by definition — a second copy of the fallback disclosure about a port that is not wrong. The
    /// guard is the difference between narrowing the window by construction and hoping the timing works
    /// out.</para>
    /// </summary>
    [Fact]
    public void InstallScript_ReReconcilesAfterTheFirstStart_ButOnlyOnAnUpgrade()
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "install-darling.ps1"));
        var branch = ExtractBracedBlock(script, "if ($isUpgrade) {");

        Assert.Contains("Invoke-FirewallReconcile", branch, StringComparison.Ordinal);

        /* And it is genuinely AFTER the start, or it is just the pre-start call with extra steps. */
        var start = script.IndexOf("Start-Service -Name $serviceName", StringComparison.Ordinal);
        var guard = script.IndexOf("if ($isUpgrade) {", StringComparison.Ordinal);
        Assert.True(start >= 0, "install-darling.ps1 no longer starts the service");
        Assert.True(guard > start, "the upgrade reconcile must run AFTER Start-Service, not before");
    }

    [Fact]
    public void InstallScript_ReReconcilesInsideTheNetworkWizardBranch()
    {
        /* The SECOND call, pinned by its REASON rather than by counting occurrences. -Network runs
           --configure-network AFTER the pre-start reconcile, and the wizard rewrites the very exposure that
           reconcile acted on: a newly exposed endpoint has no rule yet, and one turned back off has a stale
           one. Losing this call silently reproduces a subset of #1771 on every networked install.
           A count-based pin does not hold here. The design carries a definition plus TWO calls, so "at least
           two occurrences" stays GREEN with this one deleted - the exact hole a reviewer caught, and the same
           vacuous-pin shape as the earlier Contains() that matched the help text. */
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "install-darling.ps1"));
        var branch = ExtractBracedBlock(script, "if ($Network) {");

        Assert.Contains("--configure-network", branch, StringComparison.Ordinal);
        Assert.Contains("Invoke-FirewallReconcile", branch, StringComparison.Ordinal);
    }

    /// <summary>Returns the body of the first brace-balanced block introduced by <paramref name="header"/>.</summary>
    private static string ExtractBracedBlock(string script, string header)
    {
        var start = script.IndexOf(header, StringComparison.Ordinal);
        Assert.True(start >= 0, $"expected '{header}' in the script");

        var open = script.IndexOf('{', start);
        var depth = 0;
        for (var i = open; i < script.Length; i++)
        {
            if (script[i] == '{')
            {
                depth++;
            }
            else if (script[i] == '}' && --depth == 0)
            {
                return script[(open + 1)..i];
            }
        }

        Assert.Fail($"unbalanced braces after '{header}'");
        return "";
    }

    [Theory]
    [InlineData("DarlingMcpHostService.cs")]
    [InlineData("DarlingWebHostService.cs")]
    public void RuntimeHosts_VerifyTheFirewall_AndNeverWriteIt(string fileName)
    {
        /* The regression this whole change exists to prevent: a host that goes back to issuing the write it
           cannot perform, producing access-denied on every start of every hardened install. */
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", fileName));

        Assert.Contains("DarlingFirewallCheck.CheckAsync", source, StringComparison.Ordinal);
        Assert.DoesNotContain("BuildFirewallEnableCommand", source, StringComparison.Ordinal);
        Assert.DoesNotContain("BuildFirewallDisableCommand", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ReconcileFirewallAsync", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheStoreBootstrap_VerifiesTheFirewall_AndNeverWritesItAtRuntime()
    {
        /* DarlingManagedPostgres still OWNS the pure command builders (the elevated verb calls them), so this
           pins the runtime CALL SITE rather than the file's vocabulary: the bootstrap checks, and the old
           "try to configure it myself" entry point is gone. */
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedPostgres.cs"));

        Assert.Contains("CheckStoreFirewallAsync", source, StringComparison.Ordinal);
        Assert.DoesNotContain("TryConfigureStoreFirewallAsync", source, StringComparison.Ordinal);
    }

    [Fact]
    public void ConfigureFirewallVerb_IsDispatchable()
    {
        /* A verb missing from the allow-list is classified UnknownOption and never reaches its handler — the
           installer would then get "Unknown option" and exit non-zero on every install. */
        Assert.True(DarlingCliCommands.IsConfigureFirewallVerb("--configure-firewall"));
        Assert.True(DarlingCliCommands.IsConfigureFirewallVerb("--CONFIGURE-FIREWALL"));
        Assert.True(DarlingCliCommands.IsKnownVerb("--configure-firewall"));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(["--configure-firewall"]));
        Assert.Contains("--configure-firewall", DarlingCliCommands.UsageText(), StringComparison.Ordinal);
    }

    /* ---- #2445: what the NOT-elevated branch hands over ---- */

    /// <summary>
    /// The whole policy table, because the interesting cases are the two that print nothing and the one that
    /// prints a command WITHOUT failing the verb — none of which any single scenario test would show together.
    /// </summary>
    [Theory]
    /* An Open plan is unchanged by #2445: the rule has to exist with these exact parameters, which no probe
       could confirm, so it prints and fails exactly as it always has whatever the probe would have said. */
    [InlineData(DarlingCliCommands.FirewallRuleAction.Open, true, null, DarlingCliCommands.FirewallHandoff.OpenCommand)]
    [InlineData(DarlingCliCommands.FirewallRuleAction.Open, true, true, DarlingCliCommands.FirewallHandoff.OpenCommand)]
    [InlineData(DarlingCliCommands.FirewallRuleAction.Open, false, false, DarlingCliCommands.FirewallHandoff.OpenCommand)]
    /* The defect this issue is about: a Remove with a rule really sitting there now gets its own command. */
    [InlineData(DarlingCliCommands.FirewallRuleAction.Remove, true, true, DarlingCliCommands.FirewallHandoff.SweepCommand)]
    /* ...and the common case stays exactly as quiet as it was, because the probe MEASURED that. */
    [InlineData(DarlingCliCommands.FirewallRuleAction.Remove, true, false, DarlingCliCommands.FirewallHandoff.Nothing)]
    /* A probe that could not answer offers the command but is not evidence of drift. */
    [InlineData(DarlingCliCommands.FirewallRuleAction.Remove, true, null, DarlingCliCommands.FirewallHandoff.SweepCommandUnverified)]
    /* #2436's withheld sweep: nothing is handed over on any probe answer, including "a rule is there" — that
       rule may be the one the endpoint is being served on. */
    [InlineData(DarlingCliCommands.FirewallRuleAction.Remove, false, true, DarlingCliCommands.FirewallHandoff.Nothing)]
    [InlineData(DarlingCliCommands.FirewallRuleAction.Remove, false, false, DarlingCliCommands.FirewallHandoff.Nothing)]
    [InlineData(DarlingCliCommands.FirewallRuleAction.Remove, false, null, DarlingCliCommands.FirewallHandoff.Nothing)]
    public void ClassifyNotElevatedHandoff_IsTheWholeTable(
        DarlingCliCommands.FirewallRuleAction action,
        bool sweepOtherPorts,
        bool? rulesFound,
        DarlingCliCommands.FirewallHandoff expected)
    {
        Assert.Equal(expected, DarlingCliCommands.ClassifyNotElevatedHandoff(action, sweepOtherPorts, rulesFound));
    }

    /// <summary>
    /// The exit code is a claim about the FIREWALL. install-darling.ps1 prints a re-run banner on a non-zero
    /// from this verb, so an unverified sweep must not raise one: "the probe did not answer" is not drift, and
    /// a banner that fires without cause is how operators learn to ignore the one that has cause. Under-
    /// reporting is the safe direction here because the running service re-probes the same rule on every start.
    /// </summary>
    [Fact]
    public void NotElevatedRunHasWork_CountsMeasuredWorkOnly()
    {
        Assert.False(DarlingCliCommands.NotElevatedRunHasWork([]));
        Assert.False(DarlingCliCommands.NotElevatedRunHasWork([DarlingCliCommands.FirewallHandoff.Nothing]));
        Assert.False(DarlingCliCommands.NotElevatedRunHasWork([DarlingCliCommands.FirewallHandoff.SweepCommandUnverified]));

        Assert.True(DarlingCliCommands.NotElevatedRunHasWork([DarlingCliCommands.FirewallHandoff.SweepCommand]));
        Assert.True(DarlingCliCommands.NotElevatedRunHasWork([DarlingCliCommands.FirewallHandoff.OpenCommand]));

        /* Mixed: one measured removal among unverifiable siblings still means an elevated shell has work. */
        Assert.True(DarlingCliCommands.NotElevatedRunHasWork(
        [
            DarlingCliCommands.FirewallHandoff.Nothing,
            DarlingCliCommands.FirewallHandoff.SweepCommandUnverified,
            DarlingCliCommands.FirewallHandoff.SweepCommand,
        ]));
    }

    /// <summary>
    /// The wiring, against plans the real planner produced rather than against hand-built flags — this is the
    /// pairing that matters and the one an enum table alone cannot show. An admin who ran <c>--disable-mcp</c>
    /// and re-ran this verb unelevated gets the sweep command; the surface #2436 declines to sweep gets
    /// nothing, on the SAME probe answer.
    /// </summary>
    [Fact]
    public void TheHandoff_FollowsThePlannersSweepPermission_NotJustTheAction()
    {
        /* Control plane answered "off" for a fully LAN-configured MCP: a Remove that IS allowed to sweep. */
        var disabled = ManagedConfig();
        disabled.Mcp.Enabled = true;
        disabled.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };
        var disabledPlan = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(disabled, mcpStore: (false, 5152)), p => p.Surface == "MCP");

        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, disabledPlan.Action);
        Assert.True(disabledPlan.SweepOtherPorts);
        Assert.Equal(
            DarlingCliCommands.FirewallHandoff.SweepCommand,
            DarlingCliCommands.ClassifyNotElevatedHandoff(disabledPlan.Action, disabledPlan.SweepOtherPorts, rulesFound: true));

        /* Same shape, but the store could not be read, so the file's "off" may be years stale and the rule
           may be the live one. Handing that sweep to an operator would hand them the outage by copy-paste. */
        var unreadable = ManagedConfig();
        unreadable.Mcp.Enabled = false;
        unreadable.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };
        var withheldPlan = Assert.Single(
            DarlingCliCommands.PlanFirewallRules(unreadable, storeUnavailableReason: "the store did not answer within 10 seconds"),
            p => p.Surface == "MCP");

        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, withheldPlan.Action);
        Assert.False(withheldPlan.SweepOtherPorts);
        Assert.Equal(
            DarlingCliCommands.FirewallHandoff.Nothing,
            DarlingCliCommands.ClassifyNotElevatedHandoff(withheldPlan.Action, withheldPlan.SweepOtherPorts, rulesFound: true));
    }

    /// <summary>
    /// The probe has to ask EXACTLY what the sweep would act on. A narrower question reports "nothing to do"
    /// about rules the sweep would still collect; a wider one offers a command covering rules this product does
    /// not own. Both go through the same wildcard builder, which is what makes that true by construction — this
    /// pins that they still do.
    /// </summary>
    [Fact]
    public void TheProbeAsksExactlyWhatTheSweepWouldDelete()
    {
        var wildcard = DarlingFirewallCheck.SurfaceRuleWildcard(DarlingMcpHostService.McpFirewallRuleName(5152));
        Assert.Equal("PerformanceMonitor Darling MCP (port *)", wildcard);

        var probe = DarlingFirewallCheck.BuildProbeCommand(wildcard);
        var sweep = DarlingManagedPostgres.BuildFirewallSweepCommand(wildcard);

        Assert.Contains("-DisplayName 'PerformanceMonitor Darling MCP (port *)'", probe, StringComparison.Ordinal);
        Assert.Contains("-DisplayName 'PerformanceMonitor Darling MCP (port *)'", sweep, StringComparison.Ordinal);

        /* Still read-only with a wildcard in it — this is the branch that holds no privilege. */
        Assert.Contains("Get-NetFirewallRule", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("Remove-NetFirewallRule", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("New-NetFirewallRule", probe, StringComparison.Ordinal);
    }

    /* ---- #9788: the --configure-firewall verb itself, run against a recording shell ---- */

    private const string McpWildcard = "PerformanceMonitor Darling MCP (port *)";
    private const string WebWildcard = "PerformanceMonitor Darling Web (port *)";
    private const string StoreWildcard = "PerformanceMonitor Darling store (port *)";
    private const string StoreUnreadable = "the store did not answer within 10 seconds";

    /// <summary>
    /// The silent-and-total bug: <c>sweep + "; " + enable</c> exits at the sweep's <c>exit 0</c>, so the rule is
    /// never created while the verb still reports success. Observed at the only place it is visible, the commands
    /// the verb hands to PowerShell: the sweep is a command of its own, the open is a command of its own, and no
    /// single command carries both.
    /// </summary>
    [Fact]
    public async Task ConfigureFirewall_Elevated_RunsTheSweepAndTheOpenAsSeparateCommands_SweepFirst()
    {
        var config = ExposedMcpConfig(enabled: true);
        var shell = new RecordingShell { Mcp = (true, 5199), Web = (true, 5153) };

        var run = await RunVerbAsync(config, shell);

        var sweep = DarlingManagedPostgres.BuildFirewallSweepCommand(McpWildcard);
        var open = DarlingManagedPostgres.BuildFirewallEnableCommand("PerformanceMonitor Darling MCP (port 5199)", 5199, "192.168.1.0/24");

        Assert.Equal(0, run.ExitCode);
        Assert.Empty(run.Error);
        Assert.Contains(sweep, shell.Commands);
        Assert.Contains(open, shell.Commands);
        Assert.True(shell.Commands.IndexOf(sweep) < shell.Commands.IndexOf(open), "the sweep must run before the open");
        Assert.DoesNotContain(shell.Commands, c => c.Contains(sweep, StringComparison.Ordinal) && c.Contains("New-NetFirewallRule", StringComparison.Ordinal));
    }

    /// <summary>
    /// #2414 through the verb: the rule is named for the port the control plane answered with, never for the
    /// port darling.json carries. The files say 5152 and 5153, the store says 5199 and 5188, and every command
    /// the verb runs names the store's ports.
    /// </summary>
    [Fact]
    public async Task ConfigureFirewall_NamesTheRulesForTheStoresPorts_NotDarlingJsonsPorts()
    {
        var config = ManagedConfig();
        config.Mcp.Enabled = true;
        config.Mcp.Port = 5152;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };
        config.Web.Enabled = true;
        config.Web.Port = 5153;
        config.Web.Network = new WebNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };
        var shell = new RecordingShell { Mcp = (true, 5199), Web = (true, 5188) };

        var run = await RunVerbAsync(config, shell);

        Assert.Equal(0, run.ExitCode);
        Assert.Contains(DarlingManagedPostgres.BuildFirewallEnableCommand(DarlingMcpHostService.McpFirewallRuleName(5199), 5199, "192.168.1.0/24"), shell.Commands);
        Assert.Contains(DarlingManagedPostgres.BuildFirewallEnableCommand(DarlingWebHostService.WebFirewallRuleName(5188), 5188, "192.168.1.0/24"), shell.Commands);
        Assert.DoesNotContain(shell.Commands, c => c.Contains("(port 5152)", StringComparison.Ordinal) || c.Contains("(port 5153)", StringComparison.Ordinal));
    }

    /// <summary>
    /// The security half of #2414: a rule stranded on an old port is reachable only by a per-surface wildcard.
    /// A loopback config wants no rule at all, so the verb runs exactly one wildcard sweep per surface and
    /// nothing that names a single port.
    /// </summary>
    [Fact]
    public async Task ConfigureFirewall_Elevated_ClearsEachSurfaceByItsWildcard_NeverByOneExactName()
    {
        var shell = new RecordingShell { Mcp = (true, 5199), Web = (true, 5188) };

        var run = await RunVerbAsync(ManagedConfig(), shell);

        Assert.Equal(0, run.ExitCode);
        Assert.Equal(
            new[] { McpWildcard, WebWildcard, StoreWildcard }.Select(DarlingManagedPostgres.BuildFirewallSweepCommand).OrderBy(c => c, StringComparer.Ordinal),
            shell.Commands.OrderBy(c => c, StringComparer.Ordinal));
    }

    /// <summary>
    /// #2436 through the verb: a surface darling.json exposes, on a run that could not read the control plane,
    /// is not swept. Its file-seeded "off" may be stale and the rule on some other port may be the live one, so
    /// the verb runs no command that names the surface. The loopback surfaces, whose answer needs nothing the
    /// run could not read, are still swept.
    /// </summary>
    [Fact]
    public async Task ConfigureFirewall_StoreUnreadable_RunsNoCommandForAnExposedSurfaceItMayNotSweep()
    {
        var config = ExposedMcpConfig(enabled: false);
        var shell = new RecordingShell { Unavailable = StoreUnreadable };

        var run = await RunVerbAsync(config, shell);

        Assert.Equal(0, run.ExitCode);
        Assert.DoesNotContain(shell.Commands, c => c.Contains("Darling MCP", StringComparison.Ordinal));
        Assert.Contains(DarlingManagedPostgres.BuildFirewallSweepCommand(WebWildcard), shell.Commands);
        Assert.Contains(DarlingManagedPostgres.BuildFirewallSweepCommand(StoreWildcard), shell.Commands);
    }

    /// <summary>The Open half of the same gate: the file's rule is still created on a fresh install, and the
    /// sweep of the surface's other ports waits for a run that can see the control plane.</summary>
    [Fact]
    public async Task ConfigureFirewall_StoreUnreadable_OpensTheFilesRuleButDoesNotSweepTheSurfacesOtherPorts()
    {
        var config = ExposedMcpConfig(enabled: true);
        var shell = new RecordingShell { Unavailable = StoreUnreadable };

        var run = await RunVerbAsync(config, shell);

        Assert.Equal(0, run.ExitCode);
        Assert.Contains(
            DarlingManagedPostgres.BuildFirewallEnableCommand(DarlingMcpHostService.McpFirewallRuleName(config.Mcp.Port), config.Mcp.Port, "192.168.1.0/24"),
            shell.Commands);
        Assert.DoesNotContain(DarlingManagedPostgres.BuildFirewallSweepCommand(McpWildcard), shell.Commands);
    }

    /// <summary>A failed step is reported with the exact command to run by hand, and the verb fails.</summary>
    [Fact]
    public async Task ConfigureFirewall_Elevated_AFailedStepFailsTheVerbAndPrintsTheCommand()
    {
        var config = ExposedMcpConfig(enabled: true);
        var open = DarlingManagedPostgres.BuildFirewallEnableCommand(
            DarlingMcpHostService.McpFirewallRuleName(config.Mcp.Port), config.Mcp.Port, "192.168.1.0/24");
        var shell = new RecordingShell { Mcp = (true, config.Mcp.Port), Answer = c => c == open ? (1, "Access is denied") : (0, "") };

        var run = await RunVerbAsync(config, shell);

        Assert.Equal(1, run.ExitCode);
        Assert.Contains("Access is denied", run.Error, StringComparison.Ordinal);
        Assert.Contains(open, run.Error, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2445: a not-elevated run MEASURES the removal half. Where the control plane answered "off" and the
    /// read-only probe finds a rule, the verb hands over the sweep command and fails, and everything it ran
    /// was a read.
    /// </summary>
    [Fact]
    public async Task ConfigureFirewall_NotElevated_AStaleRuleIsHandedOverAsASweepAndFailsTheVerb()
    {
        var config = ExposedMcpConfig(enabled: true);
        var shell = new RecordingShell
        {
            Elevated = false,
            Mcp = (false, 5152),
            Answer = c => (0, c.Contains(McpWildcard, StringComparison.Ordinal) ? "DarlingFirewallProbe=1" : "DarlingFirewallProbe=0"),
        };

        var run = await RunVerbAsync(config, shell);

        Assert.Equal(1, run.ExitCode);
        Assert.Contains(DarlingManagedPostgres.BuildFirewallSweepCommand(McpWildcard), run.Error, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingManagedPostgres.BuildFirewallSweepCommand(WebWildcard), run.Error, StringComparison.Ordinal);
        AssertOnlyReads(shell);
    }

    /// <summary>The common case: every surface loopback, nothing found. No command is offered, nothing is
    /// written to the error stream, and the verb succeeds, exactly as a default install needs.</summary>
    [Fact]
    public async Task ConfigureFirewall_NotElevated_ACleanBoxHandsOverNothingAndSucceeds()
    {
        var shell = new RecordingShell { Elevated = false, Answer = _ => (0, "DarlingFirewallProbe=0") };

        var run = await RunVerbAsync(ManagedConfig(), shell);

        Assert.Equal(0, run.ExitCode);
        Assert.Empty(run.Error);
        Assert.DoesNotContain("NetFirewallRule", run.Output, StringComparison.Ordinal);
        Assert.Equal(3, shell.Commands.Count);
        AssertOnlyReads(shell);
    }

    /// <summary>A probe that cannot answer is not evidence of drift: the sweep is offered as a precaution on
    /// stdout, the error stream stays empty, and the verb returns 0.</summary>
    [Fact]
    public async Task ConfigureFirewall_NotElevated_AProbeThatCannotAnswerOffersTheSweepButDoesNotFailTheVerb()
    {
        var shell = new RecordingShell { Elevated = false, Answer = _ => (1, "powershell could not run") };

        var run = await RunVerbAsync(ManagedConfig(), shell);

        Assert.Equal(0, run.ExitCode);
        Assert.Empty(run.Error);
        Assert.Contains(DarlingManagedPostgres.BuildFirewallSweepCommand(McpWildcard), run.Output, StringComparison.Ordinal);
        AssertOnlyReads(shell);
    }

    /// <summary>An Open plan is never probed (a probe could not say the rule has the right parameters): the enable
    /// command is handed over, the verb fails, and the surface's wildcard is not asked about.</summary>
    [Fact]
    public async Task ConfigureFirewall_NotElevated_AnOpenPlanIsHandedOverWithoutBeingProbed()
    {
        var config = ExposedMcpConfig(enabled: true);
        var shell = new RecordingShell { Elevated = false, Mcp = (true, 5199), Answer = _ => (0, "DarlingFirewallProbe=0") };

        var run = await RunVerbAsync(config, shell);

        Assert.Equal(1, run.ExitCode);
        Assert.Contains(
            DarlingManagedPostgres.BuildFirewallEnableCommand(DarlingMcpHostService.McpFirewallRuleName(5199), 5199, "192.168.1.0/24"),
            run.Error,
            StringComparison.Ordinal);
        Assert.DoesNotContain(shell.Commands, c => c.Contains("Darling MCP", StringComparison.Ordinal));
        AssertOnlyReads(shell);
    }

    /// <summary>A sweep the run is not entitled to make is neither probed nor printed, even when a rule is
    /// there: that rule may be the one the endpoint is being served on.</summary>
    [Fact]
    public async Task ConfigureFirewall_NotElevated_AWithheldSweepIsNeitherProbedNorHandedOver()
    {
        var config = ExposedMcpConfig(enabled: false);
        var shell = new RecordingShell { Elevated = false, Unavailable = StoreUnreadable, Answer = _ => (0, "DarlingFirewallProbe=1") };

        var run = await RunVerbAsync(config, shell);

        Assert.DoesNotContain(shell.Commands, c => c.Contains("Darling MCP", StringComparison.Ordinal));
        Assert.DoesNotContain(DarlingManagedPostgres.BuildFirewallSweepCommand(McpWildcard), run.Output + run.Error, StringComparison.Ordinal);
        Assert.Contains(DarlingManagedPostgres.BuildFirewallSweepCommand(WebWildcard), run.Error, StringComparison.Ordinal);
        AssertOnlyReads(shell);
    }

    /// <summary>Nothing a not-elevated run executes may write to the firewall.</summary>
    private static void AssertOnlyReads(RecordingShell shell)
    {
        Assert.All(shell.Commands, c =>
        {
            Assert.Contains("Get-NetFirewallRule", c, StringComparison.Ordinal);
            Assert.DoesNotContain("New-NetFirewallRule", c, StringComparison.Ordinal);
            Assert.DoesNotContain("Remove-NetFirewallRule", c, StringComparison.Ordinal);
            Assert.DoesNotContain("Set-NetFirewallRule", c, StringComparison.Ordinal);
        });
    }

    private static async Task<(int ExitCode, string Output, string Error)> RunVerbAsync(DarlingConfig config, RecordingShell shell)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exitCode = await DarlingCliCommands.ConfigureFirewallAsync(config, output, error, shell.AsShell(), CancellationToken.None);
        return (exitCode, output.ToString(), error.ToString());
    }

    /// <summary>The machine as the verb sees it: it records every PowerShell command it is asked to run and
    /// answers from <see cref="Answer"/>, and it reports the control plane's toggles as given.</summary>
    private sealed class RecordingShell
    {
        public bool Elevated { get; init; } = true;

        public (bool Enabled, int Port)? Mcp { get; init; }

        public (bool Enabled, int Port)? Web { get; init; }

        public string? Unavailable { get; init; }

        public Func<string, (int ExitCode, string Output)> Answer { get; init; } = _ => (0, "");

        public System.Collections.Generic.List<string> Commands { get; } = [];

        public DarlingCliCommands.FirewallShell AsShell() => new()
        {
            IsElevated = () => Elevated,
            RunPowerShell = (command, _) =>
            {
                Commands.Add(command);
                return Task.FromResult(Answer(command));
            },
            ReadEndpointToggles = (_, _) => Task.FromResult((Mcp, Web, Unavailable)),
        };
    }

    /// <summary>A managed config whose MCP endpoint is exposed on the LAN, with its enabled seed as given.</summary>
    private static DarlingConfig ExposedMcpConfig(bool enabled)
    {
        var config = ManagedConfig();
        config.Mcp.Enabled = enabled;
        config.Mcp.Port = 5152;
        config.Mcp.Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = "t" };
        return config;
    }

    /* ---- helpers ---- */

    /// <summary>A managed, fully loopback-only config — the default install, and the baseline every exposure
    /// case above mutates one field of.</summary>
    private static DarlingConfig ManagedConfig()
    {
        var config = new DarlingConfig();
        config.Postgres!.Managed = true;
        return config;
    }

    private static System.Collections.Generic.IEnumerable<DarlingCliCommands.FirewallRulePlan> PlansFor(DarlingConfig config, string surface)
        => DarlingCliCommands.PlanFirewallRules(config).Where(p => p.Surface == surface);
}
