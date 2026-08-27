/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Reprinting an endpoint's access token (#2479, item 2).
///
/// <para><b>The gap.</b> <c>--configure-network</c> shows each generated token's plaintext exactly once.
/// Lose it and the only path was regeneration, which invalidates every client already configured against
/// it — so one mislaid token during UAT meant re-onboarding every consumer of that endpoint. An
/// unrecoverable mistake made out of a recoverable one.</para>
///
/// <para><b>What it discloses, and what the elevation gate is not.</b> Nothing new. The token is a
/// <c>DarlingSecrets</c> blob: DPAPI at <c>LocalMachine</c> scope, with an entropy constant compiled into
/// an open-source binary — so any process on the box can decrypt it, and the entropy is not a secret
/// because it is published. And <c>darling.json</c> deliberately grants INTERACTIVE read, because the
/// Viewer and these very CLI verbs are run by the interactive operator. So an ordinary logged-on
/// NON-administrator can already read and decrypt the blob in four lines of PowerShell. The gate is worth
/// having for what it actually does — consistency with the other "(run elevated)" endpoint verbs, and
/// making a reprint a deliberate act rather than something a script picks up in passing — and the refusal
/// message says so rather than implying a boundary that is not there. The token's protection is the file's
/// ACL, and it always was.</para>
/// </summary>
public class DarlingPrintEndpointTokenTests
{
    [Theory]
    [InlineData("--print-mcp-token", true)]
    [InlineData("--PRINT-MCP-TOKEN", true)]
    [InlineData("--print-web-token", false)]
    [InlineData("--print-viewer-connection", false)]
    [InlineData("--nonsense", false)]
    public void IsPrintMcpTokenVerb_RecognizesOnlyItsOwnVerb_CaseInsensitively(string arg, bool expected) =>
        Assert.Equal(expected, DarlingCliCommands.IsPrintMcpTokenVerb(arg));

    [Theory]
    [InlineData("--print-web-token", true)]
    [InlineData("--PRINT-WEB-TOKEN", true)]
    [InlineData("--print-mcp-token", false)]
    [InlineData("--nonsense", false)]
    public void IsPrintWebTokenVerb_RecognizesOnlyItsOwnVerb_CaseInsensitively(string arg, bool expected) =>
        Assert.Equal(expected, DarlingCliCommands.IsPrintWebTokenVerb(arg));

    /// <summary>The #1581 trap: a verb with a dispatch block and help text that <c>IsKnownVerb</c> never
    /// learned classifies as an unknown option, and its dispatch is unreachable.</summary>
    [Fact]
    public void IsKnownVerb_ReachesBothTokenVerbs()
    {
        Assert.True(DarlingCliCommands.IsKnownVerb("--print-mcp-token"));
        Assert.True(DarlingCliCommands.IsKnownVerb("--print-web-token"));
    }

    /// <summary>Both are listed, and both carry the elevated marker the other endpoint verbs use — an
    /// operator should not have to discover the requirement by being refused.</summary>
    [Fact]
    public void UsageText_ListsBothVerbs_AndMarksThemElevated()
    {
        var usage = DarlingCliCommands.UsageText();

        foreach (var verb in new[] { "--print-mcp-token", "--print-web-token" })
        {
            var at = usage.IndexOf(verb, StringComparison.Ordinal);
            Assert.True(at >= 0, $"{verb} is not in the help text");

            var line = usage[at..];
            var end = line.IndexOf('\n');
            line = end > 0 ? line[..end] : line;
            Assert.Contains("run elevated", line, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Not elevated: refuse, print nothing to STDOUT, and be honest about what the gate is.
    ///
    /// <para>The empty STDOUT is the assertion that matters. A refusal that still writes the token is not a
    /// refusal, and a redirect (<c>… &gt; token.txt</c>) would capture it while the operator read the
    /// error on their terminal and believed nothing had happened.</para>
    /// </summary>
    [Fact]
    public void NotElevated_RefusesWithoutPrintingAnything_AndDoesNotOverclaimTheGate()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = DarlingCliCommands.PrintEndpointToken(
            "mcp", "MCP bearer", "--print-mcp-token", "clients send it as a header",
            elevated: false, configured: true, () => "s3cret-token-value", output, error);

        Assert.Equal(1, exit);
        Assert.Equal(string.Empty, output.ToString());
        Assert.DoesNotContain("s3cret-token-value", error.ToString(), StringComparison.Ordinal);

        var message = error.ToString();
        Assert.Contains("ELEVATED", message, StringComparison.Ordinal);

        /* The honesty clause. If this ever starts reading as "the token is protected by elevation", the
           operator stops guarding the thing that actually protects it. */
        Assert.Contains("INTERACTIVE read", message, StringComparison.Ordinal);
        Assert.Contains("LocalMachine", message, StringComparison.Ordinal);
        Assert.Contains("ACL", message, StringComparison.Ordinal);
    }

    /// <summary>Elevated with nothing configured: say there is no token and name the verb that makes one,
    /// rather than printing a blank line that reads as an empty token.</summary>
    [Fact]
    public void Elevated_WithNoNetworkBlock_SaysSo_AndPrintsNothing()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = DarlingCliCommands.PrintEndpointToken(
            "web", "web dashboard access", "--print-web-token", "presented via ?token=",
            elevated: true, configured: false, () => null, output, error);

        Assert.Equal(1, exit);
        Assert.Equal(string.Empty, output.ToString());
        Assert.Contains("web.network", error.ToString(), StringComparison.Ordinal);
        Assert.Contains("--configure-network", error.ToString(), StringComparison.Ordinal);
    }

    /// <summary>A configured block whose token resolves empty is not a token — printing a blank line would
    /// have the operator paste nothing into a client and wonder why it is refused.</summary>
    [Fact]
    public void Elevated_WithAnEmptyToken_RefusesRatherThanPrintingABlankLine()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = DarlingCliCommands.PrintEndpointToken(
            "mcp", "MCP bearer", "--print-mcp-token", "clients send it as a header",
            elevated: true, configured: true, () => "", output, error);

        Assert.Equal(1, exit);
        Assert.Equal(string.Empty, output.ToString());
    }

    /// <summary>
    /// A DPAPI blob only decrypts on the machine that produced it, so a darling.json copied from another
    /// host carries a token this box can never read. Saying that is the difference between a five-minute
    /// fix and an afternoon.
    /// </summary>
    [Fact]
    public void Elevated_WhenTheBlobCannotBeDecrypted_ExplainsWhy_AndPrintsNothing()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = DarlingCliCommands.PrintEndpointToken(
            "mcp", "MCP bearer", "--print-mcp-token", "clients send it as a header",
            elevated: true, configured: true,
            () => throw new System.Security.Cryptography.CryptographicException("Key not valid for use in specified state."),
            output, error);

        Assert.Equal(1, exit);
        Assert.Equal(string.Empty, output.ToString());
        Assert.Contains("only decrypts on the machine that produced it", error.ToString(), StringComparison.Ordinal);
        Assert.Contains("--configure-network", error.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// A NON-DPAPI resolve failure must not be diagnosed as a DPAPI one.
    ///
    /// <para>Review catch on #2479. <c>ResolveToken</c> also resolves <c>env:</c> and <c>file:</c> secret
    /// references, and those fail with an <see cref="InvalidOperationException"/> whose message already
    /// names the missing variable or the unreadable path. Appending "a DPAPI blob only decrypts on the
    /// machine that produced it — regenerate it with --configure-network" to THOSE is a false diagnosis
    /// pointing at a destructive fix: regenerating invalidates every client configured against the token,
    /// when the actual repair is to set the environment variable or correct the path. The token is fine.</para>
    /// </summary>
    [Fact]
    public void Elevated_WhenAnEnvOrFileReferenceFails_DoesNotBlameDpapi_OrSuggestRegenerating()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = DarlingCliCommands.PrintEndpointToken(
            "mcp", "MCP bearer", "--print-mcp-token", "clients send it as a header",
            elevated: true, configured: true,
            () => throw new InvalidOperationException(
                "mcp.network.token references environment variable 'DARLING_MCP_TOKEN', which is not set (or empty)."),
            output, error);

        var message = error.ToString();

        Assert.Equal(1, exit);
        Assert.Equal(string.Empty, output.ToString());

        /* The real cause survives... */
        Assert.Contains("DARLING_MCP_TOKEN", message, StringComparison.Ordinal);

        /* ...and the DPAPI story, with its destructive remedy, does not appear at all. */
        Assert.DoesNotContain("DPAPI", message, StringComparison.Ordinal);
        Assert.DoesNotContain("--configure-network", message, StringComparison.Ordinal);
        Assert.Contains("does not need regenerating", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The success path, and the split that makes it usable: STDOUT is the token and NOTHING else, so
    /// <c>… &gt; token.txt</c> and <c>… | clip</c> both produce a pasteable value; every warning is on
    /// STDERR, so the redirect cannot swallow it; and the token appears on STDERR nowhere at all.
    /// </summary>
    [Fact]
    public void Elevated_PrintsTheTokenOnStdoutAlone_WithEveryWarningOnStderr()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = DarlingCliCommands.PrintEndpointToken(
            "mcp", "MCP bearer", "--print-mcp-token", "Authorization: Bearer <token>",
            elevated: true, configured: true, () => "abc123TOKEN", output, error);

        Assert.Equal(0, exit);
        Assert.Equal("abc123TOKEN" + Environment.NewLine, output.ToString());

        var warnings = error.ToString();
        Assert.DoesNotContain("abc123TOKEN", warnings, StringComparison.Ordinal);
        Assert.Contains("WARNING", warnings, StringComparison.Ordinal);
        Assert.Contains("--print-mcp-token > token.txt", warnings, StringComparison.Ordinal);
        Assert.Contains("| clip", warnings, StringComparison.Ordinal);

        /* Reprinting is not the answer to a LEAK, and the one moment an operator reads this text is the
           moment that distinction matters. */
        Assert.Contains("LEAKED", warnings, StringComparison.Ordinal);
        Assert.Contains("--configure-network", warnings, StringComparison.Ordinal);
    }

    /// <summary>Both verbs are dispatched from Program.cs behind the Windows guard the DPAPI material
    /// requires — a verb that is classified but never dispatched is #1581 the other way round.</summary>
    [Theory]
    [InlineData("IsPrintMcpTokenVerb", "PrintMcpToken", "--print-mcp-token requires Windows")]
    [InlineData("IsPrintWebTokenVerb", "PrintWebToken", "--print-web-token requires Windows")]
    public void ProgramDispatchesBothVerbs_BehindTheWindowsGuard(string classifier, string entryPoint, string guard)
    {
        var source = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Program.cs"));

        Assert.Contains($"DarlingCliCommands.{classifier}(args[0])", source, StringComparison.Ordinal);
        Assert.Contains($"DarlingCliCommands.{entryPoint}(", source, StringComparison.Ordinal);
        Assert.Contains(guard, source, StringComparison.Ordinal);
    }

    private static string ReadSource(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(dir is null, $"could not locate {relative} from the test source path");
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}

/// <summary>
/// The network block's lifetime, said out loud at every start (#2479, item 6).
///
/// <para><b>The trap.</b> <c>mcp.network</c> / <c>web.network</c> are loaded ONCE and held for the process
/// lifetime by design — exposure should require touching the host, and the DPAPI token is LocalMachine-scoped
/// so it could not live in the store anyway. That design is right and is not changing. What it produced was
/// "I enabled it and it is still loopback-bound" in another costume, because nothing said the value froze
/// when the process started.</para>
///
/// <para>#2389/#2411 fixed the seed-versus-store confusion by making the split VISIBLE rather than by
/// removing it, and #2414 did the same for the firewall port. This is that treatment applied to the third
/// face of the same object — no hot reload, just an endpoint that states what it read and when it can
/// change.</para>
/// </summary>
public class DarlingNetworkBlockLifetimeTests
{
    [Fact]
    public void TheThreeStates_ReadDifferently_AndEachNamesTheKeyAndTheRestart()
    {
        var absent = DarlingHostBinding.DescribeNetworkBlockLifetime("mcp", "MCP", blockConfigured: false, exposed: false, null, null);
        var loopback = DarlingHostBinding.DescribeNetworkBlockLifetime("mcp", "MCP", blockConfigured: true, exposed: false, null, null);
        var exposed = DarlingHostBinding.DescribeNetworkBlockLifetime("mcp", "MCP", blockConfigured: true, exposed: true, "10.0.0.7", "10.0.0.0/24");

        Assert.NotEqual(absent, loopback);
        Assert.NotEqual(loopback, exposed);
        Assert.NotEqual(absent, exposed);

        foreach (var report in new[] { absent, loopback, exposed })
        {
            Assert.Contains("mcp.network", report, StringComparison.Ordinal);
            Assert.Contains("RESTART", report, StringComparison.Ordinal);

            /* And the other half, or "cannot change until restart" reads as "this endpoint is immovable" —
               which is false, and would have an operator restart the service to do something the control
               plane already does live. */
            Assert.Contains("config.config_service.mcp_enabled", report, StringComparison.Ordinal);
            Assert.Contains("--enable-mcp", report, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Only the state that IS a call to action gets one. An endpoint already serving the LAN exactly as
    /// configured is the likeliest state, and telling that operator to restart is where a start-up line
    /// spends its credibility — the same reasoning that made <c>DescribeToggleOverride</c>'s consequence
    /// clause state-specific.
    /// </summary>
    [Fact]
    public void OnlyTheLoopbackWithABlock_TellsYouToRestart()
    {
        var exposed = DarlingHostBinding.DescribeNetworkBlockLifetime("web", "Web dashboard", true, true, "10.0.0.7", "10.0.0.0/24");
        var loopback = DarlingHostBinding.DescribeNetworkBlockLifetime("web", "Web dashboard", true, false, null, null);
        var absent = DarlingHostBinding.DescribeNetworkBlockLifetime("web", "Web dashboard", false, false, null, null);

        Assert.Contains("restart the service", loopback, StringComparison.Ordinal);
        Assert.DoesNotContain("restart the service", exposed, StringComparison.Ordinal);
        Assert.DoesNotContain("restart the service", absent, StringComparison.Ordinal);

        /* The loopback-with-a-block state must also point at the reason already logged rather than
           repeating it in different words. */
        Assert.Contains("why the exposure was refused", loopback, StringComparison.Ordinal);
    }

    /// <summary>The exposed line quotes what this process is actually bound to, so an operator comparing it
    /// against the file can see at a glance whether the file has moved underneath it.</summary>
    [Fact]
    public void TheExposedLine_QuotesTheAddressAndCidrItIsHolding()
    {
        var report = DarlingHostBinding.DescribeNetworkBlockLifetime("mcp", "MCP", true, true, "10.197.53.214", "10.197.0.0/16");

        Assert.Contains("10.197.53.214", report, StringComparison.Ordinal);
        Assert.Contains("10.197.0.0/16", report, StringComparison.Ordinal);
        Assert.Contains("FIXED for the lifetime of this process", report, StringComparison.Ordinal);
    }

    /// <summary>The section name threads through everything, so the MCP and web wordings cannot drift —
    /// the same reason <c>DescribeToggleOverride</c> takes one rather than existing twice.</summary>
    [Fact]
    public void TheSectionName_ThreadsThroughEveryKeyItNames()
    {
        var web = DarlingHostBinding.DescribeNetworkBlockLifetime("web", "Web dashboard", true, false, null, null);

        Assert.Contains("web.network", web, StringComparison.Ordinal);
        Assert.Contains("config.config_service.web_enabled", web, StringComparison.Ordinal);
        Assert.Contains("--enable-web", web, StringComparison.Ordinal);
        Assert.DoesNotContain("mcp", web, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both hosts say it, and say it in BOTH bind modes. The loopback start line was the one that never
    /// mentioned the network block at all, and loopback-when-you-expected-LAN is precisely the state being
    /// diagnosed — so a call sitting inside the network-mode branch would miss every operator who needs it.
    /// </summary>
    [Theory]
    [InlineData("DarlingMcpHostService.cs")]
    [InlineData("DarlingWebHostService.cs")]
    public void EachHost_ReportsTheLifetime_InBothBindModes(string fileName)
    {
        var source = ReadHostSource(fileName);

        var call = source.IndexOf("DarlingHostBinding.DescribeNetworkBlockLifetime(", StringComparison.Ordinal);
        Assert.True(call >= 0, $"{fileName} no longer states its network block's lifetime at start-up (#2479)");

        /* It must sit AFTER the mode-specific start lines rather than inside either of them. The tell is
           that the nearest enclosing brace context is the start-up block, not the if/else — asserted here
           the cheap way: the call is after BOTH branches' log statements. */
        var loopbackLine = source.IndexOf("(loopback only)", StringComparison.Ordinal);
        var exposedLine = source.IndexOf("loopback also bound", StringComparison.Ordinal);

        Assert.True(loopbackLine >= 0, $"{fileName}'s loopback start line is gone — this pin needs rewriting");
        Assert.True(exposedLine >= 0, $"{fileName}'s LAN start line is gone — this pin needs rewriting");
        Assert.True(call > loopbackLine, $"{fileName} states the lifetime only in network mode — the loopback case is the one that needed it");
        Assert.True(call > exposedLine, $"{fileName} states the lifetime before its LAN start line");
    }

    /// <summary>
    /// The lifetime report reads the network block NULL-SAFELY.
    ///
    /// <para>Review catch on #2479, and the exact shape of defect this project keeps finding: a call site
    /// that never learned a concept the code around it already knew. <c>config.Mcp.Network</c> is
    /// <c>McpNetworkConfig?</c> and is null on the DEFAULT secure config — no <c>mcp.network</c> block at
    /// all — which is precisely the state this line exists to describe. The dereferences inside
    /// <c>if (networkMode)</c> are safe because the bind resolution has already proven a block exists;
    /// this one runs unconditionally. A bare <c>.IsConfigured</c> throws on every start of an un-exposed
    /// server, is swallowed by the surrounding catch, and retry-fails forever because the config never
    /// changes between attempts — so the feature that explains loopback-only would have been the thing
    /// keeping the endpoint from starting at all.</para>
    /// </summary>
    [Theory]
    [InlineData("DarlingMcpHostService.cs", "config.Mcp.Network")]
    [InlineData("DarlingWebHostService.cs", "config.Web.Network")]
    public void TheLifetimeReport_ReadsTheNetworkBlockNullSafely(string fileName, string accessor)
    {
        var source = ReadHostSource(fileName);

        var call = source.IndexOf("DarlingHostBinding.DescribeNetworkBlockLifetime(", StringComparison.Ordinal);
        Assert.True(call >= 0, $"{fileName} no longer states its network block's lifetime at start-up (#2479)");

        var arguments = source[call..Math.Min(source.Length, call + 400)];
        Assert.Contains($"{accessor}?.IsConfigured ?? false", arguments, StringComparison.Ordinal);
    }

    private static string ReadHostSource(string fileName, [CallerFilePath] string thisFile = "")
    {
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", fileName);
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(dir is null, $"could not locate {relative} from the test source path");
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
