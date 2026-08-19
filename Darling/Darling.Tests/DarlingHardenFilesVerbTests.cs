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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2352: <c>--harden-files</c> — the actor that can actually apply the secret-file ACLs.
///
/// <para><b>Why the verb exists.</b> The service already computes the correct DACL
/// (<c>DarlingFileSecurity.HardenFile</c>) and already detects when the real one is wrong
/// (<c>IsReadableByOrdinaryUsers</c>). What it lacks is authority: re-ACLing a file it does not own needs
/// WRITE_DAC, and taking ownership needs a privilege a virtual service account is not granted. So it logs the
/// remedy and continues — correctly, since a monitoring service must not refuse to monitor over a permissions
/// problem — and until now the only thing that ever APPLIED the rule to an existing install was
/// <c>install-darling.ps1</c>. A box registered by hand through the README's own <c>sc create</c> path left the
/// operator typing three <c>icacls</c> lines out of a log message.</para>
///
/// <para>The ACL work itself is Windows-only and needs a real filesystem, so what is pinned here is everything
/// that decides whether the verb is REACHABLE and honest — the failure mode that shipped once already (#1912),
/// where a verb had a full dispatch block, appeared in the help text, and was bounced as "Unknown option"
/// because the allow-list never learned it.</para>
/// </summary>
public class DarlingHardenFilesVerbTests
{
    [Theory]
    [InlineData("--harden-files", true)]
    [InlineData("--HARDEN-FILES", true)]
    [InlineData("--Harden-Files", true)]
    [InlineData("--harden", false)]
    [InlineData("--harden-file", false)]
    [InlineData("--configure-firewall", false)]
    [InlineData("--nonsense", false)]
    public void IsHardenFilesVerb_RecognizesTheVerb_CaseInsensitive(string arg, bool expected)
    {
        Assert.Equal(expected, DarlingCliCommands.IsHardenFilesVerb(arg));
    }

    /// <summary>
    /// The allow-list must reach it or the Program.cs dispatch is dead code and the startup classifier answers
    /// "Unknown option" instead. The generic reflection pin covers this too; naming it makes the intent local.
    /// </summary>
    [Fact]
    public void IsKnownVerb_ReachesTheHardenVerb()
    {
        Assert.True(DarlingCliCommands.IsKnownVerb("--harden-files"));
    }

    /// <summary>An operator who cannot find the verb does not have it. It is the remedy for a CRITICAL log line,
    /// so it has to be listed where someone reading that line will look.</summary>
    [Fact]
    public void TheHelpText_ListsTheVerb_AndSaysItNeedsElevation()
    {
        var help = DarlingCliCommands.UsageText();

        Assert.Contains("--harden-files", help, StringComparison.Ordinal);
        Assert.Contains("elevated", help, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Program.cs dispatches it, guarded on Windows, and calls it rather than something adjacent — pinned
    /// structurally because the seam between the classifier and the dispatch is exactly where #1912's drift
    /// lived, and no unit test that calls <c>DarlingCliCommands</c> directly can see it.
    /// </summary>
    [Fact]
    public void ProgramDispatchesTheVerb_BehindAWindowsGuard()
    {
        var program = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Program.cs"));

        var at = program.IndexOf("DarlingCliCommands.IsHardenFilesVerb(args[0])", StringComparison.Ordinal);
        Assert.True(at >= 0, "Program.cs no longer dispatches --harden-files (#2352)");

        var block = program[at..Math.Min(program.Length, at + 900)];

        Assert.Contains("OperatingSystem.IsWindows()", block, StringComparison.Ordinal);
        Assert.Contains("DarlingCliCommands.HardenFiles(", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The verdict is the RE-READ, never the call that returned without throwing. "We tried" is not the same
    /// statement as "the secret is not readable" — the distinction that let a permissions call which silently
    /// did nothing hide in the field. Pinned on the source because the behaviour needs Windows ACLs to observe.
    /// </summary>
    [Fact]
    public void TheVerb_VerifiesEachTarget_AndFailsWhenAnythingIsStillReadable()
    {
        var source = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingCliCommands.cs"));

        var at = source.IndexOf("public static int HardenFiles(", StringComparison.Ordinal);
        Assert.True(at >= 0, "HardenFiles is gone (#2352)");

        var body = source[at..Math.Min(source.Length, at + 6000)];

        /* It re-reads rather than trusting the call. */
        Assert.Contains("DarlingFileSecurity.IsReadableByOrdinaryUsers(", body, StringComparison.Ordinal);

        /* And a still-exposed target is a non-zero exit, so it is usable in a provisioning script. */
        Assert.Contains("STILL READABLE", body, StringComparison.Ordinal);

        /* The live config is the ONLY target the interactive operator keeps read on: the Viewer and the CLI
           verbs run as that operator and must still read it. Nothing reads a backup (#1769). */
        Assert.Contains("AllowInteractive: true", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2371: the verb hardens for the account the SERVICE is registered under, never for whoever is running it.
    ///
    /// <para><b>The bug this pins.</b> Every original caller of <c>DarlingFileSecurity</c> runs INSIDE the
    /// service, so "the current identity" and "the account the service runs as" were the same value and the
    /// distinction did not exist. This verb inverts that by construction: it exists because a virtual service
    /// account cannot re-ACL a file it does not own, so it is ALWAYS run by somebody else. Resolving from the
    /// caller therefore granted the operator and stripped the service — measured on a live box, where an
    /// elevated run removed <c>NT SERVICE\PerformanceMonitor Darling</c> from all four targets and printed
    /// <c>All 4 item(s) secured</c> while doing it. The install kept working until its next restart, which is
    /// far enough away that nobody would connect the two.</para>
    /// </summary>
    [Fact]
    public void TheVerb_HardensForTheRegisteredServiceAccount_NotTheCaller()
    {
        var source = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingCliCommands.cs"));

        var at = source.IndexOf("public static int HardenFiles(", StringComparison.Ordinal);
        Assert.True(at >= 0, "HardenFiles is gone (#2352)");

        var body = source[at..Math.Min(source.Length, at + 8000)];

        /* It asks the SCM, and hardens for what it gets back. */
        Assert.Contains("RegisteredServiceAccount(ServiceName)", body, StringComparison.Ordinal);
        Assert.Contains("HardenForAccount(", body, StringComparison.Ordinal);

        /* The resolution happens BEFORE the first target is hardened, or the early targets get the caller's
           ACL and the later ones the service's -- a split-brain worse than either alone. */
        Assert.True(
            body.IndexOf("HardenForAccount(", StringComparison.Ordinal)
                < body.IndexOf("DarlingFileSecurity.HardenFile(", StringComparison.Ordinal),
            "the service account must be resolved before anything is hardened (#2371)");
    }

    /// <summary>
    /// #2371: private is only half of correct, so the verify pass checks BOTH directions.
    ///
    /// <para><c>IsReadableByOrdinaryUsers</c> asks whether anyone TOO MANY can read. It cannot see the opposite
    /// failure — an ACL that excludes ordinary users AND the service is maximally private and completely broken,
    /// and it passed the old check, which is why the live run reported success on four locked-out targets.</para>
    /// </summary>
    [Fact]
    public void TheVerb_AlsoVerifiesTheServiceCanStillReadEachTarget()
    {
        var source = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingCliCommands.cs"));

        var at = source.IndexOf("public static int HardenFiles(", StringComparison.Ordinal);
        var body = source[at..Math.Min(source.Length, at + 8000)];

        Assert.Contains("GrantsHardenedAccount(", body, StringComparison.Ordinal);
        Assert.Contains("LOCKED OUT", body, StringComparison.Ordinal);

        /* A lockout counts as exposure, so the verb exits non-zero and a provisioning script stops. Both
           branches feed the same counter the STILL READABLE path does. */
        var lockedAt = body.IndexOf("LOCKED OUT", StringComparison.Ordinal);
        Assert.Contains("exposed++", body[lockedAt..Math.Min(body.Length, lockedAt + 700)], StringComparison.Ordinal);
    }

    /// <summary>
    /// The resolver reads the SCM's own <c>ObjectName</c>, which is the account the service is logged on with —
    /// and returns null rather than throwing when the service is not registered, so a console run or a
    /// pre-install harden falls back to the caller instead of failing.
    /// </summary>
    [Fact]
    public void TheResolver_ReturnsNull_WhenTheServiceIsNotRegistered()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        Assert.Null(DarlingFileSecurity.RegisteredServiceAccount(
            "PerformanceMonitor Darling NoSuchService " + Guid.NewGuid().ToString("N")));
    }

    /// <summary>
    /// <c>LocalSystem</c> is the one <c>ObjectName</c> with no <see cref="System.Security.Principal.NTAccount"/>
    /// spelling to translate — the SCM stores it unqualified — so it is mapped to its well-known SID by hand.
    /// An install re-homed to LocalSystem would otherwise fall back to the caller and reintroduce the bug on
    /// exactly the configuration that looks most ordinary.
    /// </summary>
    [Fact]
    public void TheResolver_HandlesTheUnqualifiedLocalSystemAlias()
    {
        var source = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingFileSecurity.cs"));

        var at = source.IndexOf("RegisteredServiceAccount(", StringComparison.Ordinal);
        Assert.True(at >= 0, "the SCM resolver is gone (#2371)");

        var body = source[at..Math.Min(source.Length, at + 1800)];

        Assert.Contains("ObjectName", body, StringComparison.Ordinal);
        Assert.Contains("LocalSystem", body, StringComparison.Ordinal);
        Assert.Contains("WellKnownSidType.LocalSystemSid", body, StringComparison.Ordinal);
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
