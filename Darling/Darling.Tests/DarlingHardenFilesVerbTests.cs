/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

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

        /* The same window as the two pins below: #4004's review added the log-hash key's targets to the list, which
           moved the verify pass past the old 6,000, and its round 2 the junction refusal (DarlingHardenFilesJunctionTests),
           which moved it past 8,000. */
        var body = source[at..Math.Min(source.Length, at + 10000)];

        /* It re-reads rather than trusting the call. */
        Assert.Contains("DarlingFileSecurity.IsReadableByOrdinaryUsers(", body, StringComparison.Ordinal);

        /* And a still-exposed target is a non-zero exit, so it is usable in a provisioning script. */
        Assert.Contains("STILL READABLE", body, StringComparison.Ordinal);

        /* The live config is the ONLY target the interactive operator keeps read on: the Viewer and the CLI
           verbs run as that operator and must still read it. Nothing reads a backup (#1769). */
        Assert.Contains("AllowInteractive: true", body, StringComparison.Ordinal);

        /* #4280 Low 2: the last major upgrade's pre-upgrade postgresql.auto.conf is a target too, since
           File.Copy does not ACL it on its own. */
        Assert.Contains("DarlingStoreUpgrade.PreUpgradeAutoConfFileName", body, StringComparison.Ordinal);
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

        var body = source[at..Math.Min(source.Length, at + 10000)];

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
        var body = source[at..Math.Min(source.Length, at + 10000)];

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
}

/// <summary>
/// #4004's review, round 2 (M2): the documented install folder inherits Authenticated Users:(M) from C:\, so any
/// local user can create <c>darling-keys</c> there as a junction, and an ACL set by path follows it: the elevated run
/// rewrote the DACL of whatever it pointed at, a denial of service that also handed the service account full control.
/// In the <c>darling-config-env</c> collection because the key's directory depends on
/// <c>DOTNET_RUNNING_IN_CONTAINER</c>, which tests there set process-wide.
/// </summary>
[Collection("darling-config-env")]
public class DarlingHardenFilesJunctionTests
{
    /// <summary>
    /// On a bring-your-own install, where darling-keys is the key's directory, a junction there is refused with exit 1
    /// and its target's DACL is left as it was; on a managed install darling-keys is not a target at all. A scratch
    /// directory stands in for the install and for the junction's target.
    /// </summary>
    [Fact]
    public void AJunctionNamedDarlingKeys_IsRefused_AndItsTargetsAclIsUntouched()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("Junctions and DACLs are Windows'.");
            return;
        }

        /* A registered service makes the verb harden for that account, process-wide. */
        Assert.SkipWhen(DarlingFileSecurity.RegisteredServiceAccount("PerformanceMonitor Darling") is not null,
            "The Darling service is registered on this machine.");

        var scratch = Directory.CreateTempSubdirectory("darling-4004-junction-");
        var install = Path.Combine(scratch.FullName, "install");
        var victim = Path.Combine(scratch.FullName, "victim");
        var junction = Path.Combine(install, DarlingLogHashKeyFile.BringYourOwnDirectoryName);
        try
        {
            Directory.CreateDirectory(install);
            Directory.CreateDirectory(victim);
            var victimFile = Path.Combine(victim, DarlingLogHashKeyFile.WindowsFileName);
            File.WriteAllText(victimFile, "someone else's");
            var victimAcl = Sddl(victim);
            var victimFileAcl = Sddl(victimFile);
            MakeJunction(junction, victim);
            Assert.True((File.GetAttributes(junction) & FileAttributes.ReparsePoint) != 0, "the junction was not created");

            var config = Path.Combine(install, "darling.json");
            File.WriteAllText(config, "{\"postgres\":{\"managed\":false,\"connectionString\":\"Host=127.0.0.1;Username=darling\"}}");

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = DarlingCliCommands.HardenFiles(config, output, error);

            Assert.Equal(1, exit);
            Assert.Contains($"REFUSED  {junction} (the log-hash key directory): {junction} is a junction or symbolic link", error.ToString(), StringComparison.Ordinal);
            Assert.Contains($"REFUSED  {Path.Combine(junction, DarlingLogHashKeyFile.WindowsFileName)} (the log-hash key): {junction} is a junction or symbolic link", error.ToString(), StringComparison.Ordinal);
            Assert.Equal(victimAcl, Sddl(victim));
            Assert.Equal(victimFileAcl, Sddl(victimFile));

            /* A managed install: darling-keys never exists legitimately, so it is not a target at all. */
            File.WriteAllText(config, "{\"postgres\":{\"managed\":true,\"dataDirectory\":" + System.Text.Json.JsonSerializer.Serialize(Path.Combine(scratch.FullName, "store", "pg")) + "}}");
            var managedOutput = new StringWriter();
            var managedError = new StringWriter();

            DarlingCliCommands.HardenFiles(config, managedOutput, managedError);

            Assert.DoesNotContain(DarlingLogHashKeyFile.BringYourOwnDirectoryName, managedOutput.ToString() + managedError.ToString(), StringComparison.Ordinal);
            Assert.Equal(victimAcl, Sddl(victim));
            Assert.Equal(victimFileAcl, Sddl(victimFile));
        }
        finally
        {
            if (Directory.Exists(junction))
            {
                Directory.Delete(junction);
            }

            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>
    /// The harden itself goes through a handle to exactly the checked object, so a link swapped in after the verb's
    /// path check is not followed either: a junction in the middle of the path changes the handle's final path, a link
    /// at the name itself is opened as the link, and both are refused with nothing changed. An ordinary file gets the
    /// hardened ACL.
    /// </summary>
    [Fact]
    public void TheHandleHarden_RefusesALinkAnywhereOnThePath_AndHardensAnOrdinaryFile()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("Junctions and DACLs are Windows'.");
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-4004-handle-");
        var install = Path.Combine(scratch.FullName, "install");
        var victim = Path.Combine(scratch.FullName, "victim");
        var junction = Path.Combine(install, "through");
        try
        {
            Directory.CreateDirectory(install);
            Directory.CreateDirectory(victim);
            var victimFile = Path.Combine(victim, "target.txt");
            File.WriteAllText(victimFile, "someone else's");
            var victimAcl = Sddl(victim);
            var victimFileAcl = Sddl(victimFile);
            MakeJunction(junction, victim);

            var middle = DarlingFileSecurity.HardenWithoutFollowingLinks(Path.Combine(junction, "target.txt"), isDirectory: false, allowInteractive: false, install);
            Assert.NotNull(middle);
            Assert.Contains("resolves to", middle, StringComparison.Ordinal);

            var atTheName = DarlingFileSecurity.HardenWithoutFollowingLinks(junction, isDirectory: true, allowInteractive: false, install);
            Assert.Equal($"{junction} is a junction or symbolic link", atTheName);

            Assert.Equal(victimAcl, Sddl(victim));
            Assert.Equal(victimFileAcl, Sddl(victimFile));

            var ordinary = Path.Combine(install, "darling.json");
            File.WriteAllText(ordinary, "{}");
            Assert.Null(DarlingFileSecurity.HardenWithoutFollowingLinks(ordinary, isDirectory: false, allowInteractive: true, install));
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(ordinary));
            Assert.True(DarlingFileSecurity.GrantsHardenedAccount(ordinary));
            Assert.Contains(";;;IU)", Sddl(ordinary), StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(junction))
            {
                Directory.Delete(junction);
            }

            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>
    /// #4004's review, round 3: a hard link is not a reparse point, and the handle's final path is the name it was
    /// opened by, so a <c>darling.json.bak-x</c> planted as a hard link to someone else's file passed both checks and
    /// would have had the hardened DACL applied to that file. A file with more than one name is refused, by the handle
    /// harden and by the verb (exit 1), and the file's DACL is untouched.
    /// </summary>
    [Fact]
    public void AHardLinkedBackup_IsRefused_AndTheFileItNamesKeepsItsAcl()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("Hard links through CreateHardLinkW, and DACLs, are Windows'.");
            return;
        }

        /* A registered service makes the verb harden for that account, process-wide. */
        Assert.SkipWhen(DarlingFileSecurity.RegisteredServiceAccount("PerformanceMonitor Darling") is not null,
            "The Darling service is registered on this machine.");

        var scratch = Directory.CreateTempSubdirectory("darling-4004-hardlink-");
        var install = Path.Combine(scratch.FullName, "install");
        var victim = Path.Combine(scratch.FullName, "victim");
        try
        {
            Directory.CreateDirectory(install);
            Directory.CreateDirectory(victim);
            var victimFile = Path.Combine(victim, "someones.txt");
            File.WriteAllText(victimFile, "someone else's");
            var victimFileAcl = Sddl(victimFile);
            var backup = Path.Combine(install, "darling.json.bak-x");
            Assert.True(CreateHardLinkW(backup, victimFile, IntPtr.Zero), "the hard link was not created");

            var refusal = DarlingFileSecurity.HardenWithoutFollowingLinks(backup, isDirectory: false, allowInteractive: false, install);

            Assert.Equal($"{backup} is a hard link: the file has 2 names, and its ACL is the same under every one of them", refusal);
            Assert.Equal(victimFileAcl, Sddl(victimFile));

            /* The verb finds it as a config backup beside darling.json. The store directory is a scratch path that does
               not exist, so nothing outside the scratch directory is touched. */
            var config = Path.Combine(install, "darling.json");
            File.WriteAllText(config, "{\"postgres\":{\"managed\":false,\"connectionString\":\"Host=127.0.0.1;Username=darling\",\"dataDirectory\":"
                + System.Text.Json.JsonSerializer.Serialize(Path.Combine(scratch.FullName, "store", "pg")) + "}}");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = DarlingCliCommands.HardenFiles(config, output, error);

            Assert.Equal(1, exit);
            Assert.Contains($"REFUSED  {backup} (a config backup): {backup} is a hard link", error.ToString(), StringComparison.Ordinal);
            Assert.Contains($"SECURED  {config} (the live config)", output.ToString(), StringComparison.Ordinal);
            Assert.Equal(victimFileAcl, Sddl(victimFile));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>
    /// #4004's review, round 3: a config directory at a volume root contained nothing, because <c>C:\</c> plus a
    /// separator is <c>C:\\</c>. Its targets then took the path-based harden, and the link walk checked only the target
    /// itself, so a junction in the middle of the path was followed. A volume root now contains what is on it, and the
    /// walk from a target up to it finds that junction.
    /// </summary>
    [Fact]
    public void AConfigDirectoryAtAVolumeRoot_ContainsWhatIsOnIt()
    {
        var root = Path.GetPathRoot(Path.GetTempPath())!;
        var separator = Path.DirectorySeparatorChar.ToString();

        Assert.True(DarlingCliCommands.IsBelow(root, Path.Combine(root, "darling.json")));
        Assert.True(DarlingCliCommands.IsBelow(root, Path.Combine(root, DarlingLogHashKeyFile.BringYourOwnDirectoryName, DarlingLogHashKeyFile.WindowsFileName)));
        Assert.False(DarlingCliCommands.IsBelow(root, root));
        Assert.True(DarlingCliCommands.IsBelow(Path.Combine(root, "cfg") + separator, Path.Combine(root, "cfg", "darling.json")));
        Assert.False(DarlingCliCommands.IsBelow(Path.Combine(root, "cfg"), Path.Combine(root, "cfgx", "darling.json")));
        Assert.False(DarlingCliCommands.IsBelow(Path.Combine(root, "cfg"), Path.Combine(root, "cfg")));

        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-4004-root-");
        var junction = Path.Combine(scratch.FullName, "through");
        var victim = Path.Combine(scratch.FullName, "victim");
        try
        {
            Directory.CreateDirectory(victim);
            File.WriteAllText(Path.Combine(victim, "darling.json.bak-x"), "someone else's");
            MakeJunction(junction, victim);
            Assert.True((File.GetAttributes(junction) & FileAttributes.ReparsePoint) != 0, "the junction was not created");

            Assert.Equal(
                $"{junction} is a junction or symbolic link",
                DarlingCliCommands.ReparsePointOnPath(root, Path.Combine(junction, "darling.json.bak-x")));
        }
        finally
        {
            if (Directory.Exists(junction))
            {
                Directory.Delete(junction);
            }

            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
    private static extern bool CreateHardLinkW(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static string Sddl(string path) =>
        Directory.Exists(path)
            ? new DirectoryInfo(path).GetAccessControl().GetSecurityDescriptorSddlForm(System.Security.AccessControl.AccessControlSections.Access)
            : new FileInfo(path).GetAccessControl().GetSecurityDescriptorSddlForm(System.Security.AccessControl.AccessControlSections.Access);

    private static void MakeJunction(string link, string target)
    {
        using var process = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo("cmd.exe")
        {
            ArgumentList = { "/c", "mklink", "/J", link, target },
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        })!;
        process.StandardOutput.ReadToEnd();
        process.StandardError.ReadToEnd();
        process.WaitForExit();
    }
}
