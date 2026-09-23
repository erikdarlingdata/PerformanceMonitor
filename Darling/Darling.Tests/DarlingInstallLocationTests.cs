/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// <c>install-darling.ps1</c>'s install-location guard (#2187).
///
/// <para><b>The defect.</b> #2185 extracted the zip to
/// <c>C:\Users\username\Desktop\PerformanceMonitorDarling-3.2.0\</c> — a completely reasonable thing to do
/// with a download — and the installer created a service that could never work. The service runs as the
/// virtual account <c>NT SERVICE\PerformanceMonitor Darling</c> and never as LocalSystem, because the
/// bundled PostgreSQL refuses to run with administrative privileges; that account is not the installing
/// user, not SYSTEM, and not Administrators, and a profile directory grants access to approximately those
/// three and nobody else. Measured on Windows 11, a directory created under a profile inherits exactly
/// SYSTEM / Administrators / the profile owner, with no <c>BUILTIN\Users</c>, no Authenticated Users and no
/// CREATOR OWNER. So the install succeeded and the bundled PostgreSQL's initdb.exe died at 0xC0000135
/// (STATUS_DLL_NOT_FOUND) before writing a word of output.</para>
///
/// <para><b>Why these tests are split the way they are.</b> The ORDERING and the VERDICT are pinned
/// structurally, the same way this project pins every other PowerShell invariant it cannot compile. The
/// path BOUNDARY is not: it is a decision computed from a string, where the failure that matters most —
/// refusing <c>C:\UsersData</c> because it starts with the same characters as <c>C:\Users</c> — is
/// invisible to any source-parsing assertion and strands an install that would have worked. So that one is
/// executed, against the function as it ships, under Windows PowerShell 5.1: the host an operator on
/// Windows Server 2019 (the reporter's OS) actually gets.</para>
/// </summary>
public class DarlingInstallLocationTests
{
    private static string InstallScript => ReadRepoFile(Path.Combine("Darling", "tools", "install-darling.ps1"));

    /// <summary>
    /// The guard must run before anything that changes machine state. A check that fires after the Event
    /// Log source is registered, or after <c>sc create</c>, leaves exactly the debris #2187 exists to
    /// prevent: a service on the box that can never start.
    /// </summary>
    [Fact]
    public void LocationGuard_RunsBeforeAnythingIsInstalled()
    {
        var script = InstallScript;

        var guard = script.IndexOf("if ($underProfile -or $networkKind) {", StringComparison.Ordinal);
        Assert.True(guard >= 0, "install-darling.ps1 no longer guards the install location (#2187)");

        /* Every state-changing step in the script, in the order it runs. The pre-flight is included on
           purpose: probing every configured SQL Server before saying "this folder cannot work" spends the
           operator's time on a question that is already answered. */
        foreach (var (marker, what) in new[]
        {
            ("& $serviceExe --test-connection", "the --test-connection pre-flight"),
            ("Copy-Item $samplePath $configPath", "copying darling.sample.json to darling.json"),
            ("New-EventLog -LogName Application", "registering the Event Log source"),
            ("& sc.exe create $serviceName", "creating the service"),
            ("Set-Acl -Path $secretFile", "hardening the config's ACL"),
            ("Start-Service -Name $serviceName", "starting the service"),
        })
        {
            var at = script.IndexOf(marker, StringComparison.Ordinal);
            Assert.True(at >= 0, $"install-darling.ps1 no longer contains {what} ('{marker}')");
            Assert.True(guard < at, $"the install-location guard must run BEFORE {what}, or a doomed location still leaves debris behind (#2187)");
        }
    }

    /// <summary>
    /// A FRESH install in an unreadable location is refused outright, not warned about. #2187 chose this
    /// deliberately over warning: the install cannot work, so proceeding is never right — and the reporter's
    /// experience is precisely that of an install nothing stopped.
    ///
    /// <para>An UPGRADE is the one case that asks instead, and that asymmetry is the same one the script
    /// already applies to the service's logon account: the upgrade path exists to preserve installs
    /// operators have customized (a re-homed domain account or gMSA, #1802/#1823), and granting the service
    /// account read on the tree by hand — #2187's rejected option 2 — is exactly the customization that can
    /// make this location work. Refusing there would strand a deployment that runs today.</para>
    /// </summary>
    [Fact]
    public void LocationGuard_RefusesAFreshInstall_AndOnlyAsksOnAnUpgrade()
    {
        var script = InstallScript;
        var guard = ExtractBracedBlock(script, "if ($underProfile -or $networkKind) {");

        var refusal = guard.IndexOf("if (-not $existing) {", StringComparison.Ordinal);
        Assert.True(refusal >= 0, "the guard no longer distinguishes a fresh install from an upgrade (#2187)");

        var fresh = ExtractBracedBlock(guard, "if (-not $existing) {");
        Assert.Contains("Fail", fresh, StringComparison.Ordinal);
        Assert.DoesNotContain("Read-Host", fresh, StringComparison.Ordinal);

        /* The upgrade branch asks — and defaults to NO, so an operator who hits Enter through an unattended
           run does not silently re-point a service at a folder it cannot read. */
        Assert.Contains("Read-Host 'Point the service at this folder anyway? [y/N]'", guard, StringComparison.Ordinal);
        Assert.Contains("if ($answer -notmatch '^[Yy]') { exit 4 }", guard, StringComparison.Ordinal);

        /* $existing has to be resolved before the guard consults it, or the fresh/upgrade split silently
           collapses to "always fresh" — which would refuse every upgrade of an install already living
           there, the one outcome that breaks a working deployment. */
        var resolved = script.IndexOf("$existing = Get-Service -Name $serviceName -ErrorAction SilentlyContinue", StringComparison.Ordinal);
        Assert.True(resolved >= 0 && resolved < script.IndexOf("if ($underProfile -or $networkKind) {", StringComparison.Ordinal),
            "$existing must be resolved BEFORE the location guard, which branches on it");
    }

    /// <summary>
    /// The profile root is read from <c>ProfileList\ProfilesDirectory</c> rather than hardcoded to
    /// <c>C:\Users</c>. It is relocatable, and a literal would stop matching on exactly the box that moved
    /// it — the box where a missed check costs the most.
    /// </summary>
    [Fact]
    public void LocationGuard_ReadsTheProfileRootFromWindows_RatherThanAssumingCUsers()
    {
        var script = InstallScript;

        Assert.Contains(@"HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProfileList", script, StringComparison.Ordinal);
        Assert.Contains("ProfilesDirectory", script, StringComparison.Ordinal);

        /* And the current user's own profile is checked as well: a profile redirected outside
           ProfilesDirectory is still a profile, and it is the one whose owner is most likely running this.
           Against $classifyRoot since #2348 — the normalized spelling, so the extended-length form of a
           profile path is caught too. */
        Assert.Contains("Test-PathIsAtOrUnder $classifyRoot $env:USERPROFILE", script, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2187 weighed fixing the tree's ACLs against refusing, and rejected the fix: it would mean the
    /// product starts silently ACLing directories inside somebody's profile. This pins that decision where
    /// it can actually be broken — the installer's only <c>Set-Acl</c> targets are the credential files it
    /// has always hardened, never the install tree.
    /// </summary>
    [Fact]
    public void TheInstaller_NeverAclsTheInstallTree_OnlyTheCredentialFiles()
    {
        var script = InstallScript;

        var applications = 0;
        for (var at = script.IndexOf("Set-Acl -Path ", StringComparison.Ordinal); at >= 0; at = script.IndexOf("Set-Acl -Path ", at + 1, StringComparison.Ordinal))
        {
            applications++;
            Assert.Contains("Set-Acl -Path $secretFile", script.Substring(at, Math.Min(30, script.Length - at)), StringComparison.Ordinal);
        }

        Assert.True(applications == 2, $"expected exactly 2 Set-Acl calls (the hardened DACL and the owner), found {applications}");

        /* And the install root is never granted anything, by either route. The icacls lines the script does
           contain are remediation instructions PRINTED for the operator, which is a different thing from the
           product reaching into a profile and changing permissions itself. */
        Assert.DoesNotContain("Set-Acl -Path $root", script, StringComparison.Ordinal);
        Assert.DoesNotContain("icacls $root", script, StringComparison.Ordinal);
    }

    /// <summary>
    /// The boundary, executed rather than read. <c>C:\UsersData</c> is not under <c>C:\Users</c>, and a
    /// prefix test that says it is would refuse an install that works — a worse failure than the one the
    /// guard exists to catch, and one no structural pin can see.
    ///
    /// <para>Run under <c>powershell.exe</c> (Windows PowerShell 5.1) deliberately: it is what the
    /// reporter's Windows Server 2019 gives an operator by default, so this pins 5.1 compatibility of the
    /// guard's syntax at the same time.</para>
    /// </summary>
    [Fact]
    public void TestPathIsAtOrUnder_DecidesTheBoundary_AsShipped()
    {
        var cases = new (string Candidate, string Parent, bool Expected)[]
        {
            /* The reported shape, and the profile root itself — an install root AT the root is as
               unreadable as one below it. */
            (@"C:\Users\username\Desktop\PerformanceMonitorDarling-3.2.0", @"C:\Users", true),
            (@"C:\Users\username\Desktop\PerformanceMonitorDarling-3.2.0\", @"C:\Users", true),
            (@"C:\Users", @"C:\Users", true),
            (@"C:\Users\", @"C:\Users", true),
            /* Windows paths are case-insensitive, and neither a relative segment nor a forward slash is a
               way out of the profile. */
            (@"c:\users\bob\x", @"C:\Users", true),
            (@"C:\Users\bob\..\bob\x", @"C:\Users", true),
            ("C:/Users/bob/x", @"C:\Users", true),
            /* The false-refusal cases. A bare StartsWith fails every one of these. */
            (@"C:\UsersData\Darling", @"C:\Users", false),
            (@"C:\UsersData", @"C:\Users", false),
            (@"C:\Users2\Darling", @"C:\Users", false),
            /* The documented location, and the machine-scoped data root #2187 asked about explicitly. */
            (@"C:\PerformanceMonitorDarling", @"C:\Users", false),
            (@"C:\ProgramData\PerformanceMonitorDarling", @"C:\Users", false),
            (@"D:\PerformanceMonitorDarling", @"C:\Users", false),
            /* Nothing is not somewhere. */
            (@"C:\Users\bob", "", false),
            ("", @"C:\Users", false),
        };

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Test-PathIsAtOrUnder"));
        foreach (var (candidate, parent, _) in cases)
        {
            probe.AppendLine($"if (Test-PathIsAtOrUnder '{candidate}' '{parent}') {{ 'True' }} else {{ 'False' }}");
        }

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(cases.Length, answers.Count);

        var wrong = new List<string>();
        for (var i = 0; i < cases.Length; i++)
        {
            var (candidate, parent, expected) = cases[i];
            if (!string.Equals(answers[i], expected.ToString(), StringComparison.Ordinal))
            {
                wrong.Add($"under('{candidate}', '{parent}') returned {answers[i]}, expected {expected}");
            }
        }

        Assert.True(wrong.Count == 0, "install-darling.ps1's path-containment boundary is wrong:\n  " + string.Join("\n  ", wrong));
    }

    /// <summary>
    /// The network half, also executed. <c>\\?\C:\...</c> is the long-path prefix on a LOCAL path, not a
    /// server name, and treating it as a share would refuse a perfectly ordinary install root.
    ///
    /// <para>The probe composes <c>Convert-ExtendedLengthPath</c> ahead of <c>Get-NetworkPathKind</c> because
    /// that is what the script itself does (<c>$classifyRoot = Convert-ExtendedLengthPath $root</c>). Calling
    /// the kind function on a RAW path would be testing a contract the installer does not use: since #2348 it
    /// classifies normalized paths, so it no longer carries a <c>\\?\</c> carve-out of its own and would call a
    /// bare <c>\\?\C:\...</c> a share. Keeping the composition here is the point — the pair is the unit.</para>
    /// </summary>
    [Fact]
    public void GetNetworkPathKind_SeparatesAShareFromAnExtendedLengthLocalPath_AsShipped()
    {
        var cases = new (string Path, string Expected)[]
        {
            (@"\\fileserver\share\PerformanceMonitorDarling", "UNC"),
            (@"\\?\C:\PerformanceMonitorDarling", "<none>"),
            (@"C:\PerformanceMonitorDarling", "<none>"),
            (@"C:\Users\bob\Desktop\PerformanceMonitorDarling", "<none>"),
            ("", "<none>"),

            /* #2348: the extended-length spelling of a REAL share is a share. Previously the wholesale \\?\
               exclusion waved all three of these through as "<none>". */
            (@"\\?\UNC\fileserver\share\PerformanceMonitorDarling", "UNC"),
            (@"\\?\unc\fileserver\share\PerformanceMonitorDarling", "UNC"),
            (@"\\?\UNC\fileserver\share", "UNC"),
        };

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Convert-ExtendedLengthPath"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-NetworkPathKind"));
        foreach (var (path, _) in cases)
        {
            probe.AppendLine(
                $"$k = Get-NetworkPathKind (Convert-ExtendedLengthPath '{path}'); if ($null -eq $k) {{ '<none>' }} else {{ $k }}");
        }

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(cases.Length, answers.Count);

        for (var i = 0; i < cases.Length; i++)
        {
            Assert.Equal(cases[i].Expected, answers[i]);
        }
    }

    /// <summary>
    /// #2348, the normalization itself, executed as shipped. It is the one place in either implementation that
    /// knows the <c>\\?\</c> prefix exists, so every rule downstream can be written against real paths.
    ///
    /// <para>The lowercase case is not padding: Windows accepts <c>\\?\unc\</c>, so an ordinal match on
    /// <c>UNC</c> would leave a share spelled that way looking like the local path <c>unc\server\share</c> —
    /// re-opening exactly the hole this closes, for the operator least likely to be checked on.</para>
    /// </summary>
    [Fact]
    public void ConvertExtendedLengthPath_RewritesBothSpellings_AsShipped()
    {
        var cases = new (string Path, string Expected)[]
        {
            (@"\\?\UNC\fileserver\share\dir", @"\\fileserver\share\dir"),
            (@"\\?\unc\fileserver\share\dir", @"\\fileserver\share\dir"),
            (@"\\?\C:\PerformanceMonitorDarling", @"C:\PerformanceMonitorDarling"),
            (@"\\?\C:\Users\bob\dir", @"C:\Users\bob\dir"),

            /* Untouched: an ordinary local path, an ordinary share, and a path that merely CONTAINS the
               characters without leading with them. */
            (@"C:\PerformanceMonitorDarling", @"C:\PerformanceMonitorDarling"),
            (@"\\fileserver\share\dir", @"\\fileserver\share\dir"),
            ("", ""),
        };

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Convert-ExtendedLengthPath"));
        foreach (var (path, _) in cases)
        {
            probe.AppendLine($"$v = Convert-ExtendedLengthPath '{path}'; if ([string]::IsNullOrEmpty($v)) {{ '<empty>' }} else {{ $v }}");
        }

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(cases.Length, answers.Count);

        for (var i = 0; i < cases.Length; i++)
        {
            var expected = cases[i].Expected.Length == 0 ? "<empty>" : cases[i].Expected;
            Assert.Equal(expected, answers[i]);
        }
    }

    /// <summary>
    /// #2201: the mapped-drive probe must not fail OPEN when WMI is unavailable.
    ///
    /// <para><b>The defect.</b> The probe asks <c>Get-CimInstance Win32_LogicalDisk</c> for the drive type.
    /// On a WMI-restricted image (locked-down or Server Core) that call throws, or answers with nothing at
    /// all, and the guard then returns "not network" — for exactly the drive-letter-mapped share it exists
    /// to refuse. UNC paths are caught lexically before this point, so the exposure is only mapped LETTERS
    /// on boxes where WMI is restricted.</para>
    ///
    /// <para><b>Why executed rather than structural.</b> The fix is a fallback ORDER — try WMI, and only on
    /// its silence consult <c>Get-PSDrive</c>, whose <c>DisplayRoot</c> names the share a letter maps to
    /// without touching WMI. A source scan can see that both cmdlets appear; it cannot see which answer
    /// wins, nor that the fallback is skipped when WMI already answered. Both cmdlets are shadowed by
    /// functions here, which PowerShell resolves ahead of the real ones, so the WMI-restricted box is
    /// simulated rather than described.</para>
    ///
    /// <para>Case 4 is the one that keeps the fix honest: when WMI answers "local", Get-PSDrive is rigged
    /// to THROW. A terminating error would surface as stderr and fail this test, so the case passing proves
    /// the fallback was never consulted — the guard still trusts a definite WMI answer, and does not invent
    /// a refusal from a drive that merely has a DisplayRoot.</para>
    ///
    /// <para>Case 5 draws the line around what "definite" means. <c>DriveType 0</c> is WMI's <i>unknown</i>,
    /// not local, and a partial row is likeliest on precisely the restricted images this fallback exists
    /// for — so it must fall through rather than short-circuit. It works because 0 is falsy in PowerShell,
    /// which is worth pinning rather than trusting to stay true.</para>
    /// </summary>
    [Fact]
    public void NetworkPathKind_WhenWmiIsUnavailable_FallsBackToPSDriveDisplayRoot()
    {
        /* Shadows need [CmdletBinding()] so the shipped call sites can pass -ErrorAction, which is a common
           parameter and not one a plain function accepts. */
        const string Shadows = @"
function Get-CimInstance {
    [CmdletBinding()]
    param([Parameter(ValueFromRemainingArguments = $true)] $Rest)
    if ($env:PM_WMI -eq ""throw"") { throw ""WMI is not available on this image"" }
    if ($env:PM_WMI -eq ""silent"") { return $null }
    return [pscustomobject]@{ DriveType = [int]$env:PM_WMI }
}
function Get-PSDrive {
    [CmdletBinding()]
    param([Parameter(ValueFromRemainingArguments = $true)] $Rest)
    if ($env:PM_PSDRIVE -eq ""throw"") { throw ""Get-PSDrive must not be consulted when WMI answered"" }
    return [pscustomobject]@{ DisplayRoot = $env:PM_PSDRIVE }
}
";

        /* wmi: "throw" | "silent" | a DriveType number (4 = network, 3 = local fixed disk). */
        var cases = new (string Wmi, string DisplayRoot, string Expected, string Because)[]
        {
            ("silent", @"\\fileserver\share", "mapped drive",
                "WMI answered nothing on a restricted image and the letter maps to a share"),
            ("throw", @"\\fileserver\share", "mapped drive",
                "WMI threw and the letter maps to a share"),
            ("silent", "", "<none>",
                "no evidence either way must stay not-network: the guard may not invent a refusal"),
            ("4", "", "mapped drive", "WMI itself said network, no fallback needed"),
            ("3", "throw", "<none>",
                "a definite local answer from WMI must not consult the fallback at all"),
            ("0", @"\\fileserver\share", "mapped drive",
                "DriveType 0 is unknown, not local - a partial WMI row must not short-circuit the fallback"),
        };

        for (var i = 0; i < cases.Length; i++)
        {
            var (wmi, displayRoot, expected, because) = cases[i];

            var probe = new StringBuilder();
            probe.AppendLine($"$env:PM_WMI = '{wmi}'");
            probe.AppendLine($"$env:PM_PSDRIVE = '{displayRoot}'");
            probe.AppendLine(Shadows);
            probe.AppendLine(ExtractFunction(InstallScript, "Get-NetworkPathKind"));
            probe.AppendLine(@"$k = Get-NetworkPathKind 'Z:\PerformanceMonitorDarling'; " +
                "if ($null -eq $k) { '<none>' } else { $k }");

            var answers = RunWindowsPowerShell(probe.ToString());

            Assert.Single(answers);
            /* Assert.True over Assert.Equal so the REASON travels with the failure: "expected mapped
               drive, got <none>" is not actionable on its own, and this test has six cases. */
            Assert.True(expected == answers[0],
                $"case {i} (wmi={wmi}, DisplayRoot='{displayRoot}'): expected {expected}, got " +
                $"{answers[0]} — {because}");
        }
    }

    /// <summary>
    /// The installer's verdict and the SERVICE's verdict must be the same verdict (#2185).
    ///
    /// <para><b>Why this test exists.</b> #2187 put the rule in PowerShell, where only installs that go
    /// through the script can benefit; #2185 needed the service to reach the same conclusion by itself, for
    /// the README's manual <c>sc create</c> path and for anyone who registers the exe by hand. That is two
    /// implementations of one rule in two languages, and the one that drifted would be the one nobody was
    /// reading. So both are run over ONE table — <see cref="DarlingServiceInstallLocationTests.Cases"/> — and
    /// disagreement is a failure regardless of which side is "right".</para>
    ///
    /// <para><b>What is really being executed.</b> The PowerShell side is the shipped file: both helper
    /// functions are extracted whole, and the two lines that COMPOSE them into a verdict are lifted verbatim
    /// out of the script rather than retyped. Only the environment is injected — the profile root, the
    /// operator's profile, and the drive type — which is exactly what the C# side takes as parameters.</para>
    ///
    /// <para>Windows-only in practice, like every test in this class: it shells out to
    /// <c>powershell.exe</c>.</para>
    /// </summary>
    [Fact]
    public void TheInstallerAndTheService_ReachTheSameVerdict_OverOneTable()
    {
        const string ProfileRoot = @"C:\Users";
        const string UserProfile = @"C:\Users\installer";

        var script = InstallScript;
        var cases = DarlingServiceInstallLocationTests.Cases;

        var probe = new StringBuilder();

        /* The environment, injected. Get-ProfilesDirectory reads HKLM and $env:USERPROFILE is the box's own,
           and a parity test that took either from the machine it runs on would compare the two rules against
           two different environments. */
        probe.AppendLine($"$env:USERPROFILE = '{UserProfile}'");
        probe.AppendLine($"function Get-ProfilesDirectory {{ '{ProfileRoot}' }}");

        /* Z: is a mapped share and every other letter is a local fixed disk. A definite WMI answer is what the
           shipped function trusts, so no Get-PSDrive shadow is needed - that fallback is #2201's territory and
           is pinned on its own above. */
        probe.AppendLine(@"
function Get-CimInstance {
    [CmdletBinding()]
    param([Parameter(ValueFromRemainingArguments = $true)] $Rest)
    if (($Rest -join ' ') -match ""DeviceID='[Zz]:'"") { return [pscustomobject]@{ DriveType = 4 } }
    return [pscustomobject]@{ DriveType = 3 }
}");

        probe.AppendLine(ExtractFunction(script, "Convert-ExtendedLengthPath"));
        probe.AppendLine(ExtractFunction(script, "Test-PathIsAtOrUnder"));
        probe.AppendLine(ExtractFunction(script, "Get-NetworkPathKind"));

        /* The composition, quoted out of the shipped script. The normalization line (#2348) is part of it:
           both rules classify $classifyRoot, never the raw $root, and lifting only the two rule lines would
           quietly test a composition the installer does not perform. */
        var normalizeLine = ExtractLine(script, "$classifyRoot = Convert-ExtendedLengthPath $root");
        var networkLine = ExtractLine(script, "$networkKind = Get-NetworkPathKind $classifyRoot");
        var profileLine = ExtractLine(script, "$underProfile = (Test-PathIsAtOrUnder $classifyRoot (Get-ProfilesDirectory))");

        foreach (var (directory, _, _) in cases)
        {
            probe.AppendLine($"$root = '{directory}'");
            probe.AppendLine(normalizeLine);
            probe.AppendLine(networkLine);
            probe.AppendLine(profileLine);
            /* The script's own precedence: the profile message is chosen first when a path is somehow both
               (the 'if ($underProfile)' arm inside the guard block), and the guard fires on either. */
            probe.AppendLine("if ($underProfile) { 'UserProfile' } elseif ($networkKind -eq 'UNC') { 'UncPath' } elseif ($networkKind) { 'MappedDrive' } else { 'None' }");
        }

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(cases.Count, answers.Count);

        var disagreements = new List<string>();
        for (var i = 0; i < cases.Count; i++)
        {
            var (directory, expected, because) = cases[i];

            /* The C# side, with the same injected environment. Z: is the table's mapped drive. */
            var service = PerformanceMonitor.Darling.Service.DarlingInstallLocation.Classify(
                directory, ProfileRoot, UserProfile,
                static qualifier => string.Equals(qualifier, "Z:", StringComparison.OrdinalIgnoreCase));

            /* Both sides are also checked against the table's own expectation, so a mutual mistake cannot pass
               as agreement. */
            if (!string.Equals(answers[i], expected.ToString(), StringComparison.Ordinal) || service != expected)
            {
                disagreements.Add(
                    $"'{directory}': install-darling.ps1 said {answers[i]}, the service said {service}, the table says {expected} ({because})");
            }
        }

        Assert.True(disagreements.Count == 0,
            "the installer and the service disagree about which install locations cannot work (#2185):\n  " +
            string.Join("\n  ", disagreements));
    }

    /// <summary>
    /// The two rules must also agree about WHERE the profile root comes from, not just what they do with it
    /// (review catch on #2185).
    ///
    /// <para><b>The blind spot this closes.</b> The table-parity test above injects the profile root into both
    /// implementations, which is what makes the table comparable — and means neither side's own lookup is ever
    /// exercised. The first C# version derived the root from <c>%PUBLIC%</c>'s parent on the belief that Windows
    /// keeps <c>PUBLIC</c> in step with <c>ProfilesDirectory</c>; they are two INDEPENDENT values under one key
    /// that merely default to the same tree. On a box where profiles were relocated without moving Public, the
    /// installer would have refused an install the service waved through — a false negative on the exact case
    /// #2185 exists to catch, invisible to every test that supplies the root itself.</para>
    ///
    /// <para>So this one supplies nothing: it runs the installer's <c>Get-ProfilesDirectory</c> as shipped and
    /// requires <see cref="DarlingInstallLocation.MachineProfileRoot"/> to answer the same, on whatever box the
    /// tests are running on. It passes trivially on an unrelocated box, which is fine — its job is to fail the
    /// moment the two stop reading the same thing.</para>
    /// </summary>
    [Fact]
    public void TheInstallerAndTheService_ReadTheProfileRoot_FromTheSamePlace()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-ProfilesDirectory"));
        probe.AppendLine("Get-ProfilesDirectory");

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Single(answers);

        var installer = answers[0].TrimEnd('\\');
        var service = PerformanceMonitor.Darling.Service.DarlingInstallLocation.MachineProfileRoot().TrimEnd('\\');

        Assert.Equal(installer, service, ignoreCase: true);
    }

    /// <summary>
    /// #4034: <c>Lock-DarlingInstallTree</c> ships twice, in install-darling.ps1 (a fresh install) and
    /// upgrade-darling.ps1 (every install made before it existed, closed at its next upgrade). Neither script can
    /// import the other, since each runs from wherever its zip was extracted, so the copies are compared byte for
    /// byte: a fix made to one and not the other would leave half the installs open.
    /// </summary>
    [Fact]
    public void TheInstallTreeLock_ShipsIdenticallyInTheInstallAndUpgradeScripts()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));

        Assert.Equal(InstallTreeLockBlock(InstallScript), InstallTreeLockBlock(upgrade));
        /* The account the lock is handed comes from the same lookup in both, for the same reason. */
        Assert.Equal(ExtractFunction(InstallScript, "Get-DarlingServiceLogonName"), ExtractFunction(upgrade, "Get-DarlingServiceLogonName"));
    }

    /// <summary>
    /// #4043: the pre-lock writable-extraction check ships twice for the same reason the lock itself does -
    /// install-darling.ps1 checks the install root before 1b2 ever runs, upgrade-darling.ps1 checks a folder
    /// -Source before it copies that folder's content over the (already locked) install root, and neither
    /// script can import the other's copy. Compared byte for byte so a fix to one does not silently miss the
    /// other.
    /// </summary>
    [Fact]
    public void ThePreLockWritableExtractionCheck_ShipsIdenticallyInTheInstallAndUpgradeScripts()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));

        Assert.Equal(ExtractFunction(InstallScript, "Get-UntrustedWriteGrantees"), ExtractFunction(upgrade, "Get-UntrustedWriteGrantees"));
        Assert.Equal(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"), ExtractFunction(upgrade, "Get-DarlingPreLockTrustedSids"));
    }

    /// <summary>
    /// #4043, executed as shipped: a folder made directly under the system drive root inherits a broad write
    /// grant from it (the same shape #4034's own test relies on, and the one the pre-lock check exists to
    /// catch before any lock has run). Proves the check FIRES on that naturally-inherited shape, falls SILENT
    /// once the tree is hardened to only the trusted set, and also fires when the grant sits on the service
    /// exe alone with a clean root - the "also check the exe" requirement. The admin running this script must
    /// never be a finding on its own, since Get-DarlingPreLockTrustedSids adds them.
    /// </summary>
    [Fact]
    public void ThePreLockWritableExtractionCheck_CatchesAnInheritedGrant_AndIsSilentOnceHardened()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-UntrustedWriteGrantees"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path ([IO.Path]::GetPathRoot([Environment]::SystemDirectory)) ('pm4043-test-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            try {
                New-Item -ItemType Directory -Path $root -Force | Out-Null
                Set-Content -LiteralPath "$root\svc.exe" -Value 'x'
                $trusted = Get-DarlingPreLockTrustedSids

                # Phase 1: freshly made directly under the drive root - nothing hardened yet, the exact shape
                # extracting a zip to C:\<name> has the instant it lands, before install-darling.ps1 runs a
                # single line.
                'inheritedCount=' + @(Get-UntrustedWriteGrantees $root $trusted).Count

                # Phase 2: hardened to only the trusted set (what 1b2's lock produces) - must fall silent.
                $c = New-Object System.Security.AccessControl.DirectorySecurity
                $c.SetAccessRuleProtection($true, $false)
                foreach ($s in $trusted) { $c.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($s, 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow'))) }
                Set-Acl -LiteralPath $root -AclObject $c
                'lockedDownCount=' + @(Get-UntrustedWriteGrantees $root $trusted).Count

                # Phase 3: root stays clean, but the SERVICE EXE ALONE carries a broad grant - must still fire,
                # scoped to the exe, because an inherited root grant is not the only way a binary is writable.
                $wk = [System.Security.Principal.WellKnownSidType]
                $auth = New-Object System.Security.Principal.SecurityIdentifier($wk::AuthenticatedUserSid, $null)
                $x = New-Object System.Security.AccessControl.FileSecurity
                $x.SetAccessRuleProtection($true, $false)
                foreach ($s in $trusted) { $x.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($s, 'FullControl', 'Allow'))) }
                $x.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($auth, 'Modify', 'Allow')))
                Set-Acl -LiteralPath "$root\svc.exe" -AclObject $x
                $exeFound = @(Get-UntrustedWriteGrantees "$root\svc.exe" $trusted)
                'exeOnlyCount=' + $exeFound.Count
                'exeNamesAuthUsers=' + (($exeFound -join ';') -match 'Authenticated Users')
                'rootStillCleanCount=' + @(Get-UntrustedWriteGrantees $root $trusted).Count

                # The running admin is trusted even with no explicit grant naming them - a folder they own and
                # can write to is a normal extraction, not a finding.
                'meIsTrusted=' + ($trusted -contains [Security.Principal.WindowsIdentity]::GetCurrent().User)
            }
            finally {
                if (Test-Path -LiteralPath $root) {
                    & icacls.exe $root /reset /T /C /Q 2>&1 | Out-Null
                    Remove-Item -LiteralPath $root -Recurse -Force
                }
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        var inherited = int.Parse(answers.Find(a => a.StartsWith("inheritedCount=", StringComparison.Ordinal))!.Substring("inheritedCount=".Length));
        Assert.True(inherited > 0, "a folder made directly under the system drive root must inherit at least one write grant outside SYSTEM/Administrators/TrustedInstaller/the admin - if this box's C:\\ no longer grants one, the pre-lock check has nothing to prove here: " + string.Join(" | ", answers));

        Assert.Contains("lockedDownCount=0", answers);
        Assert.Contains("exeOnlyCount=1", answers);
        Assert.Contains("exeNamesAuthUsers=True", answers);
        Assert.Contains("rootStillCleanCount=0", answers);
        Assert.Contains("meIsTrusted=True", answers);
    }

    /// <summary>
    /// #4043 round-1 review: the check must be silent on the ACEs a REAL install location actually carries,
    /// not only on the fully-trusted synthetic ACL the test above builds by hand. The coordinator ran the
    /// shipped (unfixed) function against real folders on this machine and found it refused
    /// <c>C:\Program Files</c> and <c>C:\Program Files\dotnet</c> - the very location the refusal message
    /// itself recommends - because both carry CREATOR OWNER in their default DACL, and CREATOR OWNER,
    /// CREATOR GROUP and their two SERVER twins are inherit-only templates: they grant nothing on an object
    /// that already exists, only rights a CHILD inherits when someone later creates one, and creating that
    /// child needs write/create rights on THIS path - already covered by every other principal this check
    /// tests.
    ///
    /// <para><b>Red-watch built into the pin.</b> A second, pre-round-1 copy of the function (no exclusion,
    /// no owner check) is reproduced inline - not read from git history - and run over the SAME captured
    /// DACL. It must still flag Program Files, proving this test would have failed before the fix and is
    /// not a tautology.</para>
    ///
    /// <para>Case (b) is the contrasting REFUSE, built the identical way (a captured DACL applied to a fresh
    /// temp folder) so the PASS and the REFUSE are apples to apples: <c>C:\</c>'s own DACL carries
    /// Authenticated Users: Modify, a real broad grant that must still be caught. Case (c) pins the exclusion
    /// to exactly the four documented SIDs, regardless of what this box's Program Files happens to carry.</para>
    /// </summary>
    [Fact]
    public void ThePreLockWritableExtractionCheck_IsSilentOnRealInheritOnlyTemplates_ButStillCatchesARealBroadGrant()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-UntrustedWriteGrantees"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'

            # The pre-round-1 shape: no inherit-only exclusion, no owner check. Reproduced inline purely to
            # prove this test would have failed before the fix (#4043 round-1 review).
            function Get-UntrustedWriteGrantees-Old([string]$path, [array]$trusted) {
                $rights = [System.Security.AccessControl.FileSystemRights]
                $allow = [System.Security.AccessControl.AccessControlType]::Allow
                $sidType = [System.Security.Principal.SecurityIdentifier]
                $write = [int64]($rights::WriteData -bor $rights::AppendData -bor $rights::Delete -bor $rights::DeleteSubdirectoriesAndFiles -bor $rights::ChangePermissions -bor $rights::TakeOwnership) -bor 0x10000000 -bor 0x40000000
                $acl = Get-Acl -LiteralPath $path -ErrorAction Stop
                @($acl.GetAccessRules($true, $true, $sidType) | Where-Object {
                    $_.AccessControlType -eq $allow -and (([int64]$_.FileSystemRights) -band $write) -ne 0 -and $trusted -notcontains $_.IdentityReference })
            }

            function New-TempAclFolder([string]$sourcePath) {
                $sourceAcl = Get-Acl -LiteralPath $sourcePath
                $sddl = $sourceAcl.GetSecurityDescriptorSddlForm([System.Security.AccessControl.AccessControlSections]::Access)
                $dir = Join-Path $env:TEMP ('pm4043-r1-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
                New-Item -ItemType Directory -Path $dir -Force | Out-Null
                $sec = Get-Acl -LiteralPath $dir
                $sec.SetSecurityDescriptorSddlForm($sddl, [System.Security.AccessControl.AccessControlSections]::Access)
                Set-Acl -LiteralPath $dir -AclObject $sec
                return $dir
            }

            $trusted = Get-DarlingPreLockTrustedSids

            # (a) A NEW folder carrying C:\Program Files' own DACL - the shape a fresh subfolder inherits.
            $pf = New-TempAclFolder 'C:\Program Files'
            try {
                'caseA_fixedCount=' + @(Get-UntrustedWriteGrantees $pf $trusted).Count
                'caseA_oldCount=' + @(Get-UntrustedWriteGrantees-Old $pf $trusted).Count
            }
            finally {
                icacls.exe $pf /reset /T /C /Q 2>&1 | Out-Null
                Remove-Item -LiteralPath $pf -Recurse -Force -ErrorAction SilentlyContinue
            }

            # (b) A NEW folder carrying C:\'s own DACL - Authenticated Users: Modify, the documented hole a
            # folder made directly under the drive root inherits. Must still be caught.
            $cRoot = New-TempAclFolder 'C:\'
            try {
                'caseB_fixedCount=' + @(Get-UntrustedWriteGrantees $cRoot $trusted).Count
            }
            finally {
                icacls.exe $cRoot /reset /T /C /Q 2>&1 | Out-Null
                Remove-Item -LiteralPath $cRoot -Recurse -Force -ErrorAction SilentlyContinue
            }

            # (c) All four inherit-only templates, explicitly, regardless of what this box's Program Files
            # happens to carry - pins the exclusion to exactly those four SIDs, not "whatever this test found".
            $wk = [System.Security.Principal.WellKnownSidType]
            $templates = @($wk::CreatorOwnerSid, $wk::CreatorGroupSid, $wk::CreatorOwnerServerSid, $wk::CreatorGroupServerSid)
            $tRoot = Join-Path $env:TEMP ('pm4043-r1-tmpl-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $tRoot -Force | Out-Null
            try {
                $sec = New-Object System.Security.AccessControl.DirectorySecurity
                $sec.SetAccessRuleProtection($true, $false)
                foreach ($s in $trusted) { $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($s, 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow'))) }
                foreach ($t in $templates) {
                    $sid = New-Object System.Security.Principal.SecurityIdentifier($t, $null)
                    $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($sid, 'FullControl', 'ContainerInherit, ObjectInherit', 'InheritOnly', 'Allow')))
                }
                Set-Acl -LiteralPath $tRoot -AclObject $sec
                'caseC_count=' + @(Get-UntrustedWriteGrantees $tRoot $trusted).Count
            }
            finally {
                icacls.exe $tRoot /reset /T /C /Q 2>&1 | Out-Null
                Remove-Item -LiteralPath $tRoot -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.Contains("caseA_fixedCount=0", answers);
        var oldCount = int.Parse(answers.Find(a => a.StartsWith("caseA_oldCount=", StringComparison.Ordinal))!.Substring("caseA_oldCount=".Length));
        Assert.True(oldCount > 0, "red-watch failed: the pre-round-1 shape must still flag C:\\Program Files' CREATOR OWNER ACE, or this test proves nothing about the fix: " + string.Join(" | ", answers));

        var caseB = int.Parse(answers.Find(a => a.StartsWith("caseB_fixedCount=", StringComparison.Ordinal))!.Substring("caseB_fixedCount=".Length));
        Assert.True(caseB > 0, "a folder carrying C:\\'s own DACL must still be refused for Authenticated Users: Modify - if this box's C:\\ no longer grants it, this test has nothing to prove: " + string.Join(" | ", answers));

        Assert.Contains("caseC_count=0", answers);
    }

    /// <summary>
    /// #4043 round-1 review: a re-run of install-darling.ps1 over a tree #4038 already locked - a repair, or
    /// this script used as its own upgrade path - is not a fresh extraction. #4038's lock itself grants the
    /// service account Modify on the root, and the UNFIXED check had no way to tell that grant apart from a
    /// stranger's: every single re-run or repair over an already-locked install would have refused itself
    /// over the grant #4038 itself made. <c>Get-DarlingPreLockTrustedSids</c> now takes the CURRENT service's
    /// logon account and trusts exactly what the lock granted - nothing more.
    ///
    /// <para><c>NT AUTHORITY\LOCAL SERVICE</c> stands in for the service account precisely because it is NOT
    /// one of the four SIDs the base trusted set already carries (SYSTEM, Administrators, TrustedInstaller,
    /// the running admin) - using TrustedInstaller itself here would pass by accident and prove nothing about
    /// the new parameter.</para>
    /// </summary>
    [Fact]
    public void ThePreLockWritableExtractionCheck_TrustsTheAccountTheLockItselfGranted_OnAnAlreadyLockedTree()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-UntrustedWriteGrantees"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Lock-DarlingInstallTree"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path ([IO.Path]::GetPathRoot([Environment]::SystemDirectory)) ('pm4043-r1-lock-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            try {
                New-Item -ItemType Directory -Path $root -Force | Out-Null
                $null = Lock-DarlingInstallTree $root 'NT AUTHORITY\LOCAL SERVICE'

                $withoutAccount = Get-DarlingPreLockTrustedSids
                'withoutAccountCount=' + @(Get-UntrustedWriteGrantees $root $withoutAccount).Count

                $withAccount = Get-DarlingPreLockTrustedSids 'NT AUTHORITY\LOCAL SERVICE'
                'withAccountCount=' + @(Get-UntrustedWriteGrantees $root $withAccount).Count
            }
            finally {
                if (Test-Path -LiteralPath $root) {
                    & icacls.exe $root /reset /T /C /Q 2>&1 | Out-Null
                    Remove-Item -LiteralPath $root -Recurse -Force
                }
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        var without = int.Parse(answers.Find(a => a.StartsWith("withoutAccountCount=", StringComparison.Ordinal))!.Substring("withoutAccountCount=".Length));
        Assert.True(without > 0, "a freshly locked tree must still name the service account as untrusted when the caller does not say who it is - otherwise this test proves nothing about the new parameter: " + string.Join(" | ", answers));
        Assert.Contains("withAccountCount=0", answers);
    }

    /// <summary>
    /// #4043 round-1 review: OWNER RIGHTS is deliberately not one of the four SIDs excluded above - unlike
    /// CREATOR OWNER, it can redefine what the owner may do instead of naming a risk itself, so what matters
    /// is WHO the owner is. An owner outside the trusted set holds WRITE_DAC and WRITE_OWNER implicitly and
    /// can grant itself anything regardless of the current DACL - the same fact
    /// <c>Lock-DarlingInstallTree</c>'s own post-lock walk already acts on ("owned by").
    ///
    /// <para>A real filesystem object cannot be given an untrusted owner without elevation this suite does
    /// not assume - Windows only lets WRITE_OWNER retarget to yourself or a group your token marks
    /// owner-capable, confirmed against this exact scenario while writing this test. So <c>Get-Acl</c> is
    /// shadowed instead, the same idiom
    /// <see cref="NetworkPathKind_WhenWmiIsUnavailable_FallsBackToPSDriveDisplayRoot"/> already uses, to hand
    /// back a fully-trusted DACL with only the OWNER outside the trusted set.</para>
    /// </summary>
    [Fact]
    public void ThePreLockWritableExtractionCheck_FlagsAnUntrustedOwner_EvenWithAFullyTrustedDacl()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine("""
            function Get-Acl {
                [CmdletBinding()]
                param([Parameter(ValueFromRemainingArguments = $true)] $Rest)
                $sec = New-Object System.Security.AccessControl.DirectorySecurity
                $sec.SetAccessRuleProtection($true, $false)
                foreach ($s in (Get-DarlingPreLockTrustedSids)) { $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($s, 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow'))) }
                $sec.SetOwner((New-Object System.Security.Principal.SecurityIdentifier([System.Security.Principal.WellKnownSidType]::BuiltinUsersSid, $null)))
                return $sec
            }
            """);
        probe.AppendLine(ExtractFunction(InstallScript, "Get-UntrustedWriteGrantees"));
        probe.AppendLine("""
            $trusted = Get-DarlingPreLockTrustedSids
            $found = @(Get-UntrustedWriteGrantees 'C:\does-not-need-to-exist-for-this-probe' $trusted)
            'count=' + $found.Count
            'text=' + ($found -join ';')
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.Contains("count=1", answers);
        Assert.Contains(answers, a => a.StartsWith("text=", StringComparison.Ordinal) && a.Contains("owned by BUILTIN\\Users", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// #4034, executed as shipped against a real tree. A folder created directly under the system drive root
    /// inherits "Authenticated Users: Modify" from it (the documented install location's hole). After the lock the
    /// root must be protected and grant ordinary users read and execute only, the service account Modify (it
    /// extracts pg-runtime into the tree), and every child that inherits must follow. A file with its own
    /// protected DACL (darling.json, as step 4b leaves it) keeps it. A child that grants a broad principal write
    /// EXPLICITLY, behind its own protection, is not something a folder lock can override, so it must come back
    /// in the result for the warning to name, not be silently passed over.
    /// </summary>
    [Fact]
    public void TheInstallTreeLock_ClosesTheInheritedGrant_KeepsProtectedFiles_AndReportsWhatItCannotClose()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Lock-DarlingInstallTree"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path ([IO.Path]::GetPathRoot([Environment]::SystemDirectory)) ('pm4034-test-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            try {
                New-Item -ItemType Directory -Path "$root\pg-runtime\pgsql\bin", "$root\planted" -Force | Out-Null
                Set-Content -LiteralPath "$root\pg-runtime\pgsql\bin\postgres.exe" -Value 'x'
                Set-Content -LiteralPath "$root\darling.json" -Value '{}'
                Set-Content -LiteralPath "$root\planted\evil.dll" -Value 'x'
                Set-Content -LiteralPath "$root\svc.exe" -Value 'x'
                $wk = [System.Security.Principal.WellKnownSidType]
                $sidType = [System.Security.Principal.SecurityIdentifier]
                $auth = New-Object System.Security.Principal.SecurityIdentifier($wk::AuthenticatedUserSid, $null)
                $users = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinUsersSid, $null)
                $interactive = New-Object System.Security.Principal.SecurityIdentifier($wk::InteractiveSid, $null)
                $system = New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)
                $admins = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)
                $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
                $service = (New-Object System.Security.Principal.NTAccount('NT SERVICE\TrustedInstaller')).Translate($sidType)
                'elevated=' + ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
                'me=' + $me.Value
                # darling.json as step 4b leaves it: protected, the service and the machine's own accounts in
                # full, INTERACTIVE reading.
                $j = New-Object System.Security.AccessControl.FileSecurity
                $j.SetAccessRuleProtection($true, $false)
                foreach ($s in @($system, $admins, $service)) { $j.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($s, 'FullControl', 'Allow'))) }
                $j.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($interactive, 'Read', 'Allow')))
                Set-Acl -LiteralPath "$root\darling.json" -AclObject $j
                # #4038 round 2's High: a child an ordinary user owns and grants to itself behind its own
                # protection, as one who re-created it while an older install was open would leave it.
                $p = New-Object System.Security.AccessControl.DirectorySecurity
                $p.SetAccessRuleProtection($true, $false)
                $p.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($me, 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow')))
                $p.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($auth, 'Modify', 'ContainerInherit, ObjectInherit', 'None', 'Allow')))
                Set-Acl -LiteralPath "$root\planted" -AclObject $p
                $x = New-Object System.Security.AccessControl.FileSecurity
                $x.SetAccessRuleProtection($true, $false)
                $x.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($me, 'FullControl', 'Allow')))
                Set-Acl -LiteralPath "$root\svc.exe" -AclObject $x
                # A junction inside the tree (to the tree's own pg-runtime, so cleanup never leaves it).
                & cmd.exe /c mklink /J "$root\planted-junction" "$root\pg-runtime" | Out-Null

                function Rules($path) { (Get-Acl -LiteralPath $path).GetAccessRules($true, $true, $sidType) }
                $trustedHere = @($system, $admins, $service, (New-Object System.Security.Principal.SecurityIdentifier($wk::CreatorOwnerSid, $null)))
                function Untrusted($path) { @((Get-Acl -LiteralPath $path).GetAccessRules($true, $false, $sidType) | Where-Object { $trustedHere -notcontains $_.IdentityReference }).Count }

                # Phase 1, as install step 1b2 runs it on a fresh install: no service account yet, so the tree is
                # locked and nothing is granted to a service.
                $null = Lock-DarlingInstallTree $root ''
                'phase1Protected=' + (Get-Acl -LiteralPath $root).AreAccessRulesProtected
                'phase1Service=' + @(Rules $root | Where-Object { $_.IdentityReference -eq $service }).Count

                # Phase 2, as step 4b2 and the upgrade run it: the service's grant, and the walk's report.
                $open = @(Lock-DarlingInstallTree $root 'NT SERVICE\TrustedInstaller')
                $open | ForEach-Object { 'open:' + $_.Substring($root.Length) }

                $rootAcl = Get-Acl -LiteralPath $root
                'protected=' + $rootAcl.AreAccessRulesProtected
                'authenticatedUsers=' + @(Rules $root | Where-Object { $_.IdentityReference -eq $auth }).Count
                'usersRights=' + ((Rules $root | Where-Object { $_.IdentityReference -eq $users } | ForEach-Object { $_.FileSystemRights }) -join ',')
                'serviceRights=' + ((Rules $root | Where-Object { $_.IdentityReference -eq $service } | ForEach-Object { $_.FileSystemRights }) -join ',')
                'postgresServiceInherited=' + @(Rules "$root\pg-runtime\pgsql\bin\postgres.exe" | Where-Object { $_.IdentityReference -eq $service -and $_.IsInherited }).Count
                'jsonProtected=' + (Get-Acl -LiteralPath "$root\darling.json").AreAccessRulesProtected
                'jsonInteractive=' + ((Rules "$root\darling.json" | Where-Object { $_.IdentityReference -eq $interactive } | ForEach-Object { $_.FileSystemRights }) -join ',')
                'jsonOwnerIsService=' + ((Get-Acl -LiteralPath "$root\darling.json").GetOwner($sidType) -eq $service)
                'plantedUntrusted=' + (Untrusted "$root\planted")
                'plantedInherits=' + (-not (Get-Acl -LiteralPath "$root\planted").AreAccessRulesProtected)
                'svcUntrusted=' + (Untrusted "$root\svc.exe")
                'svcInherits=' + (-not (Get-Acl -LiteralPath "$root\svc.exe").AreAccessRulesProtected)
                'svcOwnerIsAdmins=' + ((Get-Acl -LiteralPath "$root\svc.exe").GetOwner($sidType) -eq $admins)

                # The account spellings Win32_Service reports that do not translate as written. Last, since each
                # call grants the account it is handed.
                'localSystem=' + $(try { $null = Lock-DarlingInstallTree $root 'LocalSystem'; 'ok' } catch { 'threw: ' + $_.Exception.Message })
                $local = $env:USERDOMAIN -eq $env:COMPUTERNAME
                'dotAccount=' + $(if (-not $local) { 'skipped' } else { try { $null = Lock-DarlingInstallTree $root ('.\' + $env:USERNAME); 'ok' } catch { 'threw: ' + $_.Exception.Message } })

                # A root that is itself a junction is refused, not locked through.
                & cmd.exe /c mklink /J "$root-link" "$root" | Out-Null
                'junctionRoot=' + (@(Lock-DarlingInstallTree "$root-link" 'NT SERVICE\TrustedInstaller') -join ';').Contains('the install folder itself is a junction or link')
            }
            finally {
                # The test user owns every object here and so keeps WRITE_DAC: reset to inherited, then delete. The
                # junction goes first with rmdir, which removes the link and never what it points at.
                if (Test-Path -LiteralPath "$root-link") { & cmd.exe /c rmdir "$root-link" | Out-Null }
                if (Test-Path -LiteralPath $root) {
                    & icacls.exe $root /reset /T /C /Q 2>&1 | Out-Null
                    if (Test-Path -LiteralPath "$root\planted-junction") { & cmd.exe /c rmdir "$root\planted-junction" | Out-Null }
                    Remove-Item -LiteralPath $root -Recurse -Force
                }
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.Contains("phase1Protected=True", answers);
        Assert.Contains("phase1Service=0", answers);

        /* The High: the user-owned, self-granted children are closed where they stand (reset to the tree's ACEs,
           their own grant gone), not merely reported, while darling.json keeps its protected DACL. */
        Assert.Contains("plantedUntrusted=0", answers);
        Assert.Contains("plantedInherits=True", answers);
        Assert.Contains("svcUntrusted=0", answers);
        Assert.Contains("svcInherits=True", answers);

        /* Ownership is the half that needs elevation: icacls /setowner to Administrators. Elevated (CI), nothing
           is left but the junction, which the walk does not descend; darling.json belongs to the service and the
           planted binary to Administrators. Not elevated (a developer's shell), the lock cannot take ownership,
           and the walk says so object by object: every remaining entry is one this user still owns, never a
           writer. (icacls exits 0 under /C even when /setowner is denied, so the walk's owner check is what
           catches it, not the exit code.) */
        var open = answers.FindAll(a => a.StartsWith("open:", StringComparison.Ordinal));
        Assert.Contains(@"open:\planted-junction (a junction or link)", open);
        if (answers.Contains("elevated=True"))
        {
            Assert.Single(open);
            Assert.Contains("jsonOwnerIsService=True", answers);
            Assert.Contains("svcOwnerIsAdmins=True", answers);
        }
        else
        {
            var me = answers.Find(a => a.StartsWith("me=", StringComparison.Ordinal))!.Substring(3);
            var owned = open.FindAll(o => !o.Contains("junction or link", StringComparison.Ordinal) && !o.Contains("could not make", StringComparison.Ordinal));
            Assert.Contains(owned, o => o.Equals($"open: (owned by {me})", StringComparison.Ordinal));
            Assert.All(owned, o => Assert.EndsWith($"(owned by {me})", o, StringComparison.Ordinal));
        }

        Assert.Contains("junctionRoot=True", answers);
        Assert.Contains("localSystem=ok", answers);
        Assert.True(answers.Contains("dotAccount=ok") || answers.Contains("dotAccount=skipped"),
            "a .\\ account must be normalized to this computer, not thrown on: " + string.Join(" | ", answers));
        Assert.Contains("protected=True", answers);
        Assert.Contains("authenticatedUsers=0", answers);
        Assert.Contains("usersRights=ReadAndExecute, Synchronize", answers);
        Assert.Contains("serviceRights=Modify, Synchronize", answers);
        Assert.Contains("postgresServiceInherited=1", answers);
        Assert.Contains("jsonProtected=True", answers);
        Assert.Contains("jsonInteractive=Read, Synchronize", answers);
    }

    /// <summary>The #4034 block, its explaining comment included, exactly as a script ships it.</summary>
    private static string InstallTreeLockBlock(string script)
    {
        var start = script.IndexOf("# Lock the install tree against ordinary users (#4034).", StringComparison.Ordinal);
        Assert.True(start >= 0, "a Darling script no longer carries the #4034 install-tree lock");
        var function = script.IndexOf("function Lock-DarlingInstallTree", start, StringComparison.Ordinal);
        ExtractBracedBlockAt(script, script.IndexOf('{', function), out var end);
        return script.Substring(start, end - start + 1);
    }

    /// <summary>Returns the single line of <paramref name="script"/> containing <paramref name="marker"/>,
    /// verbatim — so a composition can be executed as shipped instead of retyped into a probe.</summary>
    private static string ExtractLine(string script, string marker)
    {
        var at = script.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(at >= 0, $"install-darling.ps1 no longer contains '{marker}' (#2185)");

        var start = script.LastIndexOf('\n', at) + 1;
        var end = script.IndexOf('\n', at);
        return (end < 0 ? script.Substring(start) : script.Substring(start, end - start)).Trim();
    }

    /// <summary>Runs <paramref name="script"/> under Windows PowerShell 5.1 and returns its non-empty output
    /// lines. Written to a temp file rather than passed with -Command: the script under test is a whole
    /// function body, and quoting it through a command line is a source of failures that have nothing to do
    /// with what is being tested.</summary>
    private static List<string> RunWindowsPowerShell(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-2187-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script);
        try
        {
            var startInfo = new ProcessStartInfo("powershell.exe", $"-NoProfile -ExecutionPolicy Bypass -File \"{path}\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            /* A test host started from PowerShell 7 (CI's step shell) hands down a PSModulePath that points
               Windows PowerShell 5.1 at PowerShell 7's modules, and 5.1 then cannot autoload the ones it ships:
               Get-Acl and Set-Acl, in Microsoft.PowerShell.Security, fail with "the module could not be loaded"
               (#4038's first CI run). Without the variable, 5.1 builds its own default, as an operator's console
               does. */
            startInfo.Environment.Remove("PSModulePath");
            using var process = Process.Start(startInfo);
            Assert.NotNull(process);

            var stdout = process!.StandardOutput.ReadToEnd();
            var stderr = process.StandardError.ReadToEnd();
            process.WaitForExit(60_000);

            Assert.True(string.IsNullOrWhiteSpace(stderr), $"powershell.exe reported an error running the extracted function:\n{stderr}");

            var lines = new List<string>();
            foreach (var line in stdout.Split('\n'))
            {
                var trimmed = line.Trim();
                if (trimmed.Length > 0) { lines.Add(trimmed); }
            }

            return lines;
        }
        finally
        {
            try { File.Delete(path); } catch (IOException) { /* best-effort */ }
        }
    }

    /// <summary>Returns the full <c>function NAME(...) { ... }</c> definition text from the script —
    /// signature included, so the extracted copy takes its parameters the way the shipped one does.</summary>
    private static string ExtractFunction(string script, string name)
    {
        var start = script.IndexOf("function " + name, StringComparison.Ordinal);
        Assert.True(start >= 0, $"install-darling.ps1 no longer defines {name} (#2187)");

        ExtractBracedBlockAt(script, script.IndexOf('{', start), out var end);
        return script.Substring(start, end - start + 1);
    }

    /// <summary>Returns the body of the first brace-balanced block introduced by <paramref name="header"/> —
    /// the same idiom <c>DarlingFirewallCheckTests</c> uses.</summary>
    private static string ExtractBracedBlock(string script, string header)
    {
        var start = script.IndexOf(header, StringComparison.Ordinal);
        Assert.True(start >= 0, $"expected '{header}' in the script");
        return ExtractBracedBlockAt(script, script.IndexOf('{', start), out _);
    }

    private static string ExtractBracedBlockAt(string script, int open, out int end)
    {
        Assert.True(open >= 0, "expected an opening brace");

        var depth = 0;
        for (var i = open; i < script.Length; i++)
        {
            if (script[i] == '{') { depth++; }
            else if (script[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    end = i;
                    return script.Substring(open + 1, i - open - 1);
                }
            }
        }

        Assert.Fail("unbalanced braces while extracting a block from install-darling.ps1");
        end = -1;
        return string.Empty;
    }
}
