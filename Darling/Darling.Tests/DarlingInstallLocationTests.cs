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
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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
            ("Initialize-DarlingMinimalConfig $configPath", "writing the minimal darling.json"),
            ("New-EventLog -LogName Application", "registering the Event Log source"),
            ("Invoke-ScWithQuotedBinPath 'create'", "creating the service"),
            ("Set-Acl -LiteralPath $secretFile", "hardening the config's ACL"),
            ("Start-Service -Name $serviceName", "starting the service"),
        })
        {
            var at = script.IndexOf(marker, StringComparison.Ordinal);
            Assert.True(at >= 0, $"install-darling.ps1 no longer contains {what} ('{marker}')");
            Assert.True(guard < at, $"the install-location guard must run BEFORE {what}, or a doomed location still leaves debris behind (#2187)");
        }
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
    /// it can actually be broken — the installer's only <c>Set-Acl</c> targets are the credential file it
    /// has always hardened and the store folder it creates before the service does, never the install tree.
    ///
    /// <para>The store folder is the parent of <c>postgres.dataDirectory</c>
    /// (<c>%ProgramData%\PerformanceMonitorDarling</c> unless <c>darling.json</c> says otherwise), not a
    /// directory of the install tree. It holds the PostgreSQL data directory and every role credential. An
    /// ordinary user can create a folder under <c>C:\ProgramData</c> and would then own it, so the installer
    /// makes Administrators its owner and locks it before the service's first start. Its one <c>Set-Acl</c> is
    /// pinned as tightly as the credential file's two: it sits in the function that locks the store folder,
    /// and that function's only caller passes the folder <c>Get-DarlingStoreFolder</c> reads out of
    /// <c>darling.json</c>. Where that setting points is an operator's choice, which no source pin can see.</para>
    /// </summary>
    [Fact]
    public void TheInstaller_NeverAclsTheInstallTree_OnlyTheCredentialFilesAndTheStoreFolder()
    {
        var script = InstallScript;

        /* The script names every path with -LiteralPath since #4745, so this looks for that spelling: a loop over
           "Set-Acl -Path " would find no call and check nothing. Each call is read for the path it names, so a
           call on anything else fails here instead of being counted. */
        var targets = new List<string>();
        foreach (Match call in Regex.Matches(script, @"Set-Acl -LiteralPath (\S+)"))
        {
            targets.Add(call.Groups[1].Value);
        }

        targets.Sort(StringComparer.Ordinal);
        var found = string.Join(", ", targets);
        Assert.True(
            found == "$secretFile, $secretFile",
            $"expected Set-Acl on the credential file twice (the hardened DACL and the owner) and nowhere else, found: {found}");

        /* The store folder's DACL is written through the handle the function opened on the folder itself (never by
           name, so a link put at the name cannot redirect it), inside the function that locks the store folder.
           That function has one caller, and the caller passes $storeFolder, which is assigned once, from
           Get-DarlingStoreFolder. Neither the install root nor the script's own folder reaches it. */
        var storeFolderFunction = ExtractFunction(script, "Initialize-DarlingStoreFolder");
        Assert.Contains("$lock.WriteDacl(", storeFolderFunction, StringComparison.Ordinal);
        Assert.DoesNotContain("Set-Acl", storeFolderFunction, StringComparison.Ordinal);

        var callers = Regex.Matches(script, @"(?<!function )Initialize-DarlingStoreFolder (\S+)");
        Assert.True(
            callers.Count == 1 && callers[0].Groups[1].Value == "$storeFolder",
            "Initialize-DarlingStoreFolder must have one caller, and it must pass $storeFolder");
        Assert.Single(Regex.Matches(script, @"\$storeFolder\s*="));
        Assert.Contains("$storeFolder = Get-DarlingStoreFolder $configPath", script, StringComparison.Ordinal);

        /* And the install root is never the target of Set-Acl, and the script never runs a bare "icacls $root".
           That is all these three say: Lock-DarlingInstallTree does run icacls.exe on the root, on purpose
           (#4034), and the icacls lines the script prints for the operator quote the path, so neither matches. */
        Assert.DoesNotContain("Set-Acl -Path $root", script, StringComparison.Ordinal);
        Assert.DoesNotContain("Set-Acl -LiteralPath $root", script, StringComparison.Ordinal);
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
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingCanonicalPath"));
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
        probe.AppendLine(ExtractFunction(script, "Get-DarlingCanonicalPath"));
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
    /// #4052, from a PowerShell 5.1 run on a standalone Windows 11 box (the live lock test above runs the lock as
    /// TrustedInstaller, which is trusted everywhere, so it cannot see these):
    /// <list type="bullet">
    /// <item>the service stays trusted on darling.json and its backups. Step 4b gives it an explicit FullControl there
    /// (#1647); without the trust the walk's explicit-ACE branch strips that grant and the service cannot read its
    /// own config;</item>
    /// <item>the bring-your-own key folder is handed to the lock as a NAME, because the lock Join-Paths it onto the
    /// root and Join-Path does not treat a rooted child as rooted;</item>
    /// <item>the helper avoids <c>Split-Path -LiteralPath -Parent</c>, an ambiguous parameter set on PowerShell 5.1.</item>
    /// </list>
    /// </summary>
    [Fact]
    public void TheNarrowedLock_KeepsTheServiceOnItsConfig_AndHandsTheKeyFolderOverAsAName()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        var lockText = ExtractFunction(InstallScript, "Lock-DarlingInstallTree");
        var helper = ExtractFunction(InstallScript, "Get-DarlingExtraServiceWriteDirectories");

        Assert.Contains("$isSecretFile = (-not $target.PSIsContainer) -and ([IO.Path]::GetDirectoryName($path).TrimEnd('\\') -ieq $root.TrimEnd('\\')) -and (Test-DarlingServiceWrittenFileName $target.Name)", lockText, StringComparison.Ordinal);
        Assert.Contains("-or $isServiceWritePath -or $isSecretFile)) { $trustedHere += $serviceSid }", lockText, StringComparison.Ordinal);
        Assert.Contains("return @((Get-DarlingServiceWrittenNames).KeyDirectories)", helper, StringComparison.Ordinal);
        Assert.Contains("(Get-DarlingServiceWrittenNames).Directories", lockText, StringComparison.Ordinal);
        Assert.Contains("$names.Files", ExtractFunction(InstallScript, "Test-DarlingServiceWrittenFileName"), StringComparison.Ordinal);
        Assert.Contains("$names.BackupPrefix", ExtractFunction(InstallScript, "Test-DarlingServiceWrittenFileName"), StringComparison.Ordinal);
        Assert.Contains("'darling-keys'", ExtractFunction(InstallScript, "Get-DarlingServiceWrittenNames"), StringComparison.Ordinal);
        Assert.Equal(PerformanceMonitor.Darling.Service.DarlingLogHashKeyFile.BringYourOwnDirectoryName, "darling-keys");
        Assert.DoesNotContain("(Split-Path -LiteralPath $configPath -Parent)", helper, StringComparison.Ordinal);
        Assert.Equal(helper, ExtractFunction(upgrade, "Get-DarlingExtraServiceWriteDirectories"));
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
        Assert.Equal(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"), ExtractFunction(upgrade, "Get-LocalAdministratorsDirectMemberSids"));
        Assert.Equal(ExtractFunction(InstallScript, "Resolve-DarlingServiceAccountSid"), ExtractFunction(upgrade, "Resolve-DarlingServiceAccountSid"));
        Assert.Equal(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSidsForRerun"), ExtractFunction(upgrade, "Get-DarlingPreLockTrustedSidsForRerun"));

        /* #5627: the service-written list, its two tests, the service SIDs, the path helper they use and the text builders. */
        foreach (var name in new[]
        {
            "Get-DarlingServiceSids", "Get-DarlingServiceWrittenNames", "Test-DarlingServiceWrittenFileName",
            "Get-DarlingFileNameCount", "Initialize-DarlingNativeOwner", "Get-DarlingCanonicalPath", "Get-DarlingSegmentsBelowRoot", "Test-DarlingServiceWrittenPath", "Get-DarlingServiceWrittenRootFiles", "Test-PathIsAtOrUnder", "Get-DarlingConfiguredPathsUnder",
            "Get-DarlingNewFolderPhrase", "Get-DarlingExistingInstallSteps", "Get-DarlingFreshFolderAdvice",
            "Get-DarlingRefusalAdvice", "Get-DarlingBackupFileFindingNames", "Get-DarlingBackupFileAdvice", "Test-DarlingSamePath", "Get-DarlingInstallRootFromService",
        })
        {
            Assert.Equal(ExtractFunction(InstallScript, name), ExtractFunction(upgrade, name));
        }
    }

    /// <summary>
    /// #5627: the service SIDs derive ONLY this product's own service SID, from its service name, and the base trusted
    /// set derives none. No other NT SERVICE SID, and no NT SERVICE authority as a whole, may ever join either. Both
    /// scripts hand the service name on whether or not the service is registered.
    /// </summary>
    [Fact]
    public void TheServiceSids_DeriveOnlyTheProductsOwnServiceSid_AndTheBaseSetDerivesNone()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));

        foreach (var script in new[] { InstallScript, upgrade })
        {
            var trust = ExtractFunction(script, "Get-DarlingServiceSids");
            Assert.Contains("[string]$serviceName", trust, StringComparison.Ordinal);
            Assert.Contains("$serviceName.ToUpperInvariant()", trust, StringComparison.Ordinal);
            Assert.Contains("\"S-1-5-80-$($nameParts -join '-')\"", trust, StringComparison.Ordinal);
            Assert.DoesNotContain("NT SERVICE\\*", trust, StringComparison.Ordinal);

            var baseSet = ExtractFunction(script, "Get-DarlingPreLockTrustedSids");
            Assert.DoesNotContain("S-1-5-80", baseSet, StringComparison.Ordinal);
            Assert.DoesNotContain("$serviceName", baseSet, StringComparison.Ordinal);

            // Both paths of the re-run function hand the service name on, registered or not.
            var rerun = ExtractFunction(script, "Get-DarlingPreLockTrustedSidsForRerun");
            Assert.Contains("Get-DarlingServiceSids $null $serviceName", rerun, StringComparison.Ordinal);
            Assert.Contains("Get-DarlingServiceSids $existingAccount $serviceName", rerun, StringComparison.Ordinal);
        }

        // Every install-folder call site goes through the re-run function with the service name.
        Assert.Contains("Get-DarlingPreLockTrustedSidsForRerun $serviceName $existing", InstallScript, StringComparison.Ordinal);
        Assert.Contains("Get-DarlingPreLockTrustedSidsForRerun $serviceName $existingService", upgrade, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5627: the two source-build checks of upgrade-darling.ps1 (the folder SHA256SUMS.txt sits in, and a folder
    /// -Source) use the base trusted set, which names no service account, so a file or write grant held by the
    /// service's own account in a source folder is a finding. Pinned at both call sites and run against a tree whose
    /// pg-runtime the service owns.
    /// </summary>
    [Fact]
    public void TheSourceBuildChecks_TrustNoServiceAccount()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));

        Assert.Contains("$sourceTrusted = @(Get-DarlingPreLockTrustedSids)", upgrade, StringComparison.Ordinal);
        Assert.Contains("$sumsFolderWritable = @(Get-UntrustedWriteGrantees $sourceRoot $sourceTrusted -Recurse)", upgrade, StringComparison.Ordinal);
        Assert.Contains("$writable = @(Get-UntrustedWriteGrantees $Source $sourceTrusted -Recurse)", upgrade, StringComparison.Ordinal);
        foreach (var line in upgrade.Split('\n').Where(l => l.Contains("Get-UntrustedWriteGrantees $Source", StringComparison.Ordinal) || l.Contains("Get-UntrustedWriteGrantees $sourceRoot", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("-ServiceSids", line, StringComparison.Ordinal);
            Assert.DoesNotContain("$preLockTrusted", line, StringComparison.Ordinal);
        }

        var probe = ServiceOwnedTreeProbe();
        probe.AppendLine("""
            try {
                # The service owns pg-runtime, as in every tree above. As a source folder it is named.
                $source = @(Get-UntrustedWriteGrantees $script:root @(Get-DarlingPreLockTrustedSids) -Recurse | ForEach-Object { $_ -replace [regex]::Escape($script:root), '<root>' })
                'source.count=' + $source.Count
                'source.owned=' + (@($source | Where-Object { $_ -like '*owned by*' -and $_ -like '<root>\pg-runtime*' }).Count -gt 0)
                'source.grant=' + (@($source | Where-Object { $_ -like '*on <root>\pg-runtime*' }).Count -gt 0)
            }
            finally {
                Remove-Item -LiteralPath $script:root -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunServiceOwnedTreeProbe(probe);

        Assert.DoesNotContain("source.count=0", answers);
        Assert.Contains("source.owned=True", answers);
        Assert.Contains("source.grant=True", answers);
    }

    /// <summary>
    /// #5627, text pin: the refusal for an EXISTING install (the service is registered) gives the numbered steps
    /// (stop the service, a new empty folder, check darling.json and copy it first, run install-darling.ps1 from
    /// there, start and check, delete the old folder), and both scripts choose them by where the registered service
    /// runs from. With no registered service, or one that runs from another folder, the refusal keeps the
    /// fresh-folder wording, whatever the folder holds.
    /// </summary>
    [Fact]
    public void TheExistingInstallRefusal_GivesTheNumberedSteps_AndAFreshFolderKeepsTheFreshWording()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));

        var probe = new StringBuilder();
        AppendStepFunctions(probe);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            'STEPS'
            Get-DarlingExistingInstallSteps 'C:\PerformanceMonitorDarling' 'PerformanceMonitor Darling'
            'FRESH'
            Get-DarlingFreshFolderAdvice 'C:\PerformanceMonitorDarling'
            """);
        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));
        var rendered = all[..all.IndexOf("FRESH", StringComparison.Ordinal)];
        var fresh = all[all.IndexOf("FRESH", StringComparison.Ordinal)..];

        Assert.Contains("1. Stop the 'PerformanceMonitor Darling' service and leave it stopped until step 5.", rendered, StringComparison.Ordinal);
        Assert.Contains("2. In an elevated session, extract the new zip into", rendered, StringComparison.Ordinal);
        Assert.Contains("a new, empty folder named C:\\Program Files\\PerformanceMonitorDarling.", rendered, StringComparison.Ordinal);
        Assert.Contains("3. Open darling.json in C:\\PerformanceMonitorDarling", rendered, StringComparison.Ordinal);
        Assert.Contains("check that every setting in it is yours", rendered, StringComparison.Ordinal);
        Assert.Contains("copies the SAMPLE config in", rendered, StringComparison.Ordinal);
        Assert.Contains("If darling.json names a certificate or key file in this folder, make a new one. Keep it outside the", rendered, StringComparison.Ordinal);
        Assert.Contains("install folder, and point darling.json at it. These are the tls pfxPath, certPath and keyPath settings.", rendered, StringComparison.Ordinal);
        Assert.DoesNotContain("copy that file", rendered, StringComparison.Ordinal);
        Assert.Contains("4. In an elevated session, run install-darling.ps1 from the new folder.", rendered, StringComparison.Ordinal);
        Assert.Contains("C:\\ProgramData\\PerformanceMonitorDarling", rendered, StringComparison.Ordinal);
        Assert.Contains("sets postgres.dataDirectory, it is there instead.", rendered, StringComparison.Ordinal);
        Assert.Contains("5. Start the service and check that it collects.", rendered, StringComparison.Ordinal);
        Assert.Contains("6. Delete C:\\PerformanceMonitorDarling once the service is collecting.", rendered, StringComparison.Ordinal);

        Assert.Contains("Extract the zip into a new folder under C:\\Program Files, or another folder only an administrator can write to.", fresh, StringComparison.Ordinal);
        Assert.Contains("run install-darling.ps1 from there", fresh, StringComparison.Ordinal);
        Assert.DoesNotContain("Stop the", fresh, StringComparison.Ordinal);

        // Both scripts choose the steps by where the registered service runs from: no darling.json or pg-runtime test.
        Assert.Contains("$advice = Get-DarlingRefusalAdvice $root $serviceName (Get-DarlingInstallRootFromService $serviceName) $writableRecords", InstallScript, StringComparison.Ordinal);
        Assert.DoesNotContain("$existingInstall", InstallScript, StringComparison.Ordinal);
        Assert.Contains("$(Get-DarlingRefusalAdvice $InstallRoot $serviceName (Get-DarlingInstallRootFromService $serviceName) $writableRecords)", upgrade, StringComparison.Ordinal);
        Assert.DoesNotContain("Get-DarlingExistingInstallSteps $InstallRoot", upgrade, StringComparison.Ordinal);
        Assert.DoesNotContain("Get-DarlingExistingInstallSteps $root", InstallScript, StringComparison.Ordinal);
        Assert.DoesNotContain("a fresh install into a folder only an administrator can write to", upgrade, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5627: inside C:\Program Files the steps and the fresh wording say a new, empty folder with a DIFFERENT name
    /// under C:\Program Files, and never tell the user to extract into the folder they are in.
    /// </summary>
    [Fact]
    public void TheSteps_InsideProgramFiles_AskForADifferentFolderName()
    {
        var probe = new StringBuilder();
        AppendStepFunctions(probe);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $inPf = Join-Path $env:ProgramFiles 'PerformanceMonitorDarling'
            'STEPS'
            Get-DarlingExistingInstallSteps $inPf 'PerformanceMonitor Darling'
            'FRESH'
            Get-DarlingFreshFolderAdvice $inPf
            """);
        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));
        var steps = all[..all.IndexOf("FRESH", StringComparison.Ordinal)];
        var fresh = all[all.IndexOf("FRESH", StringComparison.Ordinal)..];

        Assert.Contains("a new, empty folder with a different name under C:\\Program Files.", steps, StringComparison.Ordinal);
        Assert.DoesNotContain("named C:\\Program Files\\PerformanceMonitorDarling", steps, StringComparison.Ordinal);
        Assert.Contains("a new, empty folder with a different name under C:\\Program Files.", fresh, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5627: every absolute path darling.json names inside the old folder (the store, a certificate or key, a file:
    /// reference) is listed in the last step, which says to keep the old folder until they are moved. A path outside
    /// the folder, a relative path and a folder sharing only a name prefix are not listed, and a darling.json that
    /// names nothing inside keeps the plain delete step.
    /// </summary>
    [Fact]
    public void TheLastStep_NamesEveryConfiguredPathUnderTheOldFolder_AndKeepsTheFolderUntilTheyMove()
    {
        var probe = new StringBuilder();
        AppendStepFunctions(probe);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $old = Join-Path $env:TEMP ('pm5627-old-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $old -Force | Out-Null
            $json = $old -replace '\\', '\\'
            try {
                $config = @"
            {
              // a comment that mentions "dataDirectory": "C:\\nowhere" must not count
              "postgres": { "dataDirectory": "$json\\data", },
              "tls": { "pfxPath": "$json\\certs\\a.pfx", "certPath": "C:\\elsewhere\\a.pem", "keyPath": "relative\\a.key" },
              "smtp": { "password": "file:$json\\secrets\\smtp.txt" },
              "other": { "dataDirectory": "$json-sibling\\data" }
            }
            "@
                Set-Content -LiteralPath (Join-Path $old 'darling.json') -Value $config
                'STEPS'
                Get-DarlingExistingInstallSteps $old 'PerformanceMonitor Darling'
                'CONFIGURED'
                (Get-DarlingConfiguredPathsUnder $old).Lines
                'incomplete=' + (Get-DarlingConfiguredPathsUnder $old).Incomplete
                Set-Content -LiteralPath (Join-Path $old 'darling.json') -Value '{ "postgres": { "managed": true } }'
                'CLEAN'
                'cleanCount=' + @((Get-DarlingConfiguredPathsUnder $old).Lines).Count
                Get-DarlingExistingInstallSteps $old 'PerformanceMonitor Darling'
            }
            finally {
                Remove-Item -LiteralPath $old -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);
        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));
        var steps = all[..all.IndexOf("CONFIGURED", StringComparison.Ordinal)];
        var configured = all[all.IndexOf("CONFIGURED", StringComparison.Ordinal)..all.IndexOf("CLEAN", StringComparison.Ordinal)];
        var clean = all[all.IndexOf("CLEAN", StringComparison.Ordinal)..];

        Assert.Contains("\\data\" (postgres.dataDirectory).", steps, StringComparison.Ordinal);
        Assert.Contains("\\certs\\a.pfx\" (pfxPath).", steps, StringComparison.Ordinal);
        Assert.Contains("\\secrets\\smtp.txt\" (a file: reference).", steps, StringComparison.Ordinal);
        Assert.Contains("incomplete=False", configured, StringComparison.Ordinal);
        Assert.Contains("6. Do not delete ", steps, StringComparison.Ordinal);
        Assert.Contains("darling.json names these paths inside it:", steps, StringComparison.Ordinal);
        Assert.DoesNotContain("elsewhere", configured, StringComparison.Ordinal);
        Assert.DoesNotContain("relative", configured, StringComparison.Ordinal);
        Assert.DoesNotContain("nowhere", configured, StringComparison.Ordinal);
        Assert.DoesNotContain("-sibling", configured, StringComparison.Ordinal);

        Assert.Contains("cleanCount=0", clean, StringComparison.Ordinal);
        Assert.Contains("6. Delete ", clean, StringComparison.Ordinal);
        Assert.DoesNotContain("Do not delete", clean, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5627: when the service's SID cannot be worked out, the walk says so on one line and the objects that account
    /// owns are listed as before. A plain ASCII name produces no line.
    /// </summary>
    [Fact]
    public void TheServiceSids_SayWhenTheSidCannotBeWorkedOut()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingServiceSids"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            function Resolve-DarlingServiceAccountSid([string]$account) { return $null }
            'plain.count=' + @(Get-DarlingServiceSids $null 'PerformanceMonitor Darling').Count
            'wide.count=' + @(Get-DarlingServiceSids $null ('Perf' + [char]0x00E9 + 'Monitor')).Count
            """);
        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.Contains("plain.count=1", answers);
        Assert.Contains("wide.count=0", answers);
        Assert.Single(answers.FindAll(a => a.StartsWith("WARNING: could not work out the Windows SID of NT SERVICE\\", StringComparison.Ordinal)));
    }

    /// <summary>
    /// #5627: a Modify grant the service's own account holds on the install folder is a finding like any other
    /// account's. The lock gives the service read and execute there, and the walk reads the folder with inherited
    /// grants, so it names the grant with the service deleted, on another logon, and on its virtual account.
    /// </summary>
    [Fact]
    public void ThePreLockTrustedSet_NamesAModifyGrantTheServiceHoldsOnTheInstallFolder_InEveryServiceState()
    {
        var probe = ServiceOwnedTreeProbe();
        probe.AppendLine("""
            try {
                $script:rootWriter = $script:serviceSid
                Show 'rootModifyDeleted' $null $null
                Show 'rootModifyOtherLogon' $registered 'NT AUTHORITY\LOCAL SERVICE'
                Show 'rootModifyVirtual' $registered "NT SERVICE\$serviceName"
            }
            finally {
                Remove-Item -LiteralPath $script:root -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunServiceOwnedTreeProbe(probe);

        foreach (var label in new[] { "rootModifyDeleted", "rootModifyOtherLogon", "rootModifyVirtual" })
        {
            Assert.Contains($"{label}.count=1", answers);
            // Windows names the account only while the service is registered, so the finding may carry the raw SID.
            Assert.Contains(answers, a => a.StartsWith($"{label}.text=", StringComparison.Ordinal)
                && (a.Contains("PerformanceMonitor Darling", StringComparison.Ordinal) || a.Contains("944500918", StringComparison.Ordinal))
                && a.Contains(" on <root>", StringComparison.Ordinal)
                && !a.Contains("owned by", StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// #5627: the refusal gives the numbered steps only when the registered service runs from the folder being
    /// refused (its ImagePath folder is that folder, compared as full paths without regard to case or a trailing
    /// separator). A service registered elsewhere, none, or a path that could not be read all get the fresh-folder
    /// wording.
    /// </summary>
    [Fact]
    public void TheRefusalAdvice_GivesTheSteps_OnlyWhenTheServiceRunsFromThisFolder()
    {
        var probe = new StringBuilder();
        AppendStepFunctions(probe);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $here = 'C:\PerformanceMonitorDarling'
            'THIS'
            Get-DarlingRefusalAdvice $here 'PerformanceMonitor Darling' 'c:\performancemonitordarling\'
            'ELSEWHERE'
            Get-DarlingRefusalAdvice $here 'PerformanceMonitor Darling' 'C:\Program Files\PerformanceMonitorDarling'
            'SIBLING'
            Get-DarlingRefusalAdvice $here 'PerformanceMonitor Darling' 'C:\PerformanceMonitorDarling-old'
            'NONE'
            Get-DarlingRefusalAdvice $here 'PerformanceMonitor Darling' ''
            """);
        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));
        string Section(string name, string? next) => all[all.IndexOf(name + "\n", StringComparison.Ordinal)..(next == null ? all.Length : all.IndexOf(next + "\n", StringComparison.Ordinal))];

        var thisFolder = Section("THIS", "ELSEWHERE");
        Assert.Contains("1. Stop the 'PerformanceMonitor Darling' service", thisFolder, StringComparison.Ordinal);
        Assert.Contains("6. Delete C:\\PerformanceMonitorDarling once the service is collecting.", thisFolder, StringComparison.Ordinal);

        foreach (var other in new[] { Section("ELSEWHERE", "SIBLING"), Section("SIBLING", "NONE"), Section("NONE", null) })
        {
            Assert.Contains("Extract the zip into a new folder under C:\\Program Files", other, StringComparison.Ordinal);
            Assert.DoesNotContain("Stop the", other, StringComparison.Ordinal);
            Assert.DoesNotContain("darling.json", other, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// #5627: a darling.json value that is not a printable path (a control character, a line break, or any of the
    /// characters &lt; &gt; | and a quote) is left out, so is one over 260 characters, and at most 10 are listed. The
    /// refusal and every step still print, no left-out value is printed, the printed ones are quoted, and the last
    /// step keeps the old folder and asks for darling.json to be checked.
    /// </summary>
    [Fact]
    public void TheConfiguredPaths_LeaveOutValuesThatAreNotPaths_AndTheStepsStillPrintQuoted()
    {
        var probe = new StringBuilder();
        AppendStepFunctions(probe);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $old = Join-Path $env:TEMP ('pm5627-odd-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $old -Force | Out-Null
            $json = $old -replace '\\', '\\'
            $esc = [string][char]27
            $lf = "`n"
            $long = 'a' * 300
            function Use([string[]]$lines) {
                Set-Content -LiteralPath (Join-Path $old 'darling.json') -Value (($lines -join "`n").Replace('@@', $json))
            }
            try {
                Use @(
                    '{',
                    '  "tls": { "pfxPath": "@@\\bad<>|\"name.pfx",',
                    ('            "keyPath": "@@\\line1' + $lf + 'line2.key",'),
                    ('            "certPath": "@@\\esc' + $esc + '[31mred.pem" },'),
                    ('  "postgres": { "dataDirectory": "@@\\' + $long + '" },'),
                    '  "smtp": { "password": "file:@@\\good\\smtp.txt" }',
                    '}')
                $r = Get-DarlingConfiguredPathsUnder $old
                'MIXED'
                'lines=' + @($r.Lines).Count
                'incomplete=' + $r.Incomplete
                Get-DarlingExistingInstallSteps $old 'PerformanceMonitor Darling'

                Use @('{', '  "tls": { "pfxPath": "@@\\bad<>|\"name.pfx" }', '}')
                $r = Get-DarlingConfiguredPathsUnder $old
                'ONLYODD'
                'lines=' + @($r.Lines).Count
                'incomplete=' + $r.Incomplete
                Get-DarlingExistingInstallSteps $old 'PerformanceMonitor Darling'

                Use @('{', ((1..12 | ForEach-Object { '  "postgres": { "dataDirectory": "@@\\d' + $_ + '" },' }) -join "`n"), '}')
                $r = Get-DarlingConfiguredPathsUnder $old
                'MANY'
                'lines=' + @($r.Lines).Count
                'incomplete=' + $r.Incomplete
            }
            finally {
                Remove-Item -LiteralPath $old -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);
        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));
        var mixed = all[all.IndexOf("MIXED", StringComparison.Ordinal)..all.IndexOf("ONLYODD", StringComparison.Ordinal)];
        var onlyOdd = all[all.IndexOf("ONLYODD", StringComparison.Ordinal)..all.IndexOf("MANY", StringComparison.Ordinal)];
        var many = all[all.IndexOf("MANY", StringComparison.Ordinal)..];

        // The good one is listed; the four odd ones and the 300-character one are not.
        Assert.Contains("lines=1", mixed, StringComparison.Ordinal);
        Assert.Contains("incomplete=True", mixed, StringComparison.Ordinal);
        Assert.Contains("1. Stop the 'PerformanceMonitor Darling' service", mixed, StringComparison.Ordinal);
        Assert.Contains("good\\smtp.txt\" (a file: reference).", mixed, StringComparison.Ordinal);
        Assert.Contains("6. Do not delete ", mixed, StringComparison.Ordinal);
        Assert.Contains("Not every value in darling.json was listed here. Check it for other paths inside", mixed, StringComparison.Ordinal);
        Assert.DoesNotContain(new string('a', 30), mixed, StringComparison.Ordinal);
        Assert.DoesNotContain("(postgres.dataDirectory)", mixed, StringComparison.Ordinal);
        foreach (var raw in new[] { "bad<", "name.pfx", "line1", "line2", "red.pem", "[31m", "\u001b" })
        {
            Assert.DoesNotContain(raw, mixed, StringComparison.Ordinal);
        }

        // Nothing could be listed: the old folder is still kept, and darling.json is to be checked.
        Assert.Contains("lines=0", onlyOdd, StringComparison.Ordinal);
        Assert.Contains("incomplete=True", onlyOdd, StringComparison.Ordinal);
        Assert.Contains("1. Stop the 'PerformanceMonitor Darling' service", onlyOdd, StringComparison.Ordinal);
        Assert.Contains("6. Do not delete ", onlyOdd, StringComparison.Ordinal);
        Assert.Contains("Not every value in darling.json was listed here. Check it for paths", onlyOdd, StringComparison.Ordinal);
        Assert.DoesNotContain("bad<", onlyOdd, StringComparison.Ordinal);
        Assert.DoesNotContain("6. Delete ", onlyOdd, StringComparison.Ordinal);

        // The list stops at 10.
        Assert.Contains("lines=10", many, StringComparison.Ordinal);
        Assert.Contains("incomplete=True", many, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5627: the service-written list matches the names the service writes and nothing wider. A backup is
    /// any darling.json.bak-* name (#5649: the glob the hardening covers), the FIRST name below the install folder is
    /// compared exactly (a trailing dot, a longer name or a '..' step matches nothing), the listed files count only in
    /// the install folder itself, and the key folder counts only when it is handed in.
    /// </summary>
    [Fact]
    public void TheServiceWrittenList_MatchesOnlyTheNamesTheServiceWrites()
    {
        var probe = new StringBuilder();
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            # Real files in a temp root: the files-only answer counts the file's names on disk (#5649), so a path that does not
            # exist is never one the service is trusted on.
            $root = Join-Path $env:TEMP ('pm5649-wp-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $root -Force | Out-Null
            foreach ($n in @('darling.json', 'darling.json.bak-20260101-120000', 'darling.json.bak-x.dll', 'darling.json.bak-before-edit', 'darling.json.bak-x', 'notes.txt')) { Set-Content -LiteralPath "$root\$n" -Value '{}' }
            New-Item -ItemType HardLink -Path "$root\darling.json.bak-linked" -Value "$root\notes.txt" | Out-Null
            $fileItem = [pscustomobject]@{ PSIsContainer = $false; LinkType = $null; Attributes = [IO.FileAttributes]::Normal }
            foreach ($name in @('darling.json', 'DARLING.JSON', 'darling.json.bak-20260101-120000', 'darling.json.bak-20260101-120000-2',
                                'darling.json.bak-x.dll', 'darling.json.bak-20260101', 'darling.json.bak-before-edit', 'DARLING.JSON.BAK-Upper', 'darling.json.bak-', 'darling.json.bak', 'darling.jsonbak-x', 'xdarling.json.bak-x', 'darling.json.', 'xdarling.json')) {
                'name:' + $name + '=' + (Test-DarlingServiceWrittenFileName $name)
            }
            foreach ($case in @(
                @('pg-runtime', @()), @('pg-runtime\pgsql\bin\postgres.exe', @()), @('pg-runtime-prev\a.dll', @()), @('PG-RUNTIME\a.dll', @()),
                @('pg-runtime.\a.dll', @()), @('pg-runtime-x\a.dll', @()), @('pg-runtimeX', @()), @('pg-runtime\..\a.dll', @()), @('other\pg-runtime\a.dll', @()),
                @('darling.json', @()), @('darling.json.bak-20260101-120000', @()), @('darling.json.bak-x.dll', @()), @('darling.json.bak-before-edit', @()), @('xdarling.json.bak-x', @()), @('darling.json.bak', @()), @('other\darling.json', @()), @('darling.json\x', @()),
                @('darling-keys\log-hash.key', @()), @('darling-keys\log-hash.key', @('darling-keys')), @('darling-keys-x\a', @('darling-keys')), @('PerformanceMonitor.Darling.Service.exe', @()))) {
                'path:' + $case[0] + '|' + @($case[1]).Count + '=' + (Test-DarlingServiceWrittenPath $root (Join-Path $root $case[0]) @($case[1]) $fileItem)
            }
            # #5649: the files-only answer comes from the item the caller listed, never from the disk.
            'item:folder=' + (Test-DarlingServiceWrittenPath $root (Join-Path $root 'darling.json.bak-x') @() ([pscustomobject]@{ PSIsContainer = $true; LinkType = $null; Attributes = [IO.FileAttributes]::Directory }))
            'item:hardLink=' + (Test-DarlingServiceWrittenPath $root (Join-Path $root 'darling.json.bak-linked') @() $fileItem)
            'item:missingFile=' + (Test-DarlingServiceWrittenPath $root (Join-Path $root 'darling.json.bak-missing') @() $fileItem)
            'item:reparse=' + (Test-DarlingServiceWrittenPath $root (Join-Path $root 'darling.json.bak-x') @() ([pscustomobject]@{ PSIsContainer = $false; LinkType = 'SymbolicLink'; Attributes = [IO.FileAttributes]::ReparsePoint }))
            'item:none=' + (Test-DarlingServiceWrittenPath $root (Join-Path $root 'darling.json.bak-x') @())
            'item:plainFile=' + (Test-DarlingServiceWrittenPath $root (Join-Path $root 'darling.json.bak-x') @() $fileItem)
            'item:directoryNeedsNoItem=' + (Test-DarlingServiceWrittenPath $root (Join-Path $root 'pg-runtime\a.dll') @())
            'path:root=' + (Test-DarlingServiceWrittenPath $root $root @('darling-keys'))
            'path:outside=' + (Test-DarlingServiceWrittenPath $root "$root-2\pg-runtime\a.dll" @())
            Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        foreach (var yes in new[] { "darling.json", "DARLING.JSON", "darling.json.bak-20260101-120000", "darling.json.bak-20260101-120000-2",
            "darling.json.bak-x.dll", "darling.json.bak-20260101", "darling.json.bak-before-edit", "DARLING.JSON.BAK-Upper", "darling.json.bak-" })
        {
            Assert.Contains($"name:{yes}=True", answers);
        }

        foreach (var no in new[] { "darling.json.bak", "darling.jsonbak-x", "xdarling.json.bak-x", "darling.json.", "xdarling.json" })
        {
            Assert.Contains($"name:{no}=False", answers);
        }

        foreach (var yes in new[] { "pg-runtime|0", "pg-runtime\\pgsql\\bin\\postgres.exe|0", "pg-runtime-prev\\a.dll|0", "PG-RUNTIME\\a.dll|0", "darling.json|0", "darling.json.bak-20260101-120000|0", "darling.json.bak-x.dll|0", "darling.json.bak-before-edit|0", "darling-keys\\log-hash.key|1" })
        {
            Assert.Contains($"path:{yes}=True", answers);
        }

        foreach (var no in new[] { "pg-runtime.\\a.dll|0", "pg-runtime-x\\a.dll|0", "pg-runtimeX|0", "pg-runtime\\..\\a.dll|0", "other\\pg-runtime\\a.dll|0", "xdarling.json.bak-x|0", "darling.json.bak|0",
            "other\\darling.json|0", "darling.json\\x|0", "darling-keys\\log-hash.key|0", "darling-keys-x\\a|1", "PerformanceMonitor.Darling.Service.exe|0" })
        {
            Assert.Contains($"path:{no}=False", answers);
        }

        Assert.Contains("path:root=False", answers);
        Assert.Contains("path:outside=False", answers);

        Assert.Contains("item:plainFile=True", answers);
        Assert.Contains("item:directoryNeedsNoItem=True", answers);
        foreach (var no in new[] { "item:folder", "item:hardLink", "item:missingFile", "item:reparse", "item:none" })
        {
            Assert.Contains($"{no}=False", answers);
        }
    }

    /// <summary>
    /// #5649: a name Windows folds into another (a trailing dot or a trailing space) is not on the service-written list,
    /// whatever its start: it resolves to a DIFFERENT file, and the path test has always refused it. The name test and the
    /// path test must give the same answer for it, so the lock and the install pass never act on a file the pre-lock walk
    /// declined to trust. The plain backup name keeps matching. Run under Windows PowerShell 5.1 and PowerShell 7, with
    /// both scripts.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheServiceWrittenFileName_RefusesANameWindowsFolds_AndTheNameAndPathTestsAgree(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Get-UntrustedWriteGrantees"));
        AppendPathHelpers(probe, script);
        AppendServiceWrittenList(probe, script);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path $env:TEMP ('pm5649-fold-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $root -Force | Out-Null
            Set-Content -LiteralPath "$root\darling.json.bak-x" -Value 'x'
            'setup.backupFileMade=' + (Test-Path -LiteralPath "$root\darling.json.bak-x")
            $fileItem = [pscustomobject]@{ PSIsContainer = $false; LinkType = $null; Attributes = [IO.FileAttributes]::Normal }
            try {
                foreach ($name in @('darling.json.bak-x', 'DARLING.JSON.BAK-X', 'darling.json.bak-x.', 'darling.json.bak-x ', 'darling.json.bak-x..', 'darling.json.bak-x. ', 'darling.json.bak-.', 'darling.json.bak- ', '')) {
                    'name:[' + $name + ']=' + (Test-DarlingServiceWrittenFileName $name)
                    if ($name -ne '') { 'path:[' + $name + ']=' + (Test-DarlingServiceWrittenPath $root ($root + '\' + $name) @() $fileItem) }
                }
            }
            finally { Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        AssertProbeSetupTook(answers);
        Assert.Contains("name:[darling.json.bak-x]=True", answers);
        Assert.Contains("name:[DARLING.JSON.BAK-X]=True", answers);
        Assert.Contains("path:[darling.json.bak-x]=True", answers);
        foreach (var folded in new[] { "darling.json.bak-x.", "darling.json.bak-x ", "darling.json.bak-x..", "darling.json.bak-x. ", "darling.json.bak-.", "darling.json.bak- " })
        {
            Assert.Contains($"name:[{folded}]=False", answers);
            Assert.Contains($"path:[{folded}]=False", answers);
        }

        Assert.Contains("name:[]=False", answers);
    }

    /// <summary>
    /// #5649: the pre-lock walk names each finding once, and Windows paths are compared without regard to case, so the same
    /// path spelled in two cases is one finding too. The probe runs the two de-duplication lines exactly as the script
    /// ships them over findings whose text differs only in case, under both shells (the way a comparer is passed to
    /// <c>New-Object</c> is the part most likely to differ between them).
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheWalksDeDuplication_TreatsTheSamePathInTwoCasesAsOneFinding(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var seenLine = ExtractLine(script, "$seen = New-Object 'System.Collections.Generic.HashSet[string]'");
        var uniqueLine = ExtractLine(script, "$unique = @(foreach ($found in $result)");
        Assert.Contains("OrdinalIgnoreCase", seenLine, StringComparison.Ordinal);

        var probe = new StringBuilder();
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $result = @(
                [pscustomobject]@{ Kind = 'Grant'; Path = 'C:\Pm\Folder'; Text = 'BUILTIN\Users on C:\Pm\Folder'; IsFile = $false },
                [pscustomobject]@{ Kind = 'Grant'; Path = 'c:\pm\folder'; Text = 'BUILTIN\Users on c:\pm\folder'; IsFile = $false },
                [pscustomobject]@{ Kind = 'Grant'; Path = 'C:\Pm\Folder'; Text = 'BUILTIN\Users on C:\Pm\Folder'; IsFile = $false },
                [pscustomobject]@{ Kind = 'Grant'; Path = 'C:\Pm\Other'; Text = 'BUILTIN\Users on C:\Pm\Other'; IsFile = $false })
            """);
        probe.AppendLine(seenLine);
        probe.AppendLine(uniqueLine);
        probe.AppendLine("""
            'unique=' + @($unique).Count
            'first=' + $unique[0].Text
            'second=' + $unique[1].Text
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        Assert.Contains("unique=2", answers);
        Assert.Contains(@"first=BUILTIN\Users on C:\Pm\Folder", answers);
        Assert.Contains(@"second=BUILTIN\Users on C:\Pm\Other", answers);
    }

    /// <summary>
    /// #5649: the pre-lock walk trusts the service account (as grantee and as owner) on EVERY file in the install root
    /// whose name starts with darling.json.bak-, the glob the hardening covers, so a backup an operator named by hand
    /// (the hardening makes the service its owner at the next start) is not read as a stranger's file. Narrow: any other
    /// grantee on that file still refuses, the service on any other root file still refuses, a folder with that name is
    /// not a file the service writes, and a file below the root is not in the root.
    /// </summary>
    [Fact]
    public void ThePreLockWalk_TrustsTheServiceOnAnyHandNamedBackupInTheRoot_AndNothingWider()
    {
        var probe = ServiceOwnedTreeProbe();
        probe.AppendLine("""
            $usersSid = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinUsersSid, $null)
            $extras = @('darling.json.bak-before-edit', 'DARLING.JSON.BAK-Upper', 'darling.json.bak', 'notes.txt', 'darling.json.bak-dir', 'other\darling.json.bak-nested')
            function New-Extras {
                foreach ($e in $extras) {
                    if ($e -eq 'darling.json.bak-dir') { New-Item -ItemType Directory -Path "$($script:root)\$e" -Force | Out-Null }
                    else { Set-Content -LiteralPath "$($script:root)\$e" -Value '{}' }
                }
            }
            function Reset-Extras {
                $script:serviceOwned = @()
                $script:fileGrants = @{}
            }
            function Own([string]$relative, $grantees) { $script:serviceOwned += $relative; $script:fileGrants[$relative] = @($grantees) }
            try {
                New-Extras
                'setup.extrasCreated=' + (@($extras | Where-Object { Test-Path -LiteralPath "$($script:root)\$_" }).Count -eq $extras.Count)

                # The hand-named backup the service owns and holds a grant on: passes, registered or not.
                Own 'darling.json.bak-before-edit' @($script:serviceSid)
                Own 'DARLING.JSON.BAK-Upper' @($script:serviceSid)
                Show 'handNamed' $registered "NT SERVICE\$serviceName"
                Show 'handNamedDeleted' $null $null
                Reset-Extras

                # The same file granted to Users as well: still refuses, naming Users and not the service.
                Own 'darling.json.bak-before-edit' @($script:serviceSid, $usersSid)
                Show 'handNamedUsers' $registered "NT SERVICE\$serviceName"
                Reset-Extras

                # The service owning and holding a grant on a root file that is not a backup: refuses (grant and owner).
                Own 'notes.txt' @($script:serviceSid)
                Show 'notBackup' $registered "NT SERVICE\$serviceName"
                Reset-Extras
                Own 'darling.json.bak' @($script:serviceSid)
                Show 'noDash' $registered "NT SERVICE\$serviceName"
                Reset-Extras

                # A folder named like a backup, owned by the service: not a file the service writes.
                Own 'darling.json.bak-dir' @()
                Show 'folder' $registered "NT SERVICE\$serviceName"
                Reset-Extras

                # A backup-named file below the root: the root only.
                Own 'other\darling.json.bak-nested' @($script:serviceSid)
                Show 'nested' $registered "NT SERVICE\$serviceName"
                Reset-Extras

                # #5649: a backup-named file with a second name (a hard link to a file outside the root) is never one the
                # service writes: it is reported as a finding of its own, and the service's grant and ownership on it
                # are named like a stranger's.
                $outside = "$($script:root)-outside.txt"
                Set-Content -LiteralPath $outside -Value 'x'
                New-Item -ItemType HardLink -Path "$($script:root)\darling.json.bak-linked" -Value $outside | Out-Null
                'setup.hardLinkMade=' + ((Get-Item -LiteralPath "$($script:root)\darling.json.bak-linked").LinkType -eq 'HardLink')
                Own 'darling.json.bak-linked' @($script:serviceSid)
                Show 'hardLink' $registered "NT SERVICE\$serviceName"
                Reset-Extras
                Remove-Item -LiteralPath $outside -Force
            }
            finally {
                Remove-Item -LiteralPath $script:root -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunServiceOwnedTreeProbe(probe);

        Assert.Contains("handNamed.count=0", answers);
        Assert.Contains("handNamedDeleted.count=0", answers);

        Assert.Contains("handNamedUsers.count=1", answers);
        Assert.Contains(answers, a => a.StartsWith("handNamedUsers.text=", StringComparison.Ordinal)
            && a.Contains("Users on <root>\\darling.json.bak-before-edit", StringComparison.Ordinal)
            && !a.Contains("owned by", StringComparison.Ordinal));

        foreach (var (label, file) in new[] { ("notBackup", "notes.txt"), ("noDash", "darling.json.bak"), ("nested", "other\\darling.json.bak-nested") })
        {
            Assert.Contains($"{label}.count=2", answers);
            Assert.Contains(answers, a => a.StartsWith($"{label}.text=", StringComparison.Ordinal)
                && a.Contains(" on <root>\\" + file, StringComparison.Ordinal)
                && a.Contains("<root>\\" + file + " (owned by", StringComparison.Ordinal));
        }

        Assert.Contains("folder.count=1", answers);
        Assert.Contains(answers, a => a.StartsWith("folder.text=", StringComparison.Ordinal)
            && a.Contains("<root>\\darling.json.bak-dir (owned by", StringComparison.Ordinal));

        Assert.Contains("hardLink.count=3", answers);
        Assert.Contains(answers, a => a.StartsWith("hardLink.text=", StringComparison.Ordinal)
            && a.Contains("<root>\\darling.json.bak-linked (a hard link: this file has more than one name", StringComparison.Ordinal)
            && a.Contains("on <root>\\darling.json.bak-linked", StringComparison.Ordinal)
            && a.Contains("<root>\\darling.json.bak-linked (owned by", StringComparison.Ordinal));
    }

    /// <summary>
    /// #5649: the refusal's advice. When EVERY finding is a grant or an owner on a darling.json.bak-* file directly in the
    /// install folder, a note comes FIRST: it names the files, says a backup can hold the secrets that were in
    /// darling.json and that the files can be moved to a folder only Administrators can read. The standard advice follows
    /// it unchanged (the service owns the files it writes, so a backup that is a finding belongs to, or is writable by,
    /// some other account, and the folder is replaced as usual). The decision is made from each finding's kind and path,
    /// never its text: any other finding (another file, a folder, a link, a file with more than one name, a path inside
    /// a folder, a path that could not be read or listed, darling.json itself, the folder) keeps the standard advice
    /// alone, even beside a backup-named file whose name makes the text of other findings look like a backup's. Both scripts.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheRefusalAdvice_PutsTheBackupNoteAheadOfTheStandardAdvice_OnlyWhenEveryFindingIsABackup(string scriptName)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        AppendStepFunctions(probe, script);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path $env:TEMP ('pm5649-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path "$root\other", "$root\darling.json.bak-dir" -Force | Out-Null
            foreach ($n in @('darling.json.bak-before-edit', 'darling.json.bak-20260101-120000', 'darling.json.bak-x', 'notes.txt', 'darling.json', 'other\darling.json.bak-nested')) { Set-Content -LiteralPath "$root\$n" -Value '{}' }
            $root = [System.IO.Path]::GetFullPath($root)
            function F([string]$kind, [string]$path, [bool]$isFile = $true) { [pscustomobject]@{ Kind = $kind; Path = $path; Text = "$kind $path"; IsFile = $isFile } }
            function Show([string]$label, [array]$findings, [string]$registered) {
                '@@' + $label
                Get-DarlingRefusalAdvice $root 'PerformanceMonitor Darling' $registered $findings
            }
            try {
                $all = @((F 'Grant' "$root\darling.json.bak-before-edit"), (F 'Owner' "$root\darling.json.bak-before-edit"), (F 'Grant' "$root\darling.json.bak-20260101-120000"))
                Show 'backupsOnly' $all ''
                Show 'backupsOnlyInstalledHere' $all $root
                Show 'withOtherFile' ($all + (F 'Grant' "$root\notes.txt")) ''
                Show 'withFolder' ($all + (F 'Grant' "$root\darling.json.bak-dir" $false)) ''
                Show 'withLink' ($all + (F 'Link' "$root\darling.json.bak-before-edit")) ''
                Show 'withHardLink' ($all + (F 'HardLink' "$root\darling.json.bak-before-edit")) ''
                Show 'withNested' ($all + (F 'Grant' "$root\other\darling.json.bak-nested")) ''
                Show 'withConfig' ($all + (F 'Grant' "$root\darling.json")) ''
                Show 'withRoot' ($all + (F 'Grant' $root $false)) ''
                Show 'withUnreadable' ($all + (F 'Unreadable' "$root\darling.json.bak-x")) ''
                Show 'withUnlisted' ($all + (F 'Unlisted' "$root\darling.json.bak-x" $false)) ''
                Show 'noFindings' @() ''
                Show 'noFindingsInstalledHere' @() $root

                # A real file called darling.json.bak-x, and findings whose rendered text would read as that file's:
                # none of them is a grant or an owner on a file in the root, so none of them gets the note.
                $x = "$root\darling.json.bak-x (owned by y)"
                Show 'craftedLink' @((F 'Owner' "$root\darling.json.bak-x"), [pscustomobject]@{ Kind = 'Link'; Path = $x; Text = "$x (a junction or link)"; IsFile = $true }) ''
                Show 'craftedNested' @((F 'Owner' "$root\darling.json.bak-x"), [pscustomobject]@{ Kind = 'Owner'; Path = "$x\evil.dll"; Text = "$x\evil.dll (owned by DOM\eve)"; IsFile = $true }) ''
                Show 'craftedUnlisted' @((F 'Owner' "$root\darling.json.bak-x"), [pscustomobject]@{ Kind = 'Unlisted'; Path = $x; Text = "$x (its contents could not be listed: UnauthorizedAccessException)"; IsFile = $true }) ''
                Show 'craftedUnreadable' @((F 'Owner' "$root\darling.json.bak-x"), [pscustomobject]@{ Kind = 'Unreadable'; Path = $x; Text = "$x (its permissions could not be read: Access is denied.)"; IsFile = $true }) ''
                Show 'plainTextFindings' @("$root\darling.json.bak-x (owned by y)") ''
            }
            finally {
                Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var text = string.Join("\n", RunWindowsPowerShell(probe.ToString())).Replace("\r\n", "\n", StringComparison.Ordinal);
        string Section(string label) => System.Text.RegularExpressions.Regex.Match(text, "@@" + label + "\n(?<body>.*?)(?=\n@@|$)", System.Text.RegularExpressions.RegexOptions.Singleline).Groups["body"].Value;

        const string Note = "Every entry above is a darling.json.bak-* backup file in this folder: darling.json.bak-before-edit, darling.json.bak-20260101-120000.";
        foreach (var (label, standard) in new[] { ("backupsOnly", "noFindings"), ("backupsOnlyInstalledHere", "noFindingsInstalledHere") })
        {
            var advice = Section(label);
            var standardAdvice = Section(standard).Trim();
            Assert.False(string.IsNullOrWhiteSpace(standardAdvice));
            Assert.StartsWith(Note, advice, StringComparison.Ordinal);
            Assert.Contains("A backup can hold the secrets that were in darling.json.", advice, StringComparison.Ordinal);
            Assert.Contains("These files can be moved out of this folder, into a", advice, StringComparison.Ordinal);
            Assert.Contains("folder only Administrators can read.", advice, StringComparison.Ordinal);
            Assert.Contains(standardAdvice, advice, StringComparison.Ordinal);
            Assert.True(advice.IndexOf(Note, StringComparison.Ordinal) < advice.IndexOf(standardAdvice, StringComparison.Ordinal), "the note comes first");
            Assert.DoesNotContain("You do not need to move the install", advice, StringComparison.Ordinal);
        }

        Assert.Contains("Do NOT try to repair this folder's permissions in place.", Section("backupsOnly"), StringComparison.Ordinal);
        Assert.Contains("Stop the 'PerformanceMonitor Darling' service", Section("backupsOnlyInstalledHere"), StringComparison.Ordinal);

        foreach (var label in new[] { "withOtherFile", "withFolder", "withLink", "withHardLink", "withNested", "withConfig", "withRoot", "withUnreadable", "withUnlisted",
            "craftedLink", "craftedNested", "craftedUnlisted", "craftedUnreadable", "plainTextFindings", "noFindings" })
        {
            var advice = Section(label);
            Assert.Equal(Section("noFindings").Trim(), advice.Trim());
            Assert.Contains("Do NOT try to repair this folder's permissions in place.", advice, StringComparison.Ordinal);
            Assert.DoesNotContain("backup file in this folder", advice, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// #5649: the walk's own records feed the advice. A tree whose only findings are a grant on a root-level backup file
    /// gets the note; the same tree with one more finding of another kind next to it (an owner below a folder, a junction,
    /// a folder named like a backup, a hard-linked backup) does not. The walk reports each finding's kind and whether the
    /// item it listed is a file, so no text is parsed and no second look at the disk happens.
    /// </summary>
    [Fact]
    public void TheWalksRecords_DecideTheBackupNote_AndNoTextIsParsed()
    {
        var probe = ServiceOwnedTreeProbe();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingBackupFileFindingNames"));
        probe.AppendLine("""
            $usersSid = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinUsersSid, $null)
            function ShowRecords([string]$label) {
                $sets = Get-DarlingPreLockTrustedSidsForRerun $serviceName $registered
                $records = @(Get-UntrustedWriteGrantees $script:root $sets.Trusted -AsRecords -Recurse -ServiceSids $sets.ServiceSids -ServiceDirectories $script:serviceDirs)
                $label + '.kinds=' + (($records | ForEach-Object { $_.Kind + ':' + $_.IsFile }) -join ',')
                $label + '.names=' + (@(Get-DarlingBackupFileFindingNames $script:root $records) -join ',')
                $label + '.texts=' + ((@(Get-UntrustedWriteGrantees $script:root $sets.Trusted -Recurse -ServiceSids $sets.ServiceSids -ServiceDirectories $script:serviceDirs)) -join ' ; ' -replace [regex]::Escape($script:root), '<root>')
            }
            $script:logon = "NT SERVICE\$serviceName"
            function Get-DarlingServiceLogonName([string]$name) { return $script:logon }
            $outside = "$($script:root)-outside.txt"
            try {
                Set-Content -LiteralPath "$($script:root)\darling.json.bak-before-edit" -Value '{}'
                New-Item -ItemType Directory -Path "$($script:root)\darling.json.bak-dir" -Force | Out-Null
                'setup.backupMade=' + (Test-Path -LiteralPath "$($script:root)\darling.json.bak-before-edit")

                $script:fileGrants['darling.json.bak-before-edit'] = @($usersSid)
                ShowRecords 'backupGrant'

                $script:serviceOwned = @('other\tool.dll')
                ShowRecords 'plusNestedOwner'
                $script:serviceOwned = @()

                $script:serviceOwned = @('darling.json.bak-dir')
                ShowRecords 'plusFolder'
                $script:serviceOwned = @()

                & cmd.exe /c mklink /J "$($script:root)\linked-dir" "$($script:root)\other" | Out-Null
                'setup.junctionMade=' + (Test-Path -LiteralPath "$($script:root)\linked-dir")
                ShowRecords 'plusJunction'
                & cmd.exe /c rmdir "$($script:root)\linked-dir" | Out-Null

                Set-Content -LiteralPath $outside -Value 'x'
                New-Item -ItemType HardLink -Path "$($script:root)\darling.json.bak-linked" -Value $outside | Out-Null
                'setup.hardLinkMade=' + ((Get-Item -LiteralPath "$($script:root)\darling.json.bak-linked").LinkType -eq 'HardLink')
                ShowRecords 'plusHardLink'
            }
            finally {
                if (Test-Path -LiteralPath "$($script:root)\linked-dir") { & cmd.exe /c rmdir "$($script:root)\linked-dir" | Out-Null }
                Remove-Item -LiteralPath $outside -Force -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath $script:root -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunServiceOwnedTreeProbe(probe);

        Assert.Contains("backupGrant.names=darling.json.bak-before-edit", answers);
        Assert.Contains("backupGrant.kinds=Grant:True", answers);
        foreach (var label in new[] { "plusNestedOwner", "plusFolder", "plusJunction", "plusHardLink" })
        {
            Assert.Contains($"{label}.names=", answers);
            Assert.DoesNotContain($"{label}.names=darling.json.bak-before-edit", answers);
        }

        Assert.Contains(answers, a => a.StartsWith("plusNestedOwner.kinds=", StringComparison.Ordinal) && a.Contains("Grant:True", StringComparison.Ordinal) && a.Contains("Owner:True", StringComparison.Ordinal));
        Assert.Contains(answers, a => a.StartsWith("plusFolder.kinds=", StringComparison.Ordinal) && a.Contains("Owner:False", StringComparison.Ordinal));
        Assert.Contains(answers, a => a.StartsWith("plusJunction.kinds=", StringComparison.Ordinal) && a.Contains("Link:False", StringComparison.Ordinal));
        Assert.Contains(answers, a => a.StartsWith("plusHardLink.kinds=", StringComparison.Ordinal) && a.Contains("HardLink:True", StringComparison.Ordinal));
    }

    /// <summary>
    /// #5649: the root files the lock and the install pass act on. Only plain files with a name on the service-written
    /// list; a hard-linked backup is left out (the service as owner, or a FullControl grant, would land on the other name
    /// too), and so is a backup whose name differs only by case under Turkish culture rules ('I' and 'i' fold
    /// differently there, so a culture-sensitive match would drop DARLING.JSON.BAK-B or keep a look-alike).
    /// </summary>
    [Fact]
    public void TheServiceWrittenRootFiles_LeaveOutHardLinks_AndMatchTheBackupPrefixByOrdinalNameUnderAnyCulture()
    {
        var probe = new StringBuilder();
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path $env:TEMP ('pm5649-rf-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path "$root\darling.json.bak-dir" -Force | Out-Null
            foreach ($n in @('darling.json', 'darling.json.bak-a', 'DARLING.JSON.BAK-B', 'notes.txt', 'darling.jsonx', 'darling.json.bak')) { Set-Content -LiteralPath "$root\$n" -Value '{}' }
            New-Item -ItemType HardLink -Path "$root\darling.json.bak-linked" -Value "$root\notes.txt" | Out-Null
            $outside = "$root-outside.txt"
            Set-Content -LiteralPath $outside -Value 'x'
            New-Item -ItemType HardLink -Path "$root\darling.json.bak-outside" -Value $outside | Out-Null
            function Names($files) { (@($files | ForEach-Object { $_.Name } | Sort-Object { $_.ToLowerInvariant() }) -join ',') }
            try {
                'setup.hardLinksMade=' + ((Get-Item -LiteralPath "$root\darling.json.bak-linked").LinkType -eq 'HardLink' -and (Get-Item -LiteralPath "$root\darling.json.bak-outside").LinkType -eq 'HardLink')
                'all=' + (Names (Get-DarlingServiceWrittenRootFiles $root))
                'backups=' + (Names (Get-DarlingServiceWrittenRootFiles $root -BackupsOnly))

                [System.Threading.Thread]::CurrentThread.CurrentCulture = New-Object System.Globalization.CultureInfo 'tr-TR'
                'setup.turkishCultureTook=' + (-not ('DARLING.JSON.BAK-B' -like 'darling.json.bak-*'))
                'tr.all=' + (Names (Get-DarlingServiceWrittenRootFiles $root))
                'tr.backups=' + (Names (Get-DarlingServiceWrittenRootFiles $root -BackupsOnly))
                foreach ($name in @('DARLING.JSON.BAK-X', 'darling.json.bak-x', 'Darling.Json.Bak-X', 'DARLING.JSON')) {
                    'tr.name:' + $name + '=' + (Test-DarlingServiceWrittenFileName $name)
                }
                $lookAlike = 'DARL' + [char]0x130 + 'NG.JSON.BAK-X'
                'tr.lookAlike=' + (Test-DarlingServiceWrittenFileName $lookAlike)
                [System.Threading.Thread]::CurrentThread.CurrentCulture = New-Object System.Globalization.CultureInfo 'en-US'
                'en.lookAlike=' + (Test-DarlingServiceWrittenFileName $lookAlike)
            }
            finally {
                Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath $outside -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());
        AssertProbeSetupTook(answers);

        Assert.Contains("all=darling.json,darling.json.bak-a,DARLING.JSON.BAK-B", answers);
        Assert.Contains("backups=darling.json.bak-a,DARLING.JSON.BAK-B", answers);
        Assert.Contains("tr.all=darling.json,darling.json.bak-a,DARLING.JSON.BAK-B", answers);
        Assert.Contains("tr.backups=darling.json.bak-a,DARLING.JSON.BAK-B", answers);
        foreach (var name in new[] { "DARLING.JSON.BAK-X", "darling.json.bak-x", "Darling.Json.Bak-X", "DARLING.JSON" })
        {
            Assert.Contains($"tr.name:{name}=True", answers);
        }

        Assert.Contains("tr.lookAlike=False", answers);
        Assert.Contains("en.lookAlike=False", answers);
    }

    /// <summary>
    /// #5649: one definition of which files are a config backup, per language, and the two are the same text. In
    /// PowerShell it is BackupPrefix in the list both scripts share, tested with an ordinal, case-insensitive StartsWith
    /// at every site (the pre-lock walk, the lock and the install-time hardening through Get-DarlingServiceWrittenRootFiles,
    /// and the upgrade's manifest guard): -like and -Filter fold case by the current culture and, in Windows PowerShell
    /// 5.1, match some look-alike characters, so they can disagree with the service. In C# it is DarlingConfigBackup,
    /// which TryHardenConfigBackups and the CLI's permission check read, with the same ordinal test. Nothing spells the
    /// name a second time, and the stamped-name pattern is gone.
    /// </summary>
    [Fact]
    public void TheBackupPrefix_HasOneDefinitionPerLanguage_AndTheSameText()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        var worker = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var cli = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCliCommands.cs"));

        foreach (var script in new[] { InstallScript, upgrade })
        {
            var prefix = System.Text.RegularExpressions.Regex.Matches(ExtractFunction(script, "Get-DarlingServiceWrittenNames"), @"BackupPrefix\s*=\s*'(?<g>[^']+)'");
            Assert.Single(prefix);
            Assert.Equal(PerformanceMonitor.Darling.Service.DarlingConfigBackup.Prefix, prefix[0].Groups["g"].Value);
            Assert.Contains("$name.StartsWith($names.BackupPrefix, [StringComparison]::OrdinalIgnoreCase)", ExtractFunction(script, "Test-DarlingServiceWrittenFileName"), StringComparison.Ordinal);
            Assert.DoesNotContain("BackupGlob", script, StringComparison.Ordinal);
            Assert.DoesNotContain("BackupPattern", script, StringComparison.Ordinal);
            Assert.DoesNotContain("-Filter (Get-DarlingServiceWrittenNames)", script, StringComparison.Ordinal);
            Assert.DoesNotContain("-Filter 'darling.json.bak-", script, StringComparison.Ordinal);
            Assert.DoesNotContain("StartsWith('darling.json.bak-'", script, StringComparison.Ordinal);
            Assert.DoesNotContain("-like 'darling.json.bak-", script, StringComparison.Ordinal);

            /* The lock's service-owned loop reads the helper (which drops hard links), and its walk does not trust a hard link as a secret file. */
            var lockText = ExtractFunction(script, "Lock-DarlingInstallTree");
            Assert.Contains("foreach ($secret in @(Get-DarlingServiceWrittenRootFiles $root))", lockText, StringComparison.Ordinal);
            Assert.Contains("(Test-DarlingServiceWrittenFileName $target.Name) -and (Get-DarlingFileNameCount $path) -ne 1) {", lockText, StringComparison.Ordinal);
            Assert.DoesNotContain("LinkType", lockText, StringComparison.Ordinal);
        }

        Assert.Contains("Get-DarlingServiceWrittenRootFiles $root -BackupsOnly", InstallScript, StringComparison.Ordinal);
        Assert.Contains("$leaf.StartsWith((Get-DarlingServiceWrittenNames).BackupPrefix, [StringComparison]::OrdinalIgnoreCase)", upgrade, StringComparison.Ordinal);

        Assert.Equal("darling.json.bak-", PerformanceMonitor.Darling.Service.DarlingConfigBackup.Prefix);
        Assert.Equal(".bak-*", PerformanceMonitor.Darling.Service.DarlingConfigBackup.GlobSuffix);
        /* The writer builds the name through the class, and the names it writes are byte-for-byte what they were. */
        Assert.Equal("/x/darling.json.bak-20260101-120000", PerformanceMonitor.Darling.Service.DarlingConfigBackup.BackupPath("/x/darling.json", "20260101-120000"));
        Assert.Equal("/x/darling.json.bak-20260101-120000-2", PerformanceMonitor.Darling.Service.DarlingConfigBackup.BackupPath("/x/darling.json", "20260101-120000", 2));
        Assert.Equal("/x/prod.json.bak-20260101-120000-13", PerformanceMonitor.Darling.Service.DarlingConfigBackup.BackupPath("/x/prod.json", "20260101-120000", 13));
        Assert.Contains("DarlingConfigBackup.BackupPath(resolvedPath, stamp)", cli, StringComparison.Ordinal);
        Assert.Contains("DarlingConfigBackup.BackupPath(resolvedPath, stamp, suffix)", cli, StringComparison.Ordinal);
        Assert.DoesNotContain("\".bak-\"", cli, StringComparison.Ordinal);
        Assert.DoesNotContain(".bak-{", cli, StringComparison.Ordinal);
        Assert.Contains("DarlingConfigBackup.ListBackups(directory, Path.GetFileName(configPath))", worker, StringComparison.Ordinal);
        Assert.DoesNotContain("\".bak-*\"", worker, StringComparison.Ordinal);
        Assert.Contains("ConfigBackupFiles(Path.GetDirectoryName(resolvedConfig), Path.GetFileName(resolvedConfig))", cli, StringComparison.Ordinal);
        Assert.DoesNotContain("\"darling.json.bak-*\"", cli, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5649: the number of names a file has is read through a handle opened for attributes only, never from
    /// <c>$item.LinkType</c>, which Windows PowerShell 5.1 leaves EMPTY when the file cannot be opened for read (a
    /// process holds it with no read sharing). One helper, shared by the pre-lock walk, the service-written root files and
    /// the path test: it returns 1 for a plain file, more for a hard link, and -1 when the count cannot be read, which every
    /// caller treats as a hard link. The probe holds a hard link open with no sharing at all (the case that reads as no link
    /// at 5.1) and the walk, the root-file list and the path test must each still see it. Run under Windows PowerShell
    /// 5.1 and PowerShell 7, with both scripts.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheFileNameCount_IsOneForAPlainFile_AndSeesAHardLinkHeldOpenWithNoReadSharing(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Get-UntrustedWriteGrantees"));
        AppendPathHelpers(probe, script);
        AppendServiceWrittenList(probe, script);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path $env:TEMP ('pm5649-nc-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            $outside = "$root-outside.txt"
            $outsideHeld = "$root-outside-held.txt"
            New-Item -ItemType Directory -Path "$root\sub" -Force | Out-Null
            Set-Content -LiteralPath "$root\plain.txt" -Value 'x'
            Set-Content -LiteralPath "$root\darling.json.bak-plain" -Value 'x'
            Set-Content -LiteralPath $outside -Value 'x'
            Set-Content -LiteralPath $outsideHeld -Value 'x'
            New-Item -ItemType HardLink -Path "$root\darling.json.bak-linked" -Value $outside | Out-Null
            New-Item -ItemType HardLink -Path "$root\darling.json.bak-held" -Value $outsideHeld | Out-Null
            $held = $null
            try {
                'plain=' + (Get-DarlingFileNameCount "$root\plain.txt")
                'linked=' + (Get-DarlingFileNameCount "$root\darling.json.bak-linked")
                'folder=' + (Get-DarlingFileNameCount "$root\sub")
                'missing=' + (Get-DarlingFileNameCount "$root\not-here.txt")

                # Held open for read AND write with no sharing at all: nothing else can open it for read.
                $held = [IO.File]::Open("$root\darling.json.bak-held", 'Open', 'ReadWrite', 'None')
                'setup.heldOpenRefusesARead=' + $(try { $s = [IO.File]::Open("$root\darling.json.bak-held", 'Open', 'Read', 'ReadWrite'); $s.Dispose(); $false } catch { $true })
                'heldLinkType=[' + $(try { (Get-Item -LiteralPath "$root\darling.json.bak-held" -Force -ErrorAction Stop).LinkType } catch { 'unreadable' }) + ']'
                'held=' + (Get-DarlingFileNameCount "$root\darling.json.bak-held")
                'heldRootFiles=' + (@(Get-DarlingServiceWrittenRootFiles $root | ForEach-Object { $_.Name } | Sort-Object) -join ',')
                $heldItem = Get-Item -LiteralPath "$root\darling.json.bak-held" -Force
                'heldTrustedPath=' + (Test-DarlingServiceWrittenPath $root "$root\darling.json.bak-held" @() $heldItem)
                $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
                $records = @(Get-UntrustedWriteGrantees $root @($me) -Recurse -AsRecords)
                foreach ($r in $records) { if ($r.Kind -eq 'HardLink') { 'walkHardLink=' + [IO.Path]::GetFileName($r.Path) } }
            }
            finally {
                if ($held) { $held.Dispose() }
                Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath $outside, $outsideHeld -Force -ErrorAction SilentlyContinue
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        AssertProbeSetupTook(answers);
        Assert.Contains("plain=1", answers);
        Assert.Contains("linked=2", answers);
        Assert.Contains("folder=1", answers);
        Assert.Contains("missing=-1", answers);
        Assert.Contains("held=2", answers);
        Assert.Contains("heldRootFiles=darling.json.bak-plain", answers);
        Assert.Contains("heldTrustedPath=False", answers);
        Assert.Contains("walkHardLink=darling.json.bak-held", answers);
        Assert.Contains("walkHardLink=darling.json.bak-linked", answers);
        Assert.DoesNotContain(answers, a => a.StartsWith("walkHardLink=darling.json.bak-plain", StringComparison.Ordinal));
    }

    /// <summary>
    /// #5649: install step 4b hardens darling.json by name, so a darling.json with more than one name (a hard link) or one
    /// that is a link would hand the service its ownership and FullControl on the other name too. The installer refuses
    /// before that loop, with the fix named (an ordinary copy), even when the count cannot be read because another
    /// process holds the file. The probe runs the refusal block as shipped, with Fail recording instead of exiting.
    /// </summary>
    [Fact]
    public void TheInstaller_RefusesADarlingJsonWithMoreThanOneName_BeforeItHardensAnything()
    {
        var script = InstallScript;
        var start = script.IndexOf("$configItem = Get-Item -LiteralPath $configPath", StringComparison.Ordinal);
        var end = script.IndexOf("$hardened = @()", StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, "install-darling.ps1 no longer checks darling.json before the hardening loop");
        var block = script.Substring(start, end - start);
        Assert.True(script.IndexOf("foreach ($secretFile in @($configPath)", end, StringComparison.Ordinal) > end, "the hardening loop must follow the refusal");

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Get-DarlingFileNameCount"));
        probe.AppendLine("function Fail([string]$message) { 'refused=' + $message; $script:didRefuse = $true }");
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path $env:TEMP ('pm5649-cfg-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $root -Force | Out-Null
            $held = $null
            try {
                function RunCase([string]$label, [string]$configPath) {
                    $script:didRefuse = $false
            """);
        probe.AppendLine(block);
        probe.AppendLine("""
                    $label + '.refused=' + $script:didRefuse
                }
                Set-Content -LiteralPath "$root\plain.json" -Value '{}'
                RunCase 'plain' "$root\plain.json"

                Set-Content -LiteralPath "$root-other.json" -Value '{}'
                New-Item -ItemType HardLink -Path "$root\linked.json" -Value "$root-other.json" | Out-Null
                RunCase 'hardLink' "$root\linked.json"

                Set-Content -LiteralPath "$root-other-held.json" -Value '{}'
                New-Item -ItemType HardLink -Path "$root\held.json" -Value "$root-other-held.json" | Out-Null
                $held = [IO.File]::Open("$root\held.json", 'Open', 'ReadWrite', 'None')
                RunCase 'heldHardLink' "$root\held.json"
                $held.Dispose(); $held = $null

                $linkMade = $false
                try { New-Item -ItemType SymbolicLink -Path "$root\symlink.json" -Value "$root\plain.json" -ErrorAction Stop | Out-Null; $linkMade = $true } catch { }
                'symlinkMade=' + $linkMade
                if ($linkMade) { RunCase 'symlink' "$root\symlink.json" }
            }
            finally {
                if ($held) { $held.Dispose() }
                Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath "$root-other.json", "$root-other-held.json" -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.Contains("plain.refused=False", answers);
        Assert.Contains("hardLink.refused=True", answers);
        Assert.Contains("heldHardLink.refused=True", answers);
        if (answers.Contains("symlinkMade=True")) { Assert.Contains("symlink.refused=True", answers); }

        var message = answers.Find(a => a.StartsWith("refused=", StringComparison.Ordinal))!;
        Assert.Contains("linked.json", message, StringComparison.Ordinal);
        Assert.Contains("copy it to a new file, delete the old darling.json, rename the copy to darling.json", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5649: the lock's verify walk reports a root file the service writes that has more than one name, and leaves it
    /// alone, the way it does a junction. A reset (or stripping the grant) on a hard-linked backup with an untrusted
    /// write grant would change the permissions of its other name too, and a reset hands that name to every local user.
    /// The probe gives a hard-linked backup an explicit BUILTIN\Users write grant behind its own protection, runs the lock
    /// and requires the grant to be untouched and the file named in what the lock returns.
    /// </summary>
    [Fact]
    public void TheInstallTreeLock_ReportsAHardLinkedBackupWithAnUntrustedGrant_AndLeavesItsPermissionsAlone()
    {
        var probe = new StringBuilder();
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine(ExtractFunction(InstallScript, "Lock-DarlingInstallTree"));
        probe.AppendLine(ConfirmProbeAclFunction);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path ([IO.Path]::GetPathRoot([Environment]::SystemDirectory)) ('pm5649-lock-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            $outside = Join-Path $env:TEMP ("pm5649-lock-outside-" + [guid]::NewGuid().ToString("N").Substring(0, 8) + ".txt")
            try {
                New-Item -ItemType Directory -Path $root -Force | Out-Null
                Set-Content -LiteralPath "$root\darling.json" -Value '{}'
                Set-Content -LiteralPath $outside -Value 'x'
                New-Item -ItemType HardLink -Path "$root\darling.json.bak-linked" -Value $outside | Out-Null
                $wk = [System.Security.Principal.WellKnownSidType]
                $sidType = [System.Security.Principal.SecurityIdentifier]
                $users = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinUsersSid, $null)
                $system = New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)
                $admins = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)
                $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
                # icacls, not Set-Acl: Set-Acl on a hard link needs SeSecurityPrivilege here, which a developer shell lacks.
                $null = & icacls.exe "$root\darling.json.bak-linked" /inheritance:d /grant "*$($users.Value):(M)" 2>&1
                Confirm-ProbeAcl 'linkedAces' "$root\darling.json.bak-linked" $null @($users)
                'setup.otherNameSharesTheAcl=' + (@((Get-Acl -LiteralPath $outside).GetAccessRules($true, $false, $sidType) | Where-Object { $_.IdentityReference -eq $users }).Count -gt 0)
                'elevated=' + ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

                $open = @(Lock-DarlingInstallTree $root 'NT SERVICE\TrustedInstaller')
                foreach ($line in $open) { if ($line.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) { 'open:' + $line.Substring($root.Length) } }
                foreach ($path in @("$root\darling.json.bak-linked", $outside)) {
                    $acl = Get-Acl -LiteralPath $path
                    $leaf = Split-Path -Leaf $path
                    $leaf + '.usersExplicit=' + (@($acl.GetAccessRules($true, $false, $sidType) | Where-Object { $_.IdentityReference -eq $users }).Count -gt 0)
                    $leaf + '.protected=' + $acl.AreAccessRulesProtected
                }
            }
            finally {
                if (Test-Path -LiteralPath $root) {
                    & icacls.exe $root /reset /T /C /Q 2>&1 | Out-Null
                    Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
                }
                if (Test-Path -LiteralPath $outside) {
                    & icacls.exe $outside /reset /C /Q 2>&1 | Out-Null
                    Remove-Item -LiteralPath $outside -Force -ErrorAction SilentlyContinue
                }
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());
        AssertProbeSetupTook(answers);

        /* The text depends on which step reports the file. An elevated session runs the owner step first: it counts the names on
           the handle, lists the file in its own words and leaves it out of the permission walk. Without elevation the owner step
           does not run and the permission walk lists it (the root-file arm, in the walk's words). Either way it is listed once. */
        var linkedText = answers.Contains("elevated=True")
            ? @"open:\darling.json.bak-linked (has 2 names (a hard link): its permissions are the same under every one"
            : @"open:\darling.json.bak-linked (a hard link: this file has more than one name";
        AssertAnswer(answers, a => a.StartsWith(linkedText, StringComparison.Ordinal), "that starts with '" + linkedText + "'");
        AssertAnswerCount(answers, a => a.StartsWith(@"open:\darling.json.bak-linked", StringComparison.Ordinal), 1, "that list darling.json.bak-linked");
        AssertAnswer(answers, "darling.json.bak-linked.usersExplicit=True");
        AssertAnswer(answers, "darling.json.bak-linked.protected=True");
        AssertAnswer(answers, a => a.EndsWith(".txt.usersExplicit=True", StringComparison.Ordinal), "that ends with '.txt.usersExplicit=True' (the other name keeps the grant)");
    }

    /// <summary>
    /// #5649: the lock never writes to a file that has more than one name, wherever it is in the tree. The earlier arm
    /// above covers the root files the service writes; any other hard-linked file (a shipped file, one in a sub-folder, a
    /// backup-named file that is not directly in the root) reached the reset, which puts the tree's inherited entries on the
    /// file's other name as well. The probe gives four hard-linked files an explicit BUILTIN\Users write grant behind
    /// their own protection and a plain file the same, runs the lock, and requires the linked ones to keep the grant, to
    /// be named in what the lock returns and to leave their other name's grant as it was, while the plain file is still
    /// reset. Run under Windows PowerShell 5.1 and PowerShell 7, with both scripts.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheInstallTreeLock_ReportsAnyHardLinkedFileWithAnUntrustedGrant_AndLeavesItsPermissionsAlone(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        AppendServiceWrittenList(probe, script);
        probe.AppendLine(ExtractFunction(script, "Lock-DarlingInstallTree"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $stamp = [guid]::NewGuid().ToString('N').Substring(0, 8)
            $root = Join-Path ([IO.Path]::GetPathRoot([Environment]::SystemDirectory)) ('pm5649-lockall-' + $stamp)
            $outsideDir = Join-Path $env:TEMP ('pm5649-lockall-outside-' + $stamp)
            try {
                New-Item -ItemType Directory -Path $root -Force | Out-Null
                New-Item -ItemType Directory -Path "$root\sub" -Force | Out-Null
                New-Item -ItemType Directory -Path $outsideDir -Force | Out-Null
                Set-Content -LiteralPath "$root\darling.json" -Value '{}'
                Set-Content -LiteralPath "$root\plain.dll" -Value 'x'
                $wk = [System.Security.Principal.WellKnownSidType]
                $sidType = [System.Security.Principal.SecurityIdentifier]
                $users = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinUsersSid, $null)
                $linked = [ordered]@{ 'vendor.dll' = "$root\vendor.dll"; 'inner.dll' = "$root\sub\inner.dll"; 'darling.json.bak-deep' = "$root\sub\darling.json.bak-deep"; 'darling.json.bak-hand' = "$root\sub\darling.json.bak-hand" }
                foreach ($leaf in $linked.Keys) {
                    $other = Join-Path $outsideDir $leaf
                    Set-Content -LiteralPath $other -Value 'x'
                    New-Item -ItemType HardLink -Path $linked[$leaf] -Value $other | Out-Null
                    # icacls, not Set-Acl: Set-Acl on a hard link needs SeSecurityPrivilege here, which a developer shell lacks.
                    $null = & icacls.exe $linked[$leaf] /inheritance:d /grant "*$($users.Value):(M)" 2>&1
                    'setup.' + $leaf + '.sharedAce=' + (@((Get-Acl -LiteralPath $other).GetAccessRules($true, $false, $sidType) | Where-Object { $_.IdentityReference -eq $users }).Count -gt 0)
                }
                $null = & icacls.exe "$root\plain.dll" /inheritance:d /grant "*$($users.Value):(M)" 2>&1
                'setup.plain.ace=' + (@((Get-Acl -LiteralPath "$root\plain.dll").GetAccessRules($true, $false, $sidType) | Where-Object { $_.IdentityReference -eq $users }).Count -gt 0)
                'elevated=' + ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

                $open = @(Lock-DarlingInstallTree $root 'NT SERVICE\TrustedInstaller')
                foreach ($line in $open) { if ($line.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) { 'open:' + $line.Substring($root.Length) } }
                foreach ($leaf in $linked.Keys) {
                    foreach ($path in @($linked[$leaf], (Join-Path $outsideDir $leaf))) {
                        $acl = Get-Acl -LiteralPath $path
                        $leaf + '.' + ($path.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) + '.usersExplicit=' + (@($acl.GetAccessRules($true, $false, $sidType) | Where-Object { $_.IdentityReference -eq $users }).Count -gt 0)
                    }
                }
                'plain.usersExplicit=' + (@((Get-Acl -LiteralPath "$root\plain.dll").GetAccessRules($true, $false, $sidType) | Where-Object { $_.IdentityReference -eq $users }).Count -gt 0)
            }
            finally {
                foreach ($dir in @($root, $outsideDir)) {
                    if (Test-Path -LiteralPath $dir) {
                        & icacls.exe $dir /reset /T /C /Q 2>&1 | Out-Null
                        Remove-Item -LiteralPath $dir -Recurse -Force -ErrorAction SilentlyContinue
                    }
                }
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        AssertProbeSetupTook(answers);
        /* An elevated session runs the owner step first, which lists a file with more than one name in its own words and keeps it
           out of the permission walk; without elevation the permission walk lists it. Either way it is listed once. */
        var linkedText = answers.Contains("elevated=True")
            ? " (has 2 names (a hard link): its permissions are the same under every one"
            : " (a hard link: this file has more than one name";
        foreach (var leaf in new[] { "vendor.dll", "inner.dll", "darling.json.bak-deep", "darling.json.bak-hand" })
        {
            AssertAnswer(answers, a => a.StartsWith("open:\\", StringComparison.Ordinal) && a.Contains("\\" + leaf + linkedText, StringComparison.Ordinal), "that starts with 'open:\\' and holds '\\" + leaf + linkedText + "'");
            AssertAnswerCount(answers, a => a.StartsWith("open:\\", StringComparison.Ordinal) && a.Contains("\\" + leaf + " (", StringComparison.Ordinal), 1, "that list " + leaf);
            AssertAnswer(answers, $"{leaf}.True.usersExplicit=True");
            AssertAnswer(answers, $"{leaf}.False.usersExplicit=True");
        }

        AssertAnswer(answers, "plain.usersExplicit=False");
    }

    /// <summary>
    /// #5677: the lock's owner step sets the owner one object at a time, in process, on the handle the name count is read from,
    /// and never with one <c>icacls /setowner ... /T</c> over the tree (which would change the owner of every name of a file
    /// that has more than one). Pins the shape in both scripts: the lock calls <c>SetOwner</c>, has no <c>/T</c> owner call,
    /// turns the two privileges on and puts them back around the step, holds the root open for the walk, skips what the owner step listed, and the native class opens the
    /// object itself (not what a link points to), reads the count from that handle and sets the owner on it.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheInstallTreeLockOwnerStep_SetsTheOwnerPerObjectOnTheCountedHandle_AndHasNoSubtreeCall(string scriptName)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var lockText = ExtractFunction(script, "Lock-DarlingInstallTree");
        Assert.DoesNotContain("/setowner \"*$($adminsSid.Value)\"", lockText, StringComparison.Ordinal);
        Assert.DoesNotContain("$adminsSid.Value)\" /T", lockText, StringComparison.Ordinal);
        Assert.Contains("[DarlingNativeOwner]::SetOwner($ownPath, $ownerBytes, [ref]$detail, [ref]$attributes)", lockText, StringComparison.Ordinal);
        Assert.Contains("[DarlingNativeOwner]::EnablePrivileges()", lockText, StringComparison.Ordinal);
        Assert.Contains("[DarlingNativeOwner]::RestorePrivileges()", lockText, StringComparison.Ordinal);
        Assert.Contains("[DarlingNativeOwner]::OpenRoot($root, [ref]$rootError)", lockText, StringComparison.Ordinal);
        Assert.Contains("[DarlingNativeOwner]::CloseRoot()", lockText, StringComparison.Ordinal);
        Assert.Contains("$skipped = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::Ordinal)", lockText, StringComparison.Ordinal);
        Assert.Contains("if ($skipped.Contains($target.FullName)) { continue }", lockText, StringComparison.Ordinal);
        Assert.DoesNotContain("ContainsKey", lockText, StringComparison.Ordinal);
        Assert.DoesNotContain("SeRestorePrivilege", lockText, StringComparison.Ordinal);
        /* A link is acted on itself and never walked into. */
        Assert.Contains("($attributes -band 0x400) -eq 0", lockText, StringComparison.Ordinal);

        var native = ExtractFunction(script, "Initialize-DarlingNativeOwner");
        Assert.Contains("CreateFileW(ExtendedPath(path), 0x80 | 0x00080000, 7, IntPtr.Zero, 3, 0x00200000 | 0x02000000, IntPtr.Zero)", native, StringComparison.Ordinal);
        Assert.Contains("CreateFileW(ExtendedPath(root), 0x80, 3, IntPtr.Zero, 3, 0x02000000, IntPtr.Zero)", native, StringComparison.Ordinal);
        Assert.Contains("GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Capacity, 1)", native, StringComparison.Ordinal);
        Assert.Contains("\"SeTakeOwnershipPrivilege\", \"SeBackupPrivilege\"", native, StringComparison.Ordinal);
        Assert.DoesNotContain("SeRestorePrivilege", native, StringComparison.Ordinal);
        Assert.Contains("string.Equals(real, root, StringComparison.Ordinal)", native, StringComparison.Ordinal);
        Assert.Contains("if (information.NumberOfLinks != 1) { return 1; }", native, StringComparison.Ordinal);
        Assert.Contains("SetSecurityInfo(handle, 1, 1, ownerSid, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero)", native, StringComparison.Ordinal);
        Assert.DoesNotContain("/setowner /T", script.Replace("\r\n", "\n"), StringComparison.Ordinal);
        Assert.DoesNotContain("/setowner *S-1-5-32-544 /T", script, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5677: the owner step on its own, run on a small tree with the current account's SID as the owner to set (so it works
    /// without elevation). A plain file and a folder take the new owner and report 0; a file with a second name outside the tree
    /// reports 1 with a count of 2, keeps its owner under both names; a junction reports its attributes and is not followed
    /// (the folder it points to keeps its owner). Run under Windows PowerShell 5.1 and PowerShell 7, with both scripts.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheNativeOwnerStep_SetsAPlainFileAndAFolder_AndLeavesAHardLinkedFileAlone(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Initialize-DarlingNativeOwner"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $stamp = [guid]::NewGuid().ToString('N').Substring(0, 8)
            $root = Join-Path $env:TEMP ('pm5677-native-' + $stamp)
            $outsideDir = Join-Path $env:TEMP ('pm5677-native-outside-' + $stamp)
            $sidType = [System.Security.Principal.SecurityIdentifier]
            $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
            $mine = New-Object byte[] $me.BinaryLength
            $me.GetBinaryForm($mine, 0)
            function Get-OwnerValue([string]$p) { return (Get-Acl -LiteralPath $p).GetOwner($sidType).Value }
            try {
                New-Item -ItemType Directory -Path "$root\sub", "$root\target", $outsideDir -Force | Out-Null
                Set-Content -LiteralPath "$root\plain.txt" -Value 'x'
                Set-Content -LiteralPath "$outsideDir\linked.txt" -Value 'x'
                New-Item -ItemType HardLink -Path "$root\sub\linked.txt" -Value "$outsideDir\linked.txt" | Out-Null
                & cmd.exe /c mklink /J "$root\junction" "$root\target" | Out-Null
                'setup.linkMade=' + ((Get-Item -LiteralPath "$root\sub\linked.txt").LinkType -eq 'HardLink')
                'setup.junctionMade=' + ((((Get-Item -LiteralPath "$root\junction" -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0))
                $linkedBefore = Get-OwnerValue "$outsideDir\linked.txt"
                $targetBefore = Get-OwnerValue "$root\target"
                Initialize-DarlingNativeOwner
                $rootError = 0
                'setup.rootOpened=' + [DarlingNativeOwner]::OpenRoot($root, [ref]$rootError)
                foreach ($name in @('plain.txt', 'sub', 'sub\linked.txt', 'junction')) {
                    $detail = 0
                    $attributes = [uint32]0
                    $result = [DarlingNativeOwner]::SetOwner("$root\$name", $mine, [ref]$detail, [ref]$attributes)
                    'result.' + $name + '=' + $result + ',' + $detail + ',' + (($attributes -band 0x400) -ne 0)
                }
                'owner.plain=' + ((Get-OwnerValue "$root\plain.txt") -eq $me.Value)
                'owner.folder=' + ((Get-OwnerValue "$root\sub") -eq $me.Value)
                'owner.linkedInside=' + ((Get-OwnerValue "$root\sub\linked.txt") -eq $linkedBefore)
                'owner.linkedOutside=' + ((Get-OwnerValue "$outsideDir\linked.txt") -eq $linkedBefore)
                'owner.junctionTarget=' + ((Get-OwnerValue "$root\target") -eq $targetBefore)
            }
            finally {
                try { [DarlingNativeOwner]::CloseRoot() } catch { }
                foreach ($dir in @($root, $outsideDir)) {
                    if (Test-Path -LiteralPath $dir) {
                        if (Test-Path -LiteralPath "$dir\junction") { & cmd.exe /c rmdir "$dir\junction" | Out-Null }
                        Remove-Item -LiteralPath $dir -Recurse -Force -ErrorAction SilentlyContinue
                    }
                }
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        AssertProbeSetupTook(answers);
        Assert.Contains("result.plain.txt=0,1,False", answers);
        Assert.Contains("result.sub=0,1,False", answers);
        Assert.Contains("result.sub\\linked.txt=1,2,False", answers);
        Assert.Contains("result.junction=0,1,True", answers);
        Assert.Contains("owner.plain=True", answers);
        Assert.Contains("owner.folder=True", answers);
        Assert.Contains("owner.linkedInside=True", answers);
        Assert.Contains("owner.linkedOutside=True", answers);
        Assert.Contains("owner.junctionTarget=True", answers);
    }

    /// <summary>
    /// #5677: the lock's owner step leaves a file with more than one name alone. The tree holds a plain file, a folder, a file
    /// inside it and a file whose second name is outside the tree, all owned by the current account first. After the lock (the
    /// service account is one that needs no account lookup) the linked file keeps that owner under BOTH names and is listed as
    /// open exactly once, while the plain file, the folder and the root are owned by Administrators. Setting an owner of
    /// Administrators needs an elevated token (CI's runner is one); a filtered token reports the root as needing an elevated
    /// session and changes no owner. Run under Windows PowerShell 5.1 and PowerShell 7, with both scripts.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheInstallTreeLock_GivesAdministratorsTheOwnerOfPlainFilesAndFolders_AndLeavesAHardLinkedFilesOwner(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        AppendServiceWrittenList(probe, script);
        probe.AppendLine(ExtractFunction(script, "Lock-DarlingInstallTree"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $stamp = [guid]::NewGuid().ToString('N').Substring(0, 8)
            $root = Join-Path $env:TEMP ('pm5677-owner-' + $stamp)
            $outsideDir = Join-Path $env:TEMP ('pm5677-owner-outside-' + $stamp)
            $sidType = [System.Security.Principal.SecurityIdentifier]
            $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
            $admins = New-Object System.Security.Principal.SecurityIdentifier([System.Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid, $null)
            function Get-OwnerValue([string]$p) { return (Get-Acl -LiteralPath $p).GetOwner($sidType).Value }
            try {
                New-Item -ItemType Directory -Path "$root\sub", $outsideDir -Force | Out-Null
                # The lock works in the long spelling of the root and lists every path in it, while $env:TEMP is the 8.3 short form on a
                # CI runner (C:\Users\RUNNER~1\...), which would make the open: filter below drop every line the lock returned.
                $root = Get-DarlingCanonicalPath $root
                Set-Content -LiteralPath "$root\plain.dll" -Value 'x'
                Set-Content -LiteralPath "$root\sub\inner.dll" -Value 'x'
                Set-Content -LiteralPath "$outsideDir\linked.dll" -Value 'x'
                New-Item -ItemType HardLink -Path "$root\sub\linked.dll" -Value "$outsideDir\linked.dll" | Out-Null
                foreach ($p in @("$root\sub", "$root\plain.dll", "$root\sub\inner.dll", "$root\sub\linked.dll")) { $null = & icacls.exe $p /setowner "*$($me.Value)" /L /Q 2>&1 }
                'setup.ownersSet=' + ((@("$root\sub", "$root\plain.dll", "$root\sub\inner.dll", "$root\sub\linked.dll", "$outsideDir\linked.dll") | Where-Object { (Get-OwnerValue $_) -ne $me.Value }).Count -eq 0)
                'elevated=' + ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

                $open = @(Lock-DarlingInstallTree $root 'NT SERVICE\TrustedInstaller')
                foreach ($line in $open) { if ($line.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) { 'open:' + $line.Substring($root.Length) } else { 'openElsewhere:' + $line } }
                'linkedListed=' + @($open | Where-Object { $_ -like '*linked.dll*' }).Count
                'owner.linkedInside=' + ((Get-OwnerValue "$root\sub\linked.dll") -eq $me.Value)
                'owner.linkedOutside=' + ((Get-OwnerValue "$outsideDir\linked.dll") -eq $me.Value)
                foreach ($leaf in @('', '\sub', '\plain.dll', '\sub\inner.dll')) { 'adminOwner[' + $leaf + ']=' + ((Get-OwnerValue ($root + $leaf)) -eq $admins.Value) }
            }
            finally {
                foreach ($dir in @($root, $outsideDir)) {
                    if (Test-Path -LiteralPath $dir) {
                        & icacls.exe $dir /reset /T /C /Q 2>&1 | Out-Null
                        Remove-Item -LiteralPath $dir -Recurse -Force -ErrorAction SilentlyContinue
                    }
                }
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        AssertProbeSetupTook(answers);
        AssertAnswer(answers, "owner.linkedInside=True");
        AssertAnswer(answers, "owner.linkedOutside=True");
        if (answers.Contains("elevated=True"))
        {
            AssertAnswer(answers, "linkedListed=1");
            AssertAnswer(answers, a => a.StartsWith("open:\\sub\\linked.dll (has 2 names (a hard link)", StringComparison.Ordinal), "that starts with 'open:\\sub\\linked.dll (has 2 names (a hard link)'");
            foreach (var leaf in new[] { "", "\\sub", "\\plain.dll", "\\sub\\inner.dll" })
            {
                AssertAnswer(answers, $"adminOwner[{leaf}]=True");
            }
        }
        else
        {
            AssertAnswer(answers, a => a.StartsWith("open:", StringComparison.Ordinal) && a.Contains("SeTakeOwnershipPrivilege and SeBackupPrivilege (see whoami /priv)", StringComparison.Ordinal), "that starts with 'open:' and names SeTakeOwnershipPrivilege and SeBackupPrivilege");
        }
    }

    /// <summary>
    /// #5700: a TLS key file the exe's verb has already secured (a protected DACL of SYSTEM and Administrators in full plus Read for
    /// the service account, owned by Administrators) is left exactly as it is by the tree lock that runs on its folder afterwards.
    /// The key is made with icacls to the same shape, in a folder that inherits Users read the way an install folder does, and the
    /// real lock function runs over the folder with that service account. The key's access descriptor is the same text before and
    /// after, it is still protected from inheritance, no Users entry has appeared on it, and (with an elevated token, which sets the owner) the lock does not list it as open.
    /// Both scripts' copies of the lock.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheInstallTreeLock_LeavesAKeyFileTheVerbAlreadySecured_AsItWas(string scriptName)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        AppendServiceWrittenList(probe, script);
        probe.AppendLine(ExtractFunction(script, "Lock-DarlingInstallTree"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path $env:TEMP ('pm5700-lock-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            $sidType = [System.Security.Principal.SecurityIdentifier]
            function Get-KeyDescriptor([string]$p) {
                $acl = Get-Acl -LiteralPath $p
                return $acl.GetSecurityDescriptorSddlForm('Access') + ' protected=' + $acl.AreAccessRulesProtected
            }
            try {
                New-Item -ItemType Directory -Path $root -Force | Out-Null
                $root = Get-DarlingCanonicalPath $root
                $users = '*S-1-5-32-545'
                & icacls.exe $root /grant "${users}:(OI)(CI)RX" /Q | Out-Null
                $key = Join-Path $root 'web.pfx'
                Set-Content -LiteralPath $key -Value 'not a real key'
                & icacls.exe $key /inheritance:r /grant:r '*S-1-5-18:(F)' '*S-1-5-32-544:(F)' '*S-1-5-19:(R)' /Q | Out-Null
                # The owner change needs an elevated token (CI's runner is one); a filtered token leaves the owner as it was.
                $ErrorActionPreference = 'Continue'
                & icacls.exe $key /setowner '*S-1-5-32-544' /Q 2>&1 | Out-Null
                $ErrorActionPreference = 'Stop'
                $before = Get-KeyDescriptor $key
                'setup.keyIsProtected=' + ($before -match 'protected=True')
                'setup.keyHasNoUsersEntry=' + (-not ($before -match 'BU'))
                'elevated=' + ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

                $open = @(Lock-DarlingInstallTree $root 'NT AUTHORITY\LOCAL SERVICE')
                $after = Get-KeyDescriptor $key
                'keyDescriptorSame=' + ($before -eq $after)
                'keyStillProtected=' + ($after -match 'protected=True')
                'keyHasNoUsersEntry=' + (-not ($after -match 'BU'))
                'keyListedOpen=' + @($open | Where-Object { $_ -like '*web.pfx*' }).Count
                'before=' + $before
                'after=' + $after
            }
            finally {
                if (Test-Path -LiteralPath $root) {
                    & icacls.exe $root /reset /T /C /Q 2>&1 | Out-Null
                    Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
                }
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        AssertProbeSetupTook(answers);
        AssertAnswer(answers, "keyDescriptorSame=True");
        AssertAnswer(answers, "keyStillProtected=True");
        AssertAnswer(answers, "keyHasNoUsersEntry=True");
        if (answers.Contains("elevated=True"))
        {
            // With an Administrators owner (set only by an elevated token) the key matches no write or owner check of the lock.
            AssertAnswer(answers, "keyListedOpen=0");
        }
    }

    /// <summary>Runs a probe in the shell a theory case names, skipping the case when PowerShell 7 is not installed.</summary>
    private static List<string> RunProbeInShell(string shell, string probe)
    {
        if (shell == "pwsh")
        {
            var answers = RunPowerShell7(probe);
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); }
            return answers ?? new List<string>();
        }

        return RunWindowsPowerShell(probe);
    }

    /// <summary>
    /// The shared head of the #5677 owner-step probes: the temp folders, the current account's SID as the owner to set (so the
    /// native step works without elevation), an owner reader and a cleaner that closes the root handle, removes any junction
    /// first and deletes through a <c>\\?\</c> path so a name ending in a dot goes too.
    /// </summary>
    private const string OwnerStepProbeHead = """
        $ErrorActionPreference = 'Stop'
        $stamp = [guid]::NewGuid().ToString('N').Substring(0, 8)
        $root = Join-Path $env:TEMP ('pm5677-step-' + $stamp)
        $outsideDir = Join-Path $env:TEMP ('pm5677-step-outside-' + $stamp)
        $sidType = [System.Security.Principal.SecurityIdentifier]
        $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
        $mine = New-Object byte[] $me.BinaryLength
        $me.GetBinaryForm($mine, 0)
        $elevated = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
        'elevated=' + $elevated
        function Get-OwnerValue([string]$p) { return (Get-Acl -LiteralPath $p).GetOwner($sidType).Value }
        function Remove-ProbeDirectory([string]$dir) {
            if (-not (Test-Path -LiteralPath $dir)) { return }
            foreach ($link in @(Get-ChildItem -LiteralPath $dir -Force -Recurse -ErrorAction SilentlyContinue | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint })) { & cmd.exe /c rmdir "$($link.FullName)" | Out-Null }
            & icacls.exe $dir /reset /T /C /L /Q 2>&1 | Out-Null
            & cmd.exe /c rd /s /q "\\?\$dir" 2>&1 | Out-Null
        }
        function Get-StepResult([string]$p) {
            $detail = 0
            $attributes = [uint32]0
            $result = [DarlingNativeOwner]::SetOwner($p, $mine, [ref]$detail, [ref]$attributes)
            return "$result,$detail"
        }
        """;

    /// <summary>
    /// #5677: the owner step changes only what is inside the root it was given. The root is opened once and its real path read
    /// from that handle; an object reached through a name in the tree whose real path is elsewhere (here a folder replaced by a
    /// junction to an outside folder that holds a file) gets result 5 and its owner is not set, so nothing needs to be timed.
    /// The junction itself is a name in the tree and gets its own owner (result 0). A path that no longer exists is result 2 (error 2), an owner the system refuses is result 4, and with no root open every
    /// path is result 5. The root itself, a plain file, a folder and a differently-cased spelling of the root are inside.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheNativeOwnerStep_ChangesNothingOutsideTheRoot_AndGivesEveryOtherOutcomeItsOwnResult(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Initialize-DarlingNativeOwner"));
        probe.AppendLine(OwnerStepProbeHead);
        probe.AppendLine("""
            try {
                New-Item -ItemType Directory -Path "$root\sub", $outsideDir -Force | Out-Null
                Set-Content -LiteralPath "$root\plain.txt" -Value 'x'
                Set-Content -LiteralPath "$outsideDir\file" -Value 'x'
                & cmd.exe /c mklink /J "$root\a" $outsideDir | Out-Null
                'setup.junctionMade=' + ((((Get-Item -LiteralPath "$root\a" -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0))
                'setup.reachable=' + (Test-Path -LiteralPath "$root\a\file")
                $outsideBefore = Get-OwnerValue "$outsideDir\file"
                Initialize-DarlingNativeOwner
                $rootError = 0
                'setup.rootOpened=' + [DarlingNativeOwner]::OpenRoot($root, [ref]$rootError)
                'result.throughJunction=' + (Get-StepResult "$root\a\file")
                'result.junctionItself=' + (Get-StepResult "$root\a")
                'result.plain=' + (Get-StepResult "$root\plain.txt")
                'result.folder=' + (Get-StepResult "$root\sub")
                'result.root=' + (Get-StepResult $root)
                'result.deleted=' + (Get-StepResult "$root\sub\deleted.txt")
                'result.upperCaseSpelling=' + (Get-StepResult ($root.ToUpperInvariant() + '\sub'))
                $detail = 0
                $attributes = [uint32]0
                $refused = [DarlingNativeOwner]::SetOwner("$root\plain.txt", [byte[]](1, 2, 0, 0, 0, 0, 0, 5, 99, 134, 1, 0, 17, 34, 51, 68), [ref]$detail, [ref]$attributes)
                'result.refusedOwner=' + $refused + ',' + ($detail -ne 0)
                'owner.outsideFile=' + ((Get-OwnerValue "$outsideDir\file") -eq $outsideBefore)
                [DarlingNativeOwner]::CloseRoot()
                'result.noRootOpen=' + (Get-StepResult "$root\plain.txt")
            }
            finally {
                try { [DarlingNativeOwner]::CloseRoot() } catch { }
                foreach ($dir in @($root, $outsideDir)) { Remove-ProbeDirectory $dir }
            }
            """);

        var answers = RunProbeInShell(shell, probe.ToString());

        AssertProbeSetupTook(answers);
        Assert.Contains("result.throughJunction=5,0", answers);
        Assert.Contains("result.junctionItself=0,1", answers);
        Assert.Contains("result.plain=0,1", answers);
        Assert.Contains("result.folder=0,1", answers);
        Assert.Contains("result.root=0,1", answers);
        Assert.Contains("result.deleted=2,2", answers);
        Assert.Contains("result.upperCaseSpelling=0,1", answers);
        Assert.Contains("result.refusedOwner=4,True", answers);
        Assert.Contains("owner.outsideFile=True", answers);
        Assert.Contains("result.noRootOpen=5,0", answers);
    }

    /// <summary>
    /// #5677: a name that ends in a dot is opened as that name. The folder holds <c>dn.</c> (one name) and <c>dn</c> (a hard link
    /// with a second name outside), both made through <c>\\?\</c> paths. Without the extended spelling Windows drops the dot and
    /// the step would open <c>dn</c> twice.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheNativeOwnerStep_OpensANameEndingInADot_AsThatName_NotAsTheNameWithoutIt(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Initialize-DarlingNativeOwner"));
        probe.AppendLine(OwnerStepProbeHead);
        probe.AppendLine("""
            try {
                New-Item -ItemType Directory -Path $root, $outsideDir -Force | Out-Null
                Set-Content -LiteralPath "$outsideDir\twin" -Value 'x'
                [IO.File]::WriteAllText("\\?\$root\dn.", 'x')
                New-Item -ItemType HardLink -Path "\\?\$root\dn" -Value "$outsideDir\twin" | Out-Null
                'setup.bothNamesListed=' + ((((@(Get-ChildItem -LiteralPath $root -Force | ForEach-Object Name) | Sort-Object) -join '|')) -eq 'dn|dn.')
                Initialize-DarlingNativeOwner
                $rootError = 0
                'setup.rootOpened=' + [DarlingNativeOwner]::OpenRoot($root, [ref]$rootError)
                'result.dotName=' + (Get-StepResult "$root\dn.")
                'result.plainName=' + (Get-StepResult "$root\dn")
            }
            finally {
                try { [DarlingNativeOwner]::CloseRoot() } catch { }
                foreach ($dir in @($root, $outsideDir)) { Remove-ProbeDirectory $dir }
            }
            """);

        var answers = RunProbeInShell(shell, probe.ToString());

        AssertProbeSetupTook(answers);
        Assert.Contains("result.dotName=0,1", answers);
        Assert.Contains("result.plainName=1,2", answers);
    }

    /// <summary>
    /// #5677: the privileges the step turns on are put back as they were. The two lines <c>whoami /priv</c> prints for them are
    /// the same before the step and after it (an elevated token has both and has them off; one that is not elevated has neither,
    /// so turning them on reports false and nothing changes).
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheNativeOwnerStep_PutsThePrivilegesBackAsItFoundThem(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Initialize-DarlingNativeOwner"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $elevated = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
            'elevated=' + $elevated
            function Get-PrivilegeLines { return (@(& "$env:SystemRoot\System32\whoami.exe" /priv) | Where-Object { $_ -match 'SeTakeOwnershipPrivilege|SeBackupPrivilege' } | ForEach-Object { ($_ -replace '\s+', ' ').Trim() }) -join ';' }
            Initialize-DarlingNativeOwner
            $before = Get-PrivilegeLines
            $enabled = [DarlingNativeOwner]::EnablePrivileges()
            $during = Get-PrivilegeLines
            $restored = [DarlingNativeOwner]::RestorePrivileges()
            $after = Get-PrivilegeLines
            'enabled=' + $enabled
            'duringDiffers=' + ($during -ne $before)
            'restored=' + $restored
            'afterSameAsBefore=' + ($after -eq $before)
            $again = [DarlingNativeOwner]::EnablePrivileges()
            $null = [DarlingNativeOwner]::RestorePrivileges()
            'twiceSameAsBefore=' + ((Get-PrivilegeLines) -eq $before)
            """);

        var answers = RunProbeInShell(shell, probe.ToString());

        Assert.Contains("restored=True", answers);
        Assert.Contains("afterSameAsBefore=True", answers);
        Assert.Contains("twiceSameAsBefore=True", answers);
        if (answers.Contains("elevated=True"))
        {
            Assert.Contains("enabled=True", answers);
            Assert.Contains("duringDiffers=True", answers);
        }
        else
        {
            Assert.Contains("enabled=False", answers);
        }
    }

    /// <summary>
    /// #5677: the lock over a tree that holds a folder replaced by a junction to an outside folder, and a name ending in a dot
    /// next to the same name without it. The outside file keeps its owner, the junction is listed, and the dot name is listed as
    /// open and not given an owner (the plain name beside it is). The owner step needs the two privileges, which only an
    /// elevated token holds; a token without them lists the root with the privilege names and changes no owner.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheInstallTreeLock_LeavesAnOutsideFolderAlone_AndListsANameEndingInADotWithoutActingOnIt(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        AppendServiceWrittenList(probe, script);
        probe.AppendLine(ExtractFunction(script, "Lock-DarlingInstallTree"));
        probe.AppendLine(OwnerStepProbeHead);
        probe.AppendLine("""
            $admins = New-Object System.Security.Principal.SecurityIdentifier([System.Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid, $null)
            try {
                New-Item -ItemType Directory -Path $root, $outsideDir -Force | Out-Null
                # The lock works in the long spelling of the root and lists every path in it, while $env:TEMP is the 8.3 short form on a
                # CI runner (C:\Users\RUNNER~1\...), which would make the open: filter below drop every line the lock returned.
                $root = Get-DarlingCanonicalPath $root
                Set-Content -LiteralPath "$outsideDir\file" -Value 'x'
                Set-Content -LiteralPath "$root\dn" -Value 'x'
                [IO.File]::WriteAllText("\\?\$root\dn.", 'x')
                & cmd.exe /c mklink /J "$root\a" $outsideDir | Out-Null
                # Every file this probe checks is given the current account as its owner first. A file made by an elevated administrator
                # is owned by Administrators from the start, so without this the check that the lock left dn. alone would pass or fail on
                # who created it. dn. is reached with a \\?\ path, which Windows does not fold into dn (icacls and Get-Acl both honour it).
                foreach ($p in @("$outsideDir\file", "$root\dn", "\\?\$root\dn.")) { $null = & icacls.exe $p /setowner "*$($me.Value)" /L /Q 2>&1 }
                'setup.bothNamesListed=' + ((((@(Get-ChildItem -LiteralPath $root -Force | ForEach-Object Name) | Sort-Object) -join '|')) -eq 'a|dn|dn.')
                'setup.ownersSet=' + (((Get-OwnerValue "$outsideDir\file") -eq $me.Value) -and ((Get-OwnerValue "$root\dn") -eq $me.Value) -and ((Get-OwnerValue "\\?\$root\dn.") -eq $me.Value))
                $open = @(Lock-DarlingInstallTree $root 'NT SERVICE\TrustedInstaller')
                foreach ($line in $open) { if ($line.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) { 'open:' + $line.Substring($root.Length) } else { 'openElsewhere:' + $line } }
                'owner.outsideFile=' + ((Get-OwnerValue "$outsideDir\file") -eq $me.Value)
                'adminOwner[dn]=' + ((Get-OwnerValue "$root\dn") -eq $admins.Value)
                'adminOwner[dn.]=' + ((Get-OwnerValue "\\?\$root\dn.") -eq $admins.Value)
                'ownerNow[dn]=' + (Get-OwnerValue "$root\dn") + ' ownerNow[dn.]=' + (Get-OwnerValue "\\?\$root\dn.") + ' me=' + $me.Value + ' admins=' + $admins.Value
            }
            finally { foreach ($dir in @($root, $outsideDir)) { Remove-ProbeDirectory $dir } }
            """);

        var answers = RunProbeInShell(shell, probe.ToString());

        AssertProbeSetupTook(answers);
        AssertAnswer(answers, "owner.outsideFile=True");
        AssertAnswer(answers, "adminOwner[dn.]=False");
        AssertAnswer(answers, a => a.StartsWith("open:\\a (a junction or link)", StringComparison.Ordinal), "that starts with 'open:\\a (a junction or link)'");
        if (answers.Contains("elevated=True"))
        {
            AssertAnswer(answers, "adminOwner[dn]=True");
            AssertAnswer(answers, a => a.StartsWith("open:\\dn. (its name ends in a dot or a space", StringComparison.Ordinal), "that starts with 'open:\\dn. (its name ends in a dot or a space'");
        }
        else
        {
            AssertAnswer(answers, "adminOwner[dn]=False");
            AssertAnswer(answers, a => a.StartsWith("open:", StringComparison.Ordinal) && a.Contains("SeTakeOwnershipPrivilege and SeBackupPrivilege (see whoami /priv); a policy may have removed them from Administrators", StringComparison.Ordinal), "that starts with 'open:' and names SeTakeOwnershipPrivilege and SeBackupPrivilege");
        }
    }

    /// <summary>
    /// #5677: an object whose owner could not be set (result 4, the count was 1) is listed with that reason but is NOT put in
    /// the skip set, so the permission walk still removes its explicit write grant for ordinary users and checks it, as it did
    /// before the owner step existed. The probe replaces the privilege call with <c>$true</c> so the owner step runs on a token
    /// without elevation: setting Administrators as owner is then refused (result 4); on an elevated token it succeeds, and the
    /// grant is removed either way.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheInstallTreeLock_StillFixesAndChecksAnObjectWhoseOwnerCouldNotBeSet(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var lockText = ExtractFunction(script, "Lock-DarlingInstallTree");
        var privilegeCall = "$ownerReady = [DarlingNativeOwner]::EnablePrivileges()";
        Assert.Contains(privilegeCall, lockText, StringComparison.Ordinal);
        var probe = new StringBuilder();
        AppendServiceWrittenList(probe, script);
        probe.AppendLine(lockText.Replace(privilegeCall, "$ownerReady = $true", StringComparison.Ordinal));
        probe.AppendLine(OwnerStepProbeHead);
        probe.AppendLine("""
            $users = New-Object System.Security.Principal.SecurityIdentifier([System.Security.Principal.WellKnownSidType]::BuiltinUsersSid, $null)
            function Test-UsersExplicit([string]$p) { return @((Get-Acl -LiteralPath $p).GetAccessRules($true, $false, $sidType) | Where-Object { $_.IdentityReference -eq $users }).Count -gt 0 }
            try {
                New-Item -ItemType Directory -Path "$root\sub" -Force | Out-Null
                Set-Content -LiteralPath "$root\sub\f.dll" -Value 'x'
                foreach ($p in @("$root\sub", "$root\sub\f.dll")) { $null = & icacls.exe $p /setowner "*$($me.Value)" /L /Q 2>&1 }
                $null = & icacls.exe "$root\sub\f.dll" /grant "*S-1-5-32-545:(W)" /Q 2>&1
                'setup.grantMade=' + (Test-UsersExplicit "$root\sub\f.dll")
                $open = @(Lock-DarlingInstallTree $root 'NT SERVICE\TrustedInstaller')
                foreach ($line in $open) { if ($line.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) { 'open:' + $line.Substring($root.Length) } }
                'usersExplicitAfter=' + (Test-UsersExplicit "$root\sub\f.dll")
            }
            finally { foreach ($dir in @($root, $outsideDir)) { Remove-ProbeDirectory $dir } }
            """);

        var answers = RunProbeInShell(shell, probe.ToString());

        AssertProbeSetupTook(answers);
        Assert.Contains("usersExplicitAfter=False", answers);
        if (!answers.Contains("elevated=True"))
        {
            Assert.Contains(answers, a => a.StartsWith("open:\\sub\\f.dll (its owner could not be set, error ", StringComparison.Ordinal));
            Assert.DoesNotContain(answers, a => a.StartsWith("open:\\sub\\f.dll (has ", StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// #5677: the skip set compares names exactly. In a folder that has the per-folder case-sensitive flag, <c>a.dll</c> (a hard
    /// link, listed by the owner step and left alone) and <c>A.DLL</c> (a plain file) are two objects: the permission walk leaves
    /// the first with its explicit grant and still removes the second's. Needs <c>fsutil file setCaseSensitiveInfo</c>, which
    /// needs an elevated token and a file system that supports it; the case is skipped, with that reason, where it cannot be set
    /// up. The privilege call is replaced as in the test above.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheInstallTreeLock_TreatsTwoNamesThatDifferOnlyInCaseAsTwoObjects(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var lockText = ExtractFunction(script, "Lock-DarlingInstallTree");
        var privilegeCall = "$ownerReady = [DarlingNativeOwner]::EnablePrivileges()";
        var probe = new StringBuilder();
        AppendServiceWrittenList(probe, script);
        probe.AppendLine(lockText.Replace(privilegeCall, "$ownerReady = $true", StringComparison.Ordinal));
        probe.AppendLine(OwnerStepProbeHead);
        probe.AppendLine("""
            $users = New-Object System.Security.Principal.SecurityIdentifier([System.Security.Principal.WellKnownSidType]::BuiltinUsersSid, $null)
            function Test-UsersExplicit([string]$p) { return @((Get-Acl -LiteralPath $p).GetAccessRules($true, $false, $sidType) | Where-Object { $_.IdentityReference -eq $users }).Count -gt 0 }
            try {
                New-Item -ItemType Directory -Path "$root\cs", $outsideDir -Force | Out-Null
                $null = & fsutil.exe file setCaseSensitiveInfo "$root\cs" enable 2>&1
                'caseSensitive=' + ($LASTEXITCODE -eq 0)
                if ($LASTEXITCODE -eq 0) {
                    Set-Content -LiteralPath "$outsideDir\twin" -Value 'x'
                    New-Item -ItemType HardLink -Path "$root\cs\a.dll" -Value "$outsideDir\twin" | Out-Null
                    Set-Content -LiteralPath "$root\cs\A.DLL" -Value 'x'
                    'twoObjects=' + (@(Get-ChildItem -LiteralPath "$root\cs" -Force).Count -eq 2)
                    foreach ($p in @("$root\cs", "$root\cs\A.DLL", "$root\cs\a.dll")) { $null = & icacls.exe $p /setowner "*$($me.Value)" /L /Q 2>&1 }
                    foreach ($p in @("$root\cs\A.DLL", "$root\cs\a.dll")) { $null = & icacls.exe $p /grant "*S-1-5-32-545:(W)" /Q 2>&1 }
                    'setup.grantsMade=' + ((Test-UsersExplicit "$root\cs\A.DLL") -and (Test-UsersExplicit "$root\cs\a.dll"))
                    $open = @(Lock-DarlingInstallTree $root 'NT SERVICE\TrustedInstaller')
                    'hardLinkListed=' + (@($open | Where-Object { $_ -like '*\cs\a.dll (has 2 names*' }).Count -eq 1)
                    'upperKeepsGrant=' + (Test-UsersExplicit "$root\cs\a.dll")
                    'upperFixed=' + (-not (Test-UsersExplicit "$root\cs\A.DLL"))
                }
            }
            finally { foreach ($dir in @($root, $outsideDir)) { Remove-ProbeDirectory $dir } }
            """);

        var answers = RunProbeInShell(shell, probe.ToString());

        if (!answers.Contains("caseSensitive=True")) { Assert.Skip("this machine cannot set the per-folder case-sensitive flag (fsutil file setCaseSensitiveInfo needs an elevated token and a supporting file system)."); return; }
        if (!answers.Contains("twoObjects=True")) { Assert.Skip("this machine's case-sensitive folder did not keep two names that differ only in case."); return; }
        AssertProbeSetupTook(answers);
        Assert.Contains("hardLinkListed=True", answers);
        Assert.Contains("upperKeepsGrant=True", answers);
        Assert.Contains("upperFixed=True", answers);
    }

    /// <summary>
    /// #5677: the by-hand advice both scripts print. A privilege failure names the two privileges and says how to check them
    /// (never "run elevated": both scripts already require it); a path marked as a hard link gets its own line (delete the name,
    /// or extract the file again, and no icacls on it); the per-path commands are said to be only for paths not marked as a hard
    /// link; and the per-path reset has no /T. The lock and the native class are the same text in both scripts.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheOwnerStepAdvice_NamesThePrivileges_GivesHardLinksTheirOwnLine_AndDropsTheTreeWideReset(string scriptName)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName)).Replace("\r\n", "\n");
        var lockText = ExtractFunction(script, "Lock-DarlingInstallTree");
        Assert.Contains("this elevated session does not hold SeTakeOwnershipPrivilege and SeBackupPrivilege (see whoami /priv); a policy may have removed them from Administrators", lockText, StringComparison.Ordinal);
        Assert.DoesNotContain("run elevated", lockText, StringComparison.Ordinal);
        Assert.Contains("has $detail names (a hard link): its permissions are the same under every one, so its owner and permissions were not changed", lockText, StringComparison.Ordinal);
        Assert.Contains("could not be opened, error $detail$belowNote", lockText, StringComparison.Ordinal);
        Assert.Contains("its names could not be counted, error $detail$belowNote", lockText, StringComparison.Ordinal);
        Assert.Contains("its owner could not be set, error $detail\"", lockText, StringComparison.Ordinal);
        Assert.Contains("its path resolved outside the install folder, so its owner was not changed$belowNote", lockText, StringComparison.Ordinal);
        Assert.Contains("; nothing below it was given an owner or checked", lockText, StringComparison.Ordinal);
        Assert.Contains("if ($result -ne 4 -and -not $isRoot) { $null = $skipped.Add($ownPath) }", lockText, StringComparison.Ordinal);

        Assert.DoesNotContain("/reset /T", script, StringComparison.Ordinal);
        Assert.DoesNotContain("from an elevated prompt: it makes", script, StringComparison.Ordinal);
        Assert.Contains("these two commands are only for paths not marked as a hard link", script, StringComparison.Ordinal);
        Assert.Contains("/reset /C. Remove any junction or link it names.", script, StringComparison.Ordinal);
        Assert.Contains("delete this name if it is not part of the install, or extract the file again if it is", script, StringComparison.Ordinal);
        Assert.Contains("o not run icacls on it", script, StringComparison.Ordinal);
    }

    /// <summary>#5677: the upgrade's staging-folder comment no longer says the lock's owner step needs no privilege.</summary>
    [Fact]
    public void TheUpgradeStagingFolderComment_SaysTheOwnerStepTurnsItsPrivilegesOnItself()
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1")).Replace("\r\n", "\n");
        Assert.DoesNotContain("already relies on", script, StringComparison.Ordinal);
        Assert.Contains("owner step, which changes objects this process did not create, turns on\n# SeTakeOwnershipPrivilege and SeBackupPrivilege itself.", script, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5649: every name after the leading double backslash of a UNC path is checked for a name Windows folds into another,
    /// the server and share names included (the check used to start at the share). Run under Windows PowerShell 5.1 and
    /// PowerShell 7, with both scripts. A name with a trailing dot or space anywhere on the way gives $null; a normal path
    /// below a UNC root still gives its names.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheSegmentsBelowRoot_ChecksEveryNameOfAUncPath_TheServerAndShareToo(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        AppendPathHelpers(probe, script);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            function Show([string]$label, $value) { $label + '=[' + $(if ($null -eq $value) { '$null' } else { @($value) -join '|' }) + ']' }
            Show 'unc.normal' (Get-DarlingSegmentsBelowRoot '\\srv\share\Install' '\\srv\share\Install\sub\darling.json')
            Show 'unc.serverDot' (Get-DarlingSegmentsBelowRoot '\\srv.\share\Install' '\\srv.\share\Install\darling.json')
            Show 'unc.serverSpace' (Get-DarlingSegmentsBelowRoot '\\srv \share\Install' '\\srv \share\Install\darling.json')
            Show 'unc.shareDot' (Get-DarlingSegmentsBelowRoot '\\srv\share.\Install' '\\srv\share.\Install\darling.json')
            Show 'unc.emptyServer' (Get-DarlingSegmentsBelowRoot '\\srv\share\Install' '\\\share\Install\darling.json')
            Show 'unc.leafDot' (Get-DarlingSegmentsBelowRoot '\\srv\share\Install' '\\srv\share\Install\darling.json.')
            Show 'unc.dotDot' (Get-DarlingSegmentsBelowRoot '\\srv\share\Install' '\\srv\share\Install\..\darling.json')
            Show 'drive.normal' (Get-DarlingSegmentsBelowRoot 'C:\Install' 'C:\Install\sub\darling.json')
            Show 'drive.dot' (Get-DarlingSegmentsBelowRoot 'C:\Install' 'C:\Install.\darling.json')
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        Assert.Contains("unc.normal=[sub|darling.json]", answers);
        Assert.Contains("unc.serverDot=[$null]", answers);
        Assert.Contains("unc.serverSpace=[$null]", answers);
        Assert.Contains("unc.shareDot=[$null]", answers);
        Assert.Contains("unc.emptyServer=[$null]", answers);
        Assert.Contains("unc.leafDot=[$null]", answers);
        Assert.Contains("unc.dotDot=[$null]", answers);
        Assert.Contains("drive.normal=[sub|darling.json]", answers);
        Assert.Contains("drive.dot=[$null]", answers);
    }

    private static void AppendStepFunctions(StringBuilder probe, string? script = null)
    {
        foreach (var name in new[] { "Get-DarlingServiceWrittenNames", "Test-DarlingServiceWrittenFileName", "Get-DarlingBackupFileFindingNames", "Get-DarlingBackupFileAdvice", "Test-PathIsAtOrUnder", "Test-DarlingSamePath", "Get-DarlingConfiguredPathsUnder", "Get-DarlingNewFolderPhrase", "Get-DarlingExistingInstallSteps", "Get-DarlingFreshFolderAdvice", "Get-DarlingRefusalAdvice" })
        {
            probe.AppendLine(ExtractFunction(script ?? InstallScript, name));
        }

        AppendPathHelpers(probe, script ?? InstallScript);
    }

    /// <summary>
    /// #5628: runs a probe built from <see cref="ServiceOwnedTreeProbe"/> and fails the test first, by name, if the
    /// probe's own setup did not take (a <c>setup.*=False</c> line, or no setup line at all), so that can never
    /// pass or read as a product result.
    /// </summary>
    private static List<string> RunServiceOwnedTreeProbe(StringBuilder probe)
    {
        var answers = RunWindowsPowerShell(probe.ToString());
        AssertProbeSetupTook(answers);
        return answers;
    }

    /// <summary>#5628: every <c>setup.*</c> marker a probe printed must be True, and there must be at least one.</summary>
    private static void AssertProbeSetupTook(IReadOnlyCollection<string> answers)
    {
        var setup = answers.Where(l => l.StartsWith("setup.", StringComparison.Ordinal)).ToList();
        Assert.True(setup.Count > 0, "the probe printed no setup.* marker, so nothing shows its owner or ACE setup took:\n" + string.Join("\n", answers));
        var failed = setup.Where(l => !l.EndsWith("=True", StringComparison.Ordinal)).ToList();
        Assert.True(failed.Count == 0, "the probe's own setup did not take (a test-environment problem, not a product result): " + string.Join(", ", failed) + "\nevery answer:\n" + string.Join("\n", answers));
    }

    /// <summary>
    /// A probe answer is required, and a failure prints every answer the probe gave (xunit's own message shows the first five and
    /// cuts the rest), so a failed CI run shows the whole list.
    /// </summary>
    private static void AssertAnswer(IReadOnlyCollection<string> answers, string exact)
    {
        Assert.True(answers.Contains(exact), $"expected the answer '{exact}'; every answer:\n" + string.Join("\n", answers));
    }

    /// <summary>The same for an answer found by a predicate; <paramref name="expected"/> says what it looks for.</summary>
    private static void AssertAnswer(IReadOnlyCollection<string> answers, Func<string, bool> match, string expected)
    {
        Assert.True(answers.Any(match), $"expected an answer {expected}; every answer:\n" + string.Join("\n", answers));
    }

    /// <summary>The count of answers a predicate matches must be exactly <paramref name="count"/>; a failure prints every answer.</summary>
    private static void AssertAnswerCount(IReadOnlyCollection<string> answers, Func<string, bool> match, int count, string what)
    {
        var found = answers.Count(match);
        Assert.True(found == count, $"expected {count} answer(s) {what}, found {found}; every answer:\n" + string.Join("\n", answers));
    }

    /// <summary>
    /// #5628: the PowerShell half of the same check, for the probes that set an owner or an ACE on a real file or
    /// folder. <c>Confirm-ProbeAcl</c> reads the object back and prints <c>setup.LABEL=True</c> only when the owner
    /// (when given) and every listed SID (as an explicit allow ACE) are there. Setting an owner other than the
    /// caller needs SeRestorePrivilege, and a failed <c>Set-Acl</c> is non-terminating, so without the read-back a
    /// setup that did not take looks like a product result.
    /// </summary>
    private const string ConfirmProbeAclFunction = """
        function Confirm-ProbeAcl([string]$label, [string]$path, $ownerSid, [array]$aceSids = @()) {
            $sidType = [System.Security.Principal.SecurityIdentifier]
            $took = $true
            try {
                $acl = Get-Acl -LiteralPath $path -ErrorAction Stop
                if ($ownerSid) { $took = $took -and ($acl.GetOwner($sidType) -eq $ownerSid) }
                $explicit = @($acl.GetAccessRules($true, $false, $sidType))
                foreach ($sid in $aceSids) { $took = $took -and (@($explicit | Where-Object { $_.IdentityReference -eq $sid }).Count -gt 0) }
            }
            catch { $took = $false }
            'setup.' + $label + '=' + $took
        }
        """;

    /// <summary>
    /// The shared probe for the #5627 shapes: the script's own trust, re-run and walk functions run over a real
    /// temp tree (root, darling.json, pg-runtime\pgsql\bin\postgres.exe), with <c>Get-Acl</c> shadowed the way this
    /// class already does for owners, so a service SID can own files without an elevated token. Each scenario
    /// prints <c>label.count=N</c> and <c>label.text=...</c>, and the prelude prints <c>setup.*</c> markers (#5628)
    /// that <see cref="RunServiceOwnedTreeProbe"/> requires to be True.
    /// </summary>
    private static StringBuilder ServiceOwnedTreeProbe()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Resolve-DarlingServiceAccountSid"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingServiceSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSidsForRerun"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-UntrustedWriteGrantees"));
        AppendPathHelpers(probe, InstallScript);
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            function Fail([string]$message) { Write-Host ('FAILCALLED:' + $message); exit 7 }
            $serviceName = 'PerformanceMonitor Darling'
            $sidType = [System.Security.Principal.SecurityIdentifier]
            $wk = [System.Security.Principal.WellKnownSidType]
            # The SID of 'NT SERVICE\PerformanceMonitor Darling', written out: Windows only translates that name while a
            # service of that name is registered, and the point of the fix is that the owner outlives the registration.
            $script:serviceSid = New-Object System.Security.Principal.SecurityIdentifier('S-1-5-80-3826879063-3043247242-2228279017-3709363858-944500918')
            $script:otherSid = (New-Object System.Security.Principal.NTAccount('NT SERVICE\Winmgmt')).Translate($sidType)
            $script:adminsSid = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)
            $script:systemSid = New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)
            $script:authSid = New-Object System.Security.Principal.SecurityIdentifier($wk::AuthenticatedUserSid, $null)
            $script:localServiceSid = New-Object System.Security.Principal.SecurityIdentifier($wk::LocalServiceSid, $null)

            # A REGISTERED service of that name translates to its SID; this box has none, so the logon-account lookup
            # (the re-run function's L4 check) is told the answer Windows would give. Every other name takes the
            # script's own route.
            $realResolve = ${function:Resolve-DarlingServiceAccountSid}
            function Resolve-DarlingServiceAccountSid([string]$account) {
                if ($account -eq "NT SERVICE\$serviceName") { return $script:serviceSid }
                return (& $realResolve $account)
            }

            # What the shadowed Get-Acl reports, per scenario.
            $script:runtimeOwner = $script:serviceSid      # who owns pg-runtime and everything below it
            $script:rootWriter = $null                     # a principal with Modify on the root (an inherited grant)
            $script:serviceAce = $script:serviceSid        # the account the lock/install granted Modify on pg-runtime
            $script:serviceOwned = @()                     # extra paths (relative to the root) the service owns
            $script:serviceDirs = @('darling-keys')        # the extra folders Get-DarlingExtraServiceWriteDirectories returns
            $script:fileGrants = @{}                       # relative path -> SIDs holding Modify on that one file (#5649)

            function Get-Acl {
                [CmdletBinding()]
                param([string]$LiteralPath, [Parameter(ValueFromRemainingArguments = $true)] $Rest)
                $sec = New-Object System.Security.AccessControl.DirectorySecurity
                $sec.SetAccessRuleProtection($true, $false)
                foreach ($s in @($script:systemSid, $script:adminsSid)) {
                    $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($s, 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow')))
                }
                $underRuntime = $LiteralPath -like "$($script:root)\pg-runtime*"
                if ($underRuntime) {
                    $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($script:serviceAce, 'Modify', 'ContainerInherit, ObjectInherit', 'None', 'Allow')))
                }
                if ($LiteralPath -eq $script:root -and $script:rootWriter) {
                    $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($script:rootWriter, 'Modify', 'ContainerInherit, ObjectInherit', 'None', 'Allow')))
                }
                $relative = if ($LiteralPath.Length -gt $script:root.Length) { $LiteralPath.Substring($script:root.Length + 1) } else { '' }
                foreach ($granted in @($script:fileGrants[$relative])) {
                    if ($granted) { $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($granted, 'Modify', 'None', 'None', 'Allow'))) }
                }
                $sec.SetOwner($(if ($underRuntime) { $script:runtimeOwner } elseif ($script:serviceOwned -contains $relative) { $script:serviceSid } else { $script:adminsSid }))
                return $sec
            }

            $script:root = Join-Path $env:TEMP ('pm5627-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path "$($script:root)\pg-runtime\pgsql\bin" -Force | Out-Null
            Set-Content -LiteralPath "$($script:root)\darling.json" -Value '{}'
            Set-Content -LiteralPath "$($script:root)\pg-runtime\pgsql\bin\postgres.exe" -Value 'x'
            Set-Content -LiteralPath "$($script:root)\PerformanceMonitor.Darling.Service.exe" -Value 'x'
            Set-Content -LiteralPath "$($script:root)\darling.json.bak-20260101-120000" -Value '{}'
            New-Item -ItemType Directory -Path "$($script:root)\darling-keys", "$($script:root)\other" -Force | Out-Null
            Set-Content -LiteralPath "$($script:root)\darling-keys\log-hash.key" -Value 'x'
            Set-Content -LiteralPath "$($script:root)\other\tool.dll" -Value 'x'

            # #5628: the root is named in the tree's long form. $env:TEMP is the 8.3 short form on a CI runner
            # (C:\Users\RUNNER~1\...), and Windows PowerShell 5.1 hands back the LONG form for every child
            # Get-ChildItem walks, so the shadowed Get-Acl's "$root\pg-runtime*" comparison matched nothing, every
            # owner fell back to Administrators (trusted) and every probe reported zero findings.
            $script:root = [System.IO.Path]::GetFullPath($script:root)

            # The setup check: ask the shadowed Get-Acl about a path the way the script's walk reaches it (a child
            # from Get-ChildItem) and print whether it answered with the owner this probe set. The C# side fails
            # the test on any setup.*=False, so a probe whose setup did not take can never pass or read as a
            # product result.
            $walked = @(Get-ChildItem -LiteralPath $script:root -Recurse -Force -Filter postgres.exe)
            'setup.walkFindsTheRuntimeFile=' + ($walked.Count -eq 1)
            'setup.walkPathIsUnderTheRoot=' + ($walked.Count -eq 1 -and $walked[0].FullName.StartsWith($script:root + '\', [StringComparison]::OrdinalIgnoreCase))
            'setup.shadowOwnerReachesTheWalkedFile=' + ($walked.Count -eq 1 -and ((Get-Acl -LiteralPath $walked[0].FullName).GetOwner($sidType) -eq $script:serviceSid))

            # service = $null (not registered) or a stand-in service object; logon = what Get-DarlingServiceLogonName says.
            function Get-DarlingServiceLogonName([string]$name) { return $script:logon }
            function Show([string]$label, $service, $logon) {
                $script:logon = $logon
                $sets = Get-DarlingPreLockTrustedSidsForRerun $serviceName $service
                $found = @(Get-UntrustedWriteGrantees $script:root $sets.Trusted -Recurse -ServiceSids $sets.ServiceSids -ServiceDirectories $script:serviceDirs | ForEach-Object { $_ -replace [regex]::Escape($script:root), '<root>' })
                $label + '.count=' + $found.Count
                $label + '.text=' + ($found -join ';')
            }
            $registered = [pscustomobject]@{ Name = $serviceName }
            """);
        return probe;
    }

    /// <summary>
    /// #5627, the 3.7.1-to-dev shapes: the service extracted pg-runtime and owns it, and the service is NOT
    /// registered (deleted) or logs on as another account. Only the findings that are not the service's own
    /// remain: a root write grant still refuses, and on its own it is the ONLY line. Without the derived service
    /// SID (the old trust call, no service name) the same tree lists every service-owned object.
    /// </summary>
    [Fact]
    public void ThePreLockTrustedSet_TrustsTheProductsOwnServiceSid_RegisteredOrNot_ButStillRefusesARootWrite()
    {
        var probe = ServiceOwnedTreeProbe();
        probe.AppendLine("""
            try {
                # No root write grant: the service-owned tree is clean whatever the service's state.
                Show 'deleted' $null $null
                Show 'otherLogon' $registered 'NT AUTHORITY\LOCAL SERVICE'
                Show 'virtual' $registered "NT SERVICE\$serviceName"

                # A root write grant (the folder was made under C:\) is the one real finding, and the only one.
                $script:rootWriter = $script:authSid
                Show 'rootWriteDeleted' $null $null
                Show 'rootWriteOtherLogon' $registered 'NT AUTHORITY\LOCAL SERVICE'
                Show 'rootWriteVirtual' $registered "NT SERVICE\$serviceName"
                $script:rootWriter = $null

                # The derived SID is the one Windows gives a registered service of that name.
                $derived = @(Get-DarlingServiceSids $null 'Winmgmt') | Where-Object { $_ -eq $script:otherSid }
                'derivedMatchesWindows=' + (@($derived).Count -eq 1)

                # The old call shape, with no service name: the false findings come back.
                $old = @(Get-UntrustedWriteGrantees $script:root @(Get-DarlingPreLockTrustedSids) -Recurse)
                # (Windows names the owner only while the service is registered, so the finding may carry the raw SID.)
                'oldShape.hasServiceOwned=' + (@($old | Where-Object { $_ -like '*owned by*' -and ($_ -like '*PerformanceMonitor Darling*' -or $_ -like '*944500918*') }).Count -gt 0)
            }
            finally {
                Remove-Item -LiteralPath $script:root -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunServiceOwnedTreeProbe(probe);

        Assert.Contains("deleted.count=0", answers);
        Assert.Contains("otherLogon.count=0", answers);
        Assert.Contains("virtual.count=0", answers);

        foreach (var label in new[] { "rootWriteDeleted", "rootWriteOtherLogon", "rootWriteVirtual" })
        {
            Assert.Contains($"{label}.count=1", answers);
            Assert.Contains(answers, a => a.StartsWith($"{label}.text=", StringComparison.Ordinal)
                && a.Contains("Authenticated Users on <root>", StringComparison.Ordinal)
                && !a.Contains("owned by", StringComparison.Ordinal));
        }

        Assert.Contains("oldShape.hasServiceOwned=True", answers);
        Assert.Contains("derivedMatchesWindows=True", answers);
    }

    /// <summary>
    /// #5627, the 3.9.0-to-dev shapes: a LOCKED tree (no root write grant) whose pg-runtime the service owns and
    /// holds Modify on, re-run with the service on its virtual account, on another logon account, and deleted.
    /// Passes in all three. The other-logon case holds the grant under that other account, as the lock gives it.
    /// </summary>
    [Fact]
    public void ThePreLockTrustedSet_PassesALockedTreeTheServiceOwns_OnTheVirtualAccount_AnotherLogon_AndDeleted()
    {
        var probe = ServiceOwnedTreeProbe();
        probe.AppendLine("""
            try {
                Show 'virtual' $registered "NT SERVICE\$serviceName"
                $script:serviceAce = $script:localServiceSid
                Show 'otherLogon' $registered 'NT AUTHORITY\LOCAL SERVICE'
                $script:serviceAce = $script:serviceSid
                Show 'deleted' $null $null
            }
            finally {
                Remove-Item -LiteralPath $script:root -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunServiceOwnedTreeProbe(probe);

        Assert.Contains("virtual.count=0", answers);
        Assert.Contains("otherLogon.count=0", answers);
        Assert.Contains("deleted.count=0", answers);
    }

    /// <summary>
    /// #5627: trusting OUR service's SID must not trust any other service's. A tree whose pg-runtime is owned by a
    /// different <c>NT SERVICE\</c> account is still listed, by that account's name and not by ours, with the
    /// service registered or not.
    /// </summary>
    [Fact]
    public void ThePreLockTrustedSet_StillRefusesATreeOwnedByADifferentServiceSid()
    {
        var probe = ServiceOwnedTreeProbe();
        probe.AppendLine("""
            try {
                $script:runtimeOwner = $script:otherSid
                $script:serviceAce = $script:otherSid
                Show 'otherOwnerDeleted' $null $null
                Show 'otherOwnerRegistered' $registered "NT SERVICE\$serviceName"
            }
            finally {
                Remove-Item -LiteralPath $script:root -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunServiceOwnedTreeProbe(probe);

        foreach (var label in new[] { "otherOwnerDeleted", "otherOwnerRegistered" })
        {
            Assert.DoesNotContain($"{label}.count=0", answers);
            Assert.Contains(answers, a => a.StartsWith($"{label}.text=", StringComparison.Ordinal)
                && a.Contains("owned by NT SERVICE\\WinMgmt", StringComparison.OrdinalIgnoreCase)
                && !a.Contains("PerformanceMonitor Darling", StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// #5627: the service's own account is trusted as owner only in the folders and files it writes (pg-runtime,
    /// pg-runtime-prev, darling-keys when it is a listed folder, darling.json and its backups) and as a write grantee
    /// there only. A file it owns anywhere else (the install folder, beside the service exe, in another folder)
    /// is named, with the service deleted, on another logon, or on its virtual account. One it owns inside the
    /// listed places passes.
    /// </summary>
    [Fact]
    public void ThePreLockTrustedSet_TrustsTheServiceAccountOnlyWhereTheServiceWrites()
    {
        var probe = ServiceOwnedTreeProbe();
        probe.AppendLine("""
            try {
                # Inside the list: the service owns the config, its backup and the key folder's file. Clean.
                $script:serviceOwned = @('darling.json', 'darling.json.bak-20260101-120000', 'darling-keys', 'darling-keys\log-hash.key')
                Show 'listedDeleted' $null $null
                Show 'listedOtherLogon' $registered 'NT AUTHORITY\LOCAL SERVICE'
                Show 'listedVirtual' $registered "NT SERVICE\$serviceName"

                # Outside the list: beside the service exe, and in another folder. Each is named, in every service state.
                $script:serviceOwned = @('PerformanceMonitor.Darling.Service.exe', 'other\tool.dll')
                Show 'outsideDeleted' $null $null
                Show 'outsideOtherLogon' $registered 'NT AUTHORITY\LOCAL SERVICE'
                Show 'outsideVirtual' $registered "NT SERVICE\$serviceName"

                # The install folder itself owned by the service is named too.
                $script:serviceOwned = @('')
                Show 'rootOwnedVirtual' $registered "NT SERVICE\$serviceName"

                # The key folder counts only when Get-DarlingExtraServiceWriteDirectories returned it (a managed install
                # does not), the same list the lock is handed: otherwise it is named like any other folder.
                $script:serviceDirs = @()
                $script:serviceOwned = @('darling-keys', 'darling-keys\log-hash.key')
                Show 'keysUnlistedDeleted' $null $null
                Show 'keysUnlistedVirtual' $registered "NT SERVICE\$serviceName"
            }
            finally {
                Remove-Item -LiteralPath $script:root -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunServiceOwnedTreeProbe(probe);

        foreach (var label in new[] { "listedDeleted", "listedOtherLogon", "listedVirtual" })
        {
            Assert.Contains($"{label}.count=0", answers);
        }

        foreach (var label in new[] { "outsideDeleted", "outsideOtherLogon", "outsideVirtual" })
        {
            Assert.Contains($"{label}.count=2", answers);
            Assert.Contains(answers, a => a.StartsWith($"{label}.text=", StringComparison.Ordinal)
                && a.Contains("<root>\\PerformanceMonitor.Darling.Service.exe (owned by", StringComparison.Ordinal)
                && a.Contains("<root>\\other\\tool.dll (owned by", StringComparison.Ordinal));
        }

        Assert.Contains("keysUnlistedDeleted.count=2", answers);
        Assert.Contains("keysUnlistedVirtual.count=2", answers);
        Assert.Contains("rootOwnedVirtual.count=1", answers);
        Assert.Contains(answers, a => a.StartsWith("rootOwnedVirtual.text=<root> (owned by", StringComparison.Ordinal));
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
        AppendPathHelpers(probe, InstallScript);
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine(ExtractFunction(InstallScript, "Resolve-DarlingServiceAccountSid"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine(ConfirmProbeAclFunction);
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
                Confirm-ProbeAcl 'rootHardened' $root $null @($trusted)
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
                Confirm-ProbeAcl 'exeGrantsAuthenticatedUsers' "$root\svc.exe" $null @($auth)
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
        AssertProbeSetupTook(answers);

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
        AppendPathHelpers(probe, InstallScript);
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine(ExtractFunction(InstallScript, "Resolve-DarlingServiceAccountSid"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine(ConfirmProbeAclFunction);
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
                Confirm-ProbeAcl 'templateAces' $tRoot $null @($templates | ForEach-Object { New-Object System.Security.Principal.SecurityIdentifier($_, $null) })
                'caseC_count=' + @(Get-UntrustedWriteGrantees $tRoot $trusted).Count
            }
            finally {
                icacls.exe $tRoot /reset /T /C /Q 2>&1 | Out-Null
                Remove-Item -LiteralPath $tRoot -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());
        AssertProbeSetupTook(answers);

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
    ///
    /// <para>#4052 moved the service's Modify off the root and onto <c>pg-runtime\</c> and <c>pg-runtime-prev\</c>, so
    /// the grant this test needs to see now sits one level down. The probe walks the tree with <c>-Recurse</c>, the
    /// way every real caller of the check does, and also pins that the root itself no longer carries a write grant
    /// for the service at all.</para>
    /// </summary>
    [Fact]
    public void ThePreLockWritableExtractionCheck_TrustsTheAccountTheLockItselfGranted_OnAnAlreadyLockedTree()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-UntrustedWriteGrantees"));
        AppendPathHelpers(probe, InstallScript);
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine(ExtractFunction(InstallScript, "Resolve-DarlingServiceAccountSid"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingServiceSids"));
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine(ExtractFunction(InstallScript, "Lock-DarlingInstallTree"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path ([IO.Path]::GetPathRoot([Environment]::SystemDirectory)) ('pm4043-r1-lock-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            try {
                'elevated=' + ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
                New-Item -ItemType Directory -Path $root -Force | Out-Null
                $open = @(Lock-DarlingInstallTree $root 'NT AUTHORITY\LOCAL SERVICE')
                $open | ForEach-Object { 'open:' + $_.Substring($root.Length) }

                $withoutAccount = Get-DarlingPreLockTrustedSids
                'withoutAccountCount=' + @(Get-UntrustedWriteGrantees $root $withoutAccount -Recurse).Count
                'rootOnlyWithoutAccountCount=' + @(Get-UntrustedWriteGrantees $root $withoutAccount).Count

                $serviceSids = @(Get-DarlingServiceSids 'NT AUTHORITY\LOCAL SERVICE' $null)
                'withAccountCount=' + @(Get-UntrustedWriteGrantees $root $withoutAccount -Recurse -ServiceSids $serviceSids).Count
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
        Assert.Contains("withAccountCount=0", answers);
        /* #4052: the root itself is RX for the service, so no write grant is left there to trust. */
        Assert.Contains("rootOnlyWithoutAccountCount=0", answers);

        if (answers.Contains("elevated=True"))
        {
            /* Elevated (CI): Administrators keeps Full Control on $root, so the lock creates BOTH pg-runtime\
               and pg-runtime-prev\ on this fresh tree and grants the service account Modify on each - an
               explicit grant Get-UntrustedWriteGrantees must still flag as untrusted when the caller does not
               name the account. */
            Assert.True(without > 0, "a freshly locked tree must still name the service account as untrusted when the caller does not say who it is - otherwise this test proves nothing about the new parameter: " + string.Join(" | ", answers));
        }
        else
        {
            /* Not elevated: a filtered admin token cannot use the Administrators grant on $root, so the lock
               cannot create pg-runtime\ or pg-runtime-prev\ on this fresh tree at all (#4052) - it reports both
               instead of granting anything, so there is nothing anywhere in the tree left for the walk to name
               as untrusted. */
            Assert.Equal(0, without);
            var open = answers.FindAll(a => a.StartsWith("open:", StringComparison.Ordinal));
            Assert.Contains(open, o => o.StartsWith(@"open:\pg-runtime (could not create this directory to grant the service Modify on it:", StringComparison.Ordinal));
            Assert.Contains(open, o => o.StartsWith(@"open:\pg-runtime-prev (could not create this directory to grant the service Modify on it:", StringComparison.Ordinal));
        }
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
        probe.AppendLine(ExtractFunction(InstallScript, "Resolve-DarlingServiceAccountSid"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"));
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
        AppendPathHelpers(probe, InstallScript);
        AppendServiceWrittenList(probe, InstallScript);
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
    /// Found live on 2026-09-23, when dev's upgrade refused DARLING01's pre-#4038 install root: the refusal
    /// listed "BUILTIN\Users on C:\PerformanceMonitorDarling" twice. A folder made under C:\ inherits
    /// BUILTIN\Users as two ACEs (one grants append, one grants write), and two of the four callers did not
    /// drop the repeated line. The function now returns each finding once, so every caller names a principal
    /// once per path. Same shadowed <c>Get-Acl</c> idiom as
    /// <see cref="ThePreLockWritableExtractionCheck_FlagsAnUntrustedOwner_EvenWithAFullyTrustedDacl"/>. The two
    /// ACEs get different inheritance flags so the ACL keeps them apart, as Windows did on the box, and the
    /// probe counts them first so the test cannot pass on a single merged ACE.
    /// </summary>
    [Fact]
    public void ThePreLockWritableExtractionCheck_NamesAPrincipalOnce_WhenTwoOfItsAcesGrantWrite()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Resolve-DarlingServiceAccountSid"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine("""
            $script:usersSid = New-Object System.Security.Principal.SecurityIdentifier([System.Security.Principal.WellKnownSidType]::BuiltinUsersSid, $null)
            function Get-Acl {
                [CmdletBinding()]
                param([Parameter(ValueFromRemainingArguments = $true)] $Rest)
                $sec = New-Object System.Security.AccessControl.DirectorySecurity
                $sec.SetAccessRuleProtection($true, $false)
                foreach ($s in (Get-DarlingPreLockTrustedSids)) { $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($s, 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow'))) }
                $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($script:usersSid, 'AppendData', 'ContainerInherit', 'None', 'Allow')))
                $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($script:usersSid, 'WriteData', 'ObjectInherit', 'None', 'Allow')))
                $sec.SetOwner((New-Object System.Security.Principal.SecurityIdentifier([System.Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid, $null)))
                return $sec
            }
            """);
        probe.AppendLine(ExtractFunction(InstallScript, "Get-UntrustedWriteGrantees"));
        AppendPathHelpers(probe, InstallScript);
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine("""
            $usersAces = @((Get-Acl).GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier]) | Where-Object { $_.IdentityReference -eq $script:usersSid })
            'usersAces=' + $usersAces.Count
            $trusted = Get-DarlingPreLockTrustedSids
            $found = @(Get-UntrustedWriteGrantees 'C:\does-not-need-to-exist-for-this-probe' $trusted)
            'count=' + $found.Count
            'text=' + ($found -join ';')
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.Contains("usersAces=2", answers);
        Assert.Contains("count=1", answers);
        Assert.Contains(answers, a => a.StartsWith("text=", StringComparison.Ordinal) && a.Contains("BUILTIN\\Users on C:\\does-not-need-to-exist-for-this-probe", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// #4043 round-1 review, M2: the pre-lock check is now a RECURSIVE walk, not just the root and the
    /// service exe - a child with an untrusted OWNER, or an EXPLICIT (non-inherited) write grant to an
    /// untrusted principal, must be reported even when the root's own DACL is perfectly clean; a clean
    /// recursive tree must stay silent; a junction below the root is reported, not silently skipped.
    /// </summary>
    [Fact]
    public void ThePreLockWritableExtractionCheck_Recurses_AndCatchesAChildTheRootDoesNotShow()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-UntrustedWriteGrantees"));
        AppendPathHelpers(probe, InstallScript);
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Resolve-DarlingServiceAccountSid"));
        probe.AppendLine(ConfirmProbeAclFunction);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path $env:TEMP ('pm4043-m2-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            try {
                New-Item -ItemType Directory -Path "$root\clean", "$root\owned" -Force | Out-Null
                Set-Content -LiteralPath "$root\clean\a.txt" -Value 'x'
                Set-Content -LiteralPath "$root\owned\evil.dll" -Value 'x'
                $trusted = Get-DarlingPreLockTrustedSids
                $wk = [System.Security.Principal.WellKnownSidType]
                $auth = New-Object System.Security.Principal.SecurityIdentifier($wk::AuthenticatedUserSid, $null)
                $users = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinUsersSid, $null)

                # $root itself is fully clean and protected - only a CHILD carries the problem.
                $rootSec = New-Object System.Security.AccessControl.DirectorySecurity
                $rootSec.SetAccessRuleProtection($true, $false)
                foreach ($s in $trusted) { $rootSec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($s, 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow'))) }
                Set-Acl -LiteralPath $root -AclObject $rootSec
                Set-Acl -LiteralPath "$root\clean" -AclObject $rootSec
                Confirm-ProbeAcl 'rootHardened' $root $null @($trusted)
                Confirm-ProbeAcl 'cleanChildHardened' "$root\clean" $null @($trusted)
                icacls.exe "$root\clean\a.txt" /reset /Q 2>&1 | Out-Null

                # Explicit write grant to Authenticated Users on a child file, root untouched.
                $fileSec = New-Object System.Security.AccessControl.FileSecurity
                $fileSec.SetAccessRuleProtection($true, $false)
                foreach ($s in $trusted) { $fileSec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($s, 'FullControl', 'Allow'))) }
                $fileSec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($auth, 'Modify', 'Allow')))
                Set-Acl -LiteralPath "$root\owned\evil.dll" -AclObject $fileSec
                Confirm-ProbeAcl 'evilGrantsAuthenticatedUsers' "$root\owned\evil.dll" $null @($auth)

                # A junction below the root - never descended, but reported on sight.
                & cmd.exe /c mklink /J "$root\link" "$root\clean" | Out-Null

                'cleanTreeOnlyCount=' + @(Get-UntrustedWriteGrantees "$root\clean" $trusted -Recurse).Count
                $found = @(Get-UntrustedWriteGrantees $root $trusted -Recurse)
                'fullTreeCount=' + $found.Count
                'namesAuthUsers=' + (($found -join ';') -match 'Authenticated Users')
                'namesJunction=' + (($found -join ';') -match '\(a junction or link\)')
                'nonRecurseStaysSilent=' + @(Get-UntrustedWriteGrantees $root $trusted).Count
            }
            finally {
                if (Test-Path -LiteralPath "$root\link") { & cmd.exe /c rmdir "$root\link" | Out-Null }
                if (Test-Path -LiteralPath $root) {
                    & icacls.exe $root /reset /T /C /Q 2>&1 | Out-Null
                    Remove-Item -LiteralPath $root -Recurse -Force
                }
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());
        AssertProbeSetupTook(answers);

        Assert.Contains("cleanTreeOnlyCount=0", answers);
        var full = int.Parse(answers.Find(a => a.StartsWith("fullTreeCount=", StringComparison.Ordinal))!.Substring("fullTreeCount=".Length));
        Assert.True(full >= 2, "the recursive walk must report both the explicit grant on the child file and the junction: " + string.Join(" | ", answers));
        Assert.Contains("namesAuthUsers=True", answers);
        Assert.Contains("namesJunction=True", answers);
        Assert.Contains("nonRecurseStaysSilent=0", answers);
    }

    /// <summary>
    /// #4043 round-1 review, M2: a principal that is a DIRECT member of BUILTIN\Administrators is trusted as
    /// an owner even though it is not the account running this script and not one of the four base SIDs -
    /// an admin can already do anything on the box, so a tree a DIFFERENT admin extracted is not a finding.
    /// The current user stands in for "a direct member" since <c>Get-DarlingPreLockTrustedSidsForRerun</c>'s
    /// probe process always runs as an actual member of that group on a dev/CI box.
    /// </summary>
    [Fact]
    public void ThePreLockWritableExtractionCheck_TrustsADirectAdministratorsMember_AsOwner()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $members = Get-LocalAdministratorsDirectMemberSids
            'count=' + $members.Count
            $me = [Security.Principal.WindowsIdentity]::GetCurrent().User
            'meIsDirectMember=' + ($members -contains $me)
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.True(int.Parse(answers.Find(a => a.StartsWith("count=", StringComparison.Ordinal))!.Substring("count=".Length)) > 0,
            "BUILTIN\\Administrators must enumerate at least one member on a real Windows box: " + string.Join(" | ", answers));
    }

    /// <summary>
    /// #4043 round-1 review, L3/L4: a re-run must fail with a message NAMING the problem, not silently
    /// mis-trust or mis-refuse. L3: the service is registered but its logon account cannot be read at all -
    /// same message as step 1b2. L4: the logon account is readable but will not resolve to a SID (a
    /// stand-in for an unreachable-domain gMSA) - names the account instead of showing a raw SID later.
    /// </summary>
    [Fact]
    public void GetDarlingPreLockTrustedSidsForRerun_FailsWithANamedReason_RatherThanSilentlyDroppingTheAccount()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"));
        probe.AppendLine(ExtractFunction(InstallScript, "Resolve-DarlingServiceAccountSid"));
        probe.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSidsForRerun"));
        probe.AppendLine("""
            function Fail([string]$message) { Write-Host ('FAILCALLED:' + $message); exit 7 }

            # L3: Get-DarlingServiceLogonName returns nothing.
            function Get-DarlingServiceLogonName([string]$name) { return $null }
            & { $null = Get-DarlingPreLockTrustedSidsForRerun 'Some Service' ([pscustomobject]@{ Name = 'Some Service' }) }
            'l3ExitCode=' + $LASTEXITCODE
            """);

        var l3 = RunWindowsPowerShell(probe.ToString());
        Assert.Contains(l3, a => a.StartsWith("FAILCALLED:", StringComparison.Ordinal) && a.Contains("sc.exe qc", StringComparison.Ordinal));

        var probe2 = new StringBuilder();
        probe2.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSids"));
        probe2.AppendLine(ExtractFunction(InstallScript, "Get-LocalAdministratorsDirectMemberSids"));
        probe2.AppendLine(ExtractFunction(InstallScript, "Resolve-DarlingServiceAccountSid"));
        probe2.AppendLine(ExtractFunction(InstallScript, "Get-DarlingPreLockTrustedSidsForRerun"));
        probe2.AppendLine("""
            function Fail([string]$message) { Write-Host ('FAILCALLED:' + $message); exit 7 }

            # L4: a name is returned but will never translate - stands in for a gMSA an unreachable DC cannot answer for.
            function Get-DarlingServiceLogonName([string]$name) { return 'CONTOSO\doesnotexist-4043-round1$' }
            & { $null = Get-DarlingPreLockTrustedSidsForRerun 'Some Service' ([pscustomobject]@{ Name = 'Some Service' }) }
            'l4ExitCode=' + $LASTEXITCODE
            """);

        var l4 = RunWindowsPowerShell(probe2.ToString());
        Assert.Contains(l4, a => a.StartsWith("FAILCALLED:", StringComparison.Ordinal) && a.Contains("CONTOSO\\doesnotexist-4043-round1$", StringComparison.Ordinal));
    }

    /// <summary>#4043 round-1 review, H1: the zip-staging folder's ACL is protected - SYSTEM and
    /// Administrators only, full control, inheritance removed, Administrators as owner.</summary>
    [Fact]
    public void ProtectDarlingStagingFolder_LocksTheFolderToSystemAndAdministratorsOnly()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(upgrade, "Protect-DarlingStagingFolder"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $path = Join-Path $env:TEMP ('pm4043-h1-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $path -Force | Out-Null
            try {
                'elevated=' + ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
                # /setowner to BUILTIN\Administrators needs an ELEVATED token (the group is only marked
                # owner-capable there) - not tested end to end on a non-elevated dev box, same reasoning
                # ThePreLockWritableExtractionCheck_FlagsAnUntrustedOwner_EvenWithAFullyTrustedDacl documents.
                # The inheritance-removal and the two grants happen BEFORE /setowner, so they are still
                # checked below even when this throws on a non-elevated run.
                try { Protect-DarlingStagingFolder $path } catch { 'protectThrew=' + $_.Exception.Message }
                $acl = Get-Acl -LiteralPath $path
                $sidType = [System.Security.Principal.SecurityIdentifier]
                $wk = [System.Security.Principal.WellKnownSidType]
                $adminsSid = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)
                'protected=' + $acl.AreAccessRulesProtected
                'ownerIsAdmins=' + ($acl.GetOwner($sidType) -eq $adminsSid)
                $rules = @($acl.GetAccessRules($true, $false, $sidType))
                'ruleCount=' + $rules.Count
                'allFullControl=' + (@($rules | Where-Object { $_.FileSystemRights -ne [System.Security.AccessControl.FileSystemRights]::FullControl }).Count -eq 0)
            }
            finally {
                icacls.exe $path /reset /T /C /Q 2>&1 | Out-Null
                Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.Contains("protected=True", answers);
        Assert.Contains("ruleCount=2", answers);
        Assert.Contains("allFullControl=True", answers);
        if (answers.Contains("elevated=True"))
        {
            Assert.Contains("ownerIsAdmins=True", answers);
        }
    }

    /// <summary>#4043: the staging lock leaves SYSTEM and Administrators ONLY even when the folder already carries
    /// explicit entries. <c>icacls /inheritance:r</c> strips only INHERITED entries, and CI's elevated runner makes
    /// a TEMP folder with an explicit one of its own: three rules where two were expected. The entry is planted here
    /// directly, BUILTIN\Users with Modify, so every box reproduces it, elevated or not. An earlier version of this
    /// test left it to the creator's default DACL to supply one, and CI's runner supplies none that way.</summary>
    [Fact]
    public void ProtectDarlingStagingFolder_StripsExplicitEntriesAlreadyOnTheFolder()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(upgrade, "Protect-DarlingStagingFolder"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $path = Join-Path $env:TEMP ('pm4043-np-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $path -Force | Out-Null
            $sidType = [System.Security.Principal.SecurityIdentifier]
            $usersSid = 'S-1-5-32-545'
            try {
                icacls.exe $path /grant "*${usersSid}:(OI)(CI)M" | Out-Null
                'usersExplicitBefore=' + (@((Get-Acl -LiteralPath $path).GetAccessRules($true, $false, $sidType) | Where-Object { $_.IdentityReference.Value -eq $usersSid }).Count -gt 0)
                try { Protect-DarlingStagingFolder $path } catch { 'protectThrew=' + $_.Exception.Message }
                $rules = @((Get-Acl -LiteralPath $path).GetAccessRules($true, $false, $sidType))
                'ruleCount=' + $rules.Count
                $wk = [System.Security.Principal.WellKnownSidType]
                $allowed = @(
                    (New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)).Value,
                    (New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)).Value)
                'onlySystemAndAdmins=' + (@($rules | Where-Object { $allowed -notcontains $_.IdentityReference.Value }).Count -eq 0)
            }
            finally {
                icacls.exe $path /reset /T /C /Q 2>&1 | Out-Null
                Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        /* The precondition, so the test cannot pass by the folder simply starting without the entry: the lock
           must have something explicit to strip. */
        Assert.Contains("usersExplicitBefore=True", answers);
        Assert.Contains("ruleCount=2", answers);
        Assert.Contains("onlySystemAndAdmins=True", answers);
    }

    /// <summary>#4043 round-2 review, M-a: a folder the recursive walk cannot list is a finding, not a silent
    /// skip. <c>Get-ChildItem -ErrorAction SilentlyContinue</c> used to drop it, and nothing under it reached the
    /// checks, so a tree hiding a swapped binary behind a folder that refuses listing read as clean. The running
    /// account is denied List Folder on one child and trusted everywhere else, so the one thing the walk can say
    /// about that child is that it could not look inside. Both scripts, since each ships its own copy.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void ThePreLockWritableExtractionCheck_ReportsAFolderItCannotList_RatherThanSkippingIt(string scriptName)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Get-UntrustedWriteGrantees"));
        AppendPathHelpers(probe, script);
        AppendServiceWrittenList(probe, script);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $root = Join-Path $env:TEMP ('pm4043-ma-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            $me = [Security.Principal.WindowsIdentity]::GetCurrent().User
            try {
                New-Item -ItemType Directory -Path "$root\sealed" -Force | Out-Null
                Set-Content -LiteralPath "$root\sealed\payload.dll" -Value 'x'
                icacls.exe "$root\sealed" /deny "*$($me.Value):(RD)" | Out-Null
                $wk = [System.Security.Principal.WellKnownSidType]
                $trusted = @(
                    $me,
                    (New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)),
                    (New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)))
                $found = @(Get-UntrustedWriteGrantees $root $trusted -Recurse)
                foreach ($f in $found) { 'finding=' + $f }
                'namesTheSealedFolder=' + (@($found | Where-Object { $_ -like "*\sealed (its contents could not be listed:*" }).Count -eq 1)
            }
            finally {
                if (Test-Path -LiteralPath "$root\sealed") { icacls.exe "$root\sealed" /remove:d "*$($me.Value)" 2>&1 | Out-Null }
                if (Test-Path -LiteralPath $root) { Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue }
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.True(answers.Contains("namesTheSealedFolder=True"),
            "the walk must name the folder it could not list, not skip it: " + string.Join(" | ", answers));
    }

    /// <summary>#4043 round-2 review: without -Sha256, SHA256SUMS.txt is the whole proof, so the upgrade reads it
    /// only after its folder passes the recursive check and the check's refusal, and reads it once. A read moved
    /// above the check, or a second read by any route, trusts a sidecar that anyone able to write the folder could
    /// have edited.</summary>
    [Fact]
    public void TheSha256SumsSidecarIsReadOnlyAfterItsFolderPassesTheRecursiveCheck()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        const string read = "Get-Content -LiteralPath $sums";

        var check = upgrade.IndexOf("$sumsFolderWritable = @(Get-UntrustedWriteGrantees $sourceRoot $sourceTrusted -Recurse)", StringComparison.Ordinal);
        Assert.True(check >= 0, "upgrade-darling.ps1 no longer checks the folder SHA256SUMS.txt sits in (#4043 round-1 review, H1)");
        var refuse = upgrade.IndexOf("if ($sumsFolderWritable.Count -gt 0) {", check, StringComparison.Ordinal);
        Assert.True(refuse > check, "the SHA256SUMS.txt folder check no longer refuses on a finding");
        var readAt = upgrade.IndexOf(read, StringComparison.Ordinal);
        Assert.True(readAt > refuse, "SHA256SUMS.txt must be read only AFTER its folder passes the check");
        var fail = upgrade.IndexOf("Fail @\"", refuse, StringComparison.Ordinal);
        Assert.True(fail > refuse && fail < readAt, "the SHA256SUMS.txt folder check must stop the run before the file is read");

        /* Every use of $sums is the path, the existence test or the one read below the check. */
        foreach (Match use in Regex.Matches(upgrade, @"\$sums\b"))
        {
            var start = upgrade.LastIndexOf('\n', use.Index) + 1;
            var end = upgrade.IndexOf('\n', use.Index);
            var line = (end < 0 ? upgrade.Substring(start) : upgrade.Substring(start, end - start)).Trim();
            Assert.True(
                line == "$sums = Join-Path $sourceRoot 'SHA256SUMS.txt'"
                || line == "if (Test-Path -LiteralPath $sums) {"
                || (use.Index >= readAt && use.Index < readAt + read.Length),
                $"an unexpected use of $sums: '{line}'. SHA256SUMS.txt is read once, after its folder check");
        }
    }

    /// <summary>#4043: each script checks the tree for write grants BEFORE it changes anything. A check after the
    /// lock would pass a tree the lock had just rewritten, which says nothing about who could write to it before;
    /// a check after the service stop, the backup or the extract refuses a tree this run has already changed.
    /// Same shape as <see cref="LocationGuard_RunsBeforeAnythingIsInstalled"/>.</summary>
    [Fact]
    public void TheWritableTreeCheck_RunsBeforeEitherScriptChangesAnything()
    {
        AssertRunsFirst(
            InstallScript,
            "install-darling.ps1",
            "$writableRecords = @(Get-UntrustedWriteGrantees $root $preLockTrusted.Trusted -AsRecords -Recurse -ServiceSids $preLockTrusted.ServiceSids -ServiceDirectories",
            new[]
            {
                ("Invoke-InstallTreeLock $lockAccount -StopOnOpen", "the step 1b2 install-tree lock"),
                ("Initialize-DarlingMinimalConfig $configPath", "writing the minimal darling.json"),
                ("& $serviceExe --test-connection", "the --test-connection pre-flight"),
                ("New-EventLog -LogName Application", "registering the Event Log source"),
                ("Invoke-ScWithQuotedBinPath 'create'", "creating the service"),
                ("Set-Acl -LiteralPath $secretFile", "hardening the config's ACL"),
                ("Start-Service -Name $serviceName", "starting the service"),
            });

        AssertRunsFirst(
            ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1")),
            "upgrade-darling.ps1",
            "$writableRecords = @(Get-UntrustedWriteGrantees $InstallRoot $preLockTrusted.Trusted -AsRecords -Recurse -ServiceSids $preLockTrusted.ServiceSids -ServiceDirectories",
            new[]
            {
                ("$zipStagingFolder = New-DarlingProtectedStagingFolder", "creating the zip staging folder"),
                ("Copy-Item -LiteralPath $Source -Destination $stagedZip", "staging the zip"),
                ("Stop-Service -Name $serviceName -Force", "stopping the service"),
                ("Lock-DarlingInstallTree $InstallRoot $logonAccount", "locking the install tree"),
                ("New-Item -ItemType Directory -Path $backupPath", "creating the rollback backup"),
                ("Expand-Archive -LiteralPath $Source -DestinationPath $InstallRoot", "extracting a zip over the tree"),
                ("Get-ChildItem -LiteralPath $Source -Force | Copy-Item -Destination $InstallRoot", "copying a folder over the tree"),
                ("Start-Service -Name $serviceName", "starting the service"),
            });

        static void AssertRunsFirst(string script, string scriptName, string check, (string Marker, string What)[] steps)
        {
            var at = script.IndexOf(check, StringComparison.Ordinal);
            Assert.True(at >= 0, $"{scriptName} no longer checks the tree for write grants before it changes anything (#4043)");
            foreach (var (marker, what) in steps)
            {
                var step = script.IndexOf(marker, StringComparison.Ordinal);
                Assert.True(step >= 0, $"{scriptName} no longer contains {what} ('{marker}')");
                Assert.True(at < step, $"{scriptName} must check the tree BEFORE {what} (#4043)");
            }
        }
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
        AppendServiceWrittenList(probe, InstallScript);
        probe.AppendLine(ExtractFunction(InstallScript, "Lock-DarlingInstallTree"));
        probe.AppendLine(ConfirmProbeAclFunction);
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
                Confirm-ProbeAcl 'configAces' "$root\darling.json" $null @($system, $admins, $service, $interactive)
                # #4038 round 2's High: a child an ordinary user owns and grants to itself behind its own
                # protection, as one who re-created it while an older install was open would leave it.
                $p = New-Object System.Security.AccessControl.DirectorySecurity
                $p.SetAccessRuleProtection($true, $false)
                $p.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($me, 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow')))
                $p.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($auth, 'Modify', 'ContainerInherit, ObjectInherit', 'None', 'Allow')))
                Set-Acl -LiteralPath "$root\planted" -AclObject $p
                Confirm-ProbeAcl 'plantedAces' "$root\planted" $null @($me, $auth)
                $x = New-Object System.Security.AccessControl.FileSecurity
                $x.SetAccessRuleProtection($true, $false)
                $x.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($me, 'FullControl', 'Allow')))
                Set-Acl -LiteralPath "$root\svc.exe" -AclObject $x
                Confirm-ProbeAcl 'exeAce' "$root\svc.exe" $null @($me)
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

                # #4038's leftover shape (round 3, #4052): an install locked BEFORE this fix left an explicit
                # (OI)(CI)M ACE for the service on $root, and plain icacls /grant only ADDS to an existing
                # explicit ACE for that SID rather than replacing it - so re-running the OLD lock would never
                # have cleared it. Planted directly (not through the old code, which no longer exists) so the
                # phase-2 assertions below prove /grant:r removes it rather than merely never having added it.
                $null = & icacls.exe $root /grant "*$($service.Value):(OI)(CI)M" 2>&1

                # Phase 2, as step 4b2 and the upgrade run it: the service's grant, and the walk's report.
                $open = @(Lock-DarlingInstallTree $root 'NT SERVICE\TrustedInstaller')
                $open | ForEach-Object { 'open:' + $_.Substring($root.Length) }

                $rootAcl = Get-Acl -LiteralPath $root
                'protected=' + $rootAcl.AreAccessRulesProtected
                'authenticatedUsers=' + @(Rules $root | Where-Object { $_.IdentityReference -eq $auth }).Count
                'usersRights=' + ((Rules $root | Where-Object { $_.IdentityReference -eq $users } | ForEach-Object { $_.FileSystemRights }) -join ',')
                'serviceRights=' + ((Rules $root | Where-Object { $_.IdentityReference -eq $service } | ForEach-Object { $_.FileSystemRights }) -join ',')
                'postgresServiceInherited=' + @(Rules "$root\pg-runtime\pgsql\bin\postgres.exe" | Where-Object { $_.IdentityReference -eq $service -and $_.IsInherited }).Count
                # #4052: the service's own real access is Modify on pg-runtime\ and pg-runtime-prev\ (an
                # explicit ACE there, not inherited from root, since root only grants RX), and NOTHING on the
                # svc.exe / planted paths above - those are a stranger's grant even though the SID is the same.
                'pgRuntimeServiceRights=' + ((Rules "$root\pg-runtime" | Where-Object { $_.IdentityReference -eq $service -and -not $_.IsInherited } | ForEach-Object { $_.FileSystemRights }) -join ',')
                # Not elevated, pg-runtime-prev\ never gets made (#4052: a filtered admin token cannot use the
                # Administrators grant left on $root, so the best-effort New-Item fails and the lock reports it
                # in $open instead) - so there is no ACL to read here, only the 'open:' line above to check.
                if (Test-Path -LiteralPath "$root\pg-runtime-prev") {
                    'pgRuntimePrevServiceRights=' + ((Rules "$root\pg-runtime-prev" | Where-Object { $_.IdentityReference -eq $service -and -not $_.IsInherited } | ForEach-Object { $_.FileSystemRights }) -join ',')
                }
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
        AssertProbeSetupTook(answers);

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
            /* #4052, not elevated: a filtered admin token cannot use the Administrators grant left on $root, so
               the best-effort New-Item for pg-runtime-prev\ (already made by this test's own setup, so pg-runtime\
               never hits this) fails, and the lock reports it here rather than granting anything. That report is
               neither a junction, a "could not make owner" line, nor an "(owned by...)" line, so it is checked on
               its own, separately from the ownership invariant below. */
            Assert.Contains(open, o => o.StartsWith(@"open:\pg-runtime-prev (could not create this directory to grant the service Modify on it:", StringComparison.Ordinal));
            var owned = open.FindAll(o => !o.Contains("junction or link", StringComparison.Ordinal) && !o.Contains("could not make", StringComparison.Ordinal) && !o.Contains("could not create this directory", StringComparison.Ordinal));
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
        /* #4052: the service's grant on $root is /grant:r'd to Read & Execute only - not Modify - so a
           leftover Modify ACE from an earlier #4038-shaped install (an explicit (OI)(CI)M ADD, which plain
           /grant would only ADD to and never remove) does not survive an upgrade through this lock. */
        Assert.Contains("serviceRights=ReadAndExecute, Synchronize", answers);
        Assert.Contains("postgresServiceInherited=1", answers);
        Assert.Contains("pgRuntimeServiceRights=Modify, Synchronize", answers);
        if (answers.Contains("elevated=True"))
        {
            /* Elevated (CI): Administrators keeps Full Control on $root as an explicit ACE, so the lock can
               create pg-runtime-prev\ too and grants the service the same explicit Modify there as pg-runtime\. */
            Assert.Contains("pgRuntimePrevServiceRights=Modify, Synchronize", answers);
        }
        else
        {
            /* Not elevated: pg-runtime-prev\ was never created (asserted above via the 'open:' report), so
               there is no ACL to read and this probe never emits the line at all. */
            Assert.DoesNotContain(answers, a => a.StartsWith("pgRuntimePrevServiceRights=", StringComparison.Ordinal));
        }
        Assert.Contains("jsonProtected=True", answers);
        Assert.Contains("jsonInteractive=Read, Synchronize", answers);
    }

    /// <summary>
    /// #5649, from the failed run of a GitHub Windows runner, where $env:TEMP is the 8.3 short form (C:\Users\RUNNER~1\...) and
    /// everything listed under it comes back in the long form: a root typed one way and a path joined or listed the other
    /// compared unequal, so the walk reported no hard link and the path test trusted none of the service's folders. The
    /// scripts now put every root and every path in one long spelling (Get-DarlingCanonicalPath) before comparing. This test
    /// gives the walk, the path test, the root-file list, the backup-name test and the key-folder test the same install
    /// folder spelled as its short path, the short path with a trailing backslash, the long path in upper case, in lower
    /// case and with a trailing backslash, and requires the same answers for each, on any machine that has 8.3 names. The
    /// trust answers are exact: only the service's own folders and its two kinds of root file, nothing else.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheWalkAndThePathTests_AnswerTheSameForARootInShortFormWithATrailingBackslashOrInOtherCase(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Get-UntrustedWriteGrantees"));
        AppendPathHelpers(probe, script);
        AppendServiceWrittenList(probe, script);
        probe.AppendLine(ExtractFunction(script, "Get-DarlingBackupFileFindingNames"));
        probe.AppendLine(ExtractFunction(script, "Get-DarlingExtraServiceWriteDirectories"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $parent = Join-Path $env:TEMP ('pm5649-short-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            $outside = "$parent-outside.txt"
            # CreateDirectory, not New-Item: Windows PowerShell 5.1's New-Item refuses a new folder chain whose parent is typed as a short path.
            foreach ($made in @("$parent\install-root-with-a-long-name\pg-runtime", "$parent\install-root-with-a-long-name\other")) { [void][IO.Directory]::CreateDirectory($made) }
            # The long spelling is what the listing returns for an existing folder; the short one comes from the file system.
            $long = (Get-Item -LiteralPath "$parent\install-root-with-a-long-name" -Force).FullName
            $short = ((cmd /c "for %I in (""$long"") do @echo %~sI") | Select-Object -First 1).Trim()
            try {
                if ($short.IndexOf('~') -lt 0 -or $short -ieq $long) { 'noShortNames=True'; return }
                'shortForm=' + $short
                'longForm=' + $long
                Set-Content -LiteralPath "$long\darling.json" -Value '{ "postgres": { "managed": false } }'
                Set-Content -LiteralPath "$long\darling.json.bak-plain" -Value 'x'
                Set-Content -LiteralPath "$long\notes.txt" -Value 'x'
                Set-Content -LiteralPath "$long\pg-runtime\a.dll" -Value 'x'
                Set-Content -LiteralPath "$long\other\darling.json" -Value 'x'
                Set-Content -LiteralPath $outside -Value 'x'
                New-Item -ItemType HardLink -Path "$long\darling.json.bak-linked" -Value $outside | Out-Null
                $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
                $plainItem = [pscustomobject]@{ PSIsContainer = $false; LinkType = $null; Attributes = [IO.FileAttributes]::Normal }
                $forms = [ordered]@{ short = $short; shortSlash = $short + '\'; upper = $long.ToUpperInvariant(); lower = $long.ToLowerInvariant(); longSlash = $long + '\' }
                foreach ($tag in $forms.Keys) {
                    $form = $forms[$tag]
                    $records = @(Get-UntrustedWriteGrantees $form @($me) -Recurse -AsRecords)
                    'walk[' + $tag + ']=' + (@($records | Where-Object { $_.Kind -eq 'HardLink' } | ForEach-Object { [IO.Path]::GetFileName($_.Path) } | Sort-Object) -join ',')
                    'rootFiles[' + $tag + ']=' + (@(Get-DarlingServiceWrittenRootFiles $form | ForEach-Object { $_.Name } | Sort-Object) -join ',')
                    foreach ($name in @('pg-runtime', 'pg-runtime\a.dll', 'darling.json', 'darling.json.bak-plain', 'darling.json.bak-linked', 'notes.txt', 'other', 'other\darling.json', 'pg-runtime.\a.dll', 'pg-runtime\..\notes.txt')) {
                        $onDisk = Join-Path $long $name
                        $item = if (Test-Path -LiteralPath $onDisk -PathType Leaf) { Get-Item -LiteralPath $onDisk -Force } else { $plainItem }
                        # The candidate is joined onto the root as the caller was handed it, and again onto the long spelling.
                        'trust[' + $tag + ']' + $name + '=' + (Test-DarlingServiceWrittenPath $form (Join-Path $form $name) @() $item)
                        'trustLongCandidate[' + $tag + ']' + $name + '=' + (Test-DarlingServiceWrittenPath $form (Join-Path $long $name) @() $item)
                        'trustShortCandidate[' + $tag + ']' + $name + '=' + (Test-DarlingServiceWrittenPath $form (Join-Path $short $name) @() $item)
                    }
                    foreach ($spelling in @(@('long', $long), @('short', $short))) {
                        $finding = [pscustomobject]@{ Kind = 'Grant'; IsFile = $true; Path = (Join-Path $spelling[1] 'darling.json.bak-plain') }
                        $other = [pscustomobject]@{ Kind = 'Grant'; IsFile = $true; Path = (Join-Path $spelling[1] 'notes.txt') }
                        'backupNames[' + $tag + '/' + $spelling[0] + ']=' + (@(Get-DarlingBackupFileFindingNames $form @($finding)) -join ',') + '|' + (@(Get-DarlingBackupFileFindingNames $form @($finding, $other)) -join ',')
                    }
                    'keyFolders[' + $tag + ']=' + (@(Get-DarlingExtraServiceWriteDirectories $form (Join-Path $form 'darling.json')) -join ',')
                }
                'canonicalShort=' + (Get-DarlingCanonicalPath $short)
                'canonicalMissingTail=' + (Get-DarlingCanonicalPath "$short\not-there\deeper")
                'canonicalNothing=[' + (Get-DarlingCanonicalPath '') + ']'
            }
            finally {
                Remove-Item -LiteralPath $parent -Recurse -Force -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath $outside -Force -ErrorAction SilentlyContinue
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        if (answers.Contains("noShortNames=True"))
        {
            Assert.Skip("8.3 short names are not created on the volume the temp folder is on, so there is no short spelling to test.");
            return;
        }

        var longForm = answers.Single(a => a.StartsWith("longForm=", StringComparison.Ordinal))["longForm=".Length..];
        var shortForm = answers.Single(a => a.StartsWith("shortForm=", StringComparison.Ordinal))["shortForm=".Length..];
        Assert.Contains("~", shortForm, StringComparison.Ordinal);
        Assert.NotEqual(longForm, shortForm, StringComparer.OrdinalIgnoreCase);

        var trusted = new[] { "pg-runtime", "pg-runtime\\a.dll", "darling.json", "darling.json.bak-plain" };
        var untrusted = new[] { "darling.json.bak-linked", "notes.txt", "other", "other\\darling.json", "pg-runtime.\\a.dll", "pg-runtime\\..\\notes.txt" };
        foreach (var tag in new[] { "short", "shortSlash", "upper", "lower", "longSlash" })
        {
            Assert.Contains($"walk[{tag}]=darling.json.bak-linked", answers);
            Assert.Contains($"rootFiles[{tag}]=darling.json,darling.json.bak-plain", answers);
            /* A finding on a backup alone is named, whichever spelling its path and the root come in; one beside a file that is not a backup is not. */
            Assert.Contains($"backupNames[{tag}/long]=darling.json.bak-plain|", answers);
            Assert.Contains($"backupNames[{tag}/short]=darling.json.bak-plain|", answers);
            Assert.Contains($"keyFolders[{tag}]=darling-keys", answers);
            foreach (var kind in new[] { "trust", "trustLongCandidate", "trustShortCandidate" })
            {
                foreach (var name in trusted) { Assert.Contains($"{kind}[{tag}]{name}=True", answers); }
                foreach (var name in untrusted) { Assert.Contains($"{kind}[{tag}]{name}=False", answers); }
            }
        }

        Assert.Contains($"canonicalShort={longForm}", answers, StringComparer.OrdinalIgnoreCase);
        Assert.Contains($"canonicalMissingTail={longForm}\\not-there\\deeper", answers, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("canonicalNothing=[]", answers);
    }

    /// <summary>
    /// #5649: the lock and the walk compare the install folder and every path under it in one spelling (the canonical long
    /// one) and never through a bare GetFullPath, which spells short names out differently from one runtime to the next. A
    /// path that cannot be worked out is reported or left untrusted, never passed. The behaviour is run by the test above;
    /// this one pins that the lock, which needs an elevated account to run, goes through the same helper.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheLockAndTheWalk_PutTheRootAndEveryPathInTheCanonicalLongSpellingBeforeComparing(string scriptName)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var lockText = ExtractFunction(script, "Lock-DarlingInstallTree");
        Assert.Contains("$rootCanonical = Get-DarlingCanonicalPath $root", lockText, StringComparison.Ordinal);
        Assert.Contains("$root = $rootCanonical", lockText, StringComparison.Ordinal);
        Assert.Contains("$path = Get-DarlingCanonicalPath $target.FullName", lockText, StringComparison.Ordinal);
        Assert.Contains("Get-DarlingCanonicalPath (Get-Item -LiteralPath $dirPath -Force).FullName", lockText, StringComparison.Ordinal);
        Assert.Contains("$rootFull = Get-DarlingCanonicalPath $path", ExtractFunction(script, "Get-UntrustedWriteGrantees"), StringComparison.Ordinal);
        foreach (var name in new[] { "Get-UntrustedWriteGrantees", "Lock-DarlingInstallTree", "Test-DarlingServiceWrittenPath", "Get-DarlingBackupFileFindingNames", "Get-DarlingExtraServiceWriteDirectories" })
        {
            Assert.DoesNotContain("GetFullPath", ExtractFunction(script, name), StringComparison.Ordinal);
        }
        Assert.Contains("Get-DarlingSegmentsBelowRoot $root $candidate", ExtractFunction(script, "Test-DarlingServiceWrittenPath"), StringComparison.Ordinal);
    }

    /// <summary>
    /// #5673: the same-folder test and the at-or-under test put both paths in the canonical long spelling
    /// (Get-DarlingCanonicalPath) before comparing, so one folder gives one answer however it is typed. This test gives each
    /// helper an install folder in its long form, as a short 8.3 path, as a short path with a trailing backslash, in upper
    /// case, in lower case and with a trailing backslash, and requires the same answer for every form, for a folder below it
    /// (also one that does not exist yet) and for a sibling whose name only starts the same. The short-path forms need 8.3
    /// names on the volume; without them those forms are left out and the test is skipped at the end, after the trailing
    /// backslash and case forms have been checked.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheSameFolderTestAndTheAtOrUnderTest_AnswerTheSameForAFolderInShortFormWithATrailingBackslashOrInOtherCase(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Get-DarlingCanonicalPath"));
        probe.AppendLine(ExtractFunction(script, "Test-DarlingSamePath"));
        probe.AppendLine(ExtractFunction(script, "Test-PathIsAtOrUnder"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $parent = Join-Path $env:TEMP ('pm5673-short-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            # CreateDirectory, not New-Item: Windows PowerShell 5.1's New-Item refuses a new folder chain whose parent is typed as a short path.
            foreach ($made in @("$parent\install-root-with-a-long-name", "$parent\install-root-with-a-long-name-sibling")) { [void][IO.Directory]::CreateDirectory($made) }
            $long = (Get-Item -LiteralPath "$parent\install-root-with-a-long-name" -Force).FullName
            $sibling = (Get-Item -LiteralPath "$parent\install-root-with-a-long-name-sibling" -Force).FullName
            $short = ((cmd /c "for %I in (""$long"") do @echo %~sI") | Select-Object -First 1).Trim()
            try {
                $forms = [ordered]@{ long = $long; longSlash = $long + '\'; upper = $long.ToUpperInvariant(); lower = $long.ToLowerInvariant() }
                if ($short.IndexOf('~') -lt 0 -or $short -ieq $long) { 'noShortNames=True' }
                else {
                    'shortForm=' + $short
                    'longForm=' + $long
                    $forms['short'] = $short
                    $forms['shortSlash'] = $short + '\'
                    $forms['shortUpper'] = $short.ToUpperInvariant()
                }
                foreach ($tag in $forms.Keys) {
                    $form = $forms[$tag]
                    foreach ($other in $forms.Keys) {
                        'same[' + $tag + '/' + $other + ']=' + (Test-DarlingSamePath $form $forms[$other])
                        'atOrUnderSelf[' + $tag + '/' + $other + ']=' + (Test-PathIsAtOrUnder $form $forms[$other])
                    }
                    'sameSibling[' + $tag + ']=' + (Test-DarlingSamePath $form $sibling)
                    'sameChild[' + $tag + ']=' + (Test-DarlingSamePath $form "$long\child")
                    'childUnder[' + $tag + ']=' + (Test-PathIsAtOrUnder "$long\child" $form)
                    'childOfFormUnder[' + $tag + ']=' + (Test-PathIsAtOrUnder (Join-Path $form 'not-there\deeper') $long)
                    'siblingUnder[' + $tag + ']=' + (Test-PathIsAtOrUnder $sibling $form)
                    'parentUnder[' + $tag + ']=' + (Test-PathIsAtOrUnder $form (Split-Path -Parent $long))
                    'rootUnderChild[' + $tag + ']=' + (Test-PathIsAtOrUnder $form "$long\child")
                }
                # A path that cannot be worked out is not the same as anything and is not under anything.
                'sameEmpty=' + (Test-DarlingSamePath $long '')
                'underEmpty=' + (Test-PathIsAtOrUnder $long '')
            }
            finally {
                Remove-Item -LiteralPath $parent -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        var tags = new List<string> { "long", "longSlash", "upper", "lower" };
        var hasShortNames = !answers.Contains("noShortNames=True");
        if (hasShortNames)
        {
            tags.AddRange(new[] { "short", "shortSlash", "shortUpper" });
            var longForm = answers.Single(a => a.StartsWith("longForm=", StringComparison.Ordinal))["longForm=".Length..];
            var shortForm = answers.Single(a => a.StartsWith("shortForm=", StringComparison.Ordinal))["shortForm=".Length..];
            Assert.Contains("~", shortForm, StringComparison.Ordinal);
            Assert.NotEqual(longForm, shortForm, StringComparer.OrdinalIgnoreCase);
        }

        foreach (var tag in tags)
        {
            foreach (var other in tags)
            {
                Assert.Contains($"same[{tag}/{other}]=True", answers);
                Assert.Contains($"atOrUnderSelf[{tag}/{other}]=True", answers);
            }
            Assert.Contains($"sameSibling[{tag}]=False", answers);
            Assert.Contains($"sameChild[{tag}]=False", answers);
            Assert.Contains($"childUnder[{tag}]=True", answers);
            Assert.Contains($"childOfFormUnder[{tag}]=True", answers);
            Assert.Contains($"siblingUnder[{tag}]=False", answers);
            Assert.Contains($"parentUnder[{tag}]=True", answers);
            Assert.Contains($"rootUnderChild[{tag}]=False", answers);
        }
        Assert.Contains("sameEmpty=False", answers);
        Assert.Contains("underEmpty=False", answers);

        if (!hasShortNames)
        {
            Assert.Skip("8.3 short names are not created on the volume the temp folder is on, so there is no short spelling to test; the trailing backslash and case forms above passed.");
        }
    }

    /// <summary>
    /// #5673: a path value in darling.json that names a network share or a device (it starts with two slashes or two
    /// backslashes) is not looked up and counts as Incomplete, so the refusal advice never waits on a remote host. A
    /// value inside the folder is still listed. Both scripts, both shells.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheConfiguredPaths_DoNotLookUpAValueThatNamesANetworkShare_AndMarkTheListIncomplete(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Get-DarlingCanonicalPath"));
        probe.AppendLine(ExtractFunction(script, "Test-PathIsAtOrUnder"));
        probe.AppendLine(ExtractFunction(script, "Get-DarlingConfiguredPathsUnder"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $old = Join-Path $env:TEMP ('pm5673-unc-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $old -Force | Out-Null
            $json = $old -replace '\\', '\\'
            try {
                Set-Content -LiteralPath (Join-Path $old 'darling.json') -Value (@(
                    '{',
                    '  "tls": { "pfxPath": "\\\\192.0.2.1\\share\\dir~1\\x.pfx",',
                    '            "certPath": "@@\\good\\x.pem" },',
                    '  "smtp": { "password": "file:\\\\192.0.2.1\\share\\dir~1\\p.txt" },',
                    '  "other": { "token": "file://192.0.2.1/share/dir~1/t.txt" }',
                    '}') -join "`n").Replace('@@', $json)
                $watch = [Diagnostics.Stopwatch]::StartNew()
                $r = Get-DarlingConfiguredPathsUnder $old
                $watch.Stop()
                'lines=' + @($r.Lines).Count
                'incomplete=' + $r.Incomplete
                'quick=' + ($watch.ElapsedMilliseconds -lt 5000)
                'good=' + (@($r.Lines | Where-Object { $_ -like '*good\x.pem*' }).Count)
            }
            finally {
                Remove-Item -LiteralPath $old -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        Assert.Contains("lines=1", answers);
        Assert.Contains("good=1", answers);
        Assert.Contains("incomplete=True", answers);
        Assert.Contains("quick=True", answers);
    }

    /// <summary>
    /// #5673: the upgrade puts -InstallRoot and -Source in their ordinary spelling (the extended-length
    /// <c>\\?\C:\dir</c> and device <c>\\.\C:\dir</c> forms lose their prefix) on the lines that resolve them, so the
    /// running-from-the-install-folder refusal and the source-is-the-install-folder refusal see the same folder
    /// however the operator typed it. This runs the two resolving lines as shipped and then asks
    /// <c>Test-DarlingSamePath</c> the way those refusals do.
    /// </summary>
    [Theory]
    [InlineData("powershell.exe")]
    [InlineData("pwsh")]
    public void TheUpgrade_ReadsAnExtendedLengthOrDeviceSpellingOfTheInstallRootAndSourceAsTheSameFolder(string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        var probe = new StringBuilder();
        foreach (var name in new[] { "Get-DarlingCanonicalPath", "Test-DarlingSamePath", "Convert-ExtendedLengthPath", "Get-DarlingOrdinaryPath" })
        {
            probe.AppendLine(ExtractFunction(script, name));
        }
        var installLine = ExtractLine(script, "try { $InstallRoot = [IO.Path]::GetFullPath(").Trim();
        var sourceLine = ExtractLine(script, "try { $Source = [IO.Path]::GetFullPath(").Trim();
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            function Fail([string]$message) { throw $message }
            $dir = Join-Path $env:TEMP ('pm5673-ext-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            [void][IO.Directory]::CreateDirectory($dir)
            $dir = (Get-Item -LiteralPath $dir -Force).FullName
            try {
                $spellings = [ordered]@{
                    plain    = $dir
                    extended = '\\?\' + $dir
                    device   = '\\.\' + $dir
                    slash    = '\\?\' + $dir + '\'
                    lower    = '\\?\' + $dir.ToLowerInvariant()
                }
                foreach ($tag in $spellings.Keys) {
                    $InstallRoot = $spellings[$tag]
                    @@INSTALL@@
                    catch { 'resolveFailed' }
                    'installRoot[' + $tag + ']=' + (Test-DarlingSamePath $dir $InstallRoot)
                    $Source = $spellings[$tag]
                    @@SOURCE@@
                    catch { 'resolveFailed' }
                    'source[' + $tag + ']=' + (Test-DarlingSamePath $dir $Source)
                }
                # A different folder still reads as different.
                $InstallRoot = '\\?\' + $dir + '-other'
                @@INSTALL@@
                catch { 'resolveFailed' }
                'other=' + (Test-DarlingSamePath $dir $InstallRoot)
            }
            finally {
                Remove-Item -LiteralPath $dir -Recurse -Force -ErrorAction SilentlyContinue
            }
            """.Replace("@@INSTALL@@", installLine, StringComparison.Ordinal).Replace("@@SOURCE@@", sourceLine, StringComparison.Ordinal));

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        Assert.DoesNotContain("resolveFailed", answers);
        foreach (var tag in new[] { "plain", "extended", "device", "slash", "lower" })
        {
            Assert.Contains($"installRoot[{tag}]=True", answers);
            Assert.Contains($"source[{tag}]=True", answers);
        }
        Assert.Contains("other=False", answers);
    }

    /// <summary>
    /// #5673: when <c>Get-DarlingCanonicalPath</c> cannot work out either path (the drive is not mounted, so no part of
    /// the path can be looked up), both helpers fall back to the <c>GetFullPath</c> spelling on both sides and answer as
    /// they did before the canonical form existed: the same text in another case, or with a trailing backslash, is the
    /// same folder; a child is under its parent; a sibling and a different folder are not.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1", "powershell.exe")]
    [InlineData("upgrade-darling.ps1", "powershell.exe")]
    [InlineData("install-darling.ps1", "pwsh")]
    [InlineData("upgrade-darling.ps1", "pwsh")]
    public void TheSameFolderTestAndTheAtOrUnderTest_KeepTheirAnswerWhenNeitherPathCanBeWorkedOut(string scriptName, string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Get-DarlingCanonicalPath"));
        probe.AppendLine(ExtractFunction(script, "Test-DarlingSamePath"));
        probe.AppendLine(ExtractFunction(script, "Test-PathIsAtOrUnder"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $mounted = [IO.DriveInfo]::GetDrives() | ForEach-Object { $_.Name.Substring(0, 1).ToUpperInvariant() }
            $letter = [char[]]'QRSTUVWXYZ' | Where-Object { $mounted -notcontains [string]$_ } | Select-Object -First 1
            'nullBothSides=' + ($null -eq (Get-DarlingCanonicalPath "${letter}:\ABCDEF~1\x"))
            'same=' + (Test-DarlingSamePath "${letter}:\ABCDEF~1\x" ("${letter}:\abcdef~1\x".ToLowerInvariant() + '\'))
            'sameOther=' + (Test-DarlingSamePath "${letter}:\ABCDEF~1\x" "${letter}:\ABCDEF~1\y")
            'under=' + (Test-PathIsAtOrUnder "${letter}:\ABCDEF~1\x\y" "${letter}:\ABCDEF~1\x")
            'underSelf=' + (Test-PathIsAtOrUnder "${letter}:\ABCDEF~1\x" "${letter}:\ABCDEF~1\x\")
            'underSibling=' + (Test-PathIsAtOrUnder "${letter}:\ABCDEF~1\xy" "${letter}:\ABCDEF~1\x")
            'underParent=' + (Test-PathIsAtOrUnder "${letter}:\ABCDEF~1" "${letter}:\ABCDEF~1\x")
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        Assert.Contains("nullBothSides=True", answers);
        Assert.Contains("same=True", answers);
        Assert.Contains("sameOther=False", answers);
        Assert.Contains("under=True", answers);
        Assert.Contains("underSelf=True", answers);
        Assert.Contains("underSibling=False", answers);
        Assert.Contains("underParent=False", answers);
    }

    /// <summary>
    /// #5673: the upgrade's self-overwrite and source-is-install refusals also ask whether the two folders are the same
    /// folder on disk (volume serial number and file index), so a junction to the install folder reads as the install
    /// folder. A folder that does not exist, or another folder, is not the same.
    /// </summary>
    [Theory]
    [InlineData("powershell.exe")]
    [InlineData("pwsh")]
    public void TheUpgrade_ReadsAJunctionToTheInstallFolderAsTheInstallFolder(string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        var probe = new StringBuilder();
        Assert.Contains("if (Test-DarlingSameFolderIdentity $runningFrom $InstallRoot) {", script, StringComparison.Ordinal);
        Assert.Contains("if (-not $sourceIsZip -and (Test-DarlingSameFolderIdentity $sourceRoot $InstallRoot)) {", script, StringComparison.Ordinal);
        probe.AppendLine(ExtractFunction(script, "Test-DarlingSameFolderIdentity"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $base = Join-Path $env:TEMP ('pm5673-id-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            [void][IO.Directory]::CreateDirectory("$base\real")
            [void][IO.Directory]::CreateDirectory("$base\other")
            try {
                [void](New-Item -ItemType Junction -Path "$base\link" -Target "$base\real")
                'junctionMade=' + (Test-Path -LiteralPath "$base\link")
                'selfSame=' + (Test-DarlingSameFolderIdentity "$base\real" "$base\real")
                'junctionSame=' + (Test-DarlingSameFolderIdentity "$base\real" "$base\link")
                'otherSame=' + (Test-DarlingSameFolderIdentity "$base\real" "$base\other")
                'missingSame=' + (Test-DarlingSameFolderIdentity "$base\real" "$base\missing")
                'emptySame=' + (Test-DarlingSameFolderIdentity "$base\real" '')
            }
            finally {
                if (Test-Path -LiteralPath "$base\link") { [IO.Directory]::Delete("$base\link") }
                Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        Assert.Contains("junctionMade=True", answers);
        Assert.Contains("selfSame=True", answers);
        Assert.Contains("junctionSame=True", answers);
        Assert.Contains("otherSame=False", answers);
        Assert.Contains("missingSame=False", answers);
        Assert.Contains("emptySame=False", answers);
    }

    /// <summary>
    /// #5673: the two refusals themselves, run the way the script has them. A folder reached through a junction to the install
    /// folder has a different spelling, so the text comparison alone lets it through; the volume-and-file-index check after
    /// each refusal is what stops it. The block is cut out of the script (from the self-overwrite refusal up to the zip
    /// handling) and run with a stand-in <c>Fail</c> that records which message was raised.
    /// </summary>
    [Theory]
    [InlineData("powershell.exe")]
    [InlineData("pwsh")]
    public void TheUpgrade_RefusesARunningScriptAndASourceThatReachTheInstallFolderThroughAJunction(string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        var start = script.IndexOf("if (Test-DarlingSamePath $runningFrom $InstallRoot) {", StringComparison.Ordinal);
        Assert.True(start >= 0, "upgrade-darling.ps1 no longer has the self-overwrite refusal (#5673)");
        var end = script.IndexOf("\nif ($sourceIsZip) {", start, StringComparison.Ordinal);
        Assert.True(end > start, "upgrade-darling.ps1 no longer has the zip handling after the refusals (#5673)");
        var refusals = script.Substring(start, end - start);

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Get-DarlingCanonicalPath"));
        probe.AppendLine(ExtractFunction(script, "Test-DarlingSamePath"));
        probe.AppendLine(ExtractFunction(script, "Test-DarlingSameFolderIdentity"));
        probe.AppendLine("function Fail([string]$message) { 'refused: ' + $message }");
        probe.AppendLine("$refusals = {");
        probe.AppendLine(refusals);
        probe.AppendLine("}");
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $base = Join-Path $env:TEMP ('pm5673-ref-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            [void][IO.Directory]::CreateDirectory("$base\real")
            [void][IO.Directory]::CreateDirectory("$base\other")
            try {
                [void](New-Item -ItemType Junction -Path "$base\link" -Target "$base\real")
                $InstallRoot = "$base\real"
                $sourceIsZip = $false

                $runningFrom = "$base\link"; $sourceRoot = "$base\other"
                'running=' + (@(& $refusals | Where-Object { $_ -like '*script is running from the install directory*' }).Count)

                $runningFrom = "$base\other"; $sourceRoot = "$base\link"
                'source=' + (@(& $refusals | Where-Object { $_ -like '*source folder is the install directory*' }).Count)

                $runningFrom = "$base\other"; $sourceRoot = "$base\other"
                'neither=' + (@(& $refusals).Count)
            }
            finally {
                if (Test-Path -LiteralPath "$base\link") { [IO.Directory]::Delete("$base\link") }
                Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        Assert.Contains("running=1", answers);
        Assert.Contains("source=1", answers);
        Assert.Contains("neither=0", answers);
    }

    /// <summary>
    /// #5673: the installer's user-profile check also runs on the install folder's final path, so a junction that leads into
    /// a profile folder reads as under the profile, a folder that is only named like one does not, and a folder that does not
    /// exist yet is resolved through the part of its path that does. The check is cut out of the script, after the typed-path
    /// check, and run with a profile root of its own.
    /// </summary>
    [Theory]
    [InlineData("powershell.exe")]
    [InlineData("pwsh")]
    public void TheInstaller_ReadsAJunctionIntoAProfileFolderAsUnderTheProfile(string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "install-darling.ps1"));
        var start = script.IndexOf("$resolvedRoot = Get-DarlingResolvedFolder $classifyRoot", StringComparison.Ordinal);
        Assert.True(start >= 0, "install-darling.ps1 no longer resolves the install folder before the profile check (#5673)");
        var end = script.IndexOf("\n}", start, StringComparison.Ordinal) + 2;
        var profileCheck = script.Substring(start, end - start);

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Convert-ExtendedLengthPath"));
        probe.AppendLine(ExtractFunction(script, "Get-DarlingCanonicalPath"));
        probe.AppendLine(ExtractFunction(script, "Test-PathIsAtOrUnder"));
        probe.AppendLine(ExtractFunction(script, "Get-DarlingResolvedFolder"));
        probe.AppendLine("function Get-ProfilesDirectory { $script:profiles }");
        probe.AppendLine("function Test-Under([string]$typed) { $classifyRoot = $typed; $underProfile = $false; " + "\n" + profileCheck + "\n" + " $underProfile }");
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $base = Join-Path $env:TEMP ('pm5673-prof-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            [void][IO.Directory]::CreateDirectory("$base\profiles\me\data")
            [void][IO.Directory]::CreateDirectory("$base\elsewhere\data")
            try {
                $script:profiles = "$base\profiles"
                $env:USERPROFILE = "$base\profiles\me"
                [void](New-Item -ItemType Junction -Path "$base\link" -Target "$base\profiles\me\data")
                [void](New-Item -ItemType Junction -Path "$base\plain" -Target "$base\elsewhere\data")
                'resolved=' + (Get-DarlingResolvedFolder "$base\link").EndsWith('\profiles\me\data')
                'resolvedMissing=' + (Get-DarlingResolvedFolder "$base\link\not\there").EndsWith('\profiles\me\data\not\there')
                'junction=' + (Test-Under "$base\link")
                'junctionNew=' + (Test-Under "$base\link\Darling")
                'typedProfile=' + (Test-Under "$base\profiles\me\x")
                'plainJunction=' + (Test-Under "$base\plain")
                'plainNew=' + (Test-Under "$base\plain\Darling")
                'elsewhere=' + (Test-Under "$base\elsewhere\x")
            }
            finally {
                foreach ($name in 'link', 'plain') { if (Test-Path -LiteralPath "$base\$name") { [IO.Directory]::Delete("$base\$name") } }
                Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        Assert.Contains("resolved=True", answers);
        Assert.Contains("resolvedMissing=True", answers);
        Assert.Contains("junction=True", answers);
        Assert.Contains("junctionNew=True", answers);
        Assert.Contains("typedProfile=True", answers);
        Assert.Contains("plainJunction=False", answers);
        Assert.Contains("plainNew=False", answers);
        Assert.Contains("elsewhere=False", answers);
    }

    /// <summary>
    /// #5673: the check that the service runs from the folder being upgraded compares the folders on disk as well as their
    /// spelling, so an install folder typed as a junction to the registered folder is the registered folder, while another
    /// folder is still refused. The check is cut out of the script and run with a stand-in <c>Fail</c>.
    /// </summary>
    [Theory]
    [InlineData("powershell.exe")]
    [InlineData("pwsh")]
    public void TheUpgrade_ReadsAJunctionToTheRegisteredFolderAsTheRegisteredFolder(string shell)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        var start = script.IndexOf("elseif (-not (Test-DarlingSamePath $registeredRoot $InstallRoot)", StringComparison.Ordinal);
        Assert.True(start >= 0, "upgrade-darling.ps1 no longer has the registered-folder refusal (#5673)");
        var end = script.IndexOf("\n}", start, StringComparison.Ordinal) + 2;
        var check = "if (" + script.Substring(start + "elseif (".Length, end - start - "elseif (".Length);

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(script, "Get-DarlingCanonicalPath"));
        probe.AppendLine(ExtractFunction(script, "Test-DarlingSamePath"));
        probe.AppendLine(ExtractFunction(script, "Test-DarlingSameFolderIdentity"));
        probe.AppendLine("function Fail([string]$message) { 'refused' }");
        probe.AppendLine("$serviceName = 'DarlingTest'");
        probe.AppendLine("function Check { " + check.Replace("\r", "") + " }");
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $base = Join-Path $env:TEMP ('pm5673-reg-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            [void][IO.Directory]::CreateDirectory("$base\real")
            [void][IO.Directory]::CreateDirectory("$base\other")
            try {
                [void](New-Item -ItemType Junction -Path "$base\link" -Target "$base\real")
                $registeredRoot = "$base\real"
                $InstallRoot = "$base\link"
                'junction=' + (@(Check).Count)
                $InstallRoot = "$base\real"
                'same=' + (@(Check).Count)
                $InstallRoot = "$base\other"
                'other=' + (@(Check).Count)
            }
            finally {
                if (Test-Path -LiteralPath "$base\link") { [IO.Directory]::Delete("$base\link") }
                Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        List<string>? answers;
        if (shell == "pwsh")
        {
            answers = RunPowerShell7(probe.ToString());
            if (answers is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }
        }
        else
        {
            answers = RunWindowsPowerShell(probe.ToString());
        }

        Assert.Contains("junction=0", answers);
        Assert.Contains("same=0", answers);
        Assert.Contains("other=1", answers);
    }

    /// <summary>
    /// #5673: the canonical spelling trims and joins with the runtime's directory separator and not a literal backslash, so a
    /// backslash that is part of a name on another platform (<c>/opt/a\</c>) is not dropped as if it were a separator. The
    /// separator is the same character on Windows, so nothing changes there; this pins the text because the rule cannot be
    /// run with another separator from a Windows run.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheCanonicalPath_TrimsAndJoinsWithTheRuntimesSeparator(string scriptName)
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", scriptName));
        var body = ExtractFunction(script, "Get-DarlingCanonicalPath");

        Assert.DoesNotContain("TrimEnd('\\')", body, StringComparison.Ordinal);
        Assert.DoesNotContain("-join '\\'", body, StringComparison.Ordinal);
        Assert.Contains("$full.TrimEnd([IO.Path]::DirectorySeparatorChar)", body, StringComparison.Ordinal);
        Assert.Contains("-join [IO.Path]::DirectorySeparatorChar", body, StringComparison.Ordinal);
    }

    /// <summary>The two path helpers the walk, the lock and the path test call (#5649), the way the scripts define them.</summary>
    private static void AppendPathHelpers(StringBuilder probe, string script)
    {
        probe.AppendLine(ExtractFunction(script, "Get-DarlingCanonicalPath"));
        probe.AppendLine(ExtractFunction(script, "Get-DarlingSegmentsBelowRoot"));
    }

    /// <summary>Adds the shared service-written list and its path test to a probe, the way the scripts define them
    /// (the lock and the pre-lock walk both read them).</summary>
    private static void AppendServiceWrittenList(StringBuilder probe, string script)
    {
        foreach (var name in new[] { "Get-DarlingServiceWrittenNames", "Test-DarlingServiceWrittenFileName", "Test-PathIsAtOrUnder", "Get-DarlingFileNameCount", "Initialize-DarlingNativeOwner", "Test-DarlingServiceWrittenPath", "Get-DarlingServiceWrittenRootFiles", "Get-DarlingCanonicalPath", "Get-DarlingSegmentsBelowRoot" })
        {
            probe.AppendLine(ExtractFunction(script, name));
        }
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
            var startInfo = WindowsPowerShellStartInfo.ForScript(path);
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

    /// <summary>The same as <see cref="RunWindowsPowerShell"/> under PowerShell 7 (<c>pwsh</c>); null when it is not installed.</summary>
    private static List<string>? RunPowerShell7(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-5649-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script);
        try
        {
            var startInfo = new ProcessStartInfo("pwsh", $"-NoProfile -File \"{path}\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            Process? process;
            try { process = Process.Start(startInfo); }
            catch (System.ComponentModel.Win32Exception) { return null; }
            Assert.NotNull(process);

            var stdout = process!.StandardOutput.ReadToEnd();
            var stderr = process.StandardError.ReadToEnd();
            process.WaitForExit(60_000);

            Assert.True(string.IsNullOrWhiteSpace(stderr), $"pwsh reported an error running the extracted function:\n{stderr}");

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
