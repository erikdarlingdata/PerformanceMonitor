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
           ProfilesDirectory is still a profile, and it is the one whose owner is most likely running this. */
        Assert.Contains("Test-PathIsAtOrUnder $root $env:USERPROFILE", script, StringComparison.Ordinal);
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
        };

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Get-NetworkPathKind"));
        foreach (var (path, _) in cases)
        {
            probe.AppendLine($"$k = Get-NetworkPathKind '{path}'; if ($null -eq $k) {{ '<none>' }} else {{ $k }}");
        }

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(cases.Length, answers.Count);

        for (var i = 0; i < cases.Length; i++)
        {
            Assert.Equal(cases[i].Expected, answers[i]);
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

        probe.AppendLine(ExtractFunction(script, "Test-PathIsAtOrUnder"));
        probe.AppendLine(ExtractFunction(script, "Get-NetworkPathKind"));

        /* The composition, quoted out of the shipped script. */
        var networkLine = ExtractLine(script, "$networkKind = Get-NetworkPathKind $root");
        var profileLine = ExtractLine(script, "$underProfile = (Test-PathIsAtOrUnder $root (Get-ProfilesDirectory))");

        foreach (var (directory, _, _) in cases)
        {
            probe.AppendLine($"$root = '{directory}'");
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
            using var process = Process.Start(new ProcessStartInfo("powershell.exe", $"-NoProfile -ExecutionPolicy Bypass -File \"{path}\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            });
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

    private static string ReadRepoFile(string relativePath)
    {
        var root = FindRepoRoot();
        Assert.NotNull(root);
        var path = Path.Combine(root!, relativePath);
        Assert.True(File.Exists(path), $"expected {path} to exist");
        return File.ReadAllText(path);
    }

    /// <summary>Walks up from the test output directory to the repo root (the directory holding
    /// <c>PerformanceMonitor.sln</c>) — the same idiom <c>DarlingFileSecurityTests</c> uses.</summary>
    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
