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
/// #5700: the install and upgrade scripts leave the TLS private-key files that darling.json names to the service exe's
/// own verb (<c>--harden-files --tls-key-files-only</c>), which is the only code that reads those paths and sets their
/// permissions. The scripts call it after the last tree lock and before the service starts, echo its lines and warn
/// (never fail) when it could not run. The upgrade script also creates each rollback backup folder with a protected
/// SYSTEM and Administrators DACL before the copy, and applies the same DACL to every retained backup. The functions
/// are lifted out of each script and run under Windows PowerShell 5.1 in a scratch folder, and the copies in the two
/// scripts are compared byte for byte.
/// </summary>
public sealed class DarlingTlsKeyFileScriptTests
{
    private static readonly string[] s_sharedFunctions =
    {
        "Get-DarlingConfiguredPathsUnder", "Get-DarlingTlsKeyFileVerbCommand", "Invoke-DarlingTlsKeyFileHardening",
    };

    private static string Script(string scriptName) =>
        ReadRepoFile("Darling", "tools", scriptName).Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The functions ship in both scripts, which cannot import each other, so a fix to one that misses the
    /// other would leave half the installs behind.</summary>
    [Fact]
    public void TheSharedFunctions_ShipIdenticallyInTheInstallAndUpgradeScripts()
    {
        var install = Script("install-darling.ps1");
        var upgrade = Script("upgrade-darling.ps1");

        foreach (var name in s_sharedFunctions)
        {
            Assert.Equal(ExtractFunction(install, name, "install-darling.ps1"), ExtractFunction(upgrade, name, "upgrade-darling.ps1"));
        }
    }

    /// <summary>The scripts hold no key-file parse and no key-file ACL code of their own: the exe's verb is the one
    /// implementation.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheScripts_HoldNoTlsKeyParseOrAclCodeOfTheirOwn(string scriptName)
    {
        var script = Script(scriptName);

        foreach (var gone in new[] { "Get-DarlingTlsKeyFiles", "Protect-DarlingTlsKeyFiles", "Get-DarlingConfiguredPathEntries" })
        {
            Assert.DoesNotContain(gone, script, StringComparison.Ordinal);
        }

        Assert.Contains("--harden-files --tls-key-files-only --config $configPath", ExtractFunction(script, "Invoke-DarlingTlsKeyFileHardening", scriptName), StringComparison.Ordinal);
    }

    /// <summary>The verb runs after the last tree lock (the lock resets any extra write grant on a file it walks) and before
    /// the service starts, in the upgrade script (the install script's order is not pinned as text: under -MsiPhase the
    /// start is the package's, #6480). A failure to run it is a warning, never a stop, in both.</summary>
    [Fact]
    public void TheScripts_CallTheVerb_AfterTheLastTreeLock_AndBeforeTheServiceStarts_AndNeverFailOverIt()
    {
        var install = Script("install-darling.ps1");
        var upgrade = Script("upgrade-darling.ps1");

        var upgradeLock = upgrade.LastIndexOf("\nInvoke-UpgradeTreeLock 'after the copy'\n", StringComparison.Ordinal);
        var upgradeCall = upgrade.IndexOf("\n$tlsKeyWarning = Get-UpgradeTlsKeyFileWarning (Join-Path $InstallRoot $serviceExeName) $configPath\n", StringComparison.Ordinal);
        var upgradeStart = upgrade.IndexOf("\nStart-Service -Name $serviceName\n", StringComparison.Ordinal);
        Assert.True(upgradeLock > 0 && upgradeCall > upgradeLock && upgradeCall < upgradeStart, $"upgrade order: lock {upgradeLock}, call {upgradeCall}, start {upgradeStart}");
        Assert.Matches(@"Get-UpgradeTlsKeyFileWarning \(Join-Path \$InstallRoot \$serviceExeName\) \$configPath\nif \(\$tlsKeyWarning\) \{ Warn \$tlsKeyWarning \}\n", upgrade);

        var body = ExtractFunction(install, "Invoke-DarlingTlsKeyFileHardening", "install-darling.ps1");
        Assert.DoesNotContain("Fail ", body, StringComparison.Ordinal);
        Assert.DoesNotContain("throw", body, StringComparison.Ordinal);
    }

    /// <summary>The upgrade makes the backup folder protected before the first file is copied into it, and restricts every
    /// retained backup after the prune and before the service starts. The per-key name matching after the copy is gone.</summary>
    [Fact]
    public void TheUpgradeScript_RestrictsTheBackupFolderBeforeTheCopy_AndTheRetainedBackupsAfterThePrune()
    {
        var upgrade = Script("upgrade-darling.ps1");

        var create = upgrade.IndexOf("New-Item -ItemType Directory -Path $backupPath -Force", StringComparison.Ordinal);
        var protect = upgrade.IndexOf("Protect-DarlingRollbackBackupFolder $backupPath\n", StringComparison.Ordinal);
        var copy = upgrade.IndexOf("Copy-Item -Destination $backupPath -Force", StringComparison.Ordinal);
        Assert.True(create > 0 && protect > create && copy > protect, $"backup order: create {create}, protect {protect}, copy {copy}");

        var prune = upgrade.IndexOf("Remove-DarlingRollbackBackups $prunable", StringComparison.Ordinal);
        var retained = upgrade.IndexOf("\nProtect-DarlingKeptRollbackBackups $InstallRoot\n", StringComparison.Ordinal);
        var start = upgrade.IndexOf("\nStart-Service -Name $serviceName\n", StringComparison.Ordinal);
        Assert.True(prune > 0 && retained > prune && retained < start, $"retained order: prune {prune}, retained {retained}, start {start}");
        Assert.Contains("Protect-DarlingRollbackBackupFolder $retained.FullName -ResetChildren", upgrade, StringComparison.Ordinal);

        Assert.DoesNotContain("$tlsKeyNames", upgrade, StringComparison.Ordinal);
    }

    /// <summary>The config path the scripts pass is the darling.json of the install folder, the file the service itself reads.</summary>
    [Theory]
    [InlineData("install-darling.ps1", "$configPath = Join-Path $root 'darling.json'\n")]
    [InlineData("upgrade-darling.ps1", "$configName = 'darling.json'\n")]
    public void TheScripts_PassTheInstallFoldersDarlingJson_ToTheVerb(string scriptName, string definition)
    {
        var script = Script(scriptName);

        Assert.Contains(definition, script, StringComparison.Ordinal);
        if (scriptName == "upgrade-darling.ps1")
        {
            Assert.Contains("\n$configPath = Join-Path $InstallRoot $configName\n", script, StringComparison.Ordinal);
        }

        Assert.Matches(@"Get-DarlingTlsKeyFileVerbCommand \$exe \$configPath", script);
    }

    /// <summary>The call, with a stub in place of the exe: the arguments reach it with the config path the caller names, the
    /// stub's lines are echoed, a nonzero exit and an exe that is not there each give a warning that names the verb and the same
    /// path, and a zero exit gives none. A DARLING_CONFIG in the calling process changes nothing about the path the stub gets.
    /// Nothing throws.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheVerbCall_PassesTheConfigPath_WarnsWithTheCommandOnANonzeroExit_AndNeverThrows(string scriptName)
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("Windows PowerShell and .cmd stubs are Windows'.");
            return;
        }

        var probe = new StringBuilder();
        AppendVerbFunctions(probe, scriptName);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $dir = Join-Path $env:TEMP ('pm5700-stub-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
            $env:DARLING_CONFIG = Join-Path $dir 'exported-viewer-config.json'
            try {
                $cfg = Join-Path $dir 'install root\darling.json'
                $bad = Join-Path $dir 'bad.cmd'
                Set-Content -LiteralPath $bad -Value @('@echo off', 'echo stub-line args=%*', 'exit /b 3') -Encoding ASCII
                $good = Join-Path $dir 'good.cmd'
                Set-Content -LiteralPath $good -Value @('@echo off', 'echo stub-line args=%*', 'exit /b 0') -Encoding ASCII
                'cfg=' + $cfg
                'env=' + $env:DARLING_CONFIG
                $w = Invoke-DarlingTlsKeyFileHardening $bad $cfg
                'nonzero=' + $w
                $w = Invoke-DarlingTlsKeyFileHardening $good $cfg
                'zero=' + $(if ($null -eq $w) { 'null' } else { $w })
                $w = Invoke-DarlingTlsKeyFileHardening (Join-Path $dir 'missing.exe') $cfg
                'missing=' + $(if ($null -eq $w) { 'null' } else { $w })
            }
            finally { Remove-Item -LiteralPath $dir -Recurse -Force -ErrorAction SilentlyContinue }
            """);

        var lines = RunWindowsPowerShell(probe.ToString());
        var all = string.Join("\n", lines);
        var cfg = lines.Single(l => l.StartsWith("cfg=", StringComparison.Ordinal))["cfg=".Length..];
        var envConfig = lines.Single(l => l.StartsWith("env=", StringComparison.Ordinal))["env=".Length..];
        Assert.NotEqual(cfg, envConfig);

        var stubLines = lines.Where(l => l.StartsWith("stub-line args=", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, stubLines.Count);
        foreach (var stub in stubLines)
        {
            Assert.Equal($"stub-line args=--harden-files --tls-key-files-only --config \"{cfg}\"", stub);
            Assert.DoesNotContain(envConfig, stub, StringComparison.Ordinal);
        }

        Assert.Matches(@"nonzero=The service could not check the TLS key files darling\.json names \(exit code 3\).*--harden-files --tls-key-files-only --config ""[^""]*install root\\darling\.json""", all);
        Assert.Contains("zero=null", all, StringComparison.Ordinal);
        Assert.Matches(@"missing=The service could not check the TLS key files darling\.json names \(.+\).*--harden-files --tls-key-files-only --config ""[^""]*install root\\darling\.json""", all);
    }

    /// <summary>The status words the key file part of the verb can print, as the verb's source spells them: the words after two
    /// spaces in the text from <c>HardenTlsKeyFiles</c> to <c>HardenTargets</c>, which holds the per-file lines, the unsecured
    /// check and the notes. Read from the code, so a status the verb gains is found here and not remembered.</summary>
    private static List<string> VerbStatusWordsInSource()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCliCommands.cs");
        var start = source.IndexOf("internal static TlsKeyFileResult HardenTlsKeyFiles(", StringComparison.Ordinal);
        var end = source.IndexOf("private static List<HardenTarget> HardenTargets(", StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, $"the verb's key file section was not found (start {start}, end {end}).");

        return Regex.Matches(source[start..end], @"""  ([A-Z]{2,}(?: [A-Z]{2,})*)(?= |""|\))")
            .Select(m => m.Groups[1].Value)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(w => w, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>The status words the stub in <see cref="TheVerbCall_WritesEveryStatusLineButSecuredAsAWarning_EvenOnExitCodeZero"/>
    /// prints. <see cref="TheStatusWordList_MatchesTheWordsTheVerbCanPrint"/> holds it to the verb's source.</summary>
    private static readonly string[] s_statusWords =
    [
        "SECURED", "REFUSED", "SKIPPED", "FAILED", "MISSING", "NOT CHANGED", "STILL READABLE", "LOCKED OUT", "OTHER ACCESS", "UNTRUSTED OWNER",
    ];

    /// <summary>The list the status test prints from is the list of words the verb's source holds, no more and no fewer.</summary>
    [Fact]
    public void TheStatusWordList_MatchesTheWordsTheVerbCanPrint()
    {
        var inSource = VerbStatusWordsInSource();

        Assert.Equal(s_statusWords.OrderBy(w => w, StringComparer.Ordinal), inSource);
    }

    /// <summary>Every status line of the verb's output other than SECURED is written as a warning, even when the exit code is 0:
    /// the stub prints one line for each status word the verb can print. The lines that are not status lines (the config file
    /// line, the DARLING_CONFIG note and the no-key-files line) and SECURED stay plain output.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheVerbCall_WritesEveryStatusLineButSecuredAsAWarning_EvenOnExitCodeZero(string scriptName)
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("Windows PowerShell and .cmd stubs are Windows'.");
            return;
        }

        var stubLines = new StringBuilder();
        for (var i = 0; i < s_statusWords.Length; i++)
        {
            stubLines.AppendLine($"                    'echo.  {s_statusWords[i]}  C:\\root\\k{i}.pfx (web): detail',");
        }

        var probe = new StringBuilder();
        AppendVerbFunctions(probe, scriptName);
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $dir = Join-Path $env:TEMP ('pm5700-lines-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
            try {
                $stub = Join-Path $dir 'lines.cmd'
                Set-Content -LiteralPath $stub -Encoding ASCII -Value @(
                    '@echo off',
                    'echo.Config file: C:\root\darling.json',
                    'echo.  (taken from the DARLING_CONFIG environment variable)',
            #STATUSLINES#
                    'echo.No TLS key files are named in darling.json.',
                    'exit /b 0')
                & { Invoke-DarlingTlsKeyFileHardening $stub (Join-Path $dir 'darling.json') } 3>&1 6>&1 | ForEach-Object {
                    if ($_ -is [System.Management.Automation.WarningRecord]) { 'W|' + $_.Message }
                    elseif ($_ -is [System.Management.Automation.InformationRecord]) { 'H|' + $_.MessageData }
                    elseif ($null -ne $_) { 'R|' + $_ }
                }
            }
            finally { Remove-Item -LiteralPath $dir -Recurse -Force -ErrorAction SilentlyContinue }
            """.Replace("#STATUSLINES#", stubLines.ToString().TrimEnd('\r', '\n')));

        var lines = RunWindowsPowerShell(probe.ToString());
        var all = string.Join("\n", lines);

        for (var i = 0; i < s_statusWords.Length; i++)
        {
            var expected = $"  {s_statusWords[i]}  C:\\root\\k{i}.pfx (web): detail";
            if (s_statusWords[i] == "SECURED")
            {
                Assert.True(lines.Contains("H|" + expected, StringComparer.Ordinal), $"SECURED was not plain output:\n{all}");
            }
            else
            {
                Assert.True(lines.Contains("W|" + expected, StringComparer.Ordinal), $"no warning for '{s_statusWords[i]}':\n{all}");
            }
        }

        Assert.Equal(s_statusWords.Length - 1, lines.Count(l => l.StartsWith("W|", StringComparison.Ordinal)));
        Assert.Contains("H|Config file: C:\\root\\darling.json", lines);
        Assert.Contains("H|  (taken from the DARLING_CONFIG environment variable)", lines);
        Assert.Contains("H|No TLS key files are named in darling.json.", lines);
        Assert.DoesNotContain(lines, l => l.StartsWith("R|", StringComparison.Ordinal));
    }

    /// <summary>The upgrade runs the verb only over a tree it has locked. When a lock reported open paths, when the lock was skipped
    /// because the service's account could not be read, and under -AcceptWritableExtraction, the stub is not called and the result is
    /// a warning that names the verb and the command to run once the tree is locked; after a clean lock the stub is called. The
    /// real Invoke-UpgradeTreeLock runs with its own helpers replaced by stand-ins, so the state the step reads is the one the lock
    /// sets.</summary>
    [Fact]
    public void TheUpgrade_DoesNotRunTheVerb_OverATreeThatIsOpenOrNotLocked_OrUnderAcceptWritableExtraction()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("Windows PowerShell and .cmd stubs are Windows'.");
            return;
        }

        var script = Script("upgrade-darling.ps1");
        var probe = new StringBuilder();
        AppendVerbFunctions(probe, "upgrade-darling.ps1");
        probe.AppendLine(ExtractFunction(script, "Get-UpgradeTlsKeyFileWarning", "upgrade-darling.ps1"));
        probe.AppendLine(ExtractFunction(script, "Invoke-UpgradeTreeLock", "upgrade-darling.ps1"));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            function Warn([string]$m) { }
            function Good([string]$m) { }
            function Get-DarlingExtraServiceWriteDirectories($r, $c) { @() }
            $script:lockOpen = @()
            function Lock-DarlingInstallTree($r, $a, $e) { return $script:lockOpen }
            $serviceName = 'PerformanceMonitor Darling'
            $InstallRoot = 'C:\pm5700-no-such-root'
            $configPath = Join-Path $InstallRoot 'darling.json'
            $dir = Join-Path $env:TEMP ('pm5700-gate-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
            try {
                $calls = Join-Path $dir 'calls.txt'
                $stub = Join-Path $dir 'stub.cmd'
                Set-Content -LiteralPath $stub -Encoding ASCII -Value @('@echo off', 'echo called>>"%~dp0calls.txt"', 'exit /b 0')
                function Run-Case([string]$label, [string]$account, [string[]]$open, [bool]$accept, [int]$locks = 1) {
                    if (Test-Path -LiteralPath $calls) { Remove-Item -LiteralPath $calls }
                    $script:installTreeLockProblem = $null
                    $script:logonAccount = $account
                    $script:lockOpen = $open
                    $script:AcceptWritableExtraction = [System.Management.Automation.SwitchParameter]$accept
                    for ($i = 0; $i -lt $locks; $i++) {
                        Invoke-UpgradeTreeLock 'after the copy'
                        if ($i -eq 0 -and $locks -gt 1) { $script:lockOpen = @() }
                    }
                    $w = Get-UpgradeTlsKeyFileWarning $stub $configPath
                    $label + '.called=' + (Test-Path -LiteralPath $calls)
                    $label + '.warning=' + $(if ($null -eq $w) { 'null' } else { $w })
                }
                Run-Case 'clean' 'NT AUTHORITY\SYSTEM' @() $false
                Run-Case 'open' 'NT AUTHORITY\SYSTEM' @('C:\x\a.dll') $false
                Run-Case 'skipped' '' @() $false
                Run-Case 'accept' 'NT AUTHORITY\SYSTEM' @() $true
                Run-Case 'openThenClean' 'NT AUTHORITY\SYSTEM' @('C:\x\a.dll') $false 2
            }
            finally { Remove-Item -LiteralPath $dir -Recurse -Force -ErrorAction SilentlyContinue }
            """);

        var lines = RunWindowsPowerShell(probe.ToString());
        var all = string.Join("\n", lines);

        Assert.Contains("clean.called=True", lines);
        Assert.Contains("clean.warning=null", lines);
        foreach (var blocked in new[] { "open", "skipped", "accept", "openThenClean" })
        {
            Assert.Contains($"{blocked}.called=False", lines);
            Assert.Matches($@"{blocked}\.warning=The TLS key files darling\.json names were not checked.*--harden-files --tls-key-files-only --config ""C:\\pm5700-no-such-root\\darling\.json""", all);
        }
    }

    private static void AppendVerbFunctions(StringBuilder probe, string scriptName)
    {
        foreach (var name in new[] { "Get-DarlingTlsKeyFileVerbCommand", "Invoke-DarlingTlsKeyFileHardening" })
        {
            probe.AppendLine(ExtractFunction(Script(scriptName), name, scriptName));
        }
    }

    // A scratch folder whose DACL grants Users read, inherited by everything made under it (the install root's shape),
    // plus the helpers the two backup probes share.
    private const string FixturePreamble = """
        $ErrorActionPreference = 'Stop'
        $wk = [System.Security.Principal.WellKnownSidType]
        $users = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinUsersSid, $null)
        $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
        $base = Join-Path $env:TEMP ('pm5700-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
        New-Item -ItemType Directory -Path $base -Force | Out-Null
        $parentAcl = Get-Acl -LiteralPath $base
        $parentAcl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($users, 'ReadAndExecute', 'ContainerInherit, ObjectInherit', 'None', 'Allow')))
        Set-Acl -LiteralPath $base -AclObject $parentAcl
        function Get-AccessSection($f) { (New-Object System.IO.DirectoryInfo($f)).GetAccessControl([System.Security.AccessControl.AccessControlSections]::Access) }
        function Get-AceSids($f) { @((Get-AccessSection $f).Access | ForEach-Object { $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value } | Sort-Object -Unique) }
        function Test-UsersAce($f) { (Get-AceSids $f) -contains $users.Value }
        function Add-MeToFolder($folder) { & icacls.exe $folder /grant ('*' + $me.Value + ':(OI)(CI)F') | Out-Null }
        """;

    /// <summary>
    /// A backup folder made under a parent that grants Users read: a copy made into an ordinary folder inherits that Users
    /// read (the control), and a copy made into a folder that Protect-DarlingRollbackBackupFolder had already restricted
    /// does not. The test account is given write access on the new folder (an administrator has it through Administrators),
    /// so a copy can be made; it adds only that account.
    /// </summary>
    [Fact]
    public void TheBackupFolder_IsProtectedBeforeTheCopy_SoACopiedFileIsNotReadableByUsers()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("DACLs and Windows PowerShell are Windows'.");
            return;
        }

        var script = Script("upgrade-darling.ps1");
        var probe = new StringBuilder();
        foreach (var name in new[] { "Get-DarlingFileNameCount", "Protect-DarlingRollbackBackupFolder" })
        {
            probe.AppendLine(ExtractFunction(script, name, "upgrade-darling.ps1"));
        }

        probe.AppendLine(FixturePreamble);
        probe.AppendLine("""
            try {
                $src = Join-Path $base 'src'
                New-Item -ItemType Directory -Path $src | Out-Null
                Set-Content -LiteralPath (Join-Path $src 'web.pfx') -Value 'placeholder'
                $plain = Join-Path $base 'plain'
                New-Item -ItemType Directory -Path $plain | Out-Null
                Copy-Item -LiteralPath (Join-Path $src 'web.pfx') -Destination $plain
                'controlUsers=' + (Test-UsersAce (Join-Path $plain 'web.pfx'))

                $backup = Join-Path $base '_rollback_new'
                New-Item -ItemType Directory -Path $backup | Out-Null
                $problem = Protect-DarlingRollbackBackupFolder $backup
                'problem=' + $(if ($problem) { $problem } else { 'none' })
                'protected=' + (Get-AccessSection $backup).AreAccessRulesProtected
                Add-MeToFolder $backup
                Copy-Item -LiteralPath (Join-Path $src 'web.pfx') -Destination $backup
                $copy = Join-Path $backup 'web.pfx'
                'copyUsers=' + (Test-UsersAce $copy)
                'copySids=' + ((Get-AceSids $copy) -join ',')
                'me=' + $me.Value
            }
            finally {
                $cleanup = Join-Path $base '_rollback_new'
                if (Test-Path -LiteralPath $cleanup) { Add-MeToFolder $cleanup }
                Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));

        Assert.Contains("controlUsers=True", all, StringComparison.Ordinal);
        Assert.Contains("problem=none", all, StringComparison.Ordinal);
        Assert.Contains("protected=True", all, StringComparison.Ordinal);
        Assert.Contains("copyUsers=False", all, StringComparison.Ordinal);

        var me = Regex.Match(all, @"me=(\S+)").Groups[1].Value;
        var copySids = Regex.Match(all, @"copySids=(\S+)").Groups[1].Value.Split(',');
        Assert.All(copySids, sid => Assert.Contains(sid, new[] { "S-1-5-18", "S-1-5-32-544", me }));
    }

    /// <summary>
    /// Older retained backups, made under a parent that grants Users read: the folder and the files in it end limited to SYSTEM
    /// and Administrators (an explicit entry the file had is cleared too); a backup folder that is a junction is not followed,
    /// so what it points at keeps its permissions.
    /// </summary>
    [Fact]
    public void TheRetainedBackups_AreRestricted_AndALinkedBackupFolderIsNotFollowed()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("DACLs and Windows PowerShell are Windows'.");
            return;
        }

        var script = Script("upgrade-darling.ps1");
        var probe = new StringBuilder();
        foreach (var name in new[] { "Get-DarlingFileNameCount", "Protect-DarlingRollbackBackupFolder" })
        {
            probe.AppendLine(ExtractFunction(script, name, "upgrade-darling.ps1"));
        }

        probe.AppendLine(FixturePreamble);
        probe.AppendLine("""
            $junction = Join-Path $base '_rollback_link'
            try {
                $old = Join-Path $base '_rollback_old'
                New-Item -ItemType Directory -Path $old | Out-Null
                Set-Content -LiteralPath (Join-Path $old 'web.pfx') -Value 'placeholder'
                Set-Content -LiteralPath (Join-Path $old 'darling.json') -Value '{}'
                $explicit = New-Object System.Security.AccessControl.FileSecurity
                $explicit.SetAccessRuleProtection($true, $false)
                foreach ($sid in @($me, (New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)))) {
                    $explicit.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($sid, 'FullControl', 'Allow')))
                }
                Set-Acl -LiteralPath (Join-Path $old 'darling.json') -AclObject $explicit
                'beforePfxUsers=' + (Test-UsersAce (Join-Path $old 'web.pfx'))

                $problem = Protect-DarlingRollbackBackupFolder $old -ResetChildren
                'oldProblem=' + $(if ($problem) { $problem } else { 'none' })
                'oldProtected=' + (Get-AccessSection $old).AreAccessRulesProtected
                'oldFolderSids=' + ((Get-AceSids $old) -join ',')
                'pfxUsers=' + (Test-UsersAce (Join-Path $old 'web.pfx'))
                'pfxSids=' + ((Get-AceSids (Join-Path $old 'web.pfx')) -join ',')
                'jsonSids=' + ((Get-AceSids (Join-Path $old 'darling.json')) -join ',')

                $target = Join-Path $base 'target'
                New-Item -ItemType Directory -Path $target | Out-Null
                Set-Content -LiteralPath (Join-Path $target 'secret.txt') -Value 'placeholder'
                $targetBefore = (Get-Acl -LiteralPath $target).Sddl
                $secretBefore = (Get-Acl -LiteralPath (Join-Path $target 'secret.txt')).Sddl
                New-Item -ItemType Junction -Path $junction -Value $target | Out-Null
                $linkProblem = Protect-DarlingRollbackBackupFolder $junction -ResetChildren
                'linkProblem=' + $(if ($linkProblem) { $linkProblem } else { 'none' })
                'targetSame=' + ((Get-Acl -LiteralPath $target).Sddl -eq $targetBefore)
                'secretSame=' + ((Get-Acl -LiteralPath (Join-Path $target 'secret.txt')).Sddl -eq $secretBefore)
            }
            finally {
                if (Test-Path -LiteralPath $junction) { [System.IO.Directory]::Delete($junction) }
                $cleanup = Join-Path $base '_rollback_old'
                if (Test-Path -LiteralPath $cleanup) { Add-MeToFolder $cleanup }
                Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));

        Assert.Contains("beforePfxUsers=True", all, StringComparison.Ordinal);
        Assert.Contains("oldProblem=none", all, StringComparison.Ordinal);
        Assert.Contains("oldProtected=True", all, StringComparison.Ordinal);
        Assert.Contains("oldFolderSids=S-1-5-18,S-1-5-32-544", all, StringComparison.Ordinal);
        Assert.Contains("pfxUsers=False", all, StringComparison.Ordinal);
        Assert.Contains("pfxSids=S-1-5-18,S-1-5-32-544", all, StringComparison.Ordinal);
        Assert.Contains("jsonSids=S-1-5-18,S-1-5-32-544", all, StringComparison.Ordinal);
        Assert.Matches(@"linkProblem=The rollback backup .* is a link", all);
        Assert.Contains("targetSame=True", all, StringComparison.Ordinal);
        Assert.Contains("secretSame=True", all, StringComparison.Ordinal);
    }

    /// <summary>
    /// A retained backup that holds a file with a second name is not changed at all: setting the folder's DACL would rewrite the
    /// inherited entries of the file's other name too. The folder, the files in it and the other name keep their descriptors,
    /// and the warning names the file and the commands to run by hand.
    /// </summary>
    [Fact]
    public void ARetainedBackup_WithAHardLinkedFile_IsLeftUnchanged_AndTheOtherNameKeepsItsPermissions()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("DACLs and Windows PowerShell are Windows'.");
            return;
        }

        var script = Script("upgrade-darling.ps1");
        var probe = new StringBuilder();
        foreach (var name in new[] { "Get-DarlingFileNameCount", "Protect-DarlingRollbackBackupFolder" })
        {
            probe.AppendLine(ExtractFunction(script, name, "upgrade-darling.ps1"));
        }

        probe.AppendLine(FixturePreamble);
        probe.AppendLine("""
            try {
                $old = Join-Path $base '_rollback_old'
                New-Item -ItemType Directory -Path $old | Out-Null
                Set-Content -LiteralPath (Join-Path $old 'web.pfx') -Value 'placeholder'
                $outsideFile = Join-Path $base 'outside.txt'
                Set-Content -LiteralPath $outsideFile -Value 'placeholder'
                New-Item -ItemType HardLink -Path (Join-Path $old 'linked.txt') -Value $outsideFile | Out-Null
                $folderBefore = (Get-Acl -LiteralPath $old).Sddl
                $pfxBefore = (Get-Acl -LiteralPath (Join-Path $old 'web.pfx')).Sddl
                $outsideBefore = (Get-Acl -LiteralPath $outsideFile).Sddl
                'outsideUsersBefore=' + ((Get-AceSids $outsideFile) -contains $users.Value)

                $problem = Protect-DarlingRollbackBackupFolder $old -ResetChildren
                'problem=' + $(if ($problem) { $problem } else { 'none' })
                'folderSame=' + ((Get-Acl -LiteralPath $old).Sddl -eq $folderBefore)
                'pfxSame=' + ((Get-Acl -LiteralPath (Join-Path $old 'web.pfx')).Sddl -eq $pfxBefore)
                'outsideSame=' + ((Get-Acl -LiteralPath $outsideFile).Sddl -eq $outsideBefore)
            }
            finally { Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue }
            """);

        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));

        Assert.Contains("outsideUsersBefore=True", all, StringComparison.Ordinal);
        Assert.Matches(@"problem=The rollback backup '.*_rollback_old' was not changed.*linked\.txt.*fsutil hardlink list.*icacls ", all);
        Assert.Contains("folderSame=True", all, StringComparison.Ordinal);
        Assert.Contains("pfxSame=True", all, StringComparison.Ordinal);
        Assert.Contains("outsideSame=True", all, StringComparison.Ordinal);
    }

    /// <summary>A retained backup with a junction anywhere inside is not changed, and nothing the junction points at is touched.</summary>
    [Fact]
    public void ARetainedBackup_WithAJunctionInside_IsLeftUnchanged_AndTheTargetIsNotTouched()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("DACLs and Windows PowerShell are Windows'.");
            return;
        }

        var script = Script("upgrade-darling.ps1");
        var probe = new StringBuilder();
        foreach (var name in new[] { "Get-DarlingFileNameCount", "Protect-DarlingRollbackBackupFolder" })
        {
            probe.AppendLine(ExtractFunction(script, name, "upgrade-darling.ps1"));
        }

        probe.AppendLine(FixturePreamble);
        probe.AppendLine("""
            $junction = $null
            try {
                $old = Join-Path $base '_rollback_old'
                $inner = Join-Path $old 'inner'
                New-Item -ItemType Directory -Path $inner | Out-Null
                Set-Content -LiteralPath (Join-Path $old 'web.pfx') -Value 'placeholder'
                $target = Join-Path $base 'target'
                New-Item -ItemType Directory -Path $target | Out-Null
                Set-Content -LiteralPath (Join-Path $target 'secret.txt') -Value 'placeholder'
                $junction = Join-Path $inner 'pointer'
                New-Item -ItemType Junction -Path $junction -Value $target | Out-Null
                $folderBefore = (Get-Acl -LiteralPath $old).Sddl
                $pfxBefore = (Get-Acl -LiteralPath (Join-Path $old 'web.pfx')).Sddl
                $targetBefore = (Get-Acl -LiteralPath $target).Sddl
                $secretBefore = (Get-Acl -LiteralPath (Join-Path $target 'secret.txt')).Sddl

                $problem = Protect-DarlingRollbackBackupFolder $old -ResetChildren
                'problem=' + $(if ($problem) { $problem } else { 'none' })
                'folderSame=' + ((Get-Acl -LiteralPath $old).Sddl -eq $folderBefore)
                'pfxSame=' + ((Get-Acl -LiteralPath (Join-Path $old 'web.pfx')).Sddl -eq $pfxBefore)
                'targetSame=' + ((Get-Acl -LiteralPath $target).Sddl -eq $targetBefore)
                'secretSame=' + ((Get-Acl -LiteralPath (Join-Path $target 'secret.txt')).Sddl -eq $secretBefore)
            }
            finally {
                if ($junction -and (Test-Path -LiteralPath $junction)) { [System.IO.Directory]::Delete($junction) }
                Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));

        Assert.Matches(@"problem=The rollback backup '.*_rollback_old' was not changed.*pointer' is a link", all);
        Assert.Contains("folderSame=True", all, StringComparison.Ordinal);
        Assert.Contains("pfxSame=True", all, StringComparison.Ordinal);
        Assert.Contains("targetSame=True", all, StringComparison.Ordinal);
        Assert.Contains("secretSame=True", all, StringComparison.Ordinal);
    }

    /// <summary>When the DACL of a new backup folder cannot be set (Set-Acl stubbed to fail), the function returns the problem and
    /// leaves the folder as it was; the upgrade script stops with Fail before the first file is copied, and does not go on with a
    /// warning.</summary>
    [Fact]
    public void ANewBackupFolder_ThatCannotBeRestricted_StopsTheUpgradeBeforeTheCopy()
    {
        var upgrade = Script("upgrade-darling.ps1");
        var protect = upgrade.IndexOf("Protect-DarlingRollbackBackupFolder $backupPath\n", StringComparison.Ordinal);
        var failAt = upgrade.IndexOf("Fail \"The new rollback backup folder could not be restricted", protect, StringComparison.Ordinal);
        var copy = upgrade.IndexOf("Copy-Item -Destination $backupPath -Force", StringComparison.Ordinal);
        Assert.True(protect > 0 && failAt > protect && copy > failAt, $"order: protect {protect}, fail {failAt}, copy {copy}");
        Assert.Contains("if ($backupProblem) {", upgrade[protect..failAt], StringComparison.Ordinal);
        Assert.DoesNotContain("Warn $backupProblem", upgrade, StringComparison.Ordinal);
        Assert.Contains("NOTHING has been copied or overwritten", upgrade[failAt..copy], StringComparison.Ordinal);

        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("DACLs and Windows PowerShell are Windows'.");
            return;
        }

        var probe = new StringBuilder();
        foreach (var name in new[] { "Get-DarlingFileNameCount", "Protect-DarlingRollbackBackupFolder" })
        {
            probe.AppendLine(ExtractFunction(upgrade, name, "upgrade-darling.ps1"));
        }

        probe.AppendLine(FixturePreamble);
        probe.AppendLine("""
            function Set-Acl { throw 'stubbed failure' }
            try {
                $backup = Join-Path $base '_rollback_new'
                New-Item -ItemType Directory -Path $backup | Out-Null
                $problem = Protect-DarlingRollbackBackupFolder $backup
                'problem=' + $(if ($problem) { $problem } else { 'none' })
                'protected=' + (Get-AccessSection $backup).AreAccessRulesProtected
            }
            finally { Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue }
            """);

        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));

        Assert.Matches(@"problem=The rollback backup '.*_rollback_new' could not be limited to SYSTEM and Administrators \(stubbed failure\)", all);
        Assert.Contains("protected=False", all, StringComparison.Ordinal);
    }

    /// <summary>The per-file reset acts on a link itself and never on what it points to.</summary>
    [Fact]
    public void TheRetainedBackupReset_CarriesTheLinkSwitch()
    {
        var upgrade = Script("upgrade-darling.ps1");
        Assert.Contains("& icacls.exe $child.FullName /reset /L 2>&1", upgrade, StringComparison.Ordinal);
        Assert.DoesNotContain("/reset 2>&1", upgrade, StringComparison.Ordinal);
    }

    /// <summary>
    /// #6920: the step that restricts a rollback backup folder read the folder's permissions back with a .NET Framework
    /// only method, so under PowerShell 7 the upgrade stopped at "Method invocation failed ... GetAccessControl". It runs
    /// to the end in both PowerShell 7 and Windows PowerShell 5.1: the folder and the file in it end limited to SYSTEM and
    /// Administrators, and no problem is reported.
    /// </summary>
    [Theory]
    [InlineData("pwsh")]
    [InlineData("powershell.exe")]
    public void TheBackupFolderStep_RunsToTheEnd_InPowerShell7AndWindowsPowerShell(string host)
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("DACLs and Windows PowerShell are Windows'.");
            return;
        }

        if (host == "pwsh" && !OnPath("pwsh.exe"))
        {
            Assert.Skip("PowerShell 7 (pwsh.exe) is not on PATH on this machine.");
            return;
        }

        var script = Script("upgrade-darling.ps1");
        var probe = new StringBuilder();
        foreach (var name in new[] { "Get-DarlingFileNameCount", "Protect-DarlingRollbackBackupFolder" })
        {
            probe.AppendLine(ExtractFunction(script, name, "upgrade-darling.ps1"));
        }

        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
            $base = Join-Path $env:TEMP ('pm6920-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            try {
                $old = Join-Path $base '_rollback_old'
                New-Item -ItemType Directory -Path $old -Force | Out-Null
                Set-Content -LiteralPath (Join-Path $old 'web.pfx') -Value 'placeholder'
                'edition=' + $PSVersionTable.PSEdition
                $problem = Protect-DarlingRollbackBackupFolder $old -ResetChildren
                'problem=' + $(if ($problem) { $problem } else { 'none' })
                $access = [System.Security.AccessControl.AccessControlSections]::Access
                $folder = New-Object System.Security.AccessControl.DirectorySecurity($old, $access)
                'folderProtected=' + $folder.AreAccessRulesProtected
                'folderSids=' + ((@($folder.Access | ForEach-Object { $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value }) | Sort-Object -Unique) -join ',')
                'fileSids=' + ((@((New-Object System.Security.AccessControl.FileSecurity((Join-Path $old 'web.pfx'), $access)).Access | ForEach-Object { $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value }) | Sort-Object -Unique) -join ',')
            }
            finally {
                $cleanup = Join-Path $base '_rollback_old'
                if (Test-Path -LiteralPath $cleanup) { & icacls.exe $cleanup /grant ('*' + $me.Value + ':(OI)(CI)F') | Out-Null }
                Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString(), host));

        Assert.Contains(host == "pwsh" ? "edition=Core" : "edition=Desktop", all, StringComparison.Ordinal);
        Assert.Contains("problem=none", all, StringComparison.Ordinal);
        Assert.Contains("folderProtected=True", all, StringComparison.Ordinal);
        Assert.Contains("folderSids=S-1-5-18,S-1-5-32-544", all, StringComparison.Ordinal);
        Assert.Contains("fileSids=S-1-5-18,S-1-5-32-544", all, StringComparison.Ordinal);
    }

    private static bool OnPath(string exe) =>
        (Environment.GetEnvironmentVariable("PATH") ?? string.Empty)
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Any(dir => File.Exists(Path.Combine(dir.Trim('"'), exe)));

    private static string ExtractFunction(string script, string name, string scriptName)
    {
        var start = script.IndexOf("function " + name + "(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{scriptName} no longer defines {name}");

        var open = script.IndexOf('{', start);
        var depth = 0;
        for (var i = open; i < script.Length; i++)
        {
            if (script[i] == '{')
            {
                depth++;
            }
            else if (script[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return script.Substring(start, i - start + 1);
                }
            }
        }

        Assert.Fail($"unbalanced braces while extracting {name} from {scriptName}");
        return string.Empty;
    }

    private static List<string> RunWindowsPowerShell(string script, string host = "powershell.exe")
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-5700-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script);
        try
        {
            using var process = Process.Start(WindowsPowerShellStartInfo.Create(host, $"-NoProfile -ExecutionPolicy Bypass -File \"{path}\""));
            Assert.NotNull(process);

            var stdoutTask = process!.StandardOutput.ReadToEndAsync();
            var stderrTask = process.StandardError.ReadToEndAsync();

            var exited = process.WaitForExit(90_000);
            if (!exited)
            {
                try { process.Kill(entireProcessTree: true); }
                catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or System.ComponentModel.Win32Exception)
                {
                    /* Already gone between the timeout and the kill. */
                }
            }

            var stdout = stdoutTask.GetAwaiter().GetResult();
            var stderr = stderrTask.GetAwaiter().GetResult();

            Assert.True(exited, $"powershell.exe did not exit within 90 seconds. Output so far:\n{stdout}\n{stderr}");
            Assert.True(string.IsNullOrWhiteSpace(stderr), $"powershell.exe reported an error:\n{stderr}\nOutput:\n{stdout}");

            return stdout.Split('\n').Select(line => line.Trim()).Where(line => line.Length > 0).ToList();
        }
        finally
        {
            try { File.Delete(path); }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                /* A temp file that cannot be removed is not a test failure. */
            }
        }
    }
}
