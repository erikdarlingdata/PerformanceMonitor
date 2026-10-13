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
        "Get-DarlingConfiguredPathsUnder", "Invoke-DarlingTlsKeyFileHardening",
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

        Assert.Contains("--harden-files --tls-key-files-only", ExtractFunction(script, "Invoke-DarlingTlsKeyFileHardening", scriptName), StringComparison.Ordinal);
    }

    /// <summary>The verb runs after the last tree lock (the lock resets any extra write grant on a file it walks) and before
    /// the service starts, in both scripts. A failure to run it is a warning, never a stop.</summary>
    [Fact]
    public void TheScripts_CallTheVerb_AfterTheLastTreeLock_AndBeforeTheServiceStarts_AndNeverFailOverIt()
    {
        var install = Script("install-darling.ps1");
        var upgrade = Script("upgrade-darling.ps1");

        var installLock = install.LastIndexOf("\nInvoke-InstallTreeLock $serviceAccount\n", StringComparison.Ordinal);
        var installCall = install.IndexOf("\n$tlsKeyWarning = Invoke-DarlingTlsKeyFileHardening $serviceExe\n", StringComparison.Ordinal);
        var installStart = install.IndexOf("\nStart-Service -Name $serviceName\n", StringComparison.Ordinal);
        Assert.True(installLock > 0 && installCall > installLock && installCall < installStart, $"install order: lock {installLock}, call {installCall}, start {installStart}");
        Assert.Matches(@"\$tlsKeyWarning = Invoke-DarlingTlsKeyFileHardening \$serviceExe\nif \(\$tlsKeyWarning\) \{ Write-Host ""WARNING: \$tlsKeyWarning"" -ForegroundColor Yellow \}\n", install);

        var upgradeLock = upgrade.LastIndexOf("\nInvoke-UpgradeTreeLock 'after the copy'\n", StringComparison.Ordinal);
        var upgradeCall = upgrade.IndexOf("\n$tlsKeyWarning = Invoke-DarlingTlsKeyFileHardening (Join-Path $InstallRoot $serviceExeName)\n", StringComparison.Ordinal);
        var upgradeStart = upgrade.IndexOf("\nStart-Service -Name $serviceName\n", StringComparison.Ordinal);
        Assert.True(upgradeLock > 0 && upgradeCall > upgradeLock && upgradeCall < upgradeStart, $"upgrade order: lock {upgradeLock}, call {upgradeCall}, start {upgradeStart}");
        Assert.Matches(@"Invoke-DarlingTlsKeyFileHardening \(Join-Path \$InstallRoot \$serviceExeName\)\nif \(\$tlsKeyWarning\) \{ Warn \$tlsKeyWarning \}\n", upgrade);

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
        var retained = upgrade.IndexOf("Protect-DarlingRollbackBackupFolder $retained.FullName -ResetChildren", StringComparison.Ordinal);
        var start = upgrade.IndexOf("\nStart-Service -Name $serviceName\n", StringComparison.Ordinal);
        Assert.True(prune > 0 && retained > prune && retained < start, $"retained order: prune {prune}, retained {retained}, start {start}");
        Assert.Contains("foreach ($retained in @(Get-DarlingRollbackBackups $InstallRoot))", upgrade, StringComparison.Ordinal);

        Assert.DoesNotContain("$tlsKeyNames", upgrade, StringComparison.Ordinal);
    }

    /// <summary>The call, with a stub in place of the exe: the arguments reach it, its lines are echoed, a nonzero exit and an
    /// exe that is not there each give a warning that names the verb, and a zero exit gives none. Nothing throws.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheVerbCall_WarnsWithTheCommandOnANonzeroExit_AndNeverThrows(string scriptName)
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("Windows PowerShell and .cmd stubs are Windows'.");
            return;
        }

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(Script(scriptName), "Invoke-DarlingTlsKeyFileHardening", scriptName));
        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $dir = Join-Path $env:TEMP ('pm5700-stub-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
            try {
                $bad = Join-Path $dir 'bad.cmd'
                Set-Content -LiteralPath $bad -Value @('@echo off', 'echo stub-line args=%*', 'exit /b 3') -Encoding ASCII
                $good = Join-Path $dir 'good.cmd'
                Set-Content -LiteralPath $good -Value @('@echo off', 'echo stub-line args=%*', 'exit /b 0') -Encoding ASCII
                $w = Invoke-DarlingTlsKeyFileHardening $bad
                'nonzero=' + $w
                $w = Invoke-DarlingTlsKeyFileHardening $good
                'zero=' + $(if ($null -eq $w) { 'null' } else { $w })
                $w = Invoke-DarlingTlsKeyFileHardening (Join-Path $dir 'missing.exe')
                'missing=' + $(if ($null -eq $w) { 'null' } else { $w })
            }
            finally { Remove-Item -LiteralPath $dir -Recurse -Force -ErrorAction SilentlyContinue }
            """);

        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));

        Assert.Equal(2, Regex.Matches(all, "stub-line args=--harden-files --tls-key-files-only").Count);
        Assert.Matches(@"nonzero=The service could not check the TLS key files darling\.json names \(exit code 3\).*--harden-files --tls-key-files-only", all);
        Assert.Contains("zero=null", all, StringComparison.Ordinal);
        Assert.Matches(@"missing=The service could not check the TLS key files darling\.json names \(.+\).*--harden-files --tls-key-files-only", all);
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
    /// and Administrators (an explicit entry the file had is cleared too); a file with a second name is left alone and named in
    /// the warning; a backup folder that is a junction is not followed, so what it points at keeps its permissions.
    /// </summary>
    [Fact]
    public void TheRetainedBackups_AreRestricted_ALinkIsNotFollowed_AndAHardLinkedFileIsNamed()
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
                $outsideFile = Join-Path $base 'outside.txt'
                Set-Content -LiteralPath $outsideFile -Value 'placeholder'
                New-Item -ItemType HardLink -Path (Join-Path $old 'linked.txt') -Value $outsideFile | Out-Null
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
        Assert.True(Regex.IsMatch(all, @"oldProblem=.*linked\.txt"), all);
        Assert.DoesNotMatch(@"oldProblem=.*(web\.pfx|darling\.json)", all);
        Assert.Contains("oldProtected=True", all, StringComparison.Ordinal);
        Assert.Contains("oldFolderSids=S-1-5-18,S-1-5-32-544", all, StringComparison.Ordinal);
        Assert.Contains("pfxUsers=False", all, StringComparison.Ordinal);
        Assert.Contains("pfxSids=S-1-5-18,S-1-5-32-544", all, StringComparison.Ordinal);
        Assert.Contains("jsonSids=S-1-5-18,S-1-5-32-544", all, StringComparison.Ordinal);
        Assert.Matches(@"linkProblem=The rollback backup .* is a link", all);
        Assert.Contains("targetSame=True", all, StringComparison.Ordinal);
        Assert.Contains("secretSame=True", all, StringComparison.Ordinal);
    }

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

    private static List<string> RunWindowsPowerShell(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-5700-{Guid.NewGuid():N}.ps1");
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
