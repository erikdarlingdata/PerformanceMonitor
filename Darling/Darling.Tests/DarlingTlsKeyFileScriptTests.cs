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
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5700: the install and upgrade scripts give the TLS private-key files that darling.json names (each pfxPath and
/// keyPath) the protection darling.json gets: SYSTEM, Administrators and the service account only. The functions are
/// lifted out of each script and run under Windows PowerShell 5.1 against a sample darling.json in a scratch folder,
/// and the copies in the two scripts are compared byte for byte.
/// </summary>
public sealed class DarlingTlsKeyFileScriptTests
{
    private static readonly string[] s_sharedFunctions =
    {
        "Get-DarlingConfiguredPathEntries", "Get-DarlingConfiguredPathsUnder", "Get-DarlingTlsKeyFiles", "Protect-DarlingTlsKeyFiles",
    };

    private static string Script(string scriptName) =>
        ReadRepoFile("Darling", "tools", scriptName).Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The four functions ship in both scripts, which cannot import each other, so a fix to one that misses the
    /// other would leave half the installs with a readable key file.</summary>
    [Fact]
    public void TheTlsKeyFileFunctions_ShipIdenticallyInTheInstallAndUpgradeScripts()
    {
        var install = Script("install-darling.ps1");
        var upgrade = Script("upgrade-darling.ps1");

        foreach (var name in s_sharedFunctions)
        {
            Assert.Equal(ExtractFunction(install, name, "install-darling.ps1"), ExtractFunction(upgrade, name, "upgrade-darling.ps1"));
        }
    }

    /// <summary>The install script runs the lock right after the step that locks darling.json; the upgrade script runs it
    /// after the operator group and before it starts the service. A key file in the rollback backup gets the restriction
    /// the config copy gets. None of it can fail the run.</summary>
    [Fact]
    public void TheScripts_RunTheTlsKeyFileLock_AtTheRightStep_AndNeverFailOverIt()
    {
        var install = Script("install-darling.ps1");
        var upgrade = Script("upgrade-darling.ps1");

        var installCall = install.IndexOf("Protect-DarlingTlsKeyFiles $root $serviceAccount", StringComparison.Ordinal);
        Assert.True(installCall > install.IndexOf("SECURITY WARNING: these files still are not restricted", StringComparison.Ordinal));
        Assert.True(installCall < install.IndexOf("Local group '$operatorGroupName' is in place", StringComparison.Ordinal));

        var upgradeCall = upgrade.IndexOf("Protect-DarlingTlsKeyFiles $InstallRoot $logonAccount", StringComparison.Ordinal);
        Assert.True(upgradeCall > upgrade.IndexOf("# ============================ the operator group", StringComparison.Ordinal));
        Assert.True(upgradeCall < upgrade.IndexOf("Start-Service -Name $serviceName", StringComparison.Ordinal));

        Assert.Contains("-contains $_.Name", upgrade, StringComparison.Ordinal);
        Assert.Contains("(Get-DarlingTlsKeyFiles $InstallRoot).Paths", upgrade, StringComparison.Ordinal);

        foreach (var name in new[] { "Get-DarlingTlsKeyFiles", "Protect-DarlingTlsKeyFiles" })
        {
            var body = ExtractFunction(install, name, "install-darling.ps1");
            Assert.DoesNotContain("Fail ", body, StringComparison.Ordinal);
            Assert.DoesNotContain("throw", body, StringComparison.Ordinal);
            Assert.DoesNotContain("Write-Host", body, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The lock, run on a sample darling.json (with a comment, as the scripts must read) that names a pfx in the install
    /// folder, a PEM key and a certificate outside it, a key that is not there and a relative pfx. The pfx and the key lose
    /// the Users read they were given and keep the service account; the folder the key sits in keeps its ACL; the
    /// certificate keeps its ACL; the missing and relative ones come back as problems, the missing one with its icacls line.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheLock_SecuresAPfxAndAKeyNamedInDarlingJson_AndReportsTheRest(string scriptName)
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("DACLs and Windows PowerShell are Windows'.");
            return;
        }

        var script = Script(scriptName);
        var probe = new StringBuilder();
        foreach (var name in new[]
        {
            "Get-DarlingSegmentsBelowRoot", "Get-DarlingCanonicalPath", "Test-PathIsAtOrUnder", "Get-DarlingFileNameCount",
            "Get-DarlingConfiguredPathEntries", "Get-DarlingTlsKeyFiles", "Protect-DarlingTlsKeyFiles",
        })
        {
            probe.AppendLine(ExtractFunction(script, name, scriptName));
        }

        probe.AppendLine("""
            $ErrorActionPreference = 'Stop'
            $base = Join-Path $env:TEMP ('pm5700-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            $install = Join-Path $base 'install'
            $outside = Join-Path $base 'outside'
            New-Item -ItemType Directory -Path $install, $outside -Force | Out-Null
            try {
                $pfx = Join-Path $install 'web.pfx'
                $key = Join-Path $outside 'mcp.key'
                $cert = Join-Path $outside 'mcp.crt'
                $gone = Join-Path $install 'gone.key'
                $users = New-Object System.Security.Principal.SecurityIdentifier([System.Security.Principal.WellKnownSidType]::BuiltinUsersSid, $null)
                foreach ($file in @($pfx, $key, $cert)) {
                    Set-Content -LiteralPath $file -Value 'placeholder'
                    $acl = Get-Acl -LiteralPath $file
                    $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($users, 'ReadAndExecute', 'Allow')))
                    Set-Acl -LiteralPath $file -AclObject $acl
                }
                $hasUsers = { param($f) @((Get-Acl -LiteralPath $f).Access | Where-Object { $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).IsWellKnown([System.Security.Principal.WellKnownSidType]::BuiltinUsersSid) }).Count -gt 0 }
                $folderBefore = (Get-Acl -LiteralPath $outside).Sddl
                $certBefore = (Get-Acl -LiteralPath $cert).Sddl
                $j = { param($p) $p -replace '\\', '\\' }
                Set-Content -LiteralPath (Join-Path $install 'darling.json') -Value (@(
                    '{',
                    '  // the web listener',
                    ('  "web": { "network": { "tls": { "pfxPath": "' + (& $j $pfx) + '" } } },'),
                    ('  "mcp": { "network": { "tls": { "certPath": "' + (& $j $cert) + '", "keyPath": "' + (& $j $key) + '" } } },'),
                    ('  "other": { "tls": { "keyPath": "' + (& $j $gone) + '", "pfxPath": "relative.pfx" } }'),
                    '}') -join "`n")
                $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
                'usersBefore=' + (& $hasUsers $pfx) + '/' + (& $hasUsers $key)
                $r = Protect-DarlingTlsKeyFiles $install $me
                foreach ($line in $r.Secured) { 'secured=' + $line.Replace($base, '<base>') }
                foreach ($line in $r.Problems) { 'problem=' + $line.Replace($base, '<base>') }
                'usersAfter=' + (& $hasUsers $pfx) + '/' + (& $hasUsers $key)
                'protected=' + (Get-Acl -LiteralPath $pfx).AreAccessRulesProtected + '/' + (Get-Acl -LiteralPath $key).AreAccessRulesProtected
                'folderSame=' + ((Get-Acl -LiteralPath $outside).Sddl -eq $folderBefore)
                'certSame=' + ((Get-Acl -LiteralPath $cert).Sddl -eq $certBefore)
            }
            finally {
                Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var answers = RunWindowsPowerShell(probe.ToString());
        var all = string.Join("\n", answers);

        Assert.Contains("usersBefore=True/True", all, StringComparison.Ordinal);
        Assert.Contains(@"secured=<base>\install\web.pfx (in the install folder)", all, StringComparison.Ordinal);
        Assert.Contains(@"secured=<base>\outside\mcp.key (outside the install folder; only the file was changed, not its folder)", all, StringComparison.Ordinal);
        Assert.Contains("usersAfter=False/False", all, StringComparison.Ordinal);
        Assert.Contains("protected=True/True", all, StringComparison.Ordinal);
        Assert.Contains("folderSame=True", all, StringComparison.Ordinal);
        Assert.Contains("certSame=True", all, StringComparison.Ordinal);
        Assert.Contains(@"problem=The TLS key file '<base>\install\gone.key' that darling.json names was not found", all, StringComparison.Ordinal);
        Assert.Contains(@"icacls ""<base>\install\gone.key"" /inheritance:r /grant:r", all, StringComparison.Ordinal);
        Assert.Contains("problem=The TLS key file 'relative.pfx' is a relative path", all, StringComparison.Ordinal);
        Assert.DoesNotContain("mcp.crt", all.Replace("certSame", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
    }

    /// <summary>A darling.json that is missing or names no key file gives an empty result and no problem.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheLock_HasNothingToSayWhenNoKeyFileIsNamed(string scriptName)
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("Windows PowerShell is Windows'.");
            return;
        }

        var script = Script(scriptName);
        var probe = new StringBuilder();
        foreach (var name in new[]
        {
            "Get-DarlingSegmentsBelowRoot", "Get-DarlingCanonicalPath", "Test-PathIsAtOrUnder", "Get-DarlingFileNameCount",
            "Get-DarlingConfiguredPathEntries", "Get-DarlingTlsKeyFiles", "Protect-DarlingTlsKeyFiles",
        })
        {
            probe.AppendLine(ExtractFunction(script, name, scriptName));
        }

        probe.AppendLine("""
            $base = Join-Path $env:TEMP ('pm5700-none-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $base -Force | Out-Null
            try {
                $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
                $r = Protect-DarlingTlsKeyFiles $base $me
                'missingConfig=' + @($r.Secured).Count + '/' + @($r.Problems).Count
                Set-Content -LiteralPath (Join-Path $base 'darling.json') -Value '{ "web": { "network": { "tls": { "certPath": "C:\\certs\\a.pem" } } } }'
                $r = Protect-DarlingTlsKeyFiles $base $me
                'certOnly=' + @($r.Secured).Count + '/' + @($r.Problems).Count
            }
            finally {
                Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue
            }
            """);

        var all = string.Join("\n", RunWindowsPowerShell(probe.ToString()));

        Assert.Contains("missingConfig=0/0", all, StringComparison.Ordinal);
        Assert.Contains("certOnly=0/0", all, StringComparison.Ordinal);
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
