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
using System.Security.Principal;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The local operator group is made by both scripts that put a Darling build on a machine. The service grants
/// that group read on <c>darling.json</c> and the store's credential files and no longer grants
/// <c>NT AUTHORITY\INTERACTIVE</c>, so a machine where the group does not exist leaves every non-elevated Viewer
/// and CLI user locked out at the service's first start. The installer makes the group; the upgrade script has to
/// make it too, or an in-place upgrade of an older install is exactly that machine.
///
/// <para>Each case runs over both scripts. The upgrade script must carry the installer's group code word for
/// word (so the two cannot drift), must run it after the new build is copied and before the service starts,
/// must say who to add, and must add nobody itself. The lookup half is also run under Windows PowerShell against
/// a group that already exists: that is the path an upgrade takes when an administrator made the group earlier,
/// and it changes nothing on the machine. The creation call is judged by its text only, because a test never
/// creates local groups.</para>
/// </summary>
public sealed class DarlingOperatorGroupScriptTests
{
    /* The scripts are CRLF in a working tree and LF in the index, so the text is compared with LF line ends. */
    private static string Script(string scriptName) =>
        ReadRepoFile("Darling", "tools", scriptName).Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The scripts repeat the service's group name because they cannot reference it. A mismatch would
    /// create a group that nothing grants.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheScriptNamesTheGroupTheServiceGrants(string scriptName)
    {
        var match = Regex.Match(Script(scriptName), @"^\$operatorGroupName\s*=\s*'([^']+)'", RegexOptions.Multiline);

        Assert.True(match.Success, $"{scriptName} does not declare $operatorGroupName");
        Assert.Equal(DarlingFileSecurity.OperatorGroupName, match.Groups[1].Value);
    }

    /// <summary>Lookup first, so a group that already exists (a re-run, an upgrade, one an administrator made) is
    /// left exactly as it is; then one creation call, for the group only. No script adds a member: who gets the
    /// group's access is the administrator's decision.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheScriptLooksTheGroupUpFirst_CreatesItEmpty_AndAddsNoMember(string scriptName)
    {
        var script = Script(scriptName);
        var body = ExtractFunction(script, "Initialize-DarlingOperatorGroup", scriptName);

        var lookup = body.IndexOf("$account.Translate($sidType)", StringComparison.Ordinal);
        var create = body.IndexOf("& net.exe localgroup $groupName /add", StringComparison.Ordinal);
        Assert.True(lookup >= 0, $"{scriptName}: the group lookup is gone");
        Assert.True(create > lookup, $"{scriptName}: the group must be looked up before it is created");

        Assert.Single(Regex.Matches(script, @"&\s+net\.exe\s+localgroup", RegexOptions.IgnoreCase));
        Assert.DoesNotContain("Add-LocalGroupMember", script, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Add-ADGroupMember", script, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The lookup asks only this machine's own accounts, and a name that resolves anywhere else is refused
    /// with the command that creates the local group.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheLookupIsMachineQualified_AndRefusesAGroupThatIsNotLocal(string scriptName)
    {
        var body = ExtractFunction(Script(scriptName), "Initialize-DarlingOperatorGroup", scriptName);

        Assert.Contains("New-Object System.Security.Principal.NTAccount($env:COMPUTERNAME, $groupName)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("NTAccount($groupName)", body, StringComparison.Ordinal);
        Assert.Contains("is not a group on this machine", body, StringComparison.Ordinal);
        Assert.Contains("net localgroup `\"$groupName`\" /add", body, StringComparison.Ordinal);
    }

    /// <summary>The upgrade script reuses the installer's code rather than a look-alike, so a fix to one is a fix
    /// to both.</summary>
    [Fact]
    public void TheUpgradeScriptCarriesTheInstallersGroupCodeWordForWord()
    {
        var installer = Script("install-darling.ps1");
        var upgrade = Script("upgrade-darling.ps1");

        foreach (var function in new[] { "Get-DarlingNativeText", "Initialize-DarlingOperatorGroup" })
        {
            Assert.Equal(
                ExtractFunction(installer, function, "install-darling.ps1"),
                ExtractFunction(upgrade, function, "upgrade-darling.ps1"));
        }
    }

    /// <summary>The service reads the group on its first start after the upgrade, so the group has to exist
    /// before that start, and only a real upgrade (the copy is done) reaches the code.</summary>
    [Fact]
    public void TheUpgradeScriptMakesTheGroupAfterTheCopyAndBeforeTheServiceStarts()
    {
        var script = Script("upgrade-darling.ps1");

        var copied = script.IndexOf("New build in place.", StringComparison.Ordinal);
        var call = script.IndexOf("Initialize-DarlingOperatorGroup $operatorGroupName", StringComparison.Ordinal);
        var start = script.IndexOf("Start-Service -Name $serviceName", StringComparison.Ordinal);

        Assert.True(copied >= 0, "upgrade-darling.ps1 no longer reports the new build in place");
        Assert.True(start > copied, "upgrade-darling.ps1 no longer starts the service after the copy");
        Assert.True(call >= 0, "upgrade-darling.ps1 never creates the operator group");
        Assert.True(call > copied && call < start,
            "the operator group must be created after the copy and before the service starts, or the service's " +
            "first start after the upgrade finds no group to grant");
    }

    /// <summary>The output names the group, says who to add and what membership allows, and says the sign-out
    /// is needed. Both scripts print the same lines.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheScriptSaysWhoToAdd_AndThatItAddsNoOne(string scriptName)
    {
        var script = Script(scriptName);

        Assert.Contains("Local group '$operatorGroupName' is in place. This script never adds members to it.", script, StringComparison.Ordinal);
        Assert.Contains("Add each person who runs the Viewer or the CLI without elevation to the local group '$operatorGroupName'", script, StringComparison.Ordinal);
        Assert.Contains("Members can read darling.json and the store's admin and viewer credentials, which decrypt every stored password", script, StringComparison.Ordinal);
        Assert.Contains("A person added to the group has to sign out and back in before the membership takes effect.", script, StringComparison.Ordinal);
    }

    /// <summary>A group the upgrade cannot make must not leave the monitoring host stopped: the files stay
    /// locked to SYSTEM, Administrators and the service, the service still collects, and the warning gives the
    /// by-hand command.</summary>
    [Fact]
    public void AGroupTheUpgradeCannotMakeIsAWarning_NotAStop()
    {
        var script = Script("upgrade-darling.ps1");

        var call = script.IndexOf("Initialize-DarlingOperatorGroup $operatorGroupName", StringComparison.Ordinal);
        var start = script.IndexOf("Start-Service -Name $serviceName", StringComparison.Ordinal);
        Assert.True(call >= 0 && start > call, "upgrade-darling.ps1 does not create the operator group before it starts the service");

        var between = script.Substring(call, start - call);
        Assert.Contains("Warn \"Could not create the local group '$operatorGroupName'", between, StringComparison.Ordinal);
        Assert.Contains("net localgroup `\"$operatorGroupName`\" /add", between, StringComparison.Ordinal);
        Assert.DoesNotContain("Fail ", between, StringComparison.Ordinal);
    }

    /// <summary>Runs the extracted lookup under Windows PowerShell against a group that already exists, which is the
    /// path that changes nothing, so this creates nothing. The group is one of this machine's own accounts when it has
    /// one; otherwise a built-in group is looked up under the BUILTIN domain in place of the machine name, which
    /// exercises the same code on a machine with no local group of its own.</summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    public void TheLookupReturnsAnExistingGroupsSid_AndCreatesNothing(string scriptName)
    {
        var script = Script(scriptName);
        var localGroups = RunWindowsPowerShell(
            "Get-CimInstance Win32_Group -Filter \"LocalAccount=True\" | Where-Object { $_.Domain -eq $env:COMPUTERNAME -and $_.SID -notlike 'S-1-5-32-*' } | ForEach-Object { $_.Name + '|' + $_.SID }");

        string groupName;
        string expectedSid;
        var lookup = ExtractFunction(script, "Initialize-DarlingOperatorGroup", scriptName);
        var setup = "$null = 0";
        if (localGroups.Count > 0)
        {
            var parts = localGroups[0].Split('|');
            groupName = parts[0];
            expectedSid = parts[1];
        }
        else
        {
            var standIn = new SecurityIdentifier(WellKnownSidType.BuiltinPerformanceLoggingUsersSid, null);
            groupName = standIn.Translate(typeof(NTAccount)).Value.Split('\\')[^1];
            expectedSid = standIn.Value;
            setup = "$env:COMPUTERNAME = 'BUILTIN'";
        }

        var probe = string.Join(
            "\n",
            setup,
            ExtractFunction(script, "Get-DarlingNativeText", scriptName),
            ExtractFunction(script, "Get-DarlingProductType", scriptName),
            ExtractFunction(script, "Test-DarlingDomainController", scriptName),
            lookup,
            $"(Initialize-DarlingOperatorGroup '{groupName.Replace("'", "''", StringComparison.Ordinal)}').Value");

        var lines = RunWindowsPowerShell(probe);

        Assert.Equal(new[] { expectedSid }, lines);
    }

    /// <summary>Returns the full <c>function NAME(...) { ... }</c> definition text from the script, signature
    /// included, so the extracted copy takes its parameters the way the shipped one does.</summary>
    private static string ExtractFunction(string script, string name, string scriptName)
    {
        var start = script.IndexOf("function " + name, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{scriptName} no longer defines {name}");

        var open = script.IndexOf('{', start);
        Assert.True(open >= 0, $"expected an opening brace after {name} in {scriptName}");

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

    /// <summary>Runs <paramref name="script"/> under Windows PowerShell 5.1 and returns its non-empty output
    /// lines. Written to a temp file, not passed with -Command, because the script is whole function bodies.</summary>
    private static List<string> RunWindowsPowerShell(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-opgroup-{Guid.NewGuid():N}.ps1");
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

            /* Both streams are drained concurrently: reading one to the end before the other deadlocks when the
               child fills the pipe it is not being read from. */
            var stdoutTask = process!.StandardOutput.ReadToEndAsync();
            var stderrTask = process.StandardError.ReadToEndAsync();

            var exited = process.WaitForExit(60_000);
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

            Assert.True(exited, $"powershell.exe did not exit within 60 seconds. Output so far:\n{stdout}\n{stderr}");
            Assert.True(string.IsNullOrWhiteSpace(stderr), $"powershell.exe reported an error:\n{stderr}");

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
