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

        /* The upgrade script never adds a member. The installer adds one only in Add-DarlingOperatorToGroup (#6478), which
           step 7 calls after an answer of yes or -AddOperator; the group creation above is its other net.exe call. */
        var isInstaller = scriptName == "install-darling.ps1";
        Assert.Equal(isInstaller ? 2 : 1, Regex.Matches(script, @"&\s+net\.exe\s+localgroup", RegexOptions.IgnoreCase).Count);
        if (isInstaller)
        {
            Assert.Contains("& net.exe localgroup $groupName $account /add", ExtractFunction(script, "Add-DarlingOperatorToGroup", scriptName), StringComparison.Ordinal);
        }

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

        /* The installer says it in Write-DarlingOperatorGuidance, after the question (#6478); its behaviour is proved by the
           operator-step tests below. The upgrade script asks nothing and still says it inline. */
        var text = scriptName == "install-darling.ps1" ? ExtractFunction(script, "Write-DarlingOperatorGuidance", scriptName) : script;
        var groupVariable = scriptName == "install-darling.ps1" ? "$groupName" : "$operatorGroupName";
        Assert.Contains(scriptName == "install-darling.ps1" ? "No one was added to it." : "This script never adds members to it.", text, StringComparison.Ordinal);
        Assert.Contains("Add each person who runs the Viewer or the CLI without elevation to the local group '" + groupVariable + "'", text, StringComparison.Ordinal);
        Assert.Contains("Members can read darling.json and the store's admin and viewer credentials, which decrypt every stored password", text, StringComparison.Ordinal);
        Assert.Contains("A person added to the group has to sign out and back in before the membership takes effect.", text, StringComparison.Ordinal);
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

    /// <summary>A fresh install that cannot make the group stops, but only once the service can read its files. Step 1b2
    /// locks the tree with no grant for the service and sc.exe create registers it to start automatically, so a stop before
    /// step 4b2's grant would leave a service that cannot read its own binaries. The refusal comes after that grant and
    /// before the service starts, and none is placed between the two lock calls.</summary>
    [Fact]
    public void TheInstallerRefusesAGroupItCannotMake_AfterTheServiceIsGrantedItsFiles_AndBeforeItStarts()
    {
        var script = Script("install-darling.ps1");
        const string refusalText = "Could not create the local group '$operatorGroupName'";

        var firstLock = script.IndexOf("Invoke-InstallTreeLock $lockAccount -StopOnOpen", StringComparison.Ordinal);
        var serviceLock = script.IndexOf("Invoke-InstallTreeLock $serviceAccount", StringComparison.Ordinal);
        var refusal = script.IndexOf(refusalText, StringComparison.Ordinal);
        var start = script.IndexOf("Start-Service -Name $serviceName", StringComparison.Ordinal);

        Assert.True(firstLock >= 0 && serviceLock > firstLock, "the two install-tree lock calls are gone or out of order");
        Assert.True(refusal >= 0 && start > refusal, "the installer no longer refuses a group it cannot make before it starts the service");
        Assert.True(refusal > serviceLock,
            "the group refusal must come after the service is granted its files (step 4b2): a stop between step 1b2's lock " +
            "and that grant leaves an auto-start service that cannot read its own binaries");

        var window = script.Substring(firstLock, serviceLock - firstLock);
        Assert.DoesNotContain(refusalText, window, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(script, Regex.Escape(refusalText)));
        Assert.Contains("Fail @\"", script.Substring(refusal - 40, 40), StringComparison.Ordinal);
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

    private const string OperatorStepGroup = "Test Operators";

    /// <summary>Runs the installer's step 7 (<c>Invoke-DarlingOperatorStep</c>) under Windows PowerShell with the machine
    /// stood in for: the answer, the signed-in account, the host, and the two calls that change the machine (the group
    /// add and the direct grant) are stubs that only write to a log, so nothing on this machine is created or changed.
    /// Returns the log: every line the step printed, the prompt it asked, each stub call, and the result.</summary>
    private static List<string> RunOperatorStep(string call, string signedInUser = "'CONTOSO\\alice'", string answer = "''", string hostIsInteractive = "$true")
    {
        const string scriptName = "install-darling.ps1";
        var script = Script(scriptName);
        var probe = string.Join(
            "\n",
            "$script:log = New-Object System.Collections.ArrayList",
            "$script:answer = " + answer,
            "function Write-Host { param($Object, $ForegroundColor) [void]$script:log.Add([string]$Object) }",
            "function Test-DarlingInteractiveHost { " + hostIsInteractive + " }",
            "function Get-DarlingInteractiveUser { " + signedInUser + " }",
            "function Resolve-DarlingOperatorAccount([string]$account) { if ($account -eq 'CONTOSO\\alice') { New-Object System.Security.Principal.SecurityIdentifier 'S-1-5-32-546' } else { New-Object System.Security.Principal.SecurityIdentifier 'S-1-5-32-545' } }",
            "function Read-Host { param($Prompt) [void]$script:log.Add('PROMPT ' + $Prompt); $script:answer }",
            "function Add-DarlingOperatorToGroup([string]$groupName, [string]$account) { [void]$script:log.Add('ADD ' + $groupName + '|' + $account) }",
            "function Grant-DarlingOperatorDirect($sid, [string]$configPath, [string]$storeFolder) { [void]$script:log.Add('GRANT ' + $sid.Value + '|' + $configPath + '|' + $storeFolder); @() }",
            ExtractFunction(script, "Write-DarlingOperatorGuidance", scriptName),
            ExtractFunction(script, "Invoke-DarlingOperatorStep", scriptName),
            "$result = " + call,
            "[void]$script:log.Add('RESULT ' + $result)",
            "$script:log");
        return RunWindowsPowerShell(probe);
    }

    private static string GuestsAccount() =>
        new SecurityIdentifier(WellKnownSidType.BuiltinGuestsSid, null).Translate(typeof(NTAccount)).Value;

    /// <summary>Nothing answered (Enter) is No: the prompt names the account and the group, the group is not touched, the
    /// closing text still says who to add and that a person signs out and back in, and the step reports that no one was
    /// added. It also says why the Viewer needs the group (what members get, and that the Viewer uses the admin role).</summary>
    [Fact]
    public void TheOperatorQuestionDefaultsToNo_AndAddsNoOne()
    {
        var log = RunOperatorStep($"Invoke-DarlingOperatorStep '{OperatorStepGroup}' '' $false 'C:\\x\\darling.json' 'C:\\s'");

        Assert.Contains($"PROMPT Add CONTOSO\\alice to '{OperatorStepGroup}'? [y/N]", log);
        Assert.DoesNotContain(log, line => line.StartsWith("ADD ", StringComparison.Ordinal) || line.StartsWith("GRANT ", StringComparison.Ordinal));
        Assert.Contains(log, line => line.Contains("Members can read darling.json and the store's admin and viewer credentials", StringComparison.Ordinal)
            && line.Contains("Viewer connects with the store's admin role", StringComparison.Ordinal));
        Assert.Contains(log, line => line.Contains("A person added to the group has to sign out and back in", StringComparison.Ordinal));
        Assert.Contains($"Add each person who runs the Viewer or the CLI without elevation to the local group '{OperatorStepGroup}' (Computer Management > Local Users and Groups, or net localgroup \"{OperatorStepGroup}\" DOMAIN\\user /add).", log);
        Assert.Equal("RESULT False", log[^1]);
    }

    /// <summary>Anything that is not a yes is a no, including a word that only starts like a no.</summary>
    [Theory]
    [InlineData("'n'")]
    [InlineData("'no'")]
    [InlineData("' '")]
    [InlineData("'maybe'")]
    public void AnythingButYesIsNo(string answer)
    {
        var log = RunOperatorStep($"Invoke-DarlingOperatorStep '{OperatorStepGroup}' '' $false 'C:\\x\\darling.json' 'C:\\s'", answer: answer);

        Assert.DoesNotContain(log, line => line.StartsWith("ADD ", StringComparison.Ordinal));
        Assert.Equal("RESULT False", log[^1]);
    }

    /// <summary>A yes adds the account to the group and gives it the direct grant, both for the account that was named in the
    /// prompt, and prints the one line that says it also has a direct grant and how to remove it.</summary>
    [Theory]
    [InlineData("'y'")]
    [InlineData("'Yes'")]
    public void AYesAddsTheAccountToTheGroup_AndGrantsItDirectly(string answer)
    {
        var log = RunOperatorStep($"Invoke-DarlingOperatorStep '{OperatorStepGroup}' '' $false 'C:\\x\\darling.json' 'C:\\s'", answer: answer);

        var guests = GuestsAccount();
        Assert.Contains($"ADD {OperatorStepGroup}|{guests}", log);
        Assert.Contains("GRANT S-1-5-32-546|C:\\x\\darling.json|C:\\s", log);
        Assert.Contains(log, line => line.Contains("also has a direct grant", StringComparison.Ordinal)
            && line.Contains("uninstall-darling.ps1 removes it", StringComparison.Ordinal)
            && line.Contains("/remove *S-1-5-32-546", StringComparison.Ordinal));
        Assert.Equal("RESULT True", log[^1]);
    }

    /// <summary>The person signed in to the session and the account the script runs as are not always the same one (someone
    /// elevated with another administrator account). The prompt names the signed-in person, and says so when they differ.</summary>
    [Fact]
    public void WhenTheSignedInAccountIsNotTheAccountTheScriptRunsAs_ThePromptSaysSo_AndStillDefaultsToNo()
    {
        var runAs = WindowsIdentity.GetCurrent().Name;
        var log = RunOperatorStep($"Invoke-DarlingOperatorStep '{OperatorStepGroup}' '' $false 'C:\\x\\darling.json' 'C:\\s'");

        Assert.Contains(log, line => line.StartsWith("You are signed in as CONTOSO\\alice, but this script runs as " + runAs, StringComparison.Ordinal)
            && line.Contains("separate decision", StringComparison.Ordinal));
        Assert.Contains($"PROMPT Add CONTOSO\\alice to '{OperatorStepGroup}'? [y/N]", log);
        Assert.DoesNotContain(log, line => line.StartsWith("ADD ", StringComparison.Ordinal));
        Assert.Equal("RESULT False", log[^1]);
    }

    /// <summary>When no desktop shell is found for the session the prompt names the account the script runs as, and has no
    /// "different account" sentence to say.</summary>
    [Fact]
    public void WhenTheSignedInAccountIsTheAccountTheScriptRunsAs_ThePromptHasNoDifferentAccountWording()
    {
        var runAs = WindowsIdentity.GetCurrent().Name;
        var log = RunOperatorStep($"Invoke-DarlingOperatorStep '{OperatorStepGroup}' '' $false 'C:\\x\\darling.json' 'C:\\s'", signedInUser: "$null");

        Assert.Contains($"PROMPT Add {runAs} to '{OperatorStepGroup}'? [y/N]", log);
        Assert.DoesNotContain(log, line => line.StartsWith("You are signed in as", StringComparison.Ordinal));
    }

    /// <summary>-NoOperatorPrompt never asks and never adds, and prints the existing closing text.</summary>
    [Fact]
    public void NoOperatorPromptNeverAsks()
    {
        var log = RunOperatorStep($"Invoke-DarlingOperatorStep '{OperatorStepGroup}' '' $true 'C:\\x\\darling.json' 'C:\\s'", answer: "'y'");

        Assert.DoesNotContain(log, line => line.StartsWith("PROMPT ", StringComparison.Ordinal) || line.StartsWith("ADD ", StringComparison.Ordinal) || line.StartsWith("GRANT ", StringComparison.Ordinal));
        Assert.Contains(log, line => line.Contains("A person added to the group has to sign out and back in", StringComparison.Ordinal));
        Assert.Equal("RESULT False", log[^1]);
    }

    /// <summary>-AddOperator adds the named account without asking, and gives it the direct grant.</summary>
    [Fact]
    public void AddOperatorAddsTheNamedAccountWithoutAsking()
    {
        var log = RunOperatorStep($"Invoke-DarlingOperatorStep '{OperatorStepGroup}' 'CONTOSO\\alice' $false 'C:\\x\\darling.json' 'C:\\s'", answer: "'n'");

        Assert.DoesNotContain(log, line => line.StartsWith("PROMPT ", StringComparison.Ordinal));
        Assert.Contains($"ADD {OperatorStepGroup}|{GuestsAccount()}", log);
        Assert.Contains("GRANT S-1-5-32-546|C:\\x\\darling.json|C:\\s", log);
        Assert.Equal("RESULT True", log[^1]);
    }

    /// <summary>A host nobody can answer on is never waited on: the answer is No, the group is not touched and the closing
    /// text prints.</summary>
    [Fact]
    public void AHostThatCannotTakeAnAnswerIsTreatedAsNo()
    {
        var log = RunOperatorStep($"Invoke-DarlingOperatorStep '{OperatorStepGroup}' '' $false 'C:\\x\\darling.json' 'C:\\s'", answer: "'y'", hostIsInteractive: "$false");

        Assert.DoesNotContain(log, line => line.StartsWith("PROMPT ", StringComparison.Ordinal) || line.StartsWith("ADD ", StringComparison.Ordinal) || line.StartsWith("GRANT ", StringComparison.Ordinal));
        Assert.Contains(log, line => line.Contains("the answer is No", StringComparison.Ordinal));
        Assert.Contains(log, line => line.Contains("A person added to the group has to sign out and back in", StringComparison.Ordinal));
        Assert.Equal("RESULT False", log[^1]);
    }

    /// <summary>The real host check, under a console started with -NonInteractive, says no.</summary>
    [Fact]
    public void TheHostCheckSaysNoUnderNonInteractive()
    {
        const string scriptName = "install-darling.ps1";
        var lines = RunWindowsPowerShell(ExtractFunction(Script(scriptName), "Test-DarlingRemoteSession", scriptName) + "\n" + ExtractFunction(Script(scriptName), "Test-DarlingInteractiveHost", scriptName) + "\nTest-DarlingInteractiveHost", nonInteractive: true);

        Assert.Equal(new[] { "False" }, lines);
    }

    /// <summary>A PowerShell remoting session can report UserInteractive as true with no console behind it, so the prompt
    /// would hang. The remoting check says yes for the remote host name and for a set $PSSenderInfo (either alone is enough),
    /// and no for an ordinary console. The real host check is then run with the sender info set, so it answers No whatever
    /// the console around it looks like.</summary>
    [Fact]
    public void ARemotingSessionIsTreatedAsAHostThatCannotAnswer()
    {
        const string scriptName = "install-darling.ps1";
        var probe = string.Join(
            "\n",
            ExtractFunction(Script(scriptName), "Test-DarlingRemoteSession", scriptName),
            "'host=' + (Test-DarlingRemoteSession 'ServerRemoteHost' $null)",
            "'sender=' + (Test-DarlingRemoteSession 'ConsoleHost' ([pscustomobject]@{ ConnectedUser = 'CONTOSO\\alice' }))",
            "'console=' + (Test-DarlingRemoteSession 'ConsoleHost' $null)",
            "$PSSenderInfo = [pscustomobject]@{ ConnectedUser = 'CONTOSO\\alice' }",
            ExtractFunction(Script(scriptName), "Test-DarlingInteractiveHost", scriptName),
            "'interactive=' + (Test-DarlingInteractiveHost)");

        var lines = RunWindowsPowerShell(probe);

        Assert.Equal(new[] { "host=True", "sender=True", "console=False", "interactive=False" }, lines);
    }

    /// <summary>The real account check: a person's own account resolves to its SID, and the names that would put every user
    /// inside the boundary (Everyone, BUILTIN\Users, Authenticated Users) and a name that resolves to nothing are refused.</summary>
    [Fact]
    public void OnlyAUserOrGroupAccountIsAcceptedAsAnOperator()
    {
        const string scriptName = "install-darling.ps1";
        var probe = string.Join(
            "\n",
            ExtractFunction(Script(scriptName), "Resolve-DarlingOperatorAccount", scriptName),
            "foreach ($name in @('Everyone', 'BUILTIN\\Users', 'NT AUTHORITY\\Authenticated Users', 'no-such-account-6478')) { try { $null = Resolve-DarlingOperatorAccount $name; \"$name=accepted\" } catch { \"$name=refused\" } }",
            "[System.Security.Principal.WindowsIdentity]::GetCurrent().Name | ForEach-Object { 'me=' + (Resolve-DarlingOperatorAccount $_).Value }");

        var lines = RunWindowsPowerShell(probe);

        Assert.Equal(
            new[]
            {
                "Everyone=refused",
                "BUILTIN\\Users=refused",
                "NT AUTHORITY\\Authenticated Users=refused",
                "no-such-account-6478=refused",
                "me=" + WindowsIdentity.GetCurrent().User!.Value,
            },
            lines);
    }

    /// <summary>The direct grant lands on exactly the paths the group gets, with the group's rights: read on darling.json,
    /// traverse only on the store folder, read on the admin and viewer credential files, and nothing else (the mcp and
    /// superuser credentials are not the operators'). A store that is not managed has darling.json alone.</summary>
    [Fact]
    public void TheDirectGrantTargetsAreTheGroupsOwn()
    {
        const string scriptName = "install-darling.ps1";
        var probe = string.Join(
            "\n",
            ExtractFunction(Script(scriptName), "Get-DarlingOperatorGrantTargets", scriptName),
            "Get-DarlingOperatorGrantTargets 'C:\\i\\darling.json' 'C:\\s' | ForEach-Object { $_.Path + '|' + $_.Rights }",
            "'--'",
            "Get-DarlingOperatorGrantTargets 'C:\\i\\darling.json' $null | ForEach-Object { $_.Path + '|' + $_.Rights }");

        var lines = RunWindowsPowerShell(probe);

        Assert.Equal(
            new[]
            {
                "C:\\i\\darling.json|(R)",
                "C:\\s|(S,X)",
                "C:\\s\\" + PerformanceMonitor.Darling.Service.DarlingManagedPostgres.AdminCredentialFileName + "|(R)",
                "C:\\s\\" + PerformanceMonitor.Darling.Service.DarlingManagedPostgres.ViewerCredentialFileName + "|(R)",
                "--",
                "C:\\i\\darling.json|(R)",
            },
            lines);
    }

    /// <summary>Uninstall leaves darling.json, the store folder and its credential files in place, and no service is left to
    /// rewrite their permissions, so a direct grant the installer gave (#6478) would stay. The uninstall function removes
    /// every direct Allow entry on those four paths except SYSTEM, Administrators, a service account and the operator group,
    /// and leaves inherited entries and denies alone. Run with stand-ins for the ACL read and for icacls, so no permission on
    /// this machine is changed.</summary>
    [Fact]
    public void UninstallRemovesTheDirectGrantFromEveryPathItLands()
    {
        const string scriptName = "uninstall-darling.ps1";
        var folder = Directory.CreateTempSubdirectory("darling-uninstall-6478-").FullName;
        try
        {
            var store = Path.Combine(folder, "store");
            Directory.CreateDirectory(store);
            var config = Path.Combine(folder, "darling.json");
            File.WriteAllText(config, "{}");
            var admin = Path.Combine(store, "pg-admin-credential.dpapi");
            var viewer = Path.Combine(store, "pg-viewer-credential.dpapi");
            File.WriteAllText(admin, "x");
            File.WriteAllText(viewer, "x");

            /* Performance Log Users stands in for the operator group (a group that exists on every machine, found under the
               BUILTIN domain in place of the machine name), so its entry must be kept. */
            var probe = string.Join(
                "\n",
                "$env:COMPUTERNAME = 'BUILTIN'",
                "$script:log = New-Object System.Collections.ArrayList",
                "function Write-Host { param($Object, $ForegroundColor) [void]$script:log.Add([string]$Object) }",
                "function New-Rule($sidText, [bool]$inherited, $type = 'Allow') { [pscustomobject]@{ IdentityReference = (New-Object System.Security.Principal.SecurityIdentifier $sidText); IsInherited = $inherited; AccessControlType = $type } }",
                "function Get-Acl { param($LiteralPath) [pscustomobject]@{ Access = @((New-Rule 'S-1-5-18' $false), (New-Rule 'S-1-5-32-544' $false), (New-Rule 'S-1-5-80-1-2-3-4-5' $false), (New-Rule 'S-1-5-32-559' $false), (New-Rule 'S-1-5-32-546' $false), (New-Rule 'S-1-5-32-545' $true), (New-Rule 'S-1-5-32-547' $false 'Deny')) } }",
                "function icacls.exe { [void]$script:log.Add('ICACLS ' + ($args -join ' ')); $global:LASTEXITCODE = 0 }",
                ExtractFunction(Script(scriptName), "Remove-DarlingDirectOperatorGrants", scriptName),
                $"Remove-DarlingDirectOperatorGrants '{config}' '{store}' 'Performance Log Users'",
                "$script:log");

            var lines = RunWindowsPowerShell(probe);

            Assert.Equal(
                new[] { config, store, admin, viewer }.Select(path => $"ICACLS {path} /remove:g *S-1-5-32-546").ToArray(),
                lines.Where(line => line.StartsWith("ICACLS ", StringComparison.Ordinal)).ToArray());
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    /// <summary>When the operator group's own SID cannot be resolved, the function cannot tell the group's entries from a
    /// direct grant, so it removes nothing from any of the four paths and says so by naming the group. The group lookup is made
    /// to throw by naming a group that does not exist; the stand-in ACL still carries entries (one of them the group's own).</summary>
    [Fact]
    public void UninstallRemovesNothingWhenTheOperatorGroupCannotBeResolved()
    {
        const string scriptName = "uninstall-darling.ps1";
        var folder = Directory.CreateTempSubdirectory("darling-uninstall-6478-").FullName;
        try
        {
            var store = Path.Combine(folder, "store");
            Directory.CreateDirectory(store);
            var config = Path.Combine(folder, "darling.json");
            File.WriteAllText(config, "{}");
            File.WriteAllText(Path.Combine(store, "pg-admin-credential.dpapi"), "x");
            File.WriteAllText(Path.Combine(store, "pg-viewer-credential.dpapi"), "x");

            var probe = string.Join(
                "\n",
                "$env:COMPUTERNAME = 'BUILTIN'",
                "$script:log = New-Object System.Collections.ArrayList",
                "function Write-Host { param($Object, $ForegroundColor) [void]$script:log.Add([string]$Object) }",
                "function New-Rule($sidText, [bool]$inherited, $type = 'Allow') { [pscustomobject]@{ IdentityReference = (New-Object System.Security.Principal.SecurityIdentifier $sidText); IsInherited = $inherited; AccessControlType = $type } }",
                "function Get-Acl { param($LiteralPath) [pscustomobject]@{ Access = @((New-Rule 'S-1-5-18' $false), (New-Rule 'S-1-5-32-544' $false), (New-Rule 'S-1-5-32-559' $false), (New-Rule 'S-1-5-32-546' $false)) } }",
                "function icacls.exe { [void]$script:log.Add('ICACLS ' + ($args -join ' ')); $global:LASTEXITCODE = 0 }",
                ExtractFunction(Script(scriptName), "Remove-DarlingDirectOperatorGrants", scriptName),
                $"Remove-DarlingDirectOperatorGrants '{config}' '{store}' 'No Such Operator Group 6478'",
                "$script:log");

            var lines = RunWindowsPowerShell(probe);

            Assert.DoesNotContain(lines, line => line.StartsWith("ICACLS ", StringComparison.Ordinal));
            Assert.Contains(lines, line => line.Contains("No Such Operator Group 6478", StringComparison.Ordinal) && line.Contains("left in place", StringComparison.Ordinal));
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    private const string OperatorGroupStandIn = "Performance Log Users";

    /// <summary>The comment the installer gives the group it makes, read from the installer's own group function, so a change to
    /// it there cannot leave the uninstall comparing against a different text.</summary>
    private static string InstallGroupComment()
    {
        var match = Regex.Match(
            ExtractFunction(Script("install-darling.ps1"), "Initialize-DarlingOperatorGroup", "install-darling.ps1"),
            @"\$comment = '([^']+)'");
        Assert.True(match.Success, "install-darling.ps1 no longer gives the group a comment in a single-quoted string");
        return match.Groups[1].Value;
    }

    /// <summary>The stand-ins shared by the group-removal cases: the machine name is BUILTIN so Performance Log Users (a group
    /// on every machine) is found as the operator group, the group's comment, the domain-controller check and net.exe are
    /// stubs, and every net.exe call and printed line goes to a log. Nothing on this machine is created or deleted.</summary>
    private static string GroupRemovalStandIns(string comment, bool domainController, int netExit) => string.Join(
        "\n",
        "$env:COMPUTERNAME = 'BUILTIN'",
        "$script:log = New-Object System.Collections.ArrayList",
        "function Write-Host { param($Object, $ForegroundColor) [void]$script:log.Add([string]$Object) }",
        $"function Test-DarlingDomainController {{ ${(domainController ? "true" : "false")} }}",
        $"function Get-DarlingLocalGroupComment {{ param($groupName) '{comment}' }}",
        $"function net.exe {{ [void]$script:log.Add('NET ' + ($args -join ' ')); $global:LASTEXITCODE = {netExit}; if ({netExit} -ne 0) {{ 'The group could not be deleted.' }} }}");

    /// <summary>Runs <c>Remove-DarlingOperatorGroup</c> from the uninstall script against the stand-ins and returns its log.</summary>
    private static List<string> RunGroupRemoval(string comment, bool domainController = false, int netExit = 0, string groupName = OperatorGroupStandIn)
    {
        const string scriptName = "uninstall-darling.ps1";
        var script = Script(scriptName);
        return RunWindowsPowerShell(string.Join(
            "\n",
            GroupRemovalStandIns(comment, domainController, netExit),
            ExtractFunction(script, "Get-DarlingNativeText", scriptName),
            ExtractFunction(script, "Remove-DarlingOperatorGroup", scriptName),
            $"Remove-DarlingOperatorGroup '{groupName}'",
            "$script:log"));
    }

    /// <summary>A group that still has the installer's comment was made by the install, so the purge deletes it with net.exe and
    /// says so.</summary>
    [Fact]
    public void PurgeRemovesTheGroupTheInstallMade()
    {
        var lines = RunGroupRemoval(InstallGroupComment());

        Assert.Equal(new[] { $"NET localgroup {OperatorGroupStandIn} /delete" }, lines.Where(line => line.StartsWith("NET ", StringComparison.Ordinal)).ToArray());
        Assert.Contains(lines, line => line.StartsWith("Removed the local group", StringComparison.Ordinal));
    }

    /// <summary>A group with any other comment, or none, was made by an administrator and is kept. The line says why and gives
    /// the command to remove it by hand.</summary>
    [Theory]
    [InlineData("Our own operators")]
    [InlineData("")]
    public void PurgeKeepsAGroupTheInstallDidNotMake(string comment)
    {
        var lines = RunGroupRemoval(comment);

        Assert.DoesNotContain(lines, line => line.StartsWith("NET ", StringComparison.Ordinal));
        Assert.Contains(lines, line => line.Contains("install did not create it", StringComparison.Ordinal) && line.Contains("net localgroup \"" + OperatorGroupStandIn + "\" /delete", StringComparison.Ordinal));
    }

    /// <summary>On a domain controller nothing is looked up or deleted: a group of that name there is a domain group.</summary>
    [Fact]
    public void PurgeNeverActsOnADomainController()
    {
        var lines = RunGroupRemoval(InstallGroupComment(), domainController: true);

        Assert.DoesNotContain(lines, line => line.StartsWith("NET ", StringComparison.Ordinal));
        Assert.Contains(lines, line => line.Contains("domain controller", StringComparison.Ordinal));
    }

    /// <summary>A group that is not on the machine (an earlier purge, or an install that never made it) is not an error and
    /// prints nothing.</summary>
    [Fact]
    public void PurgeDoesNothingWhenTheGroupIsAlreadyGone()
    {
        var lines = RunGroupRemoval(InstallGroupComment(), groupName: "No Such Operator Group 6953");

        Assert.Empty(lines);
    }

    /// <summary>When net.exe fails the function reports it with the command to run by hand and does not throw, so the rest of
    /// the purge goes on.</summary>
    [Fact]
    public void AFailureToRemoveTheGroupIsReported_AndDoesNotThrow()
    {
        var lines = RunGroupRemoval(InstallGroupComment(), netExit: 2);

        Assert.Contains(lines, line => line.StartsWith("Could not remove the local group", StringComparison.Ordinal) && line.Contains("net exit 2", StringComparison.Ordinal) && line.Contains("The group could not be deleted.", StringComparison.Ordinal));
    }

    /// <summary>Runs the script's own <c>if ($PurgeData) { ... }</c> block with the stand-ins and a temp folder as the store, so
    /// the choice between a plain uninstall, a purge and a purge that was not confirmed is the shipped one.</summary>
    private static List<string> RunPurgeBlock(bool purgeData, bool confirmPurge)
    {
        const string scriptName = "uninstall-darling.ps1";
        var script = Script(scriptName);
        var folder = Directory.CreateTempSubdirectory("darling-uninstall-6953-").FullName;
        try
        {
            var start = script.IndexOf("if ($PurgeData) {", StringComparison.Ordinal);
            Assert.True(start >= 0, $"{scriptName} no longer has the -PurgeData block");
            var block = ExtractFunction(script.Substring(start).Replace("if ($PurgeData) {", "function Invoke-PurgeBlock {", StringComparison.Ordinal), "Invoke-PurgeBlock", scriptName);

            return RunWindowsPowerShell(string.Join(
                "\n",
                GroupRemovalStandIns(InstallGroupComment(), domainController: false, netExit: 0),
                $"$PurgeData = ${(purgeData ? "true" : "false")}",
                $"$ConfirmPurge = ${(confirmPurge ? "true" : "false")}",
                "$MsiUninstall = $true",
                $"$dataRoot = '{folder}'",
                $"$operatorGroupName = '{OperatorGroupStandIn}'",
                ExtractFunction(script, "Get-DarlingNativeText", scriptName),
                ExtractFunction(script, "Test-DarlingPurgeConfirmed", scriptName),
                ExtractFunction(script, "Remove-DarlingOperatorGroup", scriptName),
                block,
                "if ($PurgeData) { Invoke-PurgeBlock }",
                "$script:log"));
        }
        finally
        {
            if (Directory.Exists(folder))
            {
                Directory.Delete(folder, recursive: true);
            }
        }
    }

    /// <summary>A plain uninstall keeps the group, so a reinstall gets back the same members.</summary>
    [Fact]
    public void APlainUninstallKeepsTheGroup()
    {
        var lines = RunPurgeBlock(purgeData: false, confirmPurge: true);

        Assert.DoesNotContain(lines, line => line.StartsWith("NET ", StringComparison.Ordinal));
    }

    /// <summary>A confirmed purge deletes the store and then the group.</summary>
    [Fact]
    public void AConfirmedPurgeRemovesTheStoreAndTheGroup()
    {
        var lines = RunPurgeBlock(purgeData: true, confirmPurge: true);

        Assert.Contains("Store, credentials, and logs deleted.", lines);
        Assert.Contains($"NET localgroup {OperatorGroupStandIn} /delete", lines);
    }

    /// <summary>A purge that was not confirmed leaves the store, so it leaves the group too.</summary>
    [Fact]
    public void APurgeThatIsNotConfirmedKeepsTheGroup()
    {
        var lines = RunPurgeBlock(purgeData: true, confirmPurge: false);

        Assert.Contains("Purge NOT confirmed - data left in place.", lines);
        Assert.DoesNotContain(lines, line => line.StartsWith("NET ", StringComparison.Ordinal));
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
    private static List<string> RunWindowsPowerShell(string script, bool nonInteractive = false)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-opgroup-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script);
        try
        {
            var startInfo = nonInteractive
                ? WindowsPowerShellStartInfo.Create("powershell.exe", $"-NoProfile -NonInteractive -ExecutionPolicy Bypass -File \"{path}\"")
                : WindowsPowerShellStartInfo.ForScript(path);
            using var process = Process.Start(startInfo);
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
