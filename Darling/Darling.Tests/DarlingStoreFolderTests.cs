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
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Installer step 4b3 (the store folder is made, owned and locked before the service starts) and the matching check in
/// the service. The installer half runs the script's own functions under Windows PowerShell 5.1 against temp folders:
/// a link at the folder's name, a folder this account cannot list, items another user made, and a folder that cannot
/// be moved while the script holds it. The order of step 4b3 against the service start, and the branches that must not
/// be removed, are pinned in the script text.
/// </summary>
public sealed class DarlingStoreFolderScriptTests
{
    private static string Script => ReadRepoFile("Darling", "tools", "install-darling.ps1").Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string UpgradeScript => ReadRepoFile("Darling", "tools", "upgrade-darling.ps1").Replace("\r\n", "\n", StringComparison.Ordinal);

    private static readonly string[] s_functions =
    {
        "Get-DarlingDataDirectory", "Get-DarlingStoreFolder", "Initialize-DarlingNativeStoreFolder", "Get-DarlingItemOwnerSid",
        "Get-DarlingStoreContentsProblem", "Get-DarlingItemOwnerProblem", "Initialize-DarlingStoreFolder", "Get-DarlingServiceSids",
        "Get-DarlingNativeText", "Get-DarlingProductType", "Test-DarlingDomainController", "Initialize-DarlingOperatorGroup",
    };

    private static bool IsElevated() => new WindowsPrincipal(WindowsIdentity.GetCurrent()).IsInRole(WindowsBuiltInRole.Administrator);

    /// <summary>The folder is made, owned and locked after the install tree is locked and the operator group exists, and
    /// before the service is started; a refusal stops the install with the service still stopped.</summary>
    [Fact]
    public void TheStoreFolderStep_RunsBeforeTheServiceStarts_AndAStopMeansNoStart()
    {
        var script = Script;
        var treeLock = script.IndexOf("Invoke-InstallTreeLock $serviceAccount", StringComparison.Ordinal);
        var call = script.IndexOf("try { Initialize-DarlingStoreFolder $storeFolder $serviceAccount $operatorSid", StringComparison.Ordinal);
        var start = script.IndexOf("Start-Service -Name $serviceName", StringComparison.Ordinal);
        var operatorSid = script.IndexOf("$operatorSid =", StringComparison.Ordinal);

        Assert.True(treeLock >= 0 && call > treeLock, "the store folder is secured after the install tree is locked");
        Assert.True(operatorSid >= 0 && operatorSid < call, "the operator group's SID is known before the store folder is locked");
        Assert.True(start > call, "the store folder must be secured before the service is started");

        var refusal = script.Substring(call, script.IndexOf("Write-Host \"Store folder", call, StringComparison.Ordinal) - call);
        Assert.Contains("Fail @\"", refusal, StringComparison.Ordinal);
        Assert.Contains("The service was not started", refusal, StringComparison.Ordinal);
    }

    /// <summary>The branches of the store-folder code that must not go: a listing failure is never read as an empty
    /// folder, a link is refused before and after the write, the owner is judged, the contents are judged before and
    /// after the lock, and the owner and the DACL are written only through the handle that was checked.</summary>
    [Fact]
    public void TheStoreFolderFunctions_RefuseWhatTheyCannotListOrTrust_AndWriteOnlyThroughTheCheckedHandle()
    {
        var script = Script;
        var initialize = ExtractFunction(script, "Initialize-DarlingStoreFolder");
        var contents = ExtractFunction(script, "Get-DarlingStoreContentsProblem");

        Assert.DoesNotContain("SilentlyContinue", initialize, StringComparison.Ordinal);
        Assert.DoesNotContain("SilentlyContinue", contents, StringComparison.Ordinal);
        Assert.Contains("Get-ChildItem -LiteralPath $folder -Force -ErrorAction Stop", contents, StringComparison.Ordinal);
        Assert.Contains("It is not treated as empty.", contents, StringComparison.Ordinal);

        Assert.DoesNotContain("Set-Acl", initialize, StringComparison.Ordinal);
        Assert.DoesNotContain("icacls.exe", initialize, StringComparison.Ordinal);
        Assert.Contains("[DarlingNativeStoreFolder]::Create($full, $descriptor)", initialize, StringComparison.Ordinal);
        Assert.Contains("[DarlingNativeStoreFolder]::Open($full, $true)", initialize, StringComparison.Ordinal);
        Assert.Contains("$lock.WriteOwner(", initialize, StringComparison.Ordinal);
        Assert.Contains("$lock.WriteDacl(", initialize, StringComparison.Ordinal);

        var write = initialize.IndexOf("$lock.WriteDacl(", StringComparison.Ordinal);
        var before = initialize.IndexOf("& $assertSame 'before the permissions were changed'", StringComparison.Ordinal);
        var after = initialize.IndexOf("& $assertSame 'after the permissions were changed'", StringComparison.Ordinal);
        Assert.True(before >= 0 && before < write && write < after, "the link and real-path checks run before the write and again after it");
        Assert.Contains("$lock.IsReparsePoint", initialize, StringComparison.Ordinal);
        Assert.Contains("[DarlingNativeStoreFolder]::ResolvedPath($parent)", initialize, StringComparison.Ordinal);

        var firstContents = initialize.IndexOf("Get-DarlingStoreContentsProblem $full", StringComparison.Ordinal);
        var lastContents = initialize.LastIndexOf("Get-DarlingStoreContentsProblem $full", StringComparison.Ordinal);
        Assert.True(firstContents >= 0 && firstContents < write && lastContents > write, "the contents are judged before the DACL is written and again once it is in place");
        Assert.Contains("$trusted -notcontains $owner", initialize, StringComparison.Ordinal);
        Assert.Contains("already exists and is owned by", initialize, StringComparison.Ordinal);

        /* The names the service writes into are the ones judged. */
        foreach (var name in new[] { "pg.log", "server.key", "server.crt", "logs" })
        {
            Assert.Contains($"'{name}'", contents, StringComparison.Ordinal);
        }
    }

    /// <summary>The installer and the service must agree on which folder is the store folder.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(@"D:\Data\pg")]
    [InlineData(@"D:\Data\pg\")]
    [InlineData(@"E:\Monitor Store\Darling\data")]
    public void TheInstallersStoreFolder_IsTheParentOfTheDataDirectoryTheServiceResolves(string? dataDirectory)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Windows PowerShell and Windows paths");

        var json = dataDirectory is null
            ? "{ \"postgres\": { \"managed\": true } }"
            : "{ \"postgres\": { \"dataDirectory\": \"" + dataDirectory.Replace("\\", "\\\\", StringComparison.Ordinal) + "\" } }";
        var probe = string.Join("\n", ExtractFunctions(Script, "Get-DarlingDataDirectory", "Get-DarlingStoreFolder"), $$"""
            $path = Join-Path $env:TEMP ('darling-config-' + [guid]::NewGuid().ToString('N') + '.json')
            [System.IO.File]::WriteAllText($path, '{{json.Replace("'", "''", StringComparison.Ordinal)}}')
            try { 'folder=' + (Get-DarlingStoreFolder $path) } finally { Remove-Item -LiteralPath $path -Force }
            """);

        var answers = RunWindowsPowerShell(probe);

        var expected = Path.GetDirectoryName(DarlingManagedPostgres.ResolveDataDirectory(new PostgresConfig { DataDirectory = dataDirectory }));
        Assert.Contains("folder=" + expected, answers);
    }

    /// <summary>A link at the folder's name is refused before anything is changed, and what it points to keeps its
    /// permissions.</summary>
    [Fact]
    public void AJunctionAtTheStoreFolder_IsRefused_AndItsTargetIsNotTouched()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs and junctions are Windows-only.");

        var answers = RunWindowsPowerShell(Probe($$"""
            $real = Join-Path $base 'real'
            $link = Join-Path $base 'link'
            New-Item -ItemType Directory $real | Out-Null
            cmd /c mklink /J "$link" "$real" | Out-Null
            $before = (Get-Acl -LiteralPath $real).Sddl
            try { Initialize-DarlingStoreFolder $link $me $null ''; 'outcome=accepted' } catch { 'outcome=refused'; 'message=' + $_.Exception.Message }
            'unchanged=' + ((Get-Acl -LiteralPath $real).Sddl -eq $before)
            cmd /c rmdir "$link" | Out-Null
            """));

        Assert.Contains("outcome=refused", answers);
        Assert.Contains(answers, a => a.StartsWith("message=", StringComparison.Ordinal) && a.Contains("junction or link", StringComparison.Ordinal));
        Assert.Contains("unchanged=True", answers);
    }

    /// <summary>A file at the folder's name is refused, as it was before the folder was handled through a handle.</summary>
    [Fact]
    public void AFileAtTheStoreFolder_IsRefused()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Windows paths");

        var answers = RunWindowsPowerShell(Probe("""
            $file = Join-Path $base 'afile'
            Set-Content -LiteralPath $file 'x'
            try { Initialize-DarlingStoreFolder $file $me $null ''; 'outcome=accepted' } catch { 'outcome=refused'; 'message=' + $_.Exception.Message }
            """));

        Assert.Contains("outcome=refused", answers);
        Assert.Contains(answers, a => a.StartsWith("message=", StringComparison.Ordinal) && a.Contains("is a file", StringComparison.Ordinal));
    }

    /// <summary>A folder this account cannot list is refused, with the reason, and is never read as empty.</summary>
    [Fact]
    public void AFolderThatCannotBeListed_IsRefused_NeverTreatedAsEmpty()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var answers = RunWindowsPowerShell(Probe($$"""
            $folder = Join-Path $base 'denied'
            New-Item -ItemType Directory $folder | Out-Null
            Set-Content -LiteralPath (Join-Path $folder 'f.txt') 'x'
            $acl = Get-Acl -LiteralPath $folder
            $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($meSid, 'ListDirectory', 'Deny')))
            Set-Acl -LiteralPath $folder -AclObject $acl
            try { Initialize-DarlingStoreFolder $folder $me $null ''; 'outcome=accepted' } catch { 'outcome=refused'; 'message=' + $_.Exception.Message }
            $acl = Get-Acl -LiteralPath $folder
            $null = $acl.PurgeAccessRules($meSid)
            Set-Acl -LiteralPath $folder -AclObject $acl
            """));

        Assert.Contains("outcome=refused", answers);
        Assert.Contains(answers, a => a.StartsWith("message=", StringComparison.Ordinal) && a.Contains("cannot be listed", StringComparison.Ordinal) && a.Contains("not treated as empty", StringComparison.Ordinal));
    }

    /// <summary>The names the service writes into are refused when someone else owns them, when they are links, and when
    /// they cannot be listed; the same folder passes when the owner is trusted.</summary>
    [Fact]
    public void TheDataDirectoryPgLogServerKeyAndLogs_AreRefusedUnlessTrustedAndReadable()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs and junctions are Windows-only.");

        var answers = RunWindowsPowerShell(Probe($$"""
            $system = New-Object System.Security.Principal.SecurityIdentifier([System.Security.Principal.WellKnownSidType]::LocalSystemSid, $null)
            function New-Store([string]$name, [string[]]$items) {
                $store = Join-Path $base $name
                New-Item -ItemType Directory (Join-Path $store 'pg') | Out-Null
                Set-Content -LiteralPath (Join-Path $store 'pg\postgresql.auto.conf') 'x'
                foreach ($item in $items) { Set-Content -LiteralPath (Join-Path $store $item) 'x' }
                return $store
            }
            $owner = (Get-Acl -LiteralPath $base).GetOwner([System.Security.Principal.SecurityIdentifier])
            foreach ($item in @('pg.log', 'server.key', 'server.crt')) {
                $store = New-Store ('own-' + $item) @($item)
                "trusted-$item=" + [bool](Get-DarlingStoreContentsProblem $store (Join-Path $store 'pg') @($owner))
                "untrusted-$item=" + (Get-DarlingStoreContentsProblem $store (Join-Path $store 'pg') @($system))
            }
            $store = New-Store 'logs' @()
            New-Item -ItemType Directory (Join-Path $store 'logs') | Out-Null
            "logsOwner=" + (Get-DarlingStoreContentsProblem $store '' @($system))
            $store = New-Store 'linked' @()
            $target = Join-Path $base 'elsewhere'
            New-Item -ItemType Directory $target | Out-Null
            cmd /c mklink /J (Join-Path $store 'logs') "$target" | Out-Null
            "linked=" + (Get-DarlingStoreContentsProblem $store (Join-Path $store 'pg') @($owner, $system))
            $store = New-Store 'unlistable' @()
            $acl = Get-Acl -LiteralPath (Join-Path $store 'pg')
            $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($meSid, 'ListDirectory', 'Deny')))
            Set-Acl -LiteralPath (Join-Path $store 'pg') -AclObject $acl
            "unlistable=" + (Get-DarlingStoreContentsProblem $store (Join-Path $store 'pg') @($owner, $system))
            $acl = Get-Acl -LiteralPath (Join-Path $store 'pg')
            $null = $acl.PurgeAccessRules($meSid)
            Set-Acl -LiteralPath (Join-Path $store 'pg') -AclObject $acl
            cmd /c rmdir (Join-Path (Join-Path $base 'linked') 'logs') | Out-Null
            """));

        foreach (var item in new[] { "pg.log", "server.key", "server.crt" })
        {
            Assert.Contains($"trusted-{item}=False", answers);
            Assert.Contains(answers, a => a.StartsWith($"untrusted-{item}=", StringComparison.Ordinal) && a.Contains("is owned by", StringComparison.Ordinal));
        }

        Assert.Contains(answers, a => a.StartsWith("logsOwner=", StringComparison.Ordinal) && a.Contains(@"logs is owned by", StringComparison.Ordinal));
        Assert.Contains(answers, a => a.StartsWith("linked=", StringComparison.Ordinal) && a.Contains("is a junction or link", StringComparison.Ordinal));
        Assert.Contains(answers, a => a.StartsWith("unlistable=", StringComparison.Ordinal) && a.Contains("cannot be listed", StringComparison.Ordinal));
    }

    /// <summary>An existing folder whose owner is not trusted is refused, empty or not. The folder here is owned by
    /// the account running the test; an elevated account owns it as Administrators, which is trusted, so only a plain
    /// account can set this up.</summary>
    [Fact]
    public void AnExistingFolderOwnedByAnUntrustedAccount_IsRefused_EvenWhenEmpty()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        Assert.SkipWhen(IsElevated(), "an elevated account creates folders owned by Administrators, which is trusted");

        var answers = RunWindowsPowerShell(Probe($$"""
            $folder = Join-Path $base 'owned-by-me'
            New-Item -ItemType Directory $folder | Out-Null
            $before = (Get-Acl -LiteralPath $folder).Sddl
            try { Initialize-DarlingStoreFolder $folder 'NT AUTHORITY\NETWORK SERVICE' $null ''; 'outcome=accepted' } catch { 'outcome=refused'; 'message=' + $_.Exception.Message }
            'unchanged=' + ((Get-Acl -LiteralPath $folder).Sddl -eq $before)
            """));

        Assert.Contains("outcome=refused", answers);
        Assert.Contains(answers, a => a.StartsWith("message=", StringComparison.Ordinal) && a.Contains("already exists and is owned by", StringComparison.Ordinal));
        Assert.Contains("unchanged=True", answers);
    }

    /// <summary>A folder the script holds cannot be moved or replaced, so a link cannot be put at its name between the
    /// check and the write; once it is released it can.</summary>
    [Fact]
    public void AFolderTheScriptHolds_CannotBeMovedAwayUntilItIsReleased()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Windows file handles");

        var answers = RunWindowsPowerShell(Probe($$"""
            Initialize-DarlingNativeStoreFolder
            $folder = Join-Path $base 'held'
            New-Item -ItemType Directory $folder | Out-Null
            $lock = [DarlingNativeStoreFolder]::Open($folder, $false)
            'open=' + $lock.IsOpen
            'reparse=' + $lock.IsReparsePoint
            try { [System.IO.Directory]::Move($folder, $folder + '-moved'); 'moved=True' } catch { 'moved=False' }
            $lock.Dispose()
            try { [System.IO.Directory]::Move($folder, $folder + '-moved'); 'movedAfter=True' } catch { 'movedAfter=False' }
            """));

        Assert.Contains("open=True", answers);
        Assert.Contains("reparse=False", answers);
        Assert.Contains("moved=False", answers);
        Assert.Contains("movedAfter=True", answers);
    }

    /// <summary>A new folder is made with its DACL in the call that creates it and ends owned by Administrators with
    /// exactly the trusted principals. Needs an elevated account: only one can make Administrators the owner.</summary>
    [Fact]
    public void ANewStoreFolder_IsCreatedOwnedByAdministrators_WithExactlyTheTrustedAccess()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        Assert.SkipUnless(IsElevated(), "making Administrators the owner needs an elevated account");

        var answers = RunWindowsPowerShell(Probe($$"""
            $folder = Join-Path $base 'fresh\store'
            Initialize-DarlingStoreFolder $folder $me $null (Join-Path $folder 'pg')
            $acl = Get-Acl -LiteralPath $folder
            'owner=' + $acl.GetOwner([System.Security.Principal.SecurityIdentifier]).Value
            'protected=' + $acl.AreAccessRulesProtected
            'rules=' + ((@($acl.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier])) | ForEach-Object { $_.IdentityReference.Value } | Sort-Object -Unique) -join ',')
            'meSid=' + $meSid.Value
            Initialize-DarlingStoreFolder $folder $me $null (Join-Path $folder 'pg')
            'again=ok'
            """));

        Assert.Contains("owner=S-1-5-32-544", answers);
        Assert.Contains("protected=True", answers);
        var me = answers.Single(a => a.StartsWith("meSid=", StringComparison.Ordinal))[6..];
        var expected = string.Join(",", new[] { "S-1-5-18", "S-1-5-32-544", me }.Distinct().OrderBy(x => x, StringComparer.Ordinal));
        Assert.Contains("rules=" + expected, answers);
        Assert.Contains("again=ok", answers);
    }

    /// <summary>The owners the installer accepts on an existing store are SYSTEM, Administrators, TrustedInstaller, the
    /// account the service logs on as and this service's own service SID, the owner of everything the service made while
    /// it ran as the default virtual account. A store whose Log On account was changed to a domain account or a gMSA is
    /// owned by that SID, and the service trusts the same list, so the re-install and the upgrade accept it.</summary>
    [Fact]
    public void TheTrustedOwners_AreSystemAdministratorsTrustedInstallerTheServiceAccountAndTheServicesOwnSid()
    {
        var initialize = ExtractFunction(Script, "Initialize-DarlingStoreFolder");
        var line = initialize.Split('\n').Single(l => l.TrimStart().StartsWith("$trusted = @(", StringComparison.Ordinal));

        Assert.Contains("$systemSid", line, StringComparison.Ordinal);
        Assert.Contains("$adminsSid", line, StringComparison.Ordinal);
        Assert.Contains("$serviceSid", line, StringComparison.Ordinal);
        Assert.Contains("NT SERVICE\\TrustedInstaller", line, StringComparison.Ordinal);
        Assert.Contains("Get-DarlingServiceSids '' $serviceName", line, StringComparison.Ordinal);
        Assert.Contains("[string]$serviceName = ''", initialize.Split('\n')[0], StringComparison.Ordinal);

        /* Both scripts hand the function the service's name. */
        Assert.Contains("(Get-DarlingDataDirectory $configPath) $serviceName }", Script, StringComparison.Ordinal);
        Assert.Contains("(Get-DarlingDataDirectory $configPath) $serviceName\n", UpgradeScript, StringComparison.Ordinal);
    }

    /// <summary>The service SID the script trusts is the one Windows derives from the service's name: the SHA-1 of the
    /// upper-cased name in UTF-16LE, read as five little-endian integers under S-1-5-80. The service name is computed, not
    /// looked up, so it is the same whether or not the service is registered.</summary>
    [Fact]
    public void TheServicesOwnSid_IsTheOneWindowsDerivesFromTheServiceName()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Windows PowerShell");

        const string serviceName = "PerformanceMonitor Darling";
        var hash = System.Security.Cryptography.SHA1.HashData(System.Text.Encoding.Unicode.GetBytes(serviceName.ToUpperInvariant()));
        var expected = "S-1-5-80-" + string.Join("-", Enumerable.Range(0, 5).Select(i => BitConverter.ToUInt32(hash, i * 4)));

        var answers = RunWindowsPowerShell(Probe($$"""
            'sids=' + ((@(Get-DarlingServiceSids '' '{{serviceName}}') | ForEach-Object { $_.Value }) -join ',')
            'none=' + @(Get-DarlingServiceSids '' '').Count
            """));

        Assert.Contains("sids=" + expected, answers);
        Assert.Contains("none=0", answers);
    }

    /// <summary>A link at <c>pg_wal</c> among the data directory's entries is the operator's own relocation of the
    /// write-ahead log, which the service accepts; a link at any other name is refused.</summary>
    [Fact]
    public void ARelocatedPgWal_IsAccepted_AndEveryOtherLinkInTheDataDirectoryIsRefused()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs and junctions are Windows-only.");

        var answers = RunWindowsPowerShell(Probe($$"""
            $owner = (Get-Acl -LiteralPath $base).GetOwner([System.Security.Principal.SecurityIdentifier])
            $target = Join-Path $base 'elsewhere'
            New-Item -ItemType Directory $target | Out-Null
            function New-Store([string]$name, [string]$link) {
                $store = Join-Path $base $name
                New-Item -ItemType Directory (Join-Path $store 'pg') | Out-Null
                Set-Content -LiteralPath (Join-Path $store 'pg\postgresql.auto.conf') 'x'
                if ($link) { cmd /c mklink /J (Join-Path $store ('pg\' + $link)) "$target" | Out-Null }
                return $store
            }
            $store = New-Store 'wal' 'pg_wal'
            'walProblem=' + [bool](Get-DarlingStoreContentsProblem $store (Join-Path $store 'pg') @($owner))
            $store = New-Store 'walupper' 'PG_WAL'
            'walUpperProblem=' + [bool](Get-DarlingStoreContentsProblem $store (Join-Path $store 'pg') @($owner))
            $store = New-Store 'other' 'pg_other'
            'other=' + (Get-DarlingStoreContentsProblem $store (Join-Path $store 'pg') @($owner))
            $store = New-Store 'plain' ''
            'plainProblem=' + [bool](Get-DarlingStoreContentsProblem $store (Join-Path $store 'pg') @($owner))
            foreach ($junction in @('wal\pg\pg_wal', 'walupper\pg\PG_WAL', 'other\pg\pg_other')) { cmd /c rmdir (Join-Path $base $junction) | Out-Null }
            """));

        Assert.Contains("walProblem=False", answers);
        Assert.Contains("walUpperProblem=False", answers);
        Assert.Contains(answers, a => a.StartsWith("other=", StringComparison.Ordinal) && a.Contains("pg_other is a junction or link", StringComparison.Ordinal));
        Assert.Contains("plainProblem=False", answers);
    }

    /// <summary>On a domain controller (product type 2) the local operator group is neither looked up nor created, because
    /// net localgroup there makes a domain group; other product types still reach the lookup. The product type is
    /// faked by redefining the function that reads it, and net.exe by a function of the same name, so nothing is
    /// created whatever this machine is.</summary>
    [Fact]
    public void OnADomainController_NoLocalGroupIsLookedUpOrCreated_AndOtherProductTypesStillGetIt()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Windows PowerShell");

        var answers = RunWindowsPowerShell(Probe("""
            $script:netCalled = $false
            function net.exe { $script:netCalled = $true; $global:LASTEXITCODE = 0 }
            $adminsName = (New-Object System.Security.Principal.SecurityIdentifier('S-1-5-32-544')).Translate([System.Security.Principal.NTAccount]).Value.Split('\')[1]
            function Get-DarlingProductType { 2 }
            'domainController=' + (Test-DarlingDomainController)
            try { $null = Initialize-DarlingOperatorGroup 'PerformanceMonitor Darling Operators'; 'outcome=returned' }
            catch { 'outcome=refused'; 'message=' + $_.Exception.Message }
            'netCalledOnController=' + $script:netCalled
            foreach ($type in 0, 1, 3) {
                Set-Item -Path function:Get-DarlingProductType -Value ([scriptblock]::Create("$type"))
                "controller$type=" + (Test-DarlingDomainController)
            }
            $env:COMPUTERNAME = 'BUILTIN'
            'sid=' + (Initialize-DarlingOperatorGroup $adminsName).Value
            'netCalledElsewhere=' + $script:netCalled
            """));

        Assert.Contains("domainController=True", answers);
        Assert.Contains("outcome=refused", answers);
        Assert.Contains(answers, a => a.StartsWith("message=", StringComparison.Ordinal) && a.Contains("domain controller", StringComparison.Ordinal));
        Assert.Contains("netCalledOnController=False", answers);
        Assert.Contains("controller0=False", answers);
        Assert.Contains("controller1=False", answers);
        Assert.Contains("controller3=False", answers);
        Assert.Contains("sid=S-1-5-32-544", answers);
        Assert.Contains("netCalledElsewhere=False", answers);
    }

    /// <summary>The domain controller is decided before step 4 stops an existing service, so a skipped group can never
    /// leave the service stopped, and the install carries on without the group instead of stopping.</summary>
    [Fact]
    public void TheDomainControllerIsDecidedBeforeTheServiceIsStopped_AndTheInstallFinishesWithoutTheGroup()
    {
        var script = Script;
        var decide = script.IndexOf("$onDomainController = Test-DarlingDomainController", StringComparison.Ordinal);
        var stop = script.IndexOf("Stop-Service -Name $serviceName", StringComparison.Ordinal);
        var group = script.IndexOf("Initialize-DarlingOperatorGroup $operatorGroupName", StringComparison.Ordinal);

        Assert.True(decide >= 0 && decide < stop, "the domain controller is decided before step 4 stops the service");
        Assert.True(stop < group, "the group is made after the service was stopped, and only off a domain controller");
        Assert.Matches(@"if \(-not \$onDomainController\) \{\s+try \{ \$operatorSid = Initialize-DarlingOperatorGroup \$operatorGroupName \}", script);
        Assert.Contains("elseif ($onDomainController) {", script, StringComparison.Ordinal);

        var body = ExtractFunction(script, "Initialize-DarlingOperatorGroup");
        Assert.True(
            body.IndexOf("Test-DarlingDomainController", StringComparison.Ordinal) is var guard && guard >= 0
            && guard < body.IndexOf("& net.exe localgroup", StringComparison.Ordinal),
            "the function itself refuses a domain controller before it can run net localgroup");
    }

    /// <summary>The upgrade script locks the store folder with the installer's own functions (the same text, so they
    /// cannot drift), after the service has stopped and before it starts, so an upgrade from a release that could not
    /// lock the folder ends with it locked. A folder it cannot lock is a warning: the upgrade does not stop over it.</summary>
    [Fact]
    public void TheUpgradeLocksTheStoreFolder_WithTheInstallersFunctions_AfterTheServiceStopsAndBeforeItStarts()
    {
        var upgrade = UpgradeScript;
        foreach (var name in new[]
        {
            "Get-DarlingDataDirectory", "Get-DarlingStoreFolder", "Initialize-DarlingNativeStoreFolder", "Get-DarlingItemOwnerSid",
            "Get-DarlingStoreContentsProblem", "Get-DarlingItemOwnerProblem", "Initialize-DarlingStoreFolder", "Get-DarlingServiceSids",
            "Get-DarlingProductType", "Test-DarlingDomainController", "Initialize-DarlingOperatorGroup",
        })
        {
            Assert.Equal(ExtractFunction(Script, name), ExtractFunction(upgrade, name));
            Assert.Single(Regex.Matches(upgrade, @"(?m)^function " + Regex.Escape(name) + @"\b"));
        }

        var stop = upgrade.IndexOf("Stop-Service -Name $serviceName", StringComparison.Ordinal);
        var copied = upgrade.IndexOf("Invoke-UpgradeTreeLock 'after the copy'", StringComparison.Ordinal);
        var call = upgrade.IndexOf("Initialize-DarlingStoreFolder $storeFolder $logonAccount $operatorSid (Get-DarlingDataDirectory $configPath) $serviceName", StringComparison.Ordinal);
        var start = upgrade.IndexOf("Start-Service -Name $serviceName", StringComparison.Ordinal);
        Assert.True(stop >= 0 && stop < call, "the store folder is locked after the service has stopped");
        Assert.True(copied < call && call < start, "the store folder is locked after the new build is in place and before the service starts");
        Assert.Single(Regex.Matches(upgrade, @"(?<!function )Initialize-DarlingStoreFolder (\S+)"));

        var between = upgrade.Substring(call, start - call);
        Assert.Contains("Warn \"The store folder '$storeFolder' could not be secured", between, StringComparison.Ordinal);
        Assert.DoesNotContain("Fail ", between, StringComparison.Ordinal);
    }

    /// <summary>The upgrade skips the local operator group on a domain controller, with a warning, and goes on to start the
    /// service.</summary>
    [Fact]
    public void TheUpgradeSkipsTheOperatorGroupOnADomainController_AndStillStartsTheService()
    {
        var upgrade = UpgradeScript;
        var decide = upgrade.IndexOf("$onDomainController = Test-DarlingDomainController", StringComparison.Ordinal);
        var call = upgrade.IndexOf("Initialize-DarlingOperatorGroup $operatorGroupName", StringComparison.Ordinal);
        var start = upgrade.IndexOf("Start-Service -Name $serviceName", StringComparison.Ordinal);

        Assert.True(decide >= 0 && decide < call && call < start);
        Assert.Matches(@"if \(-not \$onDomainController\) \{\s+try \{ \$operatorSid = Initialize-DarlingOperatorGroup \$operatorGroupName \}", upgrade);
        Assert.Contains("elseif ($onDomainController) {\n    Warn \"This machine is a domain controller", upgrade, StringComparison.Ordinal);
    }

    private static string Probe(string body) => string.Join(
        "\n",
        ExtractFunctions(Script, s_functions),
        $$"""
        $base = Join-Path $env:TEMP ('darling-store-' + [guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory $base | Out-Null
        $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
        $meSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
        try {
        """,
        body,
        """
        }
        finally { Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue }
        """);

    private static string ExtractFunctions(string script, params string[] names) =>
        string.Join("\n", names.Select(name => ExtractFunction(script, name)));

    private static string ExtractFunction(string script, string name)
    {
        var start = script.IndexOf("function " + name + " ", StringComparison.Ordinal);
        if (start < 0)
        {
            start = script.IndexOf("function " + name + "(", StringComparison.Ordinal);
        }

        Assert.True(start >= 0, $"install-darling.ps1 no longer defines {name}");
        var open = script.IndexOf('{', start);
        var depth = 0;
        var inHereString = false;
        for (var i = open; i < script.Length; i++)
        {
            if (script.AsSpan(i).StartsWith("@'\n") || script.AsSpan(i).StartsWith("@\"\n"))
            {
                inHereString = true;
            }
            else if (inHereString && (script.AsSpan(i).StartsWith("\n'@") || script.AsSpan(i).StartsWith("\n\"@")))
            {
                inHereString = false;
            }

            if (inHereString)
            {
                continue;
            }

            if (script[i] == '{')
            {
                depth++;
            }
            else if (script[i] == '}' && --depth == 0)
            {
                return script.Substring(start, i - start + 1);
            }
        }

        Assert.Fail($"unbalanced braces while extracting {name}");
        return string.Empty;
    }

    private static List<string> RunWindowsPowerShell(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-store-{Guid.NewGuid():N}.ps1");
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

            /* A runner started from PowerShell 7 hands its module path down, and Windows PowerShell 5.1 then cannot load the
               security module (Get-Acl). Dropped, 5.1 builds its own. */
            startInfo.Environment.Remove("PSModulePath");
            using var process = Process.Start(startInfo);
            Assert.NotNull(process);
            var stdoutTask = process!.StandardOutput.ReadToEndAsync();
            var stderrTask = process.StandardError.ReadToEndAsync();
            var exited = process.WaitForExit(120_000);
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
            Assert.True(exited, $"powershell.exe did not exit within 120 seconds. Output so far:\n{stdout}\n{stderr}");
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

/// <summary>
/// The service's own check of the data directory, <c>pg.log</c>, <c>server.key</c>, <c>server.crt</c> and the log folder,
/// run before initdb and before the server starts on an existing store.
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingStoreContentsGateTests
{
    private static readonly SecurityIdentifier s_users = new(WellKnownSidType.BuiltinUsersSid, null);

    private static string NewStore(bool withLogs = true, bool lockStoreFolder = true)
    {
        var store = Path.Combine(Path.GetTempPath(), "darling-contents-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(store);
        if (lockStoreFolder)
        {
            DarlingFileSecurity.HardenDirectory(store, allowInteractiveTraverse: false);
        }

        Directory.CreateDirectory(Path.Combine(store, "pg"));
        File.WriteAllText(Path.Combine(store, "pg", "postgresql.conf"), "x");
        File.WriteAllText(Path.Combine(store, "pg.log"), "x");
        File.WriteAllText(Path.Combine(store, "server.key"), "x");
        if (withLogs)
        {
            Directory.CreateDirectory(Path.Combine(store, "logs"));
        }

        return store;
    }

    private static string? Problem(string store) =>
        DarlingManagedPostgres.StoreContentsProblem(store, Path.Combine(store, "pg"), Path.Combine(store, "pg.log"), Path.Combine(store, "logs"));

    private static void Remove(string store)
    {
        try { Directory.Delete(store, recursive: true); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Temp cleanup is best effort. */
        }
    }

    private static void Grant(string path, FileSystemRights rights)
    {
        var info = new DirectoryInfo(path);
        var security = info.GetAccessControl();
        security.AddAccessRule(new FileSystemAccessRule(s_users, rights, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));
        info.SetAccessControl(security);
    }

    [Fact]
    public void ALockedStoreOwnedByTheService_IsAccepted_WithOrWithoutAnExistingCluster()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        try
        {
            Assert.Null(Problem(store));
            Directory.Delete(Path.Combine(store, "pg"), recursive: true);
            Assert.Null(Problem(store));
        }
        finally
        {
            Remove(store);
        }
    }

    private static bool HasOrdinaryUserAccess(string path)
    {
        var broad = new[]
        {
            s_users,
            new SecurityIdentifier(WellKnownSidType.AuthenticatedUserSid, null),
            new SecurityIdentifier(WellKnownSidType.WorldSid, null),
            new SecurityIdentifier(WellKnownSidType.InteractiveSid, null),
        };
        FileSystemSecurity security = Directory.Exists(path) ? new DirectoryInfo(path).GetAccessControl() : new FileInfo(path).GetAccessControl();
        return security.GetAccessRules(true, true, typeof(SecurityIdentifier))
            .Cast<FileSystemAccessRule>()
            .Any(rule => rule.AccessControlType == AccessControlType.Allow && broad.Contains((SecurityIdentifier)rule.IdentityReference));
    }

    /// <summary>A directory the service owns that Users can read is given the protected ACL and accepted, and the
    /// entries below it lose the access they inherited.</summary>
    [Theory]
    [InlineData("pg")]
    [InlineData("logs")]
    public void ADirectoryReadableByUsers_IsTightened_AndAccepted(string name)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        try
        {
            var path = Path.Combine(store, name);
            Grant(path, FileSystemRights.Read);
            Assert.True(HasOrdinaryUserAccess(path));

            Assert.Null(Problem(store));

            Assert.False(HasOrdinaryUserAccess(path));
            Assert.True(new DirectoryInfo(path).GetAccessControl().AreAccessRulesProtected);
            if (name == "pg")
            {
                Assert.False(HasOrdinaryUserAccess(Path.Combine(path, "postgresql.conf")), "the entries below the data directory carry no inherited Users access");
            }
        }
        finally
        {
            Remove(store);
        }
    }

    [Theory]
    [InlineData("pg")]
    [InlineData("logs")]
    public void ADirectoryUsersCanWriteTo_IsTightened_AndAccepted(string name)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        try
        {
            var path = Path.Combine(store, name);
            Grant(path, FileSystemRights.WriteData | FileSystemRights.AppendData | FileSystemRights.ChangePermissions);
            Assert.NotNull(DarlingFileSecurity.StoreWriteAccessBeyondTrusted(path));

            Assert.Null(Problem(store));

            Assert.Null(DarlingFileSecurity.StoreWriteAccessBeyondTrusted(path));
        }
        finally
        {
            Remove(store);
        }
    }

    [Theory]
    [InlineData("pg.log")]
    [InlineData("server.key")]
    public void AFileReadableByUsers_IsTightened_AndAccepted(string name)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        try
        {
            var path = Path.Combine(store, name);
            var info = new FileInfo(path);
            var security = info.GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(s_users, FileSystemRights.Read, AccessControlType.Allow));
            info.SetAccessControl(security);
            Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(path));

            Assert.Null(Problem(store));

            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(path));
            Assert.True(new FileInfo(path).GetAccessControl().AreAccessRulesProtected);
        }
        finally
        {
            Remove(store);
        }
    }

    /// <summary>A store the previous release could not lock (its folder was left open to Users, so the data
    /// directory, pg.log, the keys and the log folder all inherited the read) is tightened item by item and starts.</summary>
    [Fact]
    public void AStoreThePreviousReleaseCouldNotLock_IsTightened_AndAccepted()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore(lockStoreFolder: false);
        try
        {
            File.WriteAllText(Path.Combine(store, "server.crt"), "x");
            Grant(store, FileSystemRights.Read);
            Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(Path.Combine(store, "pg")));
            Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(Path.Combine(store, "pg.log")));

            Assert.Null(Problem(store));

            foreach (var name in new[] { "pg", "pg.log", "server.key", "server.crt", "logs" })
            {
                Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(Path.Combine(store, name)), name);
            }

            Assert.False(HasOrdinaryUserAccess(Path.Combine(store, "pg", "postgresql.conf")));
        }
        finally
        {
            Remove(store);
        }
    }

    /// <summary>The rewrite is refused, with the reason, when it cannot be done (the service has no right to change
    /// the ACL of an item an administrator made), and when it reports success but the access is still there.</summary>
    [Theory]
    [InlineData("pg")]
    [InlineData("pg.log")]
    [InlineData("logs")]
    public void ARewriteThatFails_IsRefused_WithTheReason(string name)
    {
        const string reason = "access is denied";
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        try
        {
            var path = Path.Combine(store, name);
            if (Directory.Exists(path))
            {
                Grant(path, FileSystemRights.Read);
            }
            else
            {
                var info = new FileInfo(path);
                var security = info.GetAccessControl();
                security.AddAccessRule(new FileSystemAccessRule(s_users, FileSystemRights.Read, AccessControlType.Allow));
                info.SetAccessControl(security);
            }

            var problem = DarlingManagedPostgres.StoreContentsProblem(
                store, Path.Combine(store, "pg"), Path.Combine(store, "pg.log"), Path.Combine(store, "logs"),
                (item, isDirectory, anchor) => item == path ? reason : null);

            Assert.NotNull(problem);
            Assert.Contains(path, problem, StringComparison.Ordinal);
            Assert.Contains("could not be restricted", problem, StringComparison.Ordinal);
            Assert.Contains(reason, problem, StringComparison.Ordinal);
        }
        finally
        {
            Remove(store);
        }
    }

    [Fact]
    public void ARewriteThatChangesNothing_IsRefused()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        try
        {
            Grant(Path.Combine(store, "pg"), FileSystemRights.Read);

            var problem = DarlingManagedPostgres.StoreContentsProblem(
                store, Path.Combine(store, "pg"), Path.Combine(store, "pg.log"), Path.Combine(store, "logs"),
                (item, isDirectory, anchor) => null);

            Assert.NotNull(problem);
            Assert.Contains("still is after its permissions were restricted", problem, StringComparison.Ordinal);
        }
        finally
        {
            Remove(store);
        }
    }

    /// <summary>An install whose Log On account was changed still has the store owned by, and granting, the account
    /// Windows derives from the service name. It is accepted while the service runs as another account.</summary>
    [Fact]
    public void AStoreOwnedByTheServicesOwnAccount_IsAccepted_WhileRunningAsAnotherAccount()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        var me = WindowsIdentity.GetCurrent().User!;
        var someoneElse = new SecurityIdentifier(WellKnownSidType.NetworkServiceSid, null);
        try
        {
            if (!TryOwnEverything(store, me))
            {
                return;
            }

            try
            {
                DarlingFileSecurity.UseStoreTrustForTests(runningAccount: someoneElse, ownServiceSid: me);
                Assert.Null(Problem(store));
            }
            finally
            {
                DarlingFileSecurity.UseStoreTrustForTests(null, null);
            }
        }
        finally
        {
            Remove(store);
        }
    }

    /// <summary>The same store, owned by an ordinary user: neither the running account nor the service's own.</summary>
    [Fact]
    public void AStoreAnOrdinaryUserOwns_IsStillRefused_WhileRunningAsAnotherAccount()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        var me = WindowsIdentity.GetCurrent().User!;
        var someoneElse = new SecurityIdentifier(WellKnownSidType.NetworkServiceSid, null);
        try
        {
            if (!TryOwnEverything(store, me))
            {
                return;
            }

            try
            {
                DarlingFileSecurity.UseStoreTrustForTests(runningAccount: someoneElse, ownServiceSid: null);
                var problem = Problem(store);
                Assert.NotNull(problem);
                Assert.Contains("is not owned by", problem, StringComparison.Ordinal);
            }
            finally
            {
                DarlingFileSecurity.UseStoreTrustForTests(null, null);
            }
        }
        finally
        {
            Remove(store);
        }
    }

    /// <summary>The service-name account is derived the way Windows derives it: the same function gives
    /// TrustedInstaller's account from its name, and the name this service runs under is the one the installer uses.</summary>
    [Fact]
    public void TheServiceNameAccount_IsDerivedTheWayWindowsDerivesIt()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "NT SERVICE accounts are Windows-only.");
        var windows = (SecurityIdentifier)new NTAccount(@"NT SERVICE\TrustedInstaller").Translate(typeof(SecurityIdentifier));
        Assert.Equal(windows, DarlingFileSecurity.ServiceSidFor("TrustedInstaller"));
        Assert.Equal(windows, DarlingFileSecurity.ServiceSidFor("trustedinstaller"));
        Assert.Equal("PerformanceMonitor Darling", DarlingCliCommands.ServiceName);
        Assert.Equal(
            "S-1-5-80-3826879063-3043247242-2228279017-3709363858-944500918",
            DarlingFileSecurity.ServiceSidFor(DarlingCliCommands.ServiceName).Value);
    }

    /// <summary>Makes <paramref name="me"/> the owner of every item the gate judges; false (the test passes without
    /// checking anything) when this account may not.</summary>
    private static bool TryOwnEverything(string store, SecurityIdentifier me)
    {
        try
        {
            foreach (var name in new[] { "pg", "pg.log", "server.key", "logs", "pg/postgresql.conf" })
            {
                var path = Path.Combine(store, name.Replace('/', Path.DirectorySeparatorChar));
                if (Directory.Exists(path))
                {
                    var info = new DirectoryInfo(path);
                    var security = info.GetAccessControl();
                    security.SetOwner(me);
                    info.SetAccessControl(security);
                }
                else
                {
                    var info = new FileInfo(path);
                    var security = info.GetAccessControl();
                    security.SetOwner(me);
                    info.SetAccessControl(security);
                }
            }

            return true;
        }
        catch (Exception ex) when (ex is InvalidOperationException or UnauthorizedAccessException or IOException)
        {
            Assert.Skip("this account may not make itself the owner: " + ex.Message);
            return false;
        }
    }

    /// <summary>A link where the data directory or a file should be is refused, wherever it points.</summary>
    [Theory]
    [InlineData("pg")]
    [InlineData("logs")]
    public void ALinkInPlaceOfAnItem_IsRefused(string name)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "junctions are Windows-only.");
        var store = NewStore();
        var target = Path.Combine(Path.GetTempPath(), "darling-contents-target-" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(target);
            Directory.Delete(Path.Combine(store, name), recursive: true);
            using var process = Process.Start(new ProcessStartInfo("cmd.exe", $"/c mklink /J \"{Path.Combine(store, name)}\" \"{target}\"")
            {
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
            });
            process!.StandardOutput.ReadToEnd();
            process.WaitForExit();

            var problem = Problem(store);
            Assert.NotNull(problem);
            Assert.Contains("junction or symbolic link", problem, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(Path.Combine(store, name)); }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                /* The junction is already gone. */
            }

            Remove(store);
            Remove(target);
        }
    }

    /// <summary>A data directory this account cannot list is refused with the reason, not read as an empty one.</summary>
    [Fact]
    public void ADataDirectoryThatCannotBeListed_IsRefused_NeverTreatedAsEmpty()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        var data = Path.Combine(store, "pg");
        var me = WindowsIdentity.GetCurrent().User!;
        try
        {
            var info = new DirectoryInfo(data);
            var security = info.GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(me, FileSystemRights.ListDirectory, AccessControlType.Deny));
            info.SetAccessControl(security);

            var problem = Problem(store);
            Assert.NotNull(problem);
            Assert.Contains("could not be listed", problem, StringComparison.Ordinal);

            security = info.GetAccessControl();
            security.PurgeAccessRules(me);
            info.SetAccessControl(security);
        }
        finally
        {
            Remove(store);
        }
    }

    /// <summary>An entry of the data directory, the directory, a file or the log folder that someone else owns is refused.
    /// Needs an account that may give an object to another owner, so a plain account skips.</summary>
    [Theory]
    [InlineData("pg")]
    [InlineData("pg.log")]
    [InlineData("server.key")]
    [InlineData("logs")]
    [InlineData("pg/postgresql.conf")]
    public void AnItemAnotherUserOwns_IsRefused(string name)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        try
        {
            var path = Path.Combine(store, name.Replace('/', Path.DirectorySeparatorChar));
            try
            {
                if (Directory.Exists(path))
                {
                    var info = new DirectoryInfo(path);
                    var security = info.GetAccessControl();
                    security.SetOwner(s_users);
                    info.SetAccessControl(security);
                }
                else
                {
                    var info = new FileInfo(path);
                    var security = info.GetAccessControl();
                    security.SetOwner(s_users);
                    info.SetAccessControl(security);
                }
            }
            catch (Exception ex) when (ex is InvalidOperationException or UnauthorizedAccessException or IOException)
            {
                Assert.Skip("this account may not hand an object to another owner: " + ex.Message);
            }

            var problem = Problem(store);
            Assert.NotNull(problem);
            Assert.Contains("is not owned by", problem, StringComparison.Ordinal);
        }
        finally
        {
            Remove(store);
        }
    }

    /// <summary>The check runs on every start, before initdb on a new store and before the server is started on an
    /// existing one, so it sits ahead of both branches.</summary>
    [Fact]
    public void TheGate_RunsOnceInEnsureRunning_BeforeInitdbAndBeforeAnyServerStart()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedPostgres.cs");
        var harden = source.IndexOf("var storeFolderLocked = TryHardenDirectory(ParentOf(_dataDirectory));", StringComparison.Ordinal);
        var gate = source.IndexOf("RefuseUntrustedStoreContents(ParentOf(_dataDirectory));", StringComparison.Ordinal);
        var existing = source.IndexOf("var existingCluster = File.Exists(", StringComparison.Ordinal);
        var initdb = source.IndexOf("await InitializeClusterAsync(binDirectory, cancellationToken);", StringComparison.Ordinal);
        var major = source.IndexOf("await EnsureDataDirectoryMajorAsync(binDirectory, networkPlan, cancellationToken);", StringComparison.Ordinal);
        var start = source.IndexOf("await StartServerAsync(binDirectory, networkPlan.Value, cancellationToken);", StringComparison.Ordinal);

        Assert.True(harden >= 0 && gate > harden, "the store contents are judged after the store folder is locked");
        Assert.True(gate < existing && gate < initdb && gate < major && gate < start, "the check precedes initdb, the major upgrade and every server start");
        Assert.Single(Regex.Matches(source, @"RefuseUntrustedStoreContents\(ParentOf"));
    }
}
