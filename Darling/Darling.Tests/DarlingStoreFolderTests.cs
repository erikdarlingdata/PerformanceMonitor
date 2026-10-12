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

    private static readonly string[] s_functions =
    {
        "Get-DarlingDataDirectory", "Get-DarlingStoreFolder", "Initialize-DarlingNativeStoreFolder", "Get-DarlingItemOwnerSid",
        "Get-DarlingStoreContentsProblem", "Get-DarlingItemOwnerProblem", "Initialize-DarlingStoreFolder",
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
public sealed class DarlingStoreContentsGateTests
{
    private static readonly SecurityIdentifier s_users = new(WellKnownSidType.BuiltinUsersSid, null);

    private static string NewStore(bool withLogs = true)
    {
        var store = Path.Combine(Path.GetTempPath(), "darling-contents-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(store);
        DarlingFileSecurity.HardenDirectory(store, allowInteractiveTraverse: false);
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

    [Theory]
    [InlineData("pg")]
    [InlineData("logs")]
    public void ADirectoryReadableByUsers_IsRefused(string name)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        try
        {
            Grant(Path.Combine(store, name), FileSystemRights.Read);
            var problem = Problem(store);
            Assert.NotNull(problem);
            Assert.Contains(name, problem, StringComparison.Ordinal);
            Assert.Contains("readable", problem, StringComparison.Ordinal);
        }
        finally
        {
            Remove(store);
        }
    }

    [Theory]
    [InlineData("pg")]
    [InlineData("logs")]
    public void ADirectoryUsersCanWriteTo_IsRefused(string name)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var store = NewStore();
        try
        {
            Grant(Path.Combine(store, name), FileSystemRights.WriteData | FileSystemRights.AppendData);
            var problem = Problem(store);
            Assert.NotNull(problem);
            Assert.Contains("can be changed by", problem, StringComparison.Ordinal);
        }
        finally
        {
            Remove(store);
        }
    }

    [Theory]
    [InlineData("pg.log")]
    [InlineData("server.key")]
    public void AFileReadableByUsers_IsRefused(string name)
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

            var problem = Problem(store);
            Assert.NotNull(problem);
            Assert.Contains(name, problem, StringComparison.Ordinal);
            Assert.Contains("readable", problem, StringComparison.Ordinal);
        }
        finally
        {
            Remove(store);
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
