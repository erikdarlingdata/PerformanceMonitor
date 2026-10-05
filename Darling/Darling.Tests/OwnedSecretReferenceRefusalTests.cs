/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A server's password may be an <c>env:</c> / <c>file:</c> reference, but never one that points at Darling's own
/// configuration or secrets. The rule lives in the shared core, so the MCP tool is covered as well as the web route.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class OwnedSecretReferenceRefusalTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "darling-ref-" + Guid.NewGuid().ToString("N"));
    private readonly DarlingOwnedSet _before = DarlingOwnedSecrets.Current;
    private readonly string _owned;

    public OwnedSecretReferenceRefusalTests()
    {
        Directory.CreateDirectory(Path.Combine(_root, "own"));
        Directory.CreateDirectory(Path.Combine(_root, "other"));
        _owned = Path.Combine(_root, "own");
        File.WriteAllText(Path.Combine(_owned, "secret.txt"), "x");
        DarlingOwnedSecrets.Set(new DarlingOwnedSet(new[] { _owned }, new[] { "DARLING_CONFIG" }));
    }

    public void Dispose()
    {
        DarlingOwnedSecrets.Set(_before);
        try
        {
            Directory.Delete(_root, true);
        }
        catch (IOException)
        {
            /* best effort */
        }
    }

    private static string? Check(string? password) => DarlingOwnedSecrets.ReferenceRefusal(password);

    private static string Body(string password) =>
        JsonSerializer.Serialize(new[] { new { host = "sql01", auth = "SQL", username = "monitor", password } });

    [Fact]
    public void TheMcpCore_RefusesAnOwnedFileAndAnOwnedEnvReference()
    {
        foreach (var pw in new[] { "file:" + Path.Combine(_owned, "secret.txt"), "env:DARLING_CONFIG" })
        {
            var (entries, invalid, whole) = DarlingMcpServerAdminTools.ParseRequest(Body(pw), isWindows: false);
            Assert.Null(whole);
            Assert.Empty(entries);
            var r = Assert.Single(invalid);
            Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, r.Detail);
        }
    }

    [Fact]
    public void TheMcpCore_AcceptsAnUnrelatedReference_AndLeavesLiteralsAlone()
    {
        foreach (var pw in new[] { "file:" + Path.Combine(_root, "other", "pw"), "env:SOME_OTHER_VAR" })
        {
            var (entries, invalid, _) = DarlingMcpServerAdminTools.ParseRequest(Body(pw), isWindows: false);
            Assert.Empty(invalid);
            Assert.Single(entries);
        }

        Assert.Null(Check("hunter2"));
        Assert.Null(Check(""));
        Assert.Null(Check(null));
        Assert.Null(Check("   "));
    }

    [Fact]
    public void AnUnrelatedReference_IsAccepted()
    {
        Assert.Null(Check("env:SOME_OTHER_VAR"));
        Assert.Null(Check("file:" + Path.Combine(_root, "other", "pw")));
        Assert.Null(Check("file:" + _owned + "-sibling" + Path.DirectorySeparatorChar + "pw"));
    }

    [Fact]
    public void TheOwnedDirectoryItselfAndEverythingUnderIt_IsRefused()
    {
        Assert.NotNull(Check("file:" + _owned));
        Assert.NotNull(Check("file:" + Path.Combine(_owned, "missing", "deep.txt")));
    }

    [Fact]
    public void EnvNames_CompareOrdinally_OnThisPlatform()
    {
        Assert.NotNull(Check("env:DARLING_CONFIG"));
        Assert.Equal(OperatingSystem.IsWindows(), Check("env:darling_config") != null);
    }

    [Theory]
    [InlineData("relative/path")]
    [InlineData("~/secret")]
    [InlineData("/etc/../etc/hostname")]
    [InlineData("//server/share/x")]
    [InlineData("\\\\server\\share\\x")]
    [InlineData("\\\\?\\C:\\x")]
    [InlineData("\\\\.\\C:\\x")]
    [InlineData("C:\\x\\f.txt:stream")]
    [InlineData("/proc/self/root/etc/hostname")]
    [InlineData("/sys/kernel/x")]
    public void EachAliasForm_IsRefused_WithTheOneSentence(string path)
    {
        Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, Check("file:" + path));
    }

    [Fact]
    public void ASymlinkIntoTheOwnedDirectory_IsRefused()
    {
        var link = Path.Combine(_root, "other", "alias");
        try
        {
            Directory.CreateSymbolicLink(link, _owned);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            return;
        }

        Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, Check("file:" + Path.Combine(link, "secret.txt")));
    }

    [Fact]
    public void ASymlinkCycle_IsRefused()
    {
        var a = Path.Combine(_root, "other", "a");
        var b = Path.Combine(_root, "other", "b");
        try
        {
            Directory.CreateSymbolicLink(a, b);
            Directory.CreateSymbolicLink(b, a);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            return;
        }

        Assert.NotNull(Check("file:" + Path.Combine(a, "x")));
    }

    [Fact]
    public void EveryRefusal_IsOneSentence_NamingNoPathAndNoVariable()
    {
        var all = new[] { "file:" + _owned, "env:DARLING_CONFIG", "file:~/x", "file:/proc/1/environ" }
            .Select(Check).Distinct().ToList();
        var text = Assert.Single(all);
        Assert.DoesNotContain(_owned, text, StringComparison.Ordinal);
        Assert.DoesNotContain("DARLING_CONFIG", text, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryEnvironmentVariableTheServiceReads_IsInTheOwnedSet()
    {
        var repo = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(ThisFile())!, ".."));
        var missing = new List<string>();
        foreach (var dir in new[] { "PerformanceMonitor.Darling.Service", "PerformanceMonitor.Darling.Storage" })
        {
            foreach (var file in Directory.EnumerateFiles(Path.Combine(repo, dir), "*.cs", SearchOption.AllDirectories)
                .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)))
            {
                var text = File.ReadAllText(file);
                foreach (Match m in Regex.Matches(text, "GetEnvironmentVariable\\(\\s*\"([^\"]+)\""))
                {
                    if (!DarlingOwnedSecrets.ServiceEnvNames.Contains(m.Groups[1].Value))
                    {
                        missing.Add(m.Groups[1].Value);
                    }
                }

                foreach (Match m in Regex.Matches(text, "GetEnvironmentVariable\\(\\s*([A-Z][A-Za-z0-9_]*)\\s*[,)]"))
                {
                    var lit = Regex.Match(text, "\\b" + m.Groups[1].Value + "\\s*=\\s*\"([^\"]+)\"");
                    if (lit.Success && !DarlingOwnedSecrets.ServiceEnvNames.Contains(lit.Groups[1].Value))
                    {
                        missing.Add(lit.Groups[1].Value);
                    }
                }
            }
        }

        Assert.Empty(missing.Distinct());
    }

    private static string ThisFile([CallerFilePath] string path = "") => path;

    [Fact]
    public void AnUnpopulatedSet_RefusesEveryReference_AndLeavesLiteralsAlone()
    {
        DarlingOwnedSecrets.Set(null!);

        Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, Check("file:" + Path.Combine(_root, "other", "pw")));
        Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, Check("env:SOME_OTHER_VAR"));
        Assert.Null(Check("hunter2"));
        Assert.Null(Check(null));
    }

    [Fact]
    public void ASetThatWasPopulatedAndOwnsNothing_IsNotTheUnpopulatedSet()
    {
        DarlingOwnedSecrets.Set(DarlingOwnedSet.Empty);

        Assert.Null(Check("env:SOME_OTHER_VAR"));
    }

    private static bool TryHardLink(string link, string target)
    {
        try
        {
            var psi = OperatingSystem.IsWindows()
                ? new System.Diagnostics.ProcessStartInfo("cmd.exe", $"/c mklink /H \"{link}\" \"{target}\"")
                : new System.Diagnostics.ProcessStartInfo("ln", $"\"{target}\" \"{link}\"");
            psi.RedirectStandardOutput = true;
            psi.RedirectStandardError = true;
            using var p = System.Diagnostics.Process.Start(psi)!;
            p.WaitForExit();
            return p.ExitCode == 0 && File.Exists(link);
        }
        catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException)
        {
            return false;
        }
    }

    [Fact]
    public void AHardLinkToAnOwnedFile_PlacedOutsideTheOwnedDirectory_IsAccepted()
    {
        /* Deliberate scope decision: the check compares identity of the owned files themselves, not of every file under
           the owned directories (thousands in the database data directory, on an interactive click). Making a hard link
           needs local read access to the target, which already defeats this control, so the scan bought nothing. */
        var link = Path.Combine(_root, "other", "linked.txt");
        Assert.True(TryHardLink(link, Path.Combine(_owned, "secret.txt")), "could not create a hard link on this machine");

        Assert.Null(Check("file:" + link));
    }

    [Fact]
    public void ACaseVariantOfAnOwnedPath_IsRefused_WhereTheVolumeIgnoresCase()
    {
        var owned = Path.Combine(_owned, "secret.txt");
        var variant = Path.Combine(_root.ToUpperInvariant(), "OWN", "SECRET.TXT");
        if (!File.Exists(variant))
        {
            return; /* a case-sensitive volume: the variant is a different (missing) file */
        }

        Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, Check("file:" + variant));
        Assert.NotNull(owned);
    }

    [Fact]
    public void AnAliasWhoseTextDiffersButWhoseIdentityMatches_IsRefused_LikeAWindowsShortName()
    {
        /* PROGRA~3 and "Program Files" name the same directory on Windows but share no text; that cannot be created on
           this machine, so the comparison is exercised with a stand-in identity source. */
        /* Every path is put through Path.GetFullPath, as DarlingOwnedSecrets.RealPath does, so the stand-in identity map is
           keyed on what the code looks up on this platform: on Windows "/data/x" is rooted to the current drive. */
        static string Key(string p) => Path.GetFullPath(p).Replace('\\', '/').TrimEnd('/');
        var comparer = OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
        var ownedDir = Path.GetFullPath("/data/Program Files/darling");
        var shortDir = Key("/data/PROGRA~3/darling");
        var longDir = Key("/data/Program Files/darling");
        var owned = new DarlingOwnedSet(new[] { ownedDir }, Array.Empty<string>());
        FileId? Identity(string p) =>
            comparer.Equals(Key(p), longDir) || comparer.Equals(Key(p), shortDir) ? new FileId(7, 42) : null;

        Assert.Equal(
            DarlingOwnedSecrets.ReferenceRefusalText,
            DarlingOwnedSecrets.ReferenceRefusal("file:/data/PROGRA~3/darling/secret.txt", owned, Identity));
        Assert.Null(DarlingOwnedSecrets.ReferenceRefusal("file:/data/PROGRA~4/other/secret.txt", owned, Identity));
    }

    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    private static extern Microsoft.Win32.SafeHandles.SafeFileHandle CreateFileW(string name, uint access, uint share, IntPtr sec, uint disposition, uint flags, IntPtr template);

    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
    private static extern bool SetFileShortNameW(Microsoft.Win32.SafeHandles.SafeFileHandle h, string shortName);

    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    private static extern uint GetShortPathNameW(string longPath, System.Text.StringBuilder buf, uint size);

    private static bool TrySetShortNameWithApi(string file, string shortName)
    {
        const uint genericRead = 0x80000000, genericWrite = 0x40000000, delete = 0x00010000, shareAll = 7, openExisting = 3, backupSemantics = 0x02000000;
        using var h = CreateFileW(file, genericRead | genericWrite | delete, shareAll, IntPtr.Zero, openExisting, backupSemantics, IntPtr.Zero);
        return !h.IsInvalid && SetFileShortNameW(h, shortName);
    }

    private static bool TrySetShortNameWithFsutil(string file, string shortName)
    {
        try
        {
            var psi = new System.Diagnostics.ProcessStartInfo("fsutil.exe", $"file setshortname \"{file}\" {shortName}")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            using var p = System.Diagnostics.Process.Start(psi)!;
            p.StandardOutput.ReadToEnd();
            p.StandardError.ReadToEnd();
            p.WaitForExit();
            return p.ExitCode == 0;
        }
        catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException)
        {
            return false;
        }
    }

    [Fact]
    public void AFileGivenAnExplicit83ShortName_IsRefusedByItsShortPath_AsWellAsItsLongPath()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "8.3 short names exist only on Windows (NTFS); the identity comparison is covered by the stand-in test elsewhere.");

        const string shortName = "LONGFI~9.TXT";
        var dir = Path.Combine(_root, "shortname");
        Directory.CreateDirectory(dir);
        try
        {
            var longFile = Path.Combine(dir, "a-deliberately-long-secret-name.txt");
            File.WriteAllText(longFile, "x");

            /* Create the short name ourselves: the volume's auto-generation setting cannot be read without elevation,
               and a test that relied on it would prove nothing. */
            var made = TrySetShortNameWithApi(longFile, shortName) || TrySetShortNameWithFsutil(longFile, shortName);
            Assert.True(made, "could not give the file an 8.3 short name (SetFileShortNameW and fsutil both failed); this test cannot prove anything here");

            var buf = new System.Text.StringBuilder(520);
            var n = GetShortPathNameW(longFile, buf, (uint)buf.Capacity);
            Assert.True(n > 0 && n < buf.Capacity, "GetShortPathName failed");
            Assert.Equal(shortName, Path.GetFileName(buf.ToString()), ignoreCase: true);

            var shortPath = Path.Combine(dir, shortName);
            Assert.NotEqual(longFile, shortPath);
            Assert.True(File.Exists(shortPath), "the short path does not reach the file");

            /* The owned entry is the file itself, so only identity (not text or a directory prefix) can catch the alias. */
            DarlingOwnedSecrets.Set(new DarlingOwnedSet(new[] { longFile }, Array.Empty<string>()));

            Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, Check("file:" + longFile));
            Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, Check("file:" + shortPath));
        }
        finally
        {
            try
            {
                Directory.Delete(dir, true);
            }
            catch (IOException)
            {
                /* best effort; Dispose removes the root */
            }
        }
    }
}
