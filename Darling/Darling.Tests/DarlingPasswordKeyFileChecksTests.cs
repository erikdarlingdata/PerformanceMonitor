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
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The checks a <c>passwordKey</c> <c>file:</c> key gets (#8006 review, request 1): a target that is not a regular
/// file, is larger than the key-file limit, or that others could write to is refused; one owned by a user that is
/// neither root nor the service's user, with more than one name, readable by others, or in a directory its group or
/// others can write to without the sticky bit loads with a Warning naming the path and the check, so a Kubernetes secret
/// mount (root-owned, 0644 or 0440) is never refused, and at 0440 gives no message at all. On Windows a file owned by an
/// account other than SYSTEM, Administrators or the service account warns too, and one in a folder that accounts beyond
/// those can change is refused, as the default key file's folder is.
/// The Unix decisions are pinned here through <see cref="StandInUnixModes"/>; the real calls run in the Linux
/// self-check (<c>--self-check-password-key</c>).
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class DarlingPasswordKeyFileChecksTests
{
    private const string KeyPath = "/run/secrets/password-key";
    private static readonly SecurityIdentifier s_builtinUsers = new(WellKnownSidType.BuiltinUsersSid, null);
    private static readonly SecurityIdentifier s_authenticatedUsers = new(WellKnownSidType.AuthenticatedUserSid, null);

    private static PasswordKeyFileFacts OwnFile(UnixFileMode mode) => new(
        RegularFile: true, Length: 2400, Mode: mode, Owner: new UnixFileOwner(1000, 1), EffectiveUserId: 1000,
        DirectoryModes: [Octal("700")], WritableByOrdinaryUsers: false, ReadableByOrdinaryUsers: false);

    private static UnixFileMode Octal(string octal) => (UnixFileMode)Convert.ToInt32(octal, 8);

    public static TheoryData<string, string> Refusals => new()
    {
        { "not a regular file", "is not a regular file" },
        { "larger than the limit", "larger than 8192 bytes" },
        { "group-writable", "can be written by its group or by other users (mode 664)" },
        { "writable by others", "can be written by its group or by other users (mode 602)" },
        { "writable by ordinary users on Windows", "can be written by Users, Authenticated Users, Everyone or INTERACTIVE" },
        {
            "in a folder others can change on Windows",
            @"is not read, because its folder C:\keys is not trusted (accounts beyond SYSTEM, Administrators and the service " +
            @"account can write to it or change what is in it: NT AUTHORITY\Authenticated Users)"
        },
    };

    [Theory]
    [MemberData(nameof(Refusals))]
    public void Judge_RefusesAFileThatIsNoKeyFile_OrThatOthersCouldReplace(string shape, string expected)
    {
        var facts = shape switch
        {
            "not a regular file" => OwnFile(Octal("600")) with { RegularFile = false, Length = 0 },
            "larger than the limit" => OwnFile(Octal("600")) with { Length = 8193 },
            "group-writable" => OwnFile(Octal("664")),
            "writable by others" => OwnFile(Octal("602")),
            "in a folder others can change on Windows" => new PasswordKeyFileFacts(
                true, 2400, null, null, 0, [], WritableByOrdinaryUsers: false, ReadableByOrdinaryUsers: false,
                UntrustedWindowsFolder: new PasswordKeyFolderWriters(@"C:\keys", @"NT AUTHORITY\Authenticated Users")),
            _ => new PasswordKeyFileFacts(true, 2400, null, null, 0, [], WritableByOrdinaryUsers: true, ReadableByOrdinaryUsers: true),
        };

        var verdict = DarlingPasswordKeyFileReader.Judge(KeyPath, facts);

        Assert.NotNull(verdict.Refusal);
        Assert.StartsWith("passwordKey: file '" + KeyPath + "' ", verdict.Refusal, StringComparison.Ordinal);
        Assert.Contains(expected, verdict.Refusal, StringComparison.Ordinal);
        Assert.Empty(verdict.Warnings);
    }

    [Fact]
    public void Judge_ALimitSizedOwnerOnlyFile_IsNeitherRefusedNorWarned()
    {
        foreach (var facts in new[]
                 {
                     OwnFile(Octal("600")) with { Length = 8192 },
                     OwnFile(Octal("400")),
                     /* The Kubernetes default the docs advise: root-owned, 0440, read by a service that runs as root. */
                     OwnFile(Octal("440")) with { Owner = new UnixFileOwner(0, 1), EffectiveUserId = 0, DirectoryModes = [Octal("1777"), Octal("755")] },
                     /* The same Secret read by a service that does not run as root, with the pod's fsGroup: root owns the
                        file, and root can replace any file anyway, so its owner carries no warning. */
                     OwnFile(Octal("440")) with { Owner = new UnixFileOwner(0, 1), EffectiveUserId = 1654, DirectoryModes = [Octal("3777"), Octal("755")] },
                     new PasswordKeyFileFacts(true, 2400, null, null, 0, [], false, false),
                 })
        {
            var verdict = DarlingPasswordKeyFileReader.Judge(KeyPath, facts);
            Assert.Null(verdict.Refusal);
            Assert.Empty(verdict.Warnings);
        }
    }

    public static TheoryData<string, string> Warnings => new()
    {
        { "owned by another user", "is owned by a user that is neither root nor the user the service runs as" },
        { "owned by another user, service as root", "is owned by a user that is neither root nor the user the service runs as" },
        { "two names", "has more than one name" },
        { "readable by others", "is readable by other users" },
        { "open directory", "is in a directory that its group or other users can write to and that has no sticky bit" },
        { "directory its group can write", "is in a directory that its group or other users can write to and that has no sticky bit" },
        { "readable by ordinary users on Windows", "is readable by Users, Authenticated Users, Everyone or INTERACTIVE" },
        { "owned by another account on Windows", "is owned by an account other than SYSTEM, Administrators or the service account" },
    };

    [Theory]
    [MemberData(nameof(Warnings))]
    public void Judge_WarnsNamingThePathAndTheCheck_AndStillLoads(string shape, string expected)
    {
        var facts = shape switch
        {
            "owned by another user" => OwnFile(Octal("440")) with { Owner = new UnixFileOwner(1001, 1) },
            "owned by another user, service as root" => OwnFile(Octal("440")) with { EffectiveUserId = 0 },
            "two names" => OwnFile(Octal("600")) with { Owner = new UnixFileOwner(1000, 2) },
            "readable by others" => OwnFile(Octal("604")),
            "open directory" => OwnFile(Octal("600")) with { DirectoryModes = [Octal("700"), Octal("777")] },
            "directory its group can write" => OwnFile(Octal("600")) with { DirectoryModes = [Octal("770"), Octal("755")] },
            "owned by another account on Windows" => new PasswordKeyFileFacts(
                true, 2400, null, null, 0, [], WritableByOrdinaryUsers: false, ReadableByOrdinaryUsers: false, UntrustedWindowsOwner: true),
            _ => new PasswordKeyFileFacts(true, 2400, null, null, 0, [], WritableByOrdinaryUsers: false, ReadableByOrdinaryUsers: true),
        };

        var verdict = DarlingPasswordKeyFileReader.Judge(KeyPath, facts);

        Assert.Null(verdict.Refusal);
        var warning = Assert.Single(verdict.Warnings);
        Assert.Equal(KeyPath, warning.Path);
        Assert.Contains("{Path}", warning.Template, StringComparison.Ordinal);
        Assert.StartsWith("passwordKey: ", warning.Sentence, StringComparison.Ordinal);
        Assert.Contains("'" + KeyPath + "'", warning.Sentence, StringComparison.Ordinal);
        Assert.Contains(expected, warning.Sentence, StringComparison.Ordinal);
        Assert.Contains("loaded anyway", warning.Sentence, StringComparison.Ordinal);
    }

    [Fact]
    public void Judge_AKubernetesSecretAtItsDefault0644_ReadByANonRootService_WarnsOnlyThatOthersCanReadIt()
    {
        var facts = OwnFile(Octal("644")) with { Owner = new UnixFileOwner(0, 1), DirectoryModes = [Octal("1777"), Octal("755")] };

        var verdict = DarlingPasswordKeyFileReader.Judge(KeyPath, facts);

        Assert.Null(verdict.Refusal);
        var warning = Assert.Single(verdict.Warnings);
        Assert.Contains("readable by other users", warning.Sentence, StringComparison.Ordinal);
    }

    [Fact]
    public void LoadFromSetting_OnUnix_WarnsOnceACheckPerCall_AndRefusesAWritableOrSpecialFile()
    {
        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory("password-key-checks-").FullName;
        var secret = Convert.ToBase64String(pkcs8);
        try
        {
            var path = Path.Combine(dir, "password-key");
            File.WriteAllText(path, secret);
            var modes = new StandInUnixModes { EffectiveUserId = 1000 };
            modes.Report(dir, "755");
            modes.ReportOwner(path, userId: 1001);
            modes.Report(path, "644");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                for (var call = 1; call <= 2; call++)
                {
                    var logger = new CapturingTestLogger();
                    var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, logger);
                    Assert.Null(load.Refusal);
                    Assert.NotNull(load.Pkcs8);
                    Assert.Equal(keyId, KeyIdOf(load.Pkcs8));
                    CryptographicOperations.ZeroMemory(load.Pkcs8);

                    Assert.Equal(2, logger.CountAtLevel(LogLevel.Warning));
                    var warnings = logger.Lines.Where(l => l.StartsWith("Warning:", StringComparison.Ordinal)).ToList();
                    Assert.All(warnings, w => Assert.Contains(path, w, StringComparison.Ordinal));
                    Assert.All(warnings, w => AssertNoKeyText(w, secret));
                    Assert.Contains(warnings, w => w.Contains("is owned by a user that is neither root", StringComparison.Ordinal));
                    Assert.Contains(warnings, w => w.Contains("readable by other users", StringComparison.Ordinal));
                }

                modes.Report(path, "664");
                var writable = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance);
                AssertRefused(writable, "file:" + path, secret, "(mode 664)");

                modes.Report(path, "600");
                modes.ReportRegularFile(path, false);
                var special = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance);
                AssertRefused(special, "file:" + path, secret, "is not a regular file");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void AFileOverTheKeyFileLimit_IsRefusedBeforeItIsRead()
    {
        var dir = Directory.CreateTempSubdirectory("password-key-checks-").FullName;
        try
        {
            var path = Path.Combine(dir, "password-key");
            File.WriteAllText(path, new string('A', 8193));

            var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance);

            AssertRefused(load, "file:" + path, new string('A', 8193), "larger than 8192 bytes");
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void OnWindows_ADevice_IsRefusedAsNotARegularFile()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "NUL is a Windows device name; the Linux self-check reads /dev/zero.");

        var load = DarlingPasswordKeySource.LoadFromSetting("file:NUL", null, NullLogger.Instance);

        AssertRefused(load, "file:NUL", "unused-key-text-0000", "is not a regular file");
        Assert.Contains("'NUL'", load.Refusal, StringComparison.Ordinal);
    }

    /// <summary>
    /// A path whose links, read as text, lead somewhere other than where the kernel takes the open: <c>/dev/stdin</c> or
    /// <c>/proc/self/fd/0</c>, whose link text is <c>pipe:[N]</c>, so the real path is a name that does not exist and its
    /// type cannot be told. The type of the path as written, every link followed by the kernel, is asked too, and a pipe
    /// there is refused before the open rather than read until its writer closes. Stood in here with a junction, whose
    /// real path differs from the path written, and a stand-in that reports only the path written as not a regular file;
    /// the real pipe is in the Linux self-check.
    /// </summary>
    [Fact]
    public void APathWrittenThroughALink_WhoseFinalTargetIsNoRegularFile_IsRefusedBeforeItIsOpened()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "A junction stands in for a Linux magic link on Windows only; Linux runs the real pipe.");

        var (pkcs8, _) = NewKey();
        var dir = Directory.CreateTempSubdirectory("password-key-checks-").FullName;
        var secret = Convert.ToBase64String(pkcs8);
        try
        {
            var target = Path.Combine(dir, "target");
            Directory.CreateDirectory(target);
            File.WriteAllText(Path.Combine(target, "password-key"), secret);
            var junction = Path.Combine(dir, "junction");
            var mklink = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo(
                "cmd.exe", ["/c", "mklink", "/J", junction, target]) { RedirectStandardOutput = true, UseShellExecute = false })!;
            mklink.WaitForExit();
            Assert.SkipUnless(mklink.ExitCode == 0 && Directory.Exists(junction), "A junction could not be made here.");

            var written = Path.Combine(junction, "password-key");
            var modes = new StandInUnixModes { EffectiveUserId = 1000 };
            modes.Report(junction, "700");
            modes.Report(target, "700");
            modes.Report(Path.Combine(target, "password-key"), "600");
            modes.ReportRegularFile(written, false);

            PasswordKeyFileLoad load;
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                load = DarlingPasswordKeySource.LoadFromSetting("file:" + written, null, NullLogger.Instance);
            }

            if (load.Pkcs8 is not null)
            {
                CryptographicOperations.ZeroMemory(load.Pkcs8);
            }

            AssertRefused(load, "file:" + written, secret, "is not a regular file");
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            /* The junction first, on its own: a recursive delete does not remove one. */
            var junction = Path.Combine(dir, "junction");
            if (Directory.Exists(junction))
            {
                Directory.Delete(junction);
            }

            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void OnWindows_AFileUsersCanWrite_IsRefused_AndOneTheyCanRead_LoadsWithAWarning()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory("password-key-checks-").FullName;
        var secret = Convert.ToBase64String(pkcs8);
        try
        {
            var readable = Path.Combine(dir, "readable-key");
            var writable = Path.Combine(dir, "writable-key");
            File.WriteAllText(readable, secret);
            File.WriteAllText(writable, secret);
            Grant(readable, FileSystemRights.Read);
            Grant(writable, FileSystemRights.Write);

            var logger = new CapturingTestLogger();
            var load = DarlingPasswordKeySource.LoadFromSetting("file:" + readable, null, logger);
            Assert.Null(load.Refusal);
            Assert.NotNull(load.Pkcs8);
            Assert.Equal(keyId, KeyIdOf(load.Pkcs8));
            CryptographicOperations.ZeroMemory(load.Pkcs8);
            var warning = Assert.Single(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
            Assert.Contains("passwordKey: ", warning, StringComparison.Ordinal);
            Assert.Contains(readable, warning, StringComparison.Ordinal);
            Assert.Contains("readable by Users", warning, StringComparison.Ordinal);
            AssertNoKeyText(warning, secret);

            var refused = DarlingPasswordKeySource.LoadFromSetting("file:" + writable, null, NullLogger.Instance);
            AssertRefused(refused, "file:" + writable, secret, "can be written by Users");
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>
    /// #8006 review: a Windows key in a folder that ordinary users can write to is refused, as the default key file's
    /// folder is, though the file itself passes every check. The layout is the review's: a folder made under a drive root,
    /// which lets Authenticated Users modify what is made below it, and a key file locked down with the file step alone
    /// (no inherited access, read for SYSTEM, Administrators and the service account). Any local user could replace the
    /// folder, and the key with it.
    /// </summary>
    [Fact]
    public void OnWindows_AKeyInAFolderOrdinaryUsersCanWriteTo_IsRefused_ThoughTheFileIsLockedDown()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Folder ACLs are Windows-only; on Linux an open directory only warns.");

        var (pkcs8, _) = NewKey();
        var secret = Convert.ToBase64String(pkcs8);
        var root = UnderADriveRootsGrant();
        try
        {
            var (keys, path) = AKeyLockedDownByTheFileStepAlone(root, secret);

            var logger = new CapturingTestLogger();
            var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, logger);

            AssertRefused(load, "file:" + path, secret, $"is not read, because its folder {keys} is not trusted");
            Assert.Contains(AccountName(s_authenticatedUsers), load.Refusal, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            RemoveTree(root);
        }
    }

    /// <summary>
    /// #8006 review: the Windows steps docs/darling/security.md gives for a <c>file:</c> key's folder, run on the layout
    /// above, leave a folder and a file that pass every check, so the key loads with no warning. The test's own account
    /// stands in for the service's: the check trusts the account it runs as. The owner step runs only from an elevated
    /// token, as the docs say; without one the folder stays the test account's, which the check trusts here.
    /// </summary>
    [Fact]
    public void OnWindows_AFolderLockedDownAsTheDocsSay_LoadsTheKeyWithNoWarning()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "icacls and folder ACLs are Windows-only.");

        var (pkcs8, keyId) = NewKey();
        var secret = Convert.ToBase64String(pkcs8);
        var root = UnderADriveRootsGrant();
        try
        {
            var (keys, path) = AKeyLockedDownByTheFileStepAlone(root, secret);
            AssertRefused(DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance), "file:" + path, secret, "is not trusted");

            Assert.Equal(0, Icacls(keys, "/inheritance:r", "/grant:r", "*S-1-5-18:(OI)(CI)F", "*S-1-5-32-544:(OI)(CI)F", $"*{CurrentUserSid}:(OI)(CI)RX"));
            Assert.Equal(0, Icacls(path, "/reset"));
            if (IsElevated)
            {
                Assert.Equal(0, Icacls(keys, "/setowner", "*S-1-5-32-544", "/T"));
            }

            var logger = new CapturingTestLogger();
            var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, logger);

            Assert.Null(load.Refusal);
            Assert.NotNull(load.Pkcs8);
            Assert.Equal(keyId, KeyIdOf(load.Pkcs8));
            CryptographicOperations.ZeroMemory(load.Pkcs8);
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            RemoveTree(root);
        }
    }

    /// <summary>
    /// #8006 review: on Windows the folder the path is written in and the folder the file really is in must each pass the
    /// folder check, and each refuses on its own. Junctions stand in for links, as they need no privilege. One sits in an
    /// open folder and leads to the locked folder that holds the key: the path's own folder is the junction, which takes
    /// its ACL from the open folder, and whoever can change it can point the path at another folder. The other sits in the
    /// locked folder and leads to an open folder holding a locked-down key: anyone can replace the file where it really
    /// is. The key in the locked folder, named directly, loads.
    /// </summary>
    [Fact]
    public void OnWindows_TheFolderThePathNames_AndTheFolderTheFileIsIn_AreEachChecked()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Junctions and folder ACLs are Windows-only.");

        var (pkcs8, keyId) = NewKey();
        var secret = Convert.ToBase64String(pkcs8);
        var root = Directory.CreateTempSubdirectory("password-key-checks-").FullName;
        var locked = Path.Combine(root, "locked");
        var open = Path.Combine(root, "open");
        var intoLocked = Path.Combine(open, "into-locked");
        var intoOpen = Path.Combine(locked, "into-open");
        try
        {
            Directory.CreateDirectory(locked);
            Assert.Equal(0, Icacls(locked, "/inheritance:r", "/grant:r", "*S-1-5-18:(OI)(CI)F", "*S-1-5-32-544:(OI)(CI)F", $"*{CurrentUserSid}:(OI)(CI)F"));
            Directory.CreateDirectory(open);
            Assert.Equal(0, Icacls(open, "/grant", "*S-1-5-11:(OI)(CI)M"));
            File.WriteAllText(Path.Combine(locked, "password-key"), secret);
            File.WriteAllText(Path.Combine(open, "password-key"), secret);
            Assert.Equal(0, Icacls(Path.Combine(open, "password-key"), "/inheritance:r", "/grant:r", "*S-1-5-18:F", "*S-1-5-32-544:F", $"*{CurrentUserSid}:F"));
            MakeJunctionOrSkip(intoLocked, locked);
            MakeJunctionOrSkip(intoOpen, open);

            var direct = DarlingPasswordKeySource.LoadFromSetting("file:" + Path.Combine(locked, "password-key"), null, NullLogger.Instance);
            Assert.Null(direct.Refusal);
            Assert.NotNull(direct.Pkcs8);
            Assert.Equal(keyId, KeyIdOf(direct.Pkcs8));
            CryptographicOperations.ZeroMemory(direct.Pkcs8);

            var throughAnOpenJunction = Path.Combine(intoLocked, "password-key");
            var asWritten = DarlingPasswordKeySource.LoadFromSetting("file:" + throughAnOpenJunction, null, NullLogger.Instance);
            AssertRefused(asWritten, "file:" + throughAnOpenJunction, secret, $"because its folder {intoLocked} is not trusted");

            var intoAnOpenFolder = Path.Combine(intoOpen, "password-key");
            var real = DarlingPasswordKeySource.LoadFromSetting("file:" + intoAnOpenFolder, null, NullLogger.Instance);
            AssertRefused(real, "file:" + intoAnOpenFolder, secret, $"{Path.DirectorySeparatorChar}open is not trusted");
            Assert.DoesNotContain("into-open is not trusted", real.Refusal, StringComparison.Ordinal);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            /* The junctions go first, themselves and never what they lead to, so nothing below walks through them. */
            foreach (var junction in new[] { intoLocked, intoOpen })
            {
                if (Directory.Exists(junction))
                {
                    Directory.Delete(junction, recursive: false);
                }
            }

            RemoveTree(root);
        }
    }

    [Fact]
    public void DescribeForValidation_PrintsEachWarningAsItsOwnLine_BeforeTheFingerprint()
    {
        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory("password-key-checks-").FullName;
        var secret = Convert.ToBase64String(pkcs8);
        try
        {
            var path = Path.Combine(dir, "password-key");
            File.WriteAllText(path, secret);
            var modes = new StandInUnixModes { EffectiveUserId = 1000 };
            modes.Report(dir, "777");
            modes.ReportOwner(path, userId: 1001);
            modes.Report(path, "644");
            var config = DarlingConfig.Parse(
                "{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":" + JsonSerializer.Serialize("file:" + path) + "}");

            IReadOnlyList<string> lines;
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                lines = DarlingPasswordKeySource.DescribeForValidation(config, null);
            }

            Assert.Equal(4, lines.Count);
            for (var i = 0; i < 3; i++)
            {
                Assert.StartsWith("WARNING: passwordKey: file '" + path + "' ", lines[i], StringComparison.Ordinal);
                Assert.DoesNotContain("{Path}", lines[i], StringComparison.Ordinal);
            }

            Assert.Contains("is owned by a user that is neither root", lines[0], StringComparison.Ordinal);
            Assert.Contains("readable by other users", lines[1], StringComparison.Ordinal);
            Assert.Contains("no sticky bit", lines[2], StringComparison.Ordinal);
            Assert.Equal("passwordKey: password key " + PasswordSeal.DisplayKeyId(keyId), lines[3]);
            Assert.All(lines, l => AssertNoKeyText(l, secret));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    private static void Grant(string path, FileSystemRights rights)
    {
        var info = new FileInfo(path);
        var security = info.GetAccessControl();
        security.AddAccessRule(new FileSystemAccessRule(s_builtinUsers, rights, AccessControlType.Allow));
        info.SetAccessControl(security);
    }

    private static string CurrentUserSid => WindowsIdentity.GetCurrent().User!.Value;

    /// <summary>Whether this process holds the Administrators group, which only an elevated token does.</summary>
    private static bool IsElevated => new WindowsPrincipal(WindowsIdentity.GetCurrent()).IsInRole(WindowsBuiltInRole.Administrator);

    private static string AccountName(SecurityIdentifier sid) => sid.Translate(typeof(NTAccount)).Value;

    /// <summary>A new temporary folder that hands what is made in it the grant a drive root such as C:\ gives:
    /// Authenticated Users may modify every folder and file below it (an inherit-only entry).</summary>
    private static string UnderADriveRootsGrant()
    {
        var root = Directory.CreateTempSubdirectory("password-key-checks-");
        var security = root.GetAccessControl();
        security.AddAccessRule(new FileSystemAccessRule(
            s_authenticatedUsers, FileSystemRights.Modify, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
            PropagationFlags.InheritOnly, AccessControlType.Allow));
        root.SetAccessControl(security);
        return root.FullName;
    }

    /// <summary>A <c>keys</c> folder under <paramref name="root"/> holding a key file locked down by the file step alone:
    /// no inherited access, and read for SYSTEM, Administrators and the test's account, the service's here. The folder
    /// keeps what it inherits.</summary>
    private static (string Folder, string Path) AKeyLockedDownByTheFileStepAlone(string root, string secret)
    {
        var keys = Directory.CreateDirectory(Path.Combine(root, "keys")).FullName;
        var path = Path.Combine(keys, "password-key");
        File.WriteAllText(path, secret);
        Assert.Equal(0, Icacls(path, "/inheritance:r", "/grant:r", "*S-1-5-18:R", "*S-1-5-32-544:R", $"*{CurrentUserSid}:R"));
        Assert.False(DarlingFileSecurity.IsWritableByOrdinaryUsers(path));
        Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(path));
        return (keys, path);
    }

    private static int Icacls(params string[] arguments)
    {
        var startInfo = new ProcessStartInfo("icacls.exe") { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        using var process = Process.Start(startInfo)!;
        _ = process.StandardOutput.ReadToEnd();
        _ = process.StandardError.ReadToEnd();
        process.WaitForExit();
        return process.ExitCode;
    }

    private static void MakeJunctionOrSkip(string junction, string target)
    {
        using var mklink = Process.Start(new ProcessStartInfo("cmd.exe", ["/c", "mklink", "/J", junction, target])
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        })!;
        _ = mklink.StandardOutput.ReadToEnd();
        _ = mklink.StandardError.ReadToEnd();
        mklink.WaitForExit();
        Assert.SkipUnless(mklink.ExitCode == 0 && Directory.Exists(junction), "A junction could not be made here.");
    }

    /// <summary>Removes a tree whose folders and files a test locked down. The test's account owns them, or holds the
    /// Administrators group, so it can always grant itself full control first. The grant's own result is not judged: a
    /// failure there shows as the delete's.</summary>
    private static void RemoveTree(string root)
    {
        if (!Directory.Exists(root))
        {
            return;
        }

        _ = Icacls(root, "/grant", $"*{CurrentUserSid}:(OI)(CI)F", "/T", "/C", "/Q");
        Directory.Delete(root, recursive: true);
    }

    private static (byte[] Pkcs8, string KeyId) NewKey()
    {
        using var key = PasswordPrivateKey.Generate();
        return (key.ExportPkcs8(), key.PublicKey.KeyId);
    }

    private static string KeyIdOf(byte[] pkcs8)
    {
        using var key = PasswordPrivateKey.FromPkcs8(pkcs8);
        return key.PublicKey.KeyId;
    }

    private static void AssertRefused(PasswordKeyFileLoad load, string reference, string secret, string expected)
    {
        Assert.True(load.Present);
        Assert.True(load.Untrusted);
        Assert.Null(load.Pkcs8);
        Assert.Equal(reference, load.Path);
        Assert.NotNull(load.Refusal);
        Assert.StartsWith("passwordKey: ", load.Refusal, StringComparison.Ordinal);
        Assert.Contains(reference["file:".Length..], load.Refusal, StringComparison.Ordinal);
        Assert.Contains(expected, load.Refusal, StringComparison.Ordinal);
        AssertNoKeyText(load.Refusal, secret);
    }

    /// <summary>The sentence holds neither the key text nor any 16-character slice of it.</summary>
    private static void AssertNoKeyText(string sentence, string secret)
    {
        Assert.DoesNotContain(secret, sentence, StringComparison.Ordinal);
        for (var i = 0; i + 16 <= secret.Length; i++)
        {
            Assert.False(sentence.AsSpan().Contains(secret.AsSpan(i, 16), StringComparison.Ordinal), $"the sentence holds key text at offset {i}");
        }
    }
}
