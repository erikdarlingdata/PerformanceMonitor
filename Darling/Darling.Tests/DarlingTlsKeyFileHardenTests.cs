/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Security.Principal;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5700: the TLS private-key files darling.json names (the pfxPath and keyPath of web.network.tls and
/// mcp.network.tls). A key inside the install folder is changed through a handle to exactly the checked file and ends
/// protected, with SYSTEM and Administrators full control and the service account read only. A key outside it is
/// reported and never changed. A path that is not fully qualified on a drive letter is skipped. Every case here sits in a
/// folder that grants Users read by inheritance, which is what a default install folder (and a drive-root folder) does.
/// </summary>
public class DarlingTlsKeyFileHardenTests
{
    private static readonly SecurityIdentifier Users = new(WellKnownSidType.BuiltinUsersSid, null);

    /// <summary>The account the verb is told the service runs as. The current user stands in for it, which is also the
    /// account the process-wide setting resolves to when nothing sets it, so no other test sees a difference.</summary>
    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static SecurityIdentifier ServiceSid => WindowsIdentity.GetCurrent().User!;

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static string MakeFolder(string path)
    {
        Directory.CreateDirectory(path);
        var folder = new DirectoryInfo(path);
        var acl = folder.GetAccessControl();
        acl.AddAccessRule(new FileSystemAccessRule(
            Users, FileSystemRights.ReadAndExecute,
            InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));
        folder.SetAccessControl(acl);
        return path;
    }

    /// <summary>A file that inherits the folder's Users read, checked to be readable by ordinary users.</summary>
    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static string MakeKey(string folder, string name)
    {
        var path = Path.Combine(folder, name);
        File.WriteAllText(path, "placeholder");
        Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(path), $"{path} should start out readable by Users");
        return path;
    }

    private static string WriteConfig(string install, string tlsJson)
    {
        var config = Path.Combine(install, "darling.json");
        File.WriteAllText(config,
            "{\"postgres\":{\"managed\":false,\"connectionString\":\"Host=127.0.0.1;Username=darling\"},"
            + "\"web\":{\"network\":{\"tls\":" + tlsJson + "}}}");
        return config;
    }

    private static string Json(string text) => System.Text.Json.JsonSerializer.Serialize(text);

    /// <summary>The whole security descriptor (owner, group and DACL) as hex, so "unchanged" means byte-identical.</summary>
    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static string Snapshot(string path) =>
        Convert.ToHexString(new FileInfo(path).GetAccessControl().GetSecurityDescriptorBinaryForm());

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static (int Exit, string Output, string Error) Run(string config, bool elevated = true, SecurityIdentifier? account = null, bool noAccount = false)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = DarlingCliCommands.HardenTlsKeyFilesOnly(config, output, error, elevated, noAccount ? null : account ?? ServiceSid);
        return (exit, output.ToString(), error.ToString());
    }

    private static bool WindowsOnly()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("DACLs, junctions and hard links are Windows'.");
            return false;
        }

        return true;
    }

    /// <summary>
    /// A key in the install folder ends with a protected DACL of exactly SYSTEM full control, Administrators full control
    /// and the service account read only: no Users grant, no operator group, nothing inherited, and no write bit for the
    /// service (which is what keeps the install tree lock, which strips an untrusted account's write grants, from resetting it).
    /// </summary>
    [Fact]
    public void AKeyInTheInstallFolder_EndsProtected_WithOnlyTheThreeGrants_AndTheServicesRead()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-in-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var pfx = MakeKey(install, "web.pfx");
            var config = WriteConfig(install, "{\"pfxPath\":" + Json(pfx) + "}");

            var (exit, output, error) = Run(config);

            Assert.Equal(0, exit);
            Assert.Equal("", error);
            Assert.Equal($"  SECURED  {pfx} (the TLS key file, web.network.tls.pfxPath)", output.TrimEnd());

            var security = new FileInfo(pfx).GetAccessControl();
            Assert.True(security.AreAccessRulesProtected, "the DACL should be protected");
            var rules = security.GetAccessRules(true, true, typeof(SecurityIdentifier)).Cast<FileSystemAccessRule>().ToList();
            Assert.Equal(3, rules.Count);
            Assert.All(rules, rule =>
            {
                Assert.Equal(AccessControlType.Allow, rule.AccessControlType);
                Assert.False(rule.IsInherited);
            });
            FileSystemRights RightsOf(SecurityIdentifier sid) => rules.Single(rule => rule.IdentityReference.Equals(sid)).FileSystemRights;
            Assert.Equal(FileSystemRights.FullControl, RightsOf(new SecurityIdentifier(WellKnownSidType.LocalSystemSid, null)));
            Assert.Equal(FileSystemRights.FullControl, RightsOf(new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null)));
            var service = RightsOf(ServiceSid);
            Assert.Equal(FileSystemRights.Read | FileSystemRights.Synchronize, service);
            Assert.Equal(default, service & (FileSystemRights.WriteData | FileSystemRights.AppendData | FileSystemRights.WriteAttributes
                | FileSystemRights.WriteExtendedAttributes | FileSystemRights.Delete | FileSystemRights.ChangePermissions | FileSystemRights.TakeOwnership));
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(pfx));
            Assert.True(DarlingFileSecurity.GrantsHardenedAccount(pfx));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A junction on a folder between the install folder and the key is refused, and what it points at is not touched.</summary>
    [Fact]
    public void AJunctionOnAParentFolderInsideTheInstallFolder_IsRefused_AndItsTargetKeepsItsAcl()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-junction-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var elsewhere = MakeFolder(Path.Combine(scratch.FullName, "elsewhere"));
            var victim = MakeKey(elsewhere, "web.pfx");
            var victimBefore = Snapshot(victim);
            var link = Path.Combine(install, "certs");
            MakeJunction(link, elsewhere);
            Assert.True(File.Exists(Path.Combine(link, "web.pfx")), "the junction was not created");
            var viaLink = Path.Combine(link, "web.pfx");
            var config = WriteConfig(install, "{\"pfxPath\":" + Json(viaLink) + "}");

            var (exit, output, _) = Run(config);

            Assert.Equal(0, exit);
            Assert.StartsWith($"  REFUSED  {viaLink} (the TLS key file, web.network.tls.pfxPath): ", output, StringComparison.Ordinal);
            Assert.Equal(victimBefore, Snapshot(victim));

            /* The change itself checks the handle it changes, so a link that appears after the path check is refused too. */
            var refusal = DarlingFileSecurity.HardenKeyFileWithoutFollowingLinks(viaLink, install);
            Assert.NotNull(refusal);
            Assert.Equal(victimBefore, Snapshot(victim));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A key with a second name is refused, because its ACL belongs to the file and is the same under both.</summary>
    [Fact]
    public void AHardLinkedKey_IsRefused_AndTheOtherNameKeepsItsAcl()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-hardlink-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var elsewhere = MakeFolder(Path.Combine(scratch.FullName, "elsewhere"));
            var victim = MakeKey(elsewhere, "someones.pfx");
            var victimBefore = Snapshot(victim);
            var pfx = Path.Combine(install, "web.pfx");
            Assert.True(CreateHardLinkW(pfx, victim, IntPtr.Zero), "the hard link was not created");
            var config = WriteConfig(install, "{\"pfxPath\":" + Json(pfx) + "}");

            var (exit, output, _) = Run(config);

            Assert.Equal(0, exit);
            Assert.StartsWith($"  REFUSED  {pfx} (the TLS key file, web.network.tls.pfxPath): ", output, StringComparison.Ordinal);
            Assert.Contains("hard link", output, StringComparison.Ordinal);
            Assert.Equal(victimBefore, Snapshot(victim));
            Assert.Equal(victimBefore, Snapshot(pfx));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A key outside the install folder is not changed (byte-identical before and after); the report says so, says
    /// whether ordinary users can read it, and gives the exact icacls line for an administrator.</summary>
    [Fact]
    public void AKeyOutsideTheInstallFolder_IsLeftUnchanged_AndReported()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-out-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var outside = MakeFolder(Path.Combine(scratch.FullName, "outside"));
            var key = MakeKey(outside, "mcp.key");
            var keyBefore = Snapshot(key);
            var folderBefore = Convert.ToHexString(new DirectoryInfo(outside).GetAccessControl().GetSecurityDescriptorBinaryForm());
            var config = WriteConfig(install, "{\"keyPath\":" + Json(key) + "}");

            var (exit, output, error) = Run(config);

            Assert.Equal(0, exit);
            Assert.Equal("", error);
            Assert.Equal(keyBefore, Snapshot(key));
            Assert.Equal(folderBefore, Convert.ToHexString(new DirectoryInfo(outside).GetAccessControl().GetSecurityDescriptorBinaryForm()));
            var line = Assert.Single(output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries));
            Assert.StartsWith($"  NOT CHANGED (outside the install folder)  {key} (the TLS key file, web.network.tls.keyPath): ordinary users can read it", line, StringComparison.Ordinal);
            Assert.EndsWith(DarlingCliCommands.TlsKeyFileIcaclsLine(key), line, StringComparison.Ordinal);
            Assert.EndsWith(":(R)\"", line, StringComparison.Ordinal);
            Assert.DoesNotContain(":F\"", line.Replace("*S-1-5-18:F", string.Empty).Replace("*S-1-5-32-544:F", string.Empty), StringComparison.Ordinal);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A UNC path, a path from the root of the drive, a drive-relative path and a relative path are skipped with a
    /// reason, in every spelling, and a relative name is not looked for in the working directory.</summary>
    [Theory]
    [InlineData(@"\\fileserver\certs\darling.pfx")]
    [InlineData("//fileserver/certs/darling.pfx")]
    [InlineData(@"\certs\darling.pfx")]
    [InlineData("/certs/darling.pfx")]
    [InlineData("C:darling.pfx")]
    [InlineData(@"certs\darling.pfx")]
    [InlineData("TLSKEY-NAME")]
    public void APathThatIsNotFullyQualifiedOnADriveLetter_IsSkipped_NeverResolvedAgainstTheWorkingDirectory(string named)
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-path-");
        var inWorkingDirectory = Path.Combine(Directory.GetCurrentDirectory(), "darling-5700-" + Guid.NewGuid().ToString("N") + ".pfx");
        try
        {
            /* A relative name really would resolve to a readable file here if the verb looked in its working directory. */
            File.WriteAllText(inWorkingDirectory, "placeholder");
            var before = Snapshot(inWorkingDirectory);
            var path = named == "TLSKEY-NAME" ? Path.GetFileName(inWorkingDirectory) : named;
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var config = WriteConfig(install, "{\"pfxPath\":" + Json(path) + "}");

            var (exit, output, error) = Run(config);

            Assert.Equal(0, exit);
            Assert.Equal("", error);
            Assert.StartsWith($"  SKIPPED  {path} (the TLS key file, web.network.tls.pfxPath): ", output, StringComparison.Ordinal);
            Assert.DoesNotContain("SECURED", output, StringComparison.Ordinal);
            Assert.DoesNotContain("NOT CHANGED", output, StringComparison.Ordinal);
            Assert.Equal(before, Snapshot(inWorkingDirectory));
            Assert.NotNull(DarlingCliCommands.TlsKeyPathProblem(path));
        }
        finally
        {
            File.Delete(inWorkingDirectory);
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    [Theory]
    [InlineData(@"C:\certs\darling.pfx")]
    [InlineData("d:/certs/darling.pfx")]
    [InlineData(@"C:\certs\..\certs\darling.pfx")]
    public void AFullyQualifiedDriveLetterPath_IsAccepted(string named)
    {
        Assert.Null(DarlingCliCommands.TlsKeyPathProblem(named));
    }

    /// <summary>
    /// What the scripts rely on: 0 whenever the step ran, whatever each file reported (here a hardened key, a refused
    /// one, an outside one, a missing one and a skipped one), one line per file on standard output and nothing on standard
    /// error; nonzero only when it could not run (not elevated, no config, no service account).
    /// </summary>
    [Fact]
    public void TheTlsKeyFilesOnlyStep_ExitsZeroWhenItRan_AndNonzeroOnlyWhenItCouldNot()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-exit-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var elsewhere = MakeFolder(Path.Combine(scratch.FullName, "elsewhere"));
            var outside = MakeFolder(Path.Combine(scratch.FullName, "outside"));
            var good = MakeKey(install, "web.pfx");
            var victim = MakeKey(elsewhere, "someones.pfx");
            var linked = Path.Combine(install, "linked.pfx");
            Assert.True(CreateHardLinkW(linked, victim, IntPtr.Zero), "the hard link was not created");
            var away = MakeKey(outside, "mcp.key");
            var gone = Path.Combine(install, "gone.key");
            var config = Path.Combine(install, "darling.json");
            File.WriteAllText(config,
                "{\"postgres\":{\"managed\":false,\"connectionString\":\"Host=127.0.0.1;Username=darling\"},"
                + "\"web\":{\"network\":{\"tls\":{\"pfxPath\":" + Json(good) + ",\"keyPath\":" + Json(linked) + "}}},"
                + "\"mcp\":{\"network\":{\"tls\":{\"pfxPath\":" + Json(gone) + ",\"keyPath\":" + Json(away) + "}}}}");

            var (exit, output, error) = Run(config);

            Assert.Equal(0, exit);
            Assert.Equal("", error);
            var lines = output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(4, lines.Length);
            Assert.StartsWith("  SECURED  ", lines[0], StringComparison.Ordinal);
            Assert.StartsWith("  REFUSED  ", lines[1], StringComparison.Ordinal);
            Assert.StartsWith("  MISSING  ", lines[2], StringComparison.Ordinal);
            Assert.StartsWith("  NOT CHANGED (outside the install folder)  ", lines[3], StringComparison.Ordinal);

            /* Could not run: each is nonzero, says why on standard error, and prints nothing on standard output. */
            var notElevated = Run(config, elevated: false);
            Assert.NotEqual(0, notElevated.Exit);
            Assert.Equal("", notElevated.Output);
            Assert.NotEqual("", notElevated.Error);
            var noAccount = Run(config, noAccount: true);
            Assert.NotEqual(0, noAccount.Exit);
            Assert.Equal("", noAccount.Output);
            Assert.NotEqual("", noAccount.Error);
            var noConfig = Run(Path.Combine(install, "absent.json"));
            Assert.NotEqual(0, noConfig.Exit);
            Assert.Equal("", noConfig.Output);
            Assert.NotEqual("", noConfig.Error);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>The flag is read from the second argument, is matched without regard to case, and stays out of the help text.</summary>
    [Fact]
    public void TheTlsKeyFilesOnlyFlag_IsRecognised_AndNotListedInTheHelp()
    {
        Assert.True(DarlingCliCommands.IsTlsKeyFilesOnlyFlag("--tls-key-files-only"));
        Assert.True(DarlingCliCommands.IsTlsKeyFilesOnlyFlag("--TLS-KEY-FILES-ONLY"));
        Assert.False(DarlingCliCommands.IsTlsKeyFilesOnlyFlag("--tls-key-files"));
        Assert.DoesNotContain("tls-key-files-only", DarlingCliCommands.UsageText(), StringComparison.Ordinal);

        var program = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Program.cs"));
        Assert.Contains("DarlingCliCommands.HardenTlsKeyFilesOnly(", program, StringComparison.Ordinal);
    }

    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
    private static extern bool CreateHardLinkW(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);

    private static void MakeJunction(string link, string target)
    {
        using var process = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo("cmd.exe")
        {
            ArgumentList = { "/c", "mklink", "/J", link, target },
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        })!;
        process.StandardOutput.ReadToEnd();
        process.StandardError.ReadToEnd();
        process.WaitForExit();
    }
}
