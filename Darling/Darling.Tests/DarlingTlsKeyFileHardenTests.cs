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

    /// <summary>A DER SEQUENCE header with a two-byte length (256), then 256 bytes: starts like a PKCS#12 bundle.</summary>
    private static byte[] DerBundle() => new byte[] { 0x30, 0x82, 0x01, 0x00 }.Concat(new byte[256]).ToArray();

    private const string PemKeyText = "-----BEGIN PRIVATE KEY-----\nTUlJRXZ3SUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0M=\n-----END PRIVATE KEY-----\n";

    /// <summary>The content a key file of that name has to have: a bundle for .pfx, a PEM key for .key, a short password for .txt.</summary>
    private static byte[] ContentFor(string name) =>
        name.EndsWith(".pfx", StringComparison.OrdinalIgnoreCase) ? DerBundle()
        : name.EndsWith(".key", StringComparison.OrdinalIgnoreCase) ? System.Text.Encoding.ASCII.GetBytes(PemKeyText)
        : System.Text.Encoding.ASCII.GetBytes("example-password");

    /// <summary>A file that inherits the folder's Users read, checked to be readable by ordinary users.</summary>
    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static string MakeKey(string folder, string name, byte[]? content = null)
    {
        var path = Path.Combine(folder, name);
        File.WriteAllBytes(path, content ?? ContentFor(name));
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
        return (exit, WithoutConfigHeader(output.ToString()), error.ToString());
    }

    /// <summary>The report without the lines the verb opens with (the config file it checked, and where that path came
    /// from), which the tests of those lines read directly.</summary>
    private static string WithoutConfigHeader(string output)
    {
        var lines = output.ReplaceLineEndings("\n").Split('\n').ToList();
        if (lines.Count > 0 && lines[0].StartsWith("Config file: ", StringComparison.Ordinal))
        {
            lines.RemoveAt(0);
            if (lines.Count > 0 && lines[0].StartsWith("  (taken from the DARLING_CONFIG", StringComparison.Ordinal))
            {
                lines.RemoveAt(0);
            }
        }

        return string.Join(Environment.NewLine, lines);
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
            var refusal = DarlingFileSecurity.HardenKeyFileWithoutFollowingLinks(viaLink, install, DarlingFileSecurity.KeyFileKind.Pfx);
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

            /* The report names the other name, and gives the command that lists every name, so an administrator can find it. */
            Assert.Contains(@"\elsewhere\someones.pfx", output, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"fsutil hardlink list \"{pfx}\"", output, StringComparison.Ordinal);
            Assert.Contains("Remove the other name(s) and re-run.", output, StringComparison.Ordinal);
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

    /// <summary>A file named as a bundle or key that is neither is refused with the reason, and its descriptor (owner and
    /// DACL) is byte-identical afterwards. A real key beside it is still secured.</summary>
    [Theory]
    [InlineData("pfxPath", "notes.txt", "this is not a bundle")]
    [InlineData("pfxPath", "darling.dll", "MZ")]
    [InlineData("keyPath", "web.crt", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")]
    [InlineData("keyPath", "settings.json", "{\"a\":1}")]
    public void AFileThatIsNotACertificateOrKey_IsRefused_AndItsAclIsUnchanged(string setting, string name, string text)
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-content-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var notAKey = MakeKey(install, name, System.Text.Encoding.ASCII.GetBytes(text));
            var before = Snapshot(notAKey);
            var config = WriteConfig(install, "{\"" + setting + "\":" + Json(notAKey) + "}");

            var (exit, output, error) = Run(config);

            Assert.Equal(0, exit);
            Assert.Equal("", error);
            Assert.StartsWith($"  REFUSED  {notAKey} (the TLS key file, web.network.tls.{setting}): ", output, StringComparison.Ordinal);
            Assert.Contains("is not a certificate or key file", output, StringComparison.Ordinal);
            Assert.DoesNotContain("SECURED", output, StringComparison.Ordinal);
            Assert.Equal(before, Snapshot(notAKey));
            Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(notAKey), "the refused file should keep the folder's ACL");
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>The same check through the full verb: the refusal counts, and the exit code says so.</summary>
    [Fact]
    public void TheFullVerb_RefusesANonKeyFileNamedAsTheBundle_AndExitsNonzero()
    {
        if (!WindowsOnly())
        {
            return;
        }

        Assert.SkipWhen(DarlingFileSecurity.RegisteredServiceAccount("PerformanceMonitor Darling") is not null,
            "The Darling service is registered on this machine.");
        var scratch = Directory.CreateTempSubdirectory("darling-5700-verb-refused-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var notAKey = MakeKey(install, "notes.txt", System.Text.Encoding.ASCII.GetBytes("hello"));
            var before = Snapshot(notAKey);
            var config = WriteStoreLessConfig(install, scratch.FullName, "{\"pfxPath\":" + Json(notAKey) + "}");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = DarlingCliCommands.HardenFiles(config, output, error);

            Assert.Equal(1, exit);
            Assert.Contains("is not a certificate or key file", error.ToString(), StringComparison.Ordinal);
            Assert.Equal(before, Snapshot(notAKey));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    private static string WriteStoreLessConfig(string install, string scratch, string tlsJson)
    {
        var config = Path.Combine(install, "darling.json");
        File.WriteAllText(config,
            "{\"postgres\":{\"managed\":false,\"connectionString\":\"Host=127.0.0.1;Username=darling\",\"dataDirectory\":"
            + Json(Path.Combine(scratch, "store", "pg")) + "},"
            + "\"web\":{\"network\":{\"tls\":" + tlsJson + "}}}");
        return config;
    }

    [Theory]
    [InlineData(new byte[] { 0x30, 0x05, 1, 2, 3, 4, 5 }, true)]
    [InlineData(new byte[] { 0x30, 0x81, 0x80 }, false)]
    [InlineData(new byte[] { 0x30, 0x80, 0x02, 0x01, 0x03 }, false)]
    [InlineData(new byte[] { 0x30, 0x81, 0x05, 1, 2, 3, 4, 5 }, false)]
    [InlineData(new byte[] { 0x30, 0x82, 0x00, 0x05, 1, 2, 3, 4, 5 }, false)]
    [InlineData(new byte[] { 0x02, 0x01, 0x03 }, false)]
    [InlineData(new byte[] { 0x30 }, false)]
    [InlineData(new byte[] { 0x30, 0x00 }, false)]
    [InlineData(new byte[] { 0x30, 0x85, 1, 0, 0, 0, 0 }, false)]
    [InlineData(new byte[] { 0x4D, 0x5A, 0x90, 0x00 }, false)]
    public void ABundleHeader_IsAcceptedOnlyAsAnAsn1SequenceWithAValidDerLength(byte[] file, bool valid)
    {
        /* The declared length also has to fit in the file: the 0x30 0x81 0x80 case claims 128 bytes after a 3-byte file. */
        var head = file.Length > 16 ? file[..16] : file;
        Assert.Equal(valid, DarlingFileSecurity.PfxContentRefusal(head, file.Length) is null);
    }

    [Fact]
    public void ABundleHeader_ThatClaimsMoreThanTheFileHolds_IsRefused()
    {
        Assert.NotNull(DarlingFileSecurity.PfxContentRefusal(new byte[] { 0x30, 0x82, 0x01, 0x00 }, 100));
        Assert.Null(DarlingFileSecurity.PfxContentRefusal(new byte[] { 0x30, 0x82, 0x01, 0x00 }, 4 + 256));
    }

    [Theory]
    [InlineData("-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----\n", true)]
    [InlineData("-----BEGIN RSA PRIVATE KEY-----\nAA==\n", true)]
    [InlineData("-----BEGIN ENCRYPTED PRIVATE KEY-----\nAA==\n", true)]
    [InlineData("-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n-----BEGIN EC PRIVATE KEY-----\nAA==\n", true)]
    [InlineData("-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n", false)]
    [InlineData("BEGIN PRIVATE KEY", false)]
    [InlineData("-----BEGIN PUBLIC KEY-----\nAA==\n", false)]
    [InlineData("", false)]
    public void APemKey_IsAcceptedOnlyWithAPrivateKeyHeader(string text, bool valid)
    {
        Assert.Equal(valid, DarlingFileSecurity.PemContentRefusal(System.Text.Encoding.ASCII.GetBytes(text)) is null);
    }

    /// <summary>A key owned by an ordinary account ends owned by Administrators (the tree lock's rule), which needs the
    /// Administrators group in the token, so this runs elevated (CI) and is skipped otherwise.</summary>
    [Fact]
    public void AKeyOwnedByAnOrdinaryAccount_EndsOwnedByAdministrators()
    {
        if (!WindowsOnly())
        {
            return;
        }

        using var identity = WindowsIdentity.GetCurrent();
        Assert.SkipUnless(new WindowsPrincipal(identity).IsInRole(WindowsBuiltInRole.Administrator),
            "Setting the owner to Administrators needs an elevated process.");
        var scratch = Directory.CreateTempSubdirectory("darling-5700-owner-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var pfx = MakeKey(install, "web.pfx");
            var administrators = new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null);
            var mine = new FileInfo(pfx).GetAccessControl();
            mine.SetOwner(ServiceSid);
            new FileInfo(pfx).SetAccessControl(mine);
            Assert.Equal(ServiceSid, new FileInfo(pfx).GetAccessControl().GetOwner(typeof(SecurityIdentifier)));
            var config = WriteConfig(install, "{\"pfxPath\":" + Json(pfx) + "}");

            /* The service runs as some other account, so the current user is not a trusted owner. */
            var other = new SecurityIdentifier("S-1-5-80-5700-1-2-3-4");
            try
            {
                var (exit, output, error) = Run(config, account: other);

                Assert.Equal(0, exit);
                Assert.Equal("", error);
                Assert.Equal($"  SECURED  {pfx} (the TLS key file, web.network.tls.pfxPath)", output.TrimEnd());
                Assert.Equal(administrators, new FileInfo(pfx).GetAccessControl().GetOwner(typeof(SecurityIdentifier)));
            }
            finally
            {
                DarlingFileSecurity.HardenForAccount(ServiceSid);
            }
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>Wherever the owner cannot be changed, a key owned by an account outside the three is reported, not SECURED.</summary>
    [Fact]
    public void AKeyWithAnOwnerOutsideTheThree_IsNotReportedSecured()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-untrusted-owner-");
        try
        {
            var folder = MakeFolder(Path.Combine(scratch.FullName, "keys"));
            var key = MakeKey(folder, "web.pfx");
            var other = new SecurityIdentifier("S-1-5-80-5700-1-2-3-4");
            var security = new FileInfo(key).GetAccessControl();
            security.SetOwner(ServiceSid);
            security.SetAccessRuleProtection(true, false);
            foreach (FileSystemAccessRule rule in security.GetAccessRules(true, false, typeof(SecurityIdentifier)))
            {
                security.RemoveAccessRuleAll(rule);
            }

            foreach (var sid in new[] { new SecurityIdentifier(WellKnownSidType.LocalSystemSid, null), new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null) })
            {
                security.AddAccessRule(new FileSystemAccessRule(sid, FileSystemRights.FullControl, AccessControlType.Allow));
            }

            security.AddAccessRule(new FileSystemAccessRule(other, FileSystemRights.Read | FileSystemRights.Synchronize, AccessControlType.Allow));
            new FileInfo(key).SetAccessControl(security);
            try
            {
                DarlingFileSecurity.HardenForAccount(other);

                var line = DarlingCliCommands.TlsKeyFileUnsecured(key, "the TLS key file, web.network.tls.pfxPath");

                Assert.NotNull(line);
                Assert.StartsWith($"  UNTRUSTED OWNER  {key} (", line, StringComparison.Ordinal);
                Assert.Contains("takeown /a /f", line, StringComparison.Ordinal);
            }
            finally
            {
                DarlingFileSecurity.HardenForAccount(ServiceSid);
            }
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>An Allow entry for any account beyond SYSTEM, Administrators and the service account (a named user, a
    /// domain group) keeps the file from being reported SECURED, though none of the four broad groups can read it.</summary>
    [Fact]
    public void AnExtraAllowEntryForAnotherAccount_IsNotReportedSecured()
    {
        if (!WindowsOnly())
        {
            return;
        }

        DarlingFileSecurity.HardenForAccount(ServiceSid);
        var scratch = Directory.CreateTempSubdirectory("darling-5700-extra-ace-");
        try
        {
            var folder = MakeFolder(Path.Combine(scratch.FullName, "keys"));
            var key = MakeKey(folder, "web.pfx");
            var security = new FileInfo(key).GetAccessControl();
            security.SetAccessRuleProtection(true, false);
            foreach (FileSystemAccessRule rule in security.GetAccessRules(true, false, typeof(SecurityIdentifier)))
            {
                security.RemoveAccessRuleAll(rule);
            }

            foreach (var sid in new[] { new SecurityIdentifier(WellKnownSidType.LocalSystemSid, null), new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null) })
            {
                security.AddAccessRule(new FileSystemAccessRule(sid, FileSystemRights.FullControl, AccessControlType.Allow));
            }

            security.AddAccessRule(new FileSystemAccessRule(ServiceSid, FileSystemRights.Read | FileSystemRights.Synchronize, AccessControlType.Allow));
            new FileInfo(key).SetAccessControl(security);
            const string what = "the TLS key file, web.network.tls.pfxPath";
            Assert.Null(DarlingCliCommands.TlsKeyFileUnsecured(key, what));

            var withExtra = new FileInfo(key).GetAccessControl();
            withExtra.AddAccessRule(new FileSystemAccessRule(
                new SecurityIdentifier(WellKnownSidType.LocalServiceSid, null), FileSystemRights.Read, AccessControlType.Allow));
            new FileInfo(key).SetAccessControl(withExtra);

            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(key), "the extra entry is not one of the four broad groups");
            var line = DarlingCliCommands.TlsKeyFileUnsecured(key, what);
            Assert.NotNull(line);
            Assert.StartsWith($"  OTHER ACCESS  {key} ({what}): ", line, StringComparison.Ordinal);
            Assert.Contains("LOCAL SERVICE", line, StringComparison.Ordinal);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>An outside key's line reports an owner outside the three, and an extra entry, as well as the broad groups.</summary>
    [Fact]
    public void AnOutsideKey_ReportsItsOwnerAndOtherAccess_InTheNotChangedLine()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-out-owner-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var outside = MakeFolder(Path.Combine(scratch.FullName, "outside"));
            var key = MakeKey(outside, "mcp.key");
            var security = new FileInfo(key).GetAccessControl();
            security.SetOwner(ServiceSid);
            new FileInfo(key).SetAccessControl(security);
            var config = WriteConfig(install, "{\"keyPath\":" + Json(key) + "}");
            var other = new SecurityIdentifier("S-1-5-80-5700-1-2-3-4");
            try
            {
                var (exit, output, _) = Run(config, account: other);

                Assert.Equal(0, exit);
                Assert.Contains("its owner is not SYSTEM, Administrators or the service account", output, StringComparison.Ordinal);
                Assert.Contains("takeown /a /f", output, StringComparison.Ordinal);
                Assert.Contains(DarlingCliCommands.TlsKeyFileIcaclsLine(key), output, StringComparison.Ordinal);
            }
            finally
            {
                DarlingFileSecurity.HardenForAccount(ServiceSid);
            }
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>With TLS key files counted, the closing line says the service re-asserts only the other items and tells
    /// the operator to re-run the verb after replacing a key file; with none counted it is the old sentence.</summary>
    [Fact]
    public void TheFullVerbsClosingLine_DoesNotCountKeyFilesAsReAsserted()
    {
        if (!WindowsOnly())
        {
            return;
        }

        Assert.SkipWhen(DarlingFileSecurity.RegisteredServiceAccount("PerformanceMonitor Darling") is not null,
            "The Darling service is registered on this machine.");
        var scratch = Directory.CreateTempSubdirectory("darling-5700-closing-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var pfx = MakeKey(install, "web.pfx");
            var config = WriteStoreLessConfig(install, scratch.FullName, "{\"pfxPath\":" + Json(pfx) + "}");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = DarlingCliCommands.HardenFiles(config, output, error);

            Assert.Equal(0, exit);
            Assert.Equal("", error.ToString());
            var closing = output.ToString().TrimEnd().Split(Environment.NewLine).Last();
            Assert.Equal(
                "All 2 item(s) secured. The service re-asserts the ACLs of the 1 item(s) that are not TLS key files at every start. "
                + "Re-run --harden-files after replacing a TLS key file.",
                closing);
            Assert.DoesNotContain("re-asserts these ACLs", output.ToString(), StringComparison.Ordinal);

            /* No key file named: the sentence is unchanged. */
            File.WriteAllText(config, "{\"postgres\":{\"managed\":false,\"connectionString\":\"Host=127.0.0.1;Username=darling\",\"dataDirectory\":"
                + Json(Path.Combine(scratch.FullName, "store", "pg")) + "}}");
            var plain = new StringWriter();
            Assert.Equal(0, DarlingCliCommands.HardenFiles(config, plain, new StringWriter()));
            Assert.EndsWith("All 1 item(s) secured. The service re-asserts these ACLs at every start.", plain.ToString().TrimEnd(), StringComparison.Ordinal);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A darling.json that does not load leaves no key file to check, and the verb says so instead of finishing as
    /// if it had.</summary>
    [Fact]
    public void TheFullVerb_SaysTheKeyFilesWereNotChecked_WhenTheConfigDoesNotLoad()
    {
        if (!WindowsOnly())
        {
            return;
        }

        Assert.SkipWhen(DarlingFileSecurity.RegisteredServiceAccount("PerformanceMonitor Darling") is not null,
            "The Darling service is registered on this machine.");
        var scratch = Directory.CreateTempSubdirectory("darling-5700-noload-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var config = Path.Combine(install, "darling.json");
            File.WriteAllText(config, "{ this is not json");
            var output = new StringWriter();
            var error = new StringWriter();

            DarlingCliCommands.HardenFiles(config, output, error);

            Assert.Contains("NOTE: could not load", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("The TLS key files named in darling.json were not checked, because darling.json did not load.", error.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A pfxPassword that is a file: reference is a key file too: inside the install folder it is hardened like a key
    /// (SYSTEM and Administrators full control, the service read only); outside it is reported and left alone.</summary>
    [Fact]
    public void AFilePasswordReference_IsHardenedInsideTheInstallFolder_AndReportedOutsideIt()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-password-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var outside = MakeFolder(Path.Combine(scratch.FullName, "outside"));
            var inside = MakeKey(install, "pfx-password.txt");
            var away = MakeKey(outside, "mcp-password.txt");
            var awayBefore = Snapshot(away);
            var config = Path.Combine(install, "darling.json");
            File.WriteAllText(config,
                "{\"postgres\":{\"managed\":false,\"connectionString\":\"Host=127.0.0.1;Username=darling\"},"
                + "\"web\":{\"network\":{\"tls\":{\"pfxPassword\":" + Json("file: " + inside) + "}}},"
                + "\"mcp\":{\"network\":{\"tls\":{\"pfxPassword\":" + Json("file:" + away) + "}}}}");

            var (exit, output, error) = Run(config);

            Assert.Equal(0, exit);
            Assert.Equal("", error);
            var lines = output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(2, lines.Length);
            Assert.Equal($"  SECURED  {inside} (the TLS key password file, web.network.tls.pfxPassword)", lines[0]);
            Assert.StartsWith($"  NOT CHANGED (outside the install folder)  {away} (the TLS key password file, mcp.network.tls.pfxPassword): ordinary users can read it", lines[1], StringComparison.Ordinal);
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(inside));
            Assert.Equal(awayBefore, Snapshot(away));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A password file over 4 KB, or one of the product's own file types, is refused and left as it was; a
    /// plaintext or env: password names no file.</summary>
    [Theory]
    [InlineData("big.txt", 4097)]
    [InlineData("settings.json", 20)]
    [InlineData("tool.DLL", 20)]
    [InlineData("run.ps1", 20)]
    [InlineData("app.config", 20)]
    public void APasswordFileThatIsTooBigOrAProductFile_IsRefused(string name, int size)
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-password-refused-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var file = MakeKey(install, name, Enumerable.Repeat((byte)'a', size).ToArray());
            var before = Snapshot(file);
            var config = WriteConfig(install, "{\"pfxPassword\":" + Json("file:" + file) + "}");

            var (exit, output, error) = Run(config);

            Assert.Equal(0, exit);
            Assert.Equal("", error);
            Assert.StartsWith($"  REFUSED  {file} (the TLS key password file, web.network.tls.pfxPassword): ", output, StringComparison.Ordinal);
            Assert.Contains("is not a password file", output, StringComparison.Ordinal);
            Assert.Equal(before, Snapshot(file));

            Assert.Empty(DarlingCliCommands.TlsKeyFilesNamed(DarlingConfig.Load(WriteConfig(install, "{\"pfxPassword\":\"env:EXAMPLE_PASSWORD\"}"))));
            Assert.Empty(DarlingCliCommands.TlsKeyFilesNamed(DarlingConfig.Load(WriteConfig(install, "{\"pfxPassword\":\"literal\"}"))));
            Assert.Empty(DarlingCliCommands.TlsKeyFilesNamed(DarlingConfig.Load(WriteConfig(install, "{\"pfxPassword\":\"FILE:C:\\\\x.txt\"}"))));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static string SnapshotFolder(string path) =>
        Convert.ToHexString(new DirectoryInfo(path).GetAccessControl().GetSecurityDescriptorBinaryForm());

    /// <summary>The install folder is itself a junction: the key behind it is not changed, and the report says which path
    /// is the link. A junction there is what lets a folder that ordinary users can rename send the change elsewhere.</summary>
    [Fact]
    public void AnInstallFolderThatIsAJunction_IsRefused_AndTheKeyBehindItKeepsItsAcl()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-rootlink-");
        try
        {
            var real = MakeFolder(Path.Combine(scratch.FullName, "real"));
            var pfx = MakeKey(real, "web.pfx");
            var before = Snapshot(pfx);
            var root = Path.Combine(scratch.FullName, "root");
            MakeJunction(root, real);
            Assert.SkipWhen(!File.Exists(Path.Combine(root, "web.pfx")), "A junction could not be made here.");
            var viaLink = Path.Combine(root, "web.pfx");
            var config = WriteConfig(root, "{\"pfxPath\":" + Json(viaLink) + "}");

            var (exit, output, _) = Run(config);

            Assert.Equal(0, exit);
            Assert.StartsWith($"  REFUSED  {viaLink} (the TLS key file, web.network.tls.pfxPath): {root} is a junction or symbolic link", output, StringComparison.Ordinal);
            Assert.Equal(before, Snapshot(pfx));
            Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(pfx));

            Assert.NotNull(DarlingFileSecurity.HardenKeyFileWithoutFollowingLinks(viaLink, root, DarlingFileSecurity.KeyFileKind.Pfx));
            Assert.Equal(before, Snapshot(pfx));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A junction above the install folder is refused too, though the check for links below the install folder
    /// does not look that far up.</summary>
    [Fact]
    public void AJunctionAboveTheInstallFolder_IsRefused_AndTheKeyBehindItKeepsItsAcl()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-parentlink-");
        try
        {
            var real = MakeFolder(Path.Combine(scratch.FullName, "real"));
            var realInstall = MakeFolder(Path.Combine(real, "install"));
            var pfx = MakeKey(realInstall, "web.pfx");
            var before = Snapshot(pfx);
            var link = Path.Combine(scratch.FullName, "link");
            MakeJunction(link, real);
            Assert.SkipWhen(!Directory.Exists(Path.Combine(link, "install")), "A junction could not be made here.");
            var install = Path.Combine(link, "install");
            var viaLink = Path.Combine(install, "web.pfx");
            var config = WriteConfig(install, "{\"pfxPath\":" + Json(viaLink) + "}");

            var (exit, output, _) = Run(config);

            Assert.Equal(0, exit);
            Assert.StartsWith($"  REFUSED  {viaLink} (the TLS key file, web.network.tls.pfxPath): {link} is a junction or symbolic link", output, StringComparison.Ordinal);
            Assert.Equal(before, Snapshot(pfx));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>The key's own name being a junction is refused by the check on the handle (the check the verb makes on the
    /// path first is not the only one): a junction needs no privilege to make, so this runs wherever the tests do.</summary>
    [Fact]
    public void AKeyThatIsAJunction_IsRefusedOnTheHandle_AndWhatItPointsAtKeepsItsAcl()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-leaflink-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var target = MakeFolder(Path.Combine(scratch.FullName, "target"));
            var before = SnapshotFolder(target);
            var leaf = Path.Combine(install, "web.pfx");
            MakeJunction(leaf, target);
            Assert.SkipWhen(!Directory.Exists(leaf), "A junction could not be made here.");

            var refusal = DarlingFileSecurity.HardenKeyFileWithoutFollowingLinks(leaf, install, DarlingFileSecurity.KeyFileKind.Pfx);

            Assert.Equal($"{leaf} is a junction or symbolic link", refusal);
            Assert.Equal(before, SnapshotFolder(target));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A key that is a symbolic link to a file is refused on the handle, and the file it points at is not changed.
    /// A file symbolic link needs a privilege (an elevated process, or developer mode), so this is skipped without one.</summary>
    [Fact]
    public void AKeyThatIsAFileSymbolicLink_IsRefusedOnTheHandle_AndTheFileItPointsAtKeepsItsAcl()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-symlink-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var elsewhere = MakeFolder(Path.Combine(scratch.FullName, "elsewhere"));
            var victim = MakeKey(elsewhere, "someones.pfx");
            var before = Snapshot(victim);
            var leaf = Path.Combine(install, "web.pfx");
            try
            {
                File.CreateSymbolicLink(leaf, victim);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                Assert.Skip("A file symbolic link needs an elevated process or developer mode: " + ex.Message);
            }

            var refusal = DarlingFileSecurity.HardenKeyFileWithoutFollowingLinks(leaf, install, DarlingFileSecurity.KeyFileKind.Pfx);

            Assert.Equal($"{leaf} is a junction or symbolic link", refusal);
            Assert.Equal(before, Snapshot(victim));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>
    /// Every folder from the volume root down to the key's folder is held open before the key is opened, and the key too
    /// until the ACL is applied: none can be renamed (a share check refuses the delete access a rename needs). Once the
    /// call returns they are released.
    /// </summary>
    [Fact]
    public void EveryFolderOnTheKeysPath_IsHeldAgainstARename_WhileTheAclIsApplied()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-pinned-");
        try
        {
            var outer = MakeFolder(Path.Combine(scratch.FullName, "outer"));
            var install = MakeFolder(Path.Combine(outer, "install"));
            var certs = MakeFolder(Path.Combine(install, "certs"));
            var pfx = MakeKey(certs, "web.pfx");
            var attempts = new System.Collections.Generic.List<string>();
            var folderStage = new System.Collections.Generic.List<string>();

            void TryRename(string from, string label)
            {
                var to = from + "-renamed";
                try
                {
                    if (Directory.Exists(from))
                    {
                        Directory.Move(from, to);
                        Directory.Move(to, from);
                    }
                    else
                    {
                        File.Move(from, to);
                        File.Move(to, from);
                    }

                    attempts.Add($"{label}: renamed");
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    attempts.Add($"{label}: held");
                }
            }

            var refusal = DarlingFileSecurity.HardenKeyFileWithoutFollowingLinks(
                pfx, install, DarlingFileSecurity.KeyFileKind.Pfx,
                stage =>
                {
                    /* The key is not open yet at the first stage, so a folder held then is held by the folder chain alone. */
                    if (stage == DarlingFileSecurity.PinnedFolders)
                    {
                        TryRename(certs, "the key's folder");
                        TryRename(install, "the install folder");
                        TryRename(outer, "the folder above the install folder");
                        TryRename(scratch.FullName, "the scratch folder");
                        folderStage.AddRange(attempts);
                        attempts.Clear();
                    }
                    else
                    {
                        TryRename(pfx, "the key");
                    }
                });

            Assert.Null(refusal);
            Assert.Equal(
                new[] { "the key's folder: held", "the install folder: held", "the folder above the install folder: held", "the scratch folder: held" },
                folderStage);
            Assert.Equal(new[] { "the key: held" }, attempts);

            /* Nothing is held once the call returns. */
            Directory.Move(outer, outer + "-after");
            Directory.Move(outer + "-after", outer);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>The config file the verb loaded is not a key file, though its text can look like one: it is refused as a key,
    /// and so is a backup of it. Neither gets the key ACL, which would take the operator's read of the config away.</summary>
    [Fact]
    public void TheConfigFileAndItsBackups_AreRefusedAsKeys_AndKeepTheirAcl()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-notkey-config-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var configPath = Path.Combine(install, "darling.json");
            var backup = MakeKey(install, "darling.json.bak-20260101", System.Text.Encoding.ASCII.GetBytes(PemKeyText));
            File.WriteAllText(configPath,
                "{\"postgres\":{\"managed\":false,\"connectionString\":\"Host=127.0.0.1;Username=darling;ApplicationName=-----BEGIN PRIVATE KEY-----\"},"
                + "\"web\":{\"network\":{\"tls\":{\"keyPath\":" + Json(configPath) + "}}},"
                + "\"mcp\":{\"network\":{\"tls\":{\"keyPath\":" + Json(backup) + "}}}}");
            Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(configPath));
            var configBefore = Snapshot(configPath);
            var backupBefore = Snapshot(backup);

            var (exit, output, _) = Run(configPath);

            Assert.Equal(0, exit);
            Assert.Contains($"  REFUSED  {configPath} (the TLS key file, web.network.tls.keyPath): it is the live config", output, StringComparison.Ordinal);
            Assert.Contains($"  REFUSED  {backup} (the TLS key file, mcp.network.tls.keyPath): it is a config backup", output, StringComparison.Ordinal);
            Assert.Equal(configBefore, Snapshot(configPath));
            Assert.Equal(backupBefore, Snapshot(backup));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A store credential file, named as a key, is refused with its own access rule left as it is.</summary>
    [Fact]
    public void AStoreCredentialFile_IsRefusedAsAKey_AndKeepsItsAcl()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-notkey-store-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var store = MakeFolder(Path.Combine(install, "store"));
            var credential = MakeKey(store, "pg-credential.dpapi", System.Text.Encoding.ASCII.GetBytes(PemKeyText));
            var before = Snapshot(credential);
            var config = Path.Combine(install, "darling.json");
            File.WriteAllText(config,
                "{\"postgres\":{\"managed\":false,\"connectionString\":\"Host=127.0.0.1;Username=darling\",\"dataDirectory\":" + Json(Path.Combine(store, "pg")) + "},"
                + "\"web\":{\"network\":{\"tls\":{\"keyPath\":" + Json(credential) + "}}}}");

            var (exit, output, _) = Run(config);

            Assert.Equal(0, exit);
            Assert.Contains($"  REFUSED  {credential} (the TLS key file, web.network.tls.keyPath): it is the store credential", output, StringComparison.Ordinal);
            Assert.Equal(before, Snapshot(credential));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>A path with a data stream name ends in a colon part: a handle to the stream answers for its file, so the path
    /// is refused and the file keeps its ACL even though the stream holds a bundle.</summary>
    [Fact]
    public void APathWithADataStreamName_IsRefused_AndTheFileKeepsItsAcl()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-stream-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var host = MakeKey(install, "web.txt", System.Text.Encoding.ASCII.GetBytes("host"));
            var stream = host + ":key";
            File.WriteAllBytes(stream, DerBundle());
            var before = Snapshot(host);
            var config = WriteConfig(install, "{\"pfxPath\":" + Json(stream) + "}");

            var (exit, output, _) = Run(config);

            Assert.Equal(0, exit);
            Assert.StartsWith($"  REFUSED  {stream} (the TLS key file, web.network.tls.pfxPath): names an alternate data stream", output, StringComparison.Ordinal);
            Assert.Equal(before, Snapshot(host));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    [Theory]
    [InlineData(new string[0], null, false, true)]
    [InlineData(new[] { "--tls-key-files-only" }, null, true, true)]
    [InlineData(new[] { "--config", @"C:\x\darling.json" }, @"C:\x\darling.json", false, true)]
    [InlineData(new[] { "--tls-key-files-only", "--config", @"C:\x\darling.json" }, @"C:\x\darling.json", true, true)]
    [InlineData(new[] { "--CONFIG", @"C:\x\darling.json", "--TLS-KEY-FILES-ONLY" }, @"C:\x\darling.json", true, true)]
    [InlineData(new[] { @"C:\x\darling.json" }, @"C:\x\darling.json", false, true)]
    [InlineData(new[] { "--config" }, null, false, false)]
    [InlineData(new[] { "--config", "--tls-key-files-only" }, null, true, false)]
    [InlineData(new[] { "--config", @"C:\a.json", @"C:\b.json" }, @"C:\a.json", false, false)]
    [InlineData(new[] { @"C:\a.json", "--config", @"C:\b.json" }, @"C:\a.json", false, false)]
    [InlineData(new[] { "--bogus" }, null, false, false)]
    public void TheHardenFilesArguments_TakeTheConfigAsDashDashConfigOrOneBarePath(string[] rest, string? expectedPath, bool expectedTlsOnly, bool valid)
    {
        var parsed = DarlingCliCommands.TryParseHardenFilesArgs(rest, out var configPath, out var tlsKeyFilesOnly, out var message);

        Assert.Equal(valid, parsed);
        if (valid)
        {
            Assert.Equal(expectedPath, configPath);
            Assert.Equal(expectedTlsOnly, tlsKeyFilesOnly);
            Assert.Null(message);
        }
        else
        {
            Assert.Contains("Usage: --harden-files [--config <path to darling.json>]", message, StringComparison.Ordinal);
        }
    }

    /// <summary>The verb's first output line names the config file it checked; nothing else is said about where it came from
    /// when a path was passed, even with the environment variable set.</summary>
    [Fact]
    public void TheTlsKeyFilesOnlyStep_StartsByNamingTheConfigFileItChecked()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-firstline-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var config = WriteConfig(install, "{}");
            var other = Path.Combine(scratch.FullName, "exported.json");

            var output = new StringWriter();
            var exit = DarlingCliCommands.HardenTlsKeyFilesOnly(config, output, new StringWriter(), true, ServiceSid, name => name == "DARLING_CONFIG" ? other : null);

            var lines = output.ToString().ReplaceLineEndings("\n").Split('\n');
            Assert.Equal(0, exit);
            Assert.Equal($"Config file: {config}", lines[0]);
            Assert.Equal("No TLS key files are named in darling.json.", lines[1]);
            Assert.DoesNotContain("DARLING_CONFIG", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>When no path was passed and DARLING_CONFIG is set, the second line says the config came from that variable
    /// (the service does not read it), and the verb really checks that file.</summary>
    [Fact]
    public void WhenTheConfigPathComesFromDarlingConfig_TheSecondLineSaysSo()
    {
        if (!WindowsOnly())
        {
            return;
        }

        var scratch = Directory.CreateTempSubdirectory("darling-5700-envline-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var pfx = MakeKey(install, "web.pfx");
            var config = WriteConfig(install, "{\"pfxPath\":" + Json(pfx) + "}");

            var output = new StringWriter();
            var exit = DarlingCliCommands.HardenTlsKeyFilesOnly(null, output, new StringWriter(), true, ServiceSid, name => name == "DARLING_CONFIG" ? config : null);

            var lines = output.ToString().ReplaceLineEndings("\n").Split('\n');
            Assert.Equal(0, exit);
            Assert.Equal($"Config file: {config}", lines[0]);
            Assert.StartsWith("  (taken from the DARLING_CONFIG environment variable, because no --config was passed.", lines[1], StringComparison.Ordinal);
            Assert.Equal($"  SECURED  {pfx} (the TLS key file, web.network.tls.pfxPath)", lines[2]);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>The full verb opens the same way.</summary>
    [Fact]
    public void TheFullVerb_StartsByNamingTheConfigFileItChecked_AndWhereThePathCameFrom()
    {
        if (!WindowsOnly())
        {
            return;
        }

        Assert.SkipWhen(DarlingFileSecurity.RegisteredServiceAccount("PerformanceMonitor Darling") is not null,
            "The Darling service is registered on this machine.");
        var scratch = Directory.CreateTempSubdirectory("darling-5700-fullline-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var config = WriteStoreLessConfig(install, scratch.FullName, "{}");

            var passed = new StringWriter();
            DarlingCliCommands.HardenFiles(config, passed, new StringWriter(), name => name == "DARLING_CONFIG" ? "C:\\elsewhere.json" : null);
            var passedLines = passed.ToString().ReplaceLineEndings("\n").Split('\n');
            Assert.Equal($"Config file: {config}", passedLines[0]);
            Assert.DoesNotContain("DARLING_CONFIG", passed.ToString(), StringComparison.Ordinal);

            var fromVariable = new StringWriter();
            DarlingCliCommands.HardenFiles(null, fromVariable, new StringWriter(), name => name == "DARLING_CONFIG" ? config : null);
            var variableLines = fromVariable.ToString().ReplaceLineEndings("\n").Split('\n');
            Assert.Equal($"Config file: {config}", variableLines[0]);
            Assert.StartsWith("  (taken from the DARLING_CONFIG environment variable, because no --config was passed.", variableLines[1], StringComparison.Ordinal);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
    }

    /// <summary>The store's own TLS key is a --harden-files target (read by the service only, no operator read); one that is
    /// not there is skipped like the other store files.</summary>
    [Fact]
    public void TheStoresTlsKey_IsAHardenFilesTarget()
    {
        if (!WindowsOnly())
        {
            return;
        }

        Assert.SkipWhen(DarlingFileSecurity.RegisteredServiceAccount("PerformanceMonitor Darling") is not null,
            "The Darling service is registered on this machine.");
        var scratch = Directory.CreateTempSubdirectory("darling-5700-serverkey-");
        try
        {
            var install = MakeFolder(Path.Combine(scratch.FullName, "install"));
            var store = MakeFolder(Path.Combine(scratch.FullName, "store"));
            var serverKey = MakeKey(store, DarlingManagedPostgres.ServerKeyFileName);
            var config = WriteStoreLessConfig(install, scratch.FullName, "{}");
            var output = new StringWriter();
            var error = new StringWriter();

            var exit = DarlingCliCommands.HardenFiles(config, output, error);

            Assert.Equal(0, exit);
            Assert.Contains($"  SECURED  {serverKey} (the store's TLS key)", output.ToString(), StringComparison.Ordinal);
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(serverKey));

            File.Delete(serverKey);
            var again = new StringWriter();
            Assert.Equal(0, DarlingCliCommands.HardenFiles(config, again, new StringWriter()));
            Assert.DoesNotContain("TLS key", again.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(scratch.FullName);
        }
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
