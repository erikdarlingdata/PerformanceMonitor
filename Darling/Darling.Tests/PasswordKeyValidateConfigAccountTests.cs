/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8006: on Windows <c>--validate-config</c> judges a <c>file:</c> <c>passwordKey</c> as the account the service is registered
/// under, not as the account that runs the verb, because the service reads the key at start. A key in a folder the test's
/// account owns passes when judged as that account, and is refused when another account is the service's. With no service
/// registered the verb keeps judging as the caller, so a console run or a check before install is unchanged.
///
/// <para>In <c>darling-config-env</c> (it runs alone) because the verb names the registered account as the service's through
/// <see cref="DarlingFileSecurity.HardenForAccount"/>, a process-wide setting, and the tests put the current user back after.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class PasswordKeyValidateConfigAccountTests
{
    /* An account the service could be registered under that is not the test's own; no such account exists. */
    private static readonly SecurityIdentifier s_registeredAccount = new("S-1-5-80-8006-5-6-7-8");

    private static SecurityIdentifier CurrentUser => WindowsIdentity.GetCurrent().User!;

    [Fact]
    public async Task OnWindows_ARegisteredService_JudgesTheKeyAsItsAccount_SoAFolderTheCallerOwnsIsRefused()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Owner SIDs and the service registration are Windows-only.");
        Assert.SkipWhen(CurrentUser.IsWellKnown(WellKnownSidType.LocalSystemSid), "SYSTEM is trusted as every account, so the folder it owns is never refused.");

        var run = await ValidateAsync(registeredServiceAccount: _ => s_registeredAccount);

        Assert.Contains("WARNING: passwordKey: file '" + run.KeyPath + "' is not read, because its folder " + run.KeyFolder + " is not trusted", run.Output, StringComparison.Ordinal);
        Assert.DoesNotContain("passwordKey: password key ", run.Output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnWindows_NoRegisteredService_JudgesTheKeyAsTheCaller_SoTheSameFolderPasses()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Owner SIDs and the service registration are Windows-only.");
        Assert.SkipWhen(CurrentUser.IsWellKnown(WellKnownSidType.LocalSystemSid), "SYSTEM is trusted as every account, so the folder it owns is never refused.");

        var run = await ValidateAsync(registeredServiceAccount: _ => null);

        Assert.Contains("passwordKey: password key ", run.Output, StringComparison.Ordinal);
        Assert.DoesNotContain("is not trusted", run.Output, StringComparison.Ordinal);
    }

    private sealed record Run(string Output, string KeyPath, string KeyFolder);

    /// <summary>Runs the verb with a <c>file:</c> key in a folder the current user owns and can only read, which nothing but
    /// the folder's owner makes untrusted, and puts the current user back as the account judged afterwards.</summary>
    private static async Task<Run> ValidateAsync(Func<string, SecurityIdentifier?> registeredServiceAccount)
    {
        var dir = Directory.CreateTempSubdirectory("password-key-validate-account-").FullName;
        var keyFolder = Path.Combine(dir, "keys");
        var folder = Directory.CreateDirectory(keyFolder);
        byte[]? pkcs8 = null;
        try
        {
            using (var key = PasswordPrivateKey.Generate())
            {
                pkcs8 = key.ExportPkcs8();
            }

            var keyPath = Path.Combine(keyFolder, "password-key");
            File.WriteAllText(keyPath, Convert.ToBase64String(pkcs8));

            var owner = folder.GetAccessControl();
            owner.SetOwner(CurrentUser);
            folder.SetAccessControl(owner);
            var readOnly = new DirectorySecurity();
            readOnly.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
            foreach (var sid in new[] { new SecurityIdentifier(WellKnownSidType.LocalSystemSid, null), new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null) })
            {
                readOnly.AddAccessRule(new FileSystemAccessRule(
                    sid, FileSystemRights.FullControl, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                    PropagationFlags.None, AccessControlType.Allow));
            }

            readOnly.AddAccessRule(new FileSystemAccessRule(
                CurrentUser, FileSystemRights.ReadAndExecute, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                PropagationFlags.None, AccessControlType.Allow));
            folder.SetAccessControl(readOnly);

            var configPath = Path.Combine(dir, "darling.json");
            /* A closed local port: the store probe fails at once and the verb falls back to the (empty) file list. */
            var connectionString = $"Host=127.0.0.1;Port={StoreParseFailureFixtures.GetClosedPort()};Username=u;Password=p;Database=d";
            File.WriteAllText(
                configPath,
                "{\"postgres\":{\"connectionString\":" + JsonSerializer.Serialize(connectionString) + "},"
                + "\"passwordKey\":" + JsonSerializer.Serialize("file:" + keyPath) + "}");

            using var output = new StringWriter();
            using var error = new StringWriter();
            try
            {
                await DarlingCliCommands.ValidateConfigAsync(
                    configPath, output, error, registeredServiceAccount, TestContext.Current.CancellationToken);
            }
            finally
            {
                DarlingFileSecurity.HardenForAccount(CurrentUser);
            }

            return new Run(output.ToString(), keyPath, keyFolder);
        }
        finally
        {
            if (pkcs8 is not null)
            {
                CryptographicOperations.ZeroMemory(pkcs8);
            }

            /* The test's account owns the folder, so it may give itself full control back before the delete. */
            var security = folder.GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(
                CurrentUser, FileSystemRights.FullControl, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                PropagationFlags.None, AccessControlType.Allow));
            folder.SetAccessControl(security);
            Directory.Delete(dir, recursive: true);
        }
    }
}
