/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live facts mint their own scratch database through ScratchPostgres and never touch another
   test's rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The password key tools (#9644): <c>new_password_key</c> and <c>reset_password_key</c> through their handlers. They call the
/// code the CLI verbs call, so these facts pin what an MCP caller sees: the answer holds no key, a new key lands only in the
/// service's own pending folder under a name the service makes, an unconfirmed or unreachable reset changes nothing, a reset needs
/// the current key's fingerprint, and a confirmed reset marks the published key replaced and counts what it leaves to re-enter.
/// Which tools are served is pinned with the other server admin tools.
/// </summary>
public sealed class DarlingMcpPasswordKeyToolsTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Status(string json) => JsonDocument.Parse(json).RootElement.GetProperty("status").GetString()!;

    private static string Message(string json) => JsonDocument.Parse(json).RootElement.GetProperty("message").GetString()!;

    /// <summary>A temp folder holding a credentials folder with an active password key file in it, the way a started service has one.</summary>
    private static (string Root, string Credentials, string Pending) Service()
    {
        var root = Path.Combine(Path.GetTempPath(), "pmpendkey-" + Guid.NewGuid().ToString("N")[..12]);
        var credentials = Path.Combine(root, "credentials");
        Directory.CreateDirectory(credentials);
        File.WriteAllText(Path.Combine(credentials, DarlingPasswordKeyFile.FileName), "placeholder");
        return (root, credentials, Path.Combine(credentials, DarlingPendingPasswordKeys.DirectoryName));
    }

    private const UnixFileMode Mode755 =
        UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute
        | UnixFileMode.GroupRead | UnixFileMode.GroupExecute | UnixFileMode.OtherRead | UnixFileMode.OtherExecute;

    [Fact]
    public void NewPasswordKey_WritesIntoThePendingFolderUnderAGeneratedName_AnswersWithPathAndFingerprintOnly()
    {
        var (root, credentials, pending) = Service();
        try
        {
            var first = DarlingMcpPasswordKeyTools.NewPasswordKeyIn(credentials, fromSetting: false, logger: null);

            Assert.Equal("written", Status(first));
            using var document = JsonDocument.Parse(first);
            var path = document.RootElement.GetProperty("path").GetString()!;
            var fingerprint = document.RootElement.GetProperty("fingerprint").GetString()!;
            Assert.Equal(Path.Combine(pending, $"new-password-key-{fingerprint}.key"), path);
            var keyText = File.ReadAllText(path).Trim();
            Assert.True(keyText.Length > 100, "the file holds the base64 key");
            Assert.DoesNotContain(keyText, first, StringComparison.Ordinal);

            if (!OperatingSystem.IsWindows())
            {
                Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(path));
                Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute, File.GetUnixFileMode(pending));
            }
            else
            {
                Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(pending), "the new folder is readable by ordinary users");
            }

            // The folder the first call made is accepted by the second, which writes a different key and leaves the first as it was.
            var second = DarlingMcpPasswordKeyTools.NewPasswordKeyIn(credentials, fromSetting: false, logger: null);
            Assert.Equal("written", Status(second));
            Assert.NotEqual(path, JsonDocument.Parse(second).RootElement.GetProperty("path").GetString());
            Assert.Equal(keyText, File.ReadAllText(path).Trim());
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void NewPasswordKey_TakesNoPathArgument()
    {
        var parameters = typeof(DarlingMcpPasswordKeyTools).GetMethod(nameof(DarlingMcpPasswordKeyTools.NewPasswordKey))!.GetParameters();

        Assert.DoesNotContain(parameters, p => p.ParameterType == typeof(string));
    }

    [Fact]
    public void NewPasswordKey_IsRefused_WhenTheActiveKeyIsNotAFileTheServiceKeeps_AndWritesNothingAnywhere()
    {
        var (root, credentials, pending) = Service();
        var working = Directory.GetFileSystemEntries(Directory.GetCurrentDirectory()).Length;
        var noKeyFile = Path.Combine(root, "empty-credentials");
        Directory.CreateDirectory(noKeyFile);
        try
        {
            // darling.json sets passwordKey: the key lives in an environment variable or a secret store, not in a folder the service owns.
            var fromSetting = DarlingMcpPasswordKeyTools.NewPasswordKeyIn(credentials, fromSetting: true, logger: null);
            // The credentials folder could not be found at all.
            var unresolved = DarlingMcpPasswordKeyTools.NewPasswordKeyIn(null, fromSetting: false, logger: null);
            // The service has no key file yet.
            var noKeyYet = DarlingMcpPasswordKeyTools.NewPasswordKeyIn(noKeyFile, fromSetting: false, logger: null);

            foreach (var answer in new[] { fromSetting, unresolved, noKeyYet })
            {
                Assert.Equal("refused", Status(answer));
            }

            Assert.False(Directory.Exists(pending));
            Assert.Equal(working, Directory.GetFileSystemEntries(Directory.GetCurrentDirectory()).Length);
            Assert.Empty(Directory.GetFileSystemEntries(AppContext.BaseDirectory, "new-password-key-*"));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void NewPasswordKey_RefusesAPendingFolderThatIsALink_AndWritesNothingThroughIt()
    {
        var (root, credentials, pending) = Service();
        try
        {
            var elsewhere = Path.Combine(root, "elsewhere");
            Directory.CreateDirectory(elsewhere);
            if (OperatingSystem.IsWindows())
            {
                using var mklink = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = $"/c mklink /J \"{pending}\" \"{elsewhere}\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                })!;
                mklink.WaitForExit();
                Assert.SkipUnless(mklink.ExitCode == 0, "could not create a junction on this machine");
            }
            else
            {
                Directory.CreateSymbolicLink(pending, elsewhere);
            }

            var answer = DarlingMcpPasswordKeyTools.NewPasswordKeyIn(credentials, fromSetting: false, logger: null);

            Assert.Equal("refused", Status(answer));
            Assert.Empty(Directory.GetFileSystemEntries(elsewhere));
        }
        finally
        {
            // The link goes first, on its own, so the recursive delete never follows it.
            if (new DirectoryInfo(pending).LinkTarget is not null)
            {
                Directory.Delete(pending);
            }

            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void NewPasswordKey_RefusesAnExistingPendingFolderOthersCanReach_AndDoesNotFixItInPlace()
    {
        var (root, credentials, pending) = Service();
        try
        {
            Directory.CreateDirectory(pending);
            if (OperatingSystem.IsWindows())
            {
                OpenToEveryUser(pending);
            }
            else
            {
                File.SetUnixFileMode(pending, Mode755);
            }

            var answer = DarlingMcpPasswordKeyTools.NewPasswordKeyIn(credentials, fromSetting: false, logger: null);

            Assert.Equal("refused", Status(answer));
            Assert.Empty(Directory.GetFileSystemEntries(pending));
            if (OperatingSystem.IsWindows())
            {
                Assert.NotNull(DarlingFileSecurity.WriteAccessBeyondTrusted(pending));
            }
            else
            {
                Assert.Equal(Mode755, File.GetUnixFileMode(pending));
            }
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static void OpenToEveryUser(string directory)
    {
        var info = new DirectoryInfo(directory);
        var security = info.GetAccessControl();
        security.AddAccessRule(new System.Security.AccessControl.FileSystemAccessRule(
            new System.Security.Principal.SecurityIdentifier(System.Security.Principal.WellKnownSidType.BuiltinUsersSid, null),
            System.Security.AccessControl.FileSystemRights.Modify,
            System.Security.AccessControl.InheritanceFlags.ContainerInherit | System.Security.AccessControl.InheritanceFlags.ObjectInherit,
            System.Security.AccessControl.PropagationFlags.None,
            System.Security.AccessControl.AccessControlType.Allow));
        info.SetAccessControl(security);
    }

    [Fact]
    public void NewPasswordKey_RefusesTheSixthWaitingKeyFile_AndWritesNothingMore()
    {
        var (root, credentials, pending) = Service();
        try
        {
            for (var i = 0; i < DarlingPendingPasswordKeys.MaxPending; i++)
            {
                Assert.Equal("written", Status(DarlingMcpPasswordKeyTools.NewPasswordKeyIn(credentials, fromSetting: false, logger: null)));
            }

            var sixth = DarlingMcpPasswordKeyTools.NewPasswordKeyIn(credentials, fromSetting: false, logger: null);

            Assert.Equal("refused", Status(sixth));
            Assert.Equal(DarlingPendingPasswordKeys.MaxPending, Directory.GetFiles(pending).Length);

            // Deleting one makes room again.
            File.Delete(Directory.GetFiles(pending)[0]);
            Assert.Equal("written", Status(DarlingMcpPasswordKeyTools.NewPasswordKeyIn(credentials, fromSetting: false, logger: null)));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task ResetPasswordKey_WithoutTheOwnerConnection_IsRefused_ForACallWithOrWithoutConfirm()
    {
        foreach (var confirm in new[] { false, true })
        {
            var answer = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(
                confirm, confirmFingerprint: "AAAA-BBBB-CCCC-DDDD", owner: null, fromSetting: false, logger: null, TestContext.Current.CancellationToken);
            Assert.Equal("refused", Status(answer));
            Assert.Equal(DarlingMcpPasswordKeyTools.StoreNotReadyText, Message(answer));
        }
    }

    [Fact]
    public async Task ResetPasswordKey_NeedsTheCurrentKeysFingerprint_AndMarksThePublishedKeyReplaced_CountsTheSealedPasswords_AndLogsNoSecret()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key reset tool against a scratch store.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var logger = new CapturingTestLogger();
            // The key start itself is not run: it needs the account's own key file protection, which not every machine allows a test.
            // The store is given a published key the way the start publishes one, and the tool is asked to replace it.
            using var key = PasswordPrivateKey.Generate();
            var keyId = key.PublicKey.KeyId;
            var fingerprint = PasswordSeal.DisplayKeyId(keyId);
            var keyText = Convert.ToBase64String(key.ExportPkcs8());
            await using (var publish = await source.OpenConnectionAsync(ct))
            {
                await DarlingPasswordKeyStore.PublishAsync(publish, keyId, key.PublicKey.Spki, ct);
            }

            await SealMailPasswordAsync(source, ct);

            // Unconfirmed: nothing changes, and the answer carries the count and the fingerprint the confirmed call must repeat.
            var preview = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(false, null, source, fromSetting: false, logger, ct);
            using (var previewDocument = JsonDocument.Parse(preview))
            {
                Assert.Equal("needs_confirmation", previewDocument.RootElement.GetProperty("status").GetString());
                Assert.Equal(1, previewDocument.RootElement.GetProperty("saved_passwords_to_re_enter").GetInt32());
                Assert.Equal(fingerprint, previewDocument.RootElement.GetProperty("key_fingerprint").GetString());
            }

            Assert.Equal(keyId, (await ReadStateAsync(source, ct)).CurrentKeyId);

            // Confirmed without a fingerprint, or with another key's: refused, and the key is still current.
            var missing = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(true, null, source, fromSetting: false, logger, ct);
            Assert.Equal("refused", Status(missing));
            Assert.Equal(keyId, (await ReadStateAsync(source, ct)).CurrentKeyId);

            using var other = PasswordPrivateKey.Generate();
            var wrong = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(
                true, PasswordSeal.DisplayKeyId(other.PublicKey.KeyId), source, fromSetting: false, logger, ct);
            Assert.Equal("refused", Status(wrong));
            var untouched = await ReadStateAsync(source, ct);
            Assert.Equal(keyId, untouched.CurrentKeyId);
            Assert.Null(untouched.OldState);

            // Confirmed with the right fingerprint: the key is replaced and the answer says how many passwords are left to re-enter, in the verb's words.
            var resetLogger = new CapturingTestLogger();
            var answer = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(true, fingerprint, source, fromSetting: false, resetLogger, ct);
            Assert.Equal(1, resetLogger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            using var document = JsonDocument.Parse(answer);
            Assert.Equal("reset", document.RootElement.GetProperty("status").GetString());
            Assert.Equal(1, document.RootElement.GetProperty("saved_passwords_to_re_enter").GetInt32());
            Assert.Equal(DarlingCliCommands.ResetPasswordKeyRestartSentence(1), document.RootElement.GetProperty("message").GetString());
            var after = await ReadStateAsync(source, ct);
            Assert.Null(after.CurrentKeyId);
            Assert.Equal(("replaced", "reset"), (after.OldState, after.OldReason));

            // A confirmed reset with no current key to replace is answered nothing_to_reset and is not logged as a reset.
            var noopLogger = new CapturingTestLogger();
            var noop = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(true, fingerprint, source, fromSetting: false, noopLogger, ct);
            Assert.Equal("nothing_to_reset", Status(noop));
            Assert.Equal(0, noopLogger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

            // A second reset finds no current key.
            var again = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(false, null, source, fromSetting: false, logger, ct);
            Assert.Equal("nothing_to_reset", Status(again));

            // Neither the key, nor the sealed value, appears in the answers or the log.
            foreach (var text in new[] { preview, missing, wrong, answer, again, logger.Joined })
            {
                Assert.DoesNotContain(keyText, text, StringComparison.Ordinal);
                Assert.DoesNotContain("0123456789abcdef", text, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ResetPassword_CountsAPasswordSavedWhileItWaitsForTheKeyRowLock()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key reset against a scratch store.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            using var key = PasswordPrivateKey.Generate();
            await using (var publish = await source.OpenConnectionAsync(ct))
            {
                await DarlingPasswordKeyStore.PublishAsync(publish, key.PublicKey.KeyId, key.PublicKey.Spki, ct);
            }

            // Another session holds the current key's row, so the reset's mark has to wait for it.
            await using var holder = await source.OpenConnectionAsync(ct);
            await using var holderTransaction = await holder.BeginTransactionAsync(ct);
            await using (var hold = new NpgsqlCommand("UPDATE config.password_key SET state = state WHERE state = 'current';", holder, holderTransaction))
            {
                await hold.ExecuteNonQueryAsync(ct);
            }

            var reset = DarlingCliCommands.ResetPasswordKeyInStoreAsync(source, fromSetting: false, ct);

            await using (var watch = await source.OpenConnectionAsync(ct))
            {
                var waiting = false;
                for (var i = 0; i < 200 && !waiting; i++)
                {
                    await using var look = new NpgsqlCommand(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock';", watch);
                    waiting = Convert.ToInt32(await look.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) > 0;
                    if (!waiting)
                    {
                        await Task.Delay(50, ct);
                    }
                }

                Assert.True(waiting, "the reset never reached the key row lock");
            }

            // A password is saved while the reset waits, then the other session lets go.
            await SealMailPasswordAsync(source, ct);
            await holderTransaction.RollbackAsync(ct);

            var outcome = await reset;

            Assert.True(outcome.Replaced);
            Assert.Equal(1, outcome.SealedPasswords);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task SealMailPasswordAsync(NpgsqlDataSource source, System.Threading.CancellationToken ct)
    {
        await using var seed = await source.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(@"
INSERT INTO config.config_notification (id, smtp_host, smtp_port, smtp_use_ssl, smtp_username, smtp_encrypted_password)
VALUES (1, 'smtp.example.com', 587, TRUE, 'mail_login', 'sealed:v1:0123456789abcdef:AAAA')
ON CONFLICT (id) DO UPDATE SET smtp_encrypted_password = EXCLUDED.smtp_encrypted_password;", seed);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(string? CurrentKeyId, string? OldState, string? OldReason)> ReadStateAsync(NpgsqlDataSource source, System.Threading.CancellationToken ct)
    {
        await using var c = await source.OpenConnectionAsync(ct);
        await using var current = new NpgsqlCommand("SELECT key_id FROM config.password_key WHERE state = 'current';", c);
        var currentKey = (string?)await current.ExecuteScalarAsync(ct);
        await using var old = new NpgsqlCommand("SELECT state, replaced_reason FROM config.password_key WHERE state <> 'current' LIMIT 1;", c);
        await using var reader = await old.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct)
            ? (currentKey, reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1))
            : (currentKey, null, null);
    }
}
