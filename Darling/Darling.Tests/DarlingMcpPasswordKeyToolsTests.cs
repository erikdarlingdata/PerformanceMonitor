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

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The password key tools (#9644): <c>new_password_key</c>, <c>reset_password_key</c> and <c>self_check_password_key</c>
/// through their handlers. They call the code the CLI verbs call, so these facts pin what an MCP caller sees: the answer holds
/// no key, an unconfirmed or unreachable reset changes nothing, and a confirmed reset marks the published key replaced and
/// counts what it leaves to re-enter. Which tools are served is pinned with the other server admin tools.
/// </summary>
public sealed class DarlingMcpPasswordKeyToolsTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Status(string json) => JsonDocument.Parse(json).RootElement.GetProperty("status").GetString()!;

    private static string Message(string json) => JsonDocument.Parse(json).RootElement.GetProperty("message").GetString()!;

    [Fact]
    public void NewPasswordKey_WritesTheKeyToTheFile_AnswersWithOnlyTheFingerprint_AndWritesOverNothing()
    {
        var directory = Path.Combine(Path.GetTempPath(), "pmnewkey-" + Guid.NewGuid().ToString("N")[..12]);
        Directory.CreateDirectory(directory);
        try
        {
            var path = Path.Combine(directory, "password-key.txt");

            var first = DarlingMcpPasswordKeyTools.NewPasswordKey(path);

            Assert.Equal("written", Status(first));
            var keyText = File.ReadAllText(path).Trim();
            Assert.True(keyText.Length > 100, "the file holds the base64 key");
            Assert.DoesNotContain(keyText, first, StringComparison.Ordinal);
            Assert.Contains("fingerprint", Message(first), StringComparison.Ordinal);

            // A second call over the same path is refused and the file is exactly as it was.
            var second = DarlingMcpPasswordKeyTools.NewPasswordKey(path);
            Assert.Equal("failed", Status(second));
            Assert.Equal(keyText, File.ReadAllText(path).Trim());
            Assert.DoesNotContain(keyText, second, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Theory]
    [InlineData("password-key.txt")]
    [InlineData("")]
    [InlineData("--config")]
    public void NewPasswordKey_RefusesARelativePathABlankOneAndAnOptionLookingOne_AndWritesNothing(string path)
    {
        var before = Directory.GetFiles(Directory.GetCurrentDirectory()).Length;

        var answer = DarlingMcpPasswordKeyTools.NewPasswordKey(path);

        Assert.Equal("refused", Status(answer));
        Assert.Equal(before, Directory.GetFiles(Directory.GetCurrentDirectory()).Length);
    }

    [Fact]
    public async Task ResetPasswordKey_WithoutConfirm_OrWithoutTheOwnerConnection_IsRefused()
    {
        var unconfirmed = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(
            confirm: false, owner: null, fromSetting: false, logger: null, TestContext.Current.CancellationToken);
        Assert.Equal("refused", Status(unconfirmed));
        Assert.Equal(DarlingMcpPasswordKeyTools.ResetNeedsConfirmText, Message(unconfirmed));

        var noStore = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(
            confirm: true, owner: null, fromSetting: false, logger: null, TestContext.Current.CancellationToken);
        Assert.Equal("refused", Status(noStore));
        Assert.Equal(DarlingMcpPasswordKeyTools.StoreNotReadyText, Message(noStore));
    }

    [Fact]
    public void SelfCheckPasswordKey_IsRefusedWhereTheUnixChecksCannotRun_AndFailsOnAMissingVectorWhereTheyCan()
    {
        var answer = DarlingMcpPasswordKeyTools.SelfCheckPasswordKey(Path.Combine(Path.GetTempPath(), "no-such-vector-9644.json"));

        if (DarlingPasswordKeySelfCheck.CanRunHere)
        {
            Assert.Equal("failed", Status(answer));
            Assert.Contains("the vector file was not found", answer, StringComparison.Ordinal);
        }
        else
        {
            Assert.Equal("refused", Status(answer));
            Assert.Equal(DarlingPasswordKeySelfCheck.UnsupportedPlatformLine, Message(answer));
        }
    }

    [Fact]
    public async Task ResetPasswordKey_MarksThePublishedKeyReplaced_CountsTheSealedPasswords_AndLogsNoSecret()
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
            var keyText = Convert.ToBase64String(key.ExportPkcs8());
            await using (var publish = await source.OpenConnectionAsync(ct))
            {
                await DarlingPasswordKeyStore.PublishAsync(publish, keyId, key.PublicKey.Spki, ct);
            }

            await using (var seed = await source.OpenConnectionAsync(ct))
            await using (var command = new NpgsqlCommand(@"
INSERT INTO config.config_notification (id, smtp_host, smtp_port, smtp_use_ssl, smtp_username, smtp_encrypted_password)
VALUES (1, 'smtp.example.com', 587, TRUE, 'mail_login', 'sealed:v1:0123456789abcdef:AAAA')
ON CONFLICT (id) DO UPDATE SET smtp_encrypted_password = EXCLUDED.smtp_encrypted_password;", seed))
            {
                await command.ExecuteNonQueryAsync(ct);
            }

            // Unconfirmed: nothing changes.
            var refused = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(false, source, fromSetting: false, logger, ct);
            Assert.Equal("refused", Status(refused));
            Assert.Equal(keyId, (await ReadStateAsync(source, ct)).CurrentKeyId);

            // Confirmed: the key is replaced and the answer says how many passwords are left to re-enter, in the verb's words.
            var answer = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(true, source, fromSetting: false, logger, ct);
            using var document = JsonDocument.Parse(answer);
            Assert.Equal("reset", document.RootElement.GetProperty("status").GetString());
            Assert.Equal(1, document.RootElement.GetProperty("saved_passwords_to_re_enter").GetInt32());
            Assert.Equal(DarlingCliCommands.ResetPasswordKeyRestartSentence(1), document.RootElement.GetProperty("message").GetString());
            var after = await ReadStateAsync(source, ct);
            Assert.Null(after.CurrentKeyId);
            Assert.Equal(("replaced", "reset"), (after.OldState, after.OldReason));

            // A second reset finds no current key.
            var again = await DarlingMcpPasswordKeyTools.ResetPasswordKeyAsync(true, source, fromSetting: false, logger, ct);
            Assert.Equal("nothing_to_reset", Status(again));

            // Neither the key, nor the sealed value, appears in the answers or the log.
            foreach (var text in new[] { refused, answer, again, logger.Joined })
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
