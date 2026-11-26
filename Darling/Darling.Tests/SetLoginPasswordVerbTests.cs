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
/// <c>--set-login-password</c> (#9689): one password, read from stdin, saved for every enabled server that uses a login.
/// The pure facts cover the arguments, the password line and the rule for each server. The live fact runs the store half
/// on a store whose saved passwords were sealed with a key this host does not hold, as a store moved from another machine is.
/// </summary>
public sealed class SetLoginPasswordVerbTests
{
    private const string Login = "monitor";
    private const string NewPassword = "n3w-Pass word ";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static MonitoredServer Server(string name, string? stored, string username = Login, string auth = "sql", string? filePassword = null) =>
        new() { Name = name, Host = name + ".example", Auth = auth, Username = username, EncryptedPassword = stored, Password = filePassword };

    private static string SealFor(MonitoredServer server, IPasswordKeyRing ring, string password) =>
        ring.Seal(password, DarlingMcpServerAdminTools.BindingOf(server));

    [Fact]
    public void TheVerbIsKnown_SoItNeverStartsTheService()
    {
        Assert.True(DarlingCliCommands.IsSetLoginPasswordVerb("--set-login-password"));
        Assert.True(DarlingCliCommands.IsSetLoginPasswordVerb("--SET-LOGIN-PASSWORD"));
        Assert.False(DarlingCliCommands.IsSetLoginPasswordVerb("--set-login"));
        Assert.True(DarlingCliCommands.IsKnownVerb("--set-login-password"));
    }

    [Fact]
    public void Arguments_TakeTheLoginTheDryRunAndTheConfigPath_InAnyOrder()
    {
        Assert.True(DarlingCliCommands.TryParseSetLoginPasswordArgs(
            ["--dry-run", "--config", "/etc/darling/darling.json", "--username", Login], out var parsed, out var error));
        Assert.Null(error);
        Assert.Equal(Login, parsed!.Username);
        Assert.True(parsed.DryRun);
        Assert.Equal("/etc/darling/darling.json", parsed.ConfigPath);

        Assert.True(DarlingCliCommands.TryParseSetLoginPasswordArgs(["--username", Login], out parsed, out _));
        Assert.False(parsed!.DryRun);
        Assert.Null(parsed.ConfigPath);
    }

    [Theory]
    [InlineData()]
    [InlineData("--dry-run")]
    [InlineData("--username")]
    [InlineData("--username", "--dry-run")]
    [InlineData("--username", "a", "--username", "b")]
    [InlineData("--username", "a", "--config")]
    [InlineData("--username", "a", "--password", "x")]
    [InlineData("--username", "a", "--name", "x")]
    public void Arguments_ThatAreIncompleteRepeatedOrUnknown_AreRefused(params string[] rest)
    {
        Assert.False(DarlingCliCommands.TryParseSetLoginPasswordArgs(rest, out var parsed, out var error));
        Assert.Null(parsed);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [Fact]
    public void ABareValue_IsRefused_AndIsNotRepeated_BecauseItMayBeAPassword()
    {
        const string Typed = "hunter2-on-the-command-line";
        Assert.False(DarlingCliCommands.TryParseSetLoginPasswordArgs(["--username", Login, Typed], out _, out var error));
        Assert.DoesNotContain(Typed, error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABareValue_StopsTheVerbBeforeItReadsStdinOrOpensAStore_AndIsInNoOutput()
    {
        const string Typed = "hunter2-on-the-command-line";
        var output = new StringWriter();
        var error = new StringWriter();
        var asked = false;
        var exit = await DarlingCliCommands.SetLoginPasswordAsync(
            ["--username", Login, Typed], TextReader.Null, output, error, TestContext.Current.CancellationToken,
            readPassword: _ => { asked = true; return "x"; });
        Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig, exit);
        Assert.False(asked);
        Assert.DoesNotContain(Typed, output.ToString() + error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ThePasswordLine_IsRefusedWhenMissing_AReference_TooLong_OrNotText_AndTheRefusalNeverRepeatsIt()
    {
        Assert.NotNull(DarlingCliCommands.LoginPasswordRefusal(null));
        Assert.NotNull(DarlingCliCommands.LoginPasswordRefusal(""));
        Assert.Null(DarlingCliCommands.LoginPasswordRefusal(NewPassword));
        Assert.Null(DarlingCliCommands.LoginPasswordRefusal(new string('p', PasswordSeal.MaxPlaintextBytes)));

        foreach (var refused in new[] { "env:DARLING_SQL_PASSWORD", "file:/run/secrets/sql", new string('p', PasswordSeal.MaxPlaintextBytes + 1), "bad\uD800text" })
        {
            var refusal = DarlingCliCommands.LoginPasswordRefusal(refused);
            Assert.NotNull(refusal);
            Assert.DoesNotContain(refused, refusal, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ThePlan_SetsEverySavedValueThatDoesNotOpenOrDiffers_LeavesTheSameOneAlone_AndSkipsWhatIsNotASavedValue()
    {
        using var key = PasswordPrivateKey.Generate();
        using var foreign = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var foreignRing = DarlingPasswordKey.FromPrivateKey(foreign);

        var none = Server("none", null);
        var foreignSeal = Server("foreign", null);
        foreignSeal.EncryptedPassword = SealFor(foreignSeal, foreignRing, "old");
        var same = Server("same", null);
        same.EncryptedPassword = SealFor(same, ring, NewPassword);
        var differs = Server("differs", null);
        differs.EncryptedPassword = SealFor(differs, ring, "old");
        var moved = Server("moved", null);
        moved.EncryptedPassword = SealFor(Server("somewhere-else", null), ring, NewPassword);
        var oldFormat = Server("old-format", "AQAAANCMnd8BFdERjHoAwE/Cl+sBAAAA");
        var env = Server("env", "env:DARLING_SQL_PASSWORD");
        var file = Server("file", "file:/run/secrets/sql");
        var fromJson = Server("from-json", null, filePassword: "file:/run/secrets/sql");
        var integrated = Server("integrated", null, auth: "integrated");
        var otherLogin = Server("other-login", null, username: "someone-else");
        var otherCase = Server("other-case", null, username: Login.ToUpperInvariant());

        var plan = DarlingCliCommands.PlanLoginPassword(
            [none, foreignSeal, same, differs, moved, oldFormat, env, file, fromJson, integrated, otherLogin, otherCase], Login, NewPassword, ring);

        var actions = plan.ToDictionary(l => l.Server.Name, l => l.Action, StringComparer.Ordinal);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, actions["none"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, actions["foreign"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Unchanged, actions["same"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, actions["differs"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, actions["moved"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, actions["old-format"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, actions["env"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, actions["file"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, actions["from-json"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, actions["integrated"]);
        Assert.False(actions.ContainsKey("other-login"));
        Assert.False(actions.ContainsKey("other-case"));

        /* No line of the list carries the password or a stored value. */
        var printed = string.Join("\n", plan.Select(l => DarlingCliCommands.FormatLoginPasswordLine(l, dryRun: true)));
        Assert.DoesNotContain(NewPassword, printed, StringComparison.Ordinal);
        Assert.DoesNotContain("sealed:", printed, StringComparison.Ordinal);
        Assert.DoesNotContain("/run/secrets/sql", printed, StringComparison.Ordinal);
        Assert.DoesNotContain("DARLING_SQL_PASSWORD", printed, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePlan_WithAKeyThatOpensNothing_SetsEverySavedValue_AndNeverCallsOneUnchanged()
    {
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var same = Server("same", null);
        same.EncryptedPassword = SealFor(same, ring, NewPassword);
        var reference = Server("file", "file:/run/secrets/sql");

        foreach (var cannotOpen in new IPasswordKeyRing[] { new DarlingCliSealKey.PublishedKeyRing(key.PublicKey), DarlingPasswordKey.Refusing("The key is not ready.") })
        {
            var plan = DarlingCliCommands.PlanLoginPassword([same, reference], Login, NewPassword, cannotOpen);
            Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, plan.Single(l => l.Server.Name == "same").Action);
            Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, plan.Single(l => l.Server.Name == "file").Action);
        }
    }

    [Fact]
    public async Task OnAStoreWhoseSavedPasswordsWereSealedWithAnotherKey_TheDryRunListsAndChangesNothing_AndTheRunSealsThemWithThisKey()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run --set-login-password against a scratch store.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            /* The store publishes this host's key, as a start after --reset-password-key leaves it. The saved passwords were
               sealed with the key of the machine the store came from, which this host does not hold. */
            using var key = PasswordPrivateKey.Generate();
            using var foreign = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);
            var foreignRing = DarlingPasswordKey.FromPrivateKey(foreign);
            await using (var publish = await source.OpenConnectionAsync(ct))
            {
                await DarlingPasswordKeyStore.PublishAsync(publish, key.PublicKey.KeyId, key.PublicKey.Spki, ct);
            }

            var foreignText = SealFor(Server("any", null), foreignRing, "the old machine's password");
            var rows = new (int Id, string Name, string Username, string? Stored, bool Enabled)[]
            {
                (9689001, "moved-a", Login, foreignText, true),
                (9689002, "moved-b", Login, foreignText, true),
                (9689003, "never-saved", Login, null, true),
                (9689004, "by-reference", Login, "file:/run/secrets/sql", true),
                (9689005, "other-login", "someone-else", foreignText, true),
                (9689006, "switched-off", Login, foreignText, false),
            };
            await using (var seed = await source.OpenConnectionAsync(ct))
            {
                foreach (var row in rows)
                {
                    await using var insert = new NpgsqlCommand(@"
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, username, encrypted_password, is_enabled)
VALUES (@id, @name, @host, 'sql', @username, @stored, @enabled);", seed);
                    insert.Parameters.AddWithValue("id", row.Id);
                    insert.Parameters.AddWithValue("name", row.Name);
                    insert.Parameters.AddWithValue("host", row.Name + ".example");
                    insert.Parameters.AddWithValue("username", row.Username);
                    insert.Parameters.AddWithValue("stored", (object?)row.Stored ?? DBNull.Value);
                    insert.Parameters.AddWithValue("enabled", row.Enabled);
                    await insert.ExecuteNonQueryAsync(ct);
                }
            }

            var config = new DarlingConfig();
            var before = await ReadStoredAsync(source, ct);

            /* The dry run lists the three it would set and the one it skips, and writes nothing. */
            var dryOutput = new StringWriter();
            var dryError = new StringWriter();
            var dryExit = await DarlingCliCommands.SetLoginPasswordInStoreAsync(
                source, config, ring, null, new DarlingCliCommands.SetLoginPasswordArgs(Login, DryRun: true, null), NewPassword, dryOutput, dryError, ct);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, dryExit);
            Assert.Equal(before, await ReadStoredAsync(source, ct));
            foreach (var listed in new[] { "moved-a", "moved-b", "never-saved", "by-reference" })
            {
                Assert.Contains(listed, dryOutput.ToString(), StringComparison.Ordinal);
            }

            Assert.DoesNotContain("other-login", dryOutput.ToString(), StringComparison.Ordinal);

            /* A key that is not ready: the dry run still lists, the run refuses, and neither writes. */
            var notReady = DarlingPasswordKey.Refusing("The key is not ready.");
            var args = new DarlingCliCommands.SetLoginPasswordArgs(Login, DryRun: false, null);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, await DarlingCliCommands.SetLoginPasswordInStoreAsync(
                source, config, notReady, null, args with { DryRun = true }, NewPassword, new StringWriter(), new StringWriter(), ct));
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig, await DarlingCliCommands.SetLoginPasswordInStoreAsync(
                source, config, notReady, null, args, NewPassword, new StringWriter(), new StringWriter(), ct));
            Assert.Equal(before, await ReadStoredAsync(source, ct));

            /* A login no enabled server uses changes nothing and is a usage failure. */
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig, await DarlingCliCommands.SetLoginPasswordInStoreAsync(
                source, config, ring, null, args with { Username = "nobody" }, NewPassword, new StringWriter(), new StringWriter(), ct));

            /* The run seals the three with this host's key. */
            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.SetLoginPasswordInStoreAsync(source, config, ring, null, args, NewPassword, output, error, ct);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, exit);
            var after = await ReadStoredAsync(source, ct);
            foreach (var untouched in new[] { 9689004, 9689005, 9689006 })
            {
                Assert.Equal(before[untouched], after[untouched]);
            }

            await using (var read = await source.OpenConnectionAsync(ct))
            {
                var servers = await StoreConfigProvider.ReadMonitoredServersAsync(read, config, ct);
                foreach (var name in new[] { "moved-a", "moved-b", "never-saved" })
                {
                    var server = servers.Single(s => s.Name == name);
                    Assert.Equal(NewPassword, DarlingSecrets.ResolvePassword(server, out _, ring));
                }
            }

            /* The password is in no line the verb wrote. */
            Assert.DoesNotContain(NewPassword, dryOutput.ToString() + dryError.ToString() + output.ToString() + error.ToString(), StringComparison.Ordinal);

            /* A second run finds them all saved and writes no row. */
            var again = await DarlingCliCommands.SetLoginPasswordInStoreAsync(source, config, ring, null, args, NewPassword, new StringWriter(), new StringWriter(), ct);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, again);
            Assert.Equal(after, await ReadStoredAsync(source, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Each server's stored value and change stamp, as text, so two reads compare row for row.</summary>
    private static async Task<Dictionary<int, string>> ReadStoredAsync(NpgsqlDataSource source, System.Threading.CancellationToken ct)
    {
        var stored = new Dictionary<int, string>();
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT server_id, coalesce(encrypted_password, '<null>') || ' @ ' || coalesce(modified_at::text, '<null>') FROM config.config_monitored_servers;", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            stored[reader.GetInt32(0)] = reader.GetString(1);
        }

        return stored;
    }
}
