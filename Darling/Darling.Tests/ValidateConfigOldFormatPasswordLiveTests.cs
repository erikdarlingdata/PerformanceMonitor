/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another one.
   The role it connects as is created for the fact and dropped in its cleanup. */

/// <summary>
/// <c>--validate-config</c> against a store (#5366): a connection that owns the pin table reads the pins, so a pinned
/// old-format saved password opens and is tested; a connection that is not the owner cannot read them, so the verb says
/// it could not check old-format passwords and leaves those servers out, and never says one needs to be entered again.
/// </summary>
public sealed class ValidateConfigOldFormatPasswordLiveTests
{
    private const string Plain = "p@ss-not-real";
    private const int ServerId = 5366101;

    /// <summary>An old-format value is any text that is neither a reference nor sealed; it is not opened until a server's
    /// password is resolved, and the registry read does not do that.</summary>
    private const string OldFormatText = "AAAAAAAAAAAAAAAAAAAAAAAA";

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct, params (string Name, NpgsqlDbType Type, object Value)[] parameters)
    {
        await using var command = source.CreateCommand(sql);
        foreach (var (name, type, value) in parameters)
        {
            command.Parameters.Add(new NpgsqlParameter(name, type) { Value = value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    /* The server is on this machine at a closed port, so the probe the verb runs on it fails at once. */
    private static Task InsertServerAsync(NpgsqlDataSource owner, string stored, CancellationToken ct) => ExecAsync(
        owner,
        "INSERT INTO config_monitored_servers (server_id, name, host, port, auth, username, encrypted_password) " +
        $"VALUES ({ServerId}, 'alpha', '127.0.0.1', 1, 'sql', 'monitor', @stored)",
        ct,
        ("stored", NpgsqlDbType.Text, stored));

    private static string WriteConfig(DirectoryInfo root, string connectionString)
    {
        var path = Path.Combine(root.FullName, "darling.json");
        File.WriteAllText(path, "{ \"postgres\": { \"connectionString\": " + JsonSerializer.Serialize(connectionString)
            + " }, \"servers\": [ { \"name\": \"file-only\", \"host\": \"file-only.example.test\" } ] }");
        return path;
    }

    private static async Task<(int Exit, string Output, string Error)> RunVerbAsync(string configPath, CancellationToken ct)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await DarlingCliCommands.ValidateConfigAsync(configPath, output, error, ct);
        return (exit, output.ToString(), error.ToString());
    }

    [Fact]
    public async Task AsTheStoreOwner_APinnedOldFormatPasswordIsOpenedAndTested_AndNeverReportedAsNeedingToBeEnteredAgain()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Old-format values are Windows DPAPI values.");
        var ct = TestContext.Current.CancellationToken;
        var stored = DarlingSecrets.Protect(Plain);
        var (scratch, owner, ownerString) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratch;
        await using var ownerHolder = owner;
        var root = Directory.CreateTempSubdirectory("darling-validate-pins-");
        var ok = false;
        try
        {
            await InsertServerAsync(owner, stored, ct);
            var config = WriteConfig(root, ownerString);

            /* Without a pin the value cannot be opened here, and the verb says so: the control for the run below. */
            var unpinned = await RunVerbAsync(config, ct);
            Assert.Contains("Validating connectivity to 1 server(s)", unpinned.Output, StringComparison.Ordinal);
            Assert.Contains("is in the old format", unpinned.Output, StringComparison.Ordinal);

            /* The pin the upgrade writes: the value's hash and the hash of the connection as the row holds it. */
            await using (var connection = await owner.OpenConnectionAsync(ct))
            {
                var row = (await StoreConfigProvider.ReadMonitoredServersAsync(connection, new DarlingConfig(), ct))[0];
                await ExecAsync(
                    owner,
                    "INSERT INTO config.legacy_secret_pin (server_id, slot, value_sha256, binding_sha256) VALUES (@id, 'server', @value, @binding)",
                    ct,
                    ("id", NpgsqlDbType.Integer, ServerId),
                    ("value", NpgsqlDbType.Bytea, SHA256.HashData(Encoding.UTF8.GetBytes(stored))),
                    ("binding", NpgsqlDbType.Bytea, row.SecretBinding.LegacyPinHash()));
            }

            var pinned = await RunVerbAsync(config, ct);
            Assert.Contains("Validating connectivity to 1 server(s)", pinned.Output, StringComparison.Ordinal);
            Assert.DoesNotContain("could not be checked", pinned.Output, StringComparison.Ordinal);
            Assert.DoesNotContain("old format", pinned.Output, StringComparison.Ordinal);
            Assert.DoesNotContain("entered again", pinned.Output + pinned.Error, StringComparison.Ordinal);
            Assert.Contains("[FAIL] alpha", pinned.Output, StringComparison.Ordinal);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(ok, () =>
            {
                root.Delete(true);
                return Task.CompletedTask;
            });
        }
    }

    [Fact]
    public async Task AsARoleThatIsNotTheStoreOwner_TheVerbSaysItCouldNotCheckOldFormatPasswords_AndNeverSaysOneNeedsToBeEnteredAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, owner, ownerString) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratch;
        await using var ownerHolder = owner;
        var role = "val_cfg_" + Guid.NewGuid().ToString("N")[..8];
        var root = Directory.CreateTempSubdirectory("darling-validate-pins-");
        var ok = false;
        try
        {
            /* A login that can read the registry (the service's other roles can) but owns nothing in the store. */
            await ExecAsync(owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{ServerAddViewerRoleLiveTests.RolePassword}'", ct);
            await ExecAsync(owner, $"GRANT USAGE ON SCHEMA config TO {role}", ct);
            await ExecAsync(owner, $"GRANT SELECT ON config.config_monitored_servers TO {role}", ct);
            await InsertServerAsync(owner, OldFormatText, ct);
            var viewerString = new NpgsqlConnectionStringBuilder(ownerString)
            {
                Username = role,
                Password = ServerAddViewerRoleLiveTests.RolePassword,
            }.ConnectionString;

            var result = await RunVerbAsync(WriteConfig(root, viewerString), ct);

            Assert.Contains(DarlingCliCommands.OldFormatPasswordsNotCheckedText(1), result.Output, StringComparison.Ordinal);
            Assert.Contains("Validating connectivity to 0 server(s)", result.Output, StringComparison.Ordinal);
            Assert.DoesNotContain("entered again", result.Output + result.Error, StringComparison.Ordinal);
            Assert.DoesNotContain("[FAIL] alpha", result.Output, StringComparison.Ordinal);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(ok, async () =>
            {
                root.Delete(true);
                await ExecAsync(owner, $"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role};", CancellationToken.None);
            });
        }
    }
}
