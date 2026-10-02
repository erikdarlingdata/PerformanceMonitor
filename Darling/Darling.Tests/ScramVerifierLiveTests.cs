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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3910 on the product's own conf: role provisioning sends SCRAM-SHA-256 verifiers, never a password. Boots a
/// managed cluster from the bundled runtime, migrates it, and runs the REAL <c>EnsureProvisionedAsync</c>, the
/// seam where the credential files, the stored-verifier read, the re-assert decision and the batch meet. It
/// proves:
/// <list type="bullet">
/// <item><description>a verifier computed here is one PostgreSQL accepts the password against, and one it
/// computes itself verifies here;</description></item>
/// <item><description>every managed role logs in with the password from its credential file;</description></item>
/// <item><description>a second start re-asserts no password: the stored verifier is byte-identical;</description></item>
/// <item><description>on the failure path the issue named (an unmarked same-named role makes the DO block raise,
/// and <c>log_min_error_statement</c> writes the whole batch to the store's own log), the log carries no
/// password.</description></item>
/// </list>
///
/// <para><b>#1776 own-store</b>: it boots its own cluster, so it is not in the <c>live-postgres</c>
/// collection.</para>
/// </summary>
public sealed class ScramVerifierLiveTests
{
    [Fact]
    public async Task OnTheProductsOwnConf_ProvisioningSendsVerifiers_AndNoPasswordReachesTheLog_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe) to run the #3910 proof on the product's own conf.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime and the DPAPI credential files are Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-scram-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig
        {
            Managed = true,
            Port = DarlingManagedPostgresTests.FindFreeTcpPort(),
            DataDirectory = dataDirectory,
        };
        var owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(8));
            var ct = timeout.Token;
            var ownerCs = new NpgsqlConnectionStringBuilder(await owner.EnsureRunningAsync(ct)) { Pooling = false };

            /* The derivation, both directions, against the server itself. */
            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                await ExecAsync(c, "CREATE ROLE scram_server_made LOGIN PASSWORD 'ServerMade3910'", ct);
                var serverMade = await StoredSecretAsync(c, "scram_server_made", ct);
                Assert.True(ScramSha256Verifier.Verifies(serverMade, "ServerMade3910"), serverMade);

                var ours = ScramSha256Verifier.Create("ClientMade3910");
                await ExecAsync(c, $"CREATE ROLE scram_client_made LOGIN PASSWORD '{ours}'", ct);
                Assert.Equal(ours, await StoredSecretAsync(c, "scram_client_made", ct));
            }

            await using (var c = await OpenAsync(Login(ownerCs, "scram_client_made", "ClientMade3910"), ct))
            {
                Assert.Equal("scram_client_made", await ScalarAsync<string>(c, "SELECT current_user::text", ct));
            }

            await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var wrong = await OpenAsync(Login(ownerCs, "scram_client_made", "NotThePassword"), ct);
            });

            /* Provisioning needs the migrated schema (its grants name tables by qualified name). */
            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                await PgMigrations.MigrateAsync(c, ct);
            }

            await using var dataSource = NpgsqlDataSource.Create(ownerCs.ConnectionString);

            /* The failure path the issue named: an unmarked `mcp` makes the DO block raise, and the default
               log_min_error_statement writes the batch that failed to the store's own log. */
            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                await ExecAsync(c, "CREATE ROLE mcp LOGIN", ct);
            }

            await Assert.ThrowsAnyAsync<Exception>(() =>
                DarlingManagedRoles.EnsureProvisionedAsync(dataSource, dataDirectory, NullLogger.Instance, ct));

            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                await ExecAsync(c, "DROP ROLE mcp", ct);
            }

            /* First real start: every role is created, each with a verifier. */
            var first = new CapturingTestLogger();
            await DarlingManagedRoles.EnsureProvisionedAsync(dataSource, dataDirectory, first, ct);
            Assert.Contains("Role passwords: re-asserted for admin, viewer, mcp", first.Joined, StringComparison.Ordinal);

            var passwords = new Dictionary<string, string>
            {
                [DarlingManagedPostgres.AdminRoleName] = ReadCredential(DarlingManagedPostgres.AdminCredentialPathFor(dataDirectory)),
                [DarlingManagedPostgres.ViewerRoleName] = ReadCredential(DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory)),
                [DarlingManagedPostgres.McpRoleName] = ReadCredential(DarlingManagedPostgres.McpCredentialPathFor(dataDirectory)),
            };

            var storedAfterFirst = new Dictionary<string, string?>();
            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                foreach (var (role, password) in passwords)
                {
                    var stored = await StoredSecretAsync(c, role, ct);
                    Assert.True(ScramSha256Verifier.Verifies(stored, password), $"{role}'s stored secret does not verify its credential file");
                    storedAfterFirst[role] = stored;
                }
            }

            foreach (var (role, password) in passwords)
            {
                await using var login = await OpenAsync(Login(ownerCs, role, password), ct);
                Assert.Equal(role, await ScalarAsync<string>(login, "SELECT current_user::text", ct));
            }

            /* Second start: nothing to re-assert, and the stored verifiers are untouched. */
            var second = new CapturingTestLogger();
            await DarlingManagedRoles.EnsureProvisionedAsync(dataSource, dataDirectory, second, ct);
            Assert.Contains("Role passwords: unchanged", second.Joined, StringComparison.Ordinal);
            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                foreach (var role in passwords.Keys)
                {
                    Assert.Equal(storedAfterFirst[role], await StoredSecretAsync(c, role, ct));
                }

                /* Flush what the collector has, so the failure above is on disk before the log is read. */
                await ScalarAsync<bool>(c, "SELECT pg_catalog.pg_rotate_logfile()", ct);
            }

            var log = await ReadStoreLogAsync(dataDirectory, "was not created by Darling", ct);
            Assert.Contains("SCRAM-SHA-256$", log, StringComparison.Ordinal);
            foreach (var (role, password) in passwords)
            {
                Assert.DoesNotContain(password, log, StringComparison.Ordinal);
            }
        }
        finally
        {
            await owner.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    private static string ReadCredential(string path) =>
        DarlingSecrets.Unprotect(File.ReadAllText(path).Trim());

    private static string Login(NpgsqlConnectionStringBuilder owner, string role, string password) =>
        new NpgsqlConnectionStringBuilder(owner.ConnectionString) { Username = role, Password = password, Pooling = false }.ConnectionString;

    /// <summary>The store's own log text once it carries <paramref name="expected"/>: the collector writes
    /// asynchronously, so the read retries briefly rather than racing it.</summary>
    private static async Task<string> ReadStoreLogAsync(string dataDirectory, string expected, CancellationToken ct)
    {
        var logDirectory = Path.Combine(dataDirectory, "log");
        var text = "";
        for (var attempt = 0; attempt < 40; attempt++)
        {
            text = string.Join("\n", Directory.Exists(logDirectory)
                ? Directory.GetFiles(logDirectory).Select(f => ReadShared(f))
                : []);
            if (text.Contains(expected, StringComparison.Ordinal))
            {
                return text;
            }

            await Task.Delay(250, ct);
        }

        Assert.Fail($"the store's log never recorded '{expected}' in {logDirectory}");
        return text;
    }

    private static string ReadShared(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    private static async Task<string?> StoredSecretAsync(NpgsqlConnection connection, string role, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT rolpassword FROM pg_catalog.pg_authid WHERE rolname = $1", connection);
        command.Parameters.AddWithValue(role);
        return await command.ExecuteScalarAsync(ct) as string;
    }

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
