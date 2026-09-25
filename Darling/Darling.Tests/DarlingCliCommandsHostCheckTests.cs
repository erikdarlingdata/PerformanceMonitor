/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4214's CLI-level coverage: the exit codes <c>--check-settings</c> and <c>--validate-config</c> actually
/// return for each outcome the issue names, not just the pure pieces underneath them.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingCliCommandsHostCheckTests
{
    /// <summary>A TCP port nothing is listening on, freshly proven closed by bind-then-release — a connection
    /// attempt to it fails fast (refused) instead of timing out against an address that merely never answers.</summary>
    private static int GetClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    [Fact]
    public async Task CheckSettingsAsync_ConfigParseError_ReturnsConfigErrorExitCode()
    {
        var root = Directory.CreateTempSubdirectory("darling-checksettings-cfgerr-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, "{ not valid json");

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.CheckSettingsAsync(configPath, false, output, error, CancellationToken.None);

            Assert.Equal(DarlingCliCommands.CheckSettingsExitCode.ConfigError, exit);
            Assert.Contains("Could not load configuration", error.ToString(), StringComparison.Ordinal);
            Assert.Equal("", output.ToString());
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
        }
    }

    [Fact]
    public async Task CheckSettingsAsync_UnreachableStore_ReturnsStoreUnreachableExitCode()
    {
        var root = Directory.CreateTempSubdirectory("darling-checksettings-unreach-");
        try
        {
            var closedPort = GetClosedPort();
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, $$"""
                {
                  "postgres": { "connectionString": "Host=127.0.0.1;Port={{closedPort}};Username=darling;Database=darlingtest;Timeout=2" },
                  "servers": [ { "name": "SQL2022", "host": "SQL2022" } ]
                }
                """);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.CheckSettingsAsync(configPath, false, output, error, CancellationToken.None);

            Assert.Equal(DarlingCliCommands.CheckSettingsExitCode.StoreUnreachable, exit);
            Assert.Contains("Could not connect to the store", error.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
        }
    }

    [Fact]
    public async Task CheckSettingsAsync_ByoStore_NothingEverStale_ReturnsOk_TextAndJson_Gated()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to run the live --check-settings exit-code tests.");

        var root = Directory.CreateTempSubdirectory("darling-checksettings-byo-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, $$"""
                {
                  "postgres": { "connectionString": {{JsonSerializer.Serialize(connectionString)}} },
                  "servers": [ { "name": "SQL2022", "host": "SQL2022" } ]
                }
                """);

            /* Bring-your-own: every setting reads not-managed, which is never stale-after-hardware-change —
               the simplest real path to the "nothing needs attention" exit code, with no conf file involved. */
            var textOutput = new StringWriter();
            var textError = new StringWriter();
            var textExit = await DarlingCliCommands.CheckSettingsAsync(configPath, false, textOutput, textError, CancellationToken.None);
            Assert.Equal(DarlingCliCommands.CheckSettingsExitCode.Ok, textExit);
            Assert.Contains("not-managed", textOutput.ToString(), StringComparison.Ordinal);

            var jsonOutput = new StringWriter();
            var jsonError = new StringWriter();
            var jsonExit = await DarlingCliCommands.CheckSettingsAsync(configPath, true, jsonOutput, jsonError, CancellationToken.None);
            Assert.Equal(DarlingCliCommands.CheckSettingsExitCode.Ok, jsonExit);
            using var parsed = JsonDocument.Parse(jsonOutput.ToString());
            Assert.True(parsed.RootElement.TryGetProperty("settings", out var settings));
            Assert.True(settings.GetArrayLength() > 0);
            foreach (var setting in settings.EnumerateArray())
            {
                Assert.Equal("not-managed", setting.GetProperty("verdict").GetString());
            }
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
        }
    }

    [Fact]
    public async Task CheckSettingsAsync_ManagedStoreWithAStaleBlock_ReturnsStaleSettingsExitCode_Gated()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to run the live --check-settings exit-code tests.");

        var ct = TestContext.Current.CancellationToken;
        var builder = new NpgsqlConnectionStringBuilder(connectionString);

        /* The "managed" connect path (DarlingManagedPostgres.TryBuildConnectionStringFromStoredCredential)
           always targets DarlingManagedPostgres.DatabaseName ("darling") — hardcoded, not read from config —
           so this test provisions that database itself rather than assuming a hand-built rig already has it.
           A rig built per the lane's own setup only carries the suite's own database and a scratch database
           (never "darling"), and asking --check-settings to connect against a database that does not exist
           fails at StoreUnreachable before it ever reaches a settings verdict — the defect this rewrite fixes
           (it used to point straight at DARLING_TEST_PGRUNTIME's own data directory and assume "darling"
           already existed there, which is true of a bootstrapped product install and false of a bare rig). */
        var adminBuilder = new NpgsqlConnectionStringBuilder(connectionString) { Database = "postgres" };
        var createdDarlingDatabase = false;
        await using (var admin = new NpgsqlConnection(adminBuilder.ConnectionString))
        {
            await admin.OpenAsync(ct);
            await using var probe = new NpgsqlCommand(
                "SELECT 1 FROM pg_database WHERE datname = 'darling'", admin);
            if (await probe.ExecuteScalarAsync(ct) is null)
            {
                await using var create = new NpgsqlCommand("CREATE DATABASE darling", admin);
                await create.ExecuteNonQueryAsync(ct);
                createdDarlingDatabase = true;
            }
        }

        /* A THROWAWAY data directory, never the rig's real one (ruling 7: a live test here must not depend on
           — or mutate — the rig's own postgresql.conf). AttributeManagedSetting reads the conf file straight
           off disk; it has no dependency on the directory actually being what PostgreSQL was started from, so
           a directory holding nothing but a hand-built postgresql.conf is exactly as real an input to it as
           the rig's own. */
        var root = Directory.CreateTempSubdirectory("darling-checksettings-stale-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "data");
            Directory.CreateDirectory(dataDirectory);

            /* max_connections = 100 inside a managed (v4) block: PostgreSQL's own untouched default already
               disagrees with today's derivation (DarlingManagedPostgres.TargetMaxConnections = 200), and
               ClassifyVerdict compares the LIVE value against TODAY's derivation — no reload needed. */
            await File.WriteAllTextAsync(
                Path.Combine(dataDirectory, "postgresql.conf"),
                "\n" + DarlingManagedPostgres.ConfMarkerV4 + "\nmax_connections = 100\n", ct);

            /* The owner credential the "managed" connect path reads — the rig trusts any password (initdb
               -A trust), so the protected value's content does not have to be the rig's real password. */
            var credentialPath = DarlingManagedPostgres.CredentialPathFor(dataDirectory);
            await File.WriteAllTextAsync(credentialPath, DarlingSecrets.Protect("trust-auth-ignores-this"), ct);

            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, $$"""
                {
                  "postgres": { "managed": true, "port": {{builder.Port}}, "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}} },
                  "servers": [ { "name": "SQL2022", "host": "SQL2022" } ]
                }
                """, ct);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.CheckSettingsAsync(configPath, false, output, error, ct);

            Assert.Equal(DarlingCliCommands.CheckSettingsExitCode.StaleSettings, exit);
            Assert.Contains("stale-after-hardware-change", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);

            if (createdDarlingDatabase)
            {
                await using var admin = new NpgsqlConnection(adminBuilder.ConnectionString);
                await admin.OpenAsync(ct);
                await using var terminate = new NpgsqlCommand(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'darling' AND pid <> pg_backend_pid()", admin);
                await terminate.ExecuteNonQueryAsync(ct);
                await using var drop = new NpgsqlCommand("DROP DATABASE IF EXISTS darling", admin);
                await drop.ExecuteNonQueryAsync(ct);
            }
        }
    }

    [Fact]
    public async Task ValidateConfigAsync_SeedOnlyServer_IsAWarning_AndExitsZero_Gated()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to run the live --validate-config registry tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        var root = Directory.CreateTempSubdirectory("darling-validate-seedonly-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, $$"""
                {
                  "postgres": { "connectionString": {{JsonSerializer.Serialize(scratch.ConnectionString)}} },
                  "servers": [ { "name": "seed-only-server", "host": "seed-only-host" } ]
                }
                """);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.ValidateConfigAsync(configPath, output, error, ct);

            /* The registry has zero rows (a fresh, migrated-but-unseeded scratch store), so the file's one
               server is entirely file-only: a WARNING, and zero servers actually probed — "all reachable"
               vacuously, which is the exit-0 contract this asserts. */
            Assert.Equal(0, exit);
            Assert.Contains("WARNING", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("seed-only-server", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("Validating connectivity to 0 server(s)", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
        }
    }

    [Fact]
    public async Task ValidateConfigAsync_RegistryServerThatFails_StillFails_Gated()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to run the live --validate-config registry tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var closedPort = GetClosedPort();
            await using var insert = new NpgsqlCommand(
                "INSERT INTO config_monitored_servers (server_id, name, host, port, engine, is_enabled) "
                + "VALUES ($1, $2, $3, $4, 'postgres', TRUE)", connection);
            insert.Parameters.AddWithValue(1);
            insert.Parameters.AddWithValue("unreachable-registry-server");
            insert.Parameters.AddWithValue("127.0.0.1");
            insert.Parameters.AddWithValue(closedPort);
            await insert.ExecuteNonQueryAsync(ct);
        }

        var root = Directory.CreateTempSubdirectory("darling-validate-regfail-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, $$"""
                {
                  "postgres": { "connectionString": {{JsonSerializer.Serialize(scratch.ConnectionString)}} },
                  "servers": [ { "name": "SQL2022", "host": "SQL2022" } ]
                }
                """);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.ValidateConfigAsync(configPath, output, error, ct);

            Assert.Equal(1, exit);
            Assert.Contains("unreachable-registry-server", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("One or more servers failed", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
        }
    }

    [Fact]
    public async Task ValidateConfigAsync_UnreachableStore_FallsBackToFileList_AndSaysSo()
    {
        var root = Directory.CreateTempSubdirectory("darling-validate-storedown-");
        try
        {
            var closedPort = GetClosedPort();
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, $$"""
                {
                  "postgres": { "connectionString": "Host=127.0.0.1;Port={{closedPort}};Username=darling;Database=darlingtest;Timeout=2" },
                  "servers": [ { "name": "file-only-server", "host": "file-only-host" } ]
                }
                """);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.ValidateConfigAsync(configPath, output, error, CancellationToken.None);

            Assert.Contains("NOTE: the store's registry is not reachable", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("validating darling.json's own server list instead", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("file-only-server", output.ToString(), StringComparison.Ordinal);

            /* The fallback list's own (fake) server cannot be reached either — falling back is not the same
               as passing. */
            Assert.Equal(1, exit);
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
        }
    }
}
