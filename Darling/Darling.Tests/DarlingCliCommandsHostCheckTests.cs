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
            root.Delete(recursive: true);
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
            root.Delete(recursive: true);
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
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task CheckSettingsAsync_ManagedStoreWithAStaleBlock_ReturnsStaleSettingsExitCode_Gated()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to run the live --check-settings exit-code tests.");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to the rig's runtime root — this drives --check-settings through a "
            + "REAL managed data directory, which DARLING_TEST_PG alone does not locate on disk.");

        var dataDirectory = Path.Combine(runtimeRoot!, "data");
        var confPath = Path.Combine(dataDirectory, "postgresql.conf");
        Assert.SkipUnless(File.Exists(confPath), $"DARLING_TEST_PGRUNTIME={runtimeRoot} has no data\\postgresql.conf.");

        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var root = Directory.CreateTempSubdirectory("darling-checksettings-stale-");
        var originalConf = File.ReadAllText(confPath);
        try
        {
            /* max_connections = 100 inside a managed (v4) block: PostgreSQL's own untouched default already
               disagrees with today's derivation (DarlingManagedPostgres.TargetMaxConnections = 200), and
               ClassifyVerdict compares the LIVE value against TODAY's derivation — no reload needed. */
            File.AppendAllText(confPath, "\n" + DarlingManagedPostgres.ConfMarkerV4 + "\nmax_connections = 100\n");

            /* The owner credential the "managed" connect path reads (DarlingManagedPostgres.
               TryBuildConnectionStringFromStoredCredential) — the rig trusts any password (initdb -A trust),
               so the protected value's content does not have to be the rig's real (nonexistent) password. */
            var credentialPath = DarlingManagedPostgres.CredentialPathFor(dataDirectory);
            await File.WriteAllTextAsync(credentialPath, DarlingSecrets.Protect("trust-auth-ignores-this"));

            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, $$"""
                {
                  "postgres": { "managed": true, "port": {{builder.Port}}, "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}} },
                  "servers": [ { "name": "SQL2022", "host": "SQL2022" } ]
                }
                """);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.CheckSettingsAsync(configPath, false, output, error, CancellationToken.None);

            Assert.Equal(DarlingCliCommands.CheckSettingsExitCode.StaleSettings, exit);
            Assert.Contains("stale-after-hardware-change", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            File.WriteAllText(confPath, originalConf);
            root.Delete(recursive: true);
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
            root.Delete(recursive: true);
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
            root.Delete(recursive: true);
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
            root.Delete(recursive: true);
        }
    }
}
