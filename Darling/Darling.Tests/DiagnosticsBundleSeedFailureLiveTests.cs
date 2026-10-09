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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */

/// <summary>#5097: a name source that cannot be read while the store is up. Names are synthetic.</summary>
public sealed class DiagnosticsBundleSeedFailureLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string WriteConfig(string dir, string connectionString)
    {
        var path = Path.Combine(dir, "darling.json");
        File.WriteAllText(path, JsonSerializer.Serialize(new
        {
            postgres = new { connectionString },
            servers = new object[] { new { name = "alpha-sql-01", host = "alpha-sql-01.example.test", database = "gamma_orders", username = "betaowner" } },
        }));
        return path;
    }

    [Fact]
    public async Task ServerNameSourceMissing_WhileTheStoreIsUp_ExitsFiveWithNoFile_AndNamesTheSource()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the live seed-failure test.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var root = Directory.CreateTempSubdirectory("darling-bundle-seedfail-");
        var ok = false;
        try
        {
            /* The store is up and reachable, but it was never migrated: the registry tables do not exist. */
            var bundle = Path.Combine(root.FullName, "b.json");
            var error = new StringWriter();
            var exit = await DarlingCliCommands.DiagnosticsBundleAsync(
                new[] { bundle, "--config", WriteConfig(root.FullName, scratch.ConnectionString), "--log-dir", root.FullName }, new StringWriter(), error, ct);

            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.LeakGuard, exit);
            Assert.False(File.Exists(bundle));
            var text = error.ToString();
            Assert.Contains("a name source could not be read, so the bundle cannot prove it removed every name", text, StringComparison.Ordinal);
            Assert.Contains("config_monitored_servers", text, StringComparison.Ordinal);
            Assert.DoesNotContain("alpha-sql-01", text, StringComparison.Ordinal);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            root.Delete(recursive: true);
        }
    }

    public static TheoryData<string, string> EachNameSource() => new()
    {
        { "config_monitored_servers", "registry" },
        { "config_monitored_servers (aws role)", "registryAws" },
        { "servers", "servers" },
        { "pg_roles", "roles" },
        { "pg_database", "databases" },
        { "collect.database_states", "inventory" },
        { "collect.pg_database_stats", "pgInventory" },
    };

    [Theory]
    [MemberData(nameof(EachNameSource))]
    public async Task AnyNameSourceFailing_WhileTheStoreIsUp_ExitsFive_WithNoFile_AndNamesTheSource(string source, string which)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the live seed-failure test.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var root = Directory.CreateTempSubdirectory("darling-bundle-seedsrc-");
        var ok = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            /* The store is migrated and up; exactly one source's read is broken. */
            const string broken = "SELECT nonexistent_column FROM pg_roles";
            var sql = which switch
            {
                "registry" => new DiagnosticsBundleRunner.NameSourceSql(Registry: broken),
                /* A missing column is tolerated for this source (a store older than 168), so it fails another way. */
                "registryAws" => new DiagnosticsBundleRunner.NameSourceSql(RegistryAws: "SELECT 1 / 0"),
                "servers" => new DiagnosticsBundleRunner.NameSourceSql(Servers: broken),
                "roles" => new DiagnosticsBundleRunner.NameSourceSql(Roles: broken),
                "databases" => new DiagnosticsBundleRunner.NameSourceSql(Databases: broken),
                "inventory" => new DiagnosticsBundleRunner.NameSourceSql(Inventory: broken),
                _ => new DiagnosticsBundleRunner.NameSourceSql(PgInventory: broken),
            };

            var options = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--log-dir", root.FullName }).Options!;
            var config = new DarlingConfig { Servers = { new MonitoredServer { Name = "alpha-sql-01" } } };
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var outcome = await DiagnosticsBundleRunner.BuildAsync(options, config, scratch.ConnectionString, postgres, null, null, ct, sql);

            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.LeakGuard, outcome.ExitCode);
            Assert.Null(outcome.Text);
            Assert.StartsWith(source + ": a name source could not be read", outcome.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("alpha-sql-01", outcome.Message, StringComparison.Ordinal);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task EveryNameSourceReadable_OnAMigratedStore_ReturnsNoFailure()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the live seed-failure test.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var ok = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var result = await DiagnosticsBundleRunner.SeedFromStoreAsync(new BundleAliaser(), source, ct);
            Assert.Null(result.FailedNameSource);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
    private const string StoredRole = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string StoredExternalId = "ext-7Hq2mZ9vLx";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /* #5452: a server known only to the store registry has its role and external ID read from the store. */
    [Fact]
    public async Task AStoreOnlyServersAwsRole_IsAliased_AndItsExternalIdRemoved()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the live seed-failure test.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var ok = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await ExecAsync(connection,
                    "INSERT INTO config.config_monitored_servers (server_id, name, host, database, auth, username, engine, port, aws_role_arn, aws_external_id) "
                    + $"VALUES (9101, 'store-only-pg', 'store-only-pg.example.test', 'postgres', 'sql', 'monitor', 'postgres', 5432, '{StoredRole}', '{StoredExternalId}')", ct);
            }

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var aliaser = new BundleAliaser();
            var result = await DiagnosticsBundleRunner.SeedFromStoreAsync(aliaser, source, ct);
            Assert.Null(result.FailedNameSource);

            var text = aliaser.Alias($"The AWS role {StoredRole} on this server is not in allowedAwsRoles (external id {StoredExternalId}, account 123456789012).");
            Assert.DoesNotContain(StoredExternalId, text, StringComparison.Ordinal);
            Assert.DoesNotContain("123456789012", text, StringComparison.Ordinal);
            Assert.DoesNotContain("darling-monitor", text, StringComparison.Ordinal);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    /* A store older than 168 has no role columns: the bundle still builds, with nothing to alias from that read. */
    [Fact]
    public async Task AStoreWithoutTheAwsRoleColumns_StillSeeds()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the live seed-failure test.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var ok = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await ExecAsync(connection,
                    "ALTER TABLE config.config_monitored_servers DROP COLUMN aws_external_id_set, DROP COLUMN aws_external_id, DROP COLUMN aws_role_arn", ct);
            }

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var result = await DiagnosticsBundleRunner.SeedFromStoreAsync(new BundleAliaser(), source, ct);
            Assert.Null(result.FailedNameSource);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
