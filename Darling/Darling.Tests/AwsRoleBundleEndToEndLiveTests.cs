/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres. */

/// <summary>
/// #5452: each server can use its own AWS role, end to end through the diagnostics bundle. The worker's role arm writes a
/// PERMISSIONS row to collection_log for a server that has a role and an external ID; a bundle is then built from that
/// store (the server is known only to the store registry); and the bundle names the role as an alias and never holds the
/// external ID. Names are synthetic (the example account 123456789012).
/// </summary>
public sealed class AwsRoleBundleEndToEndLiveTests
{
    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string ExternalId = "ext-7Hq2mZ9vLx";
    private const string ServerName = "alpha-pg-01";
    private const string Host = "alpha-pg-01.abcdefghijkl.us-east-1.rds.amazonaws.com";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* The server as the store holds it: the registry row (with its role and external ID) and the collected-data server row. */
    private static async Task SeedServerAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(
            "INSERT INTO config.config_monitored_servers (server_id, name, host, database, auth, username, engine, port, aws_role_arn, aws_external_id) "
            + "VALUES (9201, $1, $2, 'postgres', 'sql', 'pgcollect', 'postgres', 5432, $3, $4)", connection);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(Host);
        insert.Parameters.AddWithValue(Role);
        insert.Parameters.AddWithValue(ExternalId);
        await insert.ExecuteNonQueryAsync(ct);

        await using var registered = new NpgsqlCommand(
            "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, created_date, modified_date) "
            + "VALUES (9201, $1, $1, true, now(), now())", connection);
        registered.Parameters.AddWithValue(ServerName);
        await registered.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task ABundleBuiltFromARoleServersStore_NamesTheRoleAsAnAlias_AndNeverHoldsTheExternalId()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the live bundle test.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var root = Directory.CreateTempSubdirectory("darling-bundle-awsrole-");
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await SeedServerAsync(connection, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            // The role arm's input: an STS refusal whose own text repeats the external ID, read through the real RDS reader.
            var sts = new FakeSts((_, _) => throw FakeSts.Error("AccessDenied", $"not authorized (ExternalId={ExternalId})"));
            var cache = new AwsRoleCredentialCache(AwsRoleAllowlist.From([Role]), null, sts.Factory, FakeSts.Source, new ManualTimeProvider(), null, new CapturingAwsLogger());
            var ingestor = new RdsCpuIngestor(
                NpgsqlDataSource.Create("Host=localhost;Port=1;Database=unused"),
                verifier: RdsEndpointVerifier.ForTests(enforce: true, loginProbe: (_, _) => Task.FromResult<string?>(null)),
                roles: cache);
            var thrown = await Record.ExceptionAsync(
                () => ingestor.IngestAsync(9201, ServerName, Host, "Host=" + Host, new AwsRoleKey(Role, ExternalId), ct));
            var fault = DarlingWorker.AwsRoleConfigurationFault(thrown!);
            Assert.NotNull(fault);

            // The worker's role arm: its note, written to collection_log as PERMISSIONS in the storage-time slot.
            var runtime = new ServerRuntime
            {
                Config = new MonitoredServer { Name = ServerName, Host = Host },
                ConnectionString = "Host=" + Host,
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
                StorageName = ServerName,
                ServerId = 9201,
            };
            var note = DarlingWorker.AwsRoleFaultNote(new CapturingAwsLogger(), ServerName, "pg_plan_capture", fault!);
            await DarlingObservability.LogCollectionAsync(
                postgres, runtime, "pg_plan_capture", "PERMISSIONS", 0, 0, 5, note,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: null, null, ct);

            // The store really holds the role in that row, so the bundle's alias is doing the work.
            await using (var verify = await postgres.OpenConnectionAsync(ct))
            await using (var read = new NpgsqlCommand(
                "SELECT error_message FROM collection_log WHERE server_id = 9201 AND status = 'PERMISSIONS'", verify))
            {
                var stored = (string?)await read.ExecuteScalarAsync(ct);
                Assert.Contains(Role, stored, StringComparison.Ordinal);
                Assert.DoesNotContain(ExternalId, stored, StringComparison.Ordinal);
            }

            var options = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--log-dir", root.FullName }).Options!;
            var outcome = await DiagnosticsBundleRunner.BuildAsync(options, new DarlingConfig(), scratch.ConnectionString, postgres, null, null, ct);

            Assert.NotEqual(DarlingCliCommands.DiagnosticsBundleExitCode.LeakGuard, outcome.ExitCode);
            Assert.NotNull(outcome.Text);
            Assert.DoesNotContain(ExternalId, outcome.Text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(Role, outcome.Text, StringComparison.Ordinal);
            Assert.DoesNotContain("darling-monitor", outcome.Text, StringComparison.Ordinal);
            Assert.DoesNotContain("123456789012", outcome.Text, StringComparison.Ordinal);
            Assert.DoesNotContain(ServerName, outcome.Text, StringComparison.Ordinal);

            // The bundle still says what happened: the collector's PERMISSIONS answer, with the role under its alias.
            Assert.Contains("PERMISSIONS", outcome.Text, StringComparison.Ordinal);
            Assert.Contains("role-", outcome.Text, StringComparison.Ordinal);
            Assert.Contains("trust policy", outcome.Text, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            root.Delete(recursive: true);
        }
    }

    /* A second row that carries the external ID as plain text, the way an unscrubbed message from AWS could: the bundle
       removes it as well, so no text the store holds can carry the ID out. */
    [Fact]
    public async Task AnExternalIdTheStoreHoldsInAnyRowText_IsRemovedFromTheBundle()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the live bundle test.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var root = Directory.CreateTempSubdirectory("darling-bundle-awsrole-");
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await SeedServerAsync(connection, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var runtime = new ServerRuntime
            {
                Config = new MonitoredServer { Name = ServerName, Host = Host },
                ConnectionString = "Host=" + Host,
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
                StorageName = ServerName,
                ServerId = 9201,
            };
            await DarlingObservability.LogCollectionAsync(
                postgres, runtime, "pg_plan_capture", "ERROR", 0, 0, 5, $"AWS answered for role {Role}: request externalId={ExternalId}",
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: null, null, ct);

            var options = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--log-dir", root.FullName }).Options!;
            var outcome = await DiagnosticsBundleRunner.BuildAsync(options, new DarlingConfig(), scratch.ConnectionString, postgres, null, null, ct);

            Assert.NotEqual(DarlingCliCommands.DiagnosticsBundleExitCode.LeakGuard, outcome.ExitCode);
            Assert.NotNull(outcome.Text);
            Assert.DoesNotContain(ExternalId, outcome.Text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(Role, outcome.Text, StringComparison.Ordinal);
            Assert.DoesNotContain("darling-monitor", outcome.Text, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            root.Delete(recursive: true);
        }
    }
}
