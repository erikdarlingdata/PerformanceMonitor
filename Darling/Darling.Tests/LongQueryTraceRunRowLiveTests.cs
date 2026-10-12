/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5378: the row a long-query run writes when the trace's create was refused. The reconcile keeps the refusal on the server's
/// loop state, and the run (<c>RunOneAsync</c>, here through the at-connect entry) turns it into a collection_log row without
/// opening a connection to the monitored server. A denied create (error 15247, the login lacks ALTER ANY EVENT SESSION) is a
/// PERMISSIONS row that names the grant, and <c>get_collection_health</c> bands the collector NO_PERMISSIONS from it. Any other
/// refusal is still a SESSION_MISSING row. Both run the real reconcile (the create is replaced by the refusal) and the real
/// run-row write against a real store, so the catch that picks the status is what each test fails on.
/// </summary>
[Collection("live-postgres")]
public sealed class LongQueryTraceRunRowLiveTests
{
    /* #1776 own-store: the rows are keyed by distinctive fake server ids and removed in the cleanup. */
    private const int PermissionServerId = -537810;
    private const int OtherFaultServerId = -537811;
    private const string Collector = "long_query_completions";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ADeniedCreate_WritesAPermissionsRow_ThatNamesTheGrant_AndHealthBandsItNoPermissions()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live long-query run-row test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, PermissionServerId, "run-row-denied", ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);

            var (worker, server) = await RunAfterRefusalAsync(
                postgres, PermissionServerId, LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(
                    15247, 14, "User does not have permission to perform this action."), ct);

            Assert.True(server.LongQueryTraceFaultIsPermission);

            var row = Assert.Single(await ReadRowsAsync(connection, PermissionServerId, ct));
            Assert.Equal("PERMISSIONS", row.Status);
            Assert.Contains("ALTER ANY EVENT SESSION", row.Message, StringComparison.Ordinal);

            /* get_collection_health: every run in the window was a denial, so the collector bands NO_PERMISSIONS, and the
               denial is counted as one rather than as a capture that broke. */
            var health = await DarlingDataReader.GetCollectionHealthAsync(
                postgres, PermissionServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-1)), ct);
            var longQuery = health.Single(h => h.CollectorName == Collector);
            Assert.Equal(1, longQuery.PermissionDeniedCount);
            Assert.Equal(CollectorHealthClassifier.NoPermissions, longQuery.HealthStatus);

            GC.KeepAlive(worker);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ARefusalThatIsNotAPermissionDenial_StillWritesASessionMissingRow()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live long-query run-row test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, OtherFaultServerId, "run-row-other", ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);

            var (_, server) = await RunAfterRefusalAsync(
                postgres, OtherFaultServerId, LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(
                    1105, 17, "Could not allocate space for object in database because the filegroup is full."), ct);

            Assert.False(server.LongQueryTraceFaultIsPermission);

            var row = Assert.Single(await ReadRowsAsync(connection, OtherFaultServerId, ct));
            Assert.Equal("SESSION_MISSING", row.Status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>
    /// Runs the worker's real reconcile with every create refused with <paramref name="refusal"/>, then the run of the
    /// long-query collector, which reads the reconcile's kept fault and writes its row.
    /// </summary>
    private static async Task<(DarlingWorker Worker, DarlingWorker.ServerLoopState Server)> RunAfterRefusalAsync(
        NpgsqlDataSource postgres, int serverId, Exception refusal, CancellationToken ct)
    {
        var host = $"run-row-{Math.Abs(serverId)}";
        var config = new MonitoredServer { Name = host, Host = host };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{host},1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo(),
            StorageName = host,
            ServerId = serverId,
        };
        var server = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime };

        var runner = new DarlingCollectorRunner(
            postgres,
            new CollectorDeltaCalculator(),
            databaseScope: (_, _) => new List<string>(),
            separatelyMonitoredDatabases: _ => new List<string>(),
            installId: () => "0a1b2c3d");
        runner.LongQueryTraceListOverrideForTests = (_, _, _, _) => Task.FromResult(new List<string> { "master" });
        runner.LegacyLongQueryRecordsForTests = new LongQueryTraceLifecycleTests.InMemoryLegacyRecords();
        runner.LegacyLongQueryPresentForTests = (_, _, _) => Task.FromResult(false);
        runner.LongQueryTraceReplicaStateForTests = (_, _) => default;
        runner.LongQueryTraceStepOverrideForTests = (_, _, _, step, _, _) =>
            step is LongQueryTraceStep.Stop or LongQueryTraceStep.Drop ? Task.CompletedTask : Task.FromException(refusal);

        var worker = new DarlingWorker(
            NullLogger<DarlingWorker>.Instance,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator())
        {
            StoreForTests = postgres,
        };

        await DarlingWorker.ReconcileLongQueryTraceAsync(
            server, runner, true, Array.Empty<LongQueryTraceRegistration>(), Array.Empty<string>(),
            new DateTime(2026, 10, 6, 12, 0, 0, DateTimeKind.Utc), NullLogger.Instance, ct);
        Assert.NotNull(server.LongQueryTraceFault);

        var effective = StoreConfigProvider.ResolveSchedule(Collector, serverId, worker.ScheduleOverridesForTest);
        await worker.RunOnLoadAsync(server, runner, Collector, serverId, effective, ct);
        return (worker, server);
    }

    private static async Task<List<(string Status, string? Message)>> ReadRowsAsync(
        NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT status, error_message FROM collection_log WHERE server_id = $1 AND collector_name = $2", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(Collector);
        var rows = new List<(string, string?)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
        }

        return rows;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var id in new[] { PermissionServerId, OtherFaultServerId })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", id);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", id);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", id);
        }
    }
}
