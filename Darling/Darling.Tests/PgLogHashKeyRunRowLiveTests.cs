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
/// #4004's review, round 3, through the worker's real run path: a key the start discarded and regenerated is noted on the
/// collection_log row of the FIRST pg_log_events run after it, and only that run. The rotation is armed on the worker the way
/// the service start arms it, each collector run goes through <c>RunOneAsync</c> (here via the at-connect entry) with only the
/// collector's own read replaced, and the rows come back out of a real store. The holder's own rules are in
/// <see cref="PgLogHashKeyTests"/>; this class fails when the worker stops applying the note to the runs it records.
/// </summary>
[Collection("live-postgres")]
public sealed class PgLogHashKeyRunRowLiveTests
{
    /* #1776 own-store: the rows are keyed by a distinctive fake server id and removed in the cleanup. */
    private const int ServerId = -441203;
    private const string Rotated = "pg_log_events";
    private const string Other = "pg_wait_sampling";
    private const string Note = "the store's log-hash key was replaced at service start (the old key was discarded because its directory was open to other users)";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheRotationNote_RidesTheFirstPgLogEventsRow_AndNoOtherRow()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live log-hash rotation run-row test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "log-hash-rotation", ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);

            var host = "log-hash-rotation";
            var config = new MonitoredServer { Name = host, Host = host };
            var runtime = new ServerRuntime
            {
                Config = config,
                ConnectionString = $"Host={host};Port=5432",
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
                StorageName = host,
                ServerId = ServerId,
            };
            var server = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime };
            var runner = new DarlingCollectorRunner(
                postgres,
                new CollectorDeltaCalculator(),
                databaseScope: (_, _) => new List<string>(),
                separatelyMonitoredDatabases: _ => new List<string>(),
                installId: () => "0a1b2c3d");

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

            /* The collector's read is replaced; the run's rows tell the runs apart (the nth run of a collector reports n rows). */
            var runsOf = new Dictionary<string, int>();
            worker.DispatchOverrideForTests = (collector, _, _) =>
            {
                runsOf[collector] = runsOf.GetValueOrDefault(collector) + 1;
                return Task.FromResult(new CollectorRunResult(
                    runsOf[collector], 5, 7, new[] { new CollectorMeasurement("lines_read", 12) }, HostNote: "no new classifiable log events"));
            };

            /* The start arms it from its one load; a rotation armed here is taken by the first pg_log_events run. */
            worker.LogHashKeyRotationForTests.Arm(Note);

            await RunAsync(worker, server, runner, Other, ct);
            await RunAsync(worker, server, runner, Rotated, ct);
            await RunAsync(worker, server, runner, Rotated, ct);
            await RunAsync(worker, server, runner, Other, ct);

            var rows = await ReadRowsAsync(connection, ct);

            var wait = rows.Where(r => r.Collector == Other).OrderBy(r => r.Rows).ToList();
            Assert.Equal(2, wait.Count);
            Assert.All(wait, r => Assert.Equal("SUCCESS", r.Status));
            Assert.All(wait, r => Assert.DoesNotContain("log-hash key", r.Message ?? string.Empty, StringComparison.Ordinal));

            var log = rows.Where(r => r.Collector == Rotated).OrderBy(r => r.Rows).ToList();
            Assert.Equal(2, log.Count);
            Assert.All(log, r => Assert.Equal("SUCCESS", r.Status));
            Assert.Contains(Note, log[0].Message, StringComparison.Ordinal);
            Assert.Contains("lines_read", log[0].Message, StringComparison.Ordinal);
            Assert.DoesNotContain("log-hash key", log[1].Message ?? string.Empty, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task RunAsync(
        DarlingWorker worker, DarlingWorker.ServerLoopState server, DarlingCollectorRunner runner, string collector, CancellationToken ct)
    {
        var effective = StoreConfigProvider.ResolveSchedule(collector, ServerId, worker.ScheduleOverridesForTest);
        Assert.True(await worker.RunOnLoadAsync(server, runner, collector, ServerId, effective, ct));
    }

    private static async Task<List<(string Collector, string Status, int Rows, string? Message)>> ReadRowsAsync(
        NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT collector_name, status, rows_collected, error_message FROM collection_log WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(ServerId);
        var rows = new List<(string, string, int, string?)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetString(3)));
        }

        return rows;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
