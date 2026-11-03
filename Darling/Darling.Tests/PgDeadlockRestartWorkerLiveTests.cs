/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7295 through the worker: a new PostgreSQL deadlock pair that was counted before a service restart is paged once after it
/// and never again. Drives <c>EvaluatePgDeadlocksAsync</c> itself, so the worker's own wiring is under test: the hook that
/// saves the pairs still waiting for a page, the seed at the first pass of a new process, and the clear after a delivered
/// page. Three worker instances over one store stand for three lives of the service. The second test is the same wiring for a
/// pair that is dropped (#7188): its reports leave the hour before the page, so the saved pair must go too and a restart must
/// not bring it back.
/// </summary>
/* #1776 own-store: ScratchPostgres mints a database of its own. */
[Collection("live-postgres")]
public sealed class PgDeadlockRestartWorkerLiveTests
{
    private const string ServerName = "pgdeadlock-restart-worker";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly string Key = ServerId.ToString(CultureInfo.InvariantCulture);

    private const string Graph =
        "Process 3101 waits for ShareLock on transaction 5501; blocked by process 3102.\n"
        + "Process 3102 waits for ShareLock on transaction 5500; blocked by process 3101.\n"
        + "Process 3101: UPDATE orders SET qty = 1 WHERE id = 7\n"
        + "Process 3102: UPDATE stock SET qty = 1 WHERE id = 7";

    private const string KnownPairGraph =
        "Process 3101 waits for ShareLock on transaction 5501; blocked by process 3102.\n"
        + "Process 3102 waits for ShareLock on transaction 5500; blocked by process 3101.\n"
        + "Process 3101: UPDATE orders SET qty = 1 WHERE id = 7\n"
        + "Process 3102: UPDATE ledger SET qty = 1 WHERE id = 7";

    private const string NewPairHash = "AAAA0000BBBB1111CCCC2222DDDD3333";
    private const string KnownPairHash = "EEEE4444FFFF5555AAAA6666BBBB7777";

    private static async Task PlantReportAsync(
        NpgsqlConnection connection, DateTime occurredAt, CancellationToken ct,
        string hash = NewPairHash, string graph = Graph)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO collect.pg_deadlocks
                (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count,
                 deadlock_hash, lock_modes, resources, victim_statement, graph_text)
            VALUES
                ($1, $2, $3, $4, $2, 3101, 2, $6, 'ShareLock', 'transaction',
                 'UPDATE orders SET qty = 1 WHERE id = 7', $5)
            """;
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(occurredAt, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(graph);
        command.Parameters.AddWithValue(hash);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The report leaves the hour: the reader no longer returns it.</summary>
    private static async Task AgeOutReportAsync(NpgsqlConnection connection, string hash, CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM collect.pg_deadlocks WHERE deadlock_hash = $1";
        command.Parameters.AddWithValue(hash);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> AlertRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT count(*) FROM config.config_alert_log WHERE server_name = $1 AND metric_name = 'Deadlocks Detected'";
        command.Parameters.AddWithValue(ServerName);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>One life of the service: a fresh worker, so nothing is in memory, sending to <paramref name="webhookUrl"/>.</summary>
    private static (Func<Task> Pass, DarlingWorker Worker) StartLife(
        NpgsqlDataSource dataSource, string webhookUrl, CancellationToken ct)
    {
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = webhookUrl;
        var settings = new DarlingAlertSettings(config);
        var history = new PgAlertHistoryStore(dataSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

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
            StoreForTests = dataSource,
        };
        var flags = BindingFlags.NonPublic | BindingFlags.Instance;
        typeof(DarlingWorker).GetField("_alertDeliverer", flags)!.SetValue(worker, deliverer);
        var evaluate = typeof(DarlingWorker).GetMethod("EvaluatePgDeadlocksAsync", flags)!;

        var runtime = new ServerRuntime
        {
            Config = new MonitoredServer { Name = ServerName, Host = ServerName },
            ConnectionString = "Host=unused",
            Target = new CollectorTargetInfo(),
            StorageName = ServerName,
            ServerId = ServerId,
        };
        var snapshot = new AlertServerSnapshot(Key, ServerName, true, null, null, false, false, null) { ServerId = ServerId };

        return (() => (Task)evaluate.Invoke(worker, new object[] { runtime, snapshot, config, ct })!, worker);
    }

    [Fact]
    public async Task ANewPairCountedBeforeARestart_IsPagedOnceAfterIt_ByTheWorkersOwnWiring_AndNeverAgain()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the PostgreSQL deadlock restart worker test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await using var dataSource = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                SearchPath = "collect,config,public",
            }.ConnectionString);
            var state = new PgAlertStateStore(dataSource);

            await PlantReportAsync(connection, DateTime.UtcNow.AddMinutes(-2), ct);

            /* First life: the pair is new and is counted, then the send reaches no channel (the endpoint answers 500), which is
               the page that never went out. The worker's own hook must have saved the pair for a restart to find. */
            using var broken = new CapturingWebhookEndpoint(statusCode: 500);
            var (firstPass, _) = StartLife(dataSource, broken.Url, ct);
            await firstPass();
            Assert.Single(broken.Bodies);
            var waiting = await state.LoadUnannouncedPairsAsync(Key);
            Assert.NotNull(waiting);
            Assert.Single(waiting!);

            /* Second life: nothing in memory, and the signature store already knows the pair. Only the worker's seed tells it
               the pair is still waiting, and only that pages it. */
            using var healthy = new CapturingWebhookEndpoint();
            var (secondPass, _) = StartLife(dataSource, healthy.Url, ct);
            await secondPass();
            Assert.Single(healthy.Bodies);

            /* The page went out, so the worker cleared the saved pair. */
            Assert.Empty((await state.LoadUnannouncedPairsAsync(Key))!);

            /* Third life: the pair is known and announced, so it is held and nothing is sent again. */
            var (thirdPass, _) = StartLife(dataSource, healthy.Url, ct);
            await thirdPass();
            await thirdPass();
            Assert.Single(healthy.Bodies);
            Assert.Single(broken.Bodies);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task ANewPairDroppedBeforeItsPage_IsNotBroughtBackByARestart_AndAKnownPairStaysHeld()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the PostgreSQL deadlock restart worker test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await using var dataSource = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                SearchPath = "collect,config,public",
            }.ConnectionString);
            var state = new PgAlertStateStore(dataSource);

            /* One pair counted ten days ago is known to the signature store. The reader hands the worker the normalized graph, so
               the key is made from that. */
            var knownValue = PgDeadlockSignature.For(
                PgDeadlockLogParser.NormalizeGraph(KnownPairGraph)?.TrimEnd('\n'), 2, "ShareLock", "transaction").Value;
            await new DeadlockSignatureStore(dataSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds).AddAsync(
                ServerId, new Dictionary<string, int> { [knownValue] = 1 }, DateOnly.FromDateTime(DateTime.UtcNow.AddDays(-10)), ct);

            /* First life: a new pair is counted and its page reaches no channel. The pair is saved for a restart. */
            await PlantReportAsync(connection, DateTime.UtcNow.AddMinutes(-2), ct);
            using var broken = new CapturingWebhookEndpoint(statusCode: 500);
            var (firstPass, _) = StartLife(dataSource, broken.Url, ct);
            await firstPass();
            Assert.Single(broken.Bodies);
            Assert.Single((await state.LoadUnannouncedPairsAsync(Key))!);

            /* The new pair's only report leaves the hour and the known pair reports once. Nothing is new any more, so the fire
               is held and nothing is sent, and the saved pair goes with the dropped one. */
            await AgeOutReportAsync(connection, NewPairHash, ct);
            await PlantReportAsync(connection, DateTime.UtcNow.AddMinutes(-1), ct, KnownPairHash, KnownPairGraph);
            await firstPass();
            Assert.Single(broken.Bodies);
            Assert.Empty((await state.LoadUnannouncedPairsAsync(Key))!);
            var rowsBeforeRestart = await AlertRowsAsync(connection, ct);

            /* Second life: the seed finds no pair waiting, so it brings none back and leaves the count watermark where the held
               fire put it. The known pair is still held: nothing is sent and no row is written. */
            using var healthy = new CapturingWebhookEndpoint();
            var (secondPass, _) = StartLife(dataSource, healthy.Url, ct);
            await secondPass();
            await secondPass();
            Assert.Empty(healthy.Bodies);
            Assert.Empty((await state.LoadUnannouncedPairsAsync(Key))!);
            Assert.Equal(rowsBeforeRestart, await AlertRowsAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
