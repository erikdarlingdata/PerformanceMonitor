using System;
using System.Collections.Concurrent;
using System.Globalization;
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
/// #7083 (follow-up review): the PostgreSQL wiring of the held-row observation, through the worker. A burst of root blockers
/// whose waits sit under the page floor writes one held <c>Blocking Detected</c> row however often the pass repeats, and a
/// capture newer than that burst earns the next row once the cooldown has passed. Drives <c>EvaluatePgBlockingAsync</c>
/// itself against a store that holds real <c>collect.pg_blocking_edges</c> rows, with the real deliverer and history store.
/// </summary>
[Collection("live-postgres")]
public sealed class PgBlockingHeldRowWorkerLiveTests
{
    // #1776 own-store: ScratchPostgres mints a database of its own.
    private const string ServerName = "pgblocking-heldrow-worker";
    private const string Metric = "Blocking Detected";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static async Task<long> HeldRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT count(*) FROM config.config_alert_log WHERE server_name = $1 AND metric_name = $2", connection);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Metric);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task PlantBurstAsync(NpgsqlConnection connection, long collectionId, DateTime capturedAt, CancellationToken ct)
    {
        /* Three distinct roots (the PostgreSQL bar is three), each with one victim that has waited 5 s, far under the 15 s floor. */
        for (var root = 0; root < 3; root++)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT INTO collect.pg_blocking_edges
                    (collection_id, collection_time, server_id, server_name, blocked_pid, blocking_pid,
                     blocking_backend_id, database_name, blocked_query, blocking_query,
                     query_text_may_be_truncated, blocking_is_idle_in_transaction, blocked_query_duration_ms)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7, 'appdb', 'select victim', 'update root set x = 1', false, false, 5000)
                """;
            command.Parameters.AddWithValue(collectionId * 10 + root);
            command.Parameters.AddWithValue(DateTime.SpecifyKind(capturedAt, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(52_000 + root);
            command.Parameters.AddWithValue(51_000 + root);
            command.Parameters.AddWithValue((long)(900_000 + root));
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    [Fact]
    public async Task ABurstUnderTheFloor_WritesOneHeldRowOverRepeatedPasses_AndANewerCaptureAfterTheCooldownWritesTheSecond()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the PostgreSQL held-row worker test.");

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

            var config = new DarlingConfig();
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
            var lastHold = (ConcurrentDictionary<string, DateTime>)
                typeof(DarlingWorker).GetField("_lastPgBlockingHold", flags)!.GetValue(worker)!;
            var evaluate = typeof(DarlingWorker).GetMethod("EvaluatePgBlockingAsync", flags)!;

            var runtime = new ServerRuntime
            {
                Config = new MonitoredServer { Name = ServerName, Host = ServerName },
                ConnectionString = "Host=unused",
                Target = new CollectorTargetInfo(),
                StorageName = ServerName,
                ServerId = ServerId,
            };
            var key = ServerId.ToString(CultureInfo.InvariantCulture);
            var snapshot = new AlertServerSnapshot(key, ServerName, true, null, null, false, false, null) { ServerId = ServerId };

            Task Pass() => (Task)evaluate.Invoke(worker, new object[] { runtime, snapshot, config, ct })!;

            var now = DateTime.UtcNow;
            await PlantBurstAsync(connection, 1, now.AddMinutes(-3), ct);

            for (var i = 0; i < 5; i++)
            {
                await Pass();
            }

            Assert.Equal(1, await HeldRowsAsync(connection, ct));

            /* The cooldown passes but nothing newer was captured: still the one row. */
            Assert.True(lastHold.ContainsKey(key));
            lastHold[key] = DateTime.UtcNow.AddHours(-2);
            await Pass();
            Assert.Equal(1, await HeldRowsAsync(connection, ct));

            /* A capture newer than the burst the row was written for earns the second row, once. */
            await PlantBurstAsync(connection, 2, now.AddMinutes(-1), ct);
            await Pass();
            Assert.Equal(2, await HeldRowsAsync(connection, ct));
            await Pass();
            await Pass();
            Assert.Equal(2, await HeldRowsAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
