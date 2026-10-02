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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The record of the one-time legacy long-query session drop, against a real store: the record one runner writes is read
/// back by a new runner (a service restart) and again after a reconnect, and neither drops the session a second time.
/// The drop itself is replaced on the runner, so no monitored server is needed; the record goes through the real
/// store-backed implementation (<see cref="StoreLegacyLongQueryRecords"/>).
/// </summary>
[Collection("live-postgres")]
public sealed class LegacyLongQueryRecordLiveTests
{
    /* #1776 own-store: the rows are keyed by a distinctive fake server id and removed in the cleanup. */
    private const int LiveServerId = -496100;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheRecord_WrittenByOneRunner_IsReadBackByANewRunnerAndAfterAReconnect_AndNeitherDropsAgain(bool azureSqlDatabase)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live legacy long-query record test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var databases = azureSqlDatabase ? new[] { "alpha", "beta" } : new[] { string.Empty };
        var legacyDrops = new List<string>();

        (DarlingCollectorRunner Runner, DarlingWorker.ServerLoopState State) Build()
        {
            var config = new MonitoredServer { Name = "legacy-live", Host = "legacy-live.database.windows.net" };
            var runtime = new ServerRuntime
            {
                Config = config,
                ConnectionString = "Server=tcp:legacy-live.database.windows.net,1433;Initial Catalog=master;Encrypt=True",
                Target = new CollectorTargetInfo { IsAzureSqlDb = azureSqlDatabase },
                StorageName = "legacy-live",
                ServerId = LiveServerId,
            };
            var runner = new DarlingCollectorRunner(
                postgres,
                new CollectorDeltaCalculator(),
                databaseScope: (_, _) => new List<string>(),
                separatelyMonitoredDatabases: _ => new List<string>(),
                installId: () => "0a1b2c3d");
            runner.LongQueryTraceListOverrideForTests = (_, _, _, _) => Task.FromResult(new List<string> { "master", "alpha", "beta" });
            runner.LongQueryTraceDatabaseOverrideForTests = (_, database, _, sessionName, _) =>
            {
                if (sessionName == LongQueryCompletionsCollector.LegacyXeSessionName)
                {
                    legacyDrops.Add(database);
                }

                return Task.CompletedTask;
            };

            return (runner, new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime });
        }

        Task Reconcile((DarlingCollectorRunner Runner, DarlingWorker.ServerLoopState State) install) =>
            DarlingWorker.ReconcileLongQueryTraceAsync(
                install.State, install.Runner, enabled: true, Array.Empty<LongQueryTraceRegistration>(), Array.Empty<string>(),
                DateTime.UtcNow, new DarlingSelfAlertTests.CapturingLogger(), CancellationToken.None);

        var bodySucceeded = false;
        try
        {
            /* The first install drops the session once in each database, and records it in the store. */
            var first = Build();
            await Reconcile(first);
            Assert.Equal(databases, legacyDrops);

            var stored = await first.Runner.GetCollectorStateAsync(LiveServerId, LegacyLongQuerySession.StateCollector, ct);
            Assert.Equal(databases.Select(LegacyLongQuerySession.StateKey).OrderBy(k => k, StringComparer.Ordinal), stored.Keys.OrderBy(k => k, StringComparer.Ordinal));
            Assert.All(stored.Values, value => Assert.True(DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out _), value));

            /* A service restart: a new runner reads the record back, and drops nothing. */
            var restarted = Build();
            await Reconcile(restarted);
            Assert.Equal(databases, legacyDrops);

            /* A reconnect: the record is read again, and nothing is dropped. */
            restarted.Runner.OnServerReconnected(LiveServerId);
            restarted.State.LongQueryTraceApplied = null;
            restarted.State.LongQueryTraceAppliedKey = null;
            restarted.State.LongQueryTraceAppliedAtUtc = null;
            await Reconcile(restarted);
            Assert.Equal(databases, legacyDrops);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "DELETE FROM collect.collector_state WHERE server_id = " + LiveServerId.ToString(CultureInfo.InvariantCulture), connection);
        command.CommandTimeout = 30;
        await command.ExecuteNonQueryAsync(ct);
    }
}
