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
/// store-backed implementation (<see cref="StoreLegacyLongQueryRecords"/>). The record belongs to one server: another server's
/// record in the same database name does not stop a server's drop, and a server's own record stops its second one.
/// </summary>
[Collection("live-postgres")]
public sealed class LegacyLongQueryRecordLiveTests
{
    /* #1776 own-store: the rows are keyed by distinctive fake server ids and removed in the cleanup. The two-server test keeps
       its own pair, so neither test's cleanup touches the other's rows. */
    private const int LiveServerId = -496100;
    private const int FirstServerId = -496110;
    private const int SecondServerId = -496111;

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

        (DarlingCollectorRunner Runner, DarlingWorker.ServerLoopState State) Build() =>
            BuildInstall(postgres, LiveServerId, "legacy-live", azureSqlDatabase, (_, database) => legacyDrops.Add(database));

        var bodySucceeded = false;
        try
        {
            /* The first install drops the session once in each database, and records it in the store. */
            var first = Build();
            await ReconcileAsync(first);
            Assert.Equal(databases, legacyDrops);

            var stored = await first.Runner.GetCollectorStateAsync(LiveServerId, LegacyLongQuerySession.StateCollector, ct);
            Assert.Equal(databases.Select(LegacyLongQuerySession.StateKey).OrderBy(k => k, StringComparer.Ordinal), stored.Keys.OrderBy(k => k, StringComparer.Ordinal));
            Assert.All(stored.Values, value => Assert.True(DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out _), value));

            /* A service restart: a new runner reads the record back, and drops nothing. */
            var restarted = Build();
            await ReconcileAsync(restarted);
            Assert.Equal(databases, legacyDrops);

            /* A reconnect: the record is read again, and nothing is dropped. */
            restarted.Runner.OnServerReconnected(LiveServerId);
            restarted.State.LongQueryTraceApplied = null;
            restarted.State.LongQueryTraceAppliedKey = null;
            restarted.State.LongQueryTraceAppliedAtUtc = null;
            await ReconcileAsync(restarted);
            Assert.Equal(databases, legacyDrops);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheRecordOfOneServer_DoesNotStopAnotherServersDropInTheSameDatabaseName_AndEachServersOwnRecordStopsItsSecondDrop(bool azureSqlDatabase)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live legacy long-query record test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteServersAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var databases = azureSqlDatabase ? new[] { "alpha", "beta" } : new[] { string.Empty };
        var expectedKeys = databases.Select(LegacyLongQuerySession.StateKey).OrderBy(k => k, StringComparer.Ordinal).ToList();
        var legacyDrops = new List<(int ServerId, string Database)>();

        (DarlingCollectorRunner Runner, DarlingWorker.ServerLoopState State) BuildFirst() =>
            BuildInstall(postgres, FirstServerId, "legacy-live-first", azureSqlDatabase, (id, database) => legacyDrops.Add((id, database)));

        (DarlingCollectorRunner Runner, DarlingWorker.ServerLoopState State) BuildSecond() =>
            BuildInstall(postgres, SecondServerId, "legacy-live-second", azureSqlDatabase, (id, database) => legacyDrops.Add((id, database)));

        List<string> DropsOf(int serverId) => legacyDrops.Where(drop => drop.ServerId == serverId).Select(drop => drop.Database).ToList();

        var bodySucceeded = false;
        try
        {
            /* The first server drops the session once in each database, and records it under its own server id. */
            await ReconcileAsync(BuildFirst());
            Assert.Equal(databases, DropsOf(FirstServerId));

            /* The second server has the same database names. A new runner has read nothing, so the store is all that could
               tell it the session was dropped, and the first server's record is not its own: it still drops in each one. */
            var second = BuildSecond();
            await ReconcileAsync(second);
            Assert.Equal(databases, DropsOf(SecondServerId));
            Assert.Equal(databases, DropsOf(FirstServerId));

            /* Each server holds exactly its own record. */
            foreach (var serverId in new[] { FirstServerId, SecondServerId })
            {
                var stored = await second.Runner.GetCollectorStateAsync(serverId, LegacyLongQuerySession.StateCollector, ct);
                Assert.Equal(expectedKeys, stored.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList());
            }

            /* A service restart: each server's own record, read back from the store, stops its second drop. */
            await ReconcileAsync(BuildSecond());
            await ReconcileAsync(BuildFirst());
            Assert.Equal(databases, DropsOf(SecondServerId));
            Assert.Equal(databases, DropsOf(FirstServerId));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteServersAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// One server's runner and loop state over the real store. The drop of the legacy session is replaced by
    /// <paramref name="onLegacyDrop"/>, called with the server id and the database named, so no monitored server is needed;
    /// the record goes through the store-backed implementation. Each call is a new process: it has read nothing from the
    /// store yet.
    /// </summary>
    private static (DarlingCollectorRunner Runner, DarlingWorker.ServerLoopState State) BuildInstall(
        NpgsqlDataSource postgres, int serverId, string name, bool azureSqlDatabase, Action<int, string> onLegacyDrop)
    {
        var config = new MonitoredServer { Name = name, Host = name + ".database.windows.net" };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = "Server=tcp:" + name + ".database.windows.net,1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo { IsAzureSqlDb = azureSqlDatabase },
            StorageName = name,
            ServerId = serverId,
        };
        var runner = new DarlingCollectorRunner(
            postgres,
            new CollectorDeltaCalculator(),
            databaseScope: (_, _) => new List<string>(),
            separatelyMonitoredDatabases: _ => new List<string>(),
            installId: () => "0a1b2c3d");
        runner.LongQueryTraceListOverrideForTests = (_, _, _, _) => Task.FromResult(new List<string> { "master", "alpha", "beta" });
        runner.LongQueryTraceDatabaseOverrideForTests = (server, database, _, sessionName, _) =>
        {
            if (sessionName == LongQueryCompletionsCollector.LegacyXeSessionName)
            {
                onLegacyDrop(server.ServerId, database);
            }

            return Task.CompletedTask;
        };

        return (runner, new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime });
    }

    private static Task ReconcileAsync((DarlingCollectorRunner Runner, DarlingWorker.ServerLoopState State) install) =>
        DarlingWorker.ReconcileLongQueryTraceAsync(
            install.State, install.Runner, enabled: true, Array.Empty<LongQueryTraceRegistration>(), Array.Empty<string>(),
            DateTime.UtcNow, new DarlingSelfAlertTests.CapturingLogger(), CancellationToken.None);

    private static async Task DeleteServersAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "DELETE FROM collect.collector_state WHERE server_id IN ("
            + FirstServerId.ToString(CultureInfo.InvariantCulture) + ", " + SecondServerId.ToString(CultureInfo.InvariantCulture) + ")", connection);
        command.CommandTimeout = 30;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "DELETE FROM collect.collector_state WHERE server_id = " + LiveServerId.ToString(CultureInfo.InvariantCulture), connection);
        command.CommandTimeout = 30;
        await command.ExecuteNonQueryAsync(ct);
    }
}
