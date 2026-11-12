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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A service stop during an Azure SQL Database per-database read reaches the host as the PROVIDER's exception: SqlClient
/// reports a cancelled command as a SqlException. Runs the host's real per-database loop on one database whose read cancels
/// the run's own token and then throws that SqlException. The loop must surface a cancellation. Counted as a failed
/// database, the stop would be the loop's only failure and the last database would rethrow it as the SqlException, which
/// RunOneAsync records as an ERROR run. Needs the store because the loop opens a pooled store connection before its first
/// database.
/// </summary>
[Collection("live-postgres")]
public sealed class StopDuringAzureDatabaseReadLiveTests
{
    private const int LiveServerId = -490079;

    [Fact]
    public async Task AStopDuringAnAzureDatabaseRead_SurfacesACancellation_NotAFailedDatabase()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Azure stop test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: the run reads and writes only under a distinctive fake server id, and the cleanup removes
           whatever the run saved for it. */
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        var bodySucceeded = false;
        try
        {
            using var stop = new CancellationTokenSource();
            runner.TargetProviderOverrideForTests = _ => new StopDuringReadIsCancellationRunnerTests.FakeTargetProvider(stop);
            runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha" });

            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "t", Host = "h" },
                ConnectionString = "Server=azure;Database=master",
                Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
                StorageName = "h",
                ServerId = LiveServerId,
            };

            var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                runner.RunAsync(DeadlocksCollector.Instance, server, stop.Token));

            /* The provider's exception travels as the cause, so the cancellation carries the provider's message. */
            Assert.IsType<SqlException>(thrown.InnerException);

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
        var id = LiveServerId.ToString(CultureInfo.InvariantCulture);
        foreach (var sql in new[]
        {
            $"DELETE FROM collect.collector_state WHERE server_id = {id}",
            $"DELETE FROM deadlocks WHERE server_id = {id}",
        })
        {
            using var command = new NpgsqlCommand(sql, connection);
            command.CommandTimeout = 30;
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
