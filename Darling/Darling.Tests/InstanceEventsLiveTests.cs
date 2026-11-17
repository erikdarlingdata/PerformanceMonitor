/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #8884 on a live PostgreSQL store: the Darling alert adapter reads the identity pair and the memory dump stamp that the collectors persisted in
/// <c>collector_state</c>, through the shared SQL and the shared judges. Planted the way the carriers write. The first observation, a failover and a
/// count drop come back as nothing. Runs on its own scratch store.
/// </summary>
public sealed class InstanceEventsLiveTests
{
    private const string ServerName = "darling-instance-events-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheAdapter_ReadsTheSeededRestartAndDumpRise_AndNothingForAFailoverOrAFirstObservation()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live instance events test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var adapter = new DarlingAlertReadAdapter(dataSource);
        var key = ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var observed = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-3));
            var oldStart = new DateTime(2026, 11, 1, 6, 0, 0);
            var newStart = new DateTime(2026, 11, 16, 11, 58, 0);

            /* Nothing stored: nothing found. */
            var none = await adapter.GetInstanceEventsAsync(key, ct);
            Assert.Null(none.Restart);
            Assert.Null(none.MemoryDumps);

            /* The first observation: a current pair and no previous. */
            await PlantAsync(connection, ct, "wait_stats", ServerEpoch.IdentityStateKey, ServerEpoch.Serialize(new(oldStart, "NODE1")), observed);
            Assert.Null((await adapter.GetInstanceEventsAsync(key, ct)).Restart);

            /* A failover: the name moved with the start time. */
            await PlantAsync(connection, ct, "wait_stats", ServerEpoch.IdentityPreviousStateKey, ServerEpoch.Serialize(new(oldStart, "NODE1")), observed);
            await PlantAsync(connection, ct, "wait_stats", ServerEpoch.IdentityStateKey, ServerEpoch.Serialize(new(newStart, "NODE2")), observed);
            Assert.Null((await adapter.GetInstanceEventsAsync(key, ct)).Restart);

            /* A restart: the start time moved later, the name did not. */
            await PlantAsync(connection, ct, "wait_stats", ServerEpoch.IdentityStateKey, ServerEpoch.Serialize(new(newStart, "NODE1")), observed);
            var restart = (await adapter.GetInstanceEventsAsync(key, ct)).Restart;
            Assert.NotNull(restart);
            Assert.Equal(oldStart, restart!.PreviousStartTime);
            Assert.Equal(newStart, restart.NewStartTime);
            Assert.Equal(observed, restart.ObservedAtUtc);

            /* A dump rise, then a drop with no newer dump. */
            var newest = observed.AddMinutes(-1);
            await PlantAsync(connection, ct, "cpu_utilization", InstanceEvents.MemoryDumpsPreviousStateKey, InstanceEvents.SerializeDumps(new(observed.AddDays(-2), 1, 0)), observed);
            await PlantAsync(connection, ct, "cpu_utilization", InstanceEvents.MemoryDumpsStateKey, InstanceEvents.SerializeDumps(new(newest, 4, 3)), observed);
            var dumps = (await adapter.GetInstanceEventsAsync(key, ct)).MemoryDumps;
            Assert.NotNull(dumps);
            Assert.Equal(4, dumps!.TotalCount);
            Assert.Equal(3, dumps.RecentCount);
            Assert.Equal(3, dumps.NewCount);
            Assert.Equal(newest, dumps.NewestDumpUtc);

            /* The rise above never fired. The collector's next pass sees fewer dumps and no newer one: it stages the stamp it replaced as the previous one
               (the drop is the latest change), so what the store holds then is a pair that is not a rise. Run the collector's own observation to get it. */
            var context = new CollectorContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                CollectionTime = observed,
                Deltas = new CollectorDeltaCalculator(),
                Target = new CollectorTargetInfo(),
                State = new Dictionary<string, string> { [InstanceEvents.MemoryDumpsStateKey] = InstanceEvents.SerializeDumps(new(newest, 4, 3)) },
            };
            Assert.False(InstanceEvents.ObserveMemoryDumps(context, new InstanceEvents.MemoryDumpStamp(newest, 2, 0)));
            foreach (var staged in context.PendingState)
            {
                await PlantAsync(connection, ct, "cpu_utilization", staged.Key, staged.Value, observed);
            }

            Assert.Null((await adapter.GetInstanceEventsAsync(key, ct)).MemoryDumps);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static Task PlantAsync(NpgsqlConnection connection, CancellationToken ct, string collector, string key, string value, DateTime updatedAt) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (server_id, collector_name, state_key) DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at",
            ServerId, collector, key, value, DarlingMcpTestData.Naive(updatedAt));
}
