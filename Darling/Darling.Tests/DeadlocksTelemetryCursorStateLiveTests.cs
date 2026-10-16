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
/// The deadlock telemetry cursor through the host's REAL state path: land what the definition staged, save it
/// with <see cref="DarlingCollectorRunner.SaveCollectorStateAsync"/>, reload it with
/// <see cref="DarlingCollectorRunner.GetCollectorStateAsync"/>, and check the next <c>BuildQuery</c> binds it.
/// A mocked store cannot show a key that is written under one name and read under another.
/// </summary>
[Collection("live-postgres")]
public sealed class DeadlocksTelemetryCursorStateLiveTests
{
    private const int LiveServerId = -490001;
    private static readonly DateTime Now = new(2026, 8, 26, 12, 5, 0, DateTimeKind.Utc);

    private static CollectorContext Ctx(IReadOnlyDictionary<string, string>? state = null, bool azure = true) => new()
    {
        ServerId = LiveServerId,
        ServerName = "s",
        CollectionTime = Now,
        Deltas = null!,
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
        CurrentDatabaseName = azure ? "master" : null,
        State = state ?? CollectorContext.NoState,
    };

    [Fact]
    public async Task LandedCursor_SurvivesTheStoreRoundTrip_AndTheNextQueryBindsIt()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live telemetry cursor state test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: the rows are keyed by a distinctive fake server id and removed in the cleanup. */
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var name = DeadlocksCollector.Instance.Name;

        var bodySucceeded = false;
        try
        {
            var cursor = new DateTime(2026, 8, 26, 12, 0, 20, DateTimeKind.Utc);
            var ctx = Ctx();
            ctx.StagedItemState[DeadlocksCollector.TelemetryCursorStateKey] = cursor.ToString("o", CultureInfo.InvariantCulture);

            /* Staged only: nothing reaches the store until the host lands it after the item's write. */
            await runner.SaveCollectorStateAsync(LiveServerId, name, ctx.PendingState, ct);
            Assert.Empty(await runner.GetCollectorStateAsync(LiveServerId, name, ct));

            ctx.LandStagedItemState();
            await runner.SaveCollectorStateAsync(LiveServerId, name, ctx.PendingState, ct);

            var reloaded = await runner.GetCollectorStateAsync(LiveServerId, name, ct);
            Assert.Equal(cursor.ToString("o", CultureInfo.InvariantCulture), reloaded[DeadlocksCollector.TelemetryCursorStateKey]);
            Assert.Contains(DeadlocksCollector.TelemetryCursorStateKey, DeadlocksCollector.Instance.StateKeys);

            var next = DeadlocksCollector.Instance.BuildQuery(Ctx(reloaded));
            Assert.Equal(cursor.AddMinutes(-10), next.Parameters.First(p => p.Name == "@telemetry_cutoff_time").Value);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task LandedRingCursor_SurvivesTheStoreRoundTrip_AndTheNextQueryBindsItMinusTheOverlap()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live ring cursor state test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: the rows are keyed by a distinctive fake server id and removed in the cleanup. */
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var name = DeadlocksCollector.Instance.Name;

        var bodySucceeded = false;
        try
        {
            var cursor = new DateTime(2026, 8, 26, 12, 0, 30, DateTimeKind.Utc);
            var key = DeadlocksCollector.RingCursorKey("zeta");
            var ctx = Ctx();
            ctx.StagedItemState[key] = cursor.ToString("o", CultureInfo.InvariantCulture);

            await runner.SaveCollectorStateAsync(LiveServerId, name, ctx.PendingState, ct);
            Assert.Empty(await runner.GetCollectorStateAsync(LiveServerId, name, ct));

            ctx.LandStagedItemState();
            await runner.SaveCollectorStateAsync(LiveServerId, name, ctx.PendingState, ct);

            var reloaded = await runner.GetCollectorStateAsync(LiveServerId, name, ct);
            Assert.Equal(cursor.ToString("o", CultureInfo.InvariantCulture), reloaded[key]);

            var zeta = new CollectorContext
            {
                ServerId = LiveServerId,
                ServerName = "s",
                CollectionTime = Now,
                Deltas = null!,
                Target = new CollectorTargetInfo { IsAzureSqlDb = true },
                CurrentDatabaseName = "zeta",
                State = reloaded,
            };
            var next = DeadlocksCollector.Instance.BuildQuery(zeta);
            Assert.Equal(cursor.AddMinutes(-10), next.Parameters.First(p => p.Name == "@cutoff_time").Value);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The SQL Server and Managed Instance text is the same text, and the telemetry arm (Azure SQL Database only) is not in it.
    /// A hash of the whole text stood here until #5948 changed the ring-buffer read; the rule that matters is the equality.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void OnPremAndManagedInstance_QueryText_IsByteIdentical(bool managedInstance)
    {
        CollectorContext Build(bool managed) => new()
        {
            ServerId = Ctx(azure: false).ServerId,
            ServerName = Ctx(azure: false).ServerName,
            CollectionTime = Now,
            Deltas = null!,
            Target = new CollectorTargetInfo { IsAzureSqlDb = false, IsAzureManagedInstance = managed },
            Watermark = null,
        };

        var onPrem = DeadlocksCollector.Instance.BuildQuery(Build(managed: false));
        var query = DeadlocksCollector.Instance.BuildQuery(Build(managedInstance));

        Assert.False(query.Text.Contains("@telemetry_cutoff_time", StringComparison.Ordinal));
        Assert.Equal(onPrem.Text, query.Text);
        Assert.Equal(onPrem.Parameters.Select(p => p.Name), query.Parameters.Select(p => p.Name));
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            $"DELETE FROM collect.collector_state WHERE server_id = {LiveServerId.ToString(CultureInfo.InvariantCulture)}", connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
