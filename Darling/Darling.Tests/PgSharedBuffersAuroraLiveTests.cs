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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8322, through the real fact collector and scorer over a seeded store: an Aurora PostgreSQL target whose
/// <c>shared_buffers</c> moved between 128 MB and 256 MB twice in 48 hours (the engine sizing it from the current capacity)
/// roots no <c>CONFIG_PG_SHARED_BUFFERS</c> finding, while a self-managed server at 128 MB still does.
/// </summary>
[Collection("live-postgres")]
public sealed class PgSharedBuffersAuroraLiveTests
{
    private const string AuroraName = "darling-pg-shared-buffers-aurora";
    private static readonly int AuroraId = ServerIdHelper.GetDeterministicHashCode(AuroraName);
    private const string SelfManagedName = "darling-pg-shared-buffers-self-managed";
    private static readonly int SelfManagedId = ServerIdHelper.GetDeterministicHashCode(SelfManagedName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AnAuroraTarget_WhoseSharedBuffersMovedWithCapacity_RootsNoInitdbFinding_AndASelfManagedOneStillDoes()
    {
        var (aurora, selfManaged) = await WithStoreAsync(async (connection, postgres, ct) =>
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, AuroraId, AuroraName, MonitoredEngineKind.AuroraPostgres, 16, ct);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, SelfManagedId, SelfManagedName, MonitoredEngineKind.Postgres, 16, ct);

            var end = DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified).AddMinutes(-1);
            var start = end.AddHours(-4);

            foreach (var (id, name) in new[] { (AuroraId, AuroraName), (SelfManagedId, SelfManagedName) })
            {
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, id, name, end.AddHours(-25), ct);
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, id, name, start, ct);
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, id, name, end, ct);
            }

            /* Aurora: 128 MB, then 256 MB, then 128 MB again inside 48 hours, every snapshot from the configuration file and
               none pending a restart: the engine moved it with the capacity. The newest snapshot is at the initdb figure. */
            await PlantSharedBuffersAsync(connection, AuroraId, AuroraName, end.AddHours(-40), "16384", ct);
            await PlantSharedBuffersAsync(connection, AuroraId, AuroraName, end.AddHours(-20), "32768", ct);
            await PlantSharedBuffersAsync(connection, AuroraId, AuroraName, end.AddMinutes(-30), "16384", ct);

            /* Self-managed: the initdb line, untouched. */
            await PlantSharedBuffersAsync(connection, SelfManagedId, SelfManagedName, end.AddMinutes(-30), "16384", ct);

            var auroraFacts = await CollectAndScoreAsync(postgres, AuroraId, AuroraName, start, end, ct);
            var selfManagedFacts = await CollectAndScoreAsync(postgres, SelfManagedId, SelfManagedName, start, end, ct);
            return (auroraFacts, selfManagedFacts);
        });

        var auroraKnob = Assert.Single(aurora, f => f.Key == PgTargetFactKeys.ConfigSharedBuffers);
        Assert.Equal(128, auroraKnob.Value, precision: 6);
        Assert.Equal(0.0, auroraKnob.BaseSeverity);
        Assert.Equal(0.0, auroraKnob.Severity);
        Assert.True(auroraKnob.Metadata.GetValueOrDefault("not_applicable") > 0, "the knob says why it is not graded");

        var selfManagedKnob = Assert.Single(selfManaged, f => f.Key == PgTargetFactKeys.ConfigSharedBuffers);
        Assert.Equal(128, selfManagedKnob.Value, precision: 6);
        Assert.Equal(PgTargetScorer.ConfigAdvisoryBase, selfManagedKnob.BaseSeverity, precision: 6);
        Assert.False(selfManagedKnob.Metadata.ContainsKey("not_applicable"));
    }

    private static async Task<List<Fact>> CollectAndScoreAsync(NpgsqlDataSource postgres, int serverId, string serverName, DateTime start, DateTime end, CancellationToken ct)
    {
        var context = new AnalysisContext
        {
            ServerId = serverId, ServerName = serverName, TimeRangeStart = start, TimeRangeEnd = end,
            ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct
        };
        var facts = await new PgTargetFactCollector(postgres).CollectFactsAsync(context);
        new FactScorer().ScoreAll(facts);
        return facts;
    }

    private static async Task PlantSharedBuffersAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, string blocks, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype, source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, 'shared_buffers', $5, '8kB', 'Resource Usage / Memory', 'postmaster', 'integer', 'configuration file', '16384', $5, NULL, NULL, false, NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(blocks);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task<T>> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var result = await body(connection, postgres, ct);
            bodySucceeded = true;
            return result;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_server_config WHERE server_id IN ({AuroraId}, {SelfManagedId}); " +
            $"DELETE FROM pg_database_stats WHERE server_id IN ({AuroraId}, {SelfManagedId}); " +
            $"DELETE FROM servers WHERE server_id IN ({AuroraId}, {SelfManagedId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
