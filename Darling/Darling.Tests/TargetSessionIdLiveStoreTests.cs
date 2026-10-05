/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The live end-to-end half of the target session id work (#5132): a real collector run against a real SQL
/// Server records a positive <c>target_session_id</c> in a scratch store. Skips without DARLING_TEST_SQL and
/// DARLING_TEST_PG, as CI does.
/// </summary>
public sealed class TargetSessionIdLiveStoreTests
{
    [Fact]
    public async Task Live_AFullCollectorRun_RecordsANonNullTargetSessionId_InTheStore()
    {
        var pg = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(pg) || string.IsNullOrEmpty(host),
            "Set DARLING_TEST_PG and DARLING_TEST_SQL to run the live session id E2E.");
        var ct = TestContext.Current.CancellationToken;

        var scratch = await ScratchPostgres.CreateAsync(pg!, ct);
        var bodySucceeded = false;
        try
        {
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using (var migrate = await dataSource.OpenConnectionAsync(ct))
            {
                await PgMigrations.MigrateAsync(migrate, ct);
            }

            var runtime = await DarlingServerConnector.ConnectAsync(TargetSessionIdCacheTests.LiveServer(host!), null, ct);
            var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());
            var result = await runner.RunAsync(WaitStatsCollector.Instance, runtime, ct);

            Assert.NotNull(result.Drain);
            Assert.True(result.Drain!.Value.TargetSessionId > 0, "the run must carry the target's session id");

            await DarlingObservability.LogCollectionAsync(
                dataSource, runtime, "wait_stats", "SUCCESS", result.Rows, result.SqlMs, result.StorageMs, null,
                fanout: null, phases: null, drain: result.Drain, fetchPhases: null, sweepPeerMaxMs: null, null, ct);

            await using var verify = await dataSource.OpenConnectionAsync(ct);
            using var read = new NpgsqlCommand(
                "SELECT target_session_id FROM collection_log WHERE server_id = $1 AND collector_name = 'wait_stats' ORDER BY collection_time DESC LIMIT 1", verify);
            read.Parameters.AddWithValue(runtime.ServerId);
            var stored = await read.ExecuteScalarAsync(ct);
            Assert.IsType<int>(stored);
            Assert.True((int)stored! > 0);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
