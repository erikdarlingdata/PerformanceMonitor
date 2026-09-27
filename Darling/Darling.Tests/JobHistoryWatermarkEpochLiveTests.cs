/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live pins for job_history's numeric watermark (#4487): the store must answer with the NEWEST
/// collected batch's own maximum <c>instance_id</c>, not an all-time maximum across every batch ever
/// stored, and the failback timestamp bound the steady-state query builds must come from the store's own
/// resolved values, not a literal.
///
/// <para><b>Why an all-time MAX is wrong.</b> A target whose <c>msdb.dbo.sysjobhistory</c> identity was
/// reseeded (a purge, a restore, a failover to a lower-identity replica) starts a NEW epoch at a LOWER
/// <c>instance_id</c> than the OLD epoch the store already holds. An all-time MAX keeps returning the old
/// epoch's higher number forever, so every steady-state read after the reseed compares the target's
/// genuinely new rows against a watermark they can never exceed and collects nothing. The fix scopes the
/// numeric watermark to the newest already-collected batch's own <c>collection_time</c>, on both the
/// bounded (<see cref="WatermarkPolicy.RecentWatermarkWindow"/>) and unbounded fallback query text, so a
/// quiet server that has not collected in a while still reports the newest batch it DID collect, never an
/// older, larger epoch's number.</para>
///
/// <para><b>#1776 own-store:</b> every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class JobHistoryWatermarkEpochLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly MethodInfo ResolveMethod = typeof(DarlingCollectorRunner)
        .GetMethod("ResolveServerWatermarkAsync", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)!;

    private static ServerRuntime MakeServer(int serverId, string name) => new()
    {
        Config = new MonitoredServer { Name = name, Host = name },
        ConnectionString = "Server=" + name,
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = name,
        ServerId = serverId,
        EngineEdition = 3,
    };

    private static CollectorContext MakeContext(ServerRuntime server, DateTime collectionTime) => new()
    {
        ServerId = server.ServerId,
        ServerName = server.StorageName,
        CollectionTime = collectionTime,
        Deltas = new CollectorDeltaCalculator(),
        Target = server.Target,
    };

    private static async Task<(DateTime? Watermark, bool WatermarkFromUtcColumn, long? NumericWatermark, long ServerWatermarkMs, bool WatermarkCacheEligible, bool ServerWatermarkDiscarded)> ResolveAsync<TRow>(
        DarlingCollectorRunner runner, ServerRuntime server, ICollectorDefinition<TRow> definition, CancellationToken ct)
    {
        var probe = MakeContext(server, DateTime.UtcNow);
        var task = (Task)ResolveMethod.MakeGenericMethod(typeof(TRow)).Invoke(
            runner, new object?[] { server, definition, probe, null, ct })!;
        await task;
        var resultProperty = task.GetType().GetProperty("Result")!;
        return ((DateTime?, bool, long?, long, bool, bool))resultProperty.GetValue(task)!;
    }

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    /// <summary>
    /// Plants one job_history batch: every row shares <paramref name="collectionTime"/> (the batch's
    /// own collection cycle) and <paramref name="serverId"/>; <paramref name="instanceIds"/> gives each
    /// row's <c>instance_id</c> and <paramref name="runDateTime"/> its <c>run_datetime</c>. A direct
    /// insert against <c>collect.job_history</c>, not <c>WriteBatchAsync</c> — this test needs each
    /// batch's <c>collection_time</c> set to a chosen point in the past, which the runner's own write
    /// path always stamps as "now".
    /// </summary>
    private static async Task PlantBatchAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime collectionTime,
        DateTime runDateTime, IEnumerable<long> instanceIds, CancellationToken ct)
    {
        foreach (var instanceId in instanceIds)
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO collect.job_history " +
                "(job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, " +
                "run_status, run_datetime) " +
                "VALUES (@job_history_id, @collection_time, @server_id, @server_name, @instance_id, @job_id, @job_name, " +
                "@run_status, @run_datetime)", connection);
            insert.Parameters.AddWithValue("job_history_id", instanceId);
            insert.Parameters.AddWithValue("collection_time", DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue("server_id", serverId);
            insert.Parameters.AddWithValue("server_name", serverName);
            insert.Parameters.AddWithValue("instance_id", instanceId);
            insert.Parameters.AddWithValue("job_id", Guid.NewGuid().ToString());
            insert.Parameters.AddWithValue("job_name", "j1");
            insert.Parameters.AddWithValue("run_status", 1);
            insert.Parameters.AddWithValue("run_datetime", DateTime.SpecifyKind(runDateTime, DateTimeKind.Unspecified));
            await insert.ExecuteNonQueryAsync(ct);
        }
    }

    private static IEnumerable<long> Range(long startInclusive, long endInclusive)
    {
        for (var i = startInclusive; i <= endInclusive; i++)
        {
            yield return i;
        }
    }

    /// <summary>
    /// #4487 (a): a quiet server's numeric watermark is the NEWEST collected batch's own maximum
    /// <c>instance_id</c>, not an all-time maximum across every batch ever stored — proven with the
    /// new batch landing OUTSIDE the 6h bounded probe (the unbounded fallback text) and, separately,
    /// inside it (the bounded text), both against
    /// <see cref="DarlingCollectorRunner.GetLastCollectedInstanceIdAsync"/> directly, the product's own
    /// entry point for this read.
    ///
    /// <para>RED on dev: <c>GetLastCollectedInstanceIdAsync</c> exists there, but its SQL
    /// (<c>BuildServerWatermarkInstanceIdSql</c>) is a plain <c>SELECT MAX(instance_id) WHERE server_id = $1</c>
    /// with no scoping to the newest batch's own <c>collection_time</c> — an all-time maximum. It returns
    /// the OLD epoch's 10,200,040, not the new epoch's 9,545,030, on dev. A runtime mismatch, not a
    /// compile failure: the method and its parameters are unchanged.</para>
    /// </summary>
    [Fact]
    public async Task GetLastCollectedInstanceIdAsync_ReturnsTheNewestBatchsMax_NotTheAllTimeMax()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the #4487 numeric-watermark epoch pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var server = MakeServer(-419750, "wm-epoch-a-fallback");

        var bodySucceeded = false;
        try
        {
            var oldEpochTime = DateTime.UtcNow.AddDays(-3);
            var newEpochTime = DateTime.UtcNow.AddDays(-1); // outside the 6h bounded probe: unbounded fallback text
            await PlantBatchAsync(
                connection, server.ServerId, server.StorageName, oldEpochTime, oldEpochTime,
                Range(10_200_001, 10_200_040), ct);
            await PlantBatchAsync(
                connection, server.ServerId, server.StorageName, newEpochTime, newEpochTime,
                Range(9_545_001, 9_545_030), ct);

            var result = await runner.GetLastCollectedInstanceIdAsync(server.ServerId, "job_history", "instance_id", ct);

            Assert.Equal(9_545_030L, result);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var delete = new NpgsqlCommand("DELETE FROM job_history WHERE server_id = @id", cleanup);
                delete.Parameters.AddWithValue("id", server.ServerId);
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>
    /// The same fact, but with BOTH the old epoch's batch and the newest batch inside
    /// <see cref="WatermarkPolicy.RecentWatermarkWindow"/>, so the BOUNDED probe text answers directly and
    /// the unbounded fallback is never reached. The old batch stays inside the window on purpose: a bound
    /// that only filters on <c>collection_time &gt; now - 6h</c> with no further scoping to the newest
    /// batch's own <c>collection_time</c> would still see both batches and take the higher (old-epoch)
    /// <c>instance_id</c> — this is the case that actually exercises the newest-batch scoping on the
    /// bounded path, since an old batch already outside the window would be excluded by the plain bound
    /// alone and prove nothing.
    /// </summary>
    [Fact]
    public async Task GetLastCollectedInstanceIdAsync_ReturnsTheNewestBatchsMax_OnTheBoundedPath()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the #4487 numeric-watermark epoch pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var server = MakeServer(-419751, "wm-epoch-a-bounded");

        var bodySucceeded = false;
        try
        {
            var oldEpochTime = DateTime.UtcNow.AddHours(-4); // inside the 6h bounded window too
            var newEpochTime = DateTime.UtcNow.AddHours(-1); // inside the 6h bounded probe window
            await PlantBatchAsync(
                connection, server.ServerId, server.StorageName, oldEpochTime, oldEpochTime,
                Range(10_200_001, 10_200_040), ct);
            await PlantBatchAsync(
                connection, server.ServerId, server.StorageName, newEpochTime, newEpochTime,
                Range(9_545_001, 9_545_030), ct);

            var result = await runner.GetLastCollectedInstanceIdAsync(server.ServerId, "job_history", "instance_id", ct);

            Assert.Equal(9_545_030L, result);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var delete = new NpgsqlCommand("DELETE FROM job_history WHERE server_id = @id", cleanup);
                delete.Parameters.AddWithValue("id", server.ServerId);
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>
    /// #4487 (c): the failback bound the steady-state query builds must come from the STORE's own
    /// resolved watermarks, not a literal the test supplies — batch A (the older epoch) is planted first,
    /// then batch B (the newer epoch); <see cref="DarlingCollectorRunner.ResolveServerWatermarkAsync{TRow}"/>
    /// (the product's own resolve step, not a manual stand-in) must resolve <c>NumericWatermark</c> to B's
    /// max <c>instance_id</c> and <c>Watermark</c> to B's newest <c>run_datetime</c> (the PLAIN unscoped
    /// max — <see cref="DarlingCollectorRunner.GetLastCollectedTimeAsync"/> — so A's newer run_datetime,
    /// if it had one, would not win; it does not, here, on purpose). Building
    /// <see cref="JobHistoryCollector"/>'s query from a context carrying those two resolved values must then
    /// bind <c>@last_instance_id</c> = 12 and <c>@min_run_datetime</c> = B's newest run_datetime minus
    /// <see cref="JobHistoryCollector.FailbackLookbackDays"/> days.
    ///
    /// <para>RED on dev: this is a compile-level failure, not a runtime one. Dev's <c>JobHistoryCollector</c>
    /// has no <c>@min_run_datetime</c> parameter and no <c>FailbackLookbackDays</c> constant at all — the
    /// failback bound is entirely new in this branch — so a build against <c>origin/dev</c> with this test
    /// file fails to compile on those two references.</para>
    /// </summary>
    [Fact]
    public async Task ResolvedWatermarks_FeedTheFailbackQueryBound_FromTheStoredValues()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the #4487 failback-bound pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var server = MakeServer(-419752, "wm-epoch-c-failback");
        var definition = JobHistoryCollector.Instance;

        var bodySucceeded = false;
        try
        {
            // Batch A: the older epoch, planted first.
            var batchARunDateTime = new DateTime(2026, 9, 10, 0, 0, 0, DateTimeKind.Unspecified);
            var batchACollectionTime = DateTime.UtcNow.AddDays(-2);
            await PlantBatchAsync(
                connection, server.ServerId, server.StorageName, batchACollectionTime, batchARunDateTime,
                Range(500, 520), ct);

            // Batch B: the newer epoch, planted second — its run_datetime spans 09-20 through 09-21, its
            // newest row (12) landing last so the newest instance_id and the newest run_datetime agree.
            var batchBNewestRunDateTime = new DateTime(2026, 9, 21, 0, 0, 0, DateTimeKind.Unspecified);
            var batchBCollectionTime = DateTime.UtcNow.AddHours(-2);
            await using (var earlier = new NpgsqlCommand(
                "INSERT INTO collect.job_history " +
                "(job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, run_status, run_datetime) " +
                "VALUES (@id, @ct, @sid, @sname, @iid, @jid, @jname, @rs, @rdt)", connection))
            {
                earlier.Parameters.AddWithValue("id", 1L);
                earlier.Parameters.AddWithValue("ct", DateTime.SpecifyKind(batchBCollectionTime, DateTimeKind.Unspecified));
                earlier.Parameters.AddWithValue("sid", server.ServerId);
                earlier.Parameters.AddWithValue("sname", server.StorageName);
                earlier.Parameters.AddWithValue("iid", 1L);
                earlier.Parameters.AddWithValue("jid", Guid.NewGuid().ToString());
                earlier.Parameters.AddWithValue("jname", "j1");
                earlier.Parameters.AddWithValue("rs", 1);
                earlier.Parameters.AddWithValue("rdt", new DateTime(2026, 9, 20, 0, 0, 0, DateTimeKind.Unspecified));
                await earlier.ExecuteNonQueryAsync(ct);
            }
            for (long instanceId = 2; instanceId <= 12; instanceId++)
            {
                await using var insert = new NpgsqlCommand(
                    "INSERT INTO collect.job_history " +
                    "(job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, run_status, run_datetime) " +
                    "VALUES (@id, @ct, @sid, @sname, @iid, @jid, @jname, @rs, @rdt)", connection);
                insert.Parameters.AddWithValue("id", instanceId);
                insert.Parameters.AddWithValue("ct", DateTime.SpecifyKind(batchBCollectionTime, DateTimeKind.Unspecified));
                insert.Parameters.AddWithValue("sid", server.ServerId);
                insert.Parameters.AddWithValue("sname", server.StorageName);
                insert.Parameters.AddWithValue("iid", instanceId);
                insert.Parameters.AddWithValue("jid", Guid.NewGuid().ToString());
                insert.Parameters.AddWithValue("jname", "j1");
                insert.Parameters.AddWithValue("rs", 1);
                insert.Parameters.AddWithValue(
                    "rdt",
                    instanceId == 12
                        ? DateTime.SpecifyKind(batchBNewestRunDateTime, DateTimeKind.Unspecified)
                        : DateTime.SpecifyKind(batchBNewestRunDateTime.AddHours(-instanceId), DateTimeKind.Unspecified));
                await insert.ExecuteNonQueryAsync(ct);
            }

            var (resolvedWatermark, _, resolvedNumeric, _, eligible, _) = await ResolveAsync(runner, server, definition, ct);
            Assert.True(eligible, "job_history declares a WatermarkValueAccessor and must be cache-eligible");
            Assert.Equal(12L, resolvedNumeric);
            Assert.Equal(DateTime.SpecifyKind(batchBNewestRunDateTime, DateTimeKind.Unspecified), resolvedWatermark);

            var context = new CollectorContext
            {
                ServerId = server.ServerId,
                ServerName = server.StorageName,
                CollectionTime = DateTime.UtcNow,
                Deltas = new CollectorDeltaCalculator(),
                Target = server.Target,
                NumericWatermark = resolvedNumeric,
                Watermark = resolvedWatermark,
                HasCollectedBefore = true,
            };

            var query = definition.BuildQuery(context);

            var idParam = Assert.Single(query.Parameters, p => p.Name == "@last_instance_id");
            Assert.Equal(12L, idParam.Value);

            var boundParam = Assert.Single(query.Parameters, p => p.Name == "@min_run_datetime");
            Assert.Equal(batchBNewestRunDateTime.AddDays(-JobHistoryCollector.FailbackLookbackDays), boundParam.Value);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var delete = new NpgsqlCommand("DELETE FROM job_history WHERE server_id = @id", cleanup);
                delete.Parameters.AddWithValue("id", server.ServerId);
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
