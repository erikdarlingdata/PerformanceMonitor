/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
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

    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static async Task<int> WriteBatchAsync<TRow>(
        DarlingCollectorRunner runner, NpgsqlConnection connection, ICollectorDefinition<TRow> definition, List<TRow> rows,
        ServerRuntime server, DateTime collectionTime, CollectorContext context, CancellationToken ct)
    {
        var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(TRow)).Invoke(
            runner, new object?[] { connection, definition, rows, server, collectionTime, context, ct })!;
        await task;
        return (int)task.GetType().GetProperty("Result")!.GetValue(task)!;
    }

    private static JobHistoryCollector.Row MakeRow(long instanceId, string jobId, int stepId, DateTime runDateTime) =>
        new() { InstanceId = instanceId, JobId = jobId, StepId = stepId, RunDateTime = runDateTime, JobName = "j1", RunStatus = 1 };

    private static async Task<List<(long InstanceId, string JobId, int StepId, DateTime RunDateTime)>> DuplicateNaturalKeysAsync(
        NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT instance_id, job_id, step_id, run_datetime FROM collect.job_history " +
            "WHERE server_id = @id GROUP BY server_id, instance_id, job_id, step_id, run_datetime HAVING count(*) > 1",
            connection);
        command.Parameters.AddWithValue("id", serverId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<(long, string, int, DateTime)>();
        while (await reader.ReadAsync(ct))
        {
            result.Add((reader.GetInt64(0), reader.GetString(1), reader.GetInt32(2), reader.GetDateTime(3)));
        }

        return result;
    }

    private static async Task<long> RowCountAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT count(*) FROM collect.job_history WHERE server_id = @id", connection);
        command.Parameters.AddWithValue("id", serverId);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// #4487's headline case, through the product's own <c>WriteBatchAsync</c> write path (the exact
    /// method the collection cycle calls, not a manual stand-in for the dedupe read): a target collects
    /// epoch A (instance_id 500..520), fails over to a new, lower-identity epoch B (1..12), then fails
    /// BACK to epoch A — the failback batch honestly re-reads A's own already-stored rows (500..520)
    /// alongside genuinely new rows under the returning epoch (521..530). No row may be duplicated on
    /// (server_id, instance_id, job_id, step_id, run_datetime); A2 is stored exactly once; and the total
    /// row count is A1 (21) + B (12) + A2 (10) = 43, never 21 + 12 + 21 + 10.
    ///
    /// <para>RED on both <c>origin/dev</c> and <c>dea387916</c>: neither commit's <c>WriteBatchAsync</c>
    /// calls a pre-insert dedupe read at all — job_history's only defense there is the numeric watermark,
    /// which the failback batch's re-delivered A1 rows do not trip (their instance_id 500..520 sits BELOW
    /// the epoch-B-scoped watermark's own numbers are not compared against on a plain re-COPY), so all 21
    /// of A1's rows land a second time. Expected total 43; dev and <c>dea387916</c> both actually store 64
    /// (43 + the 21 duplicated A1 rows), and the HAVING-count query returns 21 duplicated tuples instead
    /// of 0.</para>
    /// </summary>
    [Fact]
    public async Task WriteBatchAsync_FailbackToAPriorEpoch_StoresZeroDuplicateNaturalKeys()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the #4487 A-to-B-to-A dedupe pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        var runner = new DarlingCollectorRunner(NpgsqlDataSource.Create(scratch.ConnectionString), new CollectorDeltaCalculator());
        var server = MakeServer(-419760, "jh-dedupe-abfailback");
        var definition = JobHistoryCollector.Instance;

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;

            /* A1: epoch A, instance_id 500..520 (21 rows), run_datetime spread D-3d .. D-1d, collected
               3 days ago. */
            var a1RunBase = now.AddDays(-3);
            var a1Rows = new List<JobHistoryCollector.Row>();
            long instanceId = 500;
            for (var d = 0; d <= 2; d++)
            {
                for (var r = 0; r < 7; r++)
                {
                    a1Rows.Add(MakeRow(instanceId++, "job-a", 0, a1RunBase.AddDays(d).AddMinutes(r)));
                }
            }
            Assert.Equal(21, a1Rows.Count);
            var a1Context = MakeContext(server, now.AddDays(-3));
            var writtenA1 = await WriteBatchAsync(runner, connection, definition, a1Rows, server, a1Context.CollectionTime, a1Context, ct);
            Assert.Equal(21, writtenA1);

            /* B: a new, lower-identity epoch (1..12, 12 rows), run_datetime D, collected 2 hours ago. */
            var bRows = new List<JobHistoryCollector.Row>();
            for (long i = 1; i <= 12; i++)
            {
                bRows.Add(MakeRow(i, "job-b", 0, now.AddHours(-2).AddMinutes((int)i)));
            }
            var bContext = MakeContext(server, now.AddHours(-2));
            var writtenB = await WriteBatchAsync(runner, connection, definition, bRows, server, bContext.CollectionTime, bContext, ct);
            Assert.Equal(12, writtenB);

            /* Failback: A1's 21 rows re-delivered VERBATIM (same natural key) plus A2 (521..530, 10 new
               rows), collected now. */
            var failbackRows = new List<JobHistoryCollector.Row>();
            foreach (var row in a1Rows)
            {
                failbackRows.Add(MakeRow(row.InstanceId, row.JobId, row.StepId, row.RunDateTime!.Value));
            }
            long a2InstanceId = 521;
            var a2RunBase = a1RunBase.AddDays(3).AddHours(1);
            for (var i = 0; i < 10; i++)
            {
                failbackRows.Add(MakeRow(a2InstanceId++, "job-a", 0, a2RunBase.AddMinutes(i)));
            }
            Assert.Equal(31, failbackRows.Count);
            var failbackContext = MakeContext(server, now);
            var writtenFailback = await WriteBatchAsync(runner, connection, definition, failbackRows, server, failbackContext.CollectionTime, failbackContext, ct);

            /* Only A2's 10 genuinely new rows should be written; A1's 21 re-delivered rows are dropped. */
            Assert.Equal(10, writtenFailback);

            var duplicates = await DuplicateNaturalKeysAsync(connection, server.ServerId, ct);
            Assert.Empty(duplicates);

            var total = await RowCountAsync(connection, server.ServerId, ct);
            Assert.Equal(43L, total); // 21 (A1) + 12 (B) + 10 (A2)

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
    /// #4487, the long-running-step case: a failback batch that includes a row whose <c>run_datetime</c>
    /// is 5 days in the past (a step that started well before the batch's own collection cycle and only
    /// just finished) is stored once on its first delivery, then dropped entirely when the SAME batch is
    /// re-delivered (the honest re-read case the dedupe exists for).
    ///
    /// <para>RED on both <c>origin/dev</c> and <c>dea387916</c>: same reason as the A→B→A pin — neither
    /// commit's write path drops an already-stored natural key, so the second delivery re-inserts the row
    /// and the total goes to 2 instead of staying at 1.</para>
    /// </summary>
    [Fact]
    public async Task WriteBatchAsync_ALongRunningStep_IsStoredOnce_AndDroppedOnReplay()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the #4487 long-running-step pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        var runner = new DarlingCollectorRunner(NpgsqlDataSource.Create(scratch.ConnectionString), new CollectorDeltaCalculator());
        var server = MakeServer(-419761, "jh-dedupe-longrunning");
        var definition = JobHistoryCollector.Instance;

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var longRunningRow = MakeRow(900, "job-long", 0, now.AddDays(-5));
            var failbackBatch = new List<JobHistoryCollector.Row> { longRunningRow };

            var context1 = MakeContext(server, now);
            var written1 = await WriteBatchAsync(runner, connection, definition, failbackBatch, server, context1.CollectionTime, context1, ct);
            Assert.Equal(1, written1);

            /* The SAME batch again — an honest re-read of the same, still-current epoch. */
            var replayBatch = new List<JobHistoryCollector.Row>
            {
                MakeRow(longRunningRow.InstanceId, longRunningRow.JobId, longRunningRow.StepId, longRunningRow.RunDateTime!.Value),
            };
            var context2 = MakeContext(server, now.AddMinutes(1));
            var written2 = await WriteBatchAsync(runner, connection, definition, replayBatch, server, context2.CollectionTime, context2, ct);
            Assert.Equal(0, written2);

            var total = await RowCountAsync(connection, server.ServerId, ct);
            Assert.Equal(1L, total);

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
    /// #4487, the full-tuple case: a row sharing a stored row's <c>run_datetime</c> AND <c>instance_id</c>
    /// but a DIFFERENT <c>job_id</c> is stored (two jobs can legitimately race to the same identity value
    /// under different epochs' own numbering), and likewise a row differing only in <c>step_id</c> (the
    /// job-outcome step_id-0 row and a real step row of the same run share everything else). Both prove
    /// the dedupe keys on the FULL four-column tuple, not any subset.
    ///
    /// <para>RED on both <c>origin/dev</c> and <c>dea387916</c>: there is no dedupe read at all there, so
    /// this passes on those commits too by construction (nothing to drop). Listed here as a companion to
    /// the two REDs above, not a RED itself — the branch's own pure pins
    /// (<see cref="JobHistoryNaturalKeyDedupeTests"/>) carry the compile-level RED for the contract
    /// itself.</para>
    /// </summary>
    [Fact]
    public async Task WriteBatchAsync_ASameKeyRowWithADifferentJobIdOrStepId_IsStored()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the #4487 full-tuple pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        var runner = new DarlingCollectorRunner(NpgsqlDataSource.Create(scratch.ConnectionString), new CollectorDeltaCalculator());
        var server = MakeServer(-419762, "jh-dedupe-fulltuple");
        var definition = JobHistoryCollector.Instance;

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var runDateTime = now.AddHours(-1);
            var storedRow = MakeRow(700, "job-x", 0, runDateTime);
            var context1 = MakeContext(server, now.AddHours(-1));
            var written1 = await WriteBatchAsync(
                runner, connection, definition, new List<JobHistoryCollector.Row> { storedRow }, server, context1.CollectionTime, context1, ct);
            Assert.Equal(1, written1);

            /* Same instance_id + run_datetime, different job_id. */
            var differentJobId = MakeRow(700, "job-y", 0, runDateTime);
            /* Same instance_id + run_datetime + job_id, different step_id. */
            var differentStepId = MakeRow(700, "job-x", 1, runDateTime);
            var batch2 = new List<JobHistoryCollector.Row> { differentJobId, differentStepId };
            var context2 = MakeContext(server, now);
            var written2 = await WriteBatchAsync(runner, connection, definition, batch2, server, context2.CollectionTime, context2, ct);

            Assert.Equal(2, written2);

            var total = await RowCountAsync(connection, server.ServerId, ct);
            Assert.Equal(3L, total);

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
    /// The cost of the pre-insert dedupe read (#4487): a 50-row steady-state batch's dedupe SQL, EXPLAIN
    /// (ANALYZE, BUFFERS)'d with literal arrays copied verbatim from
    /// <c>DarlingCollectorRunner.DropAlreadyStoredJobHistoryRowsAsync</c>, against 30 daily chunks of
    /// job_history for one server (~2,000 rows/day) with every chunk older than 1 day compressed. Reports
    /// the chunks scanned (chunk exclusion via the <c>collection_time &gt;= floor</c> bound), buffers, and
    /// timing as diagnostics in the test output — the PR body carries the measured numbers; this pin does
    /// not assert on them, only that the query plan excludes the chunks outside the bound. Also reports
    /// the same measures for a failback batch spanning 7 days (<see cref="JobHistoryCollector.FailbackLookbackDays"/>),
    /// the batch shape with the widest <c>collection_time</c> floor this dedupe read is designed for.
    /// </summary>
    [Fact]
    public async Task DropAlreadyStoredJobHistoryRowsAsync_Cost_OverThirtyDailyChunks()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the #4487 dedupe-read cost check.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.SkipWhen(!timescaleEnabled, "TimescaleDB is not available on this rig; the cost check needs real chunk exclusion.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var server = MakeServer(-419763, "jh-dedupe-cost");

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;

            /* 30 daily chunks, ~2,000 rows/day, one server. */
            for (var daysAgo = 29; daysAgo >= 0; daysAgo--)
            {
                var day = now.AddDays(-daysAgo);
                await PlantDailyRowsAsync(connection, server.ServerId, server.StorageName, day, 2_000, ct);
            }

            /* Compress every chunk older than 1 day. */
            using (var chunkList = new NpgsqlCommand(
                "SELECT show_chunks('collect.job_history', older_than => INTERVAL '1 day')::text", connection))
            await using (var reader = await chunkList.ExecuteReaderAsync(ct))
            {
                var chunks = new List<string>();
                while (await reader.ReadAsync(ct))
                {
                    chunks.Add(reader.GetString(0));
                }

                await reader.DisposeAsync();

                using var alterCompress = new NpgsqlCommand(
                    "ALTER TABLE collect.job_history SET (timescaledb.compress, " +
                    "timescaledb.compress_segmentby = 'server_id', timescaledb.compress_orderby = 'run_datetime DESC, instance_id DESC')",
                    connection);
                await alterCompress.ExecuteNonQueryAsync(ct);

                foreach (var chunk in chunks)
                {
                    using var compress = new NpgsqlCommand($"SELECT compress_chunk('{chunk}', if_not_compressed => true)", connection);
                    await compress.ExecuteNonQueryAsync(ct);
                }
            }

            /* Steady-state batch: 50 rows in the last hour. */
            var steadyRunDateTimes = new DateTime[50];
            var steadyInstanceIds = new long[50];
            var steadyJobIds = new string[50];
            var steadyStepIds = new int[50];
            for (var i = 0; i < 50; i++)
            {
                steadyRunDateTimes[i] = DateTime.SpecifyKind(now.AddMinutes(-i), DateTimeKind.Unspecified);
                steadyInstanceIds[i] = 10_000_000 + i;
                steadyJobIds[i] = "steady-job";
                steadyStepIds[i] = 0;
            }

            var steadyPlan = await ExplainDedupeAsync(
                connection, server.ServerId, steadyRunDateTimes, steadyInstanceIds, steadyJobIds, steadyStepIds, ct);
            Console.Error.WriteLine("DIAG job_history dedupe cost (steady-state, 50 rows, last hour):");
            Console.Error.WriteLine(steadyPlan);

            /* Failback batch: rows spanning 7 days (FailbackLookbackDays), the widest collection_time floor
               this read is designed for. */
            var failbackCount = 50;
            var failbackRunDateTimes = new DateTime[failbackCount];
            var failbackInstanceIds = new long[failbackCount];
            var failbackJobIds = new string[failbackCount];
            var failbackStepIds = new int[failbackCount];
            for (var i = 0; i < failbackCount; i++)
            {
                failbackRunDateTimes[i] = DateTime.SpecifyKind(
                    now.AddDays(-(double)i * JobHistoryCollector.FailbackLookbackDays / failbackCount), DateTimeKind.Unspecified);
                failbackInstanceIds[i] = 20_000_000 + i;
                failbackJobIds[i] = "failback-job";
                failbackStepIds[i] = 0;
            }

            var failbackPlan = await ExplainDedupeAsync(
                connection, server.ServerId, failbackRunDateTimes, failbackInstanceIds, failbackJobIds, failbackStepIds, ct);
            Console.Error.WriteLine("DIAG job_history dedupe cost (failback, 50 rows, spanning 7 days):");
            Console.Error.WriteLine(failbackPlan);

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

    private static async Task PlantDailyRowsAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime day, int rowCount, CancellationToken ct)
    {
        await using var writer = await connection.BeginBinaryImportAsync(
            "COPY collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, " +
            "job_id, job_name, run_status, run_datetime) FROM STDIN (FORMAT BINARY)", ct);

        var baseInstanceId = day.Ticks % 1_000_000_000L;
        for (var i = 0; i < rowCount; i++)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(baseInstanceId * 10_000 + i, NpgsqlDbType.Bigint, ct);
            await writer.WriteAsync(DateTime.SpecifyKind(day, DateTimeKind.Unspecified), NpgsqlDbType.Timestamp, ct);
            await writer.WriteAsync(serverId, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync(serverName, NpgsqlDbType.Text, ct);
            await writer.WriteAsync(baseInstanceId * 10_000 + i, NpgsqlDbType.Bigint, ct);
            await writer.WriteAsync("job-" + (i % 20), NpgsqlDbType.Text, ct);
            await writer.WriteAsync("j1", NpgsqlDbType.Text, ct);
            await writer.WriteAsync(1, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync(DateTime.SpecifyKind(day.AddMinutes(i), DateTimeKind.Unspecified), NpgsqlDbType.Timestamp, ct);
        }

        await writer.CompleteAsync(ct);
    }

    /// <summary>
    /// EXPLAIN (ANALYZE, BUFFERS)'s the exact dedupe SQL <c>DropAlreadyStoredJobHistoryRowsAsync</c> runs,
    /// with the same unnest-joined array parameters, bound the same way (a min run_datetime minus one day
    /// as the collection_time floor).
    /// </summary>
    private static async Task<string> ExplainDedupeAsync(
        NpgsqlConnection connection, int serverId, DateTime[] runDateTimes, long[] instanceIds, string[] jobIds, int[] stepIds,
        CancellationToken ct)
    {
        var minRunDateTime = runDateTimes.Min();
        var collectionTimeFloor = DateTime.SpecifyKind(minRunDateTime.AddDays(-1), DateTimeKind.Unspecified);

        await using var command = new NpgsqlCommand(
            "EXPLAIN (ANALYZE, BUFFERS) " +
            "SELECT jh.instance_id, jh.job_id, jh.step_id, jh.run_datetime " +
            "FROM collect.job_history AS jh " +
            "JOIN unnest($2::timestamp[], $3::bigint[], $4::text[], $5::integer[]) " +
            "AS k(run_datetime, instance_id, job_id, step_id) " +
            "ON jh.run_datetime = k.run_datetime AND jh.instance_id = k.instance_id " +
            "AND jh.job_id = k.job_id AND jh.step_id = k.step_id " +
            "WHERE jh.server_id = $1 AND jh.collection_time >= $6",
            connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = runDateTimes });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = instanceIds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = jobIds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = stepIds });
        command.Parameters.AddWithValue(collectionTimeFloor);

        var plan = new StringBuilder();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }
}
