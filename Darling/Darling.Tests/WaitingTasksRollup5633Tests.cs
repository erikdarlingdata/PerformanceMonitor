/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 item 18: <c>get_waiting_tasks</c> returned one row per waiting TASK, so a parallel query's exchange waits looked like thirty
/// duplicates of one session, 29 of them with <c>blocking_session_id</c> equal to the session itself (session 97, CXCONSUMER, blocked by 97).
/// The read now rolls the threads of one session and wait type up within a capture: <c>thread_count</c> says how many, and a blocker equal
/// to the session is flagged <c>intra_query_parallelism</c> and never reported as a blocker.
/// </summary>
[Collection("live-postgres")]
public sealed class WaitingTasksRollup5633Tests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void WaitingTasksSql_RollsUpPerSessionAndWaitType_AndNeverReportsSelfBlocking()
    {
        var sql = DarlingSessionReader.WaitingTasksSql;
        Assert.Contains("GROUP BY collection_time, session_id, wait_type", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS thread_count", sql, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE blocking_session_id <> session_id)", sql, StringComparison.Ordinal);
        Assert.Contains("BOOL_OR(blocking_session_id = session_id)", sql, StringComparison.Ordinal);
        /* #5633 review L-a: no independent MAX of the resource or the blocker; both come from the longest-waiting thread. */
        Assert.DoesNotContain("MAX(resource_description)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(blocking_session_id)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ParallelQueryThreads_AreOneRow_WithAThreadCount_AndNoSelfBlocker()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the waiting-tasks rollup live test.");
        var ct = TestContext.Current.CancellationToken;

        var serverName = "wt5633_" + Guid.NewGuid().ToString("N");
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var capture = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-5), DateTimeKind.Unspecified);
            await using (var seed = new NpgsqlConnection(cs))
            {
                await seed.OpenAsync(ct);
                await PgMigrations.MigrateAsync(seed, ct);
                await DarlingMcpTestData.RegisterServerAsync(seed, serverId, serverName, ct);

                /* Session 97: a parallel query, thirty exchange-wait threads, 29 of them waiting on 97 itself (the audit's shape) and
                   one on nothing. Session 55: one lock wait blocked by another session. */
                for (var thread = 0; thread < 30; thread++)
                {
                    await InsertAsync(seed, serverId, serverName, capture, 97, "CXCONSUMER", 100 + thread, thread < 29 ? 97 : (int?)null, ct);
                }

                await InsertAsync(seed, serverId, serverName, capture, 55, "LCK_M_S", 5000, 60, ct);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpSessionTools.GetWaitingTasks(postgres, serverName, 1, 30, cancellationToken: ct));
            var tasks = doc.RootElement.GetProperty("tasks").EnumerateArray().ToList();
            Assert.Equal(2, tasks.Count);

            var parallel = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 97);
            Assert.Equal(30, parallel.GetProperty("thread_count").GetInt32());
            Assert.True(parallel.GetProperty("intra_query_parallelism").GetBoolean());
            Assert.Equal(JsonValueKind.Null, parallel.GetProperty("blocking_session_id").ValueKind);
            Assert.Equal(129, parallel.GetProperty("wait_duration_ms").GetInt64());

            var locked = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 55);
            Assert.Equal(1, locked.GetProperty("thread_count").GetInt32());
            Assert.False(locked.GetProperty("intra_query_parallelism").GetBoolean());
            Assert.Equal(60, locked.GetProperty("blocking_session_id").GetInt32());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM collect.waiting_tasks WHERE server_id = $1", serverId);
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM collect.servers WHERE server_id = $1", serverId);
            });
        }
    }

    /// <summary>#5633 review L-a: threads of one session that DISAGREE. The row is the longest-waiting thread's wait, resource and blocker
    /// (it used to be three independent MAXes), and every other distinct blocker is listed in blocking_session_ids.</summary>
    [Fact]
    public async Task DisagreeingThreads_TakeResourceAndBlockerFromTheLongestWaitingThread()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the waiting-tasks rollup live test.");
        var ct = TestContext.Current.CancellationToken;

        var serverName = "wt5633b_" + Guid.NewGuid().ToString("N");
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var capture = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-5), DateTimeKind.Unspecified);
            await using (var seed = new NpgsqlConnection(cs))
            {
                await seed.OpenAsync(ct);
                await PgMigrations.MigrateAsync(seed, ct);
                await DarlingMcpTestData.RegisterServerAsync(seed, serverId, serverName, ct);

                await InsertAsync(seed, serverId, serverName, capture, 70, "LCK_M_S", 100, 40, ct, "zzz-shortest-wait");
                await InsertAsync(seed, serverId, serverName, capture, 70, "LCK_M_S", 900, 20, ct, "aaa-longest-wait");
                await InsertAsync(seed, serverId, serverName, capture, 70, "LCK_M_S", 500, 30, ct, "mmm-middle");
                /* Session 71: the longest thread waits on its own session (parallelism); a shorter one is blocked by 12. */
                await InsertAsync(seed, serverId, serverName, capture, 71, "CXPACKET", 800, 71, ct, "res-self");
                await InsertAsync(seed, serverId, serverName, capture, 71, "CXPACKET", 300, 12, ct, "res-external");
            }

            using var doc = JsonDocument.Parse(await DarlingMcpSessionTools.GetWaitingTasks(postgres, serverName, 1, 30, cancellationToken: ct));
            var tasks = doc.RootElement.GetProperty("tasks").EnumerateArray().ToList();

            var disagree = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 70);
            Assert.Equal(900, disagree.GetProperty("wait_duration_ms").GetInt64());
            Assert.Equal("aaa-longest-wait", disagree.GetProperty("resource_description").GetString());
            Assert.Equal(20, disagree.GetProperty("blocking_session_id").GetInt32());
            Assert.Equal(new[] { 20, 30, 40 }, disagree.GetProperty("blocking_session_ids").EnumerateArray().Select(e => e.GetInt32()).ToArray());

            var selfFirst = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 71);
            Assert.Equal(800, selfFirst.GetProperty("wait_duration_ms").GetInt64());
            Assert.Equal("res-self", selfFirst.GetProperty("resource_description").GetString());
            Assert.True(selfFirst.GetProperty("intra_query_parallelism").GetBoolean());
            Assert.Equal(12, selfFirst.GetProperty("blocking_session_id").GetInt32());
            Assert.Equal(JsonValueKind.Null, selfFirst.GetProperty("blocking_session_ids").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM collect.waiting_tasks WHERE server_id = $1", serverId);
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM collect.servers WHERE server_id = $1", serverId);
            });
        }
    }

    /// <summary>Review round 2 Low-2: equal waits and NULL waits. Lite's twin (<c>WaitingBlockedDatabaseFilterToolTests.WaitingTasks_TiedAndNullWaitThreads_...</c>)
    /// seeds the same threads and asserts the same rows, so the two stores' rollups stay the same row.</summary>
    [Fact]
    public async Task TiedAndNullWaitThreads_PickTheSameRowAsLite()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the waiting-tasks rollup live test.");
        var ct = TestContext.Current.CancellationToken;

        var serverName = "wt5633c_" + Guid.NewGuid().ToString("N");
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var capture = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-5), DateTimeKind.Unspecified);
            await using (var seed = new NpgsqlConnection(cs))
            {
                await seed.OpenAsync(ct);
                await PgMigrations.MigrateAsync(seed, ct);
                await DarlingMcpTestData.RegisterServerAsync(seed, serverId, serverName, ct);

                /* Session 80: the longest thread is self-blocked, two others tie at 300 ms (the higher blocker is stored first): the lowest blocker wins. */
                await InsertAsync(seed, serverId, serverName, capture, 80, "CXPACKET", 800, 80, ct, "res-self");
                await InsertAsync(seed, serverId, serverName, capture, 80, "CXPACKET", 300, 50, ct, "res-50");
                await InsertAsync(seed, serverId, serverName, capture, 80, "CXPACKET", 300, 40, ct, "res-40");
                /* Session 81: the longest thread is self-blocked and the only other-blocked thread has a NULL wait: it still supplies the blocker. */
                await InsertAsync(seed, serverId, serverName, capture, 81, "CXPACKET", 800, 81, ct, "res-self");
                await InsertAsync(seed, serverId, serverName, capture, 81, "CXPACKET", null, 70, ct, "res-null-wait");
                /* Session 82: equal waits, one self-blocked: the lowest other blocker wins, and the resource follows the lowest raw blocker. */
                await InsertAsync(seed, serverId, serverName, capture, 82, "LCK_M_S", 500, 33, ct, "res-33");
                await InsertAsync(seed, serverId, serverName, capture, 82, "LCK_M_S", 500, 22, ct, "res-22");
                await InsertAsync(seed, serverId, serverName, capture, 82, "LCK_M_S", 500, 82, ct, "res-self");
            }

            using var doc = JsonDocument.Parse(await DarlingMcpSessionTools.GetWaitingTasks(postgres, serverName, 1, 30, cancellationToken: ct));
            var tasks = doc.RootElement.GetProperty("tasks").EnumerateArray().ToList();

            var tied = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 80);
            Assert.Equal(800, tied.GetProperty("wait_duration_ms").GetInt64());
            Assert.Equal("res-self", tied.GetProperty("resource_description").GetString());
            Assert.Equal(40, tied.GetProperty("blocking_session_id").GetInt32());
            Assert.Equal(new[] { 40, 50 }, tied.GetProperty("blocking_session_ids").EnumerateArray().Select(e => e.GetInt32()).ToArray());

            var nullWait = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 81);
            Assert.Equal(800, nullWait.GetProperty("wait_duration_ms").GetInt64());
            Assert.Equal("res-self", nullWait.GetProperty("resource_description").GetString());
            Assert.True(nullWait.GetProperty("intra_query_parallelism").GetBoolean());
            Assert.Equal(70, nullWait.GetProperty("blocking_session_id").GetInt32());
            Assert.Equal(JsonValueKind.Null, nullWait.GetProperty("blocking_session_ids").ValueKind);

            var equal = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 82);
            Assert.Equal(500, equal.GetProperty("wait_duration_ms").GetInt64());
            Assert.Equal("res-22", equal.GetProperty("resource_description").GetString());
            Assert.Equal(22, equal.GetProperty("blocking_session_id").GetInt32());
            Assert.Equal(new[] { 22, 33 }, equal.GetProperty("blocking_session_ids").EnumerateArray().Select(e => e.GetInt32()).ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM collect.waiting_tasks WHERE server_id = $1", serverId);
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM collect.servers WHERE server_id = $1", serverId);
            });
        }
    }

    private static Task InsertAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime capture, int session, string waitType,
        long? waitMs, int? blocker, CancellationToken ct, string? resource = null) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            CollectionIdGenerator.Next(), capture, serverId, serverName, session, waitType, waitMs, blocker, resource, "db");
}
