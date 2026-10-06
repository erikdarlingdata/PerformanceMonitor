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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5306: the viewer's Query Store history read (<see cref="ViewerDataService.GetQueryStoreHistoryAsync"/>), run on
/// PostgreSQL, with the two identity columns it ends with (<c>runtime_stats_interval_id</c> and
/// <c>replica_role</c>, ordinals 50 and 51) and the Total Executions the history window shows over its rows
/// (<see cref="ViewerQueryStoreHistoryRow.TotalExecutions"/>).
///
/// <para><see cref="ViewerQueryStoreHistoryTotalExecutionsTests"/> pins the helper's arithmetic on built-up rows and the
/// read's text; neither one runs the read. A wrong type or ordinal against real rows would pass them, and so would a
/// disagreement between the C# helper and PostgreSQL's <c>ROW_NUMBER</c> on real NULLs and ties. This class seeds
/// <c>query_store_stats</c> and reads it back through the viewer, then reads the SAME seed through the viewer's Query
/// Store top read, whose SQL is the aggregate the helper's key and tie-break were copied from. The two counts have to
/// agree on every seed here. The window is two hours, under <c>QueryStoreIntervalWide.GridWideMinWindow</c>, so the top read
/// always takes its raw path, the <c>QueryStoreTopSql</c> dedupe over <c>query_store_stats</c>.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerQueryStoreHistoryTotalExecutionsLiveTests
{
    private const int ServerId = -953061;
    private const string ServerName = "ViewerQsHistoryTotalSrv";
    private const string Db = "HistoryTotalDb";

    /* Fixed anchors, never the wall clock: whole seconds, so nothing depends on how the store rounds a tick. */
    private static readonly DateTime T0 = new(2026, 3, 1, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = T0.AddHours(-1);
    private static readonly DateTime WindowEnd = T0.AddHours(1);
    private static readonly DateTime FirstExecOne = T0.AddMinutes(1);
    private static readonly DateTime FirstExecTwo = T0.AddMinutes(2);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// One interval (9301) collected three times, 10 then 25 then 40, and a second interval (9302) collected once, 5.
    /// The grid lists all four snapshots and its counts add up to 80, which is what the window's summary showed before
    /// #5306. The window now shows 45: 40 (interval one's last snapshot) + 5 (interval two's only one).
    /// </summary>
    [Fact]
    public async Task OneIntervalCollectedThreeTimes_PlusAnotherOnce_ListsFourRows_AndTotals45_AgainstDevPostgres()
    {
        await RunAsync(async (connection, viewer, ct) =>
        {
            const long QueryId = 1;
            foreach (var (minute, executions) in new[] { (5, 10L), (10, 25L), (15, 40L) })
            {
                await SeedAsync(connection, QueryId, planId: 77, T0.AddMinutes(minute), executions,
                    intervalId: 9301, FirstExecOne, replicaRole: null, ct: ct);
            }

            await SeedAsync(connection, QueryId, planId: 77, T0.AddMinutes(20), executionCount: 5,
                intervalId: 9302, FirstExecTwo, replicaRole: null, ct: ct);

            var rows = await viewer.GetQueryStoreHistoryAsync(ServerId, Db, QueryId, WindowStart, WindowEnd, ct);

            /* The grid is unchanged: every snapshot, oldest first. */
            Assert.Equal(4, rows.Count);
            Assert.Equal(new long[] { 10, 25, 40, 5 }, rows.Select(r => r.ExecutionCount).ToArray());
            Assert.Equal(80, rows.Sum(r => r.ExecutionCount));
            Assert.Equal(new[] { T0.AddMinutes(5), T0.AddMinutes(10), T0.AddMinutes(15), T0.AddMinutes(20) },
                rows.Select(r => r.CollectionTime).ToArray());

            /* The rest of the interval identity is mapped from ordinals 50 and 51, and so are the columns the key
               already used. Every row here has a real id and no replica role. */
            Assert.Equal(new long?[] { 9301, 9301, 9301, 9302 }, rows.Select(r => r.RuntimeStatsIntervalId).ToArray());
            Assert.All(rows, r => Assert.Null(r.ReplicaRole));
            Assert.All(rows, r => Assert.Equal(77, r.PlanId));
            Assert.All(rows, r => Assert.Equal("Regular", r.ExecutionTypeDesc));
            Assert.Equal(new DateTime?[] { FirstExecOne, FirstExecOne, FirstExecOne, FirstExecTwo },
                rows.Select(r => r.FirstExecutionTime).ToArray());

            Assert.Equal(45, ViewerQueryStoreHistoryRow.TotalExecutions(rows));
            Assert.Equal(ViewerQueryStoreHistoryRow.TotalExecutions(rows), await TopReadTotalAsync(viewer, QueryId, ct));
        });
    }

    /// <summary>
    /// Rows collected before the identity columns existed: NULL <c>runtime_stats_interval_id</c>, NULL
    /// <c>replica_role</c>, and for the oldest of them NULL <c>first_execution_time</c> too. NULL must come back as null,
    /// not 0 or "". The key's NULL parts match each other, as <c>PARTITION BY</c> puts NULLs in one partition, so two
    /// snapshots with no identity at all are ONE interval and count at their latest, and the top read says the same.
    /// </summary>
    [Fact]
    public async Task LegacyRowsWithNoIdentity_ComeBackNull_AndCountAsOneInterval_AgainstDevPostgres()
    {
        await RunAsync(async (connection, viewer, ct) =>
        {
            const long NoIdentityQuery = 2;
            await SeedAsync(connection, NoIdentityQuery, planId: 78, T0.AddMinutes(5), executionCount: 4,
                intervalId: null, firstExecutionTime: null, replicaRole: null, ct: ct);
            await SeedAsync(connection, NoIdentityQuery, planId: 78, T0.AddMinutes(10), executionCount: 8,
                intervalId: null, firstExecutionTime: null, replicaRole: null, ct: ct);

            var rows = await viewer.GetQueryStoreHistoryAsync(ServerId, Db, NoIdentityQuery, WindowStart, WindowEnd, ct);

            Assert.Equal(new long[] { 4, 8 }, rows.Select(r => r.ExecutionCount).ToArray());
            Assert.All(rows, r => Assert.Null(r.RuntimeStatsIntervalId));
            Assert.All(rows, r => Assert.Null(r.FirstExecutionTime));
            Assert.All(rows, r => Assert.Null(r.ReplicaRole));

            /* One interval, its newest snapshot: 8, not 4 + 8. The same count the SQL's partition gives. */
            Assert.Equal(8, ViewerQueryStoreHistoryRow.TotalExecutions(rows));
            Assert.Equal(8, await TopReadTotalAsync(viewer, NoIdentityQuery, ct));

            /* A legacy row that kept its first_execution_time is told apart by that proxy: the id comes back null
               and the stamp comes back set, and two proxies are two intervals (12 + 7), in the helper and in SQL. */
            const long ProxyQuery = 3;
            await SeedAsync(connection, ProxyQuery, planId: 79, T0.AddMinutes(5), executionCount: 5,
                intervalId: null, FirstExecOne, replicaRole: null, ct: ct);
            await SeedAsync(connection, ProxyQuery, planId: 79, T0.AddMinutes(10), executionCount: 12,
                intervalId: null, FirstExecOne, replicaRole: null, ct: ct);
            await SeedAsync(connection, ProxyQuery, planId: 79, T0.AddMinutes(15), executionCount: 7,
                intervalId: null, FirstExecTwo, replicaRole: null, ct: ct);

            var proxyRows = await viewer.GetQueryStoreHistoryAsync(ServerId, Db, ProxyQuery, WindowStart, WindowEnd, ct);

            Assert.Equal(3, proxyRows.Count);
            Assert.All(proxyRows, r => Assert.Null(r.RuntimeStatsIntervalId));
            Assert.Equal(new DateTime?[] { FirstExecOne, FirstExecOne, FirstExecTwo },
                proxyRows.Select(r => r.FirstExecutionTime).ToArray());
            Assert.Equal(12 + 7, ViewerQueryStoreHistoryRow.TotalExecutions(proxyRows));
            Assert.Equal(12 + 7, await TopReadTotalAsync(viewer, ProxyQuery, ct));
        });
    }

    /// <summary>
    /// One interval stored for the primary (6 then 9) and for a readable secondary (2): the primary's latest and the
    /// secondary's only snapshot both count, the primary's first one does not. Roles come back as strings.
    /// </summary>
    [Fact]
    public async Task TheSameIntervalOnTwoReplicaRoles_CountsOncePerRole_AgainstDevPostgres()
    {
        await RunAsync(async (connection, viewer, ct) =>
        {
            const long QueryId = 4;
            await SeedAsync(connection, QueryId, planId: 88, T0.AddMinutes(5), executionCount: 6,
                intervalId: 9311, FirstExecOne, replicaRole: "PRIMARY", ct: ct);
            await SeedAsync(connection, QueryId, planId: 88, T0.AddMinutes(10), executionCount: 9,
                intervalId: 9311, FirstExecOne, replicaRole: "PRIMARY", ct: ct);
            await SeedAsync(connection, QueryId, planId: 88, T0.AddMinutes(10), executionCount: 2,
                intervalId: 9311, FirstExecOne, replicaRole: "SECONDARY", ct: ct);

            var rows = await viewer.GetQueryStoreHistoryAsync(ServerId, Db, QueryId, WindowStart, WindowEnd, ct);

            Assert.Equal(3, rows.Count);
            Assert.Equal(2, rows.Count(r => r.ReplicaRole == "PRIMARY"));
            Assert.Equal(1, rows.Count(r => r.ReplicaRole == "SECONDARY"));
            Assert.All(rows, r => Assert.Equal(9311, r.RuntimeStatsIntervalId));
            Assert.Equal(17, rows.Sum(r => r.ExecutionCount));
            Assert.Equal(9 + 2, ViewerQueryStoreHistoryRow.TotalExecutions(rows));

            /* The top read splits the query into one row per replica role, and the two rows add up to the window's
               total: the helper's replica_role key is the SQL's. */
            var top = await TopReadRowsAsync(viewer, QueryId, ct);
            Assert.Equal(new[] { "PRIMARY", "SECONDARY" }, top.Select(r => r.ReplicaRole).OrderBy(r => r, StringComparer.Ordinal).ToArray());
            Assert.Equal(9 + 2, top.Sum(r => r.TotalExecutions));
        });
    }

    /// <summary>
    /// Two snapshots of ONE interval stored at the SAME collection_time, counts 8 and 94: the in-memory sliver and the
    /// flushed slice Query Store handed back before #1907. The latest-snapshot rule keeps the larger count, in the
    /// helper and in SQL, whichever one was inserted first.
    /// </summary>
    [Fact]
    public async Task SnapshotsTiedOnCollectionTime_KeepTheLargerCount_InEitherInsertOrder_AgainstDevPostgres()
    {
        await RunAsync(async (connection, viewer, ct) =>
        {
            const long SliverFirst = 5;
            const long FlushedFirst = 6;
            var tied = T0.AddMinutes(10);

            await SeedAsync(connection, SliverFirst, planId: 99, tied, executionCount: 8,
                intervalId: 9321, FirstExecOne, replicaRole: null, ct: ct);
            await SeedAsync(connection, SliverFirst, planId: 99, tied, executionCount: 94,
                intervalId: 9321, FirstExecOne, replicaRole: null, ct: ct);

            await SeedAsync(connection, FlushedFirst, planId: 99, tied, executionCount: 94,
                intervalId: 9322, FirstExecOne, replicaRole: null, ct: ct);
            await SeedAsync(connection, FlushedFirst, planId: 99, tied, executionCount: 8,
                intervalId: 9322, FirstExecOne, replicaRole: null, ct: ct);

            foreach (var queryId in new[] { SliverFirst, FlushedFirst })
            {
                var rows = await viewer.GetQueryStoreHistoryAsync(ServerId, Db, queryId, WindowStart, WindowEnd, ct);

                /* The read orders on collection_time alone, so two rows that tie on it can come back either way. */
                Assert.Equal(new long[] { 8, 94 }, rows.Select(r => r.ExecutionCount).OrderBy(c => c).ToArray());
                Assert.Equal(94, ViewerQueryStoreHistoryRow.TotalExecutions(rows));
                Assert.Equal(94, await TopReadTotalAsync(viewer, queryId, ct));
            }
        });
    }

    /// <summary>
    /// The viewer's Query Store top read over the seed, for one query: the rows it returns, one per (plan, outcome,
    /// replica role). Raw path only: the window is under the interval table's minimum, which the null plan proves.
    /// </summary>
    private static async Task<List<ViewerQueryStoreRow>> TopReadRowsAsync(ViewerDataService viewer, long queryId, CancellationToken ct)
    {
        var (rows, plan) = await viewer.GetQueryStoreTopQueriesWithReachAsync(
            ServerId, WindowStart, WindowEnd, cancellationToken: ct);
        Assert.Null(plan);
        return rows.Where(r => r.DatabaseName == Db && r.QueryId == queryId).ToList();
    }

    private static async Task<long> TopReadTotalAsync(ViewerDataService viewer, long queryId, CancellationToken ct) =>
        (await TopReadRowsAsync(viewer, queryId, ct)).Sum(r => r.TotalExecutions);

    /// <summary>Migrates the shared store, clears this class's rows, runs the body, and clears them again.</summary>
    private static async Task RunAsync(Func<NpgsqlConnection, ViewerDataService, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live viewer Query Store history test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);
        await using var viewer = new ViewerDataService(cs!);

        var bodySucceeded = false;
        try
        {
            await body(connection, viewer, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteAsync);
        }
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM query_store_stats WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// One stored snapshot. A NULL <paramref name="intervalId"/>, NULL <paramref name="firstExecutionTime"/> or NULL
    /// <paramref name="replicaRole"/> is stored as NULL: those are the shapes of the rows collected before each column existed.
    /// </summary>
    private static async Task SeedAsync(
        NpgsqlConnection connection, long queryId, long planId, DateTime collectionTime, long executionCount,
        long? intervalId, DateTime? firstExecutionTime, string? replicaRole, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, first_execution_time, last_execution_time,
     query_text, query_hash, query_plan_hash, execution_count,
     avg_cpu_time_us, avg_duration_us, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, false, 0)";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue((object?)replicaRole ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)intervalId ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)firstExecutionTime ?? DBNull.Value);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue($"SELECT /*q{queryId}*/ 1");
        command.Parameters.AddWithValue($"0xQH{queryId}");
        command.Parameters.AddWithValue($"0xPH{planId}");
        command.Parameters.AddWithValue(executionCount);
        command.Parameters.AddWithValue(100L);
        command.Parameters.AddWithValue(1000L);
        await command.ExecuteNonQueryAsync(ct);
    }
}
