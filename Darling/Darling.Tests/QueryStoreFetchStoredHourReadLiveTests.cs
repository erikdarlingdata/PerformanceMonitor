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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fact mints its own scratch store, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6211: the per-run read of the hour's stored rows (<c>DarlingCollectorRunner.ReadStoredHourRowsAsync</c>) answers from
/// <c>collect.query_store_interval_wide</c> when its coverage claim reaches the batch's oldest interval, and from raw
/// <c>query_store_stats</c> when it does not, and both routes hand the ranking the same rows: the newest snapshot of each
/// (plan, interval, execution type, replica). Needs <c>DARLING_TEST_PG</c>; skips without it.
/// </summary>
public sealed class QueryStoreFetchStoredHourReadLiveTests
{
    private const int ServerId = 62110;
    private const string Database = "hourdb";

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        foreach (var value in values) command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static List<(long Plan, long Interval, long Executions, long Cpu)> Shape(List<QueryStoreCollector.Row> rows) =>
        rows.Select(r => (r.PlanId, r.RuntimeStatsIntervalId!.Value, r.ExecutionCount, r.AvgCpuTimeUs)).OrderBy(r => r.PlanId).ThenBy(r => r.Item2).ToList();

    [Fact]
    public async Task TheHourRead_AnswersFromTheWideTable_WhenItsClaimReaches_AndFromRawWhenItDoesNot_WithTheSameRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), "Set DARLING_TEST_PG to run the stored-hour read live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "hourhost", ct);

            var start = LiveClock.AnchoredEnd().AddHours(-3);
            var startNaive = DarlingMcpTestData.Naive(start);

            /* Plan 7 has two snapshots of the one interval (the second is the later, larger cumulative one); plan 8 has one. Another interval's row
               (999) and another database's row are in the store too and must not come back. */
            const string insertRaw = @"INSERT INTO query_store_stats
(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, runtime_stats_interval_id,
 interval_start_time_utc, first_execution_time, last_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, avg_physical_io_reads,
 avg_logical_io_writes, max_query_max_used_memory, max_tempdb_space_used, is_forced_plan)
VALUES ($1,$2,$3,'hourhost',$4,$5,$6,'Regular',$7,$8,$8,$9,$10,$11,5,1,1,1,1,FALSE)";
            var snapshots = new (long Id, int Minute, string Db, long Query, long Plan, long Interval, long Executions, long Cpu)[]
            {
                (1, 10, Database, 70, 7, 500, 10, 100), (2, 25, Database, 70, 7, 500, 30, 100), (3, 25, Database, 80, 8, 500, 5, 400),
                (4, 25, Database, 90, 9, 999, 50, 100), (5, 25, "otherdb", 70, 7, 500, 99, 100),
            };
            foreach (var s in snapshots)
            {
                await ExecAsync(connection, ct, insertRaw, s.Id, DarlingMcpTestData.Naive(start.AddMinutes(s.Minute)), ServerId, s.Db, s.Query, s.Plan, s.Interval,
                    startNaive, DarlingMcpTestData.Naive(start.AddMinutes(s.Minute)), s.Executions, s.Cpu);
            }

            /* The wide table holds the newest snapshot per identity, which is what the writer's apply leaves. */
            await ExecAsync(connection, ct, @"INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, execution_count, avg_cpu_time_us,
 avg_duration_us, avg_physical_io_reads, avg_logical_io_writes, max_query_max_used_memory, max_tempdb_space_used, runtime_stats_interval_id, interval_start_time_utc)
SELECT DISTINCT ON (database_name, plan_id, runtime_stats_interval_id) collection_time, server_id, database_name, query_id, plan_id, execution_type_desc,
       first_execution_time, last_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, avg_physical_io_reads, avg_logical_io_writes,
       max_query_max_used_memory, max_tempdb_space_used, runtime_stats_interval_id, interval_start_time_utc
FROM query_store_stats WHERE server_id = $1
ORDER BY database_name, plan_id, runtime_stats_interval_id, collection_time DESC", ServerId);

            var batch = new List<QueryStoreCollector.Row>
            {
                new() { DatabaseName = Database, PlanId = 7, QueryId = 70, RuntimeStatsIntervalId = 500, IntervalStartTimeUtc = DarlingMcpTestData.Naive(start) },
            };

            /* No coverage row: the wide table cannot answer, so raw does. */
            var (raw, rawUsedWide) = await DarlingCollectorRunner.ReadStoredHourRowsAsync(connection, ServerId, Database, start.AddMinutes(40), batch, ct);
            Assert.False(rawUsedWide, "with no coverage claim the wide table cannot answer");

            /* A claim from before the interval began: the wide table answers. */
            await ExecAsync(connection, ct, "INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES ($1, $2, $2)",
                ServerId, DarlingMcpTestData.Naive(start.AddHours(-1)));
            var (wide, wideUsedWide) = await DarlingCollectorRunner.ReadStoredHourRowsAsync(connection, ServerId, Database, start.AddMinutes(40), batch, ct);
            Assert.True(wideUsedWide, "a claim older than the batch's interval lets the wide table answer");

            Assert.Equal(Shape(raw), Shape(wide));
            Assert.Equal(new[] { (7L, 500L, 30L, 100L), (8L, 500L, 5L, 400L) }, Shape(wide).ToArray());

            /* A claim that starts after the interval began does not cover it: raw answers again. */
            await ExecAsync(connection, ct, "UPDATE collect.query_store_interval_wide_coverage SET filled_since = $2 WHERE server_id = $1", ServerId, DarlingMcpTestData.Naive(start.AddMinutes(20)));
            var (late, lateUsedWide) = await DarlingCollectorRunner.ReadStoredHourRowsAsync(connection, ServerId, Database, start.AddMinutes(40), batch, ct);
            Assert.False(lateUsedWide);
            Assert.Equal(Shape(raw), Shape(late));

            /* A batch failure the wide table has not replayed (a pending row) is a gap too. */
            await ExecAsync(connection, ct, "UPDATE collect.query_store_interval_wide_coverage SET filled_since = $2 WHERE server_id = $1", ServerId, DarlingMcpTestData.Naive(start.AddHours(-1)));
            await ExecAsync(connection, ct, "INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at, failure) VALUES ($1, $2, $3, $2, 'x')",
                ServerId, DarlingMcpTestData.Naive(start.AddMinutes(25)), Database);
            var (pending, pendingUsedWide) = await DarlingCollectorRunner.ReadStoredHourRowsAsync(connection, ServerId, Database, start.AddMinutes(40), batch, ct);
            Assert.False(pendingUsedWide);
            Assert.Equal(Shape(raw), Shape(pending));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
