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
using NpgsqlTypes;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6804, live: the Viewer's Top Queries read names each query's module from <c>procedure_stats</c>. The module lookup ranked
/// EVERY procedure_stats row the server retains (no time bound, no handle filter: 2.9 million rows in, 2,302 out on a large store,
/// 10 to 12 s of a 13 s read), so the read's cost grew with retention. It now looks up only the handles of the ranked groups.
///
/// <para>Two properties, both observed on real rows: (1) the answer is unchanged, in particular a handle whose newest
/// procedure_stats row is far older than the window still names its module (a bound on the window start would turn it into
/// 'ad hoc'), and (2) the handle groups the module lookup keeps are the ranked handles' only, read from the plan's own row counts (#8549: the lookup is a hash
/// aggregate now, not a sort, so the count is the aggregate's groups).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerTopQueriesModuleLiveTests
{
    private const int ServerId = -970840;
    private const string ServerName = "viewer-top-queries-module-e2e";
    private const int UnrelatedHandles = 400;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ModuleLookup_NamesAnOldHandle_AndKeepsOnlyTheRankedHandles_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live Top Queries module test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PerformanceMonitor.Darling.Storage.PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        /* #5608: the window ends at the anchored point and spans four hours, so it sits inside one raw chunk at any hour the test runs. */
        var end = LiveClock.AnchoredEnd();
        var start = end.AddHours(-4);
        var succeeded = false;
        try
        {
            /* Two ranked queries. 0xOLD's module row was collected three days BEFORE the window; 0xNEW's two rows are inside it
               (the newer name must win); 0xLOOSE matches no module at all. */
            await InsertQueryAsync(connection, ct, start.AddHours(1), "0xQOLD", "0xHOLD", elapsed: 900_000);
            await InsertQueryAsync(connection, ct, start.AddHours(1), "0xQNEW", "0xHNEW", elapsed: 800_000);
            await InsertQueryAsync(connection, ct, start.AddHours(1), "0xQLOOSE", "0xHLOOSE", elapsed: 700_000);
            await InsertProcedureAsync(connection, ct, start.AddDays(-3), "usp_Old", "0xHOLD");
            await InsertProcedureAsync(connection, ct, start.AddHours(2), "usp_NewBefore", "0xHNEW");
            await InsertProcedureAsync(connection, ct, start.AddHours(3), "usp_NewAfter", "0xHNEW");

            /* The retained history the lookup used to rank: 400 other handles, three rows each, none of them ranked. */
            await ExecAsync(connection, ct,
                @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                               object_type, cached_time, last_execution_time, sql_handle, plan_handle,
                                               delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes,
                                               delta_physical_reads, delta_spills)
                  SELECT 1, $1 + (r * interval '1 hour'), $2, $3, 'db', 'dbo', 'usp_other_' || h, 'PROCEDURE',
                         $1, $1 + (r * interval '1 hour'), '0xHOTHER' || h, '0xPLAN',
                         1, 1, 1, 1, 0, 0, 0
                  FROM generate_series(1, " + UnrelatedHandles + @") AS h, generate_series(1, 3) AS r",
                new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start.AddHours(-48) }, new NpgsqlParameter { Value = ServerId }, new NpgsqlParameter { Value = ServerName });

            /* The answer, through the Viewer's own read. */
            await using var viewer = new ViewerDataService(cs!);
            var rows = await viewer.GetTopQueriesByCpuAsync(ServerId, start, end);
            Assert.Equal("db.dbo.usp_Old", rows.Single(r => r.QueryHash == "0xQOLD").ModuleName);     /* older than the window, still named */
            Assert.Equal("db.dbo.usp_NewAfter", rows.Single(r => r.QueryHash == "0xQNEW").ModuleName); /* the newest row wins, no fan-out */
            Assert.Equal("ad hoc", rows.Single(r => r.QueryHash == "0xQLOOSE").ModuleName);
            Assert.Equal(3, rows.Count);

            /* The cost: the handle groups the module lookup keeps, read from the plan of the SHIPPED statement. Before #6804 it ranked
               every procedure_stats row of the server (3 x 400 unrelated rows plus the three above); it must keep only the ranked handles'. */
            await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + ViewerDataService.TopQueriesSql, connection);
            ViewerDataService.AddServerWindowParameters(explain, ServerId, start, end);
            explain.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 50 });
            explain.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
            explain.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 55 });
            explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
            var json = (string)(await explain.ExecuteScalarAsync(ct))!;
            using var document = JsonDocument.Parse(json);
            var groups = GroupsKeptFromProcedureStats(document.RootElement[0].GetProperty("Plan"));
            Assert.True(groups is > 0 and <= 3, $"the module lookup kept {groups} handle groups; it should keep only the ranked handles' (at most 3), not the server's {UnrelatedHandles + 2}: {json}");

            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, succeeded, (cleanup, cleanupCt) => CleanupAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The groups the module lookup's hash aggregate keeps, read from the plan (#8549 replaced the ROW_NUMBER sort with it). It is the one
    /// aggregate grouped on sql_handle: the ranking groups on database_name, query_hash and host_object_name. A plan with no such aggregate
    /// throws with the plan in the message, so a changed plan shape fails loudly instead of reading as zero. With the ranked-handle filter it
    /// keeps one group per ranked handle that has a procedure_stats row; without it, one per handle the server retains.
    /// </summary>
    private static double GroupsKeptFromProcedureStats(JsonElement plan)
    {
        var found = new System.Collections.Generic.List<double>();
        void Visit(JsonElement node)
        {
            if (node.TryGetProperty("Node Type", out var type) && type.GetString() == "Aggregate"
                && node.TryGetProperty("Group Key", out var keys)
                && keys.EnumerateArray().Any(k => (k.GetString() ?? string.Empty).Contains("sql_handle", StringComparison.Ordinal)))
            {
                found.Add(node.GetProperty("Actual Rows").GetDouble() * node.GetProperty("Actual Loops").GetDouble());
            }

            if (node.TryGetProperty("Plans", out var plans))
            {
                foreach (var child in plans.EnumerateArray())
                {
                    Visit(child);
                }
            }
        }

        Visit(plan);
        if (found.Count == 0)
        {
            throw new Xunit.Sdk.XunitException("no aggregate grouped on sql_handle (the module lookup's newest-row pass) was found in the plan: " + plan.GetRawText());
        }

        return found.Max();
    }

    private static Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ExecAsync(connection, ct, $"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM procedure_stats WHERE server_id = {ServerId}");

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params NpgsqlParameter[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task InsertQueryAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, string queryHash, string sqlHandle, long elapsed) =>
        ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                                       last_execution_time, creation_time, sample_interval_seconds, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                       delta_logical_reads, delta_rows, delta_logical_writes, delta_physical_reads, delta_spills, total_clr_time, plan_generation_num, query_text)
              VALUES (1, $1, $2, $3, 'db', $4, '0xPLANHASH', $5, '0xPLANHANDLE', $1, $1, 60, 5, $6, $6, 10, 0, 0, 0, 0, 0, 1, 'SELECT 1')",
            new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = at }, new NpgsqlParameter { Value = ServerId }, new NpgsqlParameter { Value = ServerName },
            new NpgsqlParameter { Value = queryHash }, new NpgsqlParameter { Value = sqlHandle }, new NpgsqlParameter { Value = elapsed });

    private static Task InsertProcedureAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, string objectName, string sqlHandle) =>
        ExecAsync(connection, ct,
            @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
                                           cached_time, last_execution_time, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                           delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_spills)
              VALUES (1, $1, $2, $3, 'db', 'dbo', $4, 'PROCEDURE', $1, $1, $5, '0xPLANHANDLE', 1, 1, 1, 1, 0, 0, 0)",
            new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = at }, new NpgsqlParameter { Value = ServerId }, new NpgsqlParameter { Value = ServerName },
            new NpgsqlParameter { Value = objectName }, new NpgsqlParameter { Value = sqlHandle });
}
