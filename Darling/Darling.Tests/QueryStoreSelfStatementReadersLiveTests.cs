/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6665: the monitor's own Query Store statement is hidden from every reader that ranks Query Store queries. On Darling the
/// collector's drop of it never fires (the statement text arrives separately, so the row it tests is empty), so the statement is a
/// stored fact row and its marker is only in <c>collect.query_store_text</c>. Seeded the way a live store is: the stats rows carry
/// NO inline text, both queries' text is in <c>query_store_text</c>, and one of them carries the marker. Each ranked reader lists
/// the normal query (20) and not the monitor's own (23): the MCP Query Store top, the Viewer's Query Store grid, the aborted and
/// failed queries read, and the Viewer's current-vs-baseline comparison.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres (the rig of QueryStoreRegressionsFailureGroupsLiveTests) and works entirely inside it. It
   shares the gap-cache collection with the other regressions table tests, because the gate's cadence check reads a process-wide
   cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreSelfStatementReadersLiveTests
{
    private const int ServerId = QueryStoreRegressionsFailureGroupsLiveTests.ServerId;
    private const string Db = QueryStoreRegressionsFailureGroupsLiveTests.Db;
    private const long Normal = 20;
    private const long Own = 23;

    [Fact]
    public async Task TheMonitorsOwnStatement_StoredOnlyInTheTextTable_IsHiddenFromEveryRankedQueryStoreReader()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), "Set DARLING_TEST_PG to a Postgres connection string to run the #6665 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var now = LiveClock.Now();
        await using var rig = await QueryStoreRegressionsFailureGroupsLiveTests.OpenAsync(async (runner, server, context, today, start) =>
        {
            long id = 9700;
            foreach (var query in new[] { Normal, Own })
            {
                foreach (var type in new[] { "Regular", "Aborted" })
                {
                    id++;
                    await QueryStoreRegressionsFailureGroupsLiveTests.WriteAsync(runner, server, context, type, id, now.AddHours(-3), query, 100, 2_000, 4_000, ct);
                }
            }
        }, ct);

        /* The live shape: no inline text on the stats rows, the text of both queries in collect.query_store_text. */
        await using (var blank = new NpgsqlCommand("UPDATE query_store_stats SET query_text = NULL WHERE server_id = $1", rig.Connection))
        {
            blank.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            await blank.ExecuteNonQueryAsync(ct);
        }

        await using (var production = new NpgsqlCommand(
            "INSERT INTO query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1, $2, $3, 'SELECT normal', $5), ($1, $2, $4, $6, $5)",
            rig.Connection))
        {
            production.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            production.Parameters.Add(new NpgsqlParameter<string> { TypedValue = Db });
            production.Parameters.Add(new NpgsqlParameter<long> { TypedValue = Normal });
            production.Parameters.Add(new NpgsqlParameter<long> { TypedValue = Own });
            production.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(now, DateTimeKind.Unspecified) });
            production.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "(@cutoff_time datetime2(7))SELECT /* " + QueryStoreCollector.SelfQueryMarker + " */ qsrs.plan_id" });
            await production.ExecuteNonQueryAsync(ct);
        }

        /* Not vacuous: both queries' rows are stored, so what hides query 23 is the readers. */
        await using (var count = new NpgsqlCommand("SELECT count(DISTINCT query_id) FROM query_store_stats WHERE server_id = $1 AND query_text IS NULL", rig.Connection))
        {
            count.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            Assert.Equal(2L, await count.ExecuteScalarAsync(ct));
        }

        var start = now.AddHours(-24);
        var end = now.AddMinutes(1);

        var top = await DarlingDataReader.GetQueryStoreTopAsync(rig.Postgres, ServerId, start, end, 10, DatabaseFilter.All, ct);
        Assert.Equal([Normal], top.Select(r => r.QueryId).Distinct().ToList());

        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var grid = await viewer.GetQueryStoreTopQueriesAsync(ServerId, start, end, 10, null, null, null, ct);
        Assert.Equal([Normal], grid.Select(r => r.QueryId).Distinct().ToList());

        var aborts = await DarlingQueryStoreAbortsReader.ReadAsync(rig.Postgres, ServerId, start, end, 10, DatabaseFilter.All, ct);
        Assert.Equal([Normal], aborts.Rows.Select(r => r.QueryId).ToList());

        var comparison = await viewer.GetQueryStoreComparisonAsync(ServerId, start, end, start.AddHours(-24), start, null, ct);
        var hash = "0x" + Normal.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal([hash], comparison.Select(r => r.QueryHash).ToList());
    }
}
