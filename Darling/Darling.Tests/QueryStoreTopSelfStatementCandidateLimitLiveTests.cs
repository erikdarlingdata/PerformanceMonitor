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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7736: the Query Store top read cuts its window to <c>TopFill.FinalCandidates(top)</c> candidates in <c>ranked</c>, and the
/// monitor's own statements were removed only after that cut (the tail). A window with more of the monitor's rows than candidates,
/// all ranking above the real work, filled the page with none of it. Once the key table's backfill has run the statements are left
/// out before the cut, so the page still fills with real rows. The MCP read and the Viewer's grid are both covered.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it. Same collection as the other self-statement reader tests. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreTopSelfStatementCandidateLimitLiveTests
{
    private const int ServerId = QueryStoreRegressionsFailureGroupsLiveTests.ServerId;
    private const string Db = QueryStoreRegressionsFailureGroupsLiveTests.Db;
    private const long Real = 20;
    private const long FirstOwn = 1000;
    private const int OwnRows = 120;

    [Fact]
    public async Task MoreOfTheMonitorsOwnRowsThanCandidates_StillFillThePageWithRealRows_WhenTheKeysAreReady()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), "Set DARLING_TEST_PG to a Postgres connection string to run the #7736 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var now = LiveClock.Now();
        Assert.True(TopFill.FinalCandidates(1) < OwnRows, "the fixture must hold more of the monitor's rows than the candidate limit");

        await using var rig = await QueryStoreRegressionsFailureGroupsLiveTests.OpenAsync(async (runner, server, context, today, start) =>
        {
            var first = now.AddHours(-3);
            var rows = new List<QueryStoreCollector.Row>();
            for (long query = FirstOwn; query < FirstOwn + OwnRows; query++)
            {
                rows.Add(Row(query, first, durationUs: 4_000_000));
            }

            rows.Add(Row(Real, first, durationUs: 4_000));
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, rows, server, first.AddMinutes(40), context, ct);
        }, ct);

        /* The live shape: no inline text on the stats rows, the text in collect.query_store_text. */
        await using (var blank = new NpgsqlCommand("UPDATE query_store_stats SET query_text = NULL WHERE server_id = $1", rig.Connection))
        {
            blank.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            await blank.ExecuteNonQueryAsync(ct);
        }

        await using (var text = new NpgsqlCommand(
            @"INSERT INTO query_store_text (server_id, database_name, query_id, query_sql_text, last_seen)
SELECT $1, $2, q, $5, $6 FROM generate_series($3::bigint, $4::bigint) AS q
UNION ALL
SELECT $1, $2, $7, 'SELECT normal', $6",
            rig.Connection))
        {
            text.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            text.Parameters.Add(new NpgsqlParameter<string> { TypedValue = Db });
            text.Parameters.Add(new NpgsqlParameter<long> { TypedValue = FirstOwn });
            text.Parameters.Add(new NpgsqlParameter<long> { TypedValue = FirstOwn + OwnRows - 1 });
            text.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "(@cutoff_time datetime2(7))SELECT /* " + QueryStoreCollector.SelfQueryMarker + " */ qsrs.plan_id" });
            text.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(now, DateTimeKind.Unspecified) });
            text.Parameters.Add(new NpgsqlParameter<long> { TypedValue = Real });
            await text.ExecuteNonQueryAsync(ct);
        }

        /* Not vacuous: every row is stored and the monitor's rows outnumber the candidates, so only the readers can hide them. */
        await using (var count = new NpgsqlCommand("SELECT count(DISTINCT query_id) FROM query_store_stats WHERE server_id = $1 AND query_text IS NULL", rig.Connection))
        {
            count.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            Assert.Equal((long)OwnRows + 1, await count.ExecuteScalarAsync(ct));
        }

        await using (var stored = new NpgsqlCommand("SELECT count(*) FROM query_store_text WHERE server_id = $1 AND strpos(query_sql_text, $2) > 0", rig.Connection))
        {
            stored.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            stored.Parameters.Add(new NpgsqlParameter<string> { TypedValue = QueryStoreCollector.SelfQueryMarker });
            Assert.Equal((long)OwnRows, await stored.ExecuteScalarAsync(ct));
        }

        /* The backfill has run: the gate is open and the key table holds the monitor's statements. */
        await QueryStoreSelfKeys.BackfillAsync(rig.Connection, ct);
        Assert.True(await QueryStoreSelfKeys.ReadyAsync(rig.Connection, null, 30, ct));
        await using (var keys = new NpgsqlCommand("SELECT count(*) FROM collect.query_store_self_keys WHERE server_id = $1", rig.Connection))
        {
            keys.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            Assert.Equal((long)OwnRows, await keys.ExecuteScalarAsync(ct));
        }

        var start = now.AddHours(-24);
        var end = now.AddMinutes(1);

        var top = await DarlingDataReader.GetQueryStoreTopAsync(rig.Postgres, ServerId, start, end, 1, DatabaseFilter.All, ct);
        Assert.Equal([Real], top.Select(r => r.QueryId).Distinct().ToList());

        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var grid = await viewer.GetQueryStoreTopQueriesAsync(ServerId, start, end, 1, null, null, null, ct);
        Assert.Equal([Real], grid.Select(r => r.QueryId).Distinct().ToList());
    }

    private static QueryStoreCollector.Row Row(long queryId, DateTime first, long durationUs) => new()
    {
        DatabaseName = Db,
        QueryId = queryId,
        PlanId = queryId * 10,
        ExecutionTypeDesc = "Regular",
        FirstExecutionTime = first,
        LastExecutionTime = first.AddMinutes(30),
        QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
        QueryPlanHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
        QueryText = "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
        ExecutionCount = 100,
        AvgCpuTimeUs = 2_000,
        AvgDurationUs = durationUs,
        IsForcedPlan = false,
        ForceFailureCount = 0,
        RuntimeStatsIntervalId = queryId,
        IntervalStartTimeUtc = first,
    };
}
