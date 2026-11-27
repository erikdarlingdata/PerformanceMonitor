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
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through QueryStoreClosedDayRollupLiveTests.SeedAsync (ScratchPostgres) and touches nothing on the shared one. */

/// <summary>
/// #9478: the fleet median reads only <c>plans_per_query_p95</c> per (server, database), so the fleet arm reads it from a narrow
/// statement (<see cref="DarlingQueryStoreClutterReader.GetPlanChurnP95Async"/>) instead of the full plan-churn read. Its answer must be
/// the full read's p95, digit for digit, on every route the read can take: raw alone, whole days from the daily rollups, and whole hours
/// from the hourly rollups. The seeding, the tick and the store come from <see cref="QueryStoreClosedDayRollupLiveTests"/> and
/// <see cref="QueryStoreClutterRollupLiveTests"/>.
/// </summary>
public sealed class QueryStoreClutterFleetP95LiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the #9478 narrow fleet read live test.";
    private const int DayRetention = 30;
    private const int RawRetention = 12;
    private static int A => QueryStoreClosedDayRollupLiveTests.ServerId;
    private const int B = -9478001;
    private static DateTime E => QueryStoreClosedDayRollupLiveTests.DataEnd;
    private static DateTime TickNow => QueryStoreClosedDayRollupLiveTests.TickNow;

    private sealed record Window(string Label, DateTime Start, DateTime End, bool Hours);

    private static readonly DateTime DayEdgeA = E.AddDays(-4);
    private static readonly DateTime DayEdgeB = E.AddDays(-2);

    private static readonly Window[] Windows =
    [
        new("24 hours, both ends inside an hour", E.AddHours(-3).AddMinutes(-20).AddDays(-1), E.AddHours(-3).AddMinutes(-20), true),
        new("7 days, both ends inside an hour", E.AddHours(-3).AddMinutes(-20).AddDays(-7), E.AddHours(-3).AddMinutes(-20), true),
        new("whole days with raw minutes at the edges", DayEdgeA.AddMinutes(-1), DayEdgeB.AddMinutes(1), false),
    ];

    /// <summary>
    /// Rows with known plan counts per query, inside every window: <c>dbMany</c> has one query with seven plans and four with one,
    /// <c>dbSingle</c> has queries of one plan each, and a NULL query and plan sits in <c>dbMany</c> (the seed has one in the others).
    /// </summary>
    private const string PlantSql = @"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash,
                                       query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
SELECT 770000000 + @salt + row_number() OVER (), x.at, @server_id, 'fleet-p95', x.db, x.q, x.p, '0xQ', '0xP', 'SELECT 1', 3, 1000, 800, x.at
FROM
(
    SELECT E0.at, 'dbMany' AS db, 9001 AS q, 90010 + p AS p FROM generate_series(1, 7) AS p CROSS JOIN LATERAL (SELECT @at - make_interval(mins => p) AS at) AS E0
    UNION ALL
    SELECT @at, 'dbMany', 9100 + q, 91000 + q FROM generate_series(1, 4) AS q
    UNION ALL
    SELECT @at, 'dbMany', NULL, NULL
    UNION ALL
    SELECT @at, 'dbSingle', 9200 + q, 92000 + q FROM generate_series(1, 5) AS q
) AS x;";

    private static async Task SeedAsync(QueryStoreClosedDayRollupLiveTests.Store store, CancellationToken ct)
    {
        await QueryStoreClutterRollupLiveTests.SeedServerAsync(store, A, E.AddDays(-10), 10, 48, 12, true, ct, oddRows: true);
        await QueryStoreClutterRollupLiveTests.SeedServerAsync(store, B, E.AddDays(-6), 6, 24, 8, true, ct, oddRows: true);
        var salt = 0;
        foreach (var server in new[] { A, B })
        {
            foreach (var at in new[] { E.AddHours(-10), E.AddDays(-3).AddHours(-2) })
            {
                await using var command = new NpgsqlCommand(PlantSql, store.Connection);
                command.Parameters.AddWithValue("server_id", server);
                command.Parameters.AddWithValue("salt", salt += 1000);
                command.Parameters.Add(new NpgsqlParameter("at", NpgsqlDbType.Timestamp) { Value = at });
                await command.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private static async Task<(List<(int Server, string Database, int P95)> Narrow, List<(int Server, string Database, int P95)> Full)> ReadBothAsync(
        QueryStoreClosedDayRollupLiveTests.Store store, int[] servers, Window w, DateTime? asOf, CancellationToken ct)
    {
        var full = await DarlingQueryStoreClutterReader.GetPlanChurnAsync(store.Postgres, servers, w.Start, w.End, asOfUtc: asOf, cancellationToken: ct);
        var narrow = await DarlingQueryStoreClutterReader.GetPlanChurnP95Async(store.Postgres, servers, w.Start, w.End, asOfUtc: asOf, cancellationToken: ct);
        return (
            narrow.Select(r => (r.ServerId, r.DatabaseName, r.PlansPerQueryP95)).OrderBy(r => r.ServerId).ThenBy(r => r.DatabaseName, StringComparer.Ordinal).ToList(),
            full.Select(r => (r.ServerId, r.DatabaseName, r.PlansPerQueryP95)).OrderBy(r => r.ServerId).ThenBy(r => r.DatabaseName, StringComparer.Ordinal).ToList());
    }

    private static string Lines(List<(int Server, string Database, int P95)> rows) => string.Join("\n", rows.Select(r => $"{r.Server}|{r.Database}|{r.P95}"));

    [Fact]
    public async Task TheNarrowRead_GivesTheFullReadsP95_OnTheRawRoute_TheDayRoute_AndTheHourRoute_ForOneServerAndTwo()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedAsync(store, ct);

        var fleets = new (string Label, int[] Servers)[] { ("one server", [A]), ("two servers", [A, B]) };

        /* Before the tick nothing is built, so every window reads raw alone. */
        foreach (var f in fleets)
        {
            foreach (var w in Windows)
            {
                Assert.Null(await DarlingQueryStoreClutterReader.ResolveClutterRouteAsync(store.Postgres, f.Servers, w.Start, w.End, ct));
                var (narrow, full) = await ReadBothAsync(store, f.Servers, w, null, ct);
                Assert.True(full.Count > 0, $"{f.Label} / {w.Label}: raw read returned no rows");
                Assert.Equal(Lines(full), Lines(narrow));
                /* the planted shapes: seven plans against four singles puts dbMany's p95 on the seven, dbSingle's on one */
                Assert.Contains(narrow, r => r.Server == A && r.Database == "dbMany" && r.P95 == 7);
                Assert.Contains(narrow, r => r.Server == A && r.Database == "dbSingle" && r.P95 == 1);
            }
        }

        var tick = await QueryStoreClosedDayRollupLiveTests.TickAsync(store, DayRetention, ct, TickNow, RawRetention);
        Assert.True(tick.HoursBuilt > 0, "the tick built no hour");

        var hoursSeen = false;
        var daysSeen = false;
        foreach (var f in fleets)
        {
            foreach (var w in Windows)
            {
                var route = await DarlingQueryStoreClutterReader.ResolveClutterRouteAsync(store.Postgres, f.Servers, w.Start, w.End, ct);
                Assert.True(route is not null, $"{f.Label} / {w.Label}: nothing is served from the rollups, so the comparison would prove nothing");
                hoursSeen |= route!.Value.Hours;
                daysSeen |= route.Value.DayStart < route.Value.DayEnd;
                var (narrow, full) = await ReadBothAsync(store, f.Servers, w, null, ct);
                Assert.True(full.Count > 0, $"{f.Label} / {w.Label}: routed read returned no rows");
                Assert.Equal(Lines(full), Lines(narrow));
                Assert.Contains(narrow, r => r.Server == A && r.Database == "dbMany" && r.P95 == 7);
            }
        }

        Assert.True(hoursSeen, "no window took the hour route");
        Assert.True(daysSeen, "no window took whole days from the daily rollups");
    }

    [Fact]
    public async Task TheNarrowRead_LeavesOutADroppedDatabase_AsTheFullReadDoes_AndTheFleetMedianIsTheSame()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedAsync(store, ct);
        await QueryStoreClosedDayRollupLiveTests.TickAsync(store, DayRetention, ct, TickNow, RawRetention);

        /* The newest snapshot of each server names dbA and dbMany only, so dbB, dbC and dbSingle are dropped databases. It is newer than every
           row, so nothing keeps a dropped database by its last sighting. */
        foreach (var server in new[] { A, B })
        {
            var id = 1;
            foreach (var database in new[] { "dbA", "dbMany" })
            {
                await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
                    "INSERT INTO collect.database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES (@id, @at, @server_id, 'fleet-p95', @db, @dbid, 'ONLINE', FALSE)", ct,
                    ("id", CollectionIdGenerator.Next()), ("at", TickNow), ("server_id", server), ("db", database), ("dbid", id++));
            }
        }

        var asOf = TickNow.AddMinutes(5);
        var servers = new[] { A, B };
        var w = Windows[1];
        var (narrow, full) = await ReadBothAsync(store, servers, w, asOf, ct);
        Assert.Equal(Lines(full), Lines(narrow));
        Assert.DoesNotContain(narrow, r => r.Database is "dbB" or "dbC" or "dbSingle");
        Assert.Contains(narrow, r => r.Database == "dbMany");

        /* with no snapshot in reach nothing is dropped, and the two reads still agree */
        var (narrowAll, fullAll) = await ReadBothAsync(store, servers, w, E.AddDays(-100), ct);
        Assert.Equal(Lines(fullAll), Lines(narrowAll));
        Assert.Contains(narrowAll, r => r.Database == "dbSingle");

        /* the fleet median gives one figure from either shape of rows */
        var fullRows = await DarlingQueryStoreClutterReader.GetPlanChurnAsync(store.Postgres, servers, w.Start, w.End, asOfUtc: asOf, cancellationToken: ct);
        var narrowRows = await DarlingQueryStoreClutterReader.GetPlanChurnP95Async(store.Postgres, servers, w.Start, w.End, asOfUtc: asOf, cancellationToken: ct);
        var fromFull = QueryStoreClutter.ComputeFleetMedian(servers, [], fullRows, [], []);
        var fromNarrow = QueryStoreClutter.ComputeFleetMedian(servers, [], narrowRows, [], []);
        Assert.NotNull(fromFull.PlansPerQueryP95);
        Assert.Equal(fromFull, fromNarrow);
    }
}
