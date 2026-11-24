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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through QueryStoreClosedDayRollupLiveTests.SeedAsync (ScratchPostgres) and touches nothing on the shared one. */

/// <summary>
/// #9148: the plan-churn arm of <c>get_query_store_clutter</c> takes whole built UTC days from the clutter rollups and the rest of the
/// window from raw <c>query_store_stats</c>. Each test seeds raw rows, reads the window before any day is built (every statement is the raw one),
/// builds the days with the V283 closed-day pass, and reads again: the reply must be the same rows, digit for digit. The seeding, the tick and the
/// store come from <see cref="QueryStoreClosedDayRollupLiveTests"/>.
/// </summary>
public sealed class QueryStoreClutterRollupLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the #9148 clutter rollup live test.";
    private const int Retention = 30;
    private static int A => QueryStoreClosedDayRollupLiveTests.ServerId;
    private const int B = -9148001;
    private const int C = -9148002;

    /// <summary>The window's end and the newest instant a seeded row can sit at: a midnight. TickNow is two days later, so every day before it is closed.</summary>
    private static DateTime E => QueryStoreClosedDayRollupLiveTests.DataEnd;

    /// <summary>
    /// One server's raw rows: <paramref name="days"/> days from <paramref name="firstDay"/>, each with <paramref name="perDay"/> collections an
    /// even step apart plus one at the day's first instant and one at its last microsecond, and one more at the instant the last day ends. A plan
    /// is absent from some collections, changes id every three days, and a query's rows are in one of three databases. Every seventh query is written
    /// twice per collection (a tie on <c>collection_time</c>).
    /// </summary>
    private const string BulkSql = @"
WITH times AS
(
    SELECT d, k, @first_day + make_interval(days => d) + k * make_interval(secs => 86400.0 / @per_day) + interval '7 minutes' AS at
    FROM generate_series(0, @days - 1) AS d CROSS JOIN generate_series(0, @per_day - 1) AS k
    UNION ALL
    SELECT d, -1, @first_day + make_interval(days => d) FROM generate_series(0, @days) AS d
    UNION ALL
    SELECT d, -2, @first_day + make_interval(days => d + 1) - interval '1 microsecond' FROM generate_series(0, @days - 1) AS d
)
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash,
                                       query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
SELECT @id_base + row_number() OVER (), t.at, @server_id, 'clutter-' || @server_id,
       CASE q % 3 WHEN 0 THEN 'dbA' WHEN 1 THEN 'dbB' ELSE 'dbC' END, 100 + q, 1000 + q * 10 + t.d / 3,
       '0xQ' || q, '0xP' || (q * 10 + t.d / 3), 'SELECT 1', 10, 1000, 800, t.at
FROM times AS t
CROSS JOIN generate_series(1, @plans) AS q
CROSS JOIN generate_series(1, CASE WHEN q % 7 = 0 THEN 2 ELSE 1 END) AS dup
WHERE (q * 3 + t.d * 5 + t.k + 2) % 4 <> 0;";

    /// <summary>The rows the bulk seed does not make: a NULL query and plan, a NULL plan alone, a plan seen once, and a plan seen once at a day's last microsecond.</summary>
    private const string OddRowsSql = @"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash,
                                       query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
SELECT @id_base + 90000000 + row_number() OVER (), x.at, @server_id, 'clutter-' || @server_id, x.db, x.q, x.p, '0xQ', '0xP', 'SELECT 1', 10, 1000, 800, x.at
FROM
(
    SELECT 'dbA' AS db, NULL::bigint AS q, NULL::bigint AS p, @first_day + make_interval(days => d, hours => 6) + interval '7 minutes' AS at FROM generate_series(0, @days - 1, 2) AS d
    UNION ALL
    SELECT 'dbB', 5, NULL, @first_day + make_interval(days => d, hours => 12) + interval '7 minutes' FROM generate_series(1, @days - 1, 2) AS d
    UNION ALL
    SELECT 'dbC', 900 + d, 9000 + d, @first_day + make_interval(days => d, hours => 12) + interval '7 minutes' FROM generate_series(0, @days - 1) AS d
    UNION ALL
    SELECT 'dbB', 888, 8880, @first_day + make_interval(days => 3 + 1) - interval '1 microsecond'
) AS x;";

    private sealed record Window(string Label, DateTime Start, DateTime End, string? Database);

    private static readonly Window[] Windows =
    [
        new("whole days, start at midnight, end at the last row's instant", E.AddDays(-7), E, null),
        new("starts and ends inside a day", E.AddDays(-7).AddHours(5), E.AddDays(-1).AddHours(10), null),
        new("one database", E.AddDays(-7), E, "dbB"),
        new("starts a microsecond before midnight", E.AddDays(-5).AddTicks(-10), E.AddDays(-2), null),
        new("reaches the first seeded day", E.AddDays(-9), E, null),
    ];

    internal static async Task SeedServerAsync(QueryStoreClosedDayRollupLiveTests.Store store, int server, DateTime firstDay, int days, int perDay, int plans, bool covered, CancellationToken ct, bool oddRows = false)
    {
        if (server != A)
        {
            await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
                "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (@server_id, 'clutter-rollup-' || @server_id, TRUE) ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct,
                ("server_id", server));
            if (covered)
            {
                await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
                    "INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES (@server_id, @since, @through)", ct,
                    ("server_id", server), ("since", E.AddDays(-90)), ("through", QueryStoreClosedDayRollupLiveTests.TickNow));
            }
        }
        else
        {
            /* The shared seed's collector writes a few raw rows of its own for this server. */
            await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection, "DELETE FROM collect.query_store_stats WHERE server_id = @server_id", ct, ("server_id", A));
        }

        var parameters = new (string, object)[]
        {
            ("server_id", server), ("first_day", firstDay), ("days", days), ("per_day", perDay), ("plans", plans), ("id_base", (long)(Math.Abs(server) % 1000) * 1_000_000_000L),
        };
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection, BulkSql, ct, parameters);
        if (perDay <= 8 || oddRows)
        {
            await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection, OddRowsSql, ct, parameters);
        }
    }

    private static string Lines(List<DarlingQueryStoreClutterReader.PlanChurnRow> rows) => string.Join("\n", rows.Select(r => r.ToString()));

    private static async Task<List<DarlingQueryStoreClutterReader.PlanChurnRow>> ReadAsync(
        QueryStoreClosedDayRollupLiveTests.Store store, int[] servers, Window w, CancellationToken ct) =>
        await DarlingQueryStoreClutterReader.GetPlanChurnAsync(store.Postgres, servers, w.Start, w.End, DatabaseFilter.One(w.Database), cancellationToken: ct);

    /// <summary>The reply as raw alone answers it: every day's clutter stamp is cleared for the read, so no day is served from a rollup, and put back after.</summary>
    private static async Task<List<DarlingQueryStoreClutterReader.PlanChurnRow>> ReadRawAsync(
        QueryStoreClosedDayRollupLiveTests.Store store, int[] servers, Window w, CancellationToken ct)
    {
        var folded = new List<(int Server, DateTime Day)>();
        await using (var hide = new NpgsqlCommand(
            "UPDATE collect.query_store_clutter_day_built SET rebuild_needed = true WHERE NOT rebuild_needed RETURNING server_id, day_utc", store.Connection))
        await using (var reader = await hide.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                folded.Add((reader.GetInt32(0), reader.GetDateTime(1)));
            }
        }

        var stamped = new List<(int Server, DateTime Day)>();
        await using (var clear = new NpgsqlCommand(
            "UPDATE collect.query_store_closed_day_built SET clutter_built_at = NULL WHERE clutter_built_at = built_at RETURNING server_id, day_utc", store.Connection))
        await using (var reader = await clear.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                stamped.Add((reader.GetInt32(0), reader.GetDateTime(1)));
            }
        }

        try
        {
            Assert.Null(await DarlingQueryStoreClutterReader.ResolveClosedDaysAsync(store.Postgres, servers, w.Start, w.End, ct));
            return await ReadAsync(store, servers, w, ct);
        }
        finally
        {
            await using var restoreFolded = new NpgsqlCommand(
                "UPDATE collect.query_store_clutter_day_built AS b SET rebuild_needed = false FROM unnest(@s::int[], @d::date[]) AS u(s, d) WHERE b.server_id = u.s AND b.day_utc = u.d", store.Connection);
            restoreFolded.Parameters.Add(new NpgsqlParameter("s", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = folded.Select(x => x.Server).ToArray() });
            restoreFolded.Parameters.Add(new NpgsqlParameter("d", NpgsqlDbType.Array | NpgsqlDbType.Date) { Value = folded.Select(x => x.Day).ToArray() });
            await restoreFolded.ExecuteNonQueryAsync(ct);

            await using var restore = new NpgsqlCommand(
                "UPDATE collect.query_store_closed_day_built AS b SET clutter_built_at = b.built_at FROM unnest(@s::int[], @d::date[]) AS u(s, d) WHERE b.server_id = u.s AND b.day_utc = u.d", store.Connection);
            restore.Parameters.Add(new NpgsqlParameter("s", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = stamped.Select(x => x.Server).ToArray() });
            restore.Parameters.Add(new NpgsqlParameter("d", NpgsqlDbType.Array | NpgsqlDbType.Date) { Value = stamped.Select(x => x.Day).ToArray() });
            await restore.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<QueryStoreClosedDayRollup.ServedDays?> RouteAsync(QueryStoreClosedDayRollupLiveTests.Store store, int[] servers, Window w, CancellationToken ct) =>
        await DarlingQueryStoreClutterReader.ResolveClosedDaysAsync(store.Postgres, servers, w.Start, w.End, ct);

    private static bool Covers(QueryStoreClosedDayRollup.ServedDays? run, DateTime day) => run is { } r && r.Start <= day && day < r.End;

    [Fact]
    public async Task TheRoutedReply_EqualsTheRawReply_ForTiesNullsSingleSightingsMidnightEdgesADatabaseChoiceAndAServerWithNoBuiltDay()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);

        /* A: ten days. C: six days (a shorter run than A's). B: ten days and never built (no coverage claim, so the tick plans nothing for it). */
        await SeedServerAsync(store, A, E.AddDays(-10), 10, 4, 30, true, ct);
        await SeedServerAsync(store, C, E.AddDays(-6), 6, 4, 20, true, ct);
        await SeedServerAsync(store, B, E.AddDays(-10), 10, 4, 10, false, ct);

        var fleets = new (string Label, int[] Servers, bool Routed)[]
        {
            ("one server", [A], true),
            ("two built servers", [A, C], true),
            ("one server has no built day", [A, B], false),
        };

        var before = new Dictionary<string, string>();
        var sawSeenOnce = false;
        foreach (var f in fleets)
        {
            foreach (var w in Windows)
            {
                var rows = await ReadAsync(store, f.Servers, w, ct);
                Assert.True(rows.Count > 0, $"{f.Label} / {w.Label}: the raw read returned no rows");
                before[f.Label + w.Label] = Lines(rows);
                sawSeenOnce |= rows.Any(r => r.PlansSeenOnce > 0);
            }
        }

        /* the seed has the shapes the test names: a NULL query, a plan seen once, a tie */
        Assert.True(await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            "SELECT count(*) FROM collect.query_store_stats WHERE server_id = @server_id AND query_id IS NULL AND plan_id IS NULL", ct, ("server_id", A)) > 0);
        Assert.True(await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            "SELECT count(*) FROM (SELECT 1 FROM collect.query_store_stats WHERE server_id = @server_id GROUP BY collection_time, database_name, query_id, plan_id HAVING count(*) > 1) AS t", ct, ("server_id", A)) > 0);
        Assert.True(sawSeenOnce);

        await QueryStoreClosedDayRollupLiveTests.TickAsync(store, Retention, ct);

        foreach (var f in fleets)
        {
            foreach (var w in Windows)
            {
                var run = await RouteAsync(store, f.Servers, w, ct);
                Assert.Equal(f.Routed, run is not null);
                Assert.Equal(before[f.Label + w.Label], Lines(await ReadAsync(store, f.Servers, w, ct)));
            }
        }

        /* the shared run of two built servers is the shorter server's */
        var common = await RouteAsync(store, [A, C], Windows[0], ct);
        Assert.Equal(E.AddDays(-6), common!.Value.Start);
    }

    [Fact]
    public async Task ALateRawRowInABuiltDay_MarksTheDay_IsReadFromRawUntilTheNextTickRebuildsIt_AndThenIsInTheRollup()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedServerAsync(store, A, E.AddDays(-10), 10, 4, 30, true, ct);
        var w = Windows[0];
        var fleet = new[] { A };

        await QueryStoreClosedDayRollupLiveTests.TickAsync(store, Retention, ct);
        var served = await RouteAsync(store, fleet, w, ct);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(E.AddDays(-6), E), served);
        var routedBefore = Lines(await ReadAsync(store, fleet, w, ct));

        /* A backfilled batch in a built day, written the way the collector's batch is: its rows and the mark in one transaction. */
        var lateDay = E.AddDays(-4);
        var lateAt = lateDay.AddHours(10).AddMinutes(33);
        await using (var tx = await store.Connection.BeginTransactionAsync(ct))
        {
            await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
VALUES (990000001, @at, @server_id, 'late', 'dbA', 7777, 77770, '0xQ', '0xP', 'SELECT 1', 1, 1, 1, @at),
       (990000002, @at, @server_id, 'late', 'dbB', 101, 1010, '0xQ', '0xP', 'SELECT 1', 1, 1, 1, @at)", store.Connection, tx))
            {
                insert.Parameters.AddWithValue("at", lateAt);
                insert.Parameters.AddWithValue("server_id", A);
                await insert.ExecuteNonQueryAsync(ct);
            }

            Assert.True(await QueryStoreClosedDayRollup.TryMarkLateCollectionDayAsync(store.Connection, tx, A, lateAt, QueryStoreClosedDayRollupLiveTests.TickNow, 30, null, ct));
            await tx.CommitAsync(ct);
        }

        Assert.Equal(1L, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            "SELECT count(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND rebuild_needed AND day_utc = @day", ct, ("server_id", A), ("day", lateDay)));

        /* The marked day is left to raw: the reply has the row, and it is the raw reply. */
        var raw = Lines(await ReadRawAsync(store, fleet, w, ct));
        Assert.NotEqual(routedBefore, raw);
        Assert.False(Covers(await RouteAsync(store, fleet, w, ct), lateDay), "the read still takes the marked day from the rollup");
        Assert.Equal(raw, Lines(await ReadAsync(store, fleet, w, ct)));

        /* The next tick builds the day again, and the rollup holds the late rows. */
        var tick = await QueryStoreClosedDayRollupLiveTests.TickAsync(store, Retention, ct, QueryStoreClosedDayRollupLiveTests.TickNow.AddHours(1));
        Assert.True(tick.Built >= 1);
        Assert.Equal(0L, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            "SELECT count(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND rebuild_needed", ct, ("server_id", A)));
        Assert.Equal(served, await RouteAsync(store, fleet, w, ct));
        Assert.Equal(raw, Lines(await ReadAsync(store, fleet, w, ct)));
        Assert.Equal(1L, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            "SELECT count(*) FROM collect.query_store_clutter_plan_daily WHERE server_id = @server_id AND day_utc = @day AND plan_id = 77770", ct, ("server_id", A), ("day", lateDay)));
    }

    /// <summary>
    /// The collector's own write path marks a built day that a backfilled batch lands in, and a live batch marks nothing. The batch goes in through
    /// <see cref="DarlingCollectorRunner.WriteBackfillBatchAsync{TRow}"/>, the runner's write for Query Store rows, so the call site that sends
    /// the mark is the one under test, not the mark's own method.
    /// </summary>
    [Fact]
    public async Task TheRunnersWritePath_MarksABuiltDayThatABackfilledBatchLandsIn_AndALiveBatchMarksNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedServerAsync(store, A, E.AddDays(-10), 10, 4, 30, true, ct);
        await QueryStoreClosedDayRollupLiveTests.TickAsync(store, Retention, ct);

        const string MarkedSql = "SELECT count(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND rebuild_needed";
        Assert.Equal(0L, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection, MarkedSql, ct, ("server_id", A)));

        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "clutter-late", Host = "clutter-late-host" },
            ConnectionString = "Server=clutter-late-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "clutter-late-host",
            ServerId = A,
            EngineEdition = 3,
        };

        async Task WriteAsync(DateTime collectionTime, long queryId, DateTime lastExecution)
        {
            var context = new CollectorContext { ServerId = A, ServerName = server.StorageName, CollectionTime = collectionTime, Deltas = new CollectorDeltaCalculator() };
            var row = new QueryStoreCollector.Row
            {
                DatabaseName = "dbA",
                QueryId = queryId,
                PlanId = queryId * 10,
                ExecutionTypeDesc = "Regular",
                FirstExecutionTime = lastExecution,
                LastExecutionTime = lastExecution,
                QueryHash = "0x" + queryId.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + (queryId * 10).ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
                ExecutionCount = 1,
                AvgCpuTimeUs = 10,
                AvgDurationUs = 20,
                RuntimeStatsIntervalId = queryId,
                IntervalStartTimeUtc = lastExecution,
                IntervalEndTimeUtc = lastExecution.AddHours(1),
            };
            Assert.Equal(1, await store.Runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { row }, server, collectionTime, context, ct));
        }

        /* A live batch is stamped now: its day is today, which is never late. */
        var now = LiveClock.Now();
        await WriteAsync(now, 9160001, now.AddMinutes(-1));
        Assert.Equal(0L, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection, MarkedSql, ct, ("server_id", A)));

        /* A backfilled batch stamped in a built day marks that day, and only that day. */
        var lateDay = E.AddDays(-4);
        var lateAt = lateDay.AddHours(10).AddMinutes(33);
        /* The row's interval is today's: the wide interval table's apply marks the days of the rows' intervals (collection_time only when a row has
           none), so a row whose interval sat in the late day would be marked by that apply and the mark under test would add nothing. Only the
           batch's collection_time sits in the built day. */
        await WriteAsync(lateAt, 9160002, now.AddMinutes(-1));
        Assert.Equal(1L, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection, MarkedSql, ct, ("server_id", A)));
        Assert.Equal(1L, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            MarkedSql + " AND day_utc = @day", ct, ("server_id", A), ("day", lateDay)));
    }

    /// <summary>
    /// #9287: a day's clutter rollup is served only when it was built from raw that held the day whole. Raw's oldest row is inside the day after
    /// D, and D was never built, so its build takes no clutter from raw and must not stamp an empty rollup as built: the route reads raw for
    /// D, the reply is the raw one, and the next tick neither builds D again nor plans it.
    /// </summary>
    [Fact]
    public async Task ANeverBuiltDayRawNoLongerHoldsWhole_IsNotServedAsEmptyClutter_AndIsNotBuiltAgainEveryTick()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedServerAsync(store, A, E.AddDays(-10), 10, 4, 30, true, ct);
        var fleet = new[] { A };
        var w = new Window("reaches before the oldest raw row", E.AddDays(-10).AddHours(-1), E, null);
        var d = E.AddDays(-9);
        var dayKey = DateOnly.FromDateTime(d);
        Assert.False(QueryStoreClosedDayRollup.MayBeThinned(dayKey, QueryStoreClosedDayRollupLiveTests.TickNow, Retention), "the day is in the margin zone, so the test proves nothing");

        /* Raw's oldest row is a minute into the day after D: D and the day after it are not whole, the day after that is. */
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
            "DELETE FROM collect.query_store_stats WHERE server_id = @server_id AND collection_time < @cut", ct, ("server_id", A), ("cut", E.AddDays(-8).AddMinutes(1)));
        Assert.False(QueryStoreClosedDayRollup.StatsHoldDayWhole(E.AddDays(-8).AddMinutes(1), dayKey), "raw holds the day whole, so the test proves nothing");
        Assert.Equal(0L, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            "SELECT count(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", A), ("day", dayKey)));

        await QueryStoreClosedDayRollupLiveTests.TickAsync(store, Retention, ct);

        Assert.True(await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            "SELECT count(*) FROM collect.query_store_grid_daily WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", A), ("day", dayKey)) > 0, "the day's grid rollup was not built");
        Assert.Equal(0L, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            "SELECT count(*) FROM collect.query_store_clutter_plan_daily WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", A), ("day", dayKey)));
        Assert.False(Covers(await RouteAsync(store, fleet, w, ct), d), "the clutter route serves a day that was never built from whole raw");
        Assert.Equal(Lines(await ReadRawAsync(store, fleet, w, ct)), Lines(await ReadAsync(store, fleet, w, ct)));

        var builtAt = await QueryStoreClosedDayRollupLiveTests.ScalarAsync<DateTime>(store.Connection,
            "SELECT built_at FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", A), ("day", dayKey));
        var next = await QueryStoreClosedDayRollup.RunTickAsync(store.Postgres, QueryStoreClosedDayRollupLiveTests.TickNow, Retention, NullLogger.Instance, ct);
        Assert.Equal(0, next.Failed);
        Assert.Equal(0, next.Built);
        Assert.Equal(builtAt, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<DateTime>(store.Connection,
            "SELECT built_at FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", A), ("day", dayKey)));
    }

    /// <summary>
    /// #9287: a marked day whose clutter rows came from its previous build, with raw's oldest row inside the day after it, keeps those rows when
    /// it is built again and is still served as clutter, as collected.
    /// </summary>
    [Fact]
    public async Task AMarkedDayWithClutterFromItsPreviousBuild_KeepsItWhenRawNoLongerHoldsTheDayWhole()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedServerAsync(store, A, E.AddDays(-10), 10, 4, 30, true, ct);
        await QueryStoreClosedDayRollupLiveTests.TickAsync(store, Retention, ct);
        var fleet = new[] { A };
        var w = new Window("reaches before the oldest raw row", E.AddDays(-10).AddHours(-1), E, null);
        var d = E.AddDays(-9);
        var dayKey = DateOnly.FromDateTime(d);
        var whole = Lines(await ReadAsync(store, fleet, w, ct));
        Assert.True(Covers(await RouteAsync(store, fleet, w, ct), d), "the day is not served before the test starts");
        var clutterRows = await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            "SELECT count(*) FROM collect.query_store_clutter_plan_daily WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", A), ("day", dayKey));
        Assert.True(clutterRows > 0, "the day has no clutter rows to keep");

        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
            "DELETE FROM collect.query_store_stats WHERE server_id = @server_id AND collection_time < @cut", ct, ("server_id", A), ("cut", E.AddDays(-8).AddMinutes(1)));
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
            "UPDATE collect.query_store_closed_day_built SET rebuild_needed = true WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", A), ("day", dayKey));

        var tick = await QueryStoreClosedDayRollupLiveTests.TickAsync(store, Retention, ct);

        Assert.Equal(1, tick.Built);
        Assert.Equal(clutterRows, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection,
            "SELECT count(*) FROM collect.query_store_clutter_plan_daily WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", A), ("day", dayKey)));
        Assert.True(Covers(await RouteAsync(store, fleet, w, ct), d), "the day is no longer served as clutter");
        Assert.Equal(whole, Lines(await ReadAsync(store, fleet, w, ct)));
    }

    [Fact]
    public async Task ARawDayThatRetentionDropped_IsStillServedFromTheRollupAsCollected_WhetherRawNowStartsAtMidnightOrInsideADay()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedServerAsync(store, A, E.AddDays(-10), 10, 4, 30, true, ct);
        await QueryStoreClosedDayRollupLiveTests.TickAsync(store, Retention, ct);

        var w = new Window("reaches before the oldest raw row", E.AddDays(-10).AddHours(-1), E, null);
        var fleet = new[] { A };
        var whole = Lines(await ReadAsync(store, fleet, w, ct));
        Assert.Equal(E.AddDays(-10), (await RouteAsync(store, fleet, w, ct))!.Value.Start);

        /* #9287: raw's oldest row is now a day's first instant. The built days before it are still the rollup's, as collected, until the
           day itself is past retention. Raw alone now answers with less. */
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
            "DELETE FROM collect.query_store_stats WHERE server_id = @server_id AND collection_time < @cut", ct, ("server_id", A), ("cut", E.AddDays(-8)));
        var atMidnight = Lines(await ReadRawAsync(store, fleet, w, ct));
        Assert.NotEqual(whole, atMidnight);
        Assert.Equal(E.AddDays(-10), (await RouteAsync(store, fleet, w, ct))!.Value.Start);
        Assert.Equal(whole, Lines(await ReadAsync(store, fleet, w, ct)));

        /* raw's oldest row is inside a day: the answer from the rollup does not change either */
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
            "DELETE FROM collect.query_store_stats WHERE server_id = @server_id AND collection_time < @cut", ct, ("server_id", A), ("cut", E.AddDays(-8).AddMinutes(1)));
        var insideDay = Lines(await ReadRawAsync(store, fleet, w, ct));
        Assert.NotEqual(atMidnight, insideDay);
        Assert.Equal(E.AddDays(-10), (await RouteAsync(store, fleet, w, ct))!.Value.Start);
        Assert.Equal(whole, Lines(await ReadAsync(store, fleet, w, ct)));
    }

    [Fact]
    public async Task TheClosedDayStatements_ReadFewerBuffersThanTheRawOnes_ForAWeek()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);

        /* The production order and shape (see QueryStoreClutterChurnPlanLiveTests): hypertable where TimescaleDB is present, then the tuning
           list that makes the covering index, then raw rows and a visibility map that lets an index-only walk skip the heap. */
        var connectionString = new NpgsqlConnectionStringBuilder(store.Scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(connection, "DELETE FROM collect.query_store_stats WHERE server_id = @server_id", ct, ("server_id", A));
        if (store.Scratch.TimescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }

        await PgTableTuning.ApplyAsync(connection, null, ct);
        await SeedServerAsync(store, A, E.AddDays(-8), 8, 96, 300, true, ct);
        await LiveVisibilityMap.VacuumUntilAllVisibleAsync(connection, "query_store_stats", ct);
        await QueryStoreClosedDayRollupLiveTests.TickAsync(store, Retention, ct);
        /* Autovacuum has analyzed the rollups in production before a read plans against them. */
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(connection, "ANALYZE collect.query_store_clutter_plan_daily; ANALYZE collect.query_store_clutter_collections_daily;", ct);

        var start = E.AddDays(-7);
        var served = await DarlingQueryStoreClutterReader.ResolveClosedDaysAsync(store.Postgres, [A], start, E, ct);
        Assert.NotNull(served);

        async Task<long> PagesAsync(string sql, bool closedDays)
        {
            await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = new[] { A } });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(E, DateTimeKind.Unspecified) });
            command.Parameters.Add(DatabaseFilter.All.Parameter());
            if (closedDays)
            {
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(served!.Value.Start, DateTimeKind.Unspecified) });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(served.Value.End, DateTimeKind.Unspecified) });
            }

            return ExplainBuffers.DataPages(await command.ExecuteScalarAsync(ct));
        }

        var rawPages = await PagesAsync(DarlingQueryStoreClutterReader.PlanChurnSql, false) + await PagesAsync(DarlingQueryStoreClutterReader.PlanChurnCollectionsSql, false);
        var closedPages = await PagesAsync(DarlingQueryStoreClutterReader.PlanChurnClosedDaysSql, true) + await PagesAsync(DarlingQueryStoreClutterReader.PlanChurnCollectionsClosedDaysSql, true);

        Assert.True(closedPages * 3 < rawPages, $"the closed-day statements read {closedPages} pages, the raw ones read {rawPages}");
    }

    [Fact]
    public async Task TheRung_AppliedTwice_ChangesNothing_AndLeavesTheBuiltDaysAndTheirRowsAlone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedServerAsync(store, A, E.AddDays(-10), 10, 4, 30, true, ct);
        await QueryStoreClosedDayRollupLiveTests.TickAsync(store, Retention, ct);

        const string Digest = @"
SELECT (SELECT md5(string_agg(t::text, '|' ORDER BY t::text)) FROM collect.query_store_clutter_plan_daily AS t) || '/' ||
       (SELECT md5(string_agg(t::text, '|' ORDER BY t::text)) FROM collect.query_store_clutter_collections_daily AS t) || '/' ||
       (SELECT md5(string_agg(t::text, '|' ORDER BY t::text)) FROM collect.query_store_closed_day_built AS t)";
        var before = await QueryStoreClosedDayRollupLiveTests.ScalarAsync<string>(store.Connection, Digest, ct);
        Assert.True(await QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection, "SELECT count(*) FROM collect.query_store_clutter_plan_daily", ct) > 0);

        var rung = PgMigrations.Scripts.Single(m => m.Version == QueryStoreClosedDayRollup.ClutterVersion);
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection, rung.Sql, ct);
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection, rung.Sql, ct);

        Assert.Equal(before, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<string>(store.Connection, Digest, ct));
    }
}
