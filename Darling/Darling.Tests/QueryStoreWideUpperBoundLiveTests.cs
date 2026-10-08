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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5523: the per-server reads of <c>collect.query_store_interval_wide</c> now bound <c>first_execution_time</c> at the
/// window's end as well as its start, so <c>ix_query_store_interval_wide_server_first_exec</c> scans a finite range. This
/// pins that no row is lost: rows straddle every edge of the window (an interval that began before the window and is
/// still running, a row collected exactly at each edge, a row whose first execution is just past the end because the
/// monitored server's clock runs ahead, an interval that spans a whole day past the end of a window placed by its start),
/// and each statement returns exactly what the same statement returns with the new bound cut out. One row sits past the
/// slack on purpose, and each bound has a row one minute outside it: the bounds are live, and those are the only rows the new text drops.
/// </summary>
/* #1776 own-store: reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and then
   works entirely inside it, the same shape as QueryStoreBackgroundIndexesLiveTests. */
public sealed class QueryStoreWideUpperBoundLiveTests
{
    private const int TopServer = 51;
    private const int TrendServer = 52;

    private static readonly DateTime W0 = new(2026, 9, 10, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime W1 = new(2026, 9, 12, 18, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime S = new(2026, 9, 11, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime E = new(2026, 9, 12, 0, 0, 0, DateTimeKind.Unspecified);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* (query_id, collection_time, first_execution_time) for TopServer. Every row is its own interval (distinct interval id, plan
       id = query id), so one row is one group in the top reads. */
    private static readonly (int Q, DateTime C, DateTime F)[] TopRows =
    {
        (1, W0.AddHours(1), W0.AddHours(-20)),         // began before the window, still running
        (2, W0, W0.AddHours(-25).AddMinutes(-30)),     // collected exactly at the start, inside the 26 h floor
        (3, W1, W1.AddHours(-1)),                      // collected exactly at the end
        (4, W1, W1.AddHours(12).AddMinutes(-1)),       // monitored clock 11 h 59 min ahead: inside the 12 h slack
        (5, W1.AddMinutes(-1), W1.AddMinutes(30)),
        (6, W0.AddMinutes(-1), W0.AddHours(-1)),       // collected just before the window
        (7, W1.AddMinutes(1), W1),                     // collected just after the window
        (8, W1.AddDays(3), W1.AddDays(3)),             // newer rows the old range walked and filtered out
        (9, W0.AddHours(5), W0.AddHours(4)),           // ordinary
        (10, new DateTime(2026, 9, 11, 12, 0, 0), new DateTime(2026, 9, 11, 11, 0, 0)),  // the middle day
        (11, W1, W1.AddHours(12).AddMinutes(1)),       // past the slack: dropped by every upper bound
        (12, S.AddMinutes(-1), S.AddHours(12).AddMinutes(-1)),  // collected just before the long-window read's first edge, first execution inside the slack past it
        (13, S.AddMinutes(-1), S.AddHours(12).AddMinutes(1)),   // the same, one minute past the slack: only the long-window read's first edge drops it
    };

    /* (query_id, interval start, first_execution_time, collection_time) for TrendServer; start null is a legacy row. */
    private static readonly (int Q, DateTime? Start, DateTime F, DateTime C)[] TrendRows =
    {
        (21, W0, W0, W0.AddMinutes(30)),                                        // starts at the window start
        (22, W0.AddMinutes(10), W0.AddMinutes(-59), W0.AddHours(1)),            // first execution 59 minutes below the window start: inside the 1 h floor
        (23, W1, W1.AddHours(24), W1.AddHours(25)),                             // starts at the end, spans the whole day
        (24, W1.AddMinutes(-10), W1.AddHours(23).AddMinutes(50), W1.AddHours(24)),
        (25, W0.AddMinutes(-1), W0.AddMinutes(-1), W0.AddMinutes(10)),          // starts just before the window
        (26, W1.AddMinutes(1), W1.AddMinutes(1), W1.AddMinutes(5)),             // starts just after the window
        (27, W0.AddHours(1), W0.AddHours(1), W0.AddHours(1)),
        (30, W0.AddMinutes(20), W0.AddMinutes(-61), W0.AddHours(1)),            // first execution 61 minutes below the window start: past the floor, dropped
        (28, null, W0.AddHours(1), W0.AddHours(2)),                             // legacy rows
        (29, null, W1.AddHours(12).AddMinutes(-1), W1.AddMinutes(-5)),
        (31, null, W1.AddHours(12).AddMinutes(1), W1.AddMinutes(-3)),           // legacy, one minute past the slack: dropped
    };

    private static readonly int[] ExpectedTop = { 1, 2, 3, 4, 5, 9, 10, 12, 13 };
    private static readonly int[] ExpectedDaily = { 1, 2, 3, 4, 5, 9, 12 };
    private static readonly DateTime[] ExpectedTrendPoints =
    {
        W0, W0.AddMinutes(10), W0.AddHours(1), W0.AddHours(2), W1.AddMinutes(-10), W1.AddMinutes(-5), W1,
    };

    /* What the same trend statement returns with its new bounds cut out: the two rows the bounds exist to drop come back. */
    private static readonly DateTime[] OldTrendPoints =
    {
        W0, W0.AddMinutes(10), W0.AddMinutes(20), W0.AddHours(1), W0.AddHours(2), W1.AddMinutes(-10), W1.AddMinutes(-5), W1.AddMinutes(-3), W1,
    };

    [Fact]
    public async Task EveryReadWithTheUpperBound_ReturnsWhatItReturnedWithoutIt_AndDropsOnlyARowPastTheSlack()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5523 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);

        /* ---- MCP top: the table read. ---- */
        var mcpNew = DarlingDataReader.QueryStoreTopTableSql;
        var mcpOld = Cut(mcpNew, @"\s*AND\s+first_execution_time <= \$3 \+ interval '\d+ minutes'");
        var mcpTop = await TopIdsAsync(connection, mcpNew, ct);
        Assert.Equal(ExpectedTop, mcpTop);
        Assert.Equal(ExpectedTop.Append(11).OrderBy(q => q), await TopIdsAsync(connection, mcpOld, ct));

        /* ---- The long-window read: only the two edge arms read the wide table; no daily row is built, so the middle
           days contribute nothing, and the edges are what the bound could clip. ---- */
        var dailyNew = DarlingDataReader.QueryStoreTopDailyTableSql;
        var dailyOld = Cut(Cut(dailyNew, @"\s*AND\s+first_execution_time < \$8::date \+ interval '\d+ minutes'"),
            @"\s*AND\s+first_execution_time <= \$3 \+ interval '\d+ minutes'");
        var dailyIds = await DailyIdsAsync(connection, dailyNew, ct);
        Assert.Equal(ExpectedDaily, dailyIds);
        Assert.Equal(ExpectedDaily.Append(11).Append(13).OrderBy(q => q), await DailyIdsAsync(connection, dailyOld, ct));

        /* ---- The Queries grid's table read: a closed end, and the open end a preset window binds as NULL. ---- */
        var gridNew = ViewerDataService.QueryStoreTopTableSql;
        var gridOld = Cut(gridNew, @"\s*AND\s+\(\$3::timestamp IS NULL OR first_execution_time <= \$3 \+ interval '\d+ minutes'\)");
        Assert.Equal(ExpectedTop, await GridIdsAsync(connection, gridNew, W1, ct));
        Assert.Equal(ExpectedTop.Append(11).OrderBy(q => q), await GridIdsAsync(connection, gridOld, W1, ct));
        var openEndNew = await GridIdsAsync(connection, gridNew, null, ct);
        Assert.Equal(await GridIdsAsync(connection, gridOld, null, ct), openEndNew);
        Assert.Contains(8, openEndNew);
        Assert.Contains(11, openEndNew);

        /* ---- The history tool's table read, one query at a time. ---- */
        var historyNew = DarlingMcpQueryStoreHistoryTools.HistoryTableSql;
        var historyOld = Cut(historyNew, @"\s*AND\s+first_execution_time <= \$5 \+ interval '\d+ minutes'");
        foreach (var q in TopRows.Select(r => r.Q))
        {
            var viaNew = await HistoryPlanIdsAsync(connection, historyNew, q, ct);
            var viaOld = await HistoryPlanIdsAsync(connection, historyOld, q, ct);
            if (q == 11)
            {
                Assert.Empty(viaNew);
                Assert.Single(viaOld);
            }
            else
            {
                Assert.Equal(viaOld, viaNew);
            }

            Assert.Equal(ExpectedTop.Contains(q), viaNew.Count == 1);
        }

        /* ---- The duration trend's table read: both arms. ---- */
        var trendNew = ViewerDataService.QueryStoreDurationTrendTableSql;
        var trendOld = trendNew
            .Replace($"first_execution_time >= $2 - {QueryStoreIntervalWide.IntervalStartSlackSql}", $"first_execution_time >= $2 - {QueryStoreIntervalWide.PurgeEdgeMarginSql}", StringComparison.Ordinal);
        trendOld = Cut(Cut(trendOld, @"\s*AND\s+first_execution_time <= \$3 \+ interval '\d+ minutes'"),
            @"\s*AND\s+first_execution_time <= \$4 \+ interval '\d+ minutes'");
        Assert.NotEqual(trendNew, trendOld);
        var points = await TrendPointsAsync(connection, trendNew, ct);
        Assert.Equal(ExpectedTrendPoints, points);
        Assert.Equal(OldTrendPoints, await TrendPointsAsync(connection, trendOld, ct));
    }

    /// <summary>The statement with one regex match cut out; the match must exist, or the "old" text would silently equal the new.</summary>
    private static string Cut(string sql, string pattern)
    {
        var regex = new Regex(pattern);
        Assert.Single(regex.Matches(sql));
        var cut = regex.Replace(sql, string.Empty);
        Assert.NotEqual(sql, cut);
        return cut;
    }

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var (q, c, f) in TopRows)
        {
            await InsertAsync(connection, TopServer, q, c, f, q, c.AddMinutes(-5), ct);
        }

        foreach (var (q, start, f, c) in TrendRows)
        {
            await InsertAsync(connection, TrendServer, q, c, f, start is null ? -q : q, start, ct);
        }

        await using var analyze = new NpgsqlCommand("ANALYZE collect.query_store_interval_wide", connection);
        await analyze.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAsync(NpgsqlConnection connection, int server, int q, DateTime c, DateTime f, long intervalId, DateTime? start, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_wide
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
VALUES (@c, @server, 'db', @q, @q, 'Regular', @f, @c, 'select 1', 10, 1000, @interval, @start)", connection);
        command.Parameters.AddWithValue("c", NpgsqlDbType.Timestamp, c);
        command.Parameters.AddWithValue("server", server);
        command.Parameters.AddWithValue("q", (long)q);
        command.Parameters.AddWithValue("f", NpgsqlDbType.Timestamp, f);
        command.Parameters.AddWithValue("interval", intervalId);
        command.Parameters.Add(new NpgsqlParameter("start", NpgsqlDbType.Timestamp) { Value = (object?)start ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static NpgsqlParameter Ts(DateTime value) => new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp };

    private static NpgsqlParameter Text() => new() { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value };

    private static async Task<List<int>> IdsAsync(NpgsqlCommand command, string column, CancellationToken ct)
    {
        await using var reader = await command.ExecuteReaderAsync(ct);
        var ordinal = reader.GetOrdinal(column);
        var ids = new List<int>();
        while (await reader.ReadAsync(ct))
        {
            ids.Add((int)reader.GetInt64(ordinal));
        }

        ids.Sort();
        return ids;
    }

    private static async Task<List<int>> TopIdsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopServer });
        command.Parameters.Add(Ts(W0));
        command.Parameters.Add(Ts(W1));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 500 });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.Add(Text());
        command.Parameters.Add(Text());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FirstCandidates(500) });
        return await IdsAsync(command, "query_id", ct);
    }

    private static async Task<List<int>> DailyIdsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopServer });
        command.Parameters.Add(Ts(W0));
        command.Parameters.Add(Ts(W1));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 500 });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.Add(Text());
        command.Parameters.Add(Text());
        command.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = DateOnly.FromDateTime(S), NpgsqlDbType = NpgsqlDbType.Date });
        command.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = DateOnly.FromDateTime(E), NpgsqlDbType = NpgsqlDbType.Date });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FirstCandidates(500) });
        return await IdsAsync(command, "query_id", ct);
    }

    private static async Task<List<int>> GridIdsAsync(NpgsqlConnection connection, string sql, DateTime? end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopServer });
        command.Parameters.Add(Ts(W0));
        command.Parameters.Add(end is DateTime e
            ? Ts(e)
            : new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 500 });
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FirstCandidates(500) });
        return await IdsAsync(command, "query_id", ct);
    }

    private static async Task<List<long>> HistoryPlanIdsAsync(NpgsqlConnection connection, string sql, int q, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopServer });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "db" });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = q });
        command.Parameters.Add(Ts(W0));
        command.Parameters.Add(Ts(W1));
        await using var reader = await command.ExecuteReaderAsync(ct);
        var ids = new List<long>();
        while (await reader.ReadAsync(ct))
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids;
    }

    private static async Task<List<DateTime>> TrendPointsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TrendServer });
        command.Parameters.Add(Ts(W0));
        command.Parameters.Add(Ts(W1));
        command.Parameters.Add(Ts(W1));
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        await using var reader = await command.ExecuteReaderAsync(ct);
        var points = new List<DateTime>();
        while (await reader.ReadAsync(ct))
        {
            points.Add(reader.GetDateTime(0));
        }

        return points;
    }
}
