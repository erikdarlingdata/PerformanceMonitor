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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5882: the per-server first_execution_time floor of the 24-hour Query Store top read. The floor reaches
/// <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/> (26 hours) before the window because an interval may span a day;
/// <see cref="QueryStoreIntervalWide.ResolveEdgeMarginAsync"/> shrinks it to the server's own longest interval. Pins that
/// the narrower floor returns exactly the rows, in the order, the 26-hour floor returns on a server of 60-minute intervals
/// and on a server of 2-hour intervals one of which straddles the window start; that the floor falls back to 26 hours
/// whenever the server's interval length cannot be trusted, including a database with rows in range and no health row; and that a floor that is too tight really would lose the
/// straddling plan (so the equality above is not vacuous).
/// </summary>
/* #1776 own-store: reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and then
   works entirely inside it, the same shape as QueryStoreTopServerWideReadLiveTests. */
public sealed class QueryStoreTopEdgeMarginLiveTests
{
    private const int Hourly = 1;
    private const int TwoHour = 2;
    private const int NoHealth = 3;
    private const int MixedHealth = 4;
    private const int YoungHealth = 5;
    private const int UnreportedDatabase = 6;
    private const int ZeroLengthWithoutWideRows = 7;
    private const int ZeroLengthWithWideRows = 8;
    private const int Top = 20;
    private const long StraddlerPlan = 9999;
    private const long UnreportedStraddlerPlan = 9998;

    private static readonly DateTime FirstHour = new(2026, 10, 12, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = new(2026, 10, 15, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheNarrowFloor_ReturnsTheWideFloorsRows_OnHourlyAndStraddlingTwoHourServers_AndFallsBackWhenTheLengthIsUnknown()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5882 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);

        var wide = DarlingDataReader.QueryStoreTopTableSql;

        /* The resolved floors: three hours at 60-minute intervals, four at 120, the full 26 wherever the length is unknown. */
        var hourlyMargin = await QueryStoreIntervalWide.ResolveEdgeMarginAsync(connection, Hourly, WindowStart, WindowEnd, 60, ct);
        var twoHourMargin = await QueryStoreIntervalWide.ResolveEdgeMarginAsync(connection, TwoHour, WindowStart, WindowEnd, 60, ct);
        Assert.Equal(TimeSpan.FromHours(3), hourlyMargin);
        Assert.Equal(TimeSpan.FromHours(4), twoHourMargin);
        foreach (var unknown in new[] { NoHealth, MixedHealth, YoungHealth, UnreportedDatabase })
        {
            Assert.Equal(
                QueryStoreIntervalWide.PurgeEdgeMargin,
                await QueryStoreIntervalWide.ResolveEdgeMarginAsync(connection, unknown, WindowStart, WindowEnd, 60, ct));
        }

        /* Same rows, same order, against the 26-hour floor, on every server. */
        foreach (var (server, margin) in new[] { (Hourly, hourlyMargin), (TwoHour, twoHourMargin) })
        {
            var narrow = QueryStoreIntervalWide.WithEdgeMargin(wide, QueryStoreIntervalWide.EdgeMarginSqlFor(margin));
            Assert.NotEqual(wide, narrow);
            var wideRows = await RunAsync(connection, wide, server, ct);
            var narrowRows = await RunAsync(connection, narrow, server, ct);
            Assert.Equal(Top, wideRows.Count);
            Assert.Equal(wideRows, narrowRows);
        }

        /* The straddling plan: its only interval began at 10:00, two hours before the window, and was collected at 12:02. */
        var straddlerKey = (await RunAsync(connection, QueryStoreIntervalWide.WithEdgeMargin(wide, QueryStoreIntervalWide.EdgeMarginSqlFor(twoHourMargin)), TwoHour, ct))[0];
        Assert.StartsWith($"a|{StraddlerPlan}|{StraddlerPlan}|", straddlerKey, StringComparison.Ordinal);

        /* A floor of one hour reaches only to 11:00 and loses it, so the equality above is a real check of the bound. */
        var tooTight = QueryStoreIntervalWide.WithEdgeMargin(wide, QueryStoreIntervalWide.EdgeMarginSqlFor(TimeSpan.FromHours(1)));
        var tightRows = await RunAsync(connection, tooTight, TwoHour, ct);
        Assert.DoesNotContain(tightRows, r => r.StartsWith($"a|{StraddlerPlan}|", StringComparison.Ordinal));

        /* A database with wide rows in range and no health row: server 6 has a 60-minute database "a" with health rows and a
           2-hour database "c" with none. Without the guard the longest reported length is 60 minutes, a 3-hour floor, which loses
           c's straddling plan (first execution 08:30, collected 12:02); with it the floor is the full 26 hours and the read returns
           the plan. */
        var unreportedMargin = await QueryStoreIntervalWide.ResolveEdgeMarginAsync(connection, UnreportedDatabase, WindowStart, WindowEnd, 60, ct);
        Assert.Equal(QueryStoreIntervalWide.PurgeEdgeMargin, unreportedMargin);
        var unguardedMargin = QueryStoreIntervalWide.EdgeMarginFor(TimeSpan.FromMinutes(60));
        Assert.Equal(TimeSpan.FromHours(3), unguardedMargin);
        var unguardedRows = await RunAsync(connection, QueryStoreIntervalWide.WithEdgeMargin(wide, QueryStoreIntervalWide.EdgeMarginSqlFor(unguardedMargin)), UnreportedDatabase, ct);
        Assert.DoesNotContain(unguardedRows, r => r.StartsWith($"c|{UnreportedStraddlerPlan}|", StringComparison.Ordinal));
        var guardedRows = await RunAsync(connection, QueryStoreIntervalWide.WithEdgeMargin(wide, QueryStoreIntervalWide.EdgeMarginSqlFor(unreportedMargin)), UnreportedDatabase, ct);
        Assert.StartsWith($"c|{UnreportedStraddlerPlan}|{UnreportedStraddlerPlan}|", guardedRows[0], StringComparison.Ordinal);
        Assert.Equal(await RunAsync(connection, wide, UnreportedDatabase, ct), guardedRows);

        /* A non-positive length counts only for a database that has wide rows. Server 7: hourly database "a" with a 60-minute
           health row, plus a tempdb-style health row of length 0 with no wide rows: the 3-hour margin, not 26. Server 8: the
           same, but the zero-length database "b" owns wide rows, so its length is unknown and the read keeps the full 26 hours. */
        Assert.Equal(TimeSpan.FromHours(3),
            await QueryStoreIntervalWide.ResolveEdgeMarginAsync(connection, ZeroLengthWithoutWideRows, WindowStart, WindowEnd, 60, ct));
        Assert.Equal(QueryStoreIntervalWide.PurgeEdgeMargin,
            await QueryStoreIntervalWide.ResolveEdgeMarginAsync(connection, ZeroLengthWithWideRows, WindowStart, WindowEnd, 60, ct));
    }

    /// <summary>
    /// The Viewer's Queries grid (<c>ViewerDataService.TryGetQueryStoreTopQueriesFromTableAsync</c>, reached through
    /// <c>GetQueryStoreTopQueriesWithReachAsync</c>) resolves the margin with <c>literalEndUtc ?? DateTime.UtcNow</c>. For an
    /// open-ended preset and for a custom range, the narrowed read returns the rows, in the order, the 26-hour floor returns
    /// (the same store once its health rows are gone), and it really ran the narrowed statement: a row the 26-hour floor admits
    /// and the 4-hour floor drops is missing from the narrowed read.
    /// </summary>
    [Fact]
    public async Task TheViewerGrid_WithANarrowedMargin_ReturnsTheWideFloorsRows_ForAnOpenEndedPresetAndACustomRange()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5882 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);

        /* The gate's inputs: a coverage claim from the start of the year, applied through the same instant. */
        await ExecAsync(connection, $"""
            INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({Hourly}, 'srv{Hourly}', TRUE), ({TwoHour}, 'srv{TwoHour}', TRUE);
            INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through)
            VALUES ({Hourly}, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00'),
                   ({TwoHour}, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00')
            """, ct);

        Assert.Equal(TimeSpan.FromHours(4), await QueryStoreIntervalWide.ResolveEdgeMarginAsync(connection, TwoHour, WindowStart, WindowEnd, 60, ct));

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        async Task<List<string>> GridAsync(DateTime? literalEnd)
        {
            var (rows, plan) = await viewer.GetQueryStoreTopQueriesWithReachAsync(TwoHour, WindowStart, WindowEnd, Top, null, literalEnd, cancellationToken: ct);
            Assert.NotNull(plan);   /* the table served, so the margin was resolved */
            return rows.Select(r => string.Join('|', r.DatabaseName, r.QueryId, r.PlanId, r.AvgDurationMs, r.TotalExecutions)).ToList();
        }

        var narrowOpen = await GridAsync(null);
        var narrowCustom = await GridAsync(WindowEnd);
        Assert.Equal(Top, narrowOpen.Count);
        Assert.StartsWith($"a|{StraddlerPlan}|{StraddlerPlan}|", narrowOpen[0], StringComparison.Ordinal);

        /* A row the 4-hour floor drops and the 26-hour floor admits: collected 13:02, first executed 20 hours earlier. It breaks
           the invariant a 2-hour server keeps, so only the narrowed statement can miss it; it ranks first when read. */
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_interval_wide
            (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
             last_execution_time, query_hash, query_plan_hash, execution_count, avg_duration_us, avg_cpu_time_us,
             avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, runtime_stats_interval_id, interval_start_time_utc)
            VALUES (timestamp '{WindowStart:yyyy-MM-dd} 13:02', {TwoHour}, 'a', 9990, 9990, 'Regular',
                    timestamp '{WindowStart.AddHours(-20):yyyy-MM-dd HH:mm}', timestamp '{WindowStart:yyyy-MM-dd} 12:55', 'qhx', 'phx', 2000, 90000000, 1, 1, 0, 0, 1, 2,
                    timestamp '{WindowStart.AddHours(-20):yyyy-MM-dd HH:mm}')
            """, ct);
        Assert.Equal(narrowOpen, await GridAsync(null));
        Assert.Equal(narrowCustom, await GridAsync(WindowEnd));

        /* No health rows: the server falls back to the 26-hour floor, the old read. */
        await ExecAsync(connection, $"DELETE FROM collect.query_store_health WHERE server_id = {TwoHour}", ct);
        Assert.Equal(QueryStoreIntervalWide.PurgeEdgeMargin, await QueryStoreIntervalWide.ResolveEdgeMarginAsync(connection, TwoHour, WindowStart, WindowEnd, 60, ct));
        foreach (var (wideRows, narrowRows) in new[] { (await GridAsync(null), narrowOpen), (await GridAsync(WindowEnd), narrowCustom) })
        {
            Assert.StartsWith("a|9990|9990|", wideRows[0], StringComparison.Ordinal);
            Assert.Equal(narrowRows.Take(Top - 1), wideRows.Skip(1));
        }
    }

    private static async Task<List<string>> RunAsync(NpgsqlConnection connection, string sql, int server, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = server });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowStart, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowEnd, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FinalCandidates(Top) });
        await using var reader = await command.ExecuteReaderAsync(ct);
        var (rows, _) = await TopFill.ReadPageAsync(reader, reader.GetOrdinal("page_ord"), r => string.Join('|',
            r.GetString(r.GetOrdinal("database_name")), r.GetInt64(r.GetOrdinal("query_id")), r.GetInt64(r.GetOrdinal("plan_id")),
            r.GetDouble(r.GetOrdinal("avg_duration_ms")), r.GetInt64(r.GetOrdinal("total_executions"))), ct);
        return rows;
    }

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* Two databases a and b, 30 plans each, tied costs on purpose (duration depends on plan % 5). Servers 1, 3, 4 and 5 write
           one row per plan per hourly interval, collected two minutes after it closes. Server 2 writes 2-hour intervals starting on
           even hours, collected two minutes after they close, so the 10:00 interval on the 14th (first execution 10:00 to 10:49)
           is collected at 12:02, inside the window, with every first execution before it. Plan 9999 exists only in that interval. */
        const string columns = """
            (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
             last_execution_time, query_hash, query_plan_hash, execution_count, avg_duration_us, avg_cpu_time_us,
             avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, runtime_stats_interval_id, interval_start_time_utc)
            """;
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_interval_wide {columns}
            SELECT t.h + interval '62 minutes', s.s, d.d, p.p, p.p, 'Regular', t.h + (p.p % 50) * interval '1 minute',
                   t.h + interval '55 minutes', 'qh' || p.p, 'ph' || p.p, 10 + p.p % 7, 1000 * (p.p % 5) + 100, 100 * p.p, p.p, 0, 0, 1,
                   CAST(extract(epoch FROM t.h) AS bigint) / 3600, t.h
            FROM generate_series(timestamp '{FirstHour:yyyy-MM-dd HH:mm}', timestamp '{WindowEnd.AddHours(-1):yyyy-MM-dd HH:mm}', interval '1 hour') AS t(h)
            CROSS JOIN (VALUES ({Hourly}), ({NoHealth}), ({MixedHealth}), ({YoungHealth})) AS s(s)
            CROSS JOIN (VALUES ('a'), ('b')) AS d(d)
            CROSS JOIN generate_series(1, 30) AS p(p)
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_interval_wide {columns}
            SELECT t.h + interval '122 minutes', {TwoHour}, d.d, p.p, p.p, 'Regular', t.h + (p.p % 50) * interval '1 minute',
                   t.h + interval '115 minutes', 'qh' || p.p, 'ph' || p.p, 10 + p.p % 7, 1000 * (p.p % 5) + 100, 100 * p.p, p.p, 0, 0, 1,
                   CAST(extract(epoch FROM t.h) AS bigint) / 7200, t.h
            FROM generate_series(timestamp '{FirstHour:yyyy-MM-dd HH:mm}', timestamp '{WindowEnd.AddHours(-2):yyyy-MM-dd HH:mm}', interval '2 hours') AS t(h)
            CROSS JOIN (VALUES ('a'), ('b')) AS d(d)
            CROSS JOIN generate_series(1, 30) AS p(p)
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_interval_wide {columns}
            VALUES (timestamp '{WindowStart:yyyy-MM-dd} 12:02', {TwoHour}, 'a', {StraddlerPlan}, {StraddlerPlan}, 'Regular',
                    timestamp '{WindowStart:yyyy-MM-dd} 10:00', timestamp '{WindowStart:yyyy-MM-dd} 11:55', 'qhs', 'phs', 1000, 90000000, 1, 1, 0, 0, 1, 1,
                    timestamp '{WindowStart:yyyy-MM-dd} 10:00')
            """, ct);

        /* Server 6: hourly database "a" (health row, 60 minutes) and 2-hour database "c" (no health row at all), same shape as
           servers 1 and 2. c's plan 9998 exists only in an interval collected at 12:02 whose first execution was 08:30. */
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_interval_wide {columns}
            SELECT t.h + interval '62 minutes', {UnreportedDatabase}, 'a', p.p, p.p, 'Regular', t.h + (p.p % 50) * interval '1 minute',
                   t.h + interval '55 minutes', 'qh' || p.p, 'ph' || p.p, 10 + p.p % 7, 1000 * (p.p % 5) + 100, 100 * p.p, p.p, 0, 0, 1,
                   CAST(extract(epoch FROM t.h) AS bigint) / 3600, t.h
            FROM generate_series(timestamp '{FirstHour:yyyy-MM-dd HH:mm}', timestamp '{WindowEnd.AddHours(-1):yyyy-MM-dd HH:mm}', interval '1 hour') AS t(h)
            CROSS JOIN generate_series(1, 30) AS p(p)
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_interval_wide {columns}
            SELECT t.h + interval '122 minutes', {UnreportedDatabase}, 'c', p.p, p.p, 'Regular', t.h + (p.p % 50) * interval '1 minute',
                   t.h + interval '115 minutes', 'qh' || p.p, 'ph' || p.p, 10 + p.p % 7, 1000 * (p.p % 5) + 100, 100 * p.p, p.p, 0, 0, 1,
                   CAST(extract(epoch FROM t.h) AS bigint) / 7200, t.h
            FROM generate_series(timestamp '{FirstHour:yyyy-MM-dd HH:mm}', timestamp '{WindowEnd.AddHours(-2):yyyy-MM-dd HH:mm}', interval '2 hours') AS t(h)
            CROSS JOIN generate_series(1, 30) AS p(p)
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_interval_wide {columns}
            VALUES (timestamp '{WindowStart:yyyy-MM-dd} 12:02', {UnreportedDatabase}, 'c', {UnreportedStraddlerPlan}, {UnreportedStraddlerPlan}, 'Regular',
                    timestamp '{WindowStart:yyyy-MM-dd} 08:30', timestamp '{WindowStart:yyyy-MM-dd} 11:55', 'qhu', 'phu', 1000, 90000000, 1, 1, 0, 0, 1, 1,
                    timestamp '{WindowStart:yyyy-MM-dd} 08:30')
            """, ct);

        /* Servers 7 and 8: hourly wide rows for database "a" (and "b" on server 8), one plan, enough for the skip scan to see the
           databases; the read is never run on them, only the margin is resolved. */
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_interval_wide {columns}
            SELECT t.h + interval '62 minutes', s.s, s.d, 1, 1, 'Regular', t.h, t.h + interval '55 minutes', 'qh1', 'ph1', 10, 1100, 100, 1, 0, 0, 1,
                   CAST(extract(epoch FROM t.h) AS bigint) / 3600, t.h
            FROM generate_series(timestamp '{FirstHour:yyyy-MM-dd HH:mm}', timestamp '{WindowEnd.AddHours(-1):yyyy-MM-dd HH:mm}', interval '1 hour') AS t(h)
            CROSS JOIN (VALUES ({ZeroLengthWithoutWideRows}, 'a'), ({ZeroLengthWithWideRows}, 'a'), ({ZeroLengthWithWideRows}, 'b')) AS s(s, d)
            """, ct);

        /* Health captures: one per database every six hours from two days before the first interval. Server 1: 60, server 2: 120,
           server 4: 60 for a and 1440 for b, server 5: first capture at the window start, server 3: none, server 6: 60 for a only, server 7: 60 for a and 0 for tempdb (no wide rows), server 8: 60 for a and 0 for b (wide rows). */
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, interval_length_minutes)
            SELECT row_number() OVER (), c.c, s.s, 'srv' || s.s, s.d_, s.len
            FROM generate_series(timestamp '{FirstHour.AddDays(-2):yyyy-MM-dd HH:mm}', timestamp '{WindowEnd:yyyy-MM-dd HH:mm}', interval '6 hours') AS c(c)
            CROSS JOIN (VALUES ({Hourly}, 60, 'a'), ({Hourly}, 60, 'b'), ({TwoHour}, 120, 'a'), ({TwoHour}, 120, 'b'),
                               ({MixedHealth}, 60, 'a'), ({MixedHealth}, 1440, 'b'), ({UnreportedDatabase}, 60, 'a'),
                               ({ZeroLengthWithoutWideRows}, 60, 'a'), ({ZeroLengthWithoutWideRows}, 0, 'tempdb'),
                               ({ZeroLengthWithWideRows}, 60, 'a'), ({ZeroLengthWithWideRows}, 0, 'b')) AS s(s, len, d_)
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, interval_length_minutes)
            SELECT 100000 + row_number() OVER (), c.c, {YoungHealth}, 'srv{YoungHealth}', 'a', 60
            FROM generate_series(timestamp '{WindowStart:yyyy-MM-dd HH:mm}', timestamp '{WindowEnd:yyyy-MM-dd HH:mm}', interval '6 hours') AS c(c)
            """, ct);
        await ExecAsync(connection, "ANALYZE collect.query_store_interval_wide", ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (string?)await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
