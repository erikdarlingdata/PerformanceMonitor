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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Daily Summary's <c>coll</c> part (runs and errors per day) reads closed hours from the hourly rollup
/// (<c>collect.collection_log_hourly</c>, V186) and the raw log only for the hours the rollup does not hold. These run the
/// real statement against a 30-day store whose older chunks are compressed, whose rollup has gaps (a pass that is behind)
/// and whose log holds cycle rows, and prove two things: the counts equal the raw read exactly, and the closed hours no longer
/// read the uncompressed chunks, which hold every server's rows on shared pages and made the read cost grow with the window's
/// uncompressed chunks rather than its length.
/// </summary>
/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres. */
[Collection("live-postgres")]
public sealed class DailySummaryCollRolledLiveTests
{
    private const int ServerId = 8549;
    private const int OtherServerId = 8550;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The counts of the day-aligned 30-day window and of two windows with mid-hour edges equal the raw read, with a gap in the
    /// middle of the rolled hours, the newest rolled hours missing (a pass that is behind), the open hour, and cycle rows in the log.</summary>
    [Fact]
    public async Task Coll_EqualsTheRawRead_WithGapsInTheRollup_MidHourWindowEdges_AndCycleRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the Daily Summary coll live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = LiveClock.Now();
        var today = DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);
        var currentHour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
        await SeedAndRollUpAsync(connection, today.AddDays(-29), now, ct);

        var rolledBefore = await ScalarAsync<long>(connection, $"SELECT count(DISTINCT hour_start) FROM collect.collection_log_hourly WHERE server_id = {ServerId}", ct);
        Assert.True(rolledBefore >= 150, $"the seed should roll about a week of closed hours, rolled {rolledBefore}");

        /* A pass that is behind: seven hours missing in the middle of the week, and the newest three closed hours missing. */
        await ExecAsync(connection,
            $"DELETE FROM collect.collection_log_hourly WHERE server_id = {ServerId} AND hour_start >= '{Ts(currentHour.AddHours(-60))}' AND hour_start < '{Ts(currentHour.AddHours(-53))}'", ct);
        await ExecAsync(connection,
            $"DELETE FROM collect.collection_log_hourly WHERE server_id = {ServerId} AND hour_start >= '{Ts(currentHour.AddHours(-3))}'", ct);

        var windows = new (DateTime Start, DateTime End)[]
        {
            (today.AddDays(-29), today.AddDays(1)),
            (today.AddDays(-5).AddHours(13).AddMinutes(17).AddSeconds(23).AddTicks(1234560), today.AddDays(-2).AddHours(6).AddMinutes(41).AddSeconds(9).AddTicks(7654320)),
            (today.AddDays(-12).AddHours(3), currentHour.AddMinutes(20)),
        };

        foreach (var (start, end) in windows)
        {
            var expected = await ReadRawCollAsync(connection, start, end, ct);
            Assert.True(expected.Count >= 2, "the window should hold more than one day of runs");
            var actual = await ReadSummaryCollAsync(connection, start, end, ct);
            Assert.Equal(expected, actual);
        }

        /* The cycle rows are in the log and out of the counts: the raw read excludes them by the same predicate, so equality above
           would also hold if both counted them, and this makes sure they were seeded at all. */
        Assert.True(await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM collect.collection_log WHERE server_id = {ServerId} AND collector_name = '{CollectionCycleRow.CollectorName}'", ct) > 0);
    }

    /// <summary>
    /// #8549 (field: 771 ms of a 2.6 s month read): the month window's closed hours are all in the rollup once the backfill has run, so
    /// the read never goes back to the compressed raw chunks for them. Before, the table kept 7 days and the other three weeks were read
    /// raw. The window is the Overview's: midnight 30 days back to the end of today. The rollup pass catches up a day at a time, so the
    /// store gets the passes a 31-day backlog takes. The counts equal the raw read, and they equal the same read with the older hours
    /// deleted from the table, which is what the 7-day retention left (the older hours are then read raw and found the same).
    /// </summary>
    [Fact]
    public async Task TheMonthWindow_IsServedFromTheRollup_AfterTheBackfill_WithTheSameCountsAsTheRawRead()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the Daily Summary coll live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = LiveClock.Now();
        var today = DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);
        var currentHour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
        var start = today.AddDays(-30);
        var end = today.AddDays(1);

        /* Every pass rolls RecomputeHours plus CatchUpHours, so the retention window is filled after this many; one more finds nothing to do. */
        var passes = (int)Math.Ceiling((CollectionLogHourly.RetentionHours - CollectionLogHourly.RecomputeHours) / (double)CollectionLogHourly.CatchUpHours) + 1;
        await SeedAndRollUpAsync(connection, start, now, ct, passes);

        /* Every closed hour of the window, from its first hour to the one before the current hour, has a rollup row for the server. */
        var windowHours = (long)(currentHour - start).TotalHours;
        var held = await ScalarAsync<long>(connection,
            $"SELECT count(DISTINCT hour_start) FROM collect.collection_log_hourly WHERE server_id = {ServerId} AND hour_start >= '{Ts(start)}' AND hour_start < '{Ts(currentHour)}'", ct);
        Assert.Equal(windowHours, held);

        var expected = await ReadRawCollAsync(connection, start, end, ct);
        Assert.True(expected.Count >= 30, "the window should hold a month of days");
        Assert.Equal(expected, await ReadSummaryCollAsync(connection, start, end, ct));

        /* The 7-day retention's state: the older hours are not in the table, so the read finds them raw. Same counts. */
        await ExecAsync(connection, $"DELETE FROM collect.collection_log_hourly WHERE hour_start < '{Ts(currentHour.AddDays(-7))}'", ct);
        Assert.Equal(expected, await ReadSummaryCollAsync(connection, start, end, ct));
    }

    /// <summary>
    /// The statement's data pages (scan nodes only, <see cref="ExplainBuffers"/>) with every closed hour of the week rolled are a fraction
    /// of the raw read's. The raw read walks the uncompressed chunks through the time index, two servers' rows to a page.
    /// </summary>
    [Fact]
    public async Task Coll_ReadsNoUncompressedChunkForClosedHours_WhenTheRollupHoldsThem()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the Daily Summary coll live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = LiveClock.Now();
        var today = DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);
        await SeedAndRollUpAsync(connection, today.AddDays(-29), now, ct);

        var start = today.AddDays(-29);
        var end = today.AddDays(1);

        var rawPages = await PagesAsync(connection, RawCollSql, start, end, ct);
        var statementPages = await PagesAsync(connection, DailySummarySql.RangeSql, start, end, ct);

        /* On the seed the raw read walks about 800 pages of the uncompressed chunks; the rolled read touches the rollup's rows, the
           compressed older island, and the open hour. A bound a third of the raw read's keeps room for a different chunk layout. */
        Assert.True(rawPages >= 300, $"the seed should make the raw read walk the uncompressed chunks, it read {rawPages} pages");
        Assert.True(statementPages * 3 < rawPages, $"the Daily Summary read {statementPages} data pages against {rawPages} for the raw coll read");
    }

    private static readonly string RawCollSql = $"""
        SELECT date_trunc('day', collection_time) AS d,
               COUNT(*) AS runs,
               COUNT(*) FILTER (WHERE status = 'ERROR') AS errs
        FROM v_collection_log
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
        AND   {CollectionCycleRow.NotCycleRowSql}
        GROUP BY 1
        """;

    /// <summary>
    /// 30 days of runs for two servers on shared pages: five collectors (three real ones and both cycle rows) twice an hour, the
    /// second of each hour with a fractional second, with a 1.2 KB message on every row so a page holds a handful. Chunks older than
    /// a week are compressed. Then the rollup runs until it has caught up a week.
    /// </summary>
    private static async Task SeedAndRollUpAsync(NpgsqlConnection connection, DateTime from, DateTime nowUtc, CancellationToken ct, int passes = 9)
    {
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await ExecAsync(connection, $"""
            INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
            SELECT row_number() OVER (ORDER BY g.ts, s.sid, c.name),
                   s.sid, 'srv' || s.sid, c.name,
                   g.ts + CASE WHEN extract(minute FROM g.ts) = 30 THEN interval '0.123456 seconds' ELSE interval '0' END,
                   100 + extract(hour FROM g.ts)::int,
                   CASE WHEN (extract(hour FROM g.ts)::int + length(c.name) + s.sid) % 4 = 0 THEN 'ERROR' ELSE 'SUCCESS' END,
                   repeat('x', 1200), 5
            FROM generate_series('{Ts(from)}'::timestamp, '{Ts(nowUtc)}'::timestamp, interval '30 minutes') AS g(ts)
            CROSS JOIN (VALUES ({ServerId}), ({OtherServerId})) AS s(sid)
            CROSS JOIN (VALUES ('wait_stats'), ('query_stats'), ('cpu_utilization'), ('{CollectionCycleRow.CollectorName}'), ('{AnalysisPassRow.CollectorName}')) AS c(name)
            WHERE g.ts + CASE WHEN extract(minute FROM g.ts) = 30 THEN interval '0.123456 seconds' ELSE interval '0' END <= '{Ts(nowUtc)}'::timestamp
            """, ct);

        await ExecAsync(connection,
            "SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass) FROM timescaledb_information.chunks "
            + "WHERE hypertable_schema = 'collect' AND hypertable_name = 'collection_log' AND NOT is_compressed AND range_end <= now() - interval '7 days'", ct);
        await ExecAsync(connection, "ANALYZE collect.collection_log", ct);

        for (var pass = 0; pass < passes; pass++)
        {
            await CollectionLogHourly.RollUpAsync(connection, nowUtc, null, ct);
        }

        await ExecAsync(connection, "ANALYZE collect.collection_log_hourly", ct);
    }

    private static async Task<Dictionary<DateTime, (long Runs, long Errors)>> ReadRawCollAsync(NpgsqlConnection connection, DateTime start, DateTime end, CancellationToken ct)
    {
        var rows = new Dictionary<DateTime, (long, long)>();
        await using var command = Bind(connection, RawCollSql, start, end);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows[reader.GetDateTime(0)] = (reader.GetInt64(1), reader.GetInt64(2));
        }

        return rows;
    }

    private static async Task<Dictionary<DateTime, (long Runs, long Errors)>> ReadSummaryCollAsync(NpgsqlConnection connection, DateTime start, DateTime end, CancellationToken ct)
    {
        var rows = new Dictionary<DateTime, (long, long)>();
        await using var command = Bind(connection, DailySummarySql.RangeSql, start, end);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var day = reader.GetOrdinal("day");
        var runs = reader.GetOrdinal("collection_runs");
        var errors = reader.GetOrdinal("collection_errors");
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetInt64(runs) > 0)
            {
                rows[reader.GetDateTime(day)] = (reader.GetInt64(runs), reader.GetInt64(errors));
            }
        }

        return rows;
    }

    private static async Task<long> PagesAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        /* The statement has four parameters, the raw coll read three; an unused trailing parameter is refused, so bind what the text names. */
        await using var command = Bind(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, start, end, withFloor: sql.Contains("$4", StringComparison.Ordinal));
        return ExplainBuffers.DataPages(await command.ExecuteScalarAsync(ct));
    }

    private static NpgsqlCommand Bind(NpgsqlConnection connection, string sql, DateTime start, DateTime end, bool withFloor = true)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        if (withFloor && sql.Contains("$4", StringComparison.Ordinal))
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = EventWindowFloor.For(start) });
        }

        return command;
    }

    private static string Ts(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
