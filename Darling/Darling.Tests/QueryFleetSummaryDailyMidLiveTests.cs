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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another test's rows. */

/// <summary>
/// #8570: <c>get_query_fleet_summary</c> reads whole closed days from the daily rollup and only the two edge days (plus the hours past
/// the daily's ceiling) from the hourly one. The old statement is kept here as a fixed copy, so the new reply is compared with what the
/// old statement answers on the same store: the same rows in the same order, first_hour and last_hour to the microsecond, with EXCEPT
/// both ways on the rows. Each coverage guard has a store where dropping the guard changes the reply.
/// </summary>
public sealed class QueryFleetSummaryDailyMidLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8570 fleet summary tests.";
    private const string Hash = "0xQ8570";
    private const string Other = "0xOTHER8570";
    private static readonly (int Id, string Name)[] Servers = [(-857001, "a8570-one"), (-857002, "a8570-two"), (-857003, "a8570-three")];

    /// <summary>The statement as it stood before #8570: one relation, the hourly rollup, below the ceiling $4. A FIXED COPY: the
    /// production fallback is <see cref="DarlingMcpQueryFleetSummaryTools.BuildSql"/>, and a later edit to it must not move this oracle.</summary>
    private static string OldSql(string hourlyRelation) => $"""
        WITH hourly AS
        (
            SELECT f.server_id, f.database_name,
                   SUM(f.execution_count_sum) AS executions, SUM(f.worker_time_sum) AS cpu_us, SUM(f.elapsed_time_sum) AS elapsed_us,
                   MIN(f.bucket) AS first_hour, MAX(f.bucket) AS last_hour
            FROM {hourlyRelation}
            WHERE f.query_hash = $1
            AND   f.bucket >= $2
            AND   f.bucket < $3 AND f.bucket < $4
            GROUP BY f.server_id, f.database_name
        ),
        tail AS
        (
            SELECT q.server_id, q.database_name,
                   SUM(q.delta_execution_count) AS executions, SUM(q.delta_worker_time) AS cpu_us, SUM(q.delta_elapsed_time) AS elapsed_us,
                   MIN(date_trunc('hour', q.collection_time)) AS first_hour, MAX(date_trunc('hour', q.collection_time)) AS last_hour
            FROM query_stats AS q
            WHERE q.query_hash = $1
            AND   q.collection_time >= $4
            AND   q.collection_time <= $3
            AND   q.collection_time >= $2
            GROUP BY q.server_id, q.database_name
        ),
        pairs AS
        (
            SELECT server_id, database_name,
                   COALESCE(SUM(executions), 0)::bigint AS executions, COALESCE(SUM(cpu_us), 0)::bigint AS cpu_us,
                   COALESCE(SUM(elapsed_us), 0)::bigint AS elapsed_us, MIN(first_hour) AS first_hour, MAX(last_hour) AS last_hour
            FROM (SELECT * FROM hourly UNION ALL SELECT * FROM tail) AS u
            GROUP BY server_id, database_name
        )
        SELECT s.server_name, s.display_name, p.database_name, p.executions, p.cpu_us, p.elapsed_us, p.first_hour, p.last_hour,
               COUNT(*) OVER () AS pairs
        FROM pairs AS p
        JOIN servers AS s ON s.server_id = p.server_id
        WHERE ($6::integer[] IS NULL OR p.server_id = ANY($6))
        ORDER BY p.cpu_us DESC, s.server_name, p.database_name
        LIMIT $5
        """;

    private sealed record Row(string Server, string? Display, string? Database, long Executions, long CpuUs, long ElapsedUs, DateTime First, DateTime Last, long Pairs);

    private sealed record Store(ScratchPostgres Scratch, NpgsqlConnection Connection);

    private static async Task<Store> CreateStoreAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #8570 tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        foreach (var (id, name) in Servers)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        return new Store(scratch, connection);
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task Refresh(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static Task Insert(NpgsqlConnection connection, CancellationToken ct, int serverIndex, string db, DateTime at, string handle, long executions, long cpuUs, long elapsedUs, string hash = Hash)
        => DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 100, 1000, 3600)",
            CollectionIdGenerator.Next(), at, Servers[serverIndex].Id, Servers[serverIndex].Name, db, hash, handle, executions, cpuUs, elapsedUs);

    /// <summary>The timeline: <c>end</c> is the window's end (a whole hour), <c>start</c> is mid-day a week before it, <c>DStart</c> the first
    /// midnight after the start and <c>DEnd</c> the last midnight at or before the end.</summary>
    private sealed record Timeline(DateTime End, int HoursBack)
    {
        public DateTime Start => End.AddHours(-HoursBack);
        public DateTime DStart => Start == Start.Date ? Start : Start.Date.AddDays(1);
        public DateTime DEnd => End.Date;
    }

    private static Timeline NewTimeline()
    {
        var now = DateTime.UtcNow;
        var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);

        /* Keep the start off midnight and the end off midnight so both edge days hold hours. */
        var hoursBack = 168;
        while (end.AddHours(-hoursBack).Hour == 0)
        {
            hoursBack++;
        }

        if (end.Hour == 0)
        {
            end = end.AddHours(-1);
        }

        return new Timeline(end, hoursBack);
    }

    /// <summary>Seeds every kind of pair a mid route can meet: one that runs the whole window under two handles, one only on the first
    /// edge day, one only on the last, one only in the mid (hours 5 and 17 of two different days), one from the first edge day into the mid,
    /// one from the mid into the last edge day, one only in the raw tail, and one of another hash. Returns nothing: the oracle is the old statement.</summary>
    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct, Timeline t)
    {
        var from = t.Start.Date;
        for (var at = from; at < t.End; at = at.AddHours(1))
        {
            var h = (long)(at - from).TotalHours;

            /* (one, Alpha): every hour, two handles. */
            await Insert(c, ct, 0, "Alpha", at.AddMinutes(10), "0xH1", 1 + h % 5, 1_000 + h * 37, 2_000 + h * 91);
            await Insert(c, ct, 0, "Alpha", at.AddMinutes(20), "0xH2", 2, 500 + h, 900 + h);
            await Insert(c, ct, 0, "Alpha", at.AddMinutes(30), "0xH1", 3, 100, 200, Other);

            /* (two, Beta): hours of the first edge day only (some before the start: excluded). */
            if (at < t.DStart)
            {
                await Insert(c, ct, 1, "Beta", at.AddMinutes(15), "0xH3", 4, 7_000 + h, 8_000 + h);
            }

            /* (three, Gamma): the hours at or after the last midnight only. */
            if (at >= t.DEnd)
            {
                await Insert(c, ct, 2, "Gamma", at.AddMinutes(15), "0xH4", 5, 9_000 + h, 9_500 + h);
            }

            /* (two, Alpha): from the first edge day until two days into the mid. (three, Alpha): from two days before the last midnight into the last edge day. */
            if (at < t.DStart.AddDays(2))
            {
                await Insert(c, ct, 1, "Alpha", at.AddMinutes(25), "0xH5", 6, 3_000 + h, 4_000 + h);
            }

            if (at >= t.DEnd.AddDays(-2))
            {
                await Insert(c, ct, 2, "Alpha", at.AddMinutes(25), "0xH6", 7, 6_000 + h, 6_500 + h);
            }
        }

        /* (one, Delta): only in the mid, at hour 5 of the second mid day and hour 17 of the fourth. */
        await Insert(c, ct, 0, "Delta", t.DStart.AddDays(1).AddHours(5).AddMinutes(12), "0xH7", 11, 12_345, 23_456);
        await Insert(c, ct, 0, "Delta", t.DStart.AddDays(3).AddHours(17).AddMinutes(48), "0xH7", 13, 54_321, 65_432);
    }

    private static async Task RefreshHourlyAsync(NpgsqlConnection c, CancellationToken ct, Timeline t)
        => await Refresh(c, TimescaleSupport.QueryStatsIntervalHourlyView, t.Start.Date.AddDays(-1), t.End, ct);

    private static async Task RefreshDailyAsync(NpgsqlConnection c, CancellationToken ct, Timeline t, DateTime through)
        => await Refresh(c, TimescaleSupport.QueryStatsIntervalDailyView, t.Start.Date, through, ct);

    /// <summary>A raw row in the tail, at or past the rollup's ceiling, from a pair the rollup never held.</summary>
    private static Task InsertTailAsync(NpgsqlConnection c, CancellationToken ct, Timeline t)
        => Insert(c, ct, 0, "Eps", t.End.AddMinutes(5), "0xH8", 1, 100, 200);

    private static async Task<(DarlingMcpQueryFleetSummaryTools.DailyMidPlan? Plan, string HourlyRelation, DateTime RawFrom, DateTime Start)> PlanAsync(NpgsqlDataSource pg, Timeline t, CancellationToken ct)
    {
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        var start = t.Start;
        var hourlyRelation = coverage.StitchedRelationSql(TimescaleSupport.QueryStatsHourlyView, "f", start, RollupCoverage.StitchTier.Hourly);
        var ceiling = coverage.HourlyEndCeiling(TimescaleSupport.QueryStatsHourlyView, start);
        Assert.NotNull(ceiling);
        var rawFrom = ceiling!.Value > start ? ceiling.Value : start;
        var plan = DarlingMcpQueryFleetSummaryTools.PlanDailyMid(coverage, hourlyRelation, start, t.End, ceiling);
        if (plan is not null && !await DarlingMcpQueryFleetSummaryTools.DailyHasNoHoleAsync(pg, plan, ct))
        {
            plan = null;
        }

        return (plan, hourlyRelation, rawFrom, start);
    }

    private static async Task<List<Row>> RunAsync(NpgsqlDataSource pg, string sql, Timeline t, DateTime rawFrom, DarlingMcpQueryFleetSummaryTools.DailyMidPlan? mid, CancellationToken ct)
    {
        await using var command = pg.CreateCommand(sql);
        AddParameters(command, t, rawFrom, mid);
        var rows = new List<Row>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new Row(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5), reader.GetDateTime(6), reader.GetDateTime(7), reader.GetInt64(8)));
        }

        return rows;
    }

    private static void AddParameters(NpgsqlCommand command, Timeline t, DateTime rawFrom, DarlingMcpQueryFleetSummaryTools.DailyMidPlan? mid)
    {
        command.Parameters.AddWithValue(NpgsqlDbType.Text, Hash);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t.Start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t.End, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(rawFrom, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, 200);
        command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Integer, DBNull.Value);
        if (mid is not null)
        {
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(mid.DStart, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(mid.DEnd, DateTimeKind.Unspecified));
        }
    }

    /// <summary>The old statement and the tool's own route, on the same store and window: identical rows in the same order (the records
    /// compare first_hour and last_hour by their ticks, which hold the microseconds), EXCEPT both ways, and the tool's reply names the same
    /// rows. Returns the plan the tool chose (null when it fell back to the old statement).</summary>
    private static async Task<DarlingMcpQueryFleetSummaryTools.DailyMidPlan?> AssertSameAsOldAsync(NpgsqlDataSource pg, Timeline t, CancellationToken ct)
    {
        var (plan, hourlyRelation, rawFrom, _) = await PlanAsync(pg, t, ct);
        var old = await RunAsync(pg, OldSql(hourlyRelation), t, rawFrom, null, ct);
        Assert.NotEmpty(old);
        var routed = plan is null
            ? await RunAsync(pg, DarlingMcpQueryFleetSummaryTools.BuildSql(hourlyRelation, RollupCoverage.HourlyCeilingClause(4)), t, rawFrom, null, ct)
            : await RunAsync(pg, DarlingMcpQueryFleetSummaryTools.BuildMidSql(hourlyRelation, plan.DailyRelation), t, rawFrom, plan, ct);
        Assert.Equal(old, routed);
        Assert.Empty(old.Except(routed));
        Assert.Empty(routed.Except(old));

        /* The tool's reply, end to end. */
        var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(pg, Hash, t.HoursBack,
            as_of: t.End.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture) + "Z", limit: 200, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        Assert.False(doc.RootElement.TryGetProperty("error", out _), json);
        var reply = doc.RootElement.GetProperty("servers").EnumerateArray()
            .Select(r => (r.GetProperty("server_name").GetString(), r.GetProperty("database_name").GetString(), r.GetProperty("execution_count").GetInt64(),
                r.GetProperty("cpu_ms").GetDouble(), r.GetProperty("elapsed_ms").GetDouble(),
                r.GetProperty("first_hour").GetString(), r.GetProperty("last_hour").GetString())).ToArray();
        var expected = old.Select(r => ((string?)r.Server, (string?)(r.Database ?? ""), r.Executions, Math.Round(r.CpuUs / 1000.0, 2), Math.Round(r.ElapsedUs / 1000.0, 2),
            (string?)DateTime.SpecifyKind(r.First, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
            (string?)DateTime.SpecifyKind(r.Last, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture))).ToArray();
        Assert.Equal(expected, reply);
        return plan;
    }

    [Fact]
    public async Task MidRoute_AnswersWhatTheOldStatementAnswers_AcrossBothEdgeDays()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            await RefreshHourlyAsync(connection, ct, t);
            await RefreshDailyAsync(connection, ct, t, t.DEnd);
            await InsertTailAsync(connection, ct, t);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

            var plan = await AssertSameAsOldAsync(pg, t, ct);
            Assert.NotNull(plan);
            Assert.Equal(t.DStart, plan!.DStart);
            Assert.Equal(t.DEnd, plan.DEnd);
            Assert.Equal(TimescaleSupport.QueryStatsIntervalDailyView, plan.DailyView);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Guard: the daily ceiling. The daily refresh runs about a day behind the hourly one; here it stopped three days short. A
    /// mid route that read the daily up to the HOURLY ceiling would miss those three days.</summary>
    [Fact]
    public async Task DailyTrailingTheHourly_LeavesTheNewestClosedDaysToTheHourlyEdge()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            await RefreshHourlyAsync(connection, ct, t);
            await RefreshDailyAsync(connection, ct, t, t.DEnd.AddDays(-3));
            await InsertTailAsync(connection, ct, t);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

            var plan = await AssertSameAsOldAsync(pg, t, ct);
            Assert.NotNull(plan);
            Assert.Equal(t.DEnd.AddDays(-3), plan!.DEnd);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Guard: no whole-day hole. The daily never wrote one mid day (a store that was down across the daily refreshes of that
    /// day: the policy re-reads only four days, so it is never written later). Reading the daily as it stands would drop that day.</summary>
    [Fact]
    public async Task DailyWithAHoleDay_FallsBackToTheHourlyRollup()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            await RefreshHourlyAsync(connection, ct, t);
            var hole = t.DStart.AddDays(2);
            await RefreshDailyAsync(connection, ct, t, hole);
            await Refresh(connection, TimescaleSupport.QueryStatsIntervalDailyView, hole.AddDays(1), t.DEnd, ct);
            await InsertTailAsync(connection, ct, t);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

            var plan = await AssertSameAsOldAsync(pg, t, ct);
            Assert.Null(plan);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Guard: the legacy and successor stitch. The successor hourly holds the window and the successor daily holds nothing yet,
    /// so the daily stitch answers the LEGACY daily, which was materialized before the raw rows of one pair changed. A mid route that took
    /// the stitch as it came would serve the legacy daily's old numbers for the mid days.</summary>
    [Fact]
    public async Task DailyReadingAnotherFamilyThanTheHourly_FallsBackToTheHourlyRollup()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            var from = t.Start.Date.AddDays(-1);
            await Refresh(connection, TimescaleSupport.QueryStatsHourlyView, from, t.End, ct);
            await Refresh(connection, TimescaleSupport.QueryStatsDailyView, t.Start.Date, t.DEnd, ct);
            await Exec(connection, $"UPDATE collect.query_stats SET delta_worker_time = delta_worker_time * 2 WHERE query_hash = '{Hash}' AND database_name = 'Alpha'", ct);
            await RefreshHourlyAsync(connection, ct, t);
            await InsertTailAsync(connection, ct, t);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

            var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
            var dailyRelation = coverage.StitchedRelationSql(TimescaleSupport.QueryStatsDailyView, "d", t.DStart, RollupCoverage.StitchTier.Daily);
            Assert.Contains("collect." + TimescaleSupport.QueryStatsDailyView + " ", dailyRelation, StringComparison.Ordinal);

            var plan = await AssertSameAsOldAsync(pg, t, ct);
            Assert.Null(plan);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Guard: the legacy and successor stitch. The successor hourly starts mid-window (at F, seven hours into a mid day), the legacy
    /// holds the hours before it, and the raw rows of one pair changed after the legacy was materialized. The hourly stitch reads the legacy
    /// below F and the successor from F. The daily stitch splits at the first whole day after F, so the day holding F comes from the LEGACY
    /// daily, whose hours past F are the old numbers. A mid route over that daily would serve them.</summary>
    [Fact]
    public async Task StitchedLegacyAndSuccessor_FallsBackToTheHourlyRollup()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            var f = t.DStart.AddDays(2).AddHours(7);
            await Refresh(connection, TimescaleSupport.QueryStatsHourlyView, t.Start.Date.AddDays(-1), t.End, ct);
            await Refresh(connection, TimescaleSupport.QueryStatsDailyView, t.Start.Date, t.DEnd, ct);
            await Exec(connection, $"UPDATE collect.query_stats SET delta_worker_time = delta_worker_time * 2 WHERE query_hash = '{Hash}' AND database_name = 'Alpha' AND collection_time >= TIMESTAMP '{f:yyyy-MM-dd HH:mm:ss}'", ct);
            await Refresh(connection, TimescaleSupport.QueryStatsIntervalHourlyView, f, t.End, ct);
            await Refresh(connection, TimescaleSupport.QueryStatsIntervalDailyView, f.Date.AddDays(1), t.DEnd, ct);
            await InsertTailAsync(connection, ct, t);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

            var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
            Assert.Equal(f, coverage.StitchFloor(TimescaleSupport.QueryStatsHourlyView, RollupCoverage.StitchTier.Hourly, t.Start));
            Assert.Contains("UNION", coverage.StitchedRelationSql(TimescaleSupport.QueryStatsDailyView, "d", t.DStart, RollupCoverage.StitchTier.Daily), StringComparison.Ordinal);

            var plan = await AssertSameAsOldAsync(pg, t, ct);
            Assert.Null(plan);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>A window with fewer than <see cref="DarlingMcpQueryFleetSummaryTools.MinMidDays"/> whole days stays on the old statement.</summary>
    [Fact]
    public async Task WindowUnderTwoWholeDays_StaysOnTheHourlyRollup()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            await RefreshHourlyAsync(connection, ct, t);
            await RefreshDailyAsync(connection, ct, t, t.DEnd);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);
            var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
            var hourly = coverage.StitchedRelationSql(TimescaleSupport.QueryStatsHourlyView, "f", t.End.AddHours(-30), RollupCoverage.StitchTier.Hourly);
            var ceiling = coverage.HourlyEndCeiling(TimescaleSupport.QueryStatsHourlyView, t.End.AddHours(-30));
            Assert.Null(DarlingMcpQueryFleetSummaryTools.PlanDailyMid(coverage, hourly, t.End.AddHours(-30), t.End, ceiling));
            Assert.NotNull(DarlingMcpQueryFleetSummaryTools.PlanDailyMid(coverage, hourly, t.Start, t.End, ceiling));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
