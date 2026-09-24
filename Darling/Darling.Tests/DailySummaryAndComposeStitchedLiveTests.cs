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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A6, lane LA-4b, steps 2 and 3: ONE seeded stitched hourly pair on a real TimescaleDB 2.30.1 store,
/// read through both callers <see cref="RollupCoverage.StitchedRelationSql"/> now serves: the daily summary's
/// <see cref="DailySummarySql.RangeSqlFor(RetentionTier, RollupCoverage, DateTime)"/> (LA-3b2), and one Compose
/// panel query compiled by the real <see cref="ComposeCompiler"/> through <see cref="ComposeSourceRouter"/>
/// (LA-3a step 1). Both must see the legacy's rows below the successor's floor F and the successor's rows at
/// or after F, with no gap and no overlap — and the daily summary's not-carried probe must report correctly on
/// each side.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The live test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres, then works entirely inside it: the pass sweeps whole
   tables, so it must not meet another class's rows, and it cannot race the live collection. Leave it out; this
   comment is here so the next sweep does not "fix" it. */
public sealed class DailySummaryAndComposeStitchedLiveTests
{
    private const int ServerId = -936541;
    private const string ServerName = "a6-la4b-stitched";

    [Fact]
    public async Task DailySummaryAndCompose_BothReadTheStitchedPair_LegacyBelowF_SuccessorAtOrAboveF_NoGapNoOverlap()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 stitched-read test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live A6 stitched-read test needs TimescaleDB: the legacy/successor split exists only between materializations.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var legacy = TimescaleSupport.QueryStatsHourlyView;
        var successor = TimescaleSupport.QueryStatsIntervalHourlyView;

        /* Five whole UTC hours, aligned to the grid: D0..D1 legacy-only (below F), D2 the boundary hour
           (successor's first materialized bucket = F), D3..D4 successor-only (at/after F). One query hash per
           hour so the daily summary's DISTINCT-pair counts and the Compose sum are both easy to check by hand.
           Each hour also gets a restart row (interval 0) so the legacy/successor sample-count difference the
           not-carried probe's source filter depends on is present throughout, not just at the seam. */
        var d0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-10), DateTimeKind.Unspecified);
        const string db = "StitchedDb";
        var collectionId = 1L;

        async Task PlantHourAsync(DateTime hour, string hash, long workerUs)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, 10, 300)", connection);
            insert.Parameters.AddWithValue(collectionId++);
            insert.Parameters.AddWithValue(hour.AddMinutes(5));
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(ServerName);
            insert.Parameters.AddWithValue(db);
            insert.Parameters.AddWithValue(hash);
            insert.Parameters.AddWithValue("0x" + hash);
            insert.Parameters.AddWithValue(workerUs);
            await insert.ExecuteNonQueryAsync(ct);

            /* The restart row: every delta 0, sample_interval_seconds = 0 — the legacy counts it, the
               successor's WHERE refuses it. */
            await using var restart = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 'RESTART', 'RESTARTHANDLE', 0, 0, 0, 0)", connection);
            restart.Parameters.AddWithValue(collectionId++);
            restart.Parameters.AddWithValue(hour.AddMinutes(50));
            restart.Parameters.AddWithValue(ServerId);
            restart.Parameters.AddWithValue(ServerName);
            restart.Parameters.AddWithValue(db);
            await restart.ExecuteNonQueryAsync(ct);
        }

        /* One planted hour per DAY, deliberately, so F (the successor's first materialized bucket) lands at
           D2 10:00 — mid-day. StitchedRelationSql's FROM-clause split still runs at F's own hour (it examines
           every row on its own, with no GROUP BY day); the DAILY SUMMARY's not-carried probe (#4182's fix)
           day-aligns its own boundary to D3 00:00, so D2 as a whole reads from the LEGACY, not split between
           the two relations. */
        DateTime Hour(int n) => d0.AddDays(n).AddHours(10);

        await PlantHourAsync(Hour(0), "HASH0", 1000);
        await PlantHourAsync(Hour(1), "HASH1", 2000);
        await PlantHourAsync(Hour(2), "HASH2", 3000); /* F: the successor's first materialized bucket, at D2 00:00 */
        await PlantHourAsync(Hour(3), "HASH3", 4000);
        await PlantHourAsync(Hour(4), "HASH4", 5000);

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var f = d0.AddDays(2).AddHours(10); /* the boundary: the successor's own first materialized bucket */

        /* The legacy is materialized over the whole span; the successor only from F onward — the shape a
           successor that started later than the legacy always has. */
        await RefreshAsync(connection, legacy, d0, d0.AddDays(5), ct);
        await RefreshAsync(connection, successor, f, d0.AddDays(5), ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(rollups.QueryGrainIntervalHourly);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        var boundary = coverage.FloorOf(successor);
        Assert.Equal(f, boundary);

        /* ── step 2: the daily summary, over the whole 5-day span, at the hourly tier ── */
        var sql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage, d0);
        await using var read = new NpgsqlCommand(sql, connection);
        read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = d0 });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = d0.AddDays(5) });
        var rows = new List<(DateTime Day, long? Unique)>();
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetDateTime(0), reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3)));
        }
        await reader.CloseAsync();

        /* Five calendar days, no day missing from EITHER side of the daily summary's own (day-aligned)
           boundary, and no day duplicated by the two-sided UNION ALL — the not-carried probe found nothing
           missing on either side. The legacy-side days (D0, D1, AND D2 — D2's boundary day-aligns to D3, so
           D2 stays whole on the legacy side even though F itself falls inside D2) count TWO distinct hashes:
           the planted query AND the restart row, which the legacy admits (its own not-carried source filter
           is empty). The successor-side days (D3, D4) count ONE: the restart row is filtered by the
           successor's own WHERE (the source filter its CREATE carries), exactly the interval-honest
           distinction IntervalHonestHourlyRollupLiveTests measures directly on the rollups themselves. */
        Assert.Equal(5, rows.Count);
        Assert.Equal(new[] { d0, d0.AddDays(1), d0.AddDays(2), d0.AddDays(3), d0.AddDays(4) }, rows.Select(r => r.Day).OrderBy(d => d).ToArray());
        Assert.All(rows, r => Assert.NotNull(r.Unique));
        Assert.Equal(new long?[] { 2, 2, 2, 1, 1 }, rows.OrderBy(r => r.Day).Select(r => r.Unique).ToArray());

        /* ── step 3: one Compose panel over the SAME stitched pair, per-bucket totals split at F ── */
        var json = JsonNode.Parse(
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}")!;
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)json, Array.Empty<string>());
        Assert.True(parseError is null, parseError);

        var context = new ComposeRunContext(
            new[] { ServerName }, DarlingMcpTestData.Naive(d0), DarlingMcpTestData.Naive(d0.AddDays(5)),
            ComposeRunContext.NoVariables, rollups, DarlingMcpTestData.Naive(d0.AddDays(5)), coverage);

        var (compiled, compileError) = ComposeCompiler.Compile(plan!, context);
        Assert.True(compileError is null, compileError);
        Assert.NotNull(compiled);

        /* The compiled FROM clause is the stitch itself: a UNION ALL splicing both relation names, split at
           the same boundary the coverage probe measured — never a bare relation name once a successor exists
           to stitch to. */
        Assert.Contains("UNION ALL", compiled!.Sql, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{legacy}", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{successor}", compiled.Sql, StringComparison.Ordinal);

        var totalsByHour = new Dictionary<DateTime, double>();
        await using var composeCommand = new NpgsqlCommand(compiled.Sql, connection);
        foreach (var p in compiled.Parameters)
        {
            composeCommand.Parameters.Add(p);
        }

        await using var composeReader = await composeCommand.ExecuteReaderAsync(ct);
        while (await composeReader.ReadAsync(ct))
        {
            totalsByHour[composeReader.GetDateTime(0)] = Convert.ToDouble(composeReader.GetValue(1));
        }

        /* Every planted hour appears exactly once — no gap, no overlap across the F seam — and each side's
           total matches the relation it must have been read from: the legacy hours (below F) still carry the
           delta this test planted, and the successor hours (at/after F) carry theirs. Both are the SAME
           planted worker time per hash, so a correct split reads the exact planted value on every hour; a
           double-counted seam or a dropped hour would fail this. */
        /* query_worker_us reads and scales the same delta this test planted (its own measure definition
           converts the stored microseconds), so each hour's total is proportional to the planted value —
           what matters is the RATIO across hours, which a gap or an overlap at the F seam would break. */
        Assert.Equal(5, totalsByHour.Count);
        var scale = totalsByHour[Hour(0)];
        Assert.True(scale > 0, "the first hour must have carried a nonzero total");
        Assert.Equal(1 * scale, totalsByHour[Hour(0)], 3);
        Assert.Equal(2 * scale, totalsByHour[Hour(1)], 3);
        Assert.Equal(3 * scale, totalsByHour[Hour(2)], 3); /* F itself: successor-side */
        Assert.Equal(4 * scale, totalsByHour[Hour(3)], 3);
        Assert.Equal(5 * scale, totalsByHour[Hour(4)], 3);

        /* ── item 4 (LA-5): a THIRD caller over the SAME seeded stitched pair — DarlingTrendReader's own
           query-history read, which every production caller (get_query_history) reaches through
           RollupCoverage.StitchedRelationSql, exactly the same builder step 2 and step 3 exercised. Per-bucket
           totals must be the legacy's below F and the successor's at or above F, with exactly one row set (a
           single query_hash) landing at F itself. */
        var hourlyFromClause = coverage.StitchedRelationSql(legacy, "h", d0, RollupCoverage.StitchTier.Hourly);
        var history = await DarlingTrendReader.GetQueryHistoryAsync(
            dataSource, ServerId, db, "HASH2", d0, d0.AddDays(5),
            hourlyAvailable: rollups.QueryGrainHourly,
            coverage: coverage.For(legacy, TimescaleSupport.QueryStatsDailyView),
            hourlyRelation: hourlyFromClause,
            cancellationToken: ct);

        /* HASH2 was planted at Hour(2) — exactly F, the successor's first materialized bucket — so the
           stitched read must find it, and find it exactly once: not double-counted across the UNION ALL
           seam, and not missing because the split fell on the wrong side. */
        Assert.Single(history.Points);
        Assert.Equal(Hour(2), history.Points[0].CollectionTime);
        Assert.Equal(3000, history.Points[0].DeltaCpuUs);

        /* A hash from BELOW F (legacy-only side) must also be found through the same stitched read. */
        var belowF = await DarlingTrendReader.GetQueryHistoryAsync(
            dataSource, ServerId, db, "HASH0", d0, d0.AddDays(5),
            hourlyAvailable: rollups.QueryGrainHourly,
            coverage: coverage.For(legacy, TimescaleSupport.QueryStatsDailyView),
            hourlyRelation: hourlyFromClause,
            cancellationToken: ct);
        Assert.Single(belowF.Points);
        Assert.Equal(Hour(0), belowF.Points[0].CollectionTime);
        Assert.Equal(1000, belowF.Points[0].DeltaCpuUs);

        /* A hash from AT/ABOVE F (successor-only side) too. */
        var atOrAboveF = await DarlingTrendReader.GetQueryHistoryAsync(
            dataSource, ServerId, db, "HASH4", d0, d0.AddDays(5),
            hourlyAvailable: rollups.QueryGrainHourly,
            coverage: coverage.For(legacy, TimescaleSupport.QueryStatsDailyView),
            hourlyRelation: hourlyFromClause,
            cancellationToken: ct);
        Assert.Single(atOrAboveF.Points);
        Assert.Equal(Hour(4), atOrAboveF.Points[0].CollectionTime);
        Assert.Equal(5000, atOrAboveF.Points[0].DeltaCpuUs);
    }

    /// <summary>
    /// #4182 (this fix): the split boundary the daily summary's not-carried probe uses must be DAY-aligned, not
    /// F's own hour. A mid-day F used to split the FLOOR DAY between the legacy half (its early hours) and the
    /// successor half (its later hours), and because each half's <c>queries</c> member groups by
    /// <c>date_trunc('day', bucket)</c> independently, the calendar printed TWO rows for that one day with split
    /// distinct counts instead of one. This test plants a floor day with hashes on both sides of a mid-day F and
    /// pins there is exactly one row for it, holding the correct whole-day distinct count.
    /// </summary>
    [Fact]
    public async Task DailySummary_MidDayFloor_PrintsExactlyOneCalendarRow_NotTwo()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 mid-day-floor test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live A6 mid-day-floor test needs TimescaleDB: the legacy/successor split exists only between materializations.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        const int midDayServerId = -936542;
        const string midDayServerName = "a6-la7-midday-floor";
        await DarlingMcpTestData.RegisterServerAsync(connection, midDayServerId, midDayServerName, ct);

        var legacy = TimescaleSupport.QueryStatsHourlyView;
        var successor = TimescaleSupport.QueryStatsIntervalHourlyView;

        var d0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-10), DateTimeKind.Unspecified);
        const string db = "MidDayFloorDb";
        var collectionId = 1L;

        async Task PlantHourAsync(DateTime hour, string hash)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, 1000, 1000, 10, 300)", connection);
            insert.Parameters.AddWithValue(collectionId++);
            insert.Parameters.AddWithValue(hour.AddMinutes(5));
            insert.Parameters.AddWithValue(midDayServerId);
            insert.Parameters.AddWithValue(midDayServerName);
            insert.Parameters.AddWithValue(db);
            insert.Parameters.AddWithValue(hash);
            insert.Parameters.AddWithValue("0x" + hash);
            await insert.ExecuteNonQueryAsync(ct);
        }

        /* The floor day (D0) gets TWO hashes: one before the mid-day F, one at/after it. D1 gets one more hash,
           entirely on the successor side, so a correctly split calendar prints two rows total, not three. */
        DateTime FloorHourBefore() => d0.AddHours(2);
        DateTime FloorHourAtOrAfter() => d0.AddHours(14); /* F itself: mid-day, the successor's first bucket */
        DateTime NextDayHour() => d0.AddDays(1).AddHours(2);

        await PlantHourAsync(FloorHourBefore(), "MIDDAY0");
        await PlantHourAsync(FloorHourAtOrAfter(), "MIDDAY1");
        await PlantHourAsync(NextDayHour(), "MIDDAY2");

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var f = FloorHourAtOrAfter(); /* mid-day, deliberately not aligned to a day start */

        await RefreshAsync(connection, legacy, d0, d0.AddDays(2), ct);
        await RefreshAsync(connection, successor, f, d0.AddDays(2), ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(rollups.QueryGrainIntervalHourly);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        Assert.Equal(f, coverage.FloorOf(successor));
        Assert.NotEqual(f, f.Date); /* the floor really is mid-day, not a day boundary */

        var sql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage, d0);
        await using var read = new NpgsqlCommand(sql, connection);
        read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = midDayServerId });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = d0 });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = d0.AddDays(2) });
        var rows = new List<(DateTime Day, long? Unique)>();
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetDateTime(0), reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3)));
        }
        await reader.CloseAsync();

        /* EXACTLY ONE row for the floor day (D0), holding the WHOLE day's distinct count (2), never two rows
           with the count split 1-and-1 across the F seam. */
        Assert.Equal(2, rows.Count);
        var floorDayRows = rows.Where(r => r.Day == d0).ToArray();
        Assert.Single(floorDayRows);
        Assert.Equal(2, floorDayRows[0].Unique);
        var nextDayRows = rows.Where(r => r.Day == d0.AddDays(1)).ToArray();
        Assert.Single(nextDayRows);
        Assert.Equal(1, nextDayRows[0].Unique);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }
}
