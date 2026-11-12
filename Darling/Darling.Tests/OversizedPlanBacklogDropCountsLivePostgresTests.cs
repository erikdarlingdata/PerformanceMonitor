/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   shared, so it needs no place in the live-postgres collection. */

/// <summary>
/// #8691: the dropped counts of the oversized-plan backlog come from <c>collect.oversized_plan_backlog_drops</c>, one row per
/// server, collector and hour, and not from dropped rows the table holds for a day. Each test seeds the count table directly
/// (the trim's own write is covered in <see cref="OversizedPlanBacklogBoundLivePostgresTests"/>) and reads the rollup and the
/// census the way the MCP tool does.
/// </summary>
public sealed class OversizedPlanBacklogDropCountsLivePostgresTests
{
    private const int HeldServerId = 4401;
    private const int DropsOnlyServerId = 4402;

    private const string ServerSql = @"
INSERT INTO collect.servers (server_id, server_name, is_enabled)
VALUES (4401, 'drop-counts-server', TRUE);";

    /// <summary>
    /// The last-day window is the day before the read, to the hour: a bucket 25 hours old is out of it and one 23 hours old is
    /// in. Rows dropped count with the held rows in the total, the sizes and the oldest sighting cover held rows only, and a
    /// server with counts and no held rows is still listed. The old read counted only dropped rows the table held, so it
    /// reports none of these.
    /// </summary>
    [Fact]
    public async Task TheRollupAndTheCensusCountDropsFromTheHourlyCounts_InsideTheLastDayOnly()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8691 drop counts test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var now = WholeSecond(DateTime.UtcNow);
        var hour = Hour(now);

        /* Held: one pending row (first sighted an hour ago) and one captured row (first sighted three days ago). */
        await ExecuteAsync(postgres, @"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at, captured_at, last_attempt_at, plan_xml)
VALUES
    (4401, 'query_stats', '0xPENDING', '0xSQL1', 0, 400, 'counts_db', '0xHASH1', 700000, $1, $1, NULL, NULL, NULL),
    (4401, 'procedure_stats', '0xCAPTURED', '0xSQL2', 0, 400, 'counts_db', NULL, 900000, $2, $2, $2, $2, '<plan/>');",
            now.AddHours(-1), now.AddDays(-3));

        /* Counts: query_stats 10 dropped 25 hours ago, 5 (of which 4 were first sighted in that hour) 23 hours ago and 3 two hours
           ago; procedure_stats 2 two hours ago. The server with no held row has 4 dropped 30 minutes ago. */
        await ExecuteAsync(postgres, @"
INSERT INTO collect.oversized_plan_backlog_drops
(server_id, collector_name, hour_start, rows_dropped, rows_first_seen)
VALUES
    (4401, 'query_stats', $1, 10, 0),
    (4401, 'query_stats', $2, 5, 4),
    (4401, 'query_stats', $3, 3, 0),
    (4401, 'procedure_stats', $3, 2, 0),
    (4402, 'query_stats', $4, 4, 0);",
            hour.AddHours(-25), hour.AddHours(-23), hour.AddHours(-2), hour);

        var lastDaySince = now.AddDays(-1);
        var rollup = await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, null, lastDaySince, ct);
        Assert.Equal(new[] { HeldServerId, DropsOnlyServerId }, rollup.Select(r => r.ServerId).Order());

        var held = rollup.Single(r => r.ServerId == HeldServerId);
        Assert.Equal("drop-counts-server", held.ServerName);
        Assert.Equal(2 + 20, held.TotalRows);
        Assert.Equal(20, held.DroppedRows);
        Assert.Equal(8 + 2, held.RowsDroppedLastDay);
        Assert.Equal(1, held.PendingRows);
        Assert.Equal(1, held.CapturedRows);
        Assert.Equal(1 + 4, held.RowsAddedLastDay);
        Assert.Equal(1 + 18, held.QueryStatsRows);
        Assert.Equal(1 + 2, held.ProcedureStatsRows);
        Assert.Equal(held.PendingRows + held.CapturedRows + held.ExpiredRows + held.DroppedRows, held.TotalRows);
        /* Size and sighting figures are the held rows' alone: two rows, and the median of two is the first of them. */
        Assert.Equal(700_000, held.MinObservedBytes);
        Assert.Equal(700_000, held.MedianObservedBytes);
        Assert.Equal(900_000, held.MaxObservedBytes);

        var dropsOnly = rollup.Single(r => r.ServerId == DropsOnlyServerId);
        Assert.Null(dropsOnly.ServerName);
        Assert.Equal(4, dropsOnly.TotalRows);
        Assert.Equal(4, dropsOnly.DroppedRows);
        Assert.Equal(4, dropsOnly.RowsDroppedLastDay);
        Assert.Equal(0, dropsOnly.PendingRows);
        Assert.Null(dropsOnly.OldestFirstSeenAt);
        Assert.Null(dropsOnly.LastSeenAt);
        Assert.Null(dropsOnly.MinObservedBytes);
        Assert.Null(dropsOnly.MedianObservedBytes);
        Assert.Null(dropsOnly.MaxObservedBytes);

        var one = Assert.Single(await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, HeldServerId, lastDaySince, ct));
        Assert.Equal(held.TotalRows, one.TotalRows);
        Assert.Equal(held.RowsDroppedLastDay, one.RowsDroppedLastDay);

        var census = await DarlingOversizedPlanBacklogReader.GetCollectorCensusAsync(postgres, null, ct);
        var queryStats = census.Single(c => c.CollectorName == OversizedPlanBacklog.QueryStatsCollectorName);
        Assert.Equal(2, queryStats.Servers);
        Assert.Equal(1 + 22, queryStats.TotalRows);
        Assert.Equal(22, queryStats.DroppedRows);
        Assert.Equal(700_000, queryStats.MedianObservedBytes);
        var procedureStats = census.Single(c => c.CollectorName == OversizedPlanBacklog.ProcedureStatsCollectorName);
        Assert.Equal(1, procedureStats.Servers);
        Assert.Equal(1 + 2, procedureStats.TotalRows);
        Assert.Equal(2, procedureStats.DroppedRows);

        var forDropsOnly = Assert.Single(await DarlingOversizedPlanBacklogReader.GetCollectorCensusAsync(postgres, DropsOnlyServerId, ct));
        Assert.Equal(OversizedPlanBacklog.QueryStatsCollectorName, forDropsOnly.CollectorName);
        Assert.Equal(4, forDropsOnly.TotalRows);
        Assert.Equal(4, forDropsOnly.DroppedRows);
        Assert.Null(forDropsOnly.MedianObservedBytes);
        var forHeld = await DarlingOversizedPlanBacklogReader.GetCollectorCensusAsync(postgres, HeldServerId, ct);
        Assert.Equal(18, forHeld.Single(c => c.CollectorName == OversizedPlanBacklog.QueryStatsCollectorName).DroppedRows);
    }

    /// <summary>
    /// A store an older build wrote still holds the dropped rows that build kept for a day. The read counts them with the new
    /// counts while they last, and the trim's purge removes them when they are a day old, so the count does not double and
    /// does not drop them early.
    /// </summary>
    [Fact]
    public async Task AnUpgradedStoreWithLegacyHeldDroppedRows_CountsThemUntilTheyAgeOut()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8691 drop counts test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var now = WholeSecond(DateTime.UtcNow);

        /* Two dropped rows the older build kept: one dropped two hours ago, one two days ago. Held means expired_at set with no
           attempt. One drop was counted by this build, an hour ago. */
        await ExecuteAsync(postgres, @"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at, expired_at, last_attempt_at)
VALUES
    (4401, 'query_stats', '0xLEGACY_TODAY', '0xSQL1', 0, 400, 'counts_db', '0xHASH1', 600001, $1, $1, $2, NULL),
    (4401, 'query_stats', '0xLEGACY_OLD', '0xSQL2', 0, 400, 'counts_db', '0xHASH2', 600002, $1, $1, $3, NULL);",
            now.AddDays(-3), now.AddHours(-2), now.AddDays(-2));
        await ExecuteAsync(postgres, @"
INSERT INTO collect.oversized_plan_backlog_drops (server_id, collector_name, hour_start, rows_dropped, rows_first_seen)
VALUES (4401, 'query_stats', $1, 1, 0);",
            Hour(now).AddHours(-1));

        var lastDaySince = now.AddDays(-1);
        var before = Assert.Single(await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, HeldServerId, lastDaySince, ct));
        Assert.Equal(3, before.DroppedRows);
        Assert.Equal(3, before.TotalRows);
        /* The two-day-old drop is outside the day. */
        Assert.Equal(2, before.RowsDroppedLastDay);

        /* A pass's trim purges the rows a day old and leaves the other, which is counted still. */
        Assert.Empty(await OversizedPlanBacklogSweep.ClaimAsync(postgres, HeldServerId, ct));

        var after = Assert.Single(await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, HeldServerId, lastDaySince, ct));
        Assert.Equal(2, after.DroppedRows);
        Assert.Equal(2, after.TotalRows);
        Assert.Equal(2, after.RowsDroppedLastDay);
        Assert.Equal(after.PendingRows + after.CapturedRows + after.ExpiredRows + after.DroppedRows, after.TotalRows);
    }

    /// <summary>The counts older than the day the read keeps them for leave with the trim, and a count inside it stays.</summary>
    [Fact]
    public async Task TheTrimPurgesCountsOlderThanTheDay_AndKeepsTheRest()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8691 drop counts test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var now = WholeSecond(DateTime.UtcNow);
        await ExecuteAsync(postgres, @"
INSERT INTO collect.oversized_plan_backlog_drops (server_id, collector_name, hour_start, rows_dropped, rows_first_seen)
VALUES
    (4401, 'query_stats', $1, 7, 0),
    (4401, 'query_stats', $2, 3, 0);",
            Hour(now).AddHours(-30), Hour(now).AddHours(-1));

        Assert.Empty(await OversizedPlanBacklogSweep.ClaimAsync(postgres, HeldServerId, ct));

        var rollup = Assert.Single(await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, HeldServerId, now.AddDays(-1), ct));
        Assert.Equal(3, rollup.DroppedRows);
    }

    private static async Task ExecuteAsync(NpgsqlDataSource postgres, string sql, params DateTime[] stamps)
    {
        await using var command = postgres.CreateCommand(sql);
        foreach (var stamp in stamps)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });
        }

        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task MigrateAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using var migrate = new NpgsqlConnection(scratch.ConnectionString);
        await migrate.OpenAsync(ct);
        await PgMigrations.MigrateAsync(migrate, null, ct);

        await using var registry = new NpgsqlCommand(ServerSql, migrate);
        await registry.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A naive-UTC stamp on a whole second, so it reads back from a <c>timestamp</c> column unchanged.</summary>
    private static DateTime WholeSecond(DateTime utc) =>
        new(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, utc.Second, DateTimeKind.Unspecified);

    private static DateTime Hour(DateTime utc) =>
        new(utc.Year, utc.Month, utc.Day, utc.Hour, 0, 0, DateTimeKind.Unspecified);

    private static string ReadConnectionString(ScratchPostgres scratch) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString;
}
