/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   shared, so it needs no place in the live-postgres collection. */

/// <summary>
/// #8404: <c>get_oversized_plan_backlog</c> carries, beside each server's pending figure, the rows first sighted,
/// captured and expired over the day before the read, the sweep passes that ran in that day (its run records under
/// the fleet sentinel) and the drain those passes could reach per server, so a reader can tell a backlog that is
/// draining from one that is not, and a drain at what the passes allowed from one that fell short of it. The day
/// counts are stamped against the read's own clock, so the first server's rows are seeded relative to it; the
/// second server's rows sit on a fixed day, which puts them outside the read's day and lets the reader's own
/// boundary be tested with a day start the test chooses.
/// </summary>
public sealed class OversizedPlanBacklogDayRatesLivePostgresTests
{
    private const int ServerId = 4101;
    private const string ServerName = "day-rates-server";
    private const int BoundaryServerId = 4102;
    private const string BoundaryServerName = "day-rates-boundary-server";

    /// <summary>The fixed day start the boundary rows are stamped around.</summary>
    private static readonly DateTime BoundaryDayStart = new(2026, 9, 13, 0, 0, 0, DateTimeKind.Unspecified);

    private const string ServerSql = @"
INSERT INTO collect.servers (server_id, server_name, is_enabled)
VALUES (4101, 'day-rates-server', TRUE), (4102, 'day-rates-boundary-server', TRUE);";

    /// <summary>
    /// Seven rows for the first server, stamped as offsets from the read's clock: <c>$1</c> two hours back,
    /// <c>$2</c> thirty minutes back, <c>$3</c> one hour back, <c>$4</c> two days back, <c>$5</c> three days back.
    /// Six rows for the second server on a fixed day: one of each stamp exactly at <see cref="BoundaryDayStart"/>
    /// and one a second before it.
    /// </summary>
    private const string FixtureSql = @"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at,
 plan_xml, captured_at, expired_at, last_attempt_at, attempt_count)
VALUES
    /* pending, first sighted two hours ago: a first sighting in the last day */
    (4101, 'query_stats', '0xADDED_TODAY', '0xSQL1', 0, 400, 'fixture_db', '0xHASH1',
     1000000, $1, $1, NULL, NULL, NULL, NULL, 0),
    /* pending, first sighted three days ago */
    (4101, 'query_stats', '0xADDED_EARLIER', '0xSQL2', 0, 400, 'fixture_db', '0xHASH2',
     900000, $5, $1, NULL, NULL, NULL, NULL, 0),
    /* captured an hour ago */
    (4101, 'query_stats', '0xCAPTURED_TODAY', '0xSQL3', 0, 400, 'fixture_db', '0xHASH3',
     800000, $5, $1, '<ShowPlanXML />', $3, NULL, $3, 1),
    /* captured two days ago */
    (4101, 'query_stats', '0xCAPTURED_EARLIER', '0xSQL4', 0, 400, 'fixture_db', '0xHASH4',
     750000, $5, $4, '<ShowPlanXML />', $4, NULL, $4, 1),
    /* expired thirty minutes ago */
    (4101, 'procedure_stats', '0xEXPIRED_TODAY', '0xSQL5', 0, 0, 'fixture_db', NULL,
     600000, $5, $1, NULL, NULL, $2, $2, 1),
    /* expired two days ago */
    (4101, 'procedure_stats', '0xEXPIRED_EARLIER', '0xSQL6', 0, 0, 'fixture_db', NULL,
     550000, $5, $4, NULL, NULL, $4, $4, 1),
    /* both stamps inside the day: captured, and counted as captured, never as expired */
    (4101, 'procedure_stats', '0xBOTH_TODAY', '0xSQL7', 0, 0, 'fixture_db', NULL,
     700000, $5, $1, NULL, $3, $2, $3, 2),
    /* the boundary server: first sighted exactly at the fixed day start, and a second before it */
    (4102, 'query_stats', '0xBOUND_ADDED_AT', '0xSQL8', 0, 400, 'fixture_db', '0xHASH8',
     500000, '2026-09-13 00:00:00', '2026-09-13 00:00:00', NULL, NULL, NULL, NULL, 0),
    (4102, 'query_stats', '0xBOUND_ADDED_BEFORE', '0xSQL9', 0, 400, 'fixture_db', '0xHASH9',
     500000, '2026-09-12 23:59:59', '2026-09-12 23:59:59', NULL, NULL, NULL, NULL, 0),
    /* captured exactly at the day start, and a second before it */
    (4102, 'query_stats', '0xBOUND_CAPTURED_AT', '0xSQL10', 0, 400, 'fixture_db', '0xHASH10',
     500000, '2026-09-10 08:00:00', '2026-09-13 00:00:00', '<ShowPlanXML />', '2026-09-13 00:00:00', NULL,
     '2026-09-13 00:00:00', 1),
    (4102, 'query_stats', '0xBOUND_CAPTURED_BEFORE', '0xSQL11', 0, 400, 'fixture_db', '0xHASH11',
     500000, '2026-09-10 08:00:00', '2026-09-12 23:59:59', '<ShowPlanXML />', '2026-09-12 23:59:59', NULL,
     '2026-09-12 23:59:59', 1),
    /* expired exactly at the day start, and a second before it */
    (4102, 'procedure_stats', '0xBOUND_EXPIRED_AT', '0xSQL12', 0, 0, 'fixture_db', NULL,
     500000, '2026-09-10 08:00:00', '2026-09-13 00:00:00', NULL, NULL, '2026-09-13 00:00:00',
     '2026-09-13 00:00:00', 1),
    (4102, 'procedure_stats', '0xBOUND_EXPIRED_BEFORE', '0xSQL13', 0, 0, 'fixture_db', NULL,
     500000, '2026-09-10 08:00:00', '2026-09-12 23:59:59', NULL, NULL, '2026-09-12 23:59:59',
     '2026-09-12 23:59:59', 1);";

    /// <summary>
    /// Sweep run records: three under the fleet sentinel inside the day (<c>$2</c> two hours back, <c>$3</c> thirty
    /// minutes back, <c>$4</c> one hour back), one under the sentinel two days back (<c>$7</c>), one inside the day
    /// under a real server with the sweep's collector name, and one inside the day under the sentinel for another
    /// collector. <c>$1</c> is the first log id, <c>$5</c> the sentinel id, <c>$6</c> the sweep's collector name.
    /// Only the three count.
    /// </summary>
    private const string RunRecordsSql = @"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
VALUES
    ($1, $5, '(fleet)', $6, $2, 'SUCCESS', 4),
    ($1 + 1, $5, '(fleet)', $6, $3, 'SUCCESS', 0),
    ($1 + 2, $5, '(fleet)', $6, $4, 'SUCCESS', 7),
    ($1 + 3, $5, '(fleet)', $6, $7, 'SUCCESS', 2),
    ($1 + 4, 4101, 'day-rates-server', $6, $2, 'SUCCESS', 0),
    ($1 + 5, $5, '(fleet)', 'data_retention', $2, 'SUCCESS', 0);";

    private const int PassesInsideTheDay = 3;

    [Fact]
    public async Task TheRead_CountsTheDayBeforeIt_BesidePending_WithThePassesThatRan_AndTheDrainTheyCouldReach()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8404 backlog day-rate test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await SeedAsync(scratch, ct);

        /* The reads resolve `servers` through the search path, exactly as the MCP host's pool does. */
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var before = DateTime.UtcNow;
        var json = await DarlingMcpOversizedPlanBacklogTools.GetOversizedPlanBacklog(postgres, cancellationToken: ct);
        var after = DateTime.UtcNow;

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        /* The interval's ceiling, from the sweep's two constants: plans per server per pass times passes per day. */
        Assert.True(root.TryGetProperty("sweep_ceiling_rows_per_server_per_day", out var ceiling),
            "the payload carries no sweep_ceiling_rows_per_server_per_day: " + Head(json));
        Assert.Equal(
            OversizedPlanBacklogSweep.MaxPlansPerServerPerTick * (int)(TimeSpan.FromDays(1) / OversizedPlanBacklogSweep.SweepInterval),
            ceiling.GetInt32());

        /* The passes that ran in the day, from the sweep's run records under the fleet sentinel: the three inside
           the day, not the one two days back, not the real server's row, not the other collector's. */
        Assert.True(root.TryGetProperty("sweep_passes_last_day", out var passes), "the payload carries no sweep_passes_last_day: " + Head(json));
        Assert.Equal(PassesInsideTheDay, passes.GetInt32());

        /* And the drain those passes could reach on one server. */
        Assert.True(root.TryGetProperty("sweep_reachable_rows_per_server_last_day", out var reachable),
            "the payload carries no sweep_reachable_rows_per_server_last_day: " + Head(json));
        Assert.Equal(PassesInsideTheDay * OversizedPlanBacklogSweep.MaxPlansPerServerPerTick, reachable.GetInt32());

        /* The day the counts cover starts one day before the read, stamped as UTC. */
        Assert.True(root.TryGetProperty("last_day_since", out var since), "the payload carries no last_day_since: " + Head(json));
        var sinceUtc = DateTime.Parse(since.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.InRange(sinceUtc, before.AddDays(-1).AddSeconds(-1), after.AddDays(-1).AddSeconds(1));

        var servers = root.GetProperty("by_server").EnumerateArray().ToDictionary(s => s.GetProperty("server_id").GetInt32());
        Assert.Equal(2, servers.Count);

        var server = servers[ServerId];
        Assert.Equal(ServerName, server.GetProperty("server_name").GetString());

        /* The partition is as before: seven rows, two pending, three captured (one of them also stamped expired), two expired. */
        Assert.Equal(7L, server.GetProperty("total_rows").GetInt64());
        Assert.Equal(2L, server.GetProperty("pending_rows").GetInt64());
        Assert.Equal(3L, server.GetProperty("captured_rows").GetInt64());
        Assert.Equal(2L, server.GetProperty("expired_rows").GetInt64());

        /* And the day before the read: one row first sighted, two captured, one expired, three drained. The rows
           stamped two and three days back are outside it, and the row carrying both stamps counts as captured, as
           the partition does. */
        Assert.True(server.TryGetProperty("rows_added_last_day", out var added), "by_server carries no rows_added_last_day: " + Head(json));
        Assert.Equal(1L, added.GetInt64());
        Assert.True(server.TryGetProperty("rows_captured_last_day", out var captured), "by_server carries no rows_captured_last_day: " + Head(json));
        Assert.Equal(2L, captured.GetInt64());
        Assert.True(server.TryGetProperty("rows_expired_last_day", out var expired), "by_server carries no rows_expired_last_day: " + Head(json));
        Assert.Equal(1L, expired.GetInt64());
        Assert.True(server.TryGetProperty("rows_drained_last_day", out var drained), "by_server carries no rows_drained_last_day: " + Head(json));
        Assert.Equal(3L, drained.GetInt64());

        /* The boundary server's rows all sit on a fixed day far outside the read's: every day count is zero, and
           zero is what the payload says, not null and not a missing key. */
        var boundary = servers[BoundaryServerId];
        Assert.Equal(BoundaryServerName, boundary.GetProperty("server_name").GetString());
        Assert.Equal(6L, boundary.GetProperty("total_rows").GetInt64());
        Assert.Equal(0L, boundary.GetProperty("rows_added_last_day").GetInt64());
        Assert.Equal(0L, boundary.GetProperty("rows_captured_last_day").GetInt64());
        Assert.Equal(0L, boundary.GetProperty("rows_expired_last_day").GetInt64());
        Assert.Equal(0L, boundary.GetProperty("rows_drained_last_day").GetInt64());

        /* The note tells a reader how to use them: with passes above zero it reads the drain against the passes that
           finished, and says nothing about a sweep that is not running. */
        var note = root.GetProperty("note").GetString() ?? "";
        Assert.Contains("rows_drained_last_day", note, StringComparison.Ordinal);
        Assert.Contains("sweep_passes_last_day is 3", note, StringComparison.Ordinal);
        Assert.Contains("sweep_reachable_rows_per_server_last_day", note, StringComparison.Ordinal);
        Assert.Contains("the cap for the passes that finished", note, StringComparison.Ordinal);
        Assert.DoesNotContain("no sweep pass finished", note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheDayStart_IsInclusive_AndASecondBeforeItIsOut()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8404 backlog day-boundary test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await SeedAsync(scratch, ct);

        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        /* Given the fixed day start, the row stamped exactly at it is in and the row stamped a second before is
           out, for each of the three stamps. */
        var rows = await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, BoundaryServerId, BoundaryDayStart, ct);
        var boundary = Assert.Single(rows);
        Assert.Equal(6L, boundary.TotalRows);
        Assert.Equal(1L, boundary.RowsAddedLastDay);
        Assert.Equal(1L, boundary.RowsCapturedLastDay);
        Assert.Equal(1L, boundary.RowsExpiredLastDay);

        /* A day start one second later leaves all six out. */
        var later = await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, BoundaryServerId, BoundaryDayStart.AddSeconds(1), ct);
        var none = Assert.Single(later);
        Assert.Equal(0L, none.RowsAddedLastDay);
        Assert.Equal(0L, none.RowsCapturedLastDay);
        Assert.Equal(0L, none.RowsExpiredLastDay);
    }

    [Fact]
    public async Task WithNoPassFinishedInTheDay_TheNoteSaysTheSweepIsNotRunning_NotThatTheDrainIsAtTheCap()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8404 zero-passes test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await SeedAsync(scratch, ct, withRunRecords: false);

        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));
        var json = await DarlingMcpOversizedPlanBacklogTools.GetOversizedPlanBacklog(postgres, cancellationToken: ct);

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        /* No run record in the day: zero passes, zero reachable, and pending rows still waiting. */
        Assert.Equal(0, root.GetProperty("sweep_passes_last_day").GetInt32());
        Assert.Equal(0, root.GetProperty("sweep_reachable_rows_per_server_last_day").GetInt32());
        var server = root.GetProperty("by_server").EnumerateArray().Single(s => s.GetProperty("server_id").GetInt32() == ServerId);
        Assert.Equal(2L, server.GetProperty("pending_rows").GetInt64());

        /* A drain of zero against a reachable figure of zero is not "at the cap": no pass finished, so the note
           says the sweep is not running and sends the reader to the sentinel's run record. */
        var note = root.GetProperty("note").GetString() ?? "";
        Assert.Contains("no sweep pass finished in the day", note, StringComparison.Ordinal);
        Assert.Contains("get_collection_log", note, StringComparison.Ordinal);
        Assert.DoesNotContain("at the cap", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// Migrates the scratch store and seeds the two servers and their backlog rows, and, unless told otherwise,
    /// the sweep's run records.
    /// </summary>
    private static async Task SeedAsync(ScratchPostgres scratch, CancellationToken ct, bool withRunRecords = true)
    {
        var seeded = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await using var migrate = new NpgsqlConnection(scratch.ConnectionString);
        await migrate.OpenAsync(ct);
        await PgMigrations.MigrateAsync(migrate, null, ct);

        await using (var registry = new NpgsqlCommand(ServerSql, migrate))
        {
            await registry.ExecuteNonQueryAsync(ct);
        }

        await using (var fixture = new NpgsqlCommand(FixtureSql, migrate))
        {
            fixture.Parameters.AddWithValue(seeded.AddHours(-2));
            fixture.Parameters.AddWithValue(seeded.AddMinutes(-30));
            fixture.Parameters.AddWithValue(seeded.AddHours(-1));
            fixture.Parameters.AddWithValue(seeded.AddDays(-2));
            fixture.Parameters.AddWithValue(seeded.AddDays(-3));
            await fixture.ExecuteNonQueryAsync(ct);
        }

        if (!withRunRecords)
        {
            return;
        }

        await using (var runs = new NpgsqlCommand(RunRecordsSql, migrate))
        {
            runs.Parameters.AddWithValue(CollectionIdGenerator.Next());
            runs.Parameters.AddWithValue(seeded.AddHours(-2));
            runs.Parameters.AddWithValue(seeded.AddMinutes(-30));
            runs.Parameters.AddWithValue(seeded.AddHours(-1));
            runs.Parameters.AddWithValue(DarlingObservability.FleetServerId);
            runs.Parameters.AddWithValue(DarlingObservability.OversizedPlanSweepCollectorName);
            runs.Parameters.AddWithValue(seeded.AddDays(-2));
            await runs.ExecuteNonQueryAsync(ct);
        }
    }

    private static string Head(string json) => json.Length <= 300 ? json : json[..300];

    /// <summary>
    /// The scratch database's connection string with the store's own search path, so the reads resolve the
    /// bare <c>servers</c> name the way the MCP host's pool does.
    /// </summary>
    private static string ReadConnectionString(ScratchPostgres scratch) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString;
}
