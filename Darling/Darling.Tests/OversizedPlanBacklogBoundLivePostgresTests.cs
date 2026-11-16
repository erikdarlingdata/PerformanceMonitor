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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   shared, so it needs no place in the live-postgres collection. */

/// <summary>
/// #8567: <c>collect.oversized_plan_backlog</c> stays bounded when sightings arrive faster than the sweep's claim
/// drains them. Intake and sweep are driven by calls: <see cref="OversizedPlanBacklog.RecordSightingsAsync"/> is the
/// collector's write, <see cref="OversizedPlanBacklogSweep.ClaimAsync"/> is one server's step of a pass, and a
/// claimed row is drained with the sweep's own capture statement in place of a fetch from a monitored server.
/// </summary>
public sealed class OversizedPlanBacklogBoundLivePostgresTests
{
    private const int ServerId = 4301;
    private const string ServerName = "backlog-bound-server";
    private const string Database = "bound_db";

    /// <summary>Queries whose plans are over the cap, three times what one claim takes.</summary>
    private const int Queries = 3 * OversizedPlanBacklogSweep.MaxPlansPerServerPerTick;

    private const int Rounds = 8;

    private const string ServerSql = @"
INSERT INTO collect.servers (server_id, server_name, is_enabled)
VALUES (4301, 'backlog-bound-server', TRUE);";

    private const string PendingSql = @"
SELECT count(*), min(last_seen_at)
FROM collect.oversized_plan_backlog
WHERE server_id = $1
AND   captured_at IS NULL
AND   expired_at IS NULL;";

    /// <summary>
    /// Every round re-keys the same thirty queries under a fresh plan handle, the shape plan-cache churn gives one
    /// query's plan, and the claim takes ten. One pending row a query is all the read surfaces can use: they serve
    /// the latest capture for a query, not one for each handle it ever had.
    /// </summary>
    [Fact]
    public async Task ReKeyedSightingsOfTheSameQueries_ArrivingFasterThanTheClaimDrains_LeaveOnePendingRowAQuery()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8567 backlog bound test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var start = WholeSecond(DateTime.UtcNow.AddHours(-6));
        var lastRoundAt = await RunRoundsAsync(postgres, start, ct);

        /* What is left is the newest sighting of each query still waiting: nothing sighted in an earlier round. */
        var (pending, oldestSighting) = await PendingAsync(postgres, ct);
        Assert.InRange(pending, 1, Queries);
        Assert.Equal(lastRoundAt, oldestSighting);
    }

    /// <summary>
    /// The read reports the rows that left pending without a fetch as their own bucket, so the partition still sums
    /// to the total and the day's drain still counts fetches alone.
    /// </summary>
    [Fact]
    public async Task TheRead_CountsRowsDroppedUnfetched_AsTheirOwnBucket_AndNeverAsDrain()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8567 backlog bound test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var start = WholeSecond(DateTime.UtcNow.AddHours(-6));
        await RunRoundsAsync(postgres, start, ct);

        var json = await DarlingMcpOversizedPlanBacklogTools.GetOversizedPlanBacklog(postgres, cancellationToken: ct);
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var server = root.GetProperty("by_server").EnumerateArray().Single(s => s.GetProperty("server_id").GetInt32() == ServerId);

        Assert.True(server.TryGetProperty("dropped_rows", out var droppedRows), "by_server carries no dropped_rows: " + Head(json));
        Assert.True(server.TryGetProperty("rows_dropped_last_day", out var droppedLastDay), "by_server carries no rows_dropped_last_day: " + Head(json));

        /* Every round captures one claim. Of the thirty sightings a round adds, the twenty the claim did not take
           are superseded by the next round's sightings of the same queries, and the last round's twenty still wait. */
        var captured = (long)Rounds * OversizedPlanBacklogSweep.MaxPlansPerServerPerTick;
        var waiting = (long)Queries - OversizedPlanBacklogSweep.MaxPlansPerServerPerTick;
        var dropped = (Rounds - 1) * waiting;

        Assert.Equal((long)Rounds * Queries, server.GetProperty("total_rows").GetInt64());
        Assert.Equal(waiting, server.GetProperty("pending_rows").GetInt64());
        Assert.Equal(captured, server.GetProperty("captured_rows").GetInt64());
        Assert.Equal(0L, server.GetProperty("expired_rows").GetInt64());
        Assert.Equal(dropped, droppedRows.GetInt64());
        Assert.Equal(dropped, droppedLastDay.GetInt64());

        /* Drain is what the fetch half did: a drop is not a fetch, and no fetch found a handle gone. */
        Assert.Equal(captured, server.GetProperty("rows_drained_last_day").GetInt64());
        Assert.Equal(0L, server.GetProperty("rows_expired_last_day").GetInt64());
        Assert.Equal(JsonValueKind.Null, server.GetProperty("last_expired_at").ValueKind);

        /* The cap a server's pending rows are held to: what the sweep's ceiling drains in one retention period. */
        Assert.True(root.TryGetProperty("pending_rows_cap_per_server", out var cap), "the payload carries no pending_rows_cap_per_server: " + Head(json));
        Assert.Equal(PendingCap, cap.GetInt32());

        var census = root.GetProperty("by_collector").EnumerateArray().Single();
        Assert.Equal(dropped, census.GetProperty("dropped_rows").GetInt64());
        Assert.Equal(0L, census.GetProperty("expired_rows").GetInt64());
    }

    /// <summary>
    /// Distinct queries past the cap: the claim keeps the most recently sighted rows up to the cap, whatever their
    /// collector, and a row with no query hash is its own plan, never one query with every other such row.
    /// </summary>
    [Fact]
    public async Task PendingRowsPastTheCap_AreCutToTheMostRecentlySighted()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8567 backlog bound test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        const int over = 500;
        const int withoutAHash = 100;
        var firstSighting = WholeSecond(DateTime.UtcNow.AddHours(-12));

        /* One row a second, each a different query; the newest hundred are module plans, which carry no query hash. */
        await using (var seed = postgres.CreateCommand(@"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
SELECT
    $1,
    CASE WHEN i > $3 - $4 THEN 'procedure_stats' ELSE 'query_stats' END,
    '0xPLAN' || i,
    '0xSQL' || i,
    0,
    0,
    'bound_db',
    CASE WHEN i > $3 - $4 THEN NULL ELSE '0xHASH' || i END,
    600000 + i,
    $2 + make_interval(secs => i),
    $2 + make_interval(secs => i)
FROM generate_series(1, $3) AS i;"))
        {
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = firstSighting });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = PendingCap + over });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = withoutAHash });
            await seed.ExecuteNonQueryAsync(ct);
        }

        var claimed = await OversizedPlanBacklogSweep.ClaimAsync(postgres, ServerId, ct);
        Assert.Equal(OversizedPlanBacklogSweep.MaxPlansPerServerPerTick, claimed.Count);

        /* The five hundred least recently sighted rows are the ones that left. */
        var (pending, oldestSighting) = await PendingAsync(postgres, ct);
        Assert.Equal(PendingCap, pending);
        Assert.Equal(firstSighting.AddSeconds(over + 1), oldestSighting);

        await using var modulePlans = postgres.CreateCommand(@"
SELECT count(*)
FROM collect.oversized_plan_backlog
WHERE server_id = $1
AND   query_hash IS NULL
AND   captured_at IS NULL
AND   expired_at IS NULL;");
        modulePlans.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        Assert.Equal((long)withoutAHash, (long)(await modulePlans.ExecuteScalarAsync(ct))!);
    }

    /// <summary>
    /// A row an older build dropped unfetched and held (#8691: a build with the drop counts deletes it at once instead) leaves
    /// the table a day later, so the rows an upgrade finds held age out. A row a fetch found no plan for is not that: it stays
    /// until retention's own horizon on its last sighting.
    /// </summary>
    [Fact]
    public async Task ALegacyHeldDroppedRow_LeavesTheTableADayLater_AndAFetchExpiryStays()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8567 backlog bound test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var now = WholeSecond(DateTime.UtcNow);

        await using (var seed = postgres.CreateCommand(@"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at, expired_at, last_attempt_at)
VALUES
    /* stopped waiting without a fetch, two days ago */
    (4301, 'query_stats', '0xDROPPED_OLD_1', '0xSQL1', 0, 400, 'bound_db', '0xHASH1', 600001, $1, $1, $2, NULL),
    (4301, 'query_stats', '0xDROPPED_OLD_2', '0xSQL2', 0, 400, 'bound_db', '0xHASH2', 600002, $1, $1, $2, NULL),
    /* stopped waiting without a fetch, two hours ago */
    (4301, 'query_stats', '0xDROPPED_TODAY', '0xSQL3', 0, 400, 'bound_db', '0xHASH3', 600003, $1, $1, $3, NULL),
    /* a fetch found no plan for the handle, two days ago */
    (4301, 'query_stats', '0xFETCH_EXPIRED', '0xSQL4', 0, 400, 'bound_db', '0xHASH4', 600004, $1, $1, $2, $2);"))
        {
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now.AddDays(-3) });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now.AddDays(-2) });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now.AddHours(-2) });
            await seed.ExecuteNonQueryAsync(ct);
        }

        Assert.Empty(await OversizedPlanBacklogSweep.ClaimAsync(postgres, ServerId, ct));

        var left = new List<string>();
        await using (var read = postgres.CreateCommand(@"
SELECT plan_handle
FROM collect.oversized_plan_backlog
WHERE server_id = $1
ORDER BY plan_handle;"))
        {
            read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
            await using var reader = await read.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                left.Add(reader.GetString(0));
            }
        }

        Assert.Equal(new[] { "0xDROPPED_TODAY", "0xFETCH_EXPIRED" }, left);
    }

    /// <summary>
    /// Only a later sighting of the same query supersedes a waiting row. Rows of one query sighted in the same
    /// collection were in cache together, so each keeps waiting and each is claimed; a row the sweep has tried is
    /// never dropped; and a dropped row that is sighted again waits again, in place of the rows it now outdates. A drop
    /// deletes the row and counts it by the hour (#8691), so the re-sighting comes back as a new pending row and the read
    /// counts both drops.
    /// </summary>
    [Fact]
    public async Task OnlyALaterSightingSupersedes_ATriedRowIsKept_AndAReSightingOfADroppedPlanComesBackPending()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8567 backlog bound test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var now = WholeSecond(DateTime.UtcNow);

        await using (var seed = postgres.CreateCommand(@"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at, last_attempt_at)
VALUES
    /* one query, tried once three hours ago and not sighted since */
    (4301, 'query_stats', '0xTRIED', '0xSQL', 0, 400, 'bound_db', '0xHASH', 600001, $1, $1, $1),
    /* the same query under another handle, last sighted two hours ago */
    (4301, 'query_stats', '0xEARLIER', '0xSQL', 0, 400, 'bound_db', '0xHASH', 600002, $2, $2, NULL),
    /* and under three handles sighted together an hour ago */
    (4301, 'query_stats', '0xTOGETHER_1', '0xSQL', 0, 400, 'bound_db', '0xHASH', 600003, $3, $3, NULL),
    (4301, 'query_stats', '0xTOGETHER_2', '0xSQL', 0, 400, 'bound_db', '0xHASH', 600004, $3, $3, NULL),
    (4301, 'query_stats', '0xTOGETHER_3', '0xSQL', 0, 400, 'bound_db', '0xHASH', 600005, $3, $3, NULL);"))
        {
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now.AddHours(-3) });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now.AddHours(-2) });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now.AddHours(-1) });
            await seed.ExecuteNonQueryAsync(ct);
        }

        var claimed = await OversizedPlanBacklogSweep.ClaimAsync(postgres, ServerId, ct);
        Assert.Equal(
            new[] { "0xTOGETHER_1", "0xTOGETHER_2", "0xTOGETHER_3", "0xTRIED" },
            claimed.Select(p => p.Observation.PlanHandle).Order(StringComparer.Ordinal));

        var verdicts = await VerdictsAsync(postgres, ct);
        /* #8691: the drop deleted the row and counted it. Nothing holds it, and the read still says one row was dropped. */
        Assert.DoesNotContain("0xEARLIER", verdicts.Keys);
        Assert.Equal(4, verdicts.Values.Count(v => v == DarlingOversizedPlanBacklogReader.VerdictPending));
        var afterFirstDrop = Assert.Single(await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(
            postgres, ServerId, now.AddDays(-1), ct));
        Assert.Equal(1, afterFirstDrop.DroppedRows);
        Assert.Equal(1, afterFirstDrop.RowsDroppedLastDay);
        Assert.Equal(5, afterFirstDrop.TotalRows);

        /* The earlier handle is sighted again: it is the query's newest sighting now, so it waits and the three it outdates do not. */
        var written = await OversizedPlanBacklog.RecordSightingsAsync(
            postgres,
            ServerId,
            OversizedPlanBacklog.QueryStatsCollectorName,
            new[] { new OversizedPlanObservation("0xEARLIER", "0xSQL", 0, 400, Database, "0xHASH", 600002) },
            now.AddMinutes(-1),
            PendingCap,
            OversizedPlanBacklogSweep.MaxRowsDroppedPerServerPerTick,
            null,
            ct);
        Assert.Equal(1, written);
        Assert.Equal(DarlingOversizedPlanBacklogReader.VerdictPending, (await VerdictsAsync(postgres, ct))["0xEARLIER"]);

        claimed = await OversizedPlanBacklogSweep.ClaimAsync(postgres, ServerId, ct);
        Assert.Equal(
            new[] { "0xEARLIER", "0xTRIED" },
            claimed.Select(p => p.Observation.PlanHandle).Order(StringComparer.Ordinal));

        verdicts = await VerdictsAsync(postgres, ct);
        Assert.Equal(new[] { "0xEARLIER", "0xTRIED" }, verdicts.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(DarlingOversizedPlanBacklogReader.VerdictPending, verdicts["0xTRIED"]);

        /* Four drops in all (the first, then the three it outdated), and the plan that came back is counted as added again. */
        var afterSecondDrop = Assert.Single(await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(
            postgres, ServerId, now.AddDays(-1), ct));
        Assert.Equal(4, afterSecondDrop.DroppedRows);
        Assert.Equal(4, afterSecondDrop.RowsDroppedLastDay);
        Assert.Equal(6, afterSecondDrop.TotalRows);
        Assert.Equal(6, afterSecondDrop.RowsAddedLastDay);
        Assert.Equal(2, afterSecondDrop.PendingRows);
        Assert.Equal(600_001, afterSecondDrop.MinObservedBytes);
    }

    /// <summary>
    /// The cap is held against rows that are each the newest sighting of their query. A server whose distinct
    /// queries fit under the cap loses none of them while one query's stale re-keys, more than one pass drops,
    /// are still being cleared.
    /// </summary>
    [Fact]
    public async Task TheCapWaitsForTheSupersedeToFinish_SoAQuerysOnlyRowIsNotCutWhileReKeysRemain()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8567 backlog bound test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var distinctQueries = PendingCap - 100;
        var reKeys = 2 * OversizedPlanBacklogSweep.MaxRowsDroppedPerServerPerTick;
        var firstSighting = WholeSecond(DateTime.UtcNow.AddHours(-20));

        /* The distinct queries were sighted first, one a second; the one re-keyed query's handles after them. */
        await using (var seed = postgres.CreateCommand(@"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
SELECT
    $1,
    'query_stats',
    '0xPLAN' || i,
    '0xSQL' || i,
    0,
    400,
    'bound_db',
    CASE WHEN i > $3 THEN '0xREKEYED' ELSE '0xHASH' || i END,
    600000 + i,
    $2 + make_interval(secs => i),
    $2 + make_interval(secs => i)
FROM generate_series(1, $3 + $4) AS i;"))
        {
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = firstSighting });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = distinctQueries });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = reKeys });
            await seed.ExecuteNonQueryAsync(ct);
        }

        /* Two passes: the first stops at the per-statement limit, the second drops the rest of the re-keys. */
        await OversizedPlanBacklogSweep.ClaimAsync(postgres, ServerId, ct);
        await OversizedPlanBacklogSweep.ClaimAsync(postgres, ServerId, ct);

        await using var waiting = postgres.CreateCommand(@"
SELECT
    count(*) FILTER (WHERE query_hash <> '0xREKEYED'),
    count(*) FILTER (WHERE query_hash = '0xREKEYED')
FROM collect.oversized_plan_backlog
WHERE server_id = $1
AND   captured_at IS NULL
AND   expired_at IS NULL;");
        waiting.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        await using var reader = await waiting.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal((long)distinctQueries, reader.GetInt64(0));
        Assert.Equal(1L, reader.GetInt64(1));
    }

    /// <summary>Each row's verdict as the read lists it, by plan handle.</summary>
    private static async Task<Dictionary<string, string>> VerdictsAsync(NpgsqlDataSource postgres, CancellationToken ct) =>
        (await DarlingOversizedPlanBacklogReader.GetRowsAsync(postgres, ServerId, 100, ct))
        .ToDictionary(r => r.PlanHandle, r => r.Verdict, StringComparer.Ordinal);

    /// <summary>What the sweep's ceiling drains from one server in one retention period.</summary>
    private static int PendingCap =>
        OversizedPlanBacklogSweep.RowsPerServerPerDayCeiling * DarlingRetention.OversizedPlanBacklogRetentionDays;

    /// <summary>
    /// <see cref="Rounds"/> rounds a sweep interval apart: thirty sightings, one claim, and every claimed row captured.
    /// Pending is held to one row a query after every round. Returns the last round's sighting time.
    /// </summary>
    private static async Task<DateTime> RunRoundsAsync(NpgsqlDataSource postgres, DateTime start, CancellationToken ct)
    {
        var at = start;

        for (var round = 0; round < Rounds; round++)
        {
            at = start + (round * OversizedPlanBacklogSweep.SweepInterval);

            var sightings = new List<OversizedPlanObservation>(Queries);
            for (var query = 0; query < Queries; query++)
            {
                sightings.Add(new OversizedPlanObservation(
                    string.Create(CultureInfo.InvariantCulture, $"0xPLAN_{query:D2}_{round:D2}"),
                    string.Create(CultureInfo.InvariantCulture, $"0xSQL_{query:D2}"),
                    0,
                    400,
                    Database,
                    string.Create(CultureInfo.InvariantCulture, $"0xHASH_{query:D2}"),
                    600000 + (query * 1000)));
            }

            var written = await OversizedPlanBacklog.RecordSightingsAsync(
                postgres, ServerId, OversizedPlanBacklog.QueryStatsCollectorName, sightings, at,
                PendingCap, OversizedPlanBacklogSweep.MaxRowsDroppedPerServerPerTick, null, ct);
            Assert.Equal(Queries, written);

            var claimed = await OversizedPlanBacklogSweep.ClaimAsync(postgres, ServerId, ct);
            Assert.Equal(OversizedPlanBacklogSweep.MaxPlansPerServerPerTick, claimed.Count);

            foreach (var plan in claimed)
            {
                await CaptureAsync(postgres, plan, at.AddMinutes(1), ct);
            }

            var (pending, _) = await PendingAsync(postgres, ct);
            Assert.True(
                pending <= Queries,
                string.Create(CultureInfo.InvariantCulture, $"after round {round + 1} there are {pending} pending rows for {Queries} queries"));
        }

        return at;
    }

    /// <summary>The sweep's capture write for one claimed row, with a small document in place of a fetched plan.</summary>
    private static async Task CaptureAsync(
        NpgsqlDataSource postgres, OversizedPlanBacklog.PendingPlan plan, DateTime at, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(
            OversizedPlanBacklogSweep.OutcomeSql(OversizedPlanBacklogSweep.PlanFetchVerdict.Captured));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = plan.CollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = plan.Observation.PlanHandle });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = plan.Observation.SqlHandle });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = plan.Observation.StatementStartOffset });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = plan.Observation.StatementEndOffset });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = at });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = "<ShowPlanXML />" });
        Assert.Equal(1, await command.ExecuteNonQueryAsync(ct));
    }

    private static async Task<(long Pending, DateTime? OldestSighting)> PendingAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(PendingSql);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1));
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

    private static string Head(string json) => json.Length <= 300 ? json : json[..300];

    private static string ReadConnectionString(ScratchPostgres scratch) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString;
}
