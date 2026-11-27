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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   shared, so it needs no place in the live-postgres collection. */

/// <summary>
/// #8789: <see cref="OversizedPlanBacklog.RecordSightingsAsync"/> writes a cycle's sightings as one statement, and does not insert
/// a new row the trim would drop in the same pass. A full pass is the sightings and then <see cref="OversizedPlanBacklogSweep.TrimAsync"/>.
/// The reference pass is the code this replaced: one upsert a row, each its own commit.
/// </summary>
public sealed class OversizedPlanBacklogSightingsLivePostgresTests
{
    private const int ReferenceServer = 4601;
    private const int BatchedServer = 4602;
    private const string Database = "sightings_db";

    private static int Cap => OversizedPlanBacklogSweep.PendingRowsPerServerCap;

    private static int Limit => OversizedPlanBacklogSweep.MaxRowsDroppedPerServerPerTick;

    /// <summary>The statement the sightings were written with before #8789, one execution a row.</summary>
    private const string ReferenceUpsertSql = @"
INSERT INTO collect.oversized_plan_backlog AS b
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
ON CONFLICT ON CONSTRAINT pk_oversized_plan_backlog DO UPDATE SET
    last_seen_at = EXCLUDED.last_seen_at,
    observed_bytes = EXCLUDED.observed_bytes,
    database_name = COALESCE(EXCLUDED.database_name, b.database_name),
    query_hash = COALESCE(EXCLUDED.query_hash, b.query_hash),
    expired_at = NULL;";

    /// <summary>The seed: rows of another collector filling the cap but fifty, thirty old rows of this one, and one tried row.</summary>
    private const string SeedSql = @"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
SELECT $1, 'procedure_stats', '0xPROC' || i, '0xPSQL' || i, 0, 0, 'sightings_db', NULL, 1000 + i,
       $2 - interval '1 hour', $2
FROM generate_series(1, $3 - 50) AS i;

INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
SELECT $1, 'query_stats', '0xOLD' || i, '0xOSQL' || i, 0, 400, 'sightings_db',
       CASE WHEN i <= 10 THEN 'H_' || i ELSE 'OLD_' || i END, 700 + i,
       $2 - interval '3 hours', $2 - interval '2 hours'
FROM generate_series(1, 30) AS i;

INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at, last_attempt_at)
VALUES ($1, 'query_stats', '0xTRIED', '0xTSQL', 0, 400, 'sightings_db', 'T_1', 5000,
        $2 - interval '5 hours', $2 - interval '4 hours', $2 - interval '1 hour');";

    private const string TableRowsSql = @"
SELECT (to_jsonb(b) - 'server_id')::text
FROM collect.oversized_plan_backlog AS b
WHERE server_id = $1
ORDER BY 1;";

    private const string DropTotalsSql = @"
SELECT COALESCE(sum(rows_dropped), 0), COALESCE(sum(rows_first_seen), 0)
FROM collect.oversized_plan_backlog_drops
WHERE server_id = $1;";

    private const string StatsSql = @"
SELECT n_tup_ins, n_tup_del, n_tup_upd
FROM pg_stat_user_tables
WHERE schemaname = 'collect'
AND   relname = 'oversized_plan_backlog';";

    /// <summary>
    /// The batched pass leaves the table as the reference pass does, row for row, on a server over the cap: rows that share a
    /// query hash with older rows and rows without one, a key given twice in a batch, a tried row, and sightings of rows the
    /// table already holds. It also drops the same rows and counts them the same, and it did not insert the new rows the
    /// cap would drop.
    /// </summary>
    [Fact]
    public async Task ABatchedPass_LeavesTheTableAsTheOneRowAtATimePassDid_OnAServerOverTheCap()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8789 sightings test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var now = WholeSecond(DateTime.UtcNow);
        var seenAt = now.AddMinutes(-1);
        foreach (var server in new[] { ReferenceServer, BatchedServer })
        {
            await SeedAsync(postgres, server, now, ct);
        }

        var sightings = Sightings();

        await ReferenceSightingsAsync(postgres, ReferenceServer, sightings, seenAt, ct);
        await OversizedPlanBacklog.RecordSightingsAsync(
            postgres, BatchedServer, OversizedPlanBacklog.QueryStatsCollectorName, sightings, seenAt, Cap, Limit, null, ct);

        /* Of the 200 new rows, 155 rank past the cap. The ten that share a hash with an older row are the lowest by bytes and
           are the only rows that make the older ones superseded, so they stay for the trim; the other 145 are not inserted.
           The sightings record that number and count nothing: the pass's trim counts it. */
        Assert.Equal(145L, await LeftOutAsync(postgres, BatchedServer, ct));
        Assert.Equal((0L, 0L), await DropTotalsAsync(postgres, BatchedServer, ct));

        await TrimAsync(postgres, ReferenceServer, now, oldRanking: false, ct);
        await TrimAsync(postgres, BatchedServer, now, oldRanking: false, ct);

        Assert.Null(await LeftOutAsync(postgres, BatchedServer, ct));

        var reference = await TableRowsAsync(postgres, ReferenceServer, ct);
        var batched = await TableRowsAsync(postgres, BatchedServer, ct);

        Assert.Equal(Cap + 1, reference.Count);
        Assert.Equal(reference, batched);
        Assert.Equal(await DropTotalsAsync(postgres, ReferenceServer, ct), await DropTotalsAsync(postgres, BatchedServer, ct));
    }

    /// <summary>
    /// New rows that tie the rows already in the table on the sighting time rank after them, so a table at the cap keeps the rows it
    /// has and the new ones are not inserted.
    /// </summary>
    [Fact]
    public async Task NewRowsThatTieTheRowsAlreadyInTheTable_RankAfterThem_PastTheCap()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8789 sightings test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var now = WholeSecond(DateTime.UtcNow);

        /* A full table, every row last sighted at the instant the next sighting carries, and first sighted an hour before. */
        await using (var seed = postgres.CreateCommand(@"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
SELECT $1, 'query_stats', '0xHELD' || i, '0xHSQL' || i, 0, 400, 'sightings_db', NULL, 1000 + i,
       $2 - interval '1 hour', $2
FROM generate_series(1, $3) AS i;"))
        {
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = BatchedServer });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Cap });
            await seed.ExecuteNonQueryAsync(ct);
        }

        /* Fifty new rows with MORE bytes than any held row: bytes alone would rank them first, so only the tie-break keeps them out. */
        var sightings = Enumerable.Range(0, 50)
            .Select(k => new OversizedPlanObservation(
                "0xNEWER" + k.ToString(CultureInfo.InvariantCulture), "0xNSQL", 0, 400, Database, null, 900_000 + k))
            .ToList();
        await OversizedPlanBacklog.RecordSightingsAsync(
            postgres, BatchedServer, OversizedPlanBacklog.QueryStatsCollectorName, sightings, now, Cap, Limit, null, ct);

        Assert.Equal(0L, await CountAsync(postgres, "0xNEWER%", ct));

        /* And once they are in, as rows a stale build or a longer gap could have left there, the trim keeps the held ones. */
        await using (var insert = postgres.CreateCommand(ReferenceUpsertSql))
        {
            foreach (var row in sightings)
            {
                insert.Parameters.Clear();
                AddReferenceParameters(insert, BatchedServer, OversizedPlanBacklog.QueryStatsCollectorName, row, now);
                await insert.ExecuteNonQueryAsync(ct);
            }
        }

        await TrimAsync(postgres, BatchedServer, now, oldRanking: false, ct);

        await using var count = postgres.CreateCommand(@"
SELECT
    count(*) FILTER (WHERE plan_handle LIKE '0xHELD%'),
    count(*) FILTER (WHERE plan_handle LIKE '0xNEWER%')
FROM collect.oversized_plan_backlog
WHERE server_id = $1;");
        count.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = BatchedServer });
        await using var reader = await count.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal((long)Cap, reader.GetInt64(0));
        Assert.Equal(0L, reader.GetInt64(1));
    }

    /// <summary>
    /// A server whose sightings are over the cap, passed the same sightings again and again. The reference is the code before #8789:
    /// the one-at-a-time-style write (no left-out rows) and the trim that ranks a tied new row first. Each of its passes deletes rows
    /// and inserts the same number again, which reads as a pass that changes nothing and writes a table's worth of tuples. The batched
    /// pass inserts the cap's worth once and then neither inserts nor deletes.
    /// </summary>
    [Fact]
    public async Task RepeatPassesWithTheSameSightings_OnAServerOverTheCap_InsertAndDeleteNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8789 sightings test (it mints its own scratch database).");

        const int excess = 500;
        const int passes = 4;

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);

        /* One connection for everything, so the table's tuple counts reach the statistics from the session that did the work. */
        var single = new NpgsqlConnectionStringBuilder(ReadConnectionString(scratch)) { MaxPoolSize = 1, MinPoolSize = 0 }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(single);

        var sightings = Enumerable.Range(0, Cap + excess)
            .Select(k => new OversizedPlanObservation(
                "0xPLAN" + k.ToString("D6", CultureInfo.InvariantCulture), "0xSQL" + k.ToString("D6", CultureInfo.InvariantCulture),
                0, 400, Database, null, 600_000 + k))
            .ToList();
        var start = WholeSecond(DateTime.UtcNow.AddHours(-2));

        var before = new List<(long Ins, long Del)>();
        var after = new List<(long Ins, long Del)>();

        foreach (var (server, oldCode, into) in new[] { (ReferenceServer, true, before), (BatchedServer, false, after) })
        {
            for (var pass = 0; pass < passes; pass++)
            {
                var at = start + (pass * OversizedPlanBacklogSweep.SweepInterval);
                var (insBefore, delBefore) = await TupleCountsAsync(postgres, ct);

                if (oldCode)
                {
                    /* No simulation: a cap of int.MaxValue means the statement is the plain upsert it was. */
                    await OversizedPlanBacklog.RecordSightingsAsync(
                        postgres, server, OversizedPlanBacklog.QueryStatsCollectorName, sightings, at, int.MaxValue, Limit, null, ct);
                }
                else
                {
                    await OversizedPlanBacklog.RecordSightingsAsync(
                        postgres, server, OversizedPlanBacklog.QueryStatsCollectorName, sightings, at, Cap, Limit, null, ct);
                }

                await TrimAsync(postgres, server, at, oldRanking: oldCode, ct);

                var (insAfter, delAfter) = await TupleCountsAsync(postgres, ct);
                into.Add((insAfter - insBefore, delAfter - delBefore));
            }
        }

        var report = string.Join(
            "; ",
            Enumerable.Range(0, passes).Select(p => string.Create(
                CultureInfo.InvariantCulture, $"pass {p + 1}: before {before[p].Ins} ins / {before[p].Del} del, after {after[p].Ins} ins / {after[p].Del} del")));
        TestContext.Current.SendDiagnosticMessage("#8789 churn (cap " + Cap.ToString(CultureInfo.InvariantCulture) + ", " + excess.ToString(CultureInfo.InvariantCulture) + " over): " + report);

        /* The first pass fills the table to the cap. Before, it also inserted the rows over it and deleted them again. */
        Assert.Equal((long)(Cap + excess), before[0].Ins);
        Assert.Equal((long)excess, before[0].Del);
        Assert.Equal((long)Cap, after[0].Ins);
        Assert.Equal(0L, after[0].Del);

        /* Every later pass, before: the rows the trim dropped came back with a fresh first sighting and pushed others out. */
        foreach (var pass in Enumerable.Range(1, passes - 1))
        {
            Assert.Equal((long)excess, before[pass].Ins);
            Assert.Equal((long)excess, before[pass].Del);
            Assert.Equal((0L, 0L), after[pass]);
        }
    }

    /// <summary>
    /// A server over the cap sighted on several collection cycles between two sweep passes leaves the same new rows out of every
    /// cycle. The pass counts them once, as the trim counted a plan it deleted once per pass: the drop counts take them when the
    /// pass runs and the pass's tally takes them as over-cap drops. A cycle counts nothing itself.
    /// </summary>
    [Fact]
    public async Task SeveralCyclesBetweenTwoPasses_CountTheRowsLeftOutOncePerPass()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8789 sightings test (it mints its own scratch database).");

        const int leftOut = 50;
        const int cycles = 3;

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var now = WholeSecond(DateTime.UtcNow);
        await SeedHeldRowsAsync(postgres, BatchedServer, now.AddMinutes(10), Cap, ct);
        var sightings = NewRows(leftOut);

        /* Pass 1: three collection cycles, then the sweep's pass. */
        for (var cycle = 1; cycle <= cycles; cycle++)
        {
            await OversizedPlanBacklog.RecordSightingsAsync(
                postgres, BatchedServer, OversizedPlanBacklog.QueryStatsCollectorName, sightings, now.AddMinutes(cycle), Cap, Limit, null, ct);

            Assert.Equal((0L, 0L), await DropTotalsAsync(postgres, BatchedServer, ct));
            Assert.Equal((long)leftOut, await LeftOutAsync(postgres, BatchedServer, ct));
        }

        var firstPass = new OversizedPlanBacklogSweep.SweepTally();
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            await OversizedPlanBacklogSweep.TrimAsync(connection, BatchedServer, now.AddMinutes(15), firstPass, null, ct);
        }

        Assert.Equal(leftOut, firstPass.RowsOverCap);
        Assert.Equal(0, firstPass.RowsSuperseded);
        Assert.Equal(((long)leftOut, (long)leftOut), await DropTotalsAsync(postgres, BatchedServer, ct));
        Assert.Null(await LeftOutAsync(postgres, BatchedServer, ct));
        Assert.Equal(0L, await CountAsync(postgres, "0xNEWER%", ct));

        /* A pass with no cycle before it adds nothing. */
        var emptyPass = new OversizedPlanBacklogSweep.SweepTally();
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            await OversizedPlanBacklogSweep.TrimAsync(connection, BatchedServer, now.AddMinutes(30), emptyPass, null, ct);
        }

        Assert.Equal(0, emptyPass.RowsOverCap);
        Assert.Equal(((long)leftOut, (long)leftOut), await DropTotalsAsync(postgres, BatchedServer, ct));

        /* Pass 2: the same plans are left out again and count again, once. */
        foreach (var cycle in new[] { 16, 17 })
        {
            await OversizedPlanBacklog.RecordSightingsAsync(
                postgres, BatchedServer, OversizedPlanBacklog.QueryStatsCollectorName, sightings, now.AddMinutes(cycle), Cap, Limit, null, ct);
        }

        var secondPass = new OversizedPlanBacklogSweep.SweepTally();
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            await OversizedPlanBacklogSweep.TrimAsync(connection, BatchedServer, now.AddMinutes(45), secondPass, null, ct);
        }

        Assert.Equal(leftOut, secondPass.RowsOverCap);
        Assert.Equal(((long)(2 * leftOut), (long)(2 * leftOut)), await DropTotalsAsync(postgres, BatchedServer, ct));
    }

    /// <summary>
    /// A server that drops back under the cap leaves nothing out, so the number the last cycle recorded goes to 0 and the next pass
    /// adds nothing, even though the rows left out earlier were never counted by a pass.
    /// </summary>
    [Fact]
    public async Task AServerThatDropsBackUnderTheCap_RecordsNothingLeftOut_AndTheNextPassAddsNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8789 sightings test (it mints its own scratch database).");

        const int leftOut = 50;

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var now = WholeSecond(DateTime.UtcNow);
        await SeedHeldRowsAsync(postgres, BatchedServer, now.AddMinutes(10), Cap, ct);
        var sightings = NewRows(leftOut);

        await OversizedPlanBacklog.RecordSightingsAsync(
            postgres, BatchedServer, OversizedPlanBacklog.QueryStatsCollectorName, sightings, now.AddMinutes(1), Cap, Limit, null, ct);
        Assert.Equal((long)leftOut, await LeftOutAsync(postgres, BatchedServer, ct));

        /* The held rows leave (the sweep fetched them), so the table and the new rows are under the cap. */
        await using (var delete = postgres.CreateCommand("DELETE FROM collect.oversized_plan_backlog WHERE server_id = $1 AND plan_handle LIKE '0xHELD%';"))
        {
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = BatchedServer });
            await delete.ExecuteNonQueryAsync(ct);
        }

        var written = await OversizedPlanBacklog.RecordSightingsAsync(
            postgres, BatchedServer, OversizedPlanBacklog.QueryStatsCollectorName, sightings, now.AddMinutes(2), Cap, Limit, null, ct);
        Assert.Equal(leftOut, written);
        Assert.Equal(0L, await LeftOutAsync(postgres, BatchedServer, ct));
        Assert.Equal((long)leftOut, await CountAsync(postgres, "0xNEWER%", ct));

        var pass = new OversizedPlanBacklogSweep.SweepTally();
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            await OversizedPlanBacklogSweep.TrimAsync(connection, BatchedServer, now.AddMinutes(15), pass, null, ct);
        }

        Assert.Equal(0, pass.RowsOverCap);
        Assert.Equal((0L, 0L), await DropTotalsAsync(postgres, BatchedServer, ct));
        Assert.Null(await LeftOutAsync(postgres, BatchedServer, ct));
    }

    /// <summary>A table at the cap: rows of this collector, first sighted an hour before <paramref name="lastSeen"/> and last sighted at it.</summary>
    private static async Task SeedHeldRowsAsync(NpgsqlDataSource postgres, int server, DateTime lastSeen, int rows, CancellationToken ct)
    {
        await using var seed = postgres.CreateCommand(@"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
SELECT $1, 'query_stats', '0xHELD' || i, '0xHSQL' || i, 0, 400, 'sightings_db', NULL, 1000 + i,
       $2 - interval '1 hour', $2
FROM generate_series(1, $3) AS i;");
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = lastSeen });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = rows });
        await seed.ExecuteNonQueryAsync(ct);
    }

    /// <summary>New rows with MORE bytes than any held row, so only the tie-break or a later sighting time could keep them out.</summary>
    private static List<OversizedPlanObservation> NewRows(int count) =>
        Enumerable.Range(0, count)
            .Select(k => new OversizedPlanObservation(
                "0xNEWER" + k.ToString(CultureInfo.InvariantCulture), "0xNSQL", 0, 400, Database, null, 900_000 + k))
            .ToList();

    /// <summary>The number the sightings recorded as left out for the query_stats collector, or null when none is recorded.</summary>
    private static async Task<long?> LeftOutAsync(NpgsqlDataSource postgres, int server, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(@"
SELECT state_value::bigint
FROM collect.collector_state
WHERE server_id = $1
AND   collector_name = 'oversized_plan_backlog'
AND   state_key = 'query_stats';");
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private static List<OversizedPlanObservation> Sightings()
    {
        var sightings = new List<OversizedPlanObservation>();

        /* Sightings of rows the table holds, under the hash they have. */
        for (var i = 11; i <= 15; i++)
        {
            sightings.Add(new OversizedPlanObservation(
                "0xOLD" + i.ToString(CultureInfo.InvariantCulture), "0xOSQL" + i.ToString(CultureInfo.InvariantCulture),
                0, 400, Database, "OLD_" + i.ToString(CultureInfo.InvariantCulture), 800 + i));
        }

        for (var k = 0; k < 200; k++)
        {
            /* Ten share a hash with an older row and carry the fewest bytes, a third have no hash, the rest each have their own. */
            var hash = k < 10
                ? "H_" + (k + 1).ToString(CultureInfo.InvariantCulture)
                : (k % 3 == 0 ? null : "N_" + k.ToString(CultureInfo.InvariantCulture));
            var bytes = k < 10 ? 100 + k : 50_000 + (k * 7);
            sightings.Add(new OversizedPlanObservation(
                "0xNEW" + k.ToString(CultureInfo.InvariantCulture), "0xNSQL" + k.ToString(CultureInfo.InvariantCulture),
                0, 400, Database, hash, bytes));
        }

        /* One key twice in a batch: the later bytes win, and a null database or hash does not erase the earlier one. */
        sightings.Add(new OversizedPlanObservation("0xNEW20", "0xNSQL20", 0, 400, null, null, 77_777));

        return sightings;
    }

    private static async Task SeedAsync(NpgsqlDataSource postgres, int server, DateTime now, CancellationToken ct)
    {
        foreach (var statement in Regex.Split(SeedSql, @";\s*\n\s*\n"))
        {
            await using var seed = postgres.CreateCommand(statement);
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now });
            if (statement.Contains("$3", StringComparison.Ordinal))
            {
                seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Cap });
            }

            await seed.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>The sightings as the code before #8789 wrote them: one statement each.</summary>
    private static async Task ReferenceSightingsAsync(
        NpgsqlDataSource postgres, int server, IReadOnlyList<OversizedPlanObservation> sightings, DateTime seenAt, CancellationToken ct)
    {
        await using var connection = await postgres.OpenConnectionAsync(ct);
        foreach (var row in sightings)
        {
            await using var command = new NpgsqlCommand(ReferenceUpsertSql, connection);
            AddReferenceParameters(command, server, OversizedPlanBacklog.QueryStatsCollectorName, row, seenAt);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static void AddReferenceParameters(
        NpgsqlCommand command, int server, string collector, OversizedPlanObservation row, DateTime seenAt)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = collector });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = row.PlanHandle });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = row.SqlHandle });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = row.StatementStartOffset });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = row.StatementEndOffset });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)row.DatabaseName ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)row.QueryHash ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = row.ObservedBytes });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = seenAt });
    }

    /// <summary>
    /// The trim of one pass. The current trim, or with <paramref name="oldRanking"/> the same one with the cap ranking a tied new
    /// row first, as it did before #8789.
    /// </summary>
    private static async Task TrimAsync(NpgsqlDataSource postgres, int server, DateTime now, bool oldRanking, CancellationToken ct)
    {
        await using var connection = await postgres.OpenConnectionAsync(ct);
        if (!oldRanking)
        {
            await OversizedPlanBacklogSweep.TrimAsync(connection, server, now, null, null, ct);
            return;
        }

        var naive = DateTime.SpecifyKind(now, DateTimeKind.Unspecified);
        int superseded;
        await using (var supersede = new NpgsqlCommand(OversizedPlanBacklog.SupersedeSql, connection))
        {
            supersede.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
            supersede.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = naive });
            supersede.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = naive - OversizedPlanBacklogSweep.DroppedRowHold });
            supersede.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Limit });
            superseded = Convert.ToInt32(await supersede.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
        }

        if (superseded < Limit)
        {
            var oldCap = OversizedPlanBacklog.DropOverCapSql.Replace("first_seen_at ASC", "first_seen_at DESC", StringComparison.Ordinal);
            Assert.NotEqual(OversizedPlanBacklog.DropOverCapSql, oldCap);
            await using var cap = new NpgsqlCommand(oldCap, connection);
            cap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
            cap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = naive });
            cap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = naive - OversizedPlanBacklogSweep.DroppedRowHold });
            cap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Cap });
            cap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Limit });
            await cap.ExecuteScalarAsync(ct);
        }
    }

    private static async Task<long> CountAsync(NpgsqlDataSource postgres, string planHandlePattern, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(
            "SELECT count(*) FROM collect.oversized_plan_backlog WHERE server_id = $1 AND plan_handle LIKE $2;");
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = BatchedServer });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = planHandlePattern });
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<List<string>> TableRowsAsync(NpgsqlDataSource postgres, int server, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(TableRowsSql);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }

    private static async Task<(long Dropped, long FirstSeen)> DropTotalsAsync(NpgsqlDataSource postgres, int server, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(DropTotalsSql);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    /// <summary>The table's tuple counts as the statistics report them, after the session's pending counts are flushed.</summary>
    private static async Task<(long Ins, long Del)> TupleCountsAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        await using var connection = await postgres.OpenConnectionAsync(ct);

        foreach (var sql in new[] { "SELECT pg_stat_force_next_flush();", "SELECT 1;", "SELECT pg_stat_clear_snapshot();" })
        {
            await using var step = new NpgsqlCommand(sql, connection);
            await step.ExecuteScalarAsync(ct);
        }

        await using var command = new NpgsqlCommand(StatsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    private static async Task MigrateAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using var migrate = new NpgsqlConnection(scratch.ConnectionString);
        await migrate.OpenAsync(ct);
        await PgMigrations.MigrateAsync(migrate, null, ct);

        await using var registry = new NpgsqlCommand(@"
INSERT INTO collect.servers (server_id, server_name, is_enabled)
VALUES (4601, 'sightings-reference-server', TRUE), (4602, 'sightings-batched-server', TRUE);", migrate);
        await registry.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A naive-UTC stamp on a whole second, so it reads back from a <c>timestamp</c> column unchanged.</summary>
    private static DateTime WholeSecond(DateTime utc) =>
        new(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, utc.Second, DateTimeKind.Unspecified);

    private static string ReadConnectionString(ScratchPostgres scratch) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString;
}
