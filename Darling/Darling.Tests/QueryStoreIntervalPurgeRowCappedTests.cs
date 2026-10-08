// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5569: the retention purge of <c>collect.query_store_interval_wide</c> and
/// <c>collect.query_store_interval_latest</c> deletes in row-capped batches (the plan dimension's adaptive drain),
/// not one whole-day slice. A day of the wide table is millions of rows on a large store, so the old single
/// DELETE passed the 300 s command timeout, rolled back, and the next pass picked the same day and failed again.
///
/// <para>Source pins first (which tables take the adaptive form, and which keep the time slice), then live
/// tests on a scratch database: a multi-day backlog drains in several batches inside the command timeout and
/// leaves rows inside the horizon alone; the horizon keeps microsecond precision; a pass that hits a timeout
/// leaves the table consistent, logs the failure with the rows it reached, halves its cap, and the next pass
/// finishes the drain.</para>
///
/// <para>Both tables' <c>first_execution_time</c> is <c>timestamp NOT NULL</c> (V143/V145), so the NULL case the
/// old and new predicates would treat alike (<c>NULL &lt; $1</c> is not true, so neither deletes the row) cannot
/// occur; the predicate itself, <c>first_execution_time &lt; $1</c>, is unchanged between the two forms.</para>
/// </summary>
/* #1776 own-store: everything here lives inside its own ScratchPostgres database, so it cannot race live
   collection or any other class's tables. */
public sealed class QueryStoreIntervalPurgeRowCappedTests
{
    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the #5569 interval-purge live tests.";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>Per-table facts the shared test bodies need: the table, its horizon, and its seed statement.</summary>
    public sealed record IntervalTable(string Name, int HorizonDays)
    {
        public string Qualified => "collect." + Name;

        public override string ToString() => Name;
    }

    public static TheoryData<IntervalTable> Tables => new()
    {
        new IntervalTable(QueryStoreIntervalWide.TableName, DarlingRetention.QueryStoreIntervalWideRetentionDays),
        new IntervalTable(QueryStoreIntervalLatest.TableName, DarlingRetention.QueryStoreIntervalLatestRetentionDays),
    };

    /* ---- source pins ---------------------------------------------------------------------------------- */

    /// <summary>
    /// Both interval fact tables call <c>PurgeOneAsync</c> with the row-capped statement, the ceiling as the batch
    /// size (so the drain loop's "a full-cap batch means there may be more" contract applies), and the adaptive
    /// time column; neither uses the one-day slice. Their pending-replay tables keep the slice on
    /// <c>recorded_at</c> (small, and not the table that timed out).
    /// </summary>
    [Fact]
    public void BothIntervalFactTables_TakeTheAdaptiveRowCap_PendingTablesKeepTheSlice()
    {
        var source = ReadRetentionSource();

        foreach (var (anchor, table) in new[]
                 {
                     ("var intervalLatestDeleted = await PurgeOneAsync(", "QueryStoreIntervalLatest"),
                     ("var intervalWideDeleted = await PurgeOneAsync(", "QueryStoreIntervalWide"),
                 })
        {
            var at = source.IndexOf(anchor, StringComparison.Ordinal);
            Assert.True(at >= 0, $"the {table} purge call moved");
            var end = source.IndexOf("pacer: walPacer);", at, StringComparison.Ordinal);
            Assert.True(end > at, $"the {table} purge call has no pacer argument");
            var call = source[at..end];

            Assert.Contains($"{table}.TableName", call, StringComparison.Ordinal);
            Assert.Contains("RowCappedDeleteSql(", call, StringComparison.Ordinal);
            Assert.Contains("batchSize: IntervalDeleteRowCap", call, StringComparison.Ordinal);
            Assert.Contains("adaptiveRowCapTimeColumn: \"first_execution_time\"", call, StringComparison.Ordinal);
            Assert.DoesNotContain("TimeSlicedDeleteSql(", call, StringComparison.Ordinal);
        }

        foreach (var (anchor, table) in new[]
                 {
                     ("var intervalPendingDeleted = await PurgeOneAsync(", "QueryStoreIntervalLatest"),
                     ("var intervalWidePendingDeleted = await PurgeOneAsync(", "QueryStoreIntervalWide"),
                 })
        {
            var at = source.IndexOf(anchor, StringComparison.Ordinal);
            Assert.True(at >= 0, $"the {table} pending purge call moved");
            var end = source.IndexOf("pacer: walPacer);", at, StringComparison.Ordinal);
            var call = source[at..end];

            Assert.Contains("TimeSlicedDeleteSql(", call, StringComparison.Ordinal);
            Assert.Contains("\"recorded_at\"", call, StringComparison.Ordinal);
            Assert.DoesNotContain("adaptiveRowCapTimeColumn", call, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The statement the sweep runs for each interval table: ordered by the indexed column, capped, bound by the
    /// same strict <c>&lt; $1</c> predicate the time slice used (no cast, no truncation, so the horizon keeps its
    /// microseconds), and RETURNING the time value. The first batch of a pass has no lower bound; every later
    /// batch is bounded below by the keyset cursor (<c>&gt;= $2</c>), so it does not re-read the index entries of
    /// rows earlier batches deleted.
    /// </summary>
    [Fact]
    public void TheIntervalPurgeStatement_IsOrderedCappedStrictlyBelowTheHorizon_AndCursoredAfterTheFirstBatch()
    {
        Assert.Equal(
            "DELETE FROM collect.query_store_interval_wide WHERE ctid IN ("
          + "SELECT ctid FROM collect.query_store_interval_wide WHERE first_execution_time < $1 "
          + "ORDER BY first_execution_time LIMIT 50000) RETURNING first_execution_time",
            DarlingRetention.CursoredRowCappedDeleteSql(
                "collect.query_store_interval_wide", "first_execution_time", DarlingRetention.IntervalDeleteRowCap, hasCursor: false));
        Assert.Equal(
            "DELETE FROM collect.query_store_interval_wide WHERE ctid IN ("
          + "SELECT ctid FROM collect.query_store_interval_wide WHERE first_execution_time < $1 "
          + "AND first_execution_time >= $2 "
          + "ORDER BY first_execution_time LIMIT 50000) RETURNING first_execution_time",
            DarlingRetention.CursoredRowCappedDeleteSql(
                "collect.query_store_interval_wide", "first_execution_time", DarlingRetention.IntervalDeleteRowCap, hasCursor: true));
        Assert.Equal(DarlingRetention.PlanDimDeleteRowCap, DarlingRetention.IntervalDeleteRowCap);
        Assert.True(DarlingRetention.IntervalDeleteRowCap > DarlingRetention.PlanDimDeleteRowFloor);
    }

    /// <summary>
    /// The adaptive drain in <c>PurgeOneAsync</c> carries the cursor from one batch to the next, and only
    /// advances it after an attempt succeeded (a timed-out attempt retries from the same cursor).
    /// </summary>
    [Fact]
    public void TheAdaptiveDrain_CarriesTheCursorForward()
    {
        var source = ReadRetentionSource();
        var at = source.IndexOf("DateTime? cursor = null;", StringComparison.Ordinal);
        Assert.True(at >= 0, "the cursor declaration moved");
        var body = source[at..Math.Min(source.Length, at + 2500)];

        Assert.Contains("ExecuteCursoredBatchAsync(", body, StringComparison.Ordinal);
        Assert.Contains("cutoff, cursor,", body, StringComparison.Ordinal);
        Assert.Contains("cursor = maxDeleted;", body, StringComparison.Ordinal);
    }

    /* ---- live: the cursor ----------------------------------------------------------------------------- */

    /// <summary>
    /// Driven exactly as the drain drives it (batch by batch, the cursor taken from each batch's RETURNING): the
    /// first batch has no cursor, every later batch gets one that never moves backward, the batches delete exactly
    /// the expired rows in time order, and nothing at or past the horizon is touched.
    /// </summary>
    [Theory]
    [MemberData(nameof(Tables))]
    public async Task EveryBatchAfterTheFirst_StartsAtTheCursor_AndTheCursorOnlyMovesForward(IntervalTable table)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        const int expired = 3_000;
        const int cap = 500;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenMigratedAsync(scratch, ct);
            var start = new DateTime(2026, 3, 1, 0, 0, 0, DateTimeKind.Unspecified);
            await SeedAsync(connection, table, start, expired, 1_000, 0, ct);
            var cutoff = start.AddSeconds(expired);
            await SeedAsync(connection, table, cutoff, 10, 1_000, expired, ct);

            DateTime? cursor = null;
            var batches = 0;
            var total = 0;
            while (true)
            {
                var (deleted, max) = await DarlingRetention.ExecuteCursoredBatchAsync(
                    connection, table.Qualified, "first_execution_time", cap, cutoff, cursor, ct);
                batches++;
                total += deleted;
                if (max is not null)
                {
                    Assert.True(cursor is null || max >= cursor, "the cursor never moves backward");
                    cursor = max;
                }

                if (deleted < cap)
                {
                    break;
                }
            }

            /* 3,000 rows at 500 per batch: six full batches and the empty one that ends the drain. */
            Assert.Equal(expired, total);
            Assert.Equal(expired / cap + 1, batches);
            Assert.Equal(start.AddSeconds(expired - 1), cursor);
            Assert.Equal(10, await CountAsync(connection, table.Qualified, "true", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// Ties at a batch boundary are harmless: 1,200 rows sharing one timestamp, taken 500 at a time, leave the
    /// cursor on that same value for every batch (the bound is inclusive), and the drain still empties them.
    /// </summary>
    [Theory]
    [MemberData(nameof(Tables))]
    public async Task RowsTiedAtTheCursor_AreStillDrained(IntervalTable table)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenMigratedAsync(scratch, ct);
            var tied = new DateTime(2026, 3, 1, 0, 0, 0, DateTimeKind.Unspecified);
            await SeedAsync(connection, table, tied, 1_200, 0, 0, ct);
            var cutoff = tied.AddSeconds(1);

            DateTime? cursor = null;
            var total = 0;
            while (true)
            {
                var (deleted, max) = await DarlingRetention.ExecuteCursoredBatchAsync(
                    connection, table.Qualified, "first_execution_time", 500, cutoff, cursor, ct);
                total += deleted;
                cursor = max ?? cursor;
                Assert.True(cursor is null || cursor == tied);
                if (deleted < 500)
                {
                    break;
                }
            }

            Assert.Equal(1_200, total);
            Assert.Equal(0, await CountAsync(connection, table.Qualified, "true", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /* ---- live: the drain ------------------------------------------------------------------------------ */

    /// <summary>
    /// A backlog of 60,000 expired rows spanning six days (more than one batch at the 50,000 ceiling) drains
    /// through the real sweep in several batches inside the command timeout, and the rows inside the horizon are
    /// untouched. This pins the drain's shape (the row count, more than one batch, no failure). It is NOT the
    /// regression proof for #5569: on a fast scratch database the old whole-day statement also clears this
    /// backlog without timing out, so no timing-free assertion here can fail on it for a real reason (it fails on
    /// the old code only because the old call sites logged no drain line). The lock-based test below,
    /// <c>ATimedOutPass_KeepsItsProgress_...</c>, is the one that fails on the old code for the field's reason.
    /// </summary>
    [Theory]
    [MemberData(nameof(Tables))]
    public async Task ABacklogSpanningSeveralDays_DrainsInBatches_AndLeavesRowsInsideTheHorizon(IntervalTable table)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        const int expired = 60_000;
        const int inside = 100;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenMigratedAsync(scratch, ct);
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

            /* Six days, ending two days before the horizon: 60,000 rows 8.64 s apart. */
            var expiredStart = now.AddDays(-table.HorizonDays - 8);
            await SeedAsync(connection, table, expiredStart, expired, stepMilliseconds: 8_640, idOffset: 0, ct);
            /* A day wide of the horizon on the young side, 100 rows a minute apart. */
            await SeedAsync(connection, table, now.AddDays(-table.HorizonDays + 1), inside, 60_000, expired, ct);
            Assert.Equal(expired + inside, await CountAsync(connection, table.Qualified, "true", ct));

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var logger = new CapturingTestLogger();
            var started = DateTime.UtcNow;

            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, logger, ct);

            Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(120), "the drain ran inside the budget");
            Assert.Equal(0, await CountAsync(connection, table.Qualified, "first_execution_time < now() AT TIME ZONE 'UTC' - " + Days(table.HorizonDays), ct));
            Assert.Equal(inside, await CountAsync(connection, table.Qualified, "true", ct));

            var drained = logger.Lines.SingleOrDefault(l =>
                l.Contains($"drained {expired} row(s) from {table.Name} in", StringComparison.Ordinal));
            Assert.True(drained is not null, "the drain line is logged with the exact row count: " + logger.Joined);
            Assert.False(drained!.Contains("in 1 batch", StringComparison.Ordinal), "60,000 rows take more than one 50,000-row batch: " + drained);
            Assert.DoesNotContain($"Retention purge failed for {table.Name}", logger.Joined, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The horizon keeps microsecond precision: a row exactly AT the cutoff stays (strict <c>&lt;</c>), a row one
    /// microsecond older goes, one a microsecond younger stays. Neither the cast nor the cap rounds the cutoff.
    /// </summary>
    [Theory]
    [MemberData(nameof(Tables))]
    public async Task TheHorizon_KeepsMicrosecondPrecision(IntervalTable table)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenMigratedAsync(scratch, ct);

            /* .123456 s = 1,234,560 ticks; one microsecond = 10 ticks. */
            var cutoff = new DateTime(2026, 3, 1, 12, 0, 0, DateTimeKind.Unspecified).AddTicks(1_234_560);
            await SeedAtAsync(connection, table, cutoff.AddTicks(-10), 1, ct);
            await SeedAtAsync(connection, table, cutoff, 2, ct);
            await SeedAtAsync(connection, table, cutoff.AddTicks(10), 3, ct);

            var (deleted, _) = await DarlingRetention.ExecuteCursoredBatchAsync(
                connection, table.Qualified, "first_execution_time", 100, cutoff, cursor: null, ct);
            Assert.Equal(1, deleted);

            var survivors = await IdsAsync(connection, table, ct);
            Assert.Equal(new long[] { 2, 3 }, survivors);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The purge's plan against the migrated schema, for both interval tables and both forms (first batch, and a
    /// later batch bounded by the cursor): an ordered scan of the table's <c>first_execution_time</c> index, no
    /// Seq Scan and no Sort. Rows are inserted in time order, as ingest does, so the heap and the index agree
    /// and the planner prices the capped index scan the way it does on a real store; the DELETE is explained
    /// inside a transaction and rolled back so both forms see the same rows.
    /// </summary>
    [Theory]
    [MemberData(nameof(Tables))]
    public async Task ThePurgesPlan_IsAnOrderedIndexScan_NoSeqScan_NoSort(IntervalTable table)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenMigratedAsync(scratch, ct);
            var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
            await SeedAsync(connection, table, start, 200_000, 1_000, 0, ct);
            await using (var analyze = new NpgsqlCommand($"VACUUM ANALYZE {table.Qualified}", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            foreach (var hasCursor in new[] { false, true })
            {
                var sql = DarlingRetention.CursoredRowCappedDeleteSql(
                    table.Qualified, "first_execution_time", DarlingRetention.IntervalDeleteRowCap, hasCursor);

                await using var transaction = await connection.BeginTransactionAsync(ct);
                await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + sql, connection, transaction);
                command.Parameters.AddWithValue(start.AddSeconds(100_000));
                if (hasCursor)
                {
                    command.Parameters.AddWithValue(start.AddSeconds(30_000));
                }

                var plan = new System.Text.StringBuilder();
                await using (var reader = await command.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                    {
                        plan.AppendLine(reader.GetString(0));
                    }
                }

                await transaction.RollbackAsync(ct);

                var text = plan.ToString();
                Assert.DoesNotContain("Seq Scan", text, StringComparison.Ordinal);
                Assert.DoesNotContain("Sort", text, StringComparison.Ordinal);
                Assert.Contains($"idx_{table.Name}_first_exec", text, StringComparison.Ordinal);

                /* The cursor has to be an INDEX bound, not a Filter applied after a scan from the bottom of the
                   index: that would pass every assert above and bring back the O(n^2) re-read the cursor exists
                   to prevent (#5569). So the lower bound (">=") must sit on an "Index Cond" line of the ordered
                   scan, never on a "Filter" line; and the first batch, which has no cursor, has no lower bound. */
                var planLines = text.Split('\n');
                var indexCondLines = planLines.Where(l => l.Contains("Index Cond:", StringComparison.Ordinal)).ToArray();
                Assert.NotEmpty(indexCondLines);
                Assert.DoesNotContain(planLines, l =>
                    l.Contains("Filter:", StringComparison.Ordinal) && l.Contains("first_execution_time", StringComparison.Ordinal));
                if (hasCursor)
                {
                    Assert.Contains(indexCondLines, l =>
                        l.Contains(">=", StringComparison.Ordinal) && l.Contains("first_execution_time", StringComparison.Ordinal));
                }
                else
                {
                    Assert.DoesNotContain(indexCondLines, l => l.Contains(">=", StringComparison.Ordinal));
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /* ---- live: the timeout ---------------------------------------------------------------------------- */

    /// <summary>
    /// A pass whose batch cannot finish (a second connection holds a row lock on the 1,200th-oldest row, inside the first day, and the
    /// pass's connections carry a 500 ms <c>statement_timeout</c>, which surfaces as the same SQLSTATE 57014 a
    /// command timeout does): the cap halves from the ceiling down to the floor, the first 1,000 rows (all
    /// before the locked row) are removed and stay removed, the next batch fails at the floor, and the failure
    /// is logged with the rows reached. The table is consistent (2,000 expired rows less the 1,000 committed,
    /// the rows inside the horizon intact). Once the lock is released a later pass finishes the drain.
    /// </summary>
    [Theory]
    [MemberData(nameof(Tables))]
    public async Task ATimedOutPass_KeepsItsProgress_LogsTheRowsReached_AndALaterPassFinishes(IntervalTable table)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        const int expired = 2_000;
        const int inside = 50;
        /* Inside the OLDEST DAY (rows 1 to 1,440 at one a minute, the old whole-day statement's first slice) but
           past the 1,000-row floor batch. The old code's first slice then contains the locked row, times out, rolls
           back and removes NOTHING on every pass: the #5569 field signature. The new code still commits rows 1 to
           1,000 (all before the locked row) and fails only at the floor. */
        const int lockedRow = 1_200;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenMigratedAsync(scratch, ct);
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

            await SeedAsync(connection, table, now.AddDays(-table.HorizonDays - 3), expired, 60_000, 0, ct);
            await SeedAsync(connection, table, now.AddDays(-table.HorizonDays + 1), inside, 60_000, expired, ct);

            await using var locker = new NpgsqlConnection(scratch.ConnectionString);
            await locker.OpenAsync(ct);
            await using var transaction = await locker.BeginTransactionAsync(ct);
            await using (var hold = new NpgsqlCommand(
                $"SELECT 1 FROM {table.Qualified} WHERE query_id = {lockedRow} FOR UPDATE", locker, transaction))
            {
                Assert.Equal(1, await hold.ExecuteScalarAsync(ct));
            }

            var impatient = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Options = "-c statement_timeout=500" };
            await using (var timingOut = NpgsqlDataSource.Create(impatient.ConnectionString))
            {
                var logger = new CapturingTestLogger();
                await DarlingRetention.PurgeAsync(timingOut, timescaleAvailable: false, logger, ct);

                /* The data first, because it is the proof: exactly the committed first batch is gone and the locked
                   row's batch rolled back. The old whole-day statement removes 0 rows here (its first slice holds
                   the locked row), so this fails on it with 2,000 left instead of 1,000. */
                Assert.Equal(expired - 1_000, await CountAsync(
                    connection, table.Qualified, "first_execution_time < now() AT TIME ZONE 'UTC' - " + Days(table.HorizonDays), ct));
                Assert.Equal(inside, await CountAsync(
                    connection, table.Qualified, "first_execution_time >= now() AT TIME ZONE 'UTC' - " + Days(table.HorizonDays), ct));

                /* The halving is logged, and the failure carries the rows that did land. */
                Assert.Contains("retrying at", logger.Joined, StringComparison.Ordinal);
                Assert.Contains($"Purge batch of {table.Name} at cap {DarlingRetention.IntervalDeleteRowCap}", logger.Joined, StringComparison.Ordinal);
                Assert.Contains(
                    $"Retention purge failed for {table.Name} after removing 1000 row(s)",
                    logger.Joined, StringComparison.Ordinal);
            }

            await transaction.RollbackAsync(ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var later = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, later, ct);

            Assert.Equal(0, await CountAsync(
                connection, table.Qualified, "first_execution_time < now() AT TIME ZONE 'UTC' - " + Days(table.HorizonDays), ct));
            Assert.Equal(inside, await CountAsync(connection, table.Qualified, "true", ct));
            Assert.DoesNotContain($"Retention purge failed for {table.Name}", later.Joined, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /* ---- helpers -------------------------------------------------------------------------------------- */

    private static string Days(int days) => $"INTERVAL '{days.ToString(CultureInfo.InvariantCulture)} days'";

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string table, string predicate, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT COUNT(*) FROM {table} WHERE {predicate}", connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long[]> IdsAsync(NpgsqlConnection connection, IntervalTable table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT query_id FROM {table.Qualified} ORDER BY query_id", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var ids = new List<long>();
        while (await reader.ReadAsync(ct))
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids.ToArray();
    }

    private static Task SeedAtAsync(NpgsqlConnection connection, IntervalTable table, DateTime at, long queryId, CancellationToken ct) =>
        SeedAsync(connection, table, at, 1, 0, queryId - 1, ct);

    /// <summary>
    /// <paramref name="count"/> rows, <c>query_id</c> = <paramref name="idOffset"/> + 1..count, whose
    /// <c>first_execution_time</c> is <paramref name="start"/> plus <c>(id - 1)</c> steps of
    /// <paramref name="stepMilliseconds"/>. Distinct ids and times keep the unique index and the batch order
    /// deterministic.
    /// </summary>
    private static async Task SeedAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime start, int count, int stepMilliseconds, long idOffset, CancellationToken ct)
    {
        var isWide = string.Equals(table.Name, QueryStoreIntervalWide.TableName, StringComparison.Ordinal);
        var sql = isWide
            ? @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, module_name, query_text, query_hash, execution_count, replica_role, runtime_stats_interval_id)
SELECT $1 + (g - 1) * ($3 * INTERVAL '1 millisecond') + INTERVAL '1 minute', 1, 'db', g + $4, 1, 'exec',
       $1 + (g - 1) * ($3 * INTERVAL '1 millisecond'), $1 + (g - 1) * ($3 * INTERVAL '1 millisecond') + INTERVAL '1 minute',
       'mod', 'select 1', 'qh', 1, NULL, g + $4
FROM generate_series(1, $2) AS g;"
            : @"
INSERT INTO collect.query_store_interval_latest
(server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan, force_failure_count, query_text)
SELECT 1, 'db', g + $4, 1, NULL, g + $4,
       $1 + (g - 1) * ($3 * INTERVAL '1 millisecond'), $1 + (g - 1) * ($3 * INTERVAL '1 millisecond') + INTERVAL '1 minute',
       'ph', 'qh', 1, 1, 1, $1 + (g - 1) * ($3 * INTERVAL '1 millisecond') + INTERVAL '1 minute',
       false, 0, 'select 1'
FROM generate_series(1, $2) AS g;";

        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = count });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = stepMilliseconds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint, Value = idOffset });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string ReadRetentionSource(
        [System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var relative = System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs");
        for (var dir = new System.IO.DirectoryInfo(System.IO.Path.GetDirectoryName(thisFile)!);
             dir is not null; dir = dir.Parent)
        {
            var candidate = System.IO.Path.Combine(dir.FullName, relative);
            if (System.IO.File.Exists(candidate))
            {
                return System.IO.File.ReadAllText(candidate);
            }
        }

        throw new System.IO.FileNotFoundException("DarlingRetention.cs not found above " + thisFile);
    }
}
