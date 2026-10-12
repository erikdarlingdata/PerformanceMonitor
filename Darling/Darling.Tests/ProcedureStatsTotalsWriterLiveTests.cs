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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live pins for the per-collection procedure totals (#5638, V176): the procedure_stats batch's totals are written inside
/// the COPY's own transaction by <c>DarlingCollectorRunner.CopyBatchOnceAsync</c>, read back from the stored rows, so rows and
/// totals commit together or not at all. Every batch goes through the product's own <c>WriteBatchAsync</c> with
/// <see cref="ProcedureStatsCollector.Row.Precomputed"/> deltas, so the rows carry exactly the deltas and intervals a fact
/// names (a restart collection is interval 0 everywhere). The oracle in every fact is a GROUP BY over the stored rows, the
/// aggregate the trend's CTE takes.
///
/// <para><b>#1776 own-store:</b> every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class ProcedureStatsTotalsWriterLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to run the procedure totals writer's live pins (each mints its own scratch database).";

    private static readonly DateTime T0 = new(2026, 1, 5, 10, 0, 0, DateTimeKind.Unspecified);

    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }

        public required NpgsqlDataSource DataSource { get; init; }

        public required NpgsqlConnection Connection { get; init; }

        public required DarlingCollectorRunner Runner { get; init; }

        public required CollectorDeltaCalculator Deltas { get; init; }

        public required ServerRuntime Server { get; init; }

        public static async Task<Rig> OpenAsync(int serverId, string serverName, CancellationToken ct, bool timescale = false)
        {
            var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
            var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            if (timescale)
            {
                /* TimescaleDB must be installed where this runs (CI's darling-pg job has it): the store's real hypertable conversion, then the
                   background jobs stopped so no policy compresses or drops a chunk under the test. */
                Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available for the compressed-chunk retention pin");
                await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
                await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            }

            var deltas = new CollectorDeltaCalculator();
            var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            return new Rig
            {
                Scratch = scratch,
                DataSource = dataSource,
                Connection = connection,
                Deltas = deltas,
                Runner = new DarlingCollectorRunner(dataSource, deltas),
                Server = new ServerRuntime
                {
                    Config = new MonitoredServer { Name = serverName, Host = serverName },
                    ConnectionString = "Server=" + serverName,
                    Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
                    StorageName = serverName,
                    ServerId = serverId,
                    EngineEdition = 3,
                },
            };
        }

        /// <summary>One batch through the product's <c>WriteBatchAsync</c>; a fault surfaces as the store's own exception.</summary>
        public async Task<int> WriteAsync(DateTime collectionTime, List<ProcedureStatsCollector.Row> rows, CancellationToken ct)
        {
            var context = new CollectorContext
            {
                ServerId = Server.ServerId,
                ServerName = Server.StorageName,
                CollectionTime = collectionTime,
                Deltas = Deltas,
                Target = Server.Target,
            };

            var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(ProcedureStatsCollector.Row)).Invoke(
                Runner,
                new object?[] { Connection, ProcedureStatsCollector.Instance, rows, Server, collectionTime, context, ct })!;
            await task;
            return (int)task.GetType().GetProperty("Result")!.GetValue(task)!;
        }

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await DataSource.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>One procedure row whose deltas are given, so the interval (0 = restart) is the fact's, not the calculator's.</summary>
    private static ProcedureStatsCollector.Row Row(string database, string name, long elapsed, long executions, int interval) => new(
        database, "dbo", name, "P ", T0, T0, executions, elapsed, elapsed, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0,
        "0x" + name, "0xP" + name, null, null)
    {
        Precomputed = new ProcedureStatsCollector.ReadTimeDeltas(executions, elapsed, elapsed, 1, 1, 1, 0, interval),
    };

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// The sweep's own totals purge, as <c>DarlingRetention</c> runs it: the raw floor measured once (<see cref="ProcedureStatsTotals.MeasureRawFloorAsync"/>)
    /// and bound as <c>$2</c> of the one-day-slice DELETE. Returns the rows the DELETE removed.
    /// </summary>
    private static async Task<int> RetentionDeleteAsync(NpgsqlConnection connection, bool timescaleAvailable, DateTime cutoff, CancellationToken ct)
    {
        var floor = await ProcedureStatsTotals.MeasureRawFloorAsync(connection, timescaleAvailable, 60, ct);
        var sliced = DarlingRetention.TimeSlicedDeleteSql(ProcedureStatsTotals.Table, "collection_time", ProcedureStatsTotals.PrunePredicate);
        await using var command = new NpgsqlCommand(sliced, connection);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, cutoff);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = floor is { } value ? value : DBNull.Value });

        /* Drained like the sweep's loop (a one-day slice per statement, repeated until a statement deletes nothing). */
        var total = 0;
        int deleted;
        do
        {
            deleted = await command.ExecuteNonQueryAsync(ct);
            total += deleted;
        }
        while (deleted > 0);

        return total;
    }

    private static string Stamp(DateTime value) => "'" + value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "'::timestamp";

    /// <summary>
    /// The totals of one collection as text, one line per database in database order (NULL last), and the same text from a GROUP BY over
    /// the stored rows. The two strings are equal when the totals equal the rows' own aggregate, NULL group and NULL measures included.
    /// </summary>
    private static async Task<(string Totals, string Raw)> CompareAsync(NpgsqlConnection connection, int serverId, DateTime collectionTime, CancellationToken ct)
    {
        var where = "server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND collection_time = " + Stamp(collectionTime);
        const string cols = "COALESCE(database_name, '<null>'), total_elapsed_time, total_execution_count, row_count, max_sample_interval_seconds";
        var totals = await ScalarAsync(
            connection,
            "SELECT COALESCE(string_agg(concat_ws('|', " + cols + "), ';' ORDER BY database_name NULLS LAST), '') FROM collect.procedure_stats_totals WHERE " + where, ct);
        var raw = await ScalarAsync(
            connection,
            "SELECT COALESCE(string_agg(concat_ws('|', COALESCE(database_name, '<null>'), e, x, n, i), ';' ORDER BY database_name NULLS LAST), '') FROM ("
            + "SELECT database_name, SUM(delta_elapsed_time) AS e, SUM(delta_execution_count) AS x, COUNT(*) AS n, MAX(sample_interval_seconds) AS i "
            + "FROM collect.procedure_stats WHERE " + where + " GROUP BY database_name) g", ct);
        return ((string)totals!, (string)raw!);
    }

    /// <summary>A measured two-database batch, then a restart collection (interval 0 on every row), through the real write path.</summary>
    [Fact]
    public async Task TheTotalsEqualAGroupByOverTheRows_ForAMeasuredBatchAndARestartCollection()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945301;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotals\\INST", ct);

        var bodySucceeded = false;
        try
        {
            var measured = new List<ProcedureStatsCollector.Row>
            {
                Row("DbA", "p1", 1_000_001, 3, 60), Row("DbA", "p2", 2_000_003, 5, 60), Row("DbA", "p3", 7, 1, 60),
                Row("DbB", "p1", 5_000_000, 9, 60), Row("DbB", "p2", 11, 2, 60),
            };
            Assert.Equal(5, await rig.WriteAsync(T0.AddMinutes(1), measured, ct));

            var (totals, raw) = await CompareAsync(rig.Connection, serverId, T0.AddMinutes(1), ct);
            Assert.Equal(raw, totals);
            Assert.Equal("DbA|3000011|9|3|60;DbB|5000011|11|2|60", totals);

            /* A restart collection: no delta was knowable, so every row's interval is 0 and its deltas are 0. The totals keep the
               rows (row_count) and the 0 interval, so the read's three-state contract (0 = restart) is intact. */
            var restart = new List<ProcedureStatsCollector.Row>
            {
                Row("DbA", "p1", 0, 0, 0), Row("DbA", "p2", 0, 0, 0), Row("DbB", "p1", 0, 0, 0),
            };
            Assert.Equal(3, await rig.WriteAsync(T0.AddMinutes(2), restart, ct));
            var (restartTotals, restartRaw) = await CompareAsync(rig.Connection, serverId, T0.AddMinutes(2), ct);
            Assert.Equal(restartRaw, restartTotals);
            Assert.Equal("DbA|0|0|2|0;DbB|0|0|1|0", restartTotals);

            /* One collection is one row-group per database, and nothing was written for a collection that stored no rows. */
            Assert.Equal(0, await rig.WriteAsync(T0.AddMinutes(3), new List<ProcedureStatsCollector.Row>(), ct));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals WHERE collection_time = " + Stamp(T0.AddMinutes(3)), ct)));
            Assert.Equal(4L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals", ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A second batch at the SAME collection_time recomputes from all the stored rows rather than adding, a re-run of the upsert on its
    /// own is a no-op, and a row whose interval is NULL (stored before the column existed) and a row with a NULL database_name are
    /// kept exactly as the rows have them: the NULL interval stays NULL when every interval is NULL, and the NULL database is one group.
    /// </summary>
    [Fact]
    public async Task ASecondBatchAtTheSameCollectionTime_Recomputes_AndNullIntervalsAndANullDatabaseKeepTheRowsShape()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945302;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotals2", ct);

        var bodySucceeded = false;
        try
        {
            var at = T0.AddMinutes(5);
            Assert.Equal(2, await rig.WriteAsync(at, new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 100, 1, 60), Row("DbA", "p2", 200, 2, 30) }, ct));
            Assert.Equal("DbA|300|3|2|60", (await CompareAsync(rig.Connection, serverId, at, ct)).Totals);

            /* The same collection arrives in a second batch (a fan-out chunk): the totals are the whole collection, not the second batch. */
            Assert.Equal(2, await rig.WriteAsync(at, new List<ProcedureStatsCollector.Row> { Row("DbA", "p3", 400, 4, 10), Row("DbB", "p1", 800, 8, 45) }, ct));
            var (totals, raw) = await CompareAsync(rig.Connection, serverId, at, ct);
            Assert.Equal(raw, totals);
            Assert.Equal("DbA|700|7|3|60;DbB|800|8|1|45", totals);

            /* An upsert that runs again (a re-attempt) changes nothing. */
            await using (var tx = await rig.Connection.BeginTransactionAsync(ct))
            {
                Assert.Equal(2, await ProcedureStatsTotalsWriter.UpsertBatchAsync(rig.Connection, tx, serverId, at, 4, 30, null, ct));
                await tx.CommitAsync(ct);
            }

            Assert.Equal(raw, (await CompareAsync(rig.Connection, serverId, at, ct)).Totals);

            /* Rows stored without an interval (NULL) and without a database (NULL), the shape of rows written before the columns existed. */
            await ExecAsync(rig.Connection, "UPDATE collect.procedure_stats SET sample_interval_seconds = NULL, database_name = NULL WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND collection_time = " + Stamp(at) + " AND database_name = 'DbB'", ct);
            await ExecAsync(rig.Connection, "UPDATE collect.procedure_stats SET sample_interval_seconds = NULL WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND collection_time = " + Stamp(at) + " AND object_name IN ('p1', 'p2') AND database_name = 'DbA'", ct);
            /* The UPDATE above moved a stored group to NULL, which a real store never does; the stale DbB totals row is cleared by hand. */
            await ExecAsync(rig.Connection, "DELETE FROM collect.procedure_stats_totals WHERE database_name = 'DbB'", ct);
            await using (var tx = await rig.Connection.BeginTransactionAsync(ct))
            {
                await ProcedureStatsTotalsWriter.UpsertBatchAsync(rig.Connection, tx, serverId, at, 1, 30, null, ct);
                await tx.CommitAsync(ct);
            }

            var (nullTotals, nullRaw) = await CompareAsync(rig.Connection, serverId, at, ct);
            Assert.Equal(nullRaw, nullTotals);

            /* DbA keeps its measured 10 (p3); the NULL database is its own group, its NULL interval stays NULL (concat_ws drops it). */
            Assert.Equal("DbA|700|7|3|10;<null>|800|8|1", nullTotals);

            /* And a second upsert still leaves one NULL-database row (the key treats NULL as one group, so it conflicts). */
            await using (var tx = await rig.Connection.BeginTransactionAsync(ct))
            {
                await ProcedureStatsTotalsWriter.UpsertBatchAsync(rig.Connection, tx, serverId, at, 1, 30, null, ct);
                await tx.CommitAsync(ct);
            }

            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals WHERE database_name IS NULL", ct)));
            Assert.True((bool)(await ScalarAsync(rig.Connection, "SELECT max_sample_interval_seconds IS NULL FROM collect.procedure_stats_totals WHERE database_name IS NULL", ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A fault in the totals statement aborts the whole batch: there is no savepoint, so the COPY's rows roll back with it. A trigger
    /// that refuses every totals write stands in for the fault; once it clears, the next batch writes rows and totals together.
    /// </summary>
    [Fact]
    public async Task AFailedTotalsUpsert_AbortsTheBatch_SoRowsAndTotalsAreBothUnchanged()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945303;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotals3", ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(2, await rig.WriteAsync(T0.AddMinutes(1), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10, 1, 60), Row("DbA", "p2", 20, 2, 60) }, ct));
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats", ct)));

            await ExecAsync(
                rig.Connection,
                "CREATE FUNCTION collect.proc_totals_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'totals write refused' USING ERRCODE = 'P0001'; END $$;"
                + "CREATE TRIGGER trg_proc_totals_refuse BEFORE INSERT OR UPDATE ON collect.procedure_stats_totals FOR EACH ROW EXECUTE FUNCTION collect.proc_totals_refuse();",
                ct);

            var fault = await Assert.ThrowsAsync<PostgresException>(
                () => rig.WriteAsync(T0.AddMinutes(2), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 30, 3, 60) }, ct));
            Assert.Equal("P0001", fault.SqlState);

            /* The COPY ran inside the same transaction: its row is gone, and no totals row exists for that collection. */
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats", ct)));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals", ct)));

            await ExecAsync(rig.Connection, "DROP TRIGGER trg_proc_totals_refuse ON collect.procedure_stats_totals; DROP FUNCTION collect.proc_totals_refuse();", ct);

            Assert.Equal(1, await rig.WriteAsync(T0.AddMinutes(3), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 40, 4, 60) }, ct));
            var (totals, raw) = await CompareAsync(rig.Connection, serverId, T0.AddMinutes(3), ct);
            Assert.Equal(raw, totals);
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals", ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The read-back is served by an index on server_id + collection_time of procedure_stats (functional check only; no timing is claimed),
    /// and the retention predicate never deletes a totals row at or after the oldest raw row, whatever the cutoff, while it does delete the
    /// rows older than both. The statement is the sweep's own <c>TimeSlicedDeleteSql</c> with <see cref="ProcedureStatsTotals.PrunePredicate"/>.
    /// </summary>
    [Fact]
    public async Task TheReadBackUsesAnIndex_AndRetentionNeverPurgesTotalsBeforeTheRawRowsOfTheirAge()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945304;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotals4", ct);

        var bodySucceeded = false;
        try
        {
            /* Collections at day 0 (oldest raw), day 1 and day 2; raw is later trimmed so day 0's rows are gone but day 1 stays. */
            for (var day = 0; day < 3; day++)
            {
                Assert.Equal(1, await rig.WriteAsync(T0.AddDays(day), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10 + day, 1, 60) }, ct));
            }

            await ExecAsync(rig.Connection, "SET enable_seqscan = off", ct);
            var plan = new System.Text.StringBuilder();
            await using (var explain = new NpgsqlCommand(
                "EXPLAIN SELECT database_name, SUM(delta_elapsed_time) FROM collect.procedure_stats WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture)
                + " AND collection_time = " + Stamp(T0) + " GROUP BY database_name", rig.Connection))
            await using (var reader = await explain.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    plan.AppendLine(reader.GetString(0));
                }
            }

            Assert.Contains("Index", plan.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("Seq Scan on procedure_stats", plan.ToString(), StringComparison.Ordinal);
            await ExecAsync(rig.Connection, "RESET enable_seqscan", ct);

            /* Cutoff after day 1 while every raw row is still held: raw holds day 0, so no totals row may go. */
            Assert.Equal(0, await RetentionDeleteAsync(rig.Connection, false, T0.AddDays(1).AddHours(1), ct));

            Assert.Equal(3L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals", ct)));

            /* Raw loses day 0 (its purge ran). Now day 0's totals row is older than both the cutoff and the oldest raw row. */
            await ExecAsync(rig.Connection, "DELETE FROM collect.procedure_stats WHERE collection_time < " + Stamp(T0.AddDays(1)), ct);
            Assert.Equal(1, await RetentionDeleteAsync(rig.Connection, false, T0.AddDays(2).AddHours(1), ct));

            /* Day 1's totals row is the oldest raw row's age, so it stays even though the cutoff is past it; day 2 is newer than the cutoff. */
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals", ct)));
            Assert.Equal(T0.AddDays(1), (DateTime)(await ScalarAsync(rig.Connection, "SELECT min(collection_time) FROM collect.procedure_stats_totals", ct))!);

            /* The plain-PostgreSQL floor is the table's exact oldest row (day 1), one min() measured before the DELETE. */
            Assert.Equal(T0.AddDays(1), await ProcedureStatsTotals.MeasureRawFloorAsync(rig.Connection, false, 60, ct));

            /* Every raw row gone: the floor is NULL and the cutoff alone decides, so both remaining totals rows (older than the cutoff) go. */
            await ExecAsync(rig.Connection, "DELETE FROM collect.procedure_stats", ct);
            Assert.Null(await ProcedureStatsTotals.MeasureRawFloorAsync(rig.Connection, false, 60, ct));
            Assert.Equal(2, await RetentionDeleteAsync(rig.Connection, false, T0.AddDays(2).AddHours(1), ct));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals", ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// Review of #5706, finding 2: on Azure SQL Database the collector writes one batch per database at one <c>collection_time</c>. Each batch
    /// totals only its own databases, so the batches of the other databases are not read back (a sentinel written into DbA's totals survives
    /// DbC's batch), and the result is still exact: a second DbA batch recomputes DbA from BOTH of its batches, and a NULL database name
    /// matches its NULL rows through the writer's NULL flag.
    /// </summary>
    [Fact]
    public async Task PerDatabaseBatchesAtOneCollectionTime_TotalOnlyTheirOwnDatabases_AndStayExact()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945311;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotals5", ct);

        var bodySucceeded = false;
        try
        {
            var at = T0.AddMinutes(5);
            var id = serverId.ToString(CultureInfo.InvariantCulture);
            Assert.Equal(2, await rig.WriteAsync(at, new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 100, 1, 60), Row("DbA", "p2", 200, 2, 30) }, ct));
            Assert.Equal(1, await rig.WriteAsync(at, new List<ProcedureStatsCollector.Row> { Row("DbB", "p1", 400, 4, 45) }, ct));
            Assert.Equal("DbA|300|3|2|60;DbB|400|4|1|45", (await CompareAsync(rig.Connection, serverId, at, ct)).Totals);

            /* DbA's totals row is spoiled by hand. A batch of DbC alone must not read DbA back, so the spoiled value survives it. */
            await ExecAsync(rig.Connection, "UPDATE collect.procedure_stats_totals SET total_elapsed_time = -1 WHERE database_name = 'DbA'", ct);
            Assert.Equal(1, await rig.WriteAsync(at, new List<ProcedureStatsCollector.Row> { Row("DbC", "p1", 800, 8, 10) }, ct));
            Assert.Equal(-1L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT total_elapsed_time FROM collect.procedure_stats_totals WHERE database_name = 'DbA'", ct)));
            Assert.Equal(400L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT total_elapsed_time FROM collect.procedure_stats_totals WHERE database_name = 'DbB'", ct)));
            Assert.Equal(800L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT total_elapsed_time FROM collect.procedure_stats_totals WHERE database_name = 'DbC'", ct)));

            /* A later batch of DbA recomputes DbA from both of its batches (the rows of the earlier one included): exact. */
            Assert.Equal(1, await rig.WriteAsync(at, new List<ProcedureStatsCollector.Row> { Row("DbA", "p3", 50, 5, 20) }, ct));
            var (totals, raw) = await CompareAsync(rig.Connection, serverId, at, ct);
            Assert.Equal(raw, totals);
            Assert.Equal("DbA|350|8|3|60;DbB|400|4|1|45;DbC|800|8|1|10", totals);

            /* The NULL database: DbC's rows lose their name (the shape of rows older than the column), and a batch that names only NULL
               recomputes only the NULL group; DbA's spoiled row survives it, and a batch that names DbB and NULL leaves DbA alone too. */
            await ExecAsync(rig.Connection, "UPDATE collect.procedure_stats SET database_name = NULL WHERE server_id = " + id + " AND database_name = 'DbC'", ct);
            await ExecAsync(rig.Connection, "DELETE FROM collect.procedure_stats_totals WHERE database_name = 'DbC'", ct);
            await ExecAsync(rig.Connection, "UPDATE collect.procedure_stats_totals SET total_elapsed_time = -1 WHERE database_name = 'DbA'", ct);
            await UpsertAsync(rig, serverId, at, new string?[] { null }, ct);
            Assert.Equal("DbA|-1|8|3|60;DbB|400|4|1|45;<null>|800|8|1|10", (await CompareAsync(rig.Connection, serverId, at, ct)).Totals);

            await ExecAsync(rig.Connection, "UPDATE collect.procedure_stats_totals SET total_elapsed_time = -2 WHERE database_name = 'DbB'", ct);
            await UpsertAsync(rig, serverId, at, new string?[] { "DbB", null }, ct);
            Assert.Equal("DbA|-1|8|3|60;DbB|400|4|1|45;<null>|800|8|1|10", (await CompareAsync(rig.Connection, serverId, at, ct)).Totals);

            /* No names at all (an import) reads the whole collection back. */
            await UpsertAsync(rig, serverId, at, null, ct);
            var (allTotals, allRaw) = await CompareAsync(rig.Connection, serverId, at, ct);
            Assert.Equal(allRaw, allTotals);
            Assert.Equal("DbA|350|8|3|60;DbB|400|4|1|45;<null>|800|8|1|10", allTotals);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task UpsertAsync(Rig rig, int serverId, DateTime at, IReadOnlyCollection<string?>? names, CancellationToken ct)
    {
        await using var tx = await rig.Connection.BeginTransactionAsync(ct);
        await ProcedureStatsTotalsWriter.UpsertBatchAsync(rig.Connection, tx, serverId, at, 1, 30, names, ct);
        await tx.CommitAsync(ct);
    }

    /// <summary>
    /// Review of #5706, finding 4 on a TimescaleDB store: <c>procedure_stats</c> is a hypertable whose oldest chunk is COMPRESSED (no btree
    /// there, the case that timed out in the field, #1815). The floor the retention DELETE binds comes from the chunk catalog, is never later
    /// than the oldest raw row, and the sweep's DELETE never removes a totals row at or after it, whatever the cutoff. When the oldest chunk
    /// leaves, the floor moves up and the totals older than the new floor go.
    /// </summary>
    [Fact]
    public async Task OnTimescaleDb_WithACompressedOldestChunk_TheFloorComesFromTheChunkCatalog_AndRetentionNeverPurgesTotalsAtOrAfterIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945312;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotals6", ct, timescale: true);

        var bodySucceeded = false;
        try
        {
            /* Three collections ten days apart: three chunks. The first chunk is then compressed. */
            for (var i = 0; i < 3; i++)
            {
                Assert.Equal(1, await rig.WriteAsync(T0.AddDays(10 * i), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10 + i, 1, 60) }, ct));
            }

            Assert.Equal(3L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'procedure_stats'", ct)));
            await ExecAsync(rig.Connection, "ALTER TABLE collect.procedure_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);
            await ExecAsync(rig.Connection, "SELECT compress_chunk(c) FROM show_chunks('collect.procedure_stats', older_than => " + Stamp(T0.AddDays(5)) + ") c", ct);
            Assert.True((bool)(await ScalarAsync(rig.Connection,
                "SELECT bool_and(is_compressed) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'procedure_stats' AND range_start AT TIME ZONE 'UTC' < " + Stamp(T0.AddDays(5)), ct))!);

            /* The floor is the oldest chunk's range_start: not later than the oldest raw row (and, as the row is mid-chunk, earlier than it). */
            var floor = await ProcedureStatsTotals.MeasureRawFloorAsync(rig.Connection, true, 60, ct);
            var chunkStart = (DateTime)(await ScalarAsync(rig.Connection,
                "SELECT min(range_start AT TIME ZONE 'UTC') FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'procedure_stats'", ct))!;
            Assert.Equal(chunkStart, floor);
            Assert.True(floor <= T0, "the chunk floor is never later than the oldest raw row");
            Assert.True(floor < T0, "the floor is the chunk's start, not the row's own time: it came from the catalog, not from min(collection_time)");

            /* A cutoff far past every collection while the raw chunk is still held: nothing at or after the floor may go. */
            Assert.Equal(0, await RetentionDeleteAsync(rig.Connection, true, T0.AddDays(100), ct));
            Assert.Equal(3L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals", ct)));

            /* The oldest chunk leaves (the raw purge ran). The floor moves to the next chunk, and the first collection's totals, now older
               than every raw row, go; the other two sit at or after the oldest raw row and stay. */
            await ExecAsync(rig.Connection, "SELECT drop_chunks('collect.procedure_stats', older_than => " + Stamp(T0.AddDays(5)) + ")", ct);
            var movedFloor = await ProcedureStatsTotals.MeasureRawFloorAsync(rig.Connection, true, 60, ct);
            Assert.True(movedFloor > floor && movedFloor <= T0.AddDays(10));
            Assert.Equal(1, await RetentionDeleteAsync(rig.Connection, true, T0.AddDays(100), ct));
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals", ct)));
            Assert.Equal(T0.AddDays(10), (DateTime)(await ScalarAsync(rig.Connection, "SELECT min(collection_time) FROM collect.procedure_stats_totals", ct))!);

            /* Every raw row gone: no chunk and no row, so the floor is NULL and the cutoff alone decides (the empty-table behavior). */
            await ExecAsync(rig.Connection, "SELECT drop_chunks('collect.procedure_stats', older_than => " + Stamp(T0.AddDays(100)) + ")", ct);
            Assert.Null(await ProcedureStatsTotals.MeasureRawFloorAsync(rig.Connection, true, 60, ct));
            Assert.Equal(2, await RetentionDeleteAsync(rig.Connection, true, T0.AddDays(100), ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
