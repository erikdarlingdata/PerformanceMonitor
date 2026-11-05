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

        public static async Task<Rig> OpenAsync(int serverId, string serverName, CancellationToken ct, bool timescale = false, bool plainStore = false)
        {
            /* plainStore: the test's subject is a store WITHOUT TimescaleDB (the sweep run with its timescale flag off); CreateAsync enables the extension by default (#6317). emptyStore: the test migrates (and, for timescale, converts) the store itself, and some tests stage a legacy schema first, so it must not start from the migrated template (#7097). */
            var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, plainPostgres: plainStore, emptyStore: true);
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

        /// <summary>Another monitored server on the same store, for the tests that need several servers' rows.</summary>
        public ServerRuntime OtherServer(int serverId, string serverName) => new()
        {
            Config = new MonitoredServer { Name = serverName, Host = serverName },
            ConnectionString = "Server=" + serverName,
            Target = Server.Target,
            StorageName = serverName,
            ServerId = serverId,
            EngineEdition = 3,
        };

        /// <summary>One batch through the product's <c>WriteBatchAsync</c>; a fault surfaces as the store's own exception.</summary>
        public async Task<int> WriteAsync(DateTime collectionTime, List<ProcedureStatsCollector.Row> rows, CancellationToken ct, ServerRuntime? server = null)
        {
            var target = server ?? Server;
            var context = new CollectorContext
            {
                ServerId = target.ServerId,
                ServerName = target.StorageName,
                CollectionTime = collectionTime,
                Deltas = Deltas,
                Target = target.Target,
            };

            var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(ProcedureStatsCollector.Row)).Invoke(
                Runner,
                new object?[] { Connection, ProcedureStatsCollector.Instance, rows, target, collectionTime, context, ct })!;
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

            /* Round 2 (L2): the gap statement binds its anchor, so both bounds are constants at plan time and only the chunks inside
               (anchor, batch) are scanned: the compressed oldest chunk (and the newest one) are excluded, no DecompressChunk appears. */
            var gapAnchor = T0.AddDays(9);
            var gapBatch = T0.AddDays(11);
            var inRange = new List<string>();
            var outOfRange = new List<string>();
            await using (var chunks = new NpgsqlCommand(
                "SELECT chunk_name, (range_start AT TIME ZONE 'UTC' < " + Stamp(gapBatch) + " AND range_end AT TIME ZONE 'UTC' > " + Stamp(gapAnchor) + ") " +
                "FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'procedure_stats'", rig.Connection))
            await using (var chunkReader = await chunks.ExecuteReaderAsync(ct))
            {
                while (await chunkReader.ReadAsync(ct))
                {
                    (chunkReader.GetBoolean(1) ? inRange : outOfRange).Add(chunkReader.GetString(0));
                }
            }

            Assert.Equal(2, outOfRange.Count);
            Assert.Single(inRange);
            var gapPlan = await ExplainAsync(rig.Connection, "EXPLAIN " + ProcedureStatsTotals.GapFillSql, ct,
                (NpgsqlTypes.NpgsqlDbType.Integer, serverId), (NpgsqlTypes.NpgsqlDbType.Timestamp, gapBatch), (NpgsqlTypes.NpgsqlDbType.Timestamp, gapAnchor));
            Assert.Contains(inRange[0], gapPlan, StringComparison.Ordinal);
            Assert.All(outOfRange, name => Assert.DoesNotContain(name, gapPlan, StringComparison.Ordinal));
            Assert.DoesNotContain("DecompressChunk", gapPlan, StringComparison.Ordinal);

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

    /* ---- the gap fill never fails the batch (seat review of #5706) ---- */

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
    {
        public List<(Microsoft.Extensions.Logging.LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }

    /// <summary>
    /// Plants the gap a rollback to a pre-V176 binary leaves: a collection at <paramref name="gapAt"/> with raw rows (database
    /// <paramref name="gapDatabase"/>) and no totals row, above an anchor collection at <paramref name="anchorAt"/> that has both.
    /// </summary>
    private static async Task PlantGapAsync(Rig rig, int serverId, DateTime anchorAt, DateTime gapAt, string gapDatabase, CancellationToken ct, DateTime? batchAt = null)
    {
        Assert.Equal(1, await rig.WriteAsync(anchorAt, new List<ProcedureStatsCollector.Row> { Row("Anchor", "p0", 10, 1, 60) }, ct));
        if (batchAt is not null)
        {
            /* The batch's own raw row, for a direct UpsertBatchAsync call to total; written before the gap row so the product's own gap fill finds nothing. */
            Assert.Equal(1, await rig.WriteAsync(batchAt.Value, new List<ProcedureStatsCollector.Row> { Row("DbA", "p2", 5, 1, 60) }, ct));
        }

        Assert.Equal(1, await rig.WriteAsync(gapAt, new List<ProcedureStatsCollector.Row> { Row(gapDatabase, "p1", 100, 2, 60) }, ct));
        var where = "server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND collection_time IN (" + Stamp(gapAt) + (batchAt is null ? "" : ", " + Stamp(batchAt.Value)) + ")";
        await ExecAsync(rig.Connection, "DELETE FROM collect.procedure_stats_totals WHERE " + where, ct);
    }

    private static async Task<long> TotalsRowsAsync(Rig rig, int serverId, DateTime at, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND collection_time = " + Stamp(at), ct));

    private static async Task<long> RawRowsAsync(Rig rig, int serverId, DateTime at, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND collection_time = " + Stamp(at), ct));

    private static async Task<object?> ScalarTxAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>
    /// The normal gap: a collection with rows and no totals above the server's newest totals row is totalled by the next batch, a
    /// healthy gap fill logs nothing, and its statement_timeout does not outlive it (the batch's own upsert and the rest of the
    /// transaction run under the connection's original setting). The batch after it has an empty gap and takes the normal path.
    /// </summary>
    [Fact]
    public async Task TheGapFill_TotalsARealGap_LogsNothing_AndRestoresTheStatementTimeout()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945310;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotalsGapOk", ct);

        var bodySucceeded = false;
        try
        {
            var gapAt = T0.AddMinutes(1);
            var batchAt = T0.AddMinutes(2);
            await PlantGapAsync(rig, serverId, T0, gapAt, "GapDb", ct, batchAt);
            Assert.Equal(0L, await TotalsRowsAsync(rig, serverId, gapAt, ct));

            var logger = new CapturingLogger();
            await using (var tx = await rig.Connection.BeginTransactionAsync(ct))
            {
                var before = (string)(await ScalarTxAsync(rig.Connection, tx, "SHOW statement_timeout", ct))!;
                Assert.Equal(1, await ProcedureStatsTotalsWriter.UpsertBatchAsync(rig.Connection, tx, serverId, batchAt, 1, 30, null, ct, logger, "ProcTotalsGapOk"));
                Assert.Equal(before, (string)(await ScalarTxAsync(rig.Connection, tx, "SHOW statement_timeout", ct))!);
                await tx.CommitAsync(ct);
            }

            Assert.Equal(1L, await TotalsRowsAsync(rig, serverId, gapAt, ct));
            var (totals, raw) = await CompareAsync(rig.Connection, serverId, gapAt, ct);
            Assert.Equal(raw, totals);
            Assert.Empty(logger.Entries);

            /* The next batch's gap (after the batch above, before this one) is empty: the normal path, through the product's write path. */
            Assert.Equal(1, await rig.WriteAsync(T0.AddMinutes(3), new List<ProcedureStatsCollector.Row> { Row("DbA", "p2", 5, 1, 60) }, ct));
            Assert.Equal(1L, await TotalsRowsAsync(rig, serverId, T0.AddMinutes(3), ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A gap fill that FAILS (a trigger refusing the gap database's totals row stands in for the fault) does not fail the batch
    /// (seat review of #5706): through the product's own write path the batch's rows and its own totals commit, the failure is skipped,
    /// and because the batch's totals row is now the server's newest the NEXT batch does not retry the gap, even once the fault clears.
    /// Before the fix the first batch threw, rolled back, and every later batch found the same anchor and threw again.
    /// </summary>
    [Fact]
    public async Task AFailedGapFill_DoesNotFailTheBatch_AndTheNextBatchDoesNotRetryIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945311;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotalsGapFail", ct);

        var bodySucceeded = false;
        try
        {
            var gapAt = T0.AddMinutes(1);
            await PlantGapAsync(rig, serverId, T0, gapAt, "gapfail", ct);

            /* Only the gap database's totals row is refused; the batch's own database is not. */
            await ExecAsync(rig.Connection, "CREATE FUNCTION public.refuse_gap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.database_name = 'gapfail' THEN RAISE EXCEPTION 'planted gap failure'; END IF; RETURN NEW; END $$", ct);
            await ExecAsync(rig.Connection, "CREATE TRIGGER refuse_gap BEFORE INSERT OR UPDATE ON collect.procedure_stats_totals FOR EACH ROW EXECUTE FUNCTION public.refuse_gap()", ct);

            var batchAt = T0.AddMinutes(2);
            Assert.Equal(1, await rig.WriteAsync(batchAt, new List<ProcedureStatsCollector.Row> { Row("DbA", "p2", 5, 1, 60) }, ct));

            /* The batch committed: its rows and its own totals are stored; the gap was skipped. */
            Assert.Equal(1L, await RawRowsAsync(rig, serverId, batchAt, ct));
            Assert.Equal(1L, await TotalsRowsAsync(rig, serverId, batchAt, ct));
            Assert.Equal(0L, await TotalsRowsAsync(rig, serverId, gapAt, ct));
            var (totals, raw) = await CompareAsync(rig.Connection, serverId, batchAt, ct);
            Assert.Equal(raw, totals);

            /* The fault clears; the next batch's anchor is the batch above, so the gap (an older collection) is not attempted again. */
            await ExecAsync(rig.Connection, "DROP TRIGGER refuse_gap ON collect.procedure_stats_totals", ct);
            var nextAt = T0.AddMinutes(3);
            Assert.Equal(1, await rig.WriteAsync(nextAt, new List<ProcedureStatsCollector.Row> { Row("DbA", "p2", 7, 1, 60) }, ct));
            Assert.Equal(1L, await TotalsRowsAsync(rig, serverId, nextAt, ct));
            Assert.Equal(0L, await TotalsRowsAsync(rig, serverId, gapAt, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A gap fill that exceeds its server-side bound (a trigger sleeping on the gap database's totals row, under a 300 ms
    /// <c>statement_timeout</c>) is cancelled by the server, not by Npgsql's client timer, so the connection stays usable: the
    /// transaction rolls back to the savepoint, the batch's own upsert runs under the connection's ORIGINAL timeout, a warning names
    /// the server and the gap's range, and the commit lands.
    /// </summary>
    [Fact]
    public async Task AGapFillThatTimesOut_IsCancelledByTheServer_LogsTheRange_AndTheBatchStillCommits()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945312;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotalsGapSlow", ct);

        var bodySucceeded = false;
        try
        {
            var gapAt = T0.AddMinutes(1);
            var batchAt = T0.AddMinutes(2);
            await PlantGapAsync(rig, serverId, T0, gapAt, "gapslow", ct, batchAt);
            await ExecAsync(rig.Connection, "CREATE FUNCTION public.slow_gap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.database_name = 'gapslow' THEN PERFORM pg_sleep(30); END IF; RETURN NEW; END $$", ct);
            await ExecAsync(rig.Connection, "CREATE TRIGGER slow_gap BEFORE INSERT OR UPDATE ON collect.procedure_stats_totals FOR EACH ROW EXECUTE FUNCTION public.slow_gap()", ct);

            var logger = new CapturingLogger();
            await using (var tx = await rig.Connection.BeginTransactionAsync(ct))
            {
                var before = (string)(await ScalarTxAsync(rig.Connection, tx, "SHOW statement_timeout", ct))!;
                Assert.Equal(1, await ProcedureStatsTotalsWriter.UpsertBatchAsync(
                    rig.Connection, tx, serverId, batchAt, 1, 30, null, ct, logger, "ProcTotalsGapSlow", gapStatementTimeoutMilliseconds: 300));

                /* Rolled back to the savepoint, so the SET LOCAL is gone and the connection answers inside the same transaction. */
                Assert.Equal(before, (string)(await ScalarTxAsync(rig.Connection, tx, "SHOW statement_timeout", ct))!);
                await tx.CommitAsync(ct);
            }

            Assert.Equal(1L, await TotalsRowsAsync(rig, serverId, batchAt, ct));
            Assert.Equal(0L, await TotalsRowsAsync(rig, serverId, gapAt, ct));
            var warning = Assert.Single(logger.Entries);
            Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, warning.Level);
            Assert.Contains("ProcTotalsGapSlow", warning.Message, StringComparison.Ordinal);
            Assert.Contains(T0.ToString("yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture), warning.Message, StringComparison.Ordinal);
            Assert.Contains(batchAt.ToString("yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture), warning.Message, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The read-back limit reaches the writer on the product's own path (seat review of #5706): the runner's <c>List&lt;TRow&gt;</c>
    /// with TRow = Row arrives as the typed collection, so a second batch at the same collection_time that holds only DbA does not
    /// recompute the first batch's DbB totals row (a null collection would read the whole collection and recreate it).
    /// </summary>
    [Fact]
    public async Task TheReadBackLimit_ReachesTheWriter_OnTheRunnersOwnPath()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945313;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotalsLimit", ct);

        var bodySucceeded = false;
        try
        {
            var at = T0.AddMinutes(5);
            Assert.Equal(2, await rig.WriteAsync(at, new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 100, 1, 60), Row("DbB", "p1", 200, 2, 60) }, ct));
            await ExecAsync(rig.Connection, "DELETE FROM collect.procedure_stats_totals WHERE database_name = 'DbB'", ct);
            Assert.Equal(1, await rig.WriteAsync(at, new List<ProcedureStatsCollector.Row> { Row("DbA", "p2", 300, 3, 60) }, ct));

            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals WHERE database_name = 'DbB'", ct)));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals WHERE database_name = 'DbA'", ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /* ---- round 2 of the review of #5706 ---- */

    /// <summary>
    /// M1: on Azure SQL Database every per-database batch of one cycle shares one <c>collection_time</c>. A gap fill that fails in
    /// batch 1 must not be retried by batch 2 (same time, another database): the anchor takes the totals row batch 1 committed AT that
    /// time. A trigger that bumps a sequence counts the gap statement's attempts (a sequence survives the savepoint rollback, a
    /// counter table would not). A batch that rolled back leaves no anchor, so its re-attempt does try the gap again.
    /// </summary>
    [Fact]
    public async Task TwoBatchesAtOneCollectionTime_TryAFailingGapOnce_AndTheSecondTakesTheNormalPath()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945314;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotalsGapCycle", ct);

        var bodySucceeded = false;
        try
        {
            var gapAt = T0.AddMinutes(1);
            var batchAt = T0.AddMinutes(2);
            await PlantGapAsync(rig, serverId, T0, gapAt, "gapfail", ct, batchAt);

            await ExecAsync(rig.Connection, "CREATE SEQUENCE public.gap_attempts", ct);
            await ExecAsync(rig.Connection, "CREATE FUNCTION public.refuse_gap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.database_name = 'gapfail' THEN PERFORM nextval('public.gap_attempts'); RAISE EXCEPTION 'planted gap failure'; END IF; RETURN NEW; END $$", ct);
            await ExecAsync(rig.Connection, "CREATE TRIGGER refuse_gap BEFORE INSERT OR UPDATE ON collect.procedure_stats_totals FOR EACH ROW EXECUTE FUNCTION public.refuse_gap()", ct);

            async Task<long> AttemptsAsync() =>
                Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM public.gap_attempts", ct));

            var dbA = new List<string?> { "DbA" };

            /* A batch that rolls back leaves no totals row at the batch's time, so it has no anchor there: the gap is tried (attempt 1). */
            var rolledBack = new CapturingLogger();
            await using (var tx = await rig.Connection.BeginTransactionAsync(ct))
            {
                Assert.Equal(1, await ProcedureStatsTotalsWriter.UpsertBatchAsync(rig.Connection, tx, serverId, batchAt, 1, 30, dbA, ct, rolledBack, "ProcTotalsGapCycle"));
                await tx.RollbackAsync(ct);
            }

            Assert.Equal(1L, await AttemptsAsync());
            Assert.Equal(0L, await TotalsRowsAsync(rig, serverId, batchAt, ct));

            /* Batch 1 of the cycle (DbA at T) commits: the gap is tried again (attempt 2), fails once, and DbA's totals row lands. */
            var logger = new CapturingLogger();
            await using (var tx = await rig.Connection.BeginTransactionAsync(ct))
            {
                Assert.Equal(1, await ProcedureStatsTotalsWriter.UpsertBatchAsync(rig.Connection, tx, serverId, batchAt, 1, 30, dbA, ct, logger, "ProcTotalsGapCycle"));
                await tx.CommitAsync(ct);
            }

            Assert.Equal(2L, await AttemptsAsync());
            var warning = Assert.Single(logger.Entries);
            Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, warning.Level);
            Assert.Equal(1L, await TotalsRowsAsync(rig, serverId, batchAt, ct));

            /* Batch 2 of the same cycle (DbB, the same T), through the product's write path: the anchor is batch 1's row at T, the range
               (T, T) is empty, and the gap statement does not run (no third attempt). Its own totals are exact. */
            Assert.Equal(1, await rig.WriteAsync(batchAt, new List<ProcedureStatsCollector.Row> { Row("DbB", "p3", 7, 1, 60) }, ct));
            Assert.Equal(2L, await AttemptsAsync());
            Assert.Equal(2L, await TotalsRowsAsync(rig, serverId, batchAt, ct));
            var (totals, raw) = await CompareAsync(rig.Connection, serverId, batchAt, ct);
            Assert.Equal(raw, totals);
            Assert.Equal(0L, await TotalsRowsAsync(rig, serverId, gapAt, ct));

            /* The next cycle's batch finds T as its anchor too: still no further attempt. */
            Assert.Equal(1, await rig.WriteAsync(T0.AddMinutes(3), new List<ProcedureStatsCollector.Row> { Row("DbA", "p2", 9, 1, 60) }, ct));
            Assert.Equal(2L, await AttemptsAsync());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// L1: a batch whose gap is empty pays the one probe statement and nothing else: no SAVEPOINT, no <c>set_config</c> and no gap
    /// statement reach the server. Counted through <c>pg_stat_statements</c> (skipped on a rig that does not preload it), once for
    /// a server whose first batch has no anchor and once for a later batch with an anchor and an empty range.
    /// </summary>
    [Fact]
    public async Task AnEmptyGap_CostsOneProbeStatement_NoSavepointNoTimeoutNoGapStatement()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945315;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotalsProbe", ct);

        var bodySucceeded = false;
        try
        {
            var preload = (string)(await ScalarAsync(rig.Connection, "SELECT current_setting('shared_preload_libraries')", ct))!;
            Assert.SkipWhen(!preload.Split(',').Select(static part => part.Trim()).Contains("pg_stat_statements"),
                "pg_stat_statements is not in shared_preload_libraries ('" + preload + "'): this rig must preload it to count the statements.");
            await ExecAsync(rig.Connection, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements", ct);

            async Task<long> CallsAsync(string like) =>
                Convert.ToInt64(await ScalarAsync(rig.Connection,
                    "SELECT COALESCE(SUM(calls), 0)::bigint FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND query LIKE '" + like + "'", ct));

            await ExecAsync(rig.Connection, "SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = current_database()), 0)", ct);

            /* The server's first batch (no totals row, so no anchor) and a second one (an anchor, an empty range between it and the batch). */
            Assert.Equal(1, await rig.WriteAsync(T0, new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10, 1, 60) }, ct));
            Assert.Equal(1, await rig.WriteAsync(T0.AddMinutes(1), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 11, 1, 60) }, ct));

            Assert.Equal(2L, await CallsAsync("%a.anchor IS NOT NULL%"));
            Assert.Equal(0L, await CallsAsync("%pst_gap%"));
            Assert.Equal(0L, await CallsAsync("%set_config(%"));
            Assert.Equal(0L, await CallsAsync("%s.collection_time > $3%"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params (NpgsqlTypes.NpgsqlDbType Type, object Value)[] parameters)
    {
        var plan = new System.Text.StringBuilder();
        await using var explain = new NpgsqlCommand(sql, connection);
        foreach (var (type, value) in parameters)
        {
            explain.Parameters.AddWithValue(type, value);
        }

        await using var reader = await explain.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    /// <summary>
    /// L3: the plain-store floor is one server-leading index probe per server that has totals, never a scan of the table or the index,
    /// and it leaves out a server without totals (which has nothing to protect). Three servers with totals plus one with older raw rows
    /// and no totals; the plan reads <c>idx_procedure_stats_time</c> with <c>server_id = servers.server_id</c> three times.
    /// </summary>
    [Fact]
    public async Task ThePlainFloor_IsAnIndexProbePerServerWithTotals_AndIgnoresAServerWithoutTotals()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945316;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotalsFloorA", ct);

        var bodySucceeded = false;
        try
        {
            var a = rig.Server;
            var b = rig.OtherServer(serverId - 1, "ProcTotalsFloorB");
            var c = rig.OtherServer(serverId - 2, "ProcTotalsFloorC");
            var d = rig.OtherServer(serverId - 3, "ProcTotalsFloorD");
            await rig.WriteAsync(T0.AddDays(3), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10, 1, 60) }, ct, a);
            await rig.WriteAsync(T0.AddDays(2), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10, 1, 60) }, ct, b);
            await rig.WriteAsync(T0.AddDays(4), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10, 1, 60) }, ct, c);
            await rig.WriteAsync(T0, new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10, 1, 60) }, ct, d);
            await ExecAsync(rig.Connection, "DELETE FROM collect.procedure_stats_totals WHERE server_id = " + d.ServerId.ToString(CultureInfo.InvariantCulture), ct);

            /* Server D (older raw rows, no totals) does not move the floor; the oldest raw row of a server WITH totals is B's, day 2. */
            Assert.Equal(T0.AddDays(2), await ProcedureStatsTotals.MeasureRawFloorAsync(rig.Connection, false, 60, ct));

            await ExecAsync(rig.Connection, "SET enable_seqscan = off", ct);
            var plan = await ExplainAsync(rig.Connection, "EXPLAIN (ANALYZE, BUFFERS) " + ProcedureStatsTotals.PlainRawFloorSql, ct);
            await ExecAsync(rig.Connection, "RESET enable_seqscan", ct);

            Assert.Matches("Index (Only )?Scan using idx_procedure_stats_time on procedure_stats p", plan);
            Assert.Contains("Index Cond: (server_id = servers.server_id)", plan, StringComparison.Ordinal);
            Assert.Contains("loops=3", plan, StringComparison.Ordinal);
            Assert.DoesNotContain("Seq Scan on procedure_stats p", plan, StringComparison.Ordinal);
            Assert.DoesNotContain("Seq Scan on procedure_stats ", plan, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The live clock (#5608): whole seconds and naive, the discipline stored timestamps follow, so an equality lookup by <c>Stamp</c> finds it. Every offset below is whole days with days of margin, so the real clock the sweep reads cannot move a verdict.</summary>
    private static DateTime NowToTheSecond() => LiveClock.Now();

    private static async Task<(long Totals, DateTime? Oldest)> TotalsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var count = Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM collect.procedure_stats_totals", ct));
        var oldest = await ScalarAsync(connection, "SELECT min(collection_time) FROM collect.procedure_stats_totals", ct) as DateTime?;
        return (count, oldest);
    }

    /// <summary>
    /// L5, plain store: the REAL retention sweep (<c>DarlingRetention.PurgeAsync</c>, not a copy of its DELETE) ages the totals out with
    /// the raw rows: the two collections past the horizon go, the recent one stays. A floor that never reached the command (the
    /// <c>$2</c> unbound) would fail the table and leave all three.
    /// </summary>
    [Fact]
    public async Task TheRealRetentionSweep_OnAPlainStore_PurgesExpiredTotals_AndKeepsTheRecentOnes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945317;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotalsPurgePlain", ct, plainStore: true);

        var bodySucceeded = false;
        try
        {
            var now = NowToTheSecond();
            foreach (var age in new[] { 60, 45, 1 })
            {
                Assert.Equal(1, await rig.WriteAsync(now.AddDays(-age), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10 + age, 1, 60) }, ct));
            }

            Assert.Equal(3L, (await TotalsAsync(rig.Connection, ct)).Totals);

            var logger = new CapturingLogger();
            await DarlingRetention.PurgeAsync(rig.DataSource, false, logger, ct, retentionDaysFor: static _ => 40);

            var (count, oldest) = await TotalsAsync(rig.Connection, ct);
            Assert.Equal(1L, count);
            Assert.True(oldest > now.AddDays(-2), "only the recent collection's totals row is left");
            Assert.DoesNotContain(logger.Entries, static entry => entry.Message.Contains("procedure_stats_totals", StringComparison.Ordinal));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// L5, TimescaleDB store: the real sweep never drops the raw chunks of <c>procedure_stats</c> (the gated purge owns them), so it
    /// holds every raw row and the floor is the oldest chunk's start: no totals row at or after it may go, though all three are older
    /// than the horizon. Once the gated purge has dropped the oldest chunk, the next real sweep purges the totals older than the
    /// new floor and keeps the rest. Without the floor bound as <c>$2</c> the first sweep would delete the old totals.
    /// </summary>
    [Fact]
    public async Task TheRealRetentionSweep_OnTimescaleDb_KeepsTotalsAtOrAfterTheHeldRawChunks()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945318;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotalsPurgeTsdb", ct, timescale: true);

        var bodySucceeded = false;
        try
        {
            var now = NowToTheSecond();
            foreach (var age in new[] { 60, 45, 1 })
            {
                Assert.Equal(1, await rig.WriteAsync(now.AddDays(-age), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10 + age, 1, 60) }, ct));
            }

            var logger = new CapturingLogger();
            await DarlingRetention.PurgeAsync(rig.DataSource, true, logger, ct, retentionDaysFor: static _ => 40);

            /* Raw is held, so nothing at or after the oldest chunk's start may go: all three totals rows are older than the 40-day horizon. */
            Assert.Equal(3L, (await TotalsAsync(rig.Connection, ct)).Totals);
            Assert.Equal(3L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats", ct)));

            /* The gated raw purge drops the 60-day chunk; the floor moves to the 45-day chunk's start. */
            await ExecAsync(rig.Connection, "SELECT drop_chunks('collect.procedure_stats', older_than => " + Stamp(now.AddDays(-50)) + ")", ct);
            await DarlingRetention.PurgeAsync(rig.DataSource, true, logger, ct, retentionDaysFor: static _ => 40);

            Assert.Equal(0L, await TotalsRowsAsync(rig, serverId, now.AddDays(-60), ct));
            Assert.Equal(1L, await TotalsRowsAsync(rig, serverId, now.AddDays(-45), ct));
            Assert.Equal(1L, await TotalsRowsAsync(rig, serverId, now.AddDays(-1), ct));
            Assert.DoesNotContain(logger.Entries, static entry => entry.Message.Contains("procedure_stats_totals", StringComparison.Ordinal));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// L5, the branch that skips the purge when the floor cannot be measured: with <c>procedure_stats</c> unreadable (renamed away) the
    /// floor probe throws, the real sweep leaves the totals untouched, counts the table failed (a warning names it) and carries on; the
    /// next sweep, with the table back, catches up.
    /// </summary>
    [Fact]
    public async Task WhenTheFloorCannotBeMeasured_TheSweepSkipsTheTotalsPurge_CountsItFailed_AndCatchesUpNextSweep()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945319;

        await using var rig = await Rig.OpenAsync(serverId, "ProcTotalsPurgeFail", ct, plainStore: true);

        var bodySucceeded = false;
        try
        {
            var now = NowToTheSecond();
            foreach (var age in new[] { 60, 45, 1 })
            {
                Assert.Equal(1, await rig.WriteAsync(now.AddDays(-age), new List<ProcedureStatsCollector.Row> { Row("DbA", "p1", 10 + age, 1, 60) }, ct));
            }

            await ExecAsync(rig.Connection, "ALTER TABLE collect.procedure_stats RENAME TO procedure_stats_hidden", ct);

            var logger = new CapturingLogger();
            await DarlingRetention.PurgeAsync(rig.DataSource, false, logger, ct, retentionDaysFor: static _ => 40);

            Assert.Equal(3L, (await TotalsAsync(rig.Connection, ct)).Totals);
            Assert.Contains(logger.Entries, static entry =>
                entry.Level == Microsoft.Extensions.Logging.LogLevel.Warning
                && entry.Message.Contains("Retention purge failed for collect.procedure_stats_totals", StringComparison.Ordinal));

            await ExecAsync(rig.Connection, "ALTER TABLE collect.procedure_stats_hidden RENAME TO procedure_stats", ct);
            await DarlingRetention.PurgeAsync(rig.DataSource, false, new CapturingLogger(), ct, retentionDaysFor: static _ => 40);

            var (count, oldest) = await TotalsAsync(rig.Connection, ct);
            Assert.Equal(1L, count);
            Assert.True(oldest > now.AddDays(-2));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
