/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live pins for the hour-ledger WRITER (#4605, plan lane 2): the query_stats batch's ledger count is added inside the
/// COPY's own transaction by <c>DarlingCollectorRunner.CopyBatchOnceAsync</c>, so the raw rows and the count commit
/// together or not at all. Every fact drives the product's own <c>WriteBatchAsync</c> (the method the collection cycle
/// calls) with real <see cref="QueryStatsCollector.Row"/>s and the real delta calculator, so the intervals are the ones
/// the collector really writes: a first sighting of a statement is a restart row (interval 0), a later sighting is a
/// measured row.
///
/// <para><b>#1776 own-store:</b> every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStatsHourLedgerWriterLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to run the hour ledger writer's live pins (each mints its own scratch database).";

    /// <summary>Fixed anchor, never wall-clock relative: a whole hour. Every collection time in a fact is an offset from it.</summary>
    private static readonly DateTime H10 = new(2026, 1, 5, 10, 0, 0, DateTimeKind.Unspecified);

    private static readonly DateTime H11 = H10.AddHours(1);

    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    /// <summary>
    /// One scratch store, migrated to the newest rung, with a runner and the shared delta calculator the batches' contexts
    /// use. The server name carries a backslash, the spelling a named instance is stored under, because the ledger's key
    /// must be that exact text.
    /// </summary>
    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }

        public required NpgsqlDataSource DataSource { get; init; }

        public required NpgsqlConnection Connection { get; init; }

        public required DarlingCollectorRunner Runner { get; init; }

        public required CollectorDeltaCalculator Deltas { get; init; }

        public required ServerRuntime Server { get; init; }

        public static async Task<Rig> OpenAsync(int serverId, string serverName, CancellationToken ct)
        {
            var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
            var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
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

        /// <summary>
        /// Writes one batch through the product's own <c>WriteBatchAsync</c>. A fault surfaces as the store's own exception
        /// (the method is async, so reflection hands back a faulted task rather than a TargetInvocationException).
        /// </summary>
        public async Task<int> WriteAsync(DateTime collectionTime, List<QueryStatsCollector.Row> rows, CancellationToken ct)
        {
            var context = new CollectorContext
            {
                ServerId = Server.ServerId,
                ServerName = Server.StorageName,
                CollectionTime = collectionTime,
                Deltas = Deltas,
                Target = Server.Target,
            };

            var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(QueryStatsCollector.Row)).Invoke(
                Runner,
                new object?[] { Connection, QueryStatsCollector.Instance, rows, Server, collectionTime, context, ct })!;
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

    /// <summary>
    /// One statement's row at counter level <paramref name="level"/>. The delta key is the handle and offsets, so a row
    /// with the same <paramref name="key"/> is the SAME statement on the next sighting; raising the level between batches
    /// is what makes its delta real (a lower level would be a counter reset).
    /// </summary>
    private static QueryStatsCollector.Row Row(string key, long level, string database = "LedgerWriterDb") => new()
    {
        DatabaseName = database,
        QueryHash = "0x" + key,
        QueryPlanHash = "0xP" + key,
        SqlHandle = "0x" + key,
        PlanHandle = "0xP" + key,
        ExecutionCount = level,
        TotalWorkerTime = level * 1000,
        TotalElapsedTime = level * 900,
        TotalLogicalReads = level * 10,
        StatementStartOffset = 0,
        StatementEndOffset = -1,
    };

    private static List<QueryStatsCollector.Row> Rows(long level, params string[] keys)
    {
        var rows = new List<QueryStatsCollector.Row>();
        foreach (var key in keys)
        {
            rows.Add(Row(key, level));
        }

        return rows;
    }

    /// <summary>The ledger's n for one (server, name, hour), or -1 when the hour has no ledger row.</summary>
    private static async Task<long> LedgerAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime bucket, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT n FROM collect.query_stats_hour_ledger WHERE server_id = $1 AND server_name = $2 AND bucket = $3", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = serverName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = bucket });
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? -1L : Convert.ToInt64(value);
    }

    /// <summary>
    /// The guard's own population for one hour: raw rows whose interval is not 0 (a NULL counts). The ledger must equal this
    /// for every hour the writer counted, which is the equation the count guard checks against the rollup.
    /// </summary>
    private static async Task<long> RawMeasuredAsync(NpgsqlConnection connection, int serverId, DateTime bucket, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(
            connection,
            "SELECT count(*) FROM collect.query_stats WHERE server_id = " + serverId.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + " AND collection_time >= '" + bucket.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)
            + "' AND collection_time < '" + bucket.AddHours(1).ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)
            + "' AND sample_interval_seconds IS DISTINCT FROM 0",
            ct));

    private static async Task<long> RawRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(
            connection, "SELECT count(*) FROM collect.query_stats WHERE server_id = " + serverId.ToString(System.Globalization.CultureInfo.InvariantCulture), ct));

    private static async Task<long> LedgerRowsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger", ct));

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
    /// The headline case, through the real write path. Batch 1 is every statement's first sighting, so every row is a restart
    /// row (interval 0) and the hour gets NO ledger row (the rollup has no row for it either). Batch 2 re-reads two of them
    /// at higher counters (measured) beside one brand-new statement (a restart row in the same batch): the hour gains
    /// exactly the two measured rows. A later batch in the same hour adds to it, and a batch in the next hour starts the next
    /// bucket. After every batch the ledger equals the raw rows the guard would count (interval IS DISTINCT FROM 0).
    /// </summary>
    [Fact]
    public async Task ABatchAddsItsNonRestartRowCount_ToItsHour_AndRestartRowsAddNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945201;
        const string serverName = "LedgerWriter\\INST";

        await using var rig = await Rig.OpenAsync(serverId, serverName, ct);

        var bodySucceeded = false;
        try
        {
            /* Batch 1: first sightings. Written, but every interval is 0, so the ledger stays empty. */
            Assert.Equal(3, await rig.WriteAsync(H10.AddMinutes(10), Rows(1, "a", "b", "c"), ct));
            Assert.Equal(3L, await RawRowsAsync(rig.Connection, serverId, ct));
            Assert.Equal(0L, await RawMeasuredAsync(rig.Connection, serverId, H10, ct));
            Assert.Equal(-1L, await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));
            Assert.Equal(0L, await LedgerRowsAsync(rig.Connection, ct));

            /* Batch 2, a minute later: a and b measured, d new (a restart row beside them). */
            var batch2 = Rows(5, "a", "b");
            batch2.Add(Row("d", 1));
            Assert.Equal(3, await rig.WriteAsync(H10.AddMinutes(11), batch2, ct));
            Assert.Equal(2L, await RawMeasuredAsync(rig.Connection, serverId, H10, ct));
            Assert.Equal(2L, await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));

            /* Batch 3, the same hour later: a, b and d are all measured now; the count ADDS to the hour's row. */
            Assert.Equal(3, await rig.WriteAsync(H10.AddMinutes(50), Rows(9, "a", "b", "d"), ct));
            Assert.Equal(5L, await RawMeasuredAsync(rig.Connection, serverId, H10, ct));
            Assert.Equal(5L, await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));

            /* Batch 4, the next hour: its own bucket; the first hour is untouched. */
            Assert.Equal(3, await rig.WriteAsync(H11.AddMinutes(5), Rows(12, "a", "b", "d"), ct));
            Assert.Equal(3L, await RawMeasuredAsync(rig.Connection, serverId, H11, ct));
            Assert.Equal(3L, await LedgerAsync(rig.Connection, serverId, serverName, H11, ct));
            Assert.Equal(5L, await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));
            Assert.Equal(2L, await LedgerRowsAsync(rig.Connection, ct));

            /* The key is the exact spelling the COPY wrote: the raw rows' server_name is the ledger's. */
            Assert.Equal(
                0L,
                Convert.ToInt64(await ScalarAsync(
                    rig.Connection,
                    "SELECT count(*) FROM collect.query_stats_hour_ledger l WHERE NOT EXISTS (SELECT 1 FROM collect.query_stats q WHERE q.server_id = l.server_id AND q.server_name = l.server_name)",
                    ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A fault in the ledger upsert aborts the whole batch: there is no savepoint, so the COPY's rows roll back with it and
    /// raw never holds rows the ledger has not counted. A trigger that refuses every ledger write stands in for the fault.
    /// After the fault clears, the next batch counts normally (nothing of the aborted batch's count lingers).
    /// </summary>
    [Fact]
    public async Task AFailedUpsert_AbortsTheBatch_SoRawAndTheLedgerAreBothUnchanged()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945202;
        const string serverName = "ledgerwriter-upsertfault";

        await using var rig = await Rig.OpenAsync(serverId, serverName, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(2, await rig.WriteAsync(H10.AddMinutes(1), Rows(1, "a", "b"), ct));
            Assert.Equal(2, await rig.WriteAsync(H10.AddMinutes(2), Rows(3, "a", "b"), ct));
            Assert.Equal(2L, await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));
            Assert.Equal(4L, await RawRowsAsync(rig.Connection, serverId, ct));

            await ExecAsync(
                rig.Connection,
                "CREATE FUNCTION collect.ledger_writer_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'ledger write refused' USING ERRCODE = 'P0001'; END $$;"
                + "CREATE TRIGGER trg_ledger_writer_refuse BEFORE INSERT OR UPDATE ON collect.query_stats_hour_ledger FOR EACH ROW EXECUTE FUNCTION collect.ledger_writer_refuse();",
                ct);

            var fault = await Assert.ThrowsAsync<PostgresException>(
                () => rig.WriteAsync(H10.AddMinutes(3), Rows(6, "a", "b"), ct));
            Assert.Equal("P0001", fault.SqlState);

            /* The COPY ran inside the same transaction, so its two rows are gone with the failed upsert. */
            Assert.Equal(4L, await RawRowsAsync(rig.Connection, serverId, ct));
            Assert.Equal(2L, await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));

            await ExecAsync(rig.Connection, "DROP TRIGGER trg_ledger_writer_refuse ON collect.query_stats_hour_ledger; DROP FUNCTION collect.ledger_writer_refuse();", ct);

            /* The next batch counts its own rows once. The aborted batch's rows moved the delta baseline, which is the
               documented cost of an unstamped fault (a lost sample, never a duplicate), so the next interval is measured
               against that baseline and both rows are still measured. */
            Assert.Equal(2, await rig.WriteAsync(H10.AddMinutes(4), Rows(9, "a", "b"), ct));
            Assert.Equal(4L, await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));
            Assert.Equal(await RawMeasuredAsync(rig.Connection, serverId, H10, ct), await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A COPY that fails after some rows were already counted by the failed attempt's writer leaves the ledger as it was, and
    /// the next batch is counted once: the count lives on the attempt's own writer, so nothing of a failed attempt can carry
    /// into the next one (the same guarantee a re-attempt on a fresh connection relies on). A CHECK constraint that
    /// rejects one marked row stands in for the COPY fault.
    /// </summary>
    [Fact]
    public async Task AFailedCopy_LeavesTheLedgerUnchanged_AndTheNextBatchIsCountedOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945203;
        const string serverName = "ledgerwriter-copyfault";

        await using var rig = await Rig.OpenAsync(serverId, serverName, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(2, await rig.WriteAsync(H10.AddMinutes(1), Rows(1, "a", "b"), ct));
            Assert.Equal(2, await rig.WriteAsync(H10.AddMinutes(2), Rows(3, "a", "b"), ct));
            Assert.Equal(2L, await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));

            await ExecAsync(
                rig.Connection,
                "ALTER TABLE collect.query_stats ADD CONSTRAINT ck_ledger_writer_refuse CHECK (database_name IS DISTINCT FROM 'REFUSE_THIS_ROW')",
                ct);

            /* Two measured rows and one the table refuses: the failed attempt's writer counted 2, the COPY threw. */
            var failing = Rows(6, "a", "b");
            failing.Add(Row("c", 1, database: "REFUSE_THIS_ROW"));
            var fault = await Assert.ThrowsAsync<PostgresException>(() => rig.WriteAsync(H10.AddMinutes(3), failing, ct));
            Assert.Equal("23514", fault.SqlState);
            Assert.Equal(4L, await RawRowsAsync(rig.Connection, serverId, ct));
            Assert.Equal(2L, await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));

            /* The next batch: two measured rows, so the ledger moves by exactly two (not by four). */
            Assert.Equal(2, await rig.WriteAsync(H10.AddMinutes(4), Rows(9, "a", "b"), ct));
            Assert.Equal(4L, await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));
            Assert.Equal(await RawMeasuredAsync(rig.Connection, serverId, H10, ct), await LedgerAsync(rig.Connection, serverId, serverName, H10, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The writer's tally, on a real binary COPY: it counts the integer the COPY sends at the watched position, a NULL counts
    /// (the rollup's <c>IS DISTINCT FROM 0</c> population includes it) and a 0 does not. The product's collector can never
    /// emit a NULL interval (<c>ResolveWorkerDelta</c> returns an int), so the NULL rule is pinned here, on the seam that would
    /// count one. A value written through another overload at the watched position is NOT tallied, and
    /// <c>CountedWrites</c> shows it, which is what the runner's guard compares with the rows it wrote. The host's own prefix
    /// writes (collection_time, server_id) reach the same overloads before the payload opens, in the runner's order, and are
    /// not tallied either, even when one of them lands on the watched position.
    /// </summary>
    [Fact]
    public async Task TheWriterTalliesTheValueItSendsAtTheIntervalPosition_ANullCountsAndAZeroDoesNot()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var rig = await Rig.OpenAsync(-945204, "ledgerwriter-seam", ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(rig.Connection, "CREATE TEMP TABLE ledger_writer_seam (a integer, b integer, c text); CREATE TEMP TABLE ledger_writer_seam_long (a integer, b bigint, c text); CREATE TEMP TABLE ledger_writer_seam_prefix (t timestamp, sid integer, a integer, b integer, c text)", ct);

            var writer = new PgCollectorRowWriter();
            writer.CountNonZeroAt(1);
            using (var importer = await rig.Connection.BeginBinaryImportAsync("COPY ledger_writer_seam (a, b, c) FROM STDIN (FORMAT BINARY)", ct))
            {
                writer.Importer = importer;
                foreach (var interval in new int?[] { 0, 60, null, 0, 5 })
                {
                    await importer.StartRowAsync(ct);
                    writer.BeginPayload();
                    writer.Value(1).Value(interval).Value("x");
                }

                await importer.CompleteAsync(ct);
            }

            Assert.Equal(3L, writer.NonZeroCounted);
            Assert.Equal(5L, writer.CountedWrites);
            Assert.Equal(5L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM ledger_writer_seam", ct)));

            /* Starting a tally again starts from zero: the per-attempt reset a re-attempt relies on. */
            writer.CountNonZeroAt(1);
            Assert.Equal(0L, writer.NonZeroCounted);
            Assert.Equal(0L, writer.CountedWrites);

            using (var importer = await rig.Connection.BeginBinaryImportAsync("COPY ledger_writer_seam_long (a, b, c) FROM STDIN (FORMAT BINARY)", ct))
            {
                writer.Importer = importer;
                await importer.StartRowAsync(ct);
                writer.BeginPayload();
                writer.Value(1).Value(7L).Value("x");
                await importer.CompleteAsync(ct);
            }

            Assert.Equal(0L, writer.NonZeroCounted);
            Assert.Equal(0L, writer.CountedWrites);

            /* The runner's own order: each row's PREFIX (collection_time, server_id) goes through these same overloads BEFORE
               BeginPayload resets the position, so on a fresh writer the first row's server_id (a Value(int)) lands at
               position 1, the watched one, and must not be tallied as the interval. A FRESH writer, because the runner builds
               one per attempt and the checks above leave `writer` already past the collision. */
            var prefixWriter = new PgCollectorRowWriter();
            prefixWriter.CountNonZeroAt(1);
            var prefixRows = 0;
            using (var importer = await rig.Connection.BeginBinaryImportAsync("COPY ledger_writer_seam_prefix (t, sid, a, b, c) FROM STDIN (FORMAT BINARY)", ct))
            {
                prefixWriter.Importer = importer;
                foreach (var interval in new int?[] { 0, 60, null })
                {
                    await importer.StartRowAsync(ct);
                    prefixWriter.Value(new DateTime(2026, 1, 5, 10, 0, 0)).Value(7);
                    prefixWriter.BeginPayload();
                    prefixWriter.Value(1).Value(interval).Value("x");
                    prefixWriter.EndPayload(3);
                    prefixRows++;
                }

                await importer.CompleteAsync(ct);
            }

            Assert.Equal(prefixRows, prefixWriter.CountedWrites);
            Assert.Equal(2L, prefixWriter.NonZeroCounted);
            Assert.Equal(3L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM ledger_writer_seam_prefix WHERE sid = 7", ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
