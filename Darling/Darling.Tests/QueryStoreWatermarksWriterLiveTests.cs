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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live pins for the per-batch Query Store watermark side table (#5684, V178), WRITE side. Every batch goes through the
/// product's own <see cref="DarlingCollectorRunner.WriteBackfillBatchAsync{TRow}"/>, which routes to the same private
/// <c>WriteBatchAsync</c> -> <c>CopyBatchOnceAsync</c> the live collection paths use, so the side rows are written by the
/// COPY's own transaction exactly as in production. The oracle is a GROUP BY over the stored raw rows.
///
/// <para><b>#1776 own-store:</b> every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStoreWatermarksWriterLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to run the Query Store watermark writer's live pins (each mints its own scratch database).";

    private static readonly DateTime T0 = new(2026, 1, 5, 10, 0, 0, DateTimeKind.Unspecified);

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }

        public required NpgsqlDataSource DataSource { get; init; }

        public required NpgsqlConnection Connection { get; init; }

        public required DarlingCollectorRunner Runner { get; init; }

        public required ServerRuntime Server { get; init; }

        public required CollectorContext Context { get; init; }

        public static async Task<Rig> OpenAsync(int serverId, string serverName, CancellationToken ct)
        {
            var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
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
                Runner = new DarlingCollectorRunner(dataSource, deltas),
                Context = new CollectorContext { ServerId = serverId, ServerName = serverName, CollectionTime = DateTime.UtcNow, Deltas = deltas },
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

        /// <summary>One batch through the product's write path; a fault surfaces as the store's own exception.</summary>
        public Task<int> WriteAsync(DateTime collectionTime, params QueryStoreCollector.Row[] rows) =>
            Runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, rows.ToList(), Server, collectionTime, Context, TestContext.Current.CancellationToken);

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await DataSource.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static QueryStoreCollector.Row Row(string database, long queryId, long planId, DateTime? last, long intervalId = 100) => new()
    {
        DatabaseName = database,
        QueryId = queryId,
        PlanId = planId,
        ExecutionTypeDesc = "Regular",
        FirstExecutionTime = T0,
        LastExecutionTime = last,
        QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
        QueryPlanHash = "0x" + planId.ToString("X8", CultureInfo.InvariantCulture),
        ExecutionCount = 1,
        AvgCpuTimeUs = 10,
        AvgDurationUs = 20,
        IsForcedPlan = false,
        ForceFailureCount = 0,
        RuntimeStatsIntervalId = intervalId,
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

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection, sql, ct), CultureInfo.InvariantCulture);

    private static string Stamp(DateTime value) => "'" + value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'::timestamp";

    /// <summary>The side rows of one server at one collection time as text, one line per database in database order.</summary>
    private static async Task<string> SideRowsAsync(NpgsqlConnection connection, int serverId, DateTime? collectionTime, CancellationToken ct)
    {
        var where = collectionTime is { } time ? " AND collection_time = " + Stamp(time) : string.Empty;
        var text = new StringBuilder();
        await using var command = new NpgsqlCommand(
            "SELECT database_name, to_char(batch_max, 'YYYY-MM-DD HH24:MI:SS.US'), max_query_id::text, max_plan_id::text, to_char(collection_time, 'MI:SS')"
            + " FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + where
            + " ORDER BY database_name, collection_time", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            text.Append(reader.GetString(0)).Append('|')
                .Append(reader.IsDBNull(1) ? "NULL" : reader.GetString(1)).Append('|')
                .Append(reader.IsDBNull(2) ? "NULL" : reader.GetString(2)).Append('|')
                .Append(reader.IsDBNull(3) ? "NULL" : reader.GetString(3));
            if (collectionTime is null)
            {
                text.Append('@').Append(reader.GetString(4));
            }

            text.Append('\n');
        }

        return text.ToString();
    }

    private static string At(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    /// <summary>
    /// A mixed batch: per database the side row is the highest last_execution_time with the ids of a row holding it (ties go to the
    /// lowest query_id, plan_id so a re-run names the same row), a NULL last_execution_time never wins over a value, microseconds are
    /// kept, and a database whose rows are all NULL still gets a row, with NULL value and NULL ids.
    /// </summary>
    [Fact]
    public async Task AMixedBatch_WritesEachDatabasesMaximumWithItsIds_AndAnAllNullDatabaseGetsANullRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568401;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-a", ct);
        var bodySucceeded = false;
        try
        {
            var high = T0.AddMinutes(5).AddTicks(1230);
            Assert.Equal(7, await rig.WriteAsync(
                T0.AddMinutes(10),
                Row("dbA", 1, 11, T0.AddMinutes(1)),
                Row("dbA", 2, 22, null),
                Row("dbA", 3, 33, high),
                Row("dbA", 4, 44, T0.AddMinutes(2)),
                Row("dbB", 9, 91, high),
                Row("dbB", 8, 82, high),
                Row("dbC", 5, 55, null)));

            Assert.Equal(
                "dbA|" + At(high) + "|3|33\n"
                + "dbB|" + At(high) + "|8|82\n"
                + "dbC|NULL|NULL|NULL\n",
                await SideRowsAsync(rig.Connection, serverId, T0.AddMinutes(10), ct));

            /* The oracle: the side value is the raw GROUP BY maximum, per database. */
            Assert.Equal(0L, await CountAsync(
                rig.Connection,
                "SELECT count(*) FROM (SELECT database_name, max(last_execution_time) m FROM collect.query_store_stats WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture)
                + " GROUP BY database_name) r FULL JOIN collect.query_store_watermarks w ON w.database_name = r.database_name AND w.server_id = " + serverId.ToString(CultureInfo.InvariantCulture)
                + " WHERE r.m IS DISTINCT FROM w.batch_max", ct));

            /* The ids name a raw row that holds the value. */
            Assert.Equal(0L, await CountAsync(
                rig.Connection,
                "SELECT count(*) FROM collect.query_store_watermarks w WHERE w.batch_max IS NOT NULL AND NOT EXISTS (SELECT 1 FROM collect.query_store_stats s"
                + " WHERE s.server_id = w.server_id AND s.database_name = w.database_name AND s.collection_time = w.collection_time AND s.query_id = w.max_query_id"
                + " AND s.plan_id = w.max_plan_id AND s.last_execution_time = w.batch_max)", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// Two batches at one key: the higher wins, and a lower one never lowers it. An all-NULL batch followed by a valued one at the
    /// same key takes the value and its ids; a valued one followed by an all-NULL one keeps them. The UpsertSql guard itself (not only
    /// the read-back's own arithmetic) is pinned by running it against a side row raised above raw by hand.
    /// </summary>
    [Fact]
    public async Task TwoBatchesAtOneKey_TheHigherWins_AndALowerOrNullOneNeverLowersIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568402;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-b", ct);
        var bodySucceeded = false;
        try
        {
            var stamp = T0.AddMinutes(10);

            /* Null first, then a value: the NULL stored row takes the value and its ids. */
            await rig.WriteAsync(stamp, Row("dbA", 1, 11, null, intervalId: 1));
            Assert.Equal("dbA|NULL|NULL|NULL\n", await SideRowsAsync(rig.Connection, serverId, stamp, ct));
            await rig.WriteAsync(stamp, Row("dbA", 2, 22, T0.AddMinutes(3), intervalId: 2));
            Assert.Equal("dbA|" + At(T0.AddMinutes(3)) + "|2|22\n", await SideRowsAsync(rig.Connection, serverId, stamp, ct));

            /* A lower value, a NULL, and an equal value at the same key leave it alone (equal keeps the first row's ids). */
            await rig.WriteAsync(stamp, Row("dbA", 3, 33, T0.AddMinutes(1), intervalId: 3));
            await rig.WriteAsync(stamp, Row("dbA", 4, 44, null, intervalId: 4));
            await rig.WriteAsync(stamp, Row("dbA", 0, 1, T0.AddMinutes(3), intervalId: 5));
            Assert.Equal("dbA|" + At(T0.AddMinutes(3)) + "|2|22\n", await SideRowsAsync(rig.Connection, serverId, stamp, ct));

            /* A higher one raises it, with its ids. */
            await rig.WriteAsync(stamp, Row("dbA", 5, 55, T0.AddMinutes(9), intervalId: 6));
            Assert.Equal("dbA|" + At(T0.AddMinutes(9)) + "|5|55\n", await SideRowsAsync(rig.Connection, serverId, stamp, ct));

            /* The guard on its own: raise the side row by hand, past every raw row, and run the real statement again. */
            await ExecAsync(
                rig.Connection,
                "UPDATE collect.query_store_watermarks SET batch_max = " + Stamp(T0.AddMinutes(500)) + ", max_query_id = 77, max_plan_id = 78 WHERE server_id = "
                + serverId.ToString(CultureInfo.InvariantCulture), ct);
            await rig.WriteAsync(stamp, Row("dbA", 6, 66, T0.AddMinutes(10), intervalId: 7));
            Assert.Equal("dbA|" + At(T0.AddMinutes(500)) + "|77|78\n", await SideRowsAsync(rig.Connection, serverId, stamp, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The batch stamp is the key: collections at different stamps (a live cycle, then backfill-shaped batches written later with
    /// backdated collection_time values, one of them with a HIGHER value than the live one) each carry their own row, and no batch
    /// moves another's. The coverage start is not touched by any of them.
    /// </summary>
    [Fact]
    public async Task BackdatedBatches_EachKeepTheirOwnRow_AndTheCoverageStartIsUntouched()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568403;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-c", ct);
        var bodySucceeded = false;
        try
        {
            var coverage = (DateTime)(await ScalarAsync(rig.Connection, "SELECT covered_since FROM collect.query_store_watermarks_coverage", ct))!;

            await rig.WriteAsync(T0.AddHours(10), Row("dbA", 1, 11, T0.AddHours(9)));
            await rig.WriteAsync(T0, Row("dbA", 2, 22, T0.AddHours(1)));
            await rig.WriteAsync(T0.AddHours(5), Row("dbA", 3, 33, T0.AddHours(30)));

            Assert.Equal(
                "dbA|" + At(T0.AddHours(1)) + "|2|22@00:00\n"
                + "dbA|" + At(T0.AddHours(30)) + "|3|33@00:00\n"
                + "dbA|" + At(T0.AddHours(9)) + "|1|11@00:00\n",
                await SideRowsAsync(rig.Connection, serverId, null, ct));
            Assert.Equal(coverage, (DateTime)(await ScalarAsync(rig.Connection, "SELECT covered_since FROM collect.query_store_watermarks_coverage", ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A batch whose COPY transaction does not commit leaves no side row, and no raw row. A deferred constraint trigger on the raw
    /// table refuses the batch at COMMIT, after the side write has already run inside the transaction, so the side row existed
    /// uncommitted and must roll back with the COPY.
    /// </summary>
    [Fact]
    public async Task ATransactionThatRollsBackAtCommit_LeavesNoSideRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568404;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-d", ct);
        var bodySucceeded = false;
        try
        {
            await rig.WriteAsync(T0.AddMinutes(1), Row("dbA", 1, 11, T0));
            Assert.Equal(1L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks", ct));

            await ExecAsync(
                rig.Connection,
                "CREATE FUNCTION collect.qswm_refuse_at_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.database_name = 'dbBoom' THEN RAISE EXCEPTION 'refused at commit' USING ERRCODE = 'P0001'; END IF; RETURN NULL; END $$;"
                + "CREATE CONSTRAINT TRIGGER trg_qswm_refuse AFTER INSERT ON collect.query_store_stats DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION collect.qswm_refuse_at_commit();",
                ct);

            var fault = await Assert.ThrowsAnyAsync<PostgresException>(
                () => rig.WriteAsync(T0.AddMinutes(2), Row("dbBoom", 2, 22, T0.AddMinutes(1))));
            Assert.Equal("P0001", fault.SqlState);

            Assert.Equal(1L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_stats", ct));
            Assert.Equal("dbA|" + At(T0) + "|1|11@01:00\n", await SideRowsAsync(rig.Connection, serverId, null, ct));

            await ExecAsync(rig.Connection, "DROP TRIGGER trg_qswm_refuse ON collect.query_store_stats; DROP FUNCTION collect.qswm_refuse_at_commit();", ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A fault in the side write rolls back to its own savepoint and the batch still commits: the raw rows are stored, the batch has no
    /// side row (a lower answer, never a higher one), the savepoint does not poison the rest of the transaction (the Query Store
    /// interval apply that runs in it still lands), and the next batch writes its row once the fault is gone.
    /// </summary>
    [Fact]
    public async Task AFailedSideWrite_RollsBackToTheSavepoint_AndTheBatchStillCommits()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568405;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-e", ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(
                rig.Connection,
                "CREATE FUNCTION collect.qswm_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'side write refused' USING ERRCODE = 'P0001'; END $$;"
                + "CREATE TRIGGER trg_qswm_refuse BEFORE INSERT OR UPDATE ON collect.query_store_watermarks FOR EACH ROW EXECUTE FUNCTION collect.qswm_refuse();",
                ct);

            Assert.Equal(1, await rig.WriteAsync(T0.AddMinutes(1), Row("dbA", 1, 11, T0)));
            Assert.Equal(1L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_stats", ct));
            Assert.Equal(0L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks", ct));
            Assert.Equal(1L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_interval_latest WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture), ct));

            await ExecAsync(rig.Connection, "DROP TRIGGER trg_qswm_refuse ON collect.query_store_watermarks; DROP FUNCTION collect.qswm_refuse();", ct);

            Assert.Equal(1, await rig.WriteAsync(T0.AddMinutes(2), Row("dbA", 2, 22, T0.AddMinutes(1))));
            Assert.Equal("dbA|" + At(T0.AddMinutes(1)) + "|2|22\n", await SideRowsAsync(rig.Connection, serverId, T0.AddMinutes(2), ct));
            Assert.Equal(string.Empty, await SideRowsAsync(rig.Connection, serverId, T0.AddMinutes(1), ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A NULL database_name is skipped (the reader's database_name = $2 never matches one), checked on the real statement against a raw
    /// row the collector cannot produce but a hand-made import could.
    /// </summary>
    [Fact]
    public async Task ARawRowWithANullDatabaseName_GetsNoSideRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568406;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-f", ct);
        var bodySucceeded = false;
        try
        {
            await rig.WriteAsync(T0.AddMinutes(1), Row("dbA", 1, 11, T0));
            await ExecAsync(
                rig.Connection,
                "INSERT INTO collect.query_store_stats (collection_id, server_id, server_name, collection_time, database_name, query_id, plan_id, last_execution_time)"
                + " VALUES ((SELECT coalesce(max(collection_id), 0) + 1 FROM collect.query_store_stats), " + serverId.ToString(CultureInfo.InvariantCulture) + ", 'qswm-5684-f', " + Stamp(T0.AddMinutes(1)) + ", NULL, 5, 6, " + Stamp(T0.AddDays(1)) + ")",
                ct);

            /* The statement is fed from the batch's own rows now: a row with a NULL database name is dropped by Summarize, and the
               statement's own IS NOT NULL guard is the second line. */
            await using (var transaction = await rig.Connection.BeginTransactionAsync(ct))
            {
                Assert.True(await QueryStoreWatermarks.TryUpsertBatchAsync(
                    rig.Connection, transaction, serverId, T0.AddMinutes(1),
                    [new QueryStoreWatermarks.BatchRow("dbA", 1, 11, T0), new QueryStoreWatermarks.BatchRow(null, 5, 6, T0.AddDays(1))],
                    30, null, ct));
                await transaction.CommitAsync(ct);
            }

            Assert.Equal("dbA|" + At(T0) + "|1|11\n", await SideRowsAsync(rig.Connection, serverId, T0.AddMinutes(1), ct));
            Assert.Equal(1L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture), ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The rung: the tables exist after the ladder ran, the coverage row holds the store's own UTC clock (naive) from when the rung ran,
    /// and running the rung again, any number of times, changes nothing and keeps the first covered_since.
    /// </summary>
    [Fact]
    public async Task TheRung_IsIdempotent_AndKeepsTheFirstCoverageStart()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        var before = DateTime.UtcNow;
        await using var rig = await Rig.OpenAsync(-568407, "qswm-5684-g", ct);
        var after = DateTime.UtcNow;
        var bodySucceeded = false;
        try
        {
            Assert.Equal(1L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks_coverage", ct));
            var first = (DateTime)(await ScalarAsync(rig.Connection, "SELECT covered_since FROM collect.query_store_watermarks_coverage", ct))!;
            Assert.Equal(DateTimeKind.Unspecified, first.Kind);
            Assert.InRange(first, before.AddSeconds(-5), after.AddSeconds(5));

            var rung = PgMigrations.Scripts.Single(m => m.Version == QueryStoreWatermarks.RungVersion);
            Assert.Equal(QueryStoreWatermarks.CreateSql, rung.Sql);
            await Task.Delay(1100, ct);
            await ExecAsync(rig.Connection, rung.Sql, ct);
            await ExecAsync(rig.Connection, rung.Sql, ct);

            Assert.Equal(1L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks_coverage", ct));
            Assert.Equal(first, (DateTime)(await ScalarAsync(rig.Connection, "SELECT covered_since FROM collect.query_store_watermarks_coverage", ct))!);
            Assert.Equal(0L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks", ct));

            /* A second coverage row is refused by the single-row key. */
            await Assert.ThrowsAsync<PostgresException>(
                () => ExecAsync(rig.Connection, "INSERT INTO collect.query_store_watermarks_coverage (singleton, covered_since) VALUES (false, now() AT TIME ZONE 'UTC')", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// Retention is a fixed two days, through the sweep's own entry point: a side row older than two days is deleted, one inside the
    /// window stays, and the coverage row is never purged.
    /// </summary>
    [Fact]
    public async Task TheRetentionSweep_DeletesSideRowsPastTwoDays_AndKeepsTheRest()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568408;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-h", ct);
        var bodySucceeded = false;
        try
        {
            Assert.Equal(2, QueryStoreWatermarks.RetentionDays);
            var now = DateTime.UtcNow;
            foreach (var ageHours in new[] { 3, 40, 47, 49, 24 * 10, 24 * 100 })
            {
                await ExecAsync(
                    rig.Connection,
                    "INSERT INTO collect.query_store_watermarks (server_id, database_name, collection_time, batch_max, max_query_id, max_plan_id) VALUES ("
                    + serverId.ToString(CultureInfo.InvariantCulture) + ", 'dbA', " + Stamp(now.AddHours(-ageHours)) + ", " + Stamp(now.AddHours(-ageHours)) + ", 1, 1)",
                    ct);
            }

            await DarlingRetention.PurgeAsync(rig.DataSource, timescaleAvailable: false, logger: null, ct);

            Assert.Equal(3L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture), ct));
            Assert.Equal(0L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks WHERE collection_time < now() AT TIME ZONE 'UTC' - interval '48 hours'", ct));
            Assert.Equal(1L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks_coverage", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The write reads NOTHING from the raw table (#5707, review round 1): the first version re-read every raw row at the shared
    /// (server_id, collection_time), so database k's upsert cost grew with k and the fan-out's total with its square. The statement
    /// that remains is an unnest of the batch's own arrays inserted into the side table. This pin runs EXPLAIN with the planner's
    /// defaults on a side table of a realistic size (a day of 40 databases every minute, analyzed) and asserts the raw table does
    /// not appear in the plan at all and the conflict is arbitrated by the primary key. It fails the day someone puts a read of
    /// <c>query_store_stats</c> back, whichever scan the planner would pick for it.
    /// </summary>
    [Fact]
    public async Task TheUpsert_ReadsNoRawRows_AndArbitratesItsConflictOnThePrimaryKey()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568409;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-i", ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(
                rig.Connection,
                "INSERT INTO collect.query_store_watermarks (server_id, database_name, collection_time, batch_max, max_query_id, max_plan_id)"
                + " SELECT " + serverId.ToString(CultureInfo.InvariantCulture) + ", 'db' || d, " + Stamp(T0) + " + make_interval(mins => m), " + Stamp(T0) + ", 1, 1"
                + " FROM generate_series(1, 40) AS d, generate_series(1, 1440) AS m; ANALYZE collect.query_store_watermarks; ANALYZE collect.query_store_stats;",
                ct);

            var statement = QueryStoreWatermarks.UpsertSql
                .Replace("$1::integer", serverId.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal)
                .Replace("$2::timestamp", Stamp(T0.AddMinutes(2000)), StringComparison.Ordinal)
                .Replace("$3::text[]", "ARRAY['db1','db2']", StringComparison.Ordinal)
                .Replace("$4::timestamp[]", "ARRAY[" + Stamp(T0) + "," + Stamp(T0) + "]", StringComparison.Ordinal)
                .Replace("$5::bigint[]", "ARRAY[1,2]::bigint[]", StringComparison.Ordinal)
                .Replace("$6::bigint[]", "ARRAY[1,2]::bigint[]", StringComparison.Ordinal);
            var plan = new StringBuilder();
            await using (var explain = new NpgsqlCommand("EXPLAIN " + statement, rig.Connection))
            await using (var reader = await explain.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    plan.AppendLine(reader.GetString(0));
                }
            }

            var text = plan.ToString();
            Assert.DoesNotContain("query_store_stats", text, StringComparison.Ordinal);
            Assert.Contains("Conflict Arbiter Indexes: " + QueryStoreWatermarks.KeyConstraintName, text, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The fan-out shape (#5707): k databases, k batches, ONE shared collection_time, as the enumerated and Azure paths write them.
    /// Each batch touches only its own database's side row. The proof is the row lock: <c>ON CONFLICT DO UPDATE</c> stamps xmax on a
    /// conflicting row even when its WHERE is false, so the first version (which re-upserted every earlier database at the stamp)
    /// left a non-zero xmax on database 1 once database 2 was written. Here every side row keeps xmax 0 after all k batches.
    /// The values still equal the raw GROUP BY oracle, sub-microsecond ticks included (the in-memory maximum is floored like the
    /// stored one).
    /// </summary>
    [Fact]
    public async Task KDatabasesInKBatchesAtOneStamp_EachBatchTouchesOnlyItsOwnSideRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568410;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-j", ct);
        var bodySucceeded = false;
        try
        {
            var stamp = T0.AddMinutes(10);
            const int databases = 6;
            for (var i = 1; i <= databases; i++)
            {
                var last = T0.AddMinutes(i).AddTicks(1237);
                await rig.WriteAsync(stamp, Row("db" + i.ToString(CultureInfo.InvariantCulture), i, i * 10, last), Row("db" + i.ToString(CultureInfo.InvariantCulture), i + 100, i * 10 + 1, T0));
            }

            Assert.Equal(databases, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture), ct));
            Assert.Equal(0L, await CountAsync(
                rig.Connection,
                "SELECT count(*) FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND xmax::text <> '0'", ct));

            /* The oracle: the side value is the raw GROUP BY maximum per database, and the ids name a raw row that holds it. */
            Assert.Equal(0L, await CountAsync(
                rig.Connection,
                "SELECT count(*) FROM (SELECT database_name, max(last_execution_time) m FROM collect.query_store_stats WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture)
                + " GROUP BY database_name) r FULL JOIN collect.query_store_watermarks w ON w.database_name = r.database_name AND w.server_id = " + serverId.ToString(CultureInfo.InvariantCulture)
                + " WHERE r.m IS DISTINCT FROM w.batch_max", ct));
            Assert.Equal(0L, await CountAsync(
                rig.Connection,
                "SELECT count(*) FROM collect.query_store_watermarks w WHERE w.batch_max IS NOT NULL AND NOT EXISTS (SELECT 1 FROM collect.query_store_stats s"
                + " WHERE s.server_id = w.server_id AND s.database_name = w.database_name AND s.collection_time = w.collection_time AND s.query_id = w.max_query_id"
                + " AND s.plan_id = w.max_plan_id AND s.last_execution_time = w.batch_max)", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A persistent side-write fault warns once per server per hour, not once per batch (#5707): a refusing trigger makes every batch
    /// fail its side write; three batches in a minute log ONE Warning (the rest Debug), a batch of another server warns on its own,
    /// and a batch an hour later warns again. The raw rows commit every time.
    /// </summary>
    [Fact]
    public async Task APersistentSideWriteFault_WarnsOncePerServerPerHour()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568411;
        const int otherServerId = -568412;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-k", ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(
                rig.Connection,
                "CREATE FUNCTION collect.qswm_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'side write refused' USING ERRCODE = 'P0001'; END $$;"
                + "CREATE TRIGGER trg_qswm_refuse BEFORE INSERT OR UPDATE ON collect.query_store_watermarks FOR EACH ROW EXECUTE FUNCTION collect.qswm_refuse();",
                ct);

            var logger = new CapturingTestLogger();
            var clock = new DateTime(2026, 3, 1, 12, 0, 0, DateTimeKind.Utc);
            var rows = new[] { new QueryStoreWatermarks.BatchRow("dbA", 1, 11, T0) };

            async Task<bool> WriteOneAsync(int server)
            {
                await using var transaction = await rig.Connection.BeginTransactionAsync(ct);
                var ran = await QueryStoreWatermarks.TryUpsertBatchAsync(
                    rig.Connection, transaction, server, T0.AddMinutes(1), rows, 30, logger, ct, () => clock);
                await transaction.CommitAsync(ct);
                return ran;
            }

            Assert.False(await WriteOneAsync(serverId));
            clock = clock.AddSeconds(20);
            Assert.False(await WriteOneAsync(serverId));
            clock = clock.AddSeconds(20);
            Assert.False(await WriteOneAsync(serverId));
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Equal(2, logger.CountAtLevel(LogLevel.Debug));

            Assert.False(await WriteOneAsync(otherServerId));
            Assert.Equal(2, logger.CountAtLevel(LogLevel.Warning));

            clock = clock.AddMinutes(61);
            Assert.False(await WriteOneAsync(serverId));
            Assert.Equal(3, logger.CountAtLevel(LogLevel.Warning));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The side-write fault warning also fires when the clock has moved backwards past the last warning (#5707): a clock that steps
    /// back would otherwise silence it until the clock caught up.
    /// </summary>
    [Fact]
    public async Task ASideWriteFaultWarning_FiresAgain_WhenTheClockMovesBackwards()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568413;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-kb", ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(
                rig.Connection,
                "CREATE FUNCTION collect.qswm_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'side write refused' USING ERRCODE = 'P0001'; END $$;"
                + "CREATE TRIGGER trg_qswm_refuse BEFORE INSERT OR UPDATE ON collect.query_store_watermarks FOR EACH ROW EXECUTE FUNCTION collect.qswm_refuse();",
                ct);

            var logger = new CapturingTestLogger();
            var clock = new DateTime(2026, 3, 1, 12, 0, 0, DateTimeKind.Utc);
            var rows = new[] { new QueryStoreWatermarks.BatchRow("dbA", 1, 11, T0) };

            async Task<bool> WriteOneAsync()
            {
                await using var transaction = await rig.Connection.BeginTransactionAsync(ct);
                var ran = await QueryStoreWatermarks.TryUpsertBatchAsync(
                    rig.Connection, transaction, serverId, T0.AddMinutes(1), rows, 30, logger, ct, () => clock);
                await transaction.CommitAsync(ct);
                return ran;
            }

            Assert.False(await WriteOneAsync());
            clock = clock.AddMinutes(10);
            Assert.False(await WriteOneAsync());
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));

            clock = clock.AddHours(-1);
            Assert.False(await WriteOneAsync());
            Assert.Equal(2, logger.CountAtLevel(LogLevel.Warning));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// Summarize is the whole of the write's arithmetic now, so it is pinned without a database: the highest value per database
    /// (floored to the microsecond), the lowest (query_id, plan_id) among rows at it, a NULL never beating a value, an all-NULL
    /// database kept with NULL ids, a NULL database dropped, and the result in database order.
    /// </summary>
    [Fact]
    public void Summarize_TakesEachDatabasesMaximum_FlooredToTheMicrosecond_WithTheLowestIdsAtIt()
    {
        var high = T0.AddMinutes(5).AddTicks(1237);
        var summary = QueryStoreWatermarks.Summarize(
        [
            new QueryStoreWatermarks.BatchRow("dbB", 9, 91, high),
            new QueryStoreWatermarks.BatchRow("dbA", 1, 11, T0.AddMinutes(1)),
            new QueryStoreWatermarks.BatchRow("dbA", 2, 22, null),
            new QueryStoreWatermarks.BatchRow("dbB", 8, 82, high.AddTicks(-17)),
            new QueryStoreWatermarks.BatchRow("dbB", 7, 99, high.AddTicks(-1)),
            new QueryStoreWatermarks.BatchRow("dbC", 5, 55, null),
            new QueryStoreWatermarks.BatchRow(null, 6, 66, high),
            new QueryStoreWatermarks.BatchRow("dbA", 3, 30, T0.AddMinutes(1)),
        ]);

        var flooredHigh = T0.AddMinutes(5).AddTicks(1230);
        Assert.Equal(3, summary.Count);
        Assert.Equal(new QueryStoreWatermarks.DatabaseMax("dbA", T0.AddMinutes(1), 1, 11), summary[0]);
        Assert.Equal(new QueryStoreWatermarks.DatabaseMax("dbB", flooredHigh, 7, 99), summary[1]);
        Assert.Equal(new QueryStoreWatermarks.DatabaseMax("dbC", null, null, null), summary[2]);
    }
}
