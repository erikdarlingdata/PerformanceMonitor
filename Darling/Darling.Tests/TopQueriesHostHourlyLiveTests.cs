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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Darling.Tests;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/* #1776 own-store: each test mints a scratch database, because it seeds one server's raw rows, builds the rollup over them and then
   deletes the rollup's ledger rows, which no other class sharing the store may see. */
/// <summary>
/// #9164 part B, live: a raw top-queries window whose whole hours the hourly host rollup holds ranks those hours from the rollup and reads
/// raw only for the edges. The oracle is the same reader with the rollup's build rows deleted, which sends it to the raw statement: every
/// row, every column and the order must match, for every ranking, the parallelism floors, a database filter and host-object grouping. The
/// seed has a NULL database, a NULL hash, ad-hoc and proc-hosted rows, a CPU tie, zero-interval rows that hold the newest text, texts that
/// change by hour, an inline text, a text with no dimension row and a WAITFOR statement the page drops.
/// </summary>
/* #9686: the two tests of the Viewer's Top Queries page are in TopQueriesHostHourlyViewerLiveTests, so this file compiles in the build without
   the Viewer and its tests run on Linux. The server, the store shape and WithStoreAsync are internal for that class. */
public sealed class TopQueriesHostHourlyLiveTests
{
    internal const int ServerId = -916401;
    internal const string ServerName = "a9164-top-queries-host-hourly";
    private const int Queries = 40;

    internal enum Shape
    {
        Plain,
        Bounds,
        RestartTarget,
    }

    private sealed record Case(string Label, TopRanking Ranking, bool ByHostObject, int MinDop, DatabaseFilter Databases, int Top);

    private static readonly Case[] Cases =
    [
        new("cpu", TopRanking.Cpu, false, 0, DatabaseFilter.All, 10),
        new("duration", TopRanking.Duration, false, 0, DatabaseFilter.All, 10),
        new("reads", TopRanking.Reads, false, 0, DatabaseFilter.All, 10),
        new("executions", TopRanking.Executions, false, 0, DatabaseFilter.All, 10),
        new("every group", TopRanking.Cpu, false, 0, DatabaseFilter.All, 60),
        new("parallel_only", TopRanking.Cpu, false, 2, DatabaseFilter.All, 10),
        new("min_dop 3", TopRanking.Duration, false, 3, DatabaseFilter.All, 10),
        new("database filter", TopRanking.Reads, false, 0, DatabaseFilter.One("db1"), 10),
        new("host objects cpu", TopRanking.Cpu, true, 0, DatabaseFilter.All, 10),
        new("host objects reads", TopRanking.Reads, true, 0, DatabaseFilter.All, 10),
        new("host objects every group", TopRanking.Executions, true, 0, DatabaseFilter.All, 60),
        new("host objects parallel_only", TopRanking.Cpu, true, 2, DatabaseFilter.All, 10),
    ];

    [Fact]
    public Task RoutedPage_EqualsTheRawPage_ForEveryRankingAndShape() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var routed = new Dictionary<string, Page>();
            foreach (var c in Cases)
            {
                routed[c.Label] = await ReadAsync(scratch, c, start, end, ct);
                Assert.True(routed[c.Label].ServedByHostHourly, $"{c.Label}: the hourly rollup did not serve the page");
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_host_hourly_built");
            foreach (var c in Cases)
            {
                var raw = await ReadAsync(scratch, c, start, end, ct);
                Assert.False(raw.ServedByHostHourly);
                Assert.NotEqual("[]", raw.Json);
                Assert.Equal(raw.Json, routed[c.Label].Json);
            }

            /* The oracle must hold the groups the seed was built for, or the comparison proves little. */
            var everything = (await ReadAsync(scratch, Cases.Single(c => c.Label == "every group"), start, end, ct)).Hashes;
            Assert.Contains("", everything);
            Assert.Contains("H038", everything);
            Assert.Contains("H039", everything);
            Assert.DoesNotContain("H011", everything);
        });

    [Fact]
    public Task RoutedPage_CountsEveryRowOnAndBesideItsBoundsOnce() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var plan = DarlingDataReader.PlanTopQueriesHostHourlyMid(start, end, DateTime.UtcNow);
            Assert.NotNull(plan);
            Assert.Equal(new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0).AddHours(1), plan!.MidStart);
            Assert.Equal(new DateTime(end.Year, end.Month, end.Day, end.Hour, 0, 0), plan.MidEnd);

            var cases = Cases.Where(c => c.Label is "cpu" or "duration" or "host objects cpu").ToArray();
            var routed = new Dictionary<string, Page>();
            foreach (var c in cases)
            {
                routed[c.Label] = await ReadAsync(scratch, c, start, end, ct);
                Assert.True(routed[c.Label].ServedByHostHourly, $"{c.Label}: the hourly rollup did not serve the page");
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_host_hourly_built");
            foreach (var c in cases)
            {
                var raw = await ReadAsync(scratch, c, start, end, ct);
                Assert.False(raw.ServedByHostHourly);
                Assert.Equal(raw.Json, routed[c.Label].Json);
                foreach (var kept in new[] { "B_BEFORE_MID", "B_AT_MIDSTART", "B_BEFORE_MIDEND", "B_AT_MIDEND", "B_AT_END" })
                {
                    Assert.Contains(kept, raw.Hashes);
                }

                foreach (var left in new[] { "B_AT_START", "B_AFTER_END" })
                {
                    Assert.DoesNotContain(left, raw.Hashes);
                }
            }
        }, Shape.Bounds);

    [Fact]
    public Task ALateBatchOfOnlyRestartRows_MovesTheRoutedAnswerLikeTheRawOne() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var cases = Cases.Where(c => c.Label is "cpu" or "host objects cpu").ToArray();
            foreach (var c in cases)
            {
                var before = await ReadAsync(scratch, c, start, end, ct);
                Assert.True(before.ServedByHostHourly);
                Assert.Contains("R_TARGET", before.Hashes);
                Assert.DoesNotContain("late restart text", before.Json);
            }

            /* The hour ledger counts only interval-honest rows, so a batch of restart rows (interval 0) arriving after the build leaves its
               hour looking complete. Such rows add to no sum but they hold the newest execution time and the newest text of their group, and
               raw reads both from every row. The routed read must answer what raw answers. */
            var at = new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0).AddHours(14).AddMinutes(7);
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest,
                                                 delta_execution_count, delta_worker_time, delta_elapsed_time, last_execution_time, sample_interval_seconds)
                VALUES (916460000001, $1, {ServerId}, '{ServerName}', 'dbR', 'R_TARGET', sha256(convert_to('late restart text', 'UTF8')), 0, 0, 0, $2, 0)
                """, at, at.AddHours(3));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.query_text_dim (digest, query_text, last_seen) VALUES (sha256(convert_to('late restart text', 'UTF8')), 'late restart text', now())");

            var routed = new Dictionary<string, Page>();
            foreach (var c in cases)
            {
                routed[c.Label] = await ReadAsync(scratch, c, start, end, ct);
                Assert.True(routed[c.Label].ServedByHostHourly, $"{c.Label}: the late restart row made the hour incomplete, which the ledger cannot see");
                Assert.Contains("late restart text", routed[c.Label].Json);
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_host_hourly_built");
            foreach (var c in cases)
            {
                var raw = await ReadAsync(scratch, c, start, end, ct);
                Assert.False(raw.ServedByHostHourly);
                Assert.Equal(raw.Json, routed[c.Label].Json);
            }
        }, Shape.RestartTarget);

    [Fact]
    public Task AnHourBuiltFromFewerRowsThanTheLedgerHolds_SendsTheReadToRaw() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var c = Cases[0];
            Assert.True((await ReadAsync(scratch, c, start, end, ct)).ServedByHostHourly);

            /* A late row for a closed, built hour: the ledger counts it (as the collector's batch does), the build did not. A rollup read would
               leave it out; the raw statement ranks it first. */
            var hour = new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0).AddHours(6);
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count,
                                                 delta_worker_time, delta_elapsed_time, sample_interval_seconds)
                VALUES (916490000001, $1, {ServerId}, '{ServerName}', 'dbL', 'LATE', 1, 90000000, 90000000, 600)
                """, hour.AddMinutes(33));
            await QueryStatsLedgerSeed.RecountAsync(connection, hour, hour.AddHours(1), ct);

            var page = await ReadAsync(scratch, c, start, end, ct);
            Assert.False(page.ServedByHostHourly);
            Assert.Equal("LATE", page.Hashes[0]);
        });

    [Fact]
    public Task AWholeHourWithNoBuild_SendsTheReadToRaw_WithTheRawAnswer() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var c = Cases[0];
            var whole = await ReadAsync(scratch, c, start, end, ct);
            Assert.True(whole.ServedByHostHourly);

            var hour = new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0).AddHours(9);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_host_hourly_built WHERE server_id = $1 AND bucket = $2", ServerId, hour);
            var hole = await ReadAsync(scratch, c, start, end, ct);
            Assert.False(hole.ServedByHostHourly);
            Assert.Equal(whole.Json, hole.Json);
        });

    [Fact]
    public Task AWindowEndingNow_WithTheNewestClosedHoursNotBuiltYet_StillRoutesAndEqualsRaw() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            /* The window ends now. The newest closed hours (past the grace) are not built until the tick's next hourly run, so the planned
               mid names hours the rollup does not hold yet. Those hours belong to the raw edge; the rest of the mid is still whole. */
            var now = end;
            var midEnd = new DateTime(end.Year, end.Month, end.Day, end.Hour, 0, 0);
            Assert.Equal(midEnd, DarlingDataReader.PlanTopQueriesHostHourlyMid(start, end, now)!.MidEnd);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "DELETE FROM collect.query_stats_host_hourly_built WHERE server_id = $1 AND bucket >= $2", ServerId, midEnd.AddHours(-2));

            var cases = Cases.Where(c => c.Label is "cpu" or "duration" or "executions" or "host objects cpu" or "parallel_only").ToArray();
            var routed = new Dictionary<string, Page>();
            foreach (var c in cases)
            {
                routed[c.Label] = await ReadAsync(scratch, c, start, end, ct, now);
                Assert.True(routed[c.Label].ServedByHostHourly, $"{c.Label}: an unbuilt tail of a window that ends now sent the read to raw");
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_host_hourly_built");
            foreach (var c in cases)
            {
                var raw = await ReadAsync(scratch, c, start, end, ct, now);
                Assert.False(raw.ServedByHostHourly);
                Assert.NotEqual("[]", raw.Json);
                Assert.Equal(raw.Json, routed[c.Label].Json);
            }
        });

    [Fact]
    public Task AnUnbuiltTail_WithAHoleBeforeIt_OrLeavingTooShortAMid_ReadsRaw() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var c = Cases[0];
            var now = end;
            var midEnd = new DateTime(end.Year, end.Month, end.Day, end.Hour, 0, 0);
            var midStart = new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0).AddHours(1);
            var whole = await ReadAsync(scratch, c, start, end, ct, now);
            Assert.True(whole.ServedByHostHourly);

            /* The tail is unbuilt, and so is an hour well before it: the hole sends the whole read to raw, with the raw answer. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "DELETE FROM collect.query_stats_host_hourly_built WHERE server_id = $1 AND (bucket >= $2 OR bucket = $3)", ServerId, midEnd.AddHours(-1), midEnd.AddHours(-9));
            var hole = await ReadAsync(scratch, c, start, end, ct, now);
            Assert.False(hole.ServedByHostHourly);
            Assert.Equal(whole.Json, hole.Json);

            /* Only the first hour of the mid is built: the shrunk mid is one hour, under the minimum, so the read is raw. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "DELETE FROM collect.query_stats_host_hourly_built WHERE server_id = $1 AND bucket > $2", ServerId, midStart);
            Assert.True(await QueryStatsHostHourly.IsCompleteAsync(connection, null, ServerId, midStart, midStart.AddHours(1), 30, ct));
            var tooShort = await ReadAsync(scratch, c, start, end, ct, now);
            Assert.False(tooShort.ServedByHostHourly);
            Assert.Equal(whole.Json, tooShort.Json);
        });

    [Fact]
    public Task AStoreWithoutTheRollupTables_ReadsRaw() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var c = Cases[0];
            var routed = await ReadAsync(scratch, c, start, end, ct);
            Assert.True(routed.ServedByHostHourly);

            await DarlingMcpTestData.ExecAsync(connection, ct, "DROP TABLE collect.query_stats_host_hourly; DROP TABLE collect.query_stats_host_hourly_built");
            var raw = await ReadAsync(scratch, c, start, end, ct);
            Assert.False(raw.ServedByHostHourly);
            Assert.Equal(routed.Json, raw.Json);
        });

    /// <summary>
    /// #9684: where raw <c>query_stats</c> is a raw hypertable, raw keeps four days and the rollup is kept to the same horizon. The window is five days
    /// back, so its hours are ones raw no longer reaches in production. They are rolled up here, with rollup rows that differ from raw's, as a
    /// thirty-day prune would have kept them, and the page names which table served it. Before the tick the rollup serves the whole
    /// middle of the window, and after it the hours are gone from the rollup, the read finds a hole and the raw statement answers with raw's rows.
    /// </summary>
    [Fact]
    public async Task OnATieredStore_AWindowPastRawsHorizon_ReadsRaw_OnceTheTickPrunesTheRollupHoursRawNoLongerHolds()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9684 top queries host hourly test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "A tiered raw table needs the TimescaleDB extension.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT _timescaledb_functions.stop_background_workers()");
        await DarlingMcpTestData.ExecAsync(connection, ct, TimescaleSupport.CreateHypertableSql("collect.query_stats", "collection_time"));
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var bodySucceeded = false;
        try
        {
            var now = LiveClock.Now();
            var first = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddDays(-5);
            var start = first.AddMinutes(30);
            var end = first.AddHours(6).AddMinutes(30);
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count,
                                                 delta_worker_time, delta_elapsed_time, min_dop, max_dop, sample_interval_seconds)
                SELECT 916840000000 + row_number() OVER (ORDER BY t), t, {ServerId}, '{ServerName}', 'dbH', 'RAW_ONLY', 1, 1000, 1000, 4, 4, 600
                FROM generate_series($1::timestamp, $2::timestamp, interval '10 minutes') AS t
                """, first, first.AddHours(7));
            await QueryStatsLedgerSeed.SeedAsync(connection, first.AddHours(-1), first.AddHours(8), ct);

            for (var hour = first.AddHours(1); hour < first.AddHours(6); hour = hour.AddHours(1))
            {
                await QueryStatsHostHourly.BuildHourAsync(connection, [ServerId], hour, now, ct);
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE collect.query_stats_host_hourly SET query_hash = 'ROLLUP_ONLY' WHERE server_id = " + ServerId);

            async Task<(DarlingDataReader.TopQueriesReadResult Result, string[] Hashes, long Executions)> ReadPastHorizonAsync()
            {
                await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
                var result = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                    source, ServerId, start, end, 10, DatabaseFilter.All, rollUpByHostObject: false, minMaxDop: 2, TopRanking.Cpu, ct, nowUtc: now);
                Assert.Equal(RetentionTier.Raw, result.Tier);
                return (result, result.Rows.Select(r => r.QueryHash).ToArray(), result.Rows.Sum(r => r.TotalExecutions));
            }

            long rawRowsInWindow;
            await using (var count = new NpgsqlCommand(
                $"SELECT count(*) FROM collect.query_stats WHERE server_id = {ServerId} AND collection_time > @start AND collection_time <= @end", connection))
            {
                count.Parameters.AddWithValue("start", start);
                count.Parameters.AddWithValue("end", end);
                rawRowsInWindow = (long)(await count.ExecuteScalarAsync(ct))!;
            }

            Assert.True(rawRowsInWindow > 0);

            var before = await ReadPastHorizonAsync();
            Assert.True(before.Result.ServedByHostHourly, "the setup must make the rollup serve the window, or the read below proves nothing");
            Assert.Contains("ROLLUP_ONLY", before.Hashes);

            await using (var postgres = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                var tick = await QueryStatsHostHourly.RunTickAsync(postgres, now, 30, NullLogger.Instance, ct);
                Assert.Equal(0, tick.Failed);
            }

            var after = await ReadPastHorizonAsync();
            Assert.False(after.Result.ServedByHostHourly);
            Assert.Equal(["RAW_ONLY"], after.Hashes);
            Assert.Equal(rawRowsInWindow, after.Executions);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                Assert.Equal(0L, Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt)));
            });
        }
    }

    [Fact]
    public Task RoutedStatement_ReadsFewerPagesThanTheRawStatement() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var mid = DarlingDataReader.PlanTopQueriesHostHourlyMid(start, end, DateTime.UtcNow);
            Assert.NotNull(mid);
            var rawSql = TopRankings.Apply(DarlingDataReader.TopQueriesSql, TopRanking.Cpu, hourly: false);
            var routedSql = TopRankings.ApplyHostHourly(DarlingDataReader.TopQueriesHostHourlySql, TopRanking.Cpu);
            var rawPages = ExplainBuffers.DataPages(await ExplainAsync(connection, rawSql, start, end, null, ct));
            var routedPages = ExplainBuffers.DataPages(await ExplainAsync(connection, routedSql, start, end, mid, ct));
            Assert.True(routedPages < rawPages, $"the routed statement read {routedPages} pages, the raw statement {rawPages}");
        });

    private static async Task<object?> ExplainAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, DarlingDataReader.HostHourlyMid? mid, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 10 });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 0 });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 15 });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        if (mid is not null)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mid.MidStart });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mid.MidEnd });
        }

        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>A routed page: its rows as JSON so every column compares, which statement served it, and the hashes in page order.</summary>
    private sealed record Page(string Json, bool ServedByHostHourly, string[] Hashes);

    /// <summary>The routed read through a fresh data source (coverage and availability are cached per source).</summary>
    private static async Task<Page> ReadAsync(ScratchPostgres scratch, Case c, DateTime start, DateTime end, CancellationToken ct, DateTime? nowUtc = null)
    {
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var result = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
            source, ServerId, start, end, c.Top, c.Databases, c.ByHostObject, c.MinDop, c.Ranking, ct, nowUtc: nowUtc);
        Assert.Equal(RetentionTier.Raw, result.Tier);
        return new Page(JsonSerializer.Serialize(result.Rows), result.ServedByHostHourly, result.Rows.Select(r => r.QueryHash).ToArray());
    }

    /// <summary>
    /// Plants <see cref="Queries"/> statements, a row every ten minutes from two hours before the window to one hour after it, builds
    /// the hourly host rollup over all of it with the service's own tick, and runs the body. The window is 24 hours, ends 21 minutes past
    /// an hour of the previous UTC day (on the seed's grid, so a row sits exactly on its end and exactly on its start) and is inside
    /// raw retention, so both of its ends are raw edges and every hour between them is whole.
    /// </summary>
    internal static async Task WithStoreAsync(Func<ScratchPostgres, NpgsqlConnection, DateTime, DateTime, CancellationToken, Task> body, Shape shape = Shape.Plain)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9164 top queries host hourly tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT _timescaledb_functions.stop_background_workers()");
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var end = LiveClock.AnchoredEndNotAfterNow().AddMinutes(21);
        var start = end.AddHours(-24);

        /* Statement n carries m = n, except 39, which carries 38's load under its own hash, so the two tie on every ranking and the
           group key orders them. 7 has no database and 8 no hash; every fifth statement is hosted by one of four procedures, so the
           host-object lens folds several hashes into one row. 11 is a WAITFOR statement with fifty times the load: it ranks first and
           the page drops it. Statements 3 and 4 change text by hour (two and three digests), 12 has only an inline text and 15's
           digest has no dimension row. */
        await DarlingMcpTestData.ExecAsync(connection, ct, $"""
            INSERT INTO collect.query_stats
                (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                 host_object_name, query_text_digest, query_text,
                 delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads,
                 delta_rows, delta_spills, min_dop, max_dop, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                 last_execution_time, creation_time, min_physical_reads, max_physical_reads, min_rows, max_rows,
                 min_grant_kb, max_grant_kb, min_used_grant_kb, max_used_grant_kb, min_ideal_grant_kb, max_ideal_grant_kb,
                 min_spills, max_spills, min_reserved_threads, max_reserved_threads, min_used_threads, max_used_threads,
                 total_clr_time, plan_generation_num, sample_interval_seconds)
            SELECT 916400000000 + row_number() OVER (ORDER BY n, t), t, {ServerId}, '{ServerName}',
                   CASE WHEN n = 7 THEN NULL ELSE 'db' || (n % 3) END,
                   CASE WHEN n = 8 THEN NULL ELSE 'H' || lpad(n::text, 3, '0') END,
                   'QP' || m || (k % 2), 'SH' || m || (k % 3), 'PH' || m || (k % 3),
                   CASE WHEN n % 5 = 0 THEN 'dbo.Proc' || (n % 4) END,
                   CASE WHEN n = 12 THEN NULL
                        WHEN n = 15 THEN sha256(convert_to('orphan15', 'UTF8'))
                        ELSE sha256(convert_to('txt' || m || CASE WHEN m = 3 THEN h % 2 WHEN m = 4 THEN h % 3 ELSE 0 END, 'UTF8')) END,
                   CASE WHEN n = 12 THEN 'inline 12' END,
                   1 + m % 7 + k % 3, (1000 + m * 10 + k % 7) * CASE WHEN n = 11 THEN 50 ELSE 1 END, 5000 - m * 7 + k % 5,
                   m * 100 + k % 11, m + k % 3, k % 4, m * 3, CASE WHEN k % 9 = 0 THEN 1 END,
                   1, CASE WHEN m % 3 = 0 THEN 2 + k % 4 WHEN m % 3 = 1 THEN 1 END,
                   m + k % 13, 10 * m + k % 17, m + k % 19, 20 * m + k % 23,
                   t - interval '1 minute', timestamp '2026-01-01' + (k % 5) * interval '1 minute', k % 3, 5 + k % 7, m % 5, 100 + k % 29,
                   CASE WHEN k % 4 = 0 THEN NULL ELSE 100 + k % 31 END, 1000 + k % 37, 50 + k % 41, 500 + k % 43, 200 + k % 47, 2000 + k % 53,
                   0, k % 5, 1, 1 + k % 6, 1, 1 + k % 5, k % 100, 1 + k % 10, 600
            FROM generate_series(1, {Queries}) AS n
            CROSS JOIN generate_series($1::timestamp, $2::timestamp, interval '10 minutes') AS t
            CROSS JOIN LATERAL (SELECT CASE WHEN n = 39 THEN 38 ELSE n END AS m,
                                       extract(epoch FROM t)::bigint / 600 AS k, extract(hour FROM t)::int AS h) AS v
            """, start.AddHours(-2), end.AddHours(1));

        await DarlingMcpTestData.ExecAsync(connection, ct, """
            INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
            SELECT sha256(convert_to('txt' || m || v, 'UTF8')),
                   CASE WHEN m = 11 THEN 'WAITFOR DELAY ''00:00:05''' ELSE 'SELECT ' || m || ' /* text ' || v || ' */' END, now()
            FROM generate_series(1, 40) AS m CROSS JOIN generate_series(0, 2) AS v
            ON CONFLICT (digest) DO NOTHING
            """);

        /* Zero-interval rows (a restart: no delta is knowable) hold the newest text and the newest execution time of their group, but
           take no part in any sum, extreme, handle or count. One lands in each edge and one in the middle, and none shares a
           digest with the honest rows. */
        await DarlingMcpTestData.ExecAsync(connection, ct, $"""
            INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, host_object_name, query_text_digest,
                                             delta_execution_count, delta_worker_time, delta_elapsed_time, last_execution_time, max_dop, sample_interval_seconds)
            SELECT 916480000000 + row_number() OVER (ORDER BY n, t), t, {ServerId}, '{ServerName}', 'db' || (n % 3), 'H' || lpad(n::text, 3, '0'),
                   CASE WHEN n % 5 = 0 THEN 'dbo.Proc' || (n % 4) END, sha256(convert_to('zero' || n, 'UTF8')), 999, 999999, 999999, t + interval '30 minutes', 9, 0
            FROM unnest(ARRAY[3, 5, 20, 30, 38]) AS n
            CROSS JOIN unnest(ARRAY[$1::timestamp, $2::timestamp, $3::timestamp]) AS t
            """, start.AddMinutes(10), start.AddHours(12).AddMinutes(5), end.AddMinutes(-20));
        await DarlingMcpTestData.ExecAsync(connection, ct, """
            INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
            SELECT sha256(convert_to('zero' || n, 'UTF8')), 'SELECT zero ' || n, now() FROM unnest(ARRAY[3, 5, 20, 30, 38]) AS n
            ON CONFLICT (digest) DO NOTHING
            """);

        if (shape == Shape.Bounds)
        {
            /* One row per statement, on and beside each bound, each heavy enough to rank first when counted, so a row counted twice shows as
               a doubled total and a row dropped as a missing statement. The first and last are outside the window and must not appear. */
            var midStart = new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(1);
            var midEnd = new DateTime(end.Year, end.Month, end.Day, end.Hour, 0, 0, DateTimeKind.Unspecified);
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count,
                                                 delta_worker_time, delta_elapsed_time, sample_interval_seconds)
                VALUES (916470000001, $1, {ServerId}, '{ServerName}', 'dbB', 'B_AT_START', 1, 5000000, 5000000, 600),
                       (916470000002, $2, {ServerId}, '{ServerName}', 'dbB', 'B_BEFORE_MID', 1, 5000000, 5000000, 600),
                       (916470000003, $3, {ServerId}, '{ServerName}', 'dbB', 'B_AT_MIDSTART', 1, 5000000, 5000000, 600),
                       (916470000004, $4, {ServerId}, '{ServerName}', 'dbB', 'B_BEFORE_MIDEND', 1, 5000000, 5000000, 600),
                       (916470000005, $5, {ServerId}, '{ServerName}', 'dbB', 'B_AT_MIDEND', 1, 5000000, 5000000, 600),
                       (916470000006, $6, {ServerId}, '{ServerName}', 'dbB', 'B_AT_END', 1, 5000000, 5000000, 600),
                       (916470000007, $7, {ServerId}, '{ServerName}', 'dbB', 'B_AFTER_END', 1, 5000000, 5000000, 600)
                """, start, midStart.AddSeconds(-1), midStart, midEnd.AddSeconds(-1), midEnd, end, end.AddSeconds(1));
        }

        if (shape == Shape.RestartTarget)
        {
            /* A heavy statement that stops running 12 hours into the window, so a restart row after that is the newest row of its group. */
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest,
                                                 delta_execution_count, delta_worker_time, delta_elapsed_time, last_execution_time, sample_interval_seconds)
                SELECT 916450000000 + row_number() OVER (ORDER BY t), t, {ServerId}, '{ServerName}', 'dbR', 'R_TARGET', sha256(convert_to('rtext', 'UTF8')),
                       3, 500000, 500000, t - interval '1 minute', 600
                FROM generate_series($1::timestamp, $2::timestamp, interval '10 minutes') AS t
                """, start.AddHours(1), start.AddHours(12));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.query_text_dim (digest, query_text, last_seen) VALUES (sha256(convert_to('rtext', 'UTF8')), 'SELECT r_target', now())");
        }

        await QueryStatsLedgerSeed.SeedAsync(connection, start.AddHours(-4), end.AddHours(3), ct);
        await using (var postgres = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            var tick = await QueryStatsHostHourly.RunTickAsync(postgres, end.AddHours(3), 30, NullLogger.Instance, ct);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(0, tick.Deferred);
        }

        var bodySucceeded = false;
        try
        {
            await body(scratch, connection, start, end, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }
}
