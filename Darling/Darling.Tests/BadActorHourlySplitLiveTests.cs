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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints a scratch database, because it materializes a continuous aggregate, which no other
   class sharing the store may see. */
/// <summary>
/// #9879, live: the bad actor read that takes the whole hours before the window's last UTC day from the hourly rollup
/// (<see cref="PgFactCollector.BadActorHourlySplitSql"/>) returns the same five rows, with the same figures, as the raw
/// read (<see cref="PgFactCollector.BadActorSql"/>) over the same window, and the collector states those figures as its
/// BAD_ACTOR facts.
///
/// <para>The planted week has a query whose 100 executions sit a third in each arm (so losing any arm drops it under
/// the execution bar), a query with a row exactly on the window start, the first hour line, the cut and the window
/// end plus a row a second outside each end, a counters-went-down row (a 0 interval beside all-zero deltas), rows with
/// 0 executions and non-zero CPU (#8403) in the rollup's hours and in the last day, two sql_handles in one hour, one
/// hash in two databases, NULL reads, and three queries that rank below the five.</para>
///
/// <para>Every payload column of <c>collect.query_stats</c> is nullable, and the rollup groups by three of them
/// (database, hash, sql_handle). The NULL-group test adds a group with no database name and a group with no query
/// hash, each with rows in all three arms, a NULL sql_handle, a NULL interval and rows whose CPU delta is NULL beside
/// real executions (#4394), next to groups below the five that differ from them only in the database or the hash and
/// carry far more spills and a far higher DOP.</para>
/// </summary>
public sealed class BadActorHourlySplitLiveTests
{
    private const int ServerId = -987907;
    private const string ServerName = "a9879-bad-actor-split";
    private const string Db = "split_db";
    private const string OtherDb = "other_db";

    private const string Barely = "0x9879BARELY";
    private const string Edges = "0x9879EDGES";
    private const string PartHour = "0x9879PARTHOUR";
    private const string LastDay = "0x9879LASTDAY";
    private const string Rollup = "0x9879ROLLUP";
    private const string NoDb = "0x9879NODB";

    private static readonly DateTime Start = At(2, 6, 20, 0);
    private static readonly DateTime End = At(9, 6, 20, 0);
    private static readonly DateTime FirstHour = At(2, 7, 0, 0);
    private static readonly DateTime EndMidnight = At(9, 0, 0, 0);

    private sealed record Row(
        string Db, string Hash, long Executions, double AvgCpuMs, double AvgElapsedMs, double AvgReads,
        long TotalCpuUs, long TotalReads, long Spills, int MaxDop);

    private static DateTime At(int day, int hour, int minute, int second) =>
        new(2026, 3, day, hour, minute, second, DateTimeKind.Unspecified);

    [Fact]
    public Task TheSplitRead_ReturnsTheRawReadsFiveRows_OverEveryArmBoundary() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            await PlantWeekAsync(connection, ct);
            var raw = await ReadRawAsync(scratch, ct);
            AssertTheFixtureRanksTheFiveSpecialQueries(raw);

            await RefreshAsync(connection, Start.AddHours(-1), End.AddHours(1), ct);

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var read = await ChooseAsync(source, ct);
            Assert.True(read.UsesRollup, "the rollup reaches the first hour line, so the split serves");
            Assert.Equal(FirstHour, read.FirstHour);
            Assert.Equal(EndMidnight, read.Cut);

            Assert.Equal(raw, await ReadSplitAsync(scratch, read, ct));
            Assert.Equal(ByHash(raw), await CollectorRowsAsync(source));
        });

    [Fact]
    public Task ACeilingBelowTheEndsMidnight_CutsTheRollupArmThere_AndTheRowsStayTheRawReads() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            await PlantWeekAsync(connection, ct);
            var raw = await ReadRawAsync(scratch, ct);
            AssertTheFixtureRanksTheFiveSpecialQueries(raw);

            /* Materialized only up to noon two days before the end: the hours after it are read from raw rows. */
            await RefreshAsync(connection, Start.AddHours(-1), At(7, 12, 0, 0), ct);

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var read = await ChooseAsync(source, ct);
            Assert.True(read.UsesRollup, "the rollup reaches the first hour line, so the split serves");
            Assert.Equal(FirstHour, read.FirstHour);
            Assert.True(read.Cut > FirstHour && read.Cut <= At(7, 12, 0, 0), $"the cut follows the ceiling, got {read.Cut:o}");

            Assert.Equal(raw, await ReadSplitAsync(scratch, read, ct));
            Assert.Equal(ByHash(raw), await CollectorRowsAsync(source));
        });

    [Fact]
    public Task GroupsWithNoDatabaseNameOrNoQueryHash_AndRowsWithNoCpuDelta_AreTheRawReadsRows() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            await PlantWeekAsync(connection, ct);
            await PlantNullGroupsAsync(connection, ct);
            var raw = await ReadRawAsync(scratch, ct);
            AssertTheFixtureRanksTheTwoNullGroups(raw);

            await RefreshAsync(connection, Start.AddHours(-1), End.AddHours(1), ct);

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var read = await ChooseAsync(source, ct);
            Assert.True(read.UsesRollup, "the rollup reaches the first hour line, so the split serves");
            Assert.Equal(FirstHour, read.FirstHour);
            Assert.Equal(EndMidnight, read.Cut);

            Assert.Equal(raw, await ReadSplitAsync(scratch, read, ct));
            Assert.Equal(ByHash(raw), await CollectorRowsAsync(source));
        });

    [Fact]
    public Task ARollupThatDoesNotReachTheFirstHourLine_ReadsRawRows() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            await PlantWeekAsync(connection, ct);
            var raw = await ReadRawAsync(scratch, ct);
            AssertTheFixtureRanksTheFiveSpecialQueries(raw);

            /* Nothing materialized: no floor. */
            await using (var empty = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                Assert.False((await ChooseAsync(empty, ct)).UsesRollup, "an empty rollup serves nothing");
                Assert.Equal(ByHash(raw), await CollectorRowsAsync(empty));
            }

            /* Materialized from the window's third day on: the floor is past the first hour line. */
            await RefreshAsync(connection, At(4, 0, 0, 0), End.AddHours(1), ct);

            await using var late = NpgsqlDataSource.Create(scratch.ConnectionString);
            var read = await ChooseAsync(late, ct);
            Assert.False(read.UsesRollup, "a floor past the first hour line leaves the whole window to raw rows");
            Assert.Equal(PgFactCollector.BadActorSql, read.Sql);
            Assert.Equal(ByHash(raw), await CollectorRowsAsync(late));
        });

    private static List<Row> ByHash(List<Row> rows) => rows.OrderBy(r => r.Hash, StringComparer.Ordinal).ToList();

    /// <summary>The five rows the fixture is built to rank: each one carries a case the split must get right.</summary>
    private static void AssertTheFixtureRanksTheFiveSpecialQueries(List<Row> raw)
    {
        Assert.Equal(
            new[] { Barely, Edges, LastDay, PartHour, Rollup },
            raw.Select(r => r.Hash).OrderBy(h => h, StringComparer.Ordinal).ToArray());
        Assert.All(raw, r => Assert.Equal(Db, r.Db));
        Assert.Equal(100L, raw.Single(r => r.Hash == Barely).Executions);
        /* The eight rows inside the closed window, and neither row a second outside it. */
        Assert.Equal(25_500L, raw.Single(r => r.Hash == Edges).Executions);
        Assert.Equal(8, raw.Single(r => r.Hash == Edges).MaxDop);
        Assert.Equal(12L, raw.Single(r => r.Hash == Edges).Spills);
        Assert.Equal(3L, raw.Single(r => r.Hash == Rollup).Spills);
    }

    private static async Task PlantWeekAsync(NpgsqlConnection c, CancellationToken ct)
    {
        /* 100 executions, 30 + 40 + 30 over the part hour, the rollup's hours and the last day. */
        await PlantAsync(c, ct, At(2, 6, 50, 0), Db, Barely, 30, 50_000_000, 3_000);
        await PlantAsync(c, ct, At(5, 10, 10, 0), Db, Barely, 40, 50_000_000, 4_000);
        await PlantAsync(c, ct, At(9, 3, 10, 0), Db, Barely, 30, 50_000_000, 3_000);

        /* One row on each edge and beside it. CPU per execution grows row by row, so a dropped or doubled row moves
           the averages as well as the sums. */
        var edges = new (DateTime At, long Executions, long Spills, int MaxDop)[]
        {
            (Start, 100, 0, 1),                 /* exactly the window start */
            (At(2, 6, 45, 0), 200, 0, 1),
            (FirstHour, 400, 0, 1),             /* exactly the first hour line */
            (At(2, 7, 10, 0), 800, 7, 4),
            (At(8, 23, 59, 59), 1_600, 0, 1),
            (EndMidnight, 3_200, 0, 1),         /* exactly the cut when the rollup covers the week */
            (At(9, 0, 30, 0), 6_400, 5, 2),
            (End, 12_800, 0, 1),                /* exactly the window end */
            (Start.AddSeconds(-1), 25_600, 0, 1),
            (End.AddSeconds(1), 51_200, 0, 1),
        };
        for (var i = 0; i < edges.Length; i++)
        {
            var e = edges[i];
            await PlantAsync(c, ct, e.At, Db, Edges, e.Executions, e.Executions * 10_000 * (i + 1), e.Executions * (i + 1),
                spills: e.Spills, maxDop: e.MaxDop);
        }

        /* Counters went down: a 0 interval beside all-zero deltas. The rollup leaves the row out, and its max_dop
           still counts, as it does in the raw read. */
        await PlantAsync(c, ct, At(6, 12, 0, 10), Db, Edges, 0, 0, 0, interval: 0, maxDop: 8);

        /* Most of its work in the part hour before the first hour line, and a 0-execution row with CPU and reads
           (#8403) in the rollup's hours. */
        await PlantAsync(c, ct, At(2, 6, 30, 0), Db, PartHour, 1_000, 90_000_000, 2_000_000);
        await PlantAsync(c, ct, At(4, 10, 15, 0), Db, PartHour, 10, 100_000, 100);
        await PlantAsync(c, ct, At(4, 11, 0, 30), Db, PartHour, 0, 9_000_000, 5_000_000);

        /* All of its work in the last day, with a 0-execution row (#8403) and a NULL reads row. */
        await PlantAsync(c, ct, At(9, 1, 15, 0), Db, LastDay, 400, 28_000_000, null);
        await PlantAsync(c, ct, At(9, 5, 0, 0), Db, LastDay, 400, 28_000_000, 800);
        await PlantAsync(c, ct, At(9, 2, 0, 0), Db, LastDay, 0, 3_000_000, 70);

        /* All of its work in the rollup's hours: two sql_handles in one hour, and the week's last whole hour. */
        await PlantAsync(c, ct, At(3, 8, 10, 0), Db, Rollup, 500, 30_000_000, 1_000, handle: "0x9879H1");
        await PlantAsync(c, ct, At(3, 8, 40, 0), Db, Rollup, 250, 15_000_000, null, handle: "0x9879H2");
        await PlantAsync(c, ct, At(5, 14, 40, 0), Db, Rollup, 500, 30_000_000, 2_000);
        await PlantAsync(c, ct, At(8, 23, 50, 0), Db, Rollup, 250, 15_000_000, 500, spills: 3, maxDop: 2);
        /* The same hash in a second database is its own group, and ranks below the five. */
        await PlantAsync(c, ct, At(7, 3, 3, 0), OtherDb, Rollup, 150, 4_500_000, 300);

        /* Three more groups over the bar that rank below the five, one in each arm. */
        await PlantAsync(c, ct, At(4, 9, 0, 0), Db, "0x9879FILLER1", 300, 15_000_000, 10);
        await PlantAsync(c, ct, At(9, 4, 0, 0), Db, "0x9879FILLER2", 500, 20_000_000, 10);
        await PlantAsync(c, ct, At(2, 6, 40, 0), Db, "0x9879FILLER3", 400, 14_000_000, 10);
    }

    /// <summary>
    /// The two NULL groups rank second and third, with their own rows' figures: the spills and DOP of the groups that
    /// share only their hash or only their database are not theirs. A NULL name reads as the empty string.
    /// </summary>
    private static void AssertTheFixtureRanksTheTwoNullGroups(List<Row> raw)
    {
        Assert.Equal(new[] { Barely, NoDb, "", Edges, PartHour }, raw.Select(r => r.Hash).ToArray());

        var noDatabase = raw.Single(r => r.Hash == NoDb);
        Assert.Equal("", noDatabase.Db);
        Assert.Equal(350L, noDatabase.Executions);
        Assert.Equal(90_000_000L, noDatabase.TotalCpuUs);
        Assert.Equal(6_500L, noDatabase.TotalReads);
        Assert.Equal(6L, noDatabase.Spills);
        Assert.Equal(6, noDatabase.MaxDop);

        var noHash = raw.Single(r => r.Hash.Length == 0);
        Assert.Equal(Db, noHash.Db);
        Assert.Equal(350L, noHash.Executions);
        Assert.Equal(75_000_000L, noHash.TotalCpuUs);
        Assert.Equal(4_650L, noHash.TotalReads);
        Assert.Equal(4L, noHash.Spills);
        Assert.Equal(5, noHash.MaxDop);
    }

    private static async Task PlantNullGroupsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        /* No database name: a row in each arm, a NULL sql_handle in the rollup's hours, and a row alone in its hour
           whose CPU delta is NULL beside real executions (#4394), so that hour's CPU sum is NULL. */
        await PlantAsync(c, ct, At(2, 6, 35, 0), null, NoDb, 100, 30_000_000, 1_000, spills: 2, maxDop: 2);
        await PlantAsync(c, ct, At(5, 9, 5, 0), null, NoDb, 100, 40_000_000, 2_000, maxDop: 6, handle: null);
        await PlantAsync(c, ct, At(5, 16, 5, 0), null, NoDb, 50, null, 500, elapsedUs: 7_000_000);
        await PlantAsync(c, ct, At(9, 2, 5, 0), null, NoDb, 100, 20_000_000, 3_000, spills: 4, maxDop: 3);
        /* The same hash with a database name is its own group, below the five. */
        await PlantAsync(c, ct, At(5, 9, 6, 0), Db, NoDb, 150, 1_500_000, 10, spills: 40, maxDop: 16);

        /* No query hash: a row in each arm, a NULL interval (the rollup keeps it: only a 0 interval is left out), and a
           NULL CPU delta beside a known one in the same hour, and in the last day. */
        await PlantAsync(c, ct, At(2, 6, 36, 0), Db, null, 100, 25_000_000, 1_500, spills: 1, maxDop: 2);
        await PlantAsync(c, ct, At(6, 3, 10, 0), Db, null, 100, 35_000_000, 2_500, interval: null, maxDop: 5);
        await PlantAsync(c, ct, At(6, 3, 40, 0), Db, null, 20, null, 100, elapsedUs: 3_000_000);
        await PlantAsync(c, ct, At(9, 3, 30, 0), Db, null, 100, 15_000_000, 500, spills: 3, maxDop: 4);
        await PlantAsync(c, ct, At(9, 3, 45, 0), Db, null, 30, null, 50, elapsedUs: 1_000_000);
        /* No hash in another database, and no hash with no database: each its own group, below the five. */
        await PlantAsync(c, ct, At(6, 3, 11, 0), OtherDb, null, 150, 1_200_000, 10, spills: 50, maxDop: 32);
        await PlantAsync(c, ct, At(6, 3, 12, 0), null, null, 150, 900_000, 10, spills: 60, maxDop: 64);
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string? db, string? hash, long executions, long? cpuUs,
        long? reads, int? interval = 60, long spills = 0, int maxDop = 1, string? handle = "0x9879H0", long? elapsedUs = null)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_spills, max_dop,
     sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue((object?)db ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)hash ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)handle ?? DBNull.Value);
        command.Parameters.AddWithValue(executions);
        command.Parameters.AddWithValue((object?)cpuUs ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)(elapsedUs ?? cpuUs * 2) ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)reads ?? DBNull.Value);
        command.Parameters.AddWithValue(spills);
        command.Parameters.AddWithValue(maxDop);
        command.Parameters.AddWithValue((object?)interval ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.QueryStatsIoHourlyView}'::regclass, $1::timestamp, $2::timestamp)",
            connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The read the collector picks for this window, from the same coverage probe it runs.</summary>
    private static async Task<PgFactCollector.BadActorRead> ChooseAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        var rollups = await TimescaleSupport.DetectRollupsAsync(source, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(source, rollups, ct);
        return PgFactCollector.ChooseBadActorRead(
            Start, End,
            coverage.FloorOf(TimescaleSupport.QueryStatsIoHourlyView),
            coverage.CeilingOf(TimescaleSupport.QueryStatsIoHourlyView));
    }

    private static Task<List<Row>> ReadRawAsync(ScratchPostgres scratch, CancellationToken ct) =>
        ReadAsync(scratch, PgFactCollector.BadActorSql, ct, Start, End);

    private static Task<List<Row>> ReadSplitAsync(ScratchPostgres scratch, PgFactCollector.BadActorRead read, CancellationToken ct) =>
        ReadAsync(scratch, PgFactCollector.BadActorHourlySplitSql, ct, Start, End, read.FirstHour!.Value, read.Cut!.Value);

    private static async Task<List<Row>> ReadAsync(ScratchPostgres scratch, string sql, CancellationToken ct, params DateTime[] bounds)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using (var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection))
        {
            await setPath.ExecuteNonQueryAsync(ct);
        }

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(ServerId);
        foreach (var bound in bounds)
        {
            command.Parameters.AddWithValue(bound);
        }

        var rows = new List<Row>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new Row(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                Whole(reader, 2), Real(reader, 3), Real(reader, 4), Real(reader, 5),
                Whole(reader, 6), Whole(reader, 7), Whole(reader, 8), (int)Whole(reader, 9)));
        }

        return rows;
    }

    private static long Whole(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? 0L : Convert.ToInt64(reader.GetValue(ordinal));

    private static double Real(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? 0.0 : Convert.ToDouble(reader.GetValue(ordinal));

    /// <summary>The collector's BAD_ACTOR facts over the window, as rows ordered by hash.</summary>
    private static async Task<List<Row>> CollectorRowsAsync(NpgsqlDataSource source)
    {
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = Start,
            TimeRangeEnd = End,
            ServerUtcOffset = TimeSpan.Zero,
        };

        var facts = await new PgFactCollector(source).CollectFactsAsync(context);
        return facts
            .Where(f => f.Source == "bad_actor")
            .Select(f => new Row(
                f.DatabaseName ?? "",
                f.Key["BAD_ACTOR_".Length..],
                (long)f.Metadata["execution_count"],
                f.Metadata["avg_cpu_ms"],
                f.Metadata["avg_elapsed_ms"],
                f.Metadata["avg_reads"],
                (long)f.Metadata["total_cpu_us"],
                (long)f.Metadata["total_reads"],
                (long)f.Metadata["total_spills"],
                (int)f.Metadata["max_dop"]))
            .OrderBy(r => r.Hash, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>Mints a scratch TimescaleDB store with the continuous aggregates created and nothing materialized, and runs the body on it.</summary>
    private static async Task WithStoreAsync(Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9879 bad actor split tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #9879 bad actor split tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            await body(scratch, connection, ct);
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

/// <summary>
/// #9879: <see cref="PgFactCollector.ChooseBadActorRead"/> takes the raw read over the whole window unless the hourly
/// rollup reaches the first hour line and has at least one whole hour to serve before the cut.
/// </summary>
public sealed class BadActorReadChoiceTests
{
    private static readonly DateTime Start = new(2026, 3, 2, 6, 20, 0, DateTimeKind.Unspecified);
    private static readonly DateTime End = new(2026, 3, 9, 6, 20, 0, DateTimeKind.Unspecified);
    private static readonly DateTime FirstHour = new(2026, 3, 2, 7, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime EndMidnight = new(2026, 3, 9, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Floor = new(2026, 2, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Ceiling = new(2026, 3, 9, 7, 0, 0, DateTimeKind.Unspecified);

    private static void AssertRaw(PgFactCollector.BadActorRead read)
    {
        Assert.Equal(PgFactCollector.BadActorSql, read.Sql);
        Assert.False(read.UsesRollup);
        Assert.Null(read.FirstHour);
        Assert.Null(read.Cut);
    }

    [Fact]
    public void AnUnknownFloor_ReadsRaw() =>
        AssertRaw(PgFactCollector.ChooseBadActorRead(Start, End, null, Ceiling));

    [Fact]
    public void AnUnknownCeiling_ReadsRaw() =>
        AssertRaw(PgFactCollector.ChooseBadActorRead(Start, End, Floor, null));

    [Fact]
    public void AFloorPastTheFirstHourLine_ReadsRaw_AndAFloorOnItSplits()
    {
        AssertRaw(PgFactCollector.ChooseBadActorRead(Start, End, FirstHour.AddHours(1), Ceiling));
        AssertRaw(PgFactCollector.ChooseBadActorRead(Start, End, FirstHour.AddTicks(1), Ceiling));

        var onTheLine = PgFactCollector.ChooseBadActorRead(Start, End, FirstHour, Ceiling);
        Assert.True(onTheLine.UsesRollup);
        Assert.Equal(PgFactCollector.BadActorHourlySplitSql, onTheLine.Sql);
    }

    [Fact]
    public void AnEmptyRollupArm_ReadsRaw()
    {
        /* The ceiling is on the first hour line, or inside the hour that starts there: no whole hour is materialized. */
        AssertRaw(PgFactCollector.ChooseBadActorRead(Start, End, Floor, FirstHour));
        AssertRaw(PgFactCollector.ChooseBadActorRead(Start, End, Floor, FirstHour.AddMinutes(59)));

        /* The window sits inside its last UTC day: the end's midnight is before the first hour line. */
        AssertRaw(PgFactCollector.ChooseBadActorRead(EndMidnight.AddMinutes(80), End, Floor, Ceiling));

        /* The first hour line is the end's midnight itself. */
        AssertRaw(PgFactCollector.ChooseBadActorRead(EndMidnight.AddMinutes(-20), End, Floor, Ceiling));
    }

    [Fact]
    public void TheSplit_StartsOnTheFirstHourLine_AndCutsAtTheLesserOfTheEndsMidnightAndTheCeilingsHour()
    {
        var covering = PgFactCollector.ChooseBadActorRead(Start, End, Floor, Ceiling);
        Assert.Equal(PgFactCollector.BadActorHourlySplitSql, covering.Sql);
        Assert.Equal(FirstHour, covering.FirstHour);
        Assert.Equal(EndMidnight, covering.Cut);

        /* A ceiling inside an hour is taken down to that hour's line. */
        var behind = PgFactCollector.ChooseBadActorRead(Start, End, Floor, new DateTime(2026, 3, 7, 11, 40, 0, DateTimeKind.Unspecified));
        Assert.Equal(FirstHour, behind.FirstHour);
        Assert.Equal(new DateTime(2026, 3, 7, 11, 0, 0, DateTimeKind.Unspecified), behind.Cut);

        /* A start on an hour line is its own first hour line. */
        var onTheHour = PgFactCollector.ChooseBadActorRead(FirstHour.AddHours(-1), End, Floor, Ceiling);
        Assert.Equal(FirstHour.AddHours(-1), onTheHour.FirstHour);

        /* One whole hour is enough. */
        var oneHour = PgFactCollector.ChooseBadActorRead(Start, End, FirstHour, FirstHour.AddHours(1));
        Assert.Equal(FirstHour, oneHour.FirstHour);
        Assert.Equal(FirstHour.AddHours(1), oneHour.Cut);
    }
}
