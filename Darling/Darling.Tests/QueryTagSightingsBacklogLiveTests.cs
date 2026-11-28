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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// The sightings move over a backlog (#9956) on a real store: a drain with more than two days of expired sightings caps each slice
/// at <see cref="QueryTagSlimSightings.MoveSliceRowCap"/> rows, a kept-up day stays one slice, the next pass's first probe starts at
/// the point the last slice moved through (its end, or the cutoff when the end lies past it) until two vacuums have removed the
/// dead entries below it, a sighting upsert waits only for the
/// rows of one capped slice, and the drain stops at its share of the pass's budget and the next pass finishes it.
/// </summary>
public sealed class QueryTagSightingsBacklogLiveTests
{
    private const int Server = 7352;
    private const int Cap = QueryTagSlimSightings.MoveSliceRowCap;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static int SightingsDays => DarlingRetention.QueryTagSightingsRetentionDays(DarlingRetention.WidestDimensionFactRetentionDays(null), null);

    private sealed record Env(NpgsqlConnection Store, NpgsqlDataSource Source, CancellationToken Ct);

    private static async Task WithStoreAsync(Func<Env, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the sightings backlog pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;

        var bodySucceeded = false;
        try
        {
            await using (var store = new NpgsqlConnection(connectionString))
            await using (var source = NpgsqlDataSource.Create(connectionString))
            {
                await store.OpenAsync(ct);
                await PgMigrations.MigrateAsync(store, ct);

                /* No autovacuum of the sightings table in these tests: the dead entries a move leaves stay until a test vacuums. */
                await ExecAsync(new Env(store, source, ct), "ALTER TABLE collect.query_tag_sightings SET (autovacuum_enabled = false)");
                await body(new Env(store, source, ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(Env env, string sql)
    {
        await using var command = new NpgsqlCommand(sql, env.Store) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(env.Ct);
    }

    private static async Task<long> CountAsync(Env env, string sql)
    {
        await using var command = new NpgsqlCommand(sql, env.Store);
        return Convert.ToInt64(await command.ExecuteScalarAsync(env.Ct), CultureInfo.InvariantCulture);
    }

    private static string Timestamp(DateTime utc) => "'" + utc.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "+00'::timestamptz";

    /* Rows of one block of time: key prefix || n, last_seen spread evenly from `from` over `span`, written in random order so the rows
       of the block lie all over the heap, as they do on a store. Same caller fields as SightAtAsync. */
    private static Task SeedAsync(Env env, string prefix, int rows, DateTime from, TimeSpan span) =>
        ExecAsync(env,
            "INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, database_name, application_name, login_name, host_name, first_seen, last_seen) "
            + $"SELECT {Server}, 'hash', '{prefix}' || g, 'Sales', 'App', 'svc', 'H1', t.at, t.at "
            + $"FROM generate_series(0, {rows - 1}) AS g "
            + $"CROSS JOIN LATERAL (SELECT {Timestamp(from)} + g * interval '{(span.TotalSeconds / rows).ToString("R", CultureInfo.InvariantCulture)} seconds' AS at) AS t "
            + "ORDER BY random()");

    private static Task SightAtAsync(Env env, string key, DateTime lastSeen) =>
        ExecAsync(env,
            "INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, database_name, application_name, login_name, host_name, first_seen, last_seen) "
            + $"VALUES ({Server}, 'hash', '{key}', 'Sales', 'App', 'svc', 'H1', {Timestamp(lastSeen)}, {Timestamp(lastSeen)})");

    private static Task<long> SlimRowsAsync(Env env) => CountAsync(env, "SELECT count(*) FROM collect.query_tag_sightings_slim");

    private static Task<long> ExpiredRowsAsync(Env env, DateTime cutoff) =>
        CountAsync(env, $"SELECT count(*) FROM collect.query_tag_sightings WHERE last_seen < {Timestamp(cutoff)}");

    /* The shared buffers a statement touched, hit or read, from its EXPLAIN (ANALYZE, BUFFERS) at the plan's root, which counts its children. */
    private static async Task<long> BuffersAsync(Env env, string sql, params object[] parameters)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, env.Store);
        foreach (var parameter in parameters)
        {
            explain.Parameters.AddWithValue(parameter);
        }

        using var plan = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(env.Ct))!);
        var root = plan.RootElement[0].GetProperty("Plan");
        return root.GetProperty("Shared Hit Blocks").GetInt64() + root.GetProperty("Shared Read Blocks").GetInt64();
    }

    [Fact]
    public async Task AKeptUpDayOfMoreRowsThanTheBacklogCap_MovesAsOneSlice()
    {
        await WithStoreAsync(async env =>
        {
            /* A day of a store whose purge has kept up: every expired row lies in the 24 hours before the cutoff. */
            var cutoff = DateTime.UtcNow.AddDays(-SightingsDays);
            await SeedAsync(env, "K", Cap + 5_000, cutoff.AddHours(-23), TimeSpan.FromHours(22));
            await SeedAsync(env, "LIVE", 100, DateTime.UtcNow.AddDays(-1), TimeSpan.FromHours(12));

            var (moved, end) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, null, env.Ct);
            Assert.Equal(Cap + 5_000, moved);
            Assert.NotNull(end);
            Assert.False(end.Backlog);

            var (next, nextEnd) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, end, env.Ct);
            Assert.Equal(0, next);
            Assert.Null(nextEnd);
            Assert.Equal(Cap + 5_000L, await SlimRowsAsync(env));
            Assert.Equal(100L, await CountAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));
        });
    }

    [Fact]
    public async Task ABacklogLargerThanTheCap_MovesInSlicesOfAtMostTheCap_AndTheNextPassStartsWhereTheLastSliceEnded()
    {
        await WithStoreAsync(async env =>
        {
            /* Five days back from the cutoff: 40,000 rows in day one, 20,000 in day three, 10,000 in day five. */
            var cutoff = DateTime.UtcNow.AddDays(-SightingsDays);
            var t0 = cutoff.AddHours(-120);
            await SeedAsync(env, "A", 40_000, t0, TimeSpan.FromHours(12));
            await SeedAsync(env, "B", 20_000, t0.AddHours(48), TimeSpan.FromHours(12));
            await SeedAsync(env, "C", 10_000, t0.AddHours(100), TimeSpan.FromHours(12));
            await SeedAsync(env, "LIVE", 100, DateTime.UtcNow.AddDays(-1), TimeSpan.FromHours(12));

            /* A store with no move behind it has no stored bound: the first probe starts from the bottom of the index. */
            Assert.Null(await DarlingRetention.ReadSightingsDrainStartAsync(env.Store, env.Ct));

            /* The first pass takes two slices and stops. The first slice of day one is the cap, the second the rest of that day. */
            var (first, firstEnd) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, null, env.Ct);
            Assert.Equal(Cap, first);
            Assert.True(firstEnd!.Backlog);
            var (second, secondEnd) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, firstEnd, env.Ct);
            Assert.Equal(40_000 - Cap, second);
            Assert.True(secondEnd!.Backlog);

            /* The next pass starts at the end of the second slice. */
            var start = await DarlingRetention.ReadSightingsDrainStartAsync(env.Store, env.Ct);
            Assert.NotNull(start);
            Assert.Equal(secondEnd.Bound.ToUniversalTime(), start.Bound.ToUniversalTime());

            var slices = new List<int>();
            var cursor = start;
            while (true)
            {
                var (rows, end) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, cursor, env.Ct);
                if (rows == 0)
                {
                    break;
                }

                slices.Add(rows);
                cursor = end;
            }

            /* This drain starts more than 48 hours below the cutoff too, so its slices are capped. Day three and day five are each
               under the cap, so each is one slice: a capped slice still ends at most a day after its start. */
            Assert.Equal(new[] { 20_000, 10_000 }, slices);
            Assert.All(slices, rows => Assert.True(rows <= Cap));
            Assert.Equal(70_000L, await SlimRowsAsync(env));
            Assert.Equal(0L, await ExpiredRowsAsync(env, cutoff));
            Assert.Equal(100L, await CountAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));
        });
    }

    /* One pass the way PurgeOneAsync drains the table: the first probe starts at the stored bound, or from the bottom of the index
       when there is none, and slices follow until one moves nothing. Returns the rows moved. */
    private static async Task<int> DrainAsync(Env env, DateTime cutoff)
    {
        var moved = 0;
        var cursor = await DarlingRetention.ReadSightingsDrainStartAsync(env.Store, env.Ct);
        while (true)
        {
            var (rows, end) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, cursor, env.Ct);
            if (rows == 0)
            {
                return moved;
            }

            moved += rows;
            cursor = end;
        }
    }

    [Fact]
    public async Task ASliceThatEndsPastTheCutoff_StoresTheCutoffAsItsBound_AndTheNextPassMovesTheRowsThatExpiredSince()
    {
        await WithStoreAsync(async env =>
        {
            /* Pass one runs at cutoff c1. Its only slice starts at the oldest expired row, 20 hours below c1, and ends a day later,
               4 hours past c1. The MID rows lie between c1 and that end: not expired at c1, so the slice leaves them. Pass two runs
               at c2, 3.5 hours past c1 and still below that end, when MID has expired. No vacuum runs between the passes, so the
               stored bound holds. */
            var c1 = DateTime.UtcNow.AddDays(-SightingsDays);
            var c2 = c1.AddHours(3.5);
            await SeedAsync(env, "OLD", 500, c1.AddHours(-20), TimeSpan.FromHours(12));
            await SeedAsync(env, "MID", 300, c1.AddHours(1), TimeSpan.FromHours(2));
            await SeedAsync(env, "LIVE", 100, c1.AddHours(30), TimeSpan.FromHours(12));

            Assert.Equal(500, await DrainAsync(env, c1));
            Assert.Equal(0L, await ExpiredRowsAsync(env, c1));
            Assert.Equal(300L, await CountAsync(env, "SELECT count(*) FROM collect.query_tag_sightings WHERE key LIKE 'MID%'"));

            /* The bound pass one stored is c1, not the slice's end: every expired row below c1 moved, and the MID rows above it did not. */
            var afterOne = await DarlingRetention.ReadSightingsDrainStartAsync(env.Store, env.Ct);
            Assert.NotNull(afterOne);
            Assert.Equal(c1, afterOne.Bound.ToUniversalTime(), TimeSpan.FromMilliseconds(1));

            Assert.Equal(300, await DrainAsync(env, c2));
            Assert.Equal(0L, await ExpiredRowsAsync(env, c2));
            Assert.Equal(800L, await CountAsync(env, "SELECT count(*) FROM collect.query_tag_sightings_slim"));
            Assert.Equal(100L, await CountAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));

            var afterTwo = await DarlingRetention.ReadSightingsDrainStartAsync(env.Store, env.Ct);
            Assert.NotNull(afterTwo);
            Assert.Equal(c2, afterTwo.Bound.ToUniversalTime(), TimeSpan.FromMilliseconds(1));
        });
    }

    [Fact]
    public async Task TheNextPassesFirstProbe_StartsAtTheStoredEnd_AndReadsAFewBuffers_WhereTheProbeFromTheBottomVisitsEveryDeadEntry()
    {
        await WithStoreAsync(async env =>
        {
            var cutoff = DateTime.UtcNow.AddDays(-SightingsDays);
            await SeedAsync(env, "D", 20_000, cutoff.AddHours(-20), TimeSpan.FromHours(12));
            await SeedAsync(env, "E", 1_000, cutoff.AddHours(-6), TimeSpan.FromHours(4));
            await ExecAsync(env, "VACUUM (ANALYZE) collect.query_tag_sightings");

            /* One slice moves day D and the 1,000 rows after it: the whole day before the cutoff. */
            var (moved, _) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, null, env.Ct);
            Assert.Equal(21_000, moved);
            await SeedAsync(env, "F", 10, cutoff.AddHours(30), TimeSpan.FromHours(1));

            var start = await DarlingRetention.ReadSightingsDrainStartAsync(env.Store, env.Ct);
            Assert.NotNull(start);

            /* The probe the next pass runs, from the stored end, reads a few index pages. The probe from the bottom of the index
               visits the heap for each of the 21,000 dead entries the move left (it runs second, because a walk marks the entries
               it finds dead). Both find nothing expired. */
            var bounded = await BuffersAsync(env, QueryTagSlimSightings.NextSliceProbeSql, cutoff, start.Bound, false);
            var fromTheBottom = await BuffersAsync(env, QueryTagSlimSightings.FirstSliceProbeSql, cutoff);
            Assert.True(bounded < 50, $"the bounded probe read {bounded} buffers");
            Assert.True(fromTheBottom > 5_000, $"the probe from the bottom read {fromTheBottom} buffers");

            /* One vacuum keeps the bound: it may have started before the move committed. The second one ends it. */
            await ExecAsync(env, "VACUUM collect.query_tag_sightings");
            Assert.NotNull(await DarlingRetention.ReadSightingsDrainStartAsync(env.Store, env.Ct));
            await ExecAsync(env, "VACUUM collect.query_tag_sightings");
            Assert.Null(await DarlingRetention.ReadSightingsDrainStartAsync(env.Store, env.Ct));
        });
    }

    [Fact]
    public async Task ASightingWrittenBelowTheStoredEnd_IsLeftByAProbeFromThatEnd_AndMovedOnceTwoVacuumsEndTheBound()
    {
        await WithStoreAsync(async env =>
        {
            var cutoff = DateTime.UtcNow.AddDays(-SightingsDays);
            await SeedAsync(env, "D", 500, cutoff.AddHours(-20), TimeSpan.FromHours(12));
            var (moved, firstEnd) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, null, env.Ct);
            Assert.Equal(500, moved);

            /* A sighting stamped below the slice's end arrives after the move: a restored row, say. The writer never stamps one. */
            await SightAtAsync(env, "0xBELOW", firstEnd!.Bound.AddHours(-30));

            var start = await DarlingRetention.ReadSightingsDrainStartAsync(env.Store, env.Ct);
            Assert.NotNull(start);
            var (fromTheEnd, _) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, start, env.Ct);
            Assert.Equal(0, fromTheEnd);
            Assert.Equal(1L, await ExpiredRowsAsync(env, cutoff));

            await ExecAsync(env, "VACUUM collect.query_tag_sightings");
            await ExecAsync(env, "VACUUM collect.query_tag_sightings");
            var afterVacuums = await DarlingRetention.ReadSightingsDrainStartAsync(env.Store, env.Ct);
            Assert.Null(afterVacuums);
            var (fromTheBottom, _) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, afterVacuums, env.Ct);
            Assert.Equal(1, fromTheBottom);
            Assert.Equal(0L, await ExpiredRowsAsync(env, cutoff));
            Assert.Equal(1L, await CountAsync(env, "SELECT count(*) FROM collect.query_tag_sightings_slim WHERE key = '0xBELOW'"));
        });
    }

    [Fact]
    public async Task AnUpsertOfAnExpiredKeyOfTheSameDay_FinishesWhileACappedSliceHoldsItsLocks_AndAKeyInsideTheSliceIsLocked()
    {
        await WithStoreAsync(async env =>
        {
            /* A backlog day of more rows than the cap. 0xIN lies at the start of the day, inside the first capped slice, and 0xOUT near
               its end, past the cap. A one-day slice would hold both. */
            var cutoff = DateTime.UtcNow.AddDays(-SightingsDays);
            var t0 = cutoff.AddHours(-100);
            await SeedAsync(env, "A", Cap + 5_000, t0, TimeSpan.FromHours(12));
            await SightAtAsync(env, "0xIN", t0.AddMinutes(1));
            await SightAtAsync(env, "0xOUT", t0.AddHours(11.9));

            DateTime start, end;
            await using (var probe = new NpgsqlCommand(QueryTagSlimSightings.FirstSliceProbeSql, env.Store))
            {
                probe.Parameters.AddWithValue(cutoff);
                await using var reader = await probe.ExecuteReaderAsync(env.Ct);
                Assert.True(await reader.ReadAsync(env.Ct));
                start = reader.GetDateTime(0);
                end = reader.GetDateTime(1);
                Assert.True(reader.GetBoolean(2));
            }

            Assert.True(end < t0.AddHours(11.9) && end > t0.AddMinutes(1), $"slice end {end:O}");

            /* The move of that slice, held open: its DELETE holds the row lock of every sighting it took. */
            await using var mover = await env.Source.OpenConnectionAsync(env.Ct);
            await using var held = await mover.BeginTransactionAsync(env.Ct);
            await using (var setting = new NpgsqlCommand(DarlingRetention.SliceDeleteScanSettingSql, mover, held))
            {
                await setting.ExecuteNonQueryAsync(env.Ct);
            }

            await using (var move = new NpgsqlCommand(QueryTagSlimSightings.MoveSliceSql, mover, held))
            {
                move.Parameters.AddWithValue(cutoff);
                move.Parameters.AddWithValue(start);
                move.Parameters.AddWithValue(end);
                Assert.Equal(Cap, await move.ExecuteNonQueryAsync(env.Ct));
            }

            /* A key inside the slice is locked: a write to it with a short lock timeout is refused. */
            await using (var other = await env.Source.OpenConnectionAsync(env.Ct))
            {
                await using var lockTimeout = new NpgsqlCommand("SET lock_timeout = '200ms'", other);
                await lockTimeout.ExecuteNonQueryAsync(env.Ct);
                await using var touch = new NpgsqlCommand("UPDATE collect.query_tag_sightings SET seen_count = seen_count + 1 WHERE key = '0xIN'", other);
                var refused = await Assert.ThrowsAsync<PostgresException>(() => touch.ExecuteNonQueryAsync(env.Ct));
                Assert.Equal("55P03", refused.SqlState);
            }

            /* The sighting writer's upsert of the expired key past the cap moves its last_seen inside its own timeout. */
            var written = await QueryTagSightingWriter.RecordAsync(
                env.Source, Server,
                new[] { new QueryTagSighting(QueryTagKeyKind.Hash, "0xOUT", Database: "Sales", Application: "App", Login: "svc", Host: "H1") },
                null, env.Ct);
            Assert.Equal(1, written);
            Assert.Equal(Cap + 5_000L + 1, await ExpiredRowsAsync(env, cutoff));

            await held.CommitAsync(env.Ct);
            Assert.Equal(1L, await CountAsync(env, "SELECT count(*) FROM collect.query_tag_sightings_slim WHERE key = '0xIN'"));
            Assert.Equal(1L, await CountAsync(env, $"SELECT count(*) FROM collect.query_tag_sightings WHERE key = '0xOUT' AND last_seen > {Timestamp(DateTime.UtcNow.AddDays(-1))}"));
        });
    }

    private sealed class NeverBehind : ICollectionPressure
    {
        public string? BehindReason() => null;
    }

    [Fact]
    public async Task ABacklogDrain_StopsAtItsShareOfThePassBudget_ThePassReachesTheTablesAfterIt_AndTheNextPassFinishesTheMove()
    {
        await WithStoreAsync(async env =>
        {
            var cutoff = DateTime.UtcNow.AddDays(-SightingsDays);
            var t0 = cutoff.AddHours(-120);
            await SeedAsync(env, "A", 40_000, t0, TimeSpan.FromHours(12));
            await SeedAsync(env, "B", 20_000, t0.AddHours(48), TimeSpan.FromHours(12));
            await SeedAsync(env, "C", 10_000, t0.AddHours(100), TimeSpan.FromHours(12));

            /* Fake time that follows the move: 300 s for every cap of rows in the slim table. The share is a third of a 1,800 s
               budget, 600 s: slices of 25,000, 15,000 and 20,000 rows reach it at 720 s, and the drain stops before a fourth. */
            await using var counter = await env.Source.OpenConnectionAsync(env.Ct);
            double WorkClock()
            {
                using var count = new NpgsqlCommand("SELECT count(*) FROM collect.query_tag_sightings_slim", counter);
                return (long)count.ExecuteScalar()! / (double)Cap * 300;
            }

            var pacerOne = new RetentionWalPacer(rateBytesPerSecond: 1_000_000_000, delay: (_, _) => Task.CompletedTask)
            {
                Yield = new RetentionCollectionYield(
                    new NeverBehind(), TimeSpan.FromSeconds(1_800), logger: null, delay: (_, _) => Task.CompletedTask, secondsClock: WorkClock),
            };
            var firstLog = new CapturingTestLogger();
            var first = await DarlingRetention.PurgeWithPacerAsync(
                env.Source, timescaleAvailable: false, firstLog, env.Ct, retentionDaysFor: null, planContentRetentionDays: 0, walPacer: pacerOne);

            Assert.Equal(60_000L, await SlimRowsAsync(env));
            Assert.Equal(10_000L, await ExpiredRowsAsync(env, cutoff));
            Assert.True(first.StoppedOnBudget, firstLog.Joined);
            Assert.False(pacerOne.Yield.StoppedOnBudget, firstLog.Joined);
            Assert.Equal(1, pacerOne.Yield.TablesStoppedOnShare);
            Assert.Equal("collect.query_tag_sightings", pacerOne.Yield.FirstShareStoppedTable);
            Assert.Equal(0, pacerOne.Yield.TablesNotReached);
            Assert.DoesNotContain(firstLog.Lines, line => line.Contains("Retention purge stopped at its", StringComparison.Ordinal));
            Assert.Contains(firstLog.Lines, line => line.Contains("Retention purge moved 60000 row(s) of collect.query_tag_sightings in 3 slice(s) of at most", StringComparison.Ordinal)
                && line.Contains("starting at the oldest expired row; it stopped at its 10-minute share of the pass's time budget", StringComparison.Ordinal));

            var pacerTwo = new RetentionWalPacer(rateBytesPerSecond: 1_000_000_000, delay: (_, _) => Task.CompletedTask)
            {
                Yield = new RetentionCollectionYield(new NeverBehind(), TimeSpan.FromHours(1), logger: null, delay: (_, _) => Task.CompletedTask),
            };
            var secondLog = new CapturingTestLogger();
            var second = await DarlingRetention.PurgeWithPacerAsync(
                env.Source, timescaleAvailable: false, secondLog, env.Ct, retentionDaysFor: null, planContentRetentionDays: 0, walPacer: pacerTwo);

            Assert.False(second.StoppedOnBudget, secondLog.Joined);
            Assert.Equal(70_000L, await SlimRowsAsync(env));
            Assert.Equal(0L, await ExpiredRowsAsync(env, cutoff));
            Assert.Contains(secondLog.Lines, line => line.Contains("Retention purge moved 10000 row(s) of collect.query_tag_sightings in 1 slice(s) of one day, starting at the end of the last slice moved; no expired row is left", StringComparison.Ordinal));
        });
    }
}
