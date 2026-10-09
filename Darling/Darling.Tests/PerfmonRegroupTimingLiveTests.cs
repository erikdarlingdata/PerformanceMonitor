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
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.PerfmonRegroupLiveSupport;

namespace Darling.Tests;

/// <summary>
/// #5574: the two perfmon_stats re-group tests that start a clock and judge a time against a limit. They are split out of
/// <see cref="PerfmonRegroupLiveTests"/> into the <c>timing</c> collection, so the timed calls run on a quiet process
/// instead of sharing CPU with the parallel classes. Each test mints its own scratch database, like the class they came
/// from, and every assertion and bar is unchanged.
/// </summary>
/* #1776 own-store: both facts mint their own scratch database through ScratchPostgres and never touch another
   test's rows, so this class is deliberately NOT [Collection("live-postgres")]. */
[Collection("timing")]
public sealed class PerfmonRegroupTimingLiveTests
{

    /// <summary>
    /// A reader beside the re-group of a chunk of about 1.5 M rows never waits more than a few seconds (#5574; the bound is 2.5 s, against 0.1 to 0.2 s measured with two transactions and 4.9 s of a 10.3 s re-group with one): the
    /// decompress holds only a lock that lets reads go on, and the ACCESS EXCLUSIVE it takes at its end is released by
    /// its own commit, before the compress half starts. The same re-group in ONE transaction made the reader wait for
    /// the whole compress half.
    /// </summary>
    [Fact]
    public async Task AReaderBesideTheReGroupOfALargeChunk_NeverWaitsMoreThanAFewSeconds()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available on this cluster.");
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await RequirePerChunkSettingsAsync(connection, ct);
        await ExecAsync(connection, TimescaleSupport.CreateHypertableSql(TimescaleSupport.PerfmonStatsTable, "collection_time"), ct);
        await ExecAsync(connection, "ALTER TABLE perfmon_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

        /* 4 servers x 130 counters x 2 instances = 1040 series, a sample a minute for a day: 1,497,600 rows. */
        var day = DateTime.SpecifyKind(RunStartUtc.Date.AddDays(-3), DateTimeKind.Unspecified);
        using (var seed = new NpgsqlCommand(
            @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
              SELECT $1 - row_number() OVER (), g.t AT TIME ZONE 'UTC', s.id, 'REGROUP-SRV', 'SQLServer:Test', 'Counter ' || c.n, 'inst' || i.i,
                     (extract(epoch FROM g.t)::bigint / 60 + s.id + c.n * 7 + i.i) % 100000, (extract(epoch FROM g.t)::bigint / 60 + c.n) % 50, 60, 65792
              FROM generate_series($2::timestamp, $2::timestamp + INTERVAL '23 hours 59 minutes', INTERVAL '1 minute') AS g(t)
              CROSS JOIN generate_series(1, 4) AS s(id)
              CROSS JOIN generate_series(1, 130) AS c(n)
              CROSS JOIN generate_series(0, 1) AS i(i)", connection) { CommandTimeout = 300 })
        {
            seed.Parameters.AddWithValue(IdBase);
            seed.Parameters.AddWithValue(day);
            await seed.ExecuteNonQueryAsync(ct);
        }

        var chunkName = await ScalarAsync<string>(connection,
            "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks WHERE hypertable_name = 'perfmon_stats' AND NOT is_compressed AND range_end <= now()", ct);
        await ExecAsync(connection, $"SELECT compress_chunk('{chunkName}')", ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);

        /* The bound is on what the re-group ADDS to a read (#5574): the same read is timed without a re-group first, in this test,
           so a slow runner moves both numbers and only a lock wait moves the difference. */
        static async Task<TimeSpan> TimeReadAsync(NpgsqlConnection readConnection, CancellationToken token)
        {
            var clock = System.Diagnostics.Stopwatch.StartNew();
            using var read = new NpgsqlCommand(
                "SELECT count(*), max(cntr_value) FROM v_perfmon_stats WHERE server_id = 2 AND counter_name = 'Counter 7' AND collection_time >= now() - INTERVAL '30 days'", readConnection)
            { CommandTimeout = 120 };
            await read.ExecuteScalarAsync(token);
            clock.Stop();
            return clock.Elapsed;
        }

        /* The baseline is the MEDIAN of the warm reads (#5574, check round 2): the first read after the compress is cold (plan,
           buffers) and is dropped, and one slow outlier must not lift the baseline, because the stall this test guards is a
           lock wait of about 3 s on top of a warm read, and a baseline raised by 0.5 s lets it through. */
        var warm = new List<TimeSpan>();
        using (var baselineConnection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await baselineConnection.OpenAsync(ct);
            await TimeReadAsync(baselineConnection, ct);
            for (var i = 0; i < 5; i++)
            {
                warm.Add(await TimeReadAsync(baselineConnection, ct));
            }
        }

        var baseline = warm.OrderBy(took => took).ElementAt(warm.Count / 2);

        var stop = false;
        var slowest = TimeSpan.Zero;
        var reads = 0;
        var reader = Task.Run(async () =>
        {
            using var readConnection = new NpgsqlConnection(scratch.ConnectionString);
            await readConnection.OpenAsync(ct);
            while (!Volatile.Read(ref stop))
            {
                var took = await TimeReadAsync(readConnection, ct);
                if (took > slowest)
                {
                    slowest = took;
                }

                reads++;
                await Task.Delay(100, ct);
            }
        }, ct);

        var logger = new CapturingTestLogger();
        var outcome = await TimescaleSupport.RegroupPerfmonChunkAsync(
            connection, logger, new TimescaleSupport.PerfmonRegroupCandidate(chunkName, OldChunkSegmentBy), ct);
        Volatile.Write(ref stop, true);
        await reader;

        Assert.Equal(TimescaleSupport.PerfmonRegroupChunkResult.Regrouped, outcome.Result);
        Assert.Equal(1_497_600, outcome.Rows);
        Assert.True(reads >= 3, $"the reader only got {reads} reads in during a {outcome.Elapsed.TotalSeconds:0.0} s re-group");
        Assert.True(slowest - baseline < TimeSpan.FromSeconds(2.5),
            $"a read took {slowest.TotalSeconds:0.0} s during a {outcome.Elapsed.TotalSeconds:0.0} s re-group, against {baseline.TotalSeconds:0.0} s without one");
    }

    /// <summary>
    /// A service stop during the compress half (#5574): the wait for a held lock is cancelled at once (not after the 3 s lock
    /// timeout), the cancellation propagates, and the log says the chunk is left for the compression policy.
    /// </summary>
    [Fact]
    public async Task AServiceStopDuringTheCompressHalf_CancelsTheWait_AndSaysTheChunkIsLeftToThePolicy()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);
        TimescaleSupport.BusyStreaks.Reset();

        var chunk = ChunkOfDay(await ReadChunksAsync(connection, ct), 3);
        await ExecAsync(connection, $"SELECT decompress_chunk('{chunk.Name}')", ct);
        await using var holder = await HoldLockAsync(scratch.ConnectionString, chunk.Name, ct);
        var logger = new CapturingTestLogger();
        var clock = System.Diagnostics.Stopwatch.StartNew();
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(ct);
        stopping.CancelAfter(TimeSpan.FromMilliseconds(500));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TimescaleSupport.CompressRegroupedChunkAsync(connection, logger, chunk.Name, 3, TimeSpan.FromSeconds(5), stopping.Token));
        await holder.ReleaseAsync(ct);

        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2.5), $"the stop took {clock.Elapsed.TotalSeconds:0.0} s to end the wait");
        Assert.Single(logger.Lines, line => line.Contains("the service is stopping with perfmon_stats chunk", StringComparison.Ordinal));
        TimescaleSupport.BusyStreaks.Reset();
    }
}
