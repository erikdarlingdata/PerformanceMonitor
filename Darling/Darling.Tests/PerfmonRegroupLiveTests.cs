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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5574: the perfmon_stats re-group loop against a real TimescaleDB store. Each test mints its own scratch database, like
/// <see cref="CollectionLogSegmentByLiveTests"/>, which this class mirrors. The old store is built with the statements
/// every build before #5574 ran (create_hypertable, then the enable ALTER with <c>server_id</c> alone), with its chunks
/// compressed under those settings. The expected groupings are literals on purpose: they pin the value the store ends up
/// with, so they must not move when the product's constant does.
/// </summary>
[Collection("live-postgres")]
public sealed class PerfmonRegroupLiveTests
{
    private const string WantedChunkSegmentBy = "server_id,counter_name";
    private const string OldChunkSegmentBy = "server_id";
    private const long IdBase = -495_200_000_000L;

    private sealed record ChunkRow(string Name, bool IsCompressed, string SegmentBy, DateTime Start);

    /// <summary>
    /// The upgraded store converges: with the hypertable already on the new grouping, the drain re-groups the chunks inside
    /// the 30-day reach newest first, leaves the chunk outside it alone, changes no row (count and a checksum per counter),
    /// pauses after every chunk but the last, and a second run is a no-op.
    /// </summary>
    [Fact]
    public async Task AnUpgradedStore_IsReGroupedNewestFirst_KeepsEveryRow_LeavesOutOfReachAlone_AndASecondRunIsANoOp()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);

        var before = await ReadChunksAsync(connection, ct);
        Assert.Equal(6, before.Count);
        Assert.All(before, chunk => Assert.Equal(OldChunkSegmentBy, chunk.SegmentBy));
        var fingerprint = await FingerprintAsync(connection, ct);

        /* The hypertable still has the old grouping: the gate keeps the drain from writing the old grouping again. */
        var gated = new CapturingTestLogger();
        Assert.Equal(0, await RunBoundedAsync(NewDrain(scratch.ConnectionString, gated, TimeSpan.Zero, new List<TimeSpan>()), 60, ct));
        Assert.DoesNotContain(gated.Lines, line => line.Contains("re-grouped perfmon_stats chunk", StringComparison.Ordinal));

        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);
        var inReachNewestFirst = before.Where(chunk => chunk.Start > DateTime.UtcNow.AddDays(-30)).OrderByDescending(chunk => chunk.Start).Select(chunk => chunk.Name).ToList();
        Assert.Equal(5, inReachNewestFirst.Count);

        var logger = new CapturingTestLogger();
        var pauses = new List<TimeSpan>();
        Assert.Equal(5, await RunBoundedAsync(NewDrain(scratch.ConnectionString, logger, TimeSpan.FromSeconds(90), pauses), 120, ct));

        var after = await ReadChunksAsync(connection, ct);
        Assert.All(after.Where(chunk => inReachNewestFirst.Contains(chunk.Name)), chunk => Assert.Equal(WantedChunkSegmentBy, chunk.SegmentBy));
        Assert.Equal(OldChunkSegmentBy, after.Single(chunk => !inReachNewestFirst.Contains(chunk.Name)).SegmentBy);
        Assert.Equal(inReachNewestFirst, logger.Lines
            .Select(line => Regex.Match(line, @"re-grouped perfmon_stats chunk (\S+) "))
            .Where(match => match.Success).Select(match => match.Groups[1].Value).ToList());
        Assert.Equal(fingerprint, await FingerprintAsync(connection, ct));

        /* Four pauses for five chunks, none after the last, each the floor (a chunk this small takes far less). */
        Assert.Equal(4, pauses.Count);
        Assert.All(pauses, pause => Assert.Equal(TimeSpan.FromSeconds(90), pause));

        var second = new CapturingTestLogger();
        Assert.Equal(0, await RunBoundedAsync(NewDrain(scratch.ConnectionString, second, TimeSpan.Zero, new List<TimeSpan>()), 60, ct));
        Assert.DoesNotContain(second.Lines, line => line.StartsWith("Information:", StringComparison.Ordinal));
        Assert.Equal(after, await ReadChunksAsync(connection, ct));
    }

    /* ---------------- round 2b: the rest of the behaviours, each with its own store ---------------- */

    /// <summary>
    /// A fresh store gets the new grouping on its first compressed chunk (#5574): the usual order (migrations first, the
    /// extension after), converted and given its policy by the product's own two steps, then days of samples, then the
    /// policy's run. No chunk is ever compressed by <c>server_id</c> alone, so the drain has nothing to do.
    /// </summary>
    [Fact]
    public async Task AFreshStore_CompressesItsFirstChunksWithTheNewGrouping_AndTheDrainHasNothingToDo()
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

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
        Assert.Equal("server_id, counter_name", await HypertableSegmentByAsync(connection, ct));

        var today = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        foreach (var daysBack in new[] { 2, 3, 4 })
        {
            await SeedDayAsync(connection, today.AddDays(-daysBack), daysBack, ct);
        }

        var fingerprint = await FingerprintAsync(connection, ct);
        await RunPerfmonPolicyAsync(connection, ct);

        var chunks = await ReadChunksAsync(connection, ct);
        Assert.Equal(3, chunks.Count);
        Assert.All(chunks, chunk =>
        {
            Assert.True(chunk.IsCompressed, chunk.Name + " was not compressed by the policy");
            Assert.Equal(WantedChunkSegmentBy, chunk.SegmentBy);
        });
        Assert.Equal(fingerprint, await FingerprintAsync(connection, ct));

        var logger = new CapturingTestLogger();
        Assert.Equal(0, await RunBoundedAsync(NewDrain(scratch.ConnectionString, logger, TimeSpan.Zero, new List<TimeSpan>()), 60, ct));
        Assert.DoesNotContain(logger.Lines, line => line.StartsWith("Information:", StringComparison.Ordinal));
    }

    /// <summary>
    /// A crash between the two halves (the decompress committed, the compress never ran) leaves a chunk that is
    /// uncompressed, not broken (#5574). The drain does not see it (it is not compressed, so it is not a candidate) and
    /// goes on with the others; the compression policy then compresses it with the hypertable's NEW grouping, with every
    /// row intact. The settings change itself is the product's: the policy step run on the old store.
    /// </summary>
    [Fact]
    public async Task ACrashBetweenTheTwoHalves_LeavesAnUncompressedChunk_ThatThePolicyCompressesWithTheNewGrouping()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
        Assert.Equal("server_id, counter_name", await HypertableSegmentByAsync(connection, ct));
        var fingerprint = await FingerprintAsync(connection, ct);

        var crashed = ChunkOfDay(await ReadChunksAsync(connection, ct), 3);
        await ExecAsync(connection, $"SELECT decompress_chunk('{crashed.Name}')", ct);

        /* The drain goes on with the four others and leaves the crashed one alone. */
        var logger = new CapturingTestLogger();
        Assert.Equal(4, await RunBoundedAsync(NewDrain(scratch.ConnectionString, logger, TimeSpan.Zero, new List<TimeSpan>()), 120, ct));
        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        var afterDrain = await ReadChunksAsync(connection, ct);
        Assert.False(afterDrain.Single(chunk => chunk.Name == crashed.Name).IsCompressed);
        Assert.All(afterDrain.Where(chunk => chunk.Name != crashed.Name && chunk.Start > DateTime.UtcNow.AddDays(-30)), chunk => Assert.Equal(WantedChunkSegmentBy, chunk.SegmentBy));

        /* The policy's next run compresses it with the hypertable's grouping. */
        await RunPerfmonPolicyAsync(connection, ct);
        var recompressed = (await ReadChunksAsync(connection, ct)).Single(chunk => chunk.Name == crashed.Name);
        Assert.True(recompressed.IsCompressed);
        Assert.Equal(WantedChunkSegmentBy, recompressed.SegmentBy);
        Assert.Equal(fingerprint, await FingerprintAsync(connection, ct));
    }

    /// <summary>
    /// The compression policy and the drain compressing the same chunk must not fail the drain (#5574): the policy got there
    /// first, so the compress half finds the chunk already compressed (<c>if_not_compressed</c>) and reports done, with
    /// no warning.
    /// </summary>
    [Fact]
    public async Task WhenThePolicyCompressedTheChunkFirst_TheCompressHalfIsDone_NotAnError()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);

        var chunk = ChunkOfDay(await ReadChunksAsync(connection, ct), 3);
        await ExecAsync(connection, $"SELECT decompress_chunk('{chunk.Name}')", ct);
        await RunPerfmonPolicyAsync(connection, ct);
        Assert.True((await ReadChunksAsync(connection, ct)).Single(c => c.Name == chunk.Name).IsCompressed);

        var logger = new CapturingTestLogger();
        Assert.True(await TimescaleSupport.CompressRegroupedChunkAsync(connection, logger, chunk.Name, 3, TimeSpan.Zero, ct));
        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        var after = (await ReadChunksAsync(connection, ct)).Single(c => c.Name == chunk.Name);
        Assert.True(after.IsCompressed);
        Assert.Equal(WantedChunkSegmentBy, after.SegmentBy);
    }

    /// <summary>
    /// The pause after a chunk is the chunk's own time when that is longer than the floor (#5574): with no floor, the
    /// pause the drain asks for after each chunk equals the milliseconds the chunk's own log line says it took.
    /// </summary>
    [Fact]
    public async Task ThePauseAfterAChunk_IsAtLeastTheTimeThatChunkTook()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);

        var logger = new CapturingTestLogger();
        var pauses = new List<TimeSpan>();
        Assert.Equal(5, await RunBoundedAsync(NewDrain(scratch.ConnectionString, logger, TimeSpan.Zero, pauses), 120, ct));

        var tookMs = logger.Lines
            .Select(line => Regex.Match(line, @"re-grouped perfmon_stats chunk \S+ \(\d+ rows\) from .* in (\d+) ms"))
            .Where(match => match.Success).Select(match => long.Parse(match.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture)).ToList();
        Assert.Equal(5, tookMs.Count);
        Assert.Equal(4, pauses.Count);
        for (var i = 0; i < pauses.Count; i++)
        {
            Assert.True(pauses[i].TotalMilliseconds >= tookMs[i] && pauses[i].TotalMilliseconds < tookMs[i] + 250,
                $"pause {i} was {pauses[i].TotalMilliseconds} ms, but chunk {i} took {tookMs[i]} ms");
            Assert.True(pauses[i] > TimeSpan.Zero);
        }
    }

    /// <summary>
    /// A chunk whose lock is held is skipped for the rest of the run (#5574): the newest chunk is held by another session,
    /// the decompress gives up after the 3 s lock timeout, the other four are re-grouped, the held one is not tried a
    /// second time, and a pause (the floor) follows the busy one like any other.
    /// </summary>
    [Fact]
    public async Task ABusyChunk_IsSkippedForTheRun_AndTheOthersAreReGrouped()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);
        TimescaleSupport.BusyStreaks.Reset();

        var held = ChunkOfDay(await ReadChunksAsync(connection, ct), 1);
        await using var holder = await HoldLockAsync(scratch.ConnectionString, held.Name, ct);
        var logger = new CapturingTestLogger();
        var pauses = new List<TimeSpan>();
        Assert.Equal(4, await RunBoundedAsync(NewDrain(scratch.ConnectionString, logger, TimeSpan.FromSeconds(90), pauses), 60, ct));
        await holder.ReleaseAsync(ct);

        var after = await ReadChunksAsync(connection, ct);
        Assert.Equal(OldChunkSegmentBy, after.Single(chunk => chunk.Name == held.Name).SegmentBy);
        Assert.All(after.Where(chunk => chunk.Name != held.Name && chunk.Start > DateTime.UtcNow.AddDays(-30)), chunk => Assert.Equal(WantedChunkSegmentBy, chunk.SegmentBy));
        Assert.Single(logger.Lines, line => line.Contains("not changed this pass", StringComparison.Ordinal) && line.Contains(held.Name, StringComparison.Ordinal));
        Assert.Contains(logger.Lines, line => line.Contains("1 skipped", StringComparison.Ordinal));
        Assert.Equal(4, pauses.Count);
        Assert.All(pauses, pause => Assert.Equal(TimeSpan.FromSeconds(90), pause));
        TimescaleSupport.BusyStreaks.Reset();
    }

    /// <summary>
    /// Three chunks in a row that fail (an error that is not a busy lock) stop the run (#5574). The drain's connection is
    /// read-only here, so every decompress fails the same way; the run ends after the third, with one Warning saying so,
    /// and the other two chunks are never touched.
    /// </summary>
    [Fact]
    public async Task ThreeFailuresInARow_StopTheRun()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);
        var before = await ReadChunksAsync(connection, ct);

        var logger = new CapturingTestLogger();
        var drain = new PerfmonRegroupDrain(
            async token =>
            {
                var readOnly = new NpgsqlConnection(scratch.ConnectionString);
                await readOnly.OpenAsync(token);
                await ExecAsync(readOnly, "SET default_transaction_read_only = on", token);
                return readOnly;
            },
            logger, 30, TimeSpan.Zero, (pause, token) => Task.CompletedTask);

        Assert.Equal(0, await RunBoundedAsync(drain, 60, ct));
        Assert.Equal(3, logger.Lines.Count(line => line.Contains("(decompress) could not be changed", StringComparison.Ordinal)));
        Assert.Single(logger.Lines, line => line.Contains("3 perfmon_stats chunks in a row could not be re-grouped", StringComparison.Ordinal));
        Assert.Equal(before, await ReadChunksAsync(connection, ct));
    }

    /// <summary>
    /// The compress half is retried (#5574): the chunk is already decompressed when it starts, so a lock another session
    /// holds for a moment costs a try, not the chunk. The first try gives up after the lock timeout, the lock is then
    /// released, and the second try compresses the chunk with the new grouping.
    /// </summary>
    [Fact]
    public async Task TheCompressHalf_IsRetriedAfterABusyLock_AndSucceedsOnALaterTry()
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
        var compress = TimescaleSupport.CompressRegroupedChunkAsync(connection, logger, chunk.Name, 3, TimeSpan.FromMilliseconds(200), ct);
        await WaitForAsync(() => logger.Lines.Any(line => line.Contains("not changed this pass", StringComparison.Ordinal)), 15, ct);
        await holder.ReleaseAsync(ct);
        var compressed = await compress;

        Assert.True(compressed);
        Assert.Equal(1, logger.Lines.Count(line => line.Contains("not changed this pass", StringComparison.Ordinal)));
        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        var after = (await ReadChunksAsync(connection, ct)).Single(c => c.Name == chunk.Name);
        Assert.True(after.IsCompressed);
        Assert.Equal(WantedChunkSegmentBy, after.SegmentBy);
        TimescaleSupport.BusyStreaks.Reset();
    }

    /// <summary>
    /// When every try of the compress half is busy (#5574), the chunk is left uncompressed with ONE Warning, after exactly
    /// the attempts it is given, and the compression policy compresses it with the new grouping once the lock is gone.
    /// </summary>
    [Fact]
    public async Task WhenEveryCompressTryIsBusy_TheChunkIsLeftToThePolicy_WithOneWarning()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
        TimescaleSupport.BusyStreaks.Reset();

        var chunk = ChunkOfDay(await ReadChunksAsync(connection, ct), 3);
        await ExecAsync(connection, $"SELECT decompress_chunk('{chunk.Name}')", ct);
        await using var holder = await HoldLockAsync(scratch.ConnectionString, chunk.Name, ct);
        var logger = new CapturingTestLogger();
        Assert.False(await TimescaleSupport.CompressRegroupedChunkAsync(connection, logger, chunk.Name, 3, TimeSpan.Zero, ct));
        await holder.ReleaseAsync(ct);

        Assert.Equal(3, logger.Lines.Count(line => line.Contains("not changed this pass", StringComparison.Ordinal)));
        Assert.Single(logger.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal) && line.Contains("stays uncompressed", StringComparison.Ordinal));
        Assert.False((await ReadChunksAsync(connection, ct)).Single(c => c.Name == chunk.Name).IsCompressed);

        await RunPerfmonPolicyAsync(connection, ct);
        var after = (await ReadChunksAsync(connection, ct)).Single(c => c.Name == chunk.Name);
        Assert.True(after.IsCompressed);
        Assert.Equal(WantedChunkSegmentBy, after.SegmentBy);
        TimescaleSupport.BusyStreaks.Reset();
    }

    /// <summary>
    /// The plan shape the whole change is for (#5574): a one-counter read over a chunk filters the COMPRESSED chunk on the
    /// segment columns, <c>server_id</c> and <c>counter_name</c>, once it is re-grouped. Over the same chunk before the
    /// re-group only <c>server_id</c> is a segment column, so the counter is tested after the batches are decompressed.
    /// The filters are the trend read's own (equality on both, a time window).
    /// </summary>
    [Fact]
    public async Task AOneCounterRead_FiltersTheCompressedChunkOnBothSegmentColumns_OnceItIsReGrouped()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        var day = ChunkOfDay(await ReadChunksAsync(connection, ct), 3);

        var before = await CompressedScanFilterAsync(connection, day.Start, ct);
        Assert.Contains("server_id", before, StringComparison.Ordinal);
        Assert.DoesNotContain("counter_name", before, StringComparison.Ordinal);

        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);
        Assert.Equal(5, await RunBoundedAsync(NewDrain(scratch.ConnectionString, new CapturingTestLogger(), TimeSpan.Zero, new List<TimeSpan>()), 120, ct));

        var after = await CompressedScanFilterAsync(connection, day.Start, ct);
        Assert.Contains("server_id", after, StringComparison.Ordinal);
        Assert.Contains("counter_name", after, StringComparison.Ordinal);
    }

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
        var day = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-3), DateTimeKind.Unspecified);
        using (var seed = new NpgsqlCommand(
            @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
              SELECT $1 - row_number() OVER (), g.t, s.id, 'REGROUP-SRV', 'SQLServer:Test', 'Counter ' || c.n, 'inst' || i.i,
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

        var stop = false;
        var slowest = TimeSpan.Zero;
        var reads = 0;
        var reader = Task.Run(async () =>
        {
            using var readConnection = new NpgsqlConnection(scratch.ConnectionString);
            await readConnection.OpenAsync(ct);
            while (!Volatile.Read(ref stop))
            {
                var clock = System.Diagnostics.Stopwatch.StartNew();
                using var read = new NpgsqlCommand(
                    "SELECT count(*), max(cntr_value) FROM v_perfmon_stats WHERE server_id = 2 AND counter_name = 'Counter 7' AND collection_time >= now() - INTERVAL '30 days'", readConnection)
                { CommandTimeout = 120 };
                await read.ExecuteScalarAsync(ct);
                clock.Stop();
                if (clock.Elapsed > slowest)
                {
                    slowest = clock.Elapsed;
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
        Assert.True(slowest < TimeSpan.FromSeconds(2.5), $"a read waited {slowest.TotalSeconds:0.0} s during a {outcome.Elapsed.TotalSeconds:0.0} s re-group");
    }

    /// <summary>
    /// A service stop during the drain's pause ends the run at once (#5574): the pause is waited on with the stopping token,
    /// so the run's task completes within a moment, without a warning, with the chunks it had not reached untouched.
    /// </summary>
    [Fact]
    public async Task AServiceStopDuringThePause_EndsTheRunPromptly_WithoutAWarning()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);

        var logger = new CapturingTestLogger();
        var pauseRequested = 0;
        var drain = new PerfmonRegroupDrain(
            async token =>
            {
                var drainConnection = new NpgsqlConnection(scratch.ConnectionString);
                await drainConnection.OpenAsync(token);
                return drainConnection;
            },
            logger, 30, TimeSpan.FromSeconds(90),
            async (pause, token) =>
            {
                Interlocked.Increment(ref pauseRequested);
                await Task.Delay(Timeout.Infinite, token);
            });

        using var stopping = new CancellationTokenSource();
        Assert.True(drain.StartIfIdle(stopping.Token));
        await WaitForAsync(() => Volatile.Read(ref pauseRequested) > 0, 60, ct);
        stopping.Cancel();

        var finished = await Task.WhenAny(drain.Completion!, Task.Delay(TimeSpan.FromSeconds(5), ct));
        Assert.Same(drain.Completion, finished);
        Assert.False(drain.Completion!.IsFaulted);
        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        var after = await ReadChunksAsync(connection, ct);
        Assert.Equal(1, after.Count(chunk => chunk.SegmentBy == WantedChunkSegmentBy));
        Assert.Equal(5, after.Count(chunk => chunk.SegmentBy == OldChunkSegmentBy));
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

    /* ---------------- fixture ---------------- */

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the perfmon_stats re-group tests (each mints its own scratch database).");
        return connectionString!;
    }

    private static PerfmonRegroupDrain NewDrain(string connectionString, CapturingTestLogger logger, TimeSpan minPause, List<TimeSpan> pauses) =>
        new(async token =>
            {
                var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(token);
                return connection;
            },
            logger, 30, minPause,
            (pause, token) =>
            {
                pauses.Add(pause);
                return Task.CompletedTask;
            });

    /// <summary>
    /// perfmon_stats as every build before #5574 left it: a hypertable compressed by <c>server_id</c> alone, five daily
    /// chunks inside the reach (yesterday back to five days ago) and one 45 days back, all compressed. Skips on a TimescaleDB
    /// without the per-chunk settings view (before 2.14.1), which these tests read.
    /// </summary>
    private static async Task BuildOldStoreAsync(NpgsqlConnection connection, string connectionString, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(connectionString, ct), "TimescaleDB is not available on this cluster.");
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        Assert.SkipUnless(
            await ScalarAsync<bool>(connection, "SELECT to_regclass('timescaledb_information.chunk_compression_settings') IS NOT NULL", ct),
            "this TimescaleDB has no timescaledb_information.chunk_compression_settings (before 2.14.1).");

        await ExecAsync(connection, TimescaleSupport.CreateHypertableSql(TimescaleSupport.PerfmonStatsTable, "collection_time"), ct);
        await ExecAsync(connection, "ALTER TABLE perfmon_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

        var today = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        foreach (var daysBack in new[] { 1, 2, 3, 4, 5, 45 })
        {
            await SeedDayAsync(connection, today.AddDays(-daysBack), daysBack, ct);
        }

        var chunks = new List<string>();
        using (var list = new NpgsqlCommand(
            "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks "
            + "WHERE hypertable_schema = 'collect' AND hypertable_name = 'perfmon_stats' AND NOT is_compressed AND range_end <= now()", connection))
        await using (var reader = await list.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        foreach (var chunk in chunks)
        {
            await ExecAsync(connection, $"SELECT compress_chunk('{chunk}')", ct);
        }
    }

    /// <summary>Two servers, four counters, two instances each, a sample every 30 minutes: 768 rows for the day.</summary>
    private static async Task SeedDayAsync(NpgsqlConnection connection, DateTime day, int salt, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
              SELECT $1 - $3 * 100000 - row_number() OVER (), g.t, s.id, 'REGROUP-SRV', 'SQLServer:Test', 'Counter ' || c.n,
                     CASE WHEN i.i = 0 THEN '' ELSE 'inst' || i.i END,
                     (extract(epoch FROM g.t)::bigint / 60 + s.id + c.n * 7 + i.i) % 100000, (extract(epoch FROM g.t)::bigint / 60 + c.n) % 50, 60, 65792
              FROM generate_series($2::timestamp, $2::timestamp + INTERVAL '23 hours 30 minutes', INTERVAL '30 minutes') AS g(t)
              CROSS JOIN generate_series(1, 2) AS s(id)
              CROSS JOIN generate_series(1, 4) AS c(n)
              CROSS JOIN generate_series(0, 1) AS i(i)", connection);
        command.Parameters.AddWithValue(IdBase);
        command.Parameters.AddWithValue(day);
        command.Parameters.AddWithValue(salt);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<List<ChunkRow>> ReadChunksAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"SELECT format('%I.%I', c.chunk_schema, c.chunk_name), c.is_compressed, coalesce(s.segmentby, ''), (c.range_start AT TIME ZONE 'UTC')
              FROM timescaledb_information.chunks AS c
              LEFT JOIN timescaledb_information.chunk_compression_settings AS s ON s.chunk = to_regclass(format('%I.%I', c.chunk_schema, c.chunk_name))
              WHERE c.hypertable_schema = 'collect' AND c.hypertable_name = 'perfmon_stats'
              ORDER BY c.range_start DESC", connection);
        var chunks = new List<ChunkRow>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            chunks.Add(new ChunkRow(reader.GetString(0), reader.GetBoolean(1), reader.GetString(2), DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc)));
        }

        return chunks;
    }

    /// <summary>One md5 over the row count and a hash sum per (server, counter): unchanged data gives the same value.</summary>
    private static Task<string> FingerprintAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ScalarAsync<string>(connection,
            @"SELECT md5(string_agg(concat_ws('|', server_id, counter_name, n, h), ',' ORDER BY server_id, counter_name))
              FROM (SELECT server_id, counter_name, count(*) AS n,
                           sum(hashtextextended(concat_ws('|', collection_id, collection_time, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type), 0)::numeric) AS h
                    FROM perfmon_stats GROUP BY server_id, counter_name) AS t", ct);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return (T)Convert.ChangeType(value!, typeof(T), System.Globalization.CultureInfo.InvariantCulture);
    }

    /* ---------------- round 2b helpers ---------------- */

    /// <summary>Runs the drain to the end, or fails the test when it has not ended in <paramref name="seconds"/> (a drain
    /// that loops on a chunk it should have skipped would otherwise hang the suite).</summary>
    private static async Task<int> RunBoundedAsync(PerfmonRegroupDrain drain, int seconds, CancellationToken ct)
    {
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(TimeSpan.FromSeconds(seconds));
        try
        {
            return await drain.RunAsync(limit.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            Assert.Fail($"the drain had not finished {seconds} s into the run");
            return -1;
        }
    }

    private static async Task WaitForAsync(Func<bool> condition, int seconds, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"the condition did not hold within {seconds} s");
            await Task.Delay(50, ct);
        }
    }

    private static ChunkRow ChunkOfDay(List<ChunkRow> chunks, int daysBack) =>
        chunks.Single(chunk => chunk.Start.Date == DateTime.UtcNow.Date.AddDays(-daysBack));

    /// <summary>Another session holds ROW EXCLUSIVE on the chunk inside an open transaction, which conflicts with the
    /// EXCLUSIVE lock both halves of the re-group ask for.</summary>
    private static async Task<HeldLock> HoldLockAsync(string connectionString, string chunk, CancellationToken ct)
    {
        var holder = new NpgsqlConnection(connectionString);
        await holder.OpenAsync(ct);
        await ExecAsync(holder, "BEGIN", ct);
        await ExecAsync(holder, $"LOCK TABLE {chunk} IN ROW EXCLUSIVE MODE", ct);
        return new HeldLock(holder);
    }

    /// <summary>The session that holds a chunk's lock; released by <see cref="ReleaseAsync"/> or, at the latest, when
    /// the test's scope ends.</summary>
    private sealed class HeldLock : IAsyncDisposable
    {
        private NpgsqlConnection? _connection;

        public HeldLock(NpgsqlConnection connection) => _connection = connection;

        public async Task ReleaseAsync(CancellationToken ct)
        {
            var connection = Interlocked.Exchange(ref _connection, null);
            if (connection is not null)
            {
                await ExecAsync(connection, "ROLLBACK", ct);
                await connection.DisposeAsync();
            }
        }

        public async ValueTask DisposeAsync()
        {
            var connection = Interlocked.Exchange(ref _connection, null);
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }
        }
    }

    private static async Task RequirePerChunkSettingsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        Assert.SkipUnless(
            await ScalarAsync<bool>(connection, "SELECT to_regclass('timescaledb_information.chunk_compression_settings') IS NOT NULL", ct),
            "this TimescaleDB has no timescaledb_information.chunk_compression_settings (before 2.14.1).");

    /// <summary>perfmon_stats's own segmentby columns in order, joined with ", " (how the product's gate reads them).</summary>
    private static Task<string> HypertableSegmentByAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ScalarAsync<string>(connection,
            @"SELECT string_agg(attname, ', ' ORDER BY segmentby_column_index) FROM timescaledb_information.compression_settings
              WHERE hypertable_schema = 'collect' AND hypertable_name = 'perfmon_stats' AND segmentby_column_index IS NOT NULL", ct);

    /// <summary>One run of perfmon_stats's compression policy in this session, as the scheduler runs it: every chunk older
    /// than the policy's compress-after is compressed with the settings the hypertable has at that moment.</summary>
    private static async Task RunPerfmonPolicyAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var jobId = await ScalarAsync<int>(connection,
            @"SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression'
              AND hypertable_schema = 'collect' AND hypertable_name = 'perfmon_stats'", ct);
        await ExecAsync(connection, $"CALL run_job({jobId})", ct);
    }

    /// <summary>
    /// The filter text of every scan of a COMPRESSED chunk's own relation (named <c>compress_hyper_*</c> or <c>_hyper_N_M_chunk_compressed</c>, by TimescaleDB release) in the plan of a
    /// one-counter read over the chunk starting at <paramref name="dayStart"/>: what the read can skip without
    /// decompressing. The read has the trend read's filters (equality on server and counter, a time window).
    /// </summary>
    private static bool IsCompressedRelationName(string? name) =>
        name is not null && (name.StartsWith("compress_hyper", StringComparison.Ordinal) || name.EndsWith("_compressed", StringComparison.Ordinal));

    private static async Task<string> CompressedScanFilterAsync(NpgsqlConnection connection, DateTime dayStart, CancellationToken ct)
    {
        var from = dayStart.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        var to = dayStart.AddDays(1).ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        var json = await ScalarAsync<string>(connection,
            "EXPLAIN (FORMAT JSON, COSTS OFF) SELECT collection_time, cntr_value FROM v_perfmon_stats "
            + $"WHERE server_id = 1 AND counter_name = 'Counter 2' AND collection_time >= '{from}' AND collection_time < '{to}'", ct);

        var filters = new List<string>();
        void Walk(System.Text.Json.JsonElement node)
        {
            if (node.ValueKind == System.Text.Json.JsonValueKind.Object)
            {
                if (node.TryGetProperty("Relation Name", out var relation)
                    && IsCompressedRelationName(relation.GetString()))
                {
                    foreach (var key in new[] { "Filter", "Index Cond", "Recheck Cond" })
                    {
                        if (node.TryGetProperty(key, out var text))
                        {
                            filters.Add(text.GetString() ?? string.Empty);
                        }
                    }
                }

                foreach (var property in node.EnumerateObject())
                {
                    Walk(property.Value);
                }
            }
            else if (node.ValueKind == System.Text.Json.JsonValueKind.Array)
            {
                foreach (var item in node.EnumerateArray())
                {
                    Walk(item);
                }
            }
        }

        using var document = System.Text.Json.JsonDocument.Parse(json);
        Walk(document.RootElement);
        Assert.True(filters.Count > 0, "no compressed chunk scan in the plan: " + json);
        return string.Join(" | ", filters);
    }
}
