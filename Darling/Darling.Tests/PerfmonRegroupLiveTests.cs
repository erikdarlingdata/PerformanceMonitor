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
using static Darling.Tests.PerfmonRegroupLiveSupport;

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
        var gatedRun = await RunDetailedBoundedAsync(NewDrain(scratch.ConnectionString, gated, TimeSpan.Zero, new List<TimeSpan>()), 60, ct);
        Assert.Equal(0, gatedRun.Done);
        Assert.Equal(PerfmonRegroupRunEnd.Blocked, gatedRun.End);
        Assert.DoesNotContain(gated.Lines, line => line.Contains("re-grouped perfmon_stats chunk", StringComparison.Ordinal));

        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);
        var inReachNewestFirst = before.Where(chunk => chunk.Start > RunStartUtc.AddDays(-30)).OrderByDescending(chunk => chunk.Start).Select(chunk => chunk.Name).ToList();
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
        var secondRun = await RunDetailedBoundedAsync(NewDrain(scratch.ConnectionString, second, TimeSpan.Zero, new List<TimeSpan>()), 60, ct);
        Assert.Equal(0, secondRun.Done);
        Assert.Equal(PerfmonRegroupRunEnd.NothingLeft, secondRun.End);
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

        var today = DateTime.SpecifyKind(RunStartUtc.Date, DateTimeKind.Unspecified);
        /* Three to five days back, not two to four: perfmon_stats compresses after 1 day plus its heavy-table offset hours
           (TimescaleSupport.CompressAfterFor), so the chunk of two days back is not yet old enough in the first hours after
           midnight UTC, and the policy rightly leaves it alone. From three days back every chunk is due at any hour. */
        foreach (var daysBack in new[] { 3, 4, 5 })
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
        var run = await RunDetailedBoundedAsync(NewDrain(scratch.ConnectionString, logger, TimeSpan.Zero, new List<TimeSpan>()), 60, ct);
        Assert.Equal(0, run.Done);
        Assert.Equal(PerfmonRegroupRunEnd.NothingLeft, run.End);
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
        Assert.All(afterDrain.Where(chunk => chunk.Name != crashed.Name && chunk.Start > RunStartUtc.AddDays(-30)), chunk => Assert.Equal(WantedChunkSegmentBy, chunk.SegmentBy));

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
        Assert.Equal(TimescaleSupport.PerfmonRegroupChunkResult.Regrouped, await TimescaleSupport.CompressRegroupedChunkAsync(connection, logger, chunk.Name, 3, TimeSpan.Zero, ct));
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
            Assert.True(pauses[i].TotalMilliseconds >= tookMs[i] && pauses[i].TotalMilliseconds < tookMs[i] + 2000,
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
        Assert.All(after.Where(chunk => chunk.Name != held.Name && chunk.Start > RunStartUtc.AddDays(-30)), chunk => Assert.Equal(WantedChunkSegmentBy, chunk.SegmentBy));
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

        Assert.Equal(TimescaleSupport.PerfmonRegroupChunkResult.Regrouped, compressed);
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
        Assert.Equal(TimescaleSupport.PerfmonRegroupChunkResult.LeftUncompressed, await TimescaleSupport.CompressRegroupedChunkAsync(connection, logger, chunk.Name, 3, TimeSpan.Zero, ct));
        await holder.ReleaseAsync(ct);

        Assert.Equal(3, logger.Lines.Count(line => line.Contains("not changed this pass", StringComparison.Ordinal)));
        Assert.Single(logger.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal) && line.Contains("stays uncompressed", StringComparison.Ordinal));

        /* Every Warning, not one text (#5574): the three in-call tries are not "three passes in a row". */
        Assert.Equal(1, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
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

        var finished = await Task.WhenAny(drain.Completion!, Task.Delay(TimeSpan.FromSeconds(10), ct));
        Assert.Same(drain.Completion, finished);
        Assert.False(drain.Completion!.IsFaulted);
        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        var after = await ReadChunksAsync(connection, ct);
        Assert.Equal(1, after.Count(chunk => chunk.SegmentBy == WantedChunkSegmentBy));
        Assert.Equal(5, after.Count(chunk => chunk.SegmentBy == OldChunkSegmentBy));
    }

    /* ---------------- check round 1 (#5579) ---------------- */

    /// <summary>
    /// A busy lock pauses the drain as long as the work it cost, like every other outcome (#5574, M1): a lock lost at the END of
    /// a decompress throws the whole rewrite away, and the pause is that time, not the floor. Here the lock is lost at the start
    /// (the 3 s lock wait is the time spent), the floor is 1 ms, and the pause after the busy chunk must still be the wait; a
    /// busy chunk that cost at least the lost-work threshold also gets its own Information line saying so.
    /// </summary>
    [Fact]
    public async Task ABusyLock_PausesAsLongAsTheTimeItCost_AndALongOneSaysSo()
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
        var run = await RunDetailedBoundedAsync(
            NewDrain(scratch.ConnectionString, logger, TimeSpan.FromMilliseconds(1), pauses, lostWorkThreshold: TimeSpan.FromSeconds(1)), 60, ct);
        await holder.ReleaseAsync(ct);

        Assert.Equal(4, run.Done);
        Assert.True(pauses[0] >= TimeSpan.FromSeconds(2.5), $"the pause after the busy chunk was {pauses[0].TotalSeconds:0.0} s, not the {TimescaleSupport.HourlyDdlLockTimeout} it cost");
        Assert.Single(logger.Lines, line => line.Contains("was lost after", StringComparison.Ordinal) && line.Contains(held.Name, StringComparison.Ordinal));
        TimescaleSupport.BusyStreaks.Reset();
    }

    /// <summary>
    /// A decompress that runs into its statement cap is not retried while the process lives (#5574, M2): the newest chunk's lock is
    /// held, so its decompress waits and the 2 s cap (a test value for the 30 minutes) ends it before the 3 s lock timeout. The
    /// other four chunks are re-grouped, the capped one is untouched with ONE Warning, and a second run of the same drain does not
    /// try it again even though it is still a candidate.
    /// </summary>
    [Fact]
    public async Task ADecompressThatHitsTheStatementCap_IsNotTriedAgainForTheLifeOfTheProcess()
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
        var drain = NewDrain(scratch.ConnectionString, logger, TimeSpan.Zero, new List<TimeSpan>(), statementTimeoutSeconds: 2);
        var first = await RunDetailedBoundedAsync(drain, 60, ct);
        await holder.ReleaseAsync(ct);

        Assert.Equal(4, first.Done);
        Assert.Equal(1, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.Single(logger.Lines, line => line.Contains("could not be decompressed within 2 s", StringComparison.Ordinal) && line.Contains("hit the statement cap after", StringComparison.Ordinal) && line.Contains(held.Name, StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Lines, line => line.Contains("cancelled by the server", StringComparison.Ordinal));
        Assert.Equal(OldChunkSegmentBy, (await ReadChunksAsync(connection, ct)).Single(chunk => chunk.Name == held.Name).SegmentBy);

        var linesBefore = logger.Lines.Count;
        var second = await RunDetailedBoundedAsync(drain, 60, ct);
        Assert.Equal(0, second.Done);
        Assert.Equal(PerfmonRegroupRunEnd.AllSkipped, second.End);
        Assert.DoesNotContain(logger.Lines.Skip(linesBefore), line => line.Contains("(decompress)", StringComparison.Ordinal));
        Assert.Equal(OldChunkSegmentBy, (await ReadChunksAsync(connection, ct)).Single(chunk => chunk.Name == held.Name).SegmentBy);
        TimescaleSupport.BusyStreaks.Reset();
    }

    /// <summary>
    /// A decompress the SERVER cancels before the 30-minute cap (a short <c>statement_timeout</c> on the role, a cancel from
    /// another session) is still not tried again until the restart, so a short server timeout does not cost a rewrite every hour,
    /// but the log tells the truth about it (#5574, check round 2): "cancelled by the server" with the real elapsed time, not
    /// "could not be decompressed within 1800 s". The session's <c>statement_timeout</c> is 2 s here, under the 3 s lock wait the
    /// held chunk gives, so the server cancels first and the cap (the default 1800 s) is nowhere near.
    /// </summary>
    [Fact]
    public async Task ADecompressTheServerCancelsBeforeTheCap_IsNotTriedAgain_AndTheLogSaysTheServerDidIt()
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
        var stub = new TimescaleSupport.PerfmonRegroupRead(
            TimescaleSupport.PerfmonRegroupReadStatus.Candidates,
            new[] { new TimescaleSupport.PerfmonRegroupCandidate(held.Name, OldChunkSegmentBy) }, null);
        var drainConnection = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Options = "-c statement_timeout=2000" }.ConnectionString;
        await using var holder = await HoldLockAsync(scratch.ConnectionString, held.Name, ct);
        var logger = new CapturingTestLogger();
        var drain = NewDrain(drainConnection, logger, TimeSpan.Zero, new List<TimeSpan>(), readCandidates: FixedRead(stub));
        var first = await RunDetailedBoundedAsync(drain, 60, ct);
        await holder.ReleaseAsync(ct);

        Assert.Equal(0, first.Done);
        Assert.Single(logger.Lines, line => line.Contains("cancelled by the server", StringComparison.Ordinal)
            && line.Contains(held.Name, StringComparison.Ordinal) && line.Contains("before the 1800 s statement cap", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Lines, line => line.Contains("could not be decompressed within", StringComparison.Ordinal) || line.Contains("hit the statement cap", StringComparison.Ordinal));
        Assert.Equal(OldChunkSegmentBy, (await ReadChunksAsync(connection, ct)).Single(chunk => chunk.Name == held.Name).SegmentBy);

        /* Not tried again while the process lives, though the stub keeps answering with it. */
        var linesBefore = logger.Lines.Count;
        var second = await RunDetailedBoundedAsync(drain, 60, ct);
        Assert.Equal(0, second.Done);
        Assert.Equal(PerfmonRegroupRunEnd.AllSkipped, second.End);
        Assert.DoesNotContain(logger.Lines.Skip(linesBefore), line => line.Contains("(decompress)", StringComparison.Ordinal));
        TimescaleSupport.BusyStreaks.Reset();
    }

    /// <summary>
    /// The compress half retries ONLY a busy lock (#5574, M2): a statement that ran into its cap ends the tries at once, with the
    /// statement's own Warning and no second one, instead of three whole compressions.
    /// </summary>
    [Fact]
    public async Task TheCompressHalf_IsNotRetriedAfterAnythingButABusyLock()
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
        Assert.Equal(TimescaleSupport.PerfmonRegroupChunkResult.LeftUncompressed, await TimescaleSupport.CompressRegroupedChunkAsync(connection, logger, chunk.Name, 3, TimeSpan.Zero, ct, statementTimeoutSeconds: 1));
        await holder.ReleaseAsync(ct);

        Assert.Equal(1, logger.Lines.Count(line => line.Contains("(compress) could not be changed", StringComparison.Ordinal)));
        Assert.Equal(1, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.Equal(0, logger.Lines.Count(line => line.Contains("not changed this pass", StringComparison.Ordinal)));
        TimescaleSupport.BusyStreaks.Reset();
    }

    /// <summary>
    /// A chunk that stays a candidate after this process re-grouped it is not rewritten forever (#5574, L2): the candidates read is
    /// stubbed to keep answering with the chunk (as a TimescaleDB that spells the setting another way would), the drain rewrites it
    /// once, skips it when it comes back with ONE Warning naming the setting text, and says nothing more in a second run.
    /// </summary>
    [Fact]
    public async Task AChunkThatStaysACandidateAfterItsReGroup_IsSkipped_WithOneWarningPerProcess()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);

        var chunk = ChunkOfDay(await ReadChunksAsync(connection, ct), 2);
        var stub = new TimescaleSupport.PerfmonRegroupRead(
            TimescaleSupport.PerfmonRegroupReadStatus.Candidates,
            new[] { new TimescaleSupport.PerfmonRegroupCandidate(chunk.Name, "server_id, counter_name") }, null);
        var logger = new CapturingTestLogger();
        var drain = NewDrain(scratch.ConnectionString, logger, TimeSpan.Zero, new List<TimeSpan>(), readCandidates: FixedRead(stub));

        var first = await RunDetailedBoundedAsync(drain, 30, ct);
        Assert.Equal(1, first.Done);
        Assert.Equal(PerfmonRegroupRunEnd.AllSkipped, first.End);
        Assert.Equal(1, logger.Lines.Count(line => line.Contains("re-grouped perfmon_stats chunk", StringComparison.Ordinal)));
        Assert.Equal(1, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.Single(logger.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal) && line.Contains("'server_id, counter_name'", StringComparison.Ordinal));

        var second = await RunDetailedBoundedAsync(drain, 30, ct);
        Assert.Equal(0, second.Done);
        Assert.Equal(1, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.Equal(1, logger.Lines.Count(line => line.Contains("re-grouped perfmon_stats chunk", StringComparison.Ordinal)));
    }

    /// <summary>
    /// A compressed chunk with no per-chunk settings row counts as the old grouping (#5574, L3): the row is deleted from the
    /// catalog, and the candidates read still lists the chunk (an INNER JOIN to the view dropped it).
    /// </summary>
    [Fact]
    public async Task AChunkWithNoPerChunkSettingsRow_IsACandidate()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);

        var chunk = ChunkOfDay(await ReadChunksAsync(connection, ct), 2);
        await ExecAsync(connection, $"DELETE FROM _timescaledb_catalog.compression_settings WHERE relid = '{chunk.Name}'::regclass", ct);
        Assert.Equal("", (await ReadChunksAsync(connection, ct)).Single(c => c.Name == chunk.Name).SegmentBy);

        var read = await TimescaleSupport.ReadPerfmonRegroupCandidatesAsync(connection, null, 30, ct);
        Assert.Equal(TimescaleSupport.PerfmonRegroupReadStatus.Candidates, read.Status);
        Assert.Contains(read.Candidates, candidate => candidate.Chunk == chunk.Name && candidate.OldSegmentBy is null);
        Assert.Equal(5, read.Candidates.Count);
    }

    /// <summary>
    /// A chunk that retention dropped between the candidates read and the decompress is skipped, not failed (#5574, L4): no
    /// Warning, no count toward the failure limit (so the four others behind it, here three, are still tried).
    /// </summary>
    [Fact]
    public async Task AChunkThatIsGoneAtDecompressTime_IsSkippedQuietly_NotFailed()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);

        /* THREE gone chunks (the failure limit is three in a row): a gone chunk that counted as a failure would end the run at the
           third, and the run would end FailureLimit with a Warning instead of getting past all three (#5574, check round 2). */
        var gone = Enumerable.Range(1, 3)
            .Select(n => new TimescaleSupport.PerfmonRegroupCandidate($"_timescaledb_internal._hyper_99999_{n}_chunk", OldChunkSegmentBy))
            .ToArray();
        var stub = new TimescaleSupport.PerfmonRegroupRead(TimescaleSupport.PerfmonRegroupReadStatus.Candidates, gone, null);
        var logger = new CapturingTestLogger();
        var run = await RunDetailedBoundedAsync(NewDrain(scratch.ConnectionString, logger, TimeSpan.Zero, new List<TimeSpan>(), readCandidates: FixedRead(stub)), 30, ct);

        Assert.Equal(0, run.Done);
        Assert.Equal(PerfmonRegroupRunEnd.AllSkipped, run.End);
        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.DoesNotContain(logger.Lines, line => line.Contains("in a row could not be re-grouped", StringComparison.Ordinal));
        foreach (var candidate in gone)
        {
            Assert.Contains(logger.Lines, line => line.StartsWith("Debug:", StringComparison.Ordinal)
                && line.Contains(candidate.Chunk, StringComparison.Ordinal) && line.Contains("no longer exists", StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// A chunk that is gone by the compress half is Gone too, not "left for the policy" (#5574, check round 2): no Warning, no
    /// "stays uncompressed until the compression policy compresses it" line (there is nothing to compress). And a 42P01 on a chunk the
    /// catalog still lists is a Failed chunk with a Warning, never Gone.
    /// </summary>
    [Fact]
    public async Task ARelationErrorIsGoneOnlyWhenTheChunkCatalogAgrees_AtEitherHalf()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);
        TimescaleSupport.BusyStreaks.Reset();

        /* The compress half on a chunk that is not there. */
        var goneLogger = new CapturingTestLogger();
        var result = await TimescaleSupport.CompressRegroupedChunkAsync(
            connection, goneLogger, "_timescaledb_internal._hyper_99999_9_chunk", 3, TimeSpan.Zero, ct);
        Assert.Equal(TimescaleSupport.PerfmonRegroupChunkResult.Gone, result);
        Assert.Equal(0, goneLogger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.DoesNotContain(goneLogger.Lines, line => line.Contains("compression policy", StringComparison.Ordinal));
        Assert.Single(goneLogger.Lines, line => line.StartsWith("Debug:", StringComparison.Ordinal) && line.Contains("no longer exists (compress half;", StringComparison.Ordinal));

        /* The same relation error on a chunk the catalog still lists: Failed, with a Warning that says why. */
        var listed = ChunkOfDay(await ReadChunksAsync(connection, ct), 2);
        foreach (var half in new[] { "decompress", "compress" })
        {
            var listedLogger = new CapturingTestLogger();
            var listedResult = await TimescaleSupport.ResolveMissingRelationAsync(connection, listedLogger, listed.Name, half, ct);
            Assert.Equal(TimescaleSupport.PerfmonRegroupChunkResult.Failed, listedResult);
            Assert.Equal(1, listedLogger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            Assert.Single(listedLogger.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal)
                && line.Contains(half, StringComparison.Ordinal) && line.Contains("still lists the chunk", StringComparison.Ordinal));
        }

        var goneDirect = await TimescaleSupport.ResolveMissingRelationAsync(connection, null, "_timescaledb_internal._hyper_99999_9_chunk", "decompress", ct);
        Assert.Equal(TimescaleSupport.PerfmonRegroupChunkResult.Gone, goneDirect);
    }

    /// <summary>
    /// The reach is the shorter of 30 days and one day under the perfmon retention (#5574, L4): with a fleet retention of 4 days
    /// only the chunks of the last 3 days are re-grouped, the 4- and 5-day-old ones (about to be dropped) are left.
    /// </summary>
    [Fact]
    public async Task TheReach_IsClampedToTheRetentionLessADay()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);
        await ExecAsync(connection,
            "INSERT INTO config.config_collector_schedules (server_id, collector_name, retention_days) VALUES (NULL, 'perfmon_stats', 4)", ct);

        /* The expected chunks come from the DATABASE clock, read here (#5574, check round 2): the reach is "ends within 3 days of
           now()", and the day-3 chunk drops out of it at midnight UTC, so a count of 3 fixed when the class loaded fails a run that
           crosses midnight between the class start and this test. The chunks themselves are named by the seeded days. */
        var expected = new List<string>();
        using (var inReach = new NpgsqlCommand(
            @"SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks
              WHERE hypertable_schema = 'collect' AND hypertable_name = 'perfmon_stats' AND is_compressed
              AND range_end > now() - make_interval(days => 3)", connection))
        await using (var names = await inReach.ExecuteReaderAsync(ct))
        {
            while (await names.ReadAsync(ct))
            {
                expected.Add(names.GetString(0));
            }
        }

        Assert.InRange(expected.Count, 2, 3);
        var run = await RunDetailedBoundedAsync(NewDrain(scratch.ConnectionString, new CapturingTestLogger(), TimeSpan.Zero, new List<TimeSpan>()), 60, ct);

        Assert.Equal(expected.Count, run.Done);
        var after = await ReadChunksAsync(connection, ct);
        Assert.All(after.Where(chunk => expected.Contains(chunk.Name)), chunk => Assert.Equal(WantedChunkSegmentBy, chunk.SegmentBy));
        Assert.All(after.Where(chunk => !expected.Contains(chunk.Name) && chunk.IsCompressed), chunk => Assert.Equal(OldChunkSegmentBy, chunk.SegmentBy));
        Assert.Equal(OldChunkSegmentBy, ChunkOfDay(after, 4).SegmentBy);
        Assert.Equal(OldChunkSegmentBy, ChunkOfDay(after, 5).SegmentBy);
    }

    /// <summary>
    /// A catalog read that fails is not reported as "converged" (#5574, L5): the run ends as <c>ReadFailed</c>, with ONE Warning
    /// the first time in the process and Debug lines after; a closed gate ends it as <c>Blocked</c> with no Warning at all.
    /// </summary>
    [Fact]
    public async Task AFailedCandidatesRead_IsWarnedOncePerProcess_AndIsNotConvergence()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var failed = new TimescaleSupport.PerfmonRegroupRead(TimescaleSupport.PerfmonRegroupReadStatus.ReadFailed, Array.Empty<TimescaleSupport.PerfmonRegroupCandidate>(), "permission denied for view chunks");
        var logger = new CapturingTestLogger();
        var drain = NewDrain(scratch.ConnectionString, logger, TimeSpan.Zero, new List<TimeSpan>(), readCandidates: FixedRead(failed));
        Assert.Equal(PerfmonRegroupRunEnd.ReadFailed, (await RunDetailedBoundedAsync(drain, 30, ct)).End);
        Assert.Equal(PerfmonRegroupRunEnd.ReadFailed, (await RunDetailedBoundedAsync(drain, 30, ct)).End);
        Assert.Equal(1, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.Contains(logger.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal) && line.Contains("permission denied for view chunks", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Lines, line => line.Contains("has the old grouping to re-group", StringComparison.Ordinal));

        var blocked = new TimescaleSupport.PerfmonRegroupRead(TimescaleSupport.PerfmonRegroupReadStatus.Blocked, Array.Empty<TimescaleSupport.PerfmonRegroupCandidate>(), "closed");
        var blockedLogger = new CapturingTestLogger();
        var blockedDrain = NewDrain(scratch.ConnectionString, blockedLogger, TimeSpan.Zero, new List<TimeSpan>(), readCandidates: FixedRead(blocked));
        Assert.Equal(PerfmonRegroupRunEnd.Blocked, (await RunDetailedBoundedAsync(blockedDrain, 30, ct)).End);
        Assert.Equal(0, blockedLogger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.DoesNotContain(blockedLogger.Lines, line => line.Contains("has the old grouping to re-group", StringComparison.Ordinal));
    }

    /* ---------------- fixture ---------------- */

    private static PerfmonRegroupDrain NewDrain(
        string connectionString, CapturingTestLogger logger, TimeSpan minPause, List<TimeSpan> pauses,
        int statementTimeoutSeconds = TimescaleSupport.PerfmonRegroupStatementTimeoutSeconds,
        Func<NpgsqlConnection, Microsoft.Extensions.Logging.ILogger?, int, CancellationToken, Task<TimescaleSupport.PerfmonRegroupRead>>? readCandidates = null,
        TimeSpan? lostWorkThreshold = null) =>
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
            },
            statementTimeoutSeconds, readCandidates, lostWorkThreshold);

    /// <summary>A candidates read that always answers with <paramref name="read"/> (a catalog the product's query cannot be made
    /// to produce on a real store: a chunk that stays a candidate, a failed read, a chunk that is already gone).</summary>
    private static Func<NpgsqlConnection, Microsoft.Extensions.Logging.ILogger?, int, CancellationToken, Task<TimescaleSupport.PerfmonRegroupRead>> FixedRead(
        TimescaleSupport.PerfmonRegroupRead read) =>
        (connection, logger, reach, token) => Task.FromResult(read);

    /// <summary>One md5 over the row count and a hash sum per (server, counter): unchanged data gives the same value.</summary>
    private static Task<string> FingerprintAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ScalarAsync<string>(connection,
            @"SELECT md5(string_agg(concat_ws('|', server_id, counter_name, n, h), ',' ORDER BY server_id, counter_name))
              FROM (SELECT server_id, counter_name, count(*) AS n,
                           sum(hashtextextended(concat_ws('|', collection_id, collection_time, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type), 0)::numeric) AS h
                    FROM perfmon_stats GROUP BY server_id, counter_name) AS t", ct);

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

    /// <summary><see cref="RunBoundedAsync"/> with the reason the run ended.</summary>
    private static async Task<PerfmonRegroupRunResult> RunDetailedBoundedAsync(PerfmonRegroupDrain drain, int seconds, CancellationToken ct)
    {
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(TimeSpan.FromSeconds(seconds));
        try
        {
            return await drain.RunDetailedAsync(limit.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            Assert.Fail($"the drain had not finished {seconds} s into the run");
            return default;
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
