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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The large start-path indexes share one build time budget per start (#9795): a start builds every missing one while its builds
/// fit the budget, and the ones that do not fit wait for the next start and the log names each. Also pins how a start-path build
/// that runs out of time is told apart from any other failure, and the text of the warning it gets. Own scratch database per live test.
/// </summary>
/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres. */
public sealed class TuningStartPathOneBuildTests
{
    private static readonly string[] Deferred =
    {
        PgTableTuning.LegacyRowIndexName,
        PgTableTuning.QueryStatsRestartRowIndexName,
        PgTableTuning.ProcedureStatsRestartRowIndexName,
    };

    /// <summary>The large indexes a store upgraded from v3.13.0 lacks at its first start (#9795), in statement order.</summary>
    private static readonly string[] Upgraded =
    {
        ProcedureStatsNameIndex.IndexName,
        PgTableTuning.CpuWindowCoverIndexName,
        PgTableTuning.QueryStoreServerHashIndexName,
    };

    private static readonly string[] UpgradedTables =
    {
        "collect.procedure_stats",
        "collect.cpu_utilization_stats",
        "collect.query_store_stats",
    };

    /// <summary>#9827: the line a start logs before it builds a missing index that shares the build time, for that index on that table.</summary>
    private static IReadOnlyList<string> BeforeBuildLines(CapturingTestLogger log, string index, string table) =>
        log.Lines.Where(l => l.StartsWith("Information:", StringComparison.Ordinal)
            && l.Contains("collection waits", StringComparison.Ordinal)
            && l.Contains(index, StringComparison.Ordinal)
            && l.Contains(table, StringComparison.Ordinal)).ToList();

    /// <summary>#9827: the line a start logs after it built a missing index that shares the build time, for that index on that table.</summary>
    private static IReadOnlyList<string> AfterBuildLines(CapturingTestLogger log, string index, string table) =>
        log.Lines.Where(l => l.StartsWith("Information:", StringComparison.Ordinal)
            && l.Contains(" was built ", StringComparison.Ordinal)
            && l.Contains(index, StringComparison.Ordinal)
            && l.Contains(table, StringComparison.Ordinal)).ToList();

    /// <summary>#9827: every build line of a pass, before or after, whatever the index.</summary>
    private static IReadOnlyList<string> AnyBuildLines(CapturingTestLogger log) =>
        log.Lines.Where(l => l.Contains("collection waits", StringComparison.Ordinal)
            || l.Contains(" was built ", StringComparison.Ordinal)).ToList();

    /// <summary>
    /// #9827: the pass logged one line before and one after the build of <paramref name="index"/> on <paramref name="table"/>, in that
    /// order, and the line before names the longest the build may take. Returns the line after, which names how long it took.
    /// </summary>
    private static string AssertBuiltWithBothLines(CapturingTestLogger log, string index, string table, int? upToSeconds)
    {
        var before = Assert.Single(BeforeBuildLines(log, index, table));
        var after = Assert.Single(AfterBuildLines(log, index, table));
        var limit = int.Parse(Regex.Match(before, @"up to (\d+) s", RegexOptions.CultureInvariant).Groups[1].Value, CultureInfo.InvariantCulture);
        if (upToSeconds is { } expected)
        {
            Assert.Equal(expected, limit);
        }
        else
        {
            /* On the system clock the builds before this one used a little of the 300 s, so the limit is 300 or just under it. */
            Assert.InRange(limit, 1, 300);
        }

        var lines = log.Lines.ToList();
        Assert.True(lines.IndexOf(before) < lines.IndexOf(after), log.Joined);
        return after;
    }

    /// <summary>
    /// #9795: a store with rows that lacks several large indexes at once, as one upgraded across the releases that added them does,
    /// has every one of them after ONE start when the builds fit the shared build time, and the hourly pass after it has nothing to warn about.
    /// </summary>
    [Fact]
    public async Task AStartBuildsEveryMissingLargeIndex_WhenTheBuildsFitTheSharedTime()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var body = store.Body;
        await store.DropAsync(ct, Upgraded);

        /* The hourly pass never builds any of them, however many are missing. */
        var firstHourly = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, firstHourly, hourly: true, ct);
        foreach (var name in Upgraded)
        {
            Assert.False(await IndexExistsAsync(body, name, ct), name);
        }

        Assert.Empty(AnyBuildLines(firstHourly));

        var start = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start, ct);
        foreach (var name in Upgraded)
        {
            Assert.True(await IndexExistsAsync(body, name, ct), name + " should be built by the one start: " + start.Joined);
        }

        Assert.DoesNotContain(start.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.Equal(0, start.CountAtLevel(LogLevel.Warning));

        /* #9827: the start says what it builds while collection waits, once before and once after each index, naming the index and the table. */
        for (var i = 0; i < Upgraded.Length; i++)
        {
            AssertBuiltWithBothLines(start, Upgraded[i], UpgradedTables[i], i == 0 ? 300 : null);
        }

        Assert.Equal(6, AnyBuildLines(start).Count);

        PgTableTuning.ResetHourlyMissingWarnings();
        var hourly = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, hourly, hourly: true, ct);
        Assert.DoesNotContain(hourly.Lines, l => l.Contains("is missing", StringComparison.Ordinal));
        Assert.Empty(AnyBuildLines(hourly));

        /* The next start: nothing is missing, nothing is built or deferred, and no index OID changes. */
        var oids = await OidsAsync(body, ct);
        var next = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, next, ct);
        Assert.Equal(oids, await OidsAsync(body, ct));
        Assert.DoesNotContain(next.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.Equal(0, next.CountAtLevel(LogLevel.Warning));

        /* #9827: nothing is missing, so the start says nothing about a build. */
        Assert.Empty(AnyBuildLines(next));
    }

    /// <summary>
    /// #9795: the shared build time is the bound. On a clock where every build takes 200 s of the 300 s, the first missing index is
    /// built, the second is built under the 100 s that are left, and the third waits: the log names it, the hourly pass does not
    /// build it, and the next start does.
    /// </summary>
    [Fact]
    public async Task AStartWhoseBuildsUseTheSharedTime_DefersTheRest_AndTheNextStartBuildsThem()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var body = store.Body;
        await store.DropAsync(ct, Upgraded);

        var start1 = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start1, hourly: false, new TuningStartPasses.BuildClock(secondsPerBuild: 200), 300, ct);
        Assert.True(await IndexExistsAsync(body, ProcedureStatsNameIndex.IndexName, ct), start1.Joined);
        Assert.True(await IndexExistsAsync(body, PgTableTuning.CpuWindowCoverIndexName, ct), start1.Joined);
        Assert.False(await IndexExistsAsync(body, PgTableTuning.QueryStoreServerHashIndexName, ct), start1.Joined);
        Assert.Single(start1.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal)
            && l.Contains(PgTableTuning.QueryStoreServerHashIndexName, StringComparison.Ordinal)
            && l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.Single(start1.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.Equal(0, start1.CountAtLevel(LogLevel.Warning));

        /* #9827: the first build may take its own 300 s, the second only the 100 s left of the shared time, and each says how long it took.
           The deferred third announces no build, because none starts. */
        Assert.Contains("200.0 s", AssertBuiltWithBothLines(start1, ProcedureStatsNameIndex.IndexName, UpgradedTables[0], 300), StringComparison.Ordinal);
        Assert.Contains("200.0 s", AssertBuiltWithBothLines(start1, PgTableTuning.CpuWindowCoverIndexName, UpgradedTables[1], 100), StringComparison.Ordinal);
        Assert.Empty(BeforeBuildLines(start1, PgTableTuning.QueryStoreServerHashIndexName, UpgradedTables[2]));
        Assert.Empty(AfterBuildLines(start1, PgTableTuning.QueryStoreServerHashIndexName, UpgradedTables[2]));
        Assert.Equal(4, AnyBuildLines(start1).Count);

        var hourly = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, hourly, hourly: true, ct);
        Assert.False(await IndexExistsAsync(body, PgTableTuning.QueryStoreServerHashIndexName, ct), hourly.Joined);
        Assert.Single(hourly.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal)
            && l.Contains(PgTableTuning.QueryStoreServerHashIndexName, StringComparison.Ordinal)
            && l.Contains("is missing", StringComparison.Ordinal));

        Assert.Empty(AnyBuildLines(hourly));

        var start2 = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start2, ct);
        Assert.True(await IndexExistsAsync(body, PgTableTuning.QueryStoreServerHashIndexName, ct), start2.Joined);
        Assert.DoesNotContain(start2.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.Equal(0, start2.CountAtLevel(LogLevel.Warning));

        /* #9827: the next start announces only the index it builds. */
        AssertBuiltWithBothLines(start2, PgTableTuning.QueryStoreServerHashIndexName, UpgradedTables[2], 300);
        Assert.Equal(2, AnyBuildLines(start2).Count);
    }

    /// <summary>
    /// #9795 with #9289: a build that runs under what earlier builds left of the shared time, and runs out of it, is rolled back with a
    /// warning and is NOT recorded as a timed-out build, so the next start tries it with its own limit and builds it. The timeout is
    /// forced without a sleep: the earlier build takes 299 s on the test clock, which leaves 1 s, while another session holds a lock
    /// the CREATE INDEX queues behind.
    /// </summary>
    [Fact]
    public async Task ABuildThatRunsOutOfWhatIsLeftOfTheSharedTime_IsNotRecorded_AndTheNextStartBuildsIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var body = store.Body;
        await store.DropAsync(ct, PgTableTuning.CpuWindowCoverIndexName, PgTableTuning.QueryStoreServerHashIndexName);

        await using (var holder = new NpgsqlConnection(store.ConnectionString))
        {
            await holder.OpenAsync(ct);
            await Exec(holder, "BEGIN; LOCK TABLE collect.query_store_stats IN SHARE UPDATE EXCLUSIVE MODE", ct);
            var cut = new CapturingTestLogger();
            await PgTableTuning.ApplyAsync(body, cut, hourly: false, new TuningStartPasses.BuildClock(secondsPerBuild: 299), 300, ct);
            await Exec(holder, "ROLLBACK", ct);

            Assert.True(await IndexExistsAsync(body, PgTableTuning.CpuWindowCoverIndexName, ct), cut.Joined);
            Assert.False(await IndexExistsAsync(body, PgTableTuning.QueryStoreServerHashIndexName, ct), cut.Joined);
            Assert.Single(cut.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal)
                && l.Contains(PgTableTuning.QueryStoreServerHashIndexName, StringComparison.Ordinal)
                && l.Contains("was not built at this start", StringComparison.Ordinal));

            /* #9827: the build that ran out of its 1 s says it started and no line says it was built, and the build before it says both. */
            Assert.Contains("299.0 s", AssertBuiltWithBothLines(cut, PgTableTuning.CpuWindowCoverIndexName, UpgradedTables[1], 300), StringComparison.Ordinal);
            Assert.Contains("up to 1 s", Assert.Single(BeforeBuildLines(cut, PgTableTuning.QueryStoreServerHashIndexName, UpgradedTables[2])), StringComparison.Ordinal);
            Assert.Empty(AfterBuildLines(cut, PgTableTuning.QueryStoreServerHashIndexName, UpgradedTables[2]));
        }

        Assert.Equal(0L, await BuildRecordsAsync(body, ct));

        var next = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, next, ct);
        Assert.True(await IndexExistsAsync(body, PgTableTuning.QueryStoreServerHashIndexName, ct), next.Joined);
        Assert.Equal(0, next.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0L, await BuildRecordsAsync(body, ct));
    }

    /// <summary>
    /// #9827 with #9289: the earlier builds of a start took 0.5 s of the shared 300 s, which is less than a whole second, so the
    /// bounded Query Store build still has its whole limit and is not shortened (the whole-seconds floor of 299.5 s is a rounding
    /// artifact). When it runs out of time its timeout is recorded with the bounded Warning, and the next start skips it. The timeout
    /// is forced without waiting for the limit: the session cancels the queued CREATE INDEX after 2 s while another session holds a lock.
    /// </summary>
    [Fact]
    public async Task ATimeoutOfTheBoundedBuild_AfterAnEarlierBuildOfUnderASecond_IsRecorded()
    {
        var ct = TestContext.Current.CancellationToken;
        var (store, log, records) = await TimeOutBoundedBuildAfterAnEarlierBuildAsync(earlierBuildSeconds: 0.5, ct);
        await using (store)
        {
            Assert.True(await IndexExistsAsync(store.Body, PgTableTuning.CpuWindowCoverIndexName, ct), log.Joined);
            Assert.False(await IndexExistsAsync(store.Body, PgTableTuning.QueryStoreServerHashIndexName, ct), log.Joined);
            Assert.Equal(1L, records);
            Assert.Single(log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal)
                && l.Contains(PgTableTuning.QueryStoreServerHashIndexName, StringComparison.Ordinal)
                && l.Contains("uncompressed heap pages", StringComparison.Ordinal));
            Assert.DoesNotContain(log.Lines, l => l.Contains("was not built at this start", StringComparison.Ordinal));

            var next = new CapturingTestLogger();
            await PgTableTuning.ApplyAsync(store.Body, next, ct);
            Assert.False(await IndexExistsAsync(store.Body, PgTableTuning.QueryStoreServerHashIndexName, ct), next.Joined);
            Assert.Single(next.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal)
                && l.Contains(PgTableTuning.QueryStoreServerHashIndexName, StringComparison.Ordinal)
                && l.Contains("is not built this start", StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// #9827 with #9795: the earlier build took 5 s of the shared 300 s, so the bounded build runs under the 295 s left and is
    /// shortened. When it runs out of time it gets the shortened Warning and no record, and the next start tries it again.
    /// </summary>
    [Fact]
    public async Task ATimeoutOfTheBoundedBuild_AfterAnEarlierBuildOfSeveralSeconds_IsNotRecorded()
    {
        var ct = TestContext.Current.CancellationToken;
        var (store, log, records) = await TimeOutBoundedBuildAfterAnEarlierBuildAsync(earlierBuildSeconds: 5, ct);
        await using (store)
        {
            Assert.True(await IndexExistsAsync(store.Body, PgTableTuning.CpuWindowCoverIndexName, ct), log.Joined);
            Assert.False(await IndexExistsAsync(store.Body, PgTableTuning.QueryStoreServerHashIndexName, ct), log.Joined);
            Assert.Equal(0L, records);
            Assert.Contains("up to 295 s", Assert.Single(BeforeBuildLines(log, PgTableTuning.QueryStoreServerHashIndexName, UpgradedTables[2])), StringComparison.Ordinal);
            Assert.Single(log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal)
                && l.Contains(PgTableTuning.QueryStoreServerHashIndexName, StringComparison.Ordinal)
                && l.Contains("was not built at this start", StringComparison.Ordinal));
            Assert.DoesNotContain(log.Lines, l => l.Contains("uncompressed heap pages", StringComparison.Ordinal));

            var next = new CapturingTestLogger();
            await PgTableTuning.ApplyAsync(store.Body, next, ct);
            Assert.True(await IndexExistsAsync(store.Body, PgTableTuning.QueryStoreServerHashIndexName, ct), next.Joined);
        }
    }

    /// <summary>
    /// #9827 with #9289: the bounded build's own limit is below the start's 300 s, so what shortens it is the time left measured
    /// against that limit. With a 120 s limit and an earlier build of 180.4 s, 119.6 s are left, which the whole-seconds floor reads as
    /// 119 s: a shortfall of 0.4 s is not a shortening and the timeout is recorded, though the earlier build took more than a whole
    /// second. An earlier build of 181.5 s leaves 118.5 s, a shortfall of 1.5 s, which is a shortening and is not recorded.
    /// </summary>
    [Theory]
    [InlineData(180.4, 1L)]
    [InlineData(181.5, 0L)]
    public async Task ATimeoutOfTheBoundedBuild_WhoseLimitIsBelowTheStartsTime_IsRecordedOnlyWhenTheTimeLeftFallsAWholeSecondShort(
        double earlierBuildSeconds, long expectedRecords)
    {
        var ct = TestContext.Current.CancellationToken;
        var (store, log, records) = await TimeOutBoundedBuildAfterAnEarlierBuildAsync(earlierBuildSeconds, ct, boundedLimitSeconds: 120);
        await using (store)
        {
            Assert.True(await IndexExistsAsync(store.Body, PgTableTuning.CpuWindowCoverIndexName, ct), log.Joined);
            Assert.False(await IndexExistsAsync(store.Body, PgTableTuning.QueryStoreServerHashIndexName, ct), log.Joined);
            Assert.Equal(expectedRecords, records);
            Assert.Equal(expectedRecords == 1L, log.Lines.Any(l => l.Contains("uncompressed heap pages", StringComparison.Ordinal)));
            Assert.Equal(expectedRecords == 0L, log.Lines.Any(l => l.Contains("was not built at this start", StringComparison.Ordinal)));
        }
    }

    /// <summary>
    /// A start where the CPU cover builds first and takes <paramref name="earlierBuildSeconds"/> on the test clock, and the bounded
    /// Query Store build, whose own limit is <paramref name="boundedLimitSeconds"/>, then times out against a lock another session holds.
    /// The caller disposes the returned store.
    /// </summary>
    private static async Task<(SeededStore Store, CapturingTestLogger Log, long Records)> TimeOutBoundedBuildAfterAnEarlierBuildAsync(
        double earlierBuildSeconds, CancellationToken ct, int boundedLimitSeconds = 300)
    {
        var store = await SeededStore.CreateAsync(ct);
        try
        {
            await store.DropAsync(ct, PgTableTuning.CpuWindowCoverIndexName, PgTableTuning.QueryStoreServerHashIndexName);
            var log = new CapturingTestLogger();
            await using (var holder = new NpgsqlConnection(store.ConnectionString))
            {
                await holder.OpenAsync(ct);
                await Exec(holder, "BEGIN; LOCK TABLE collect.query_store_stats IN SHARE UPDATE EXCLUSIVE MODE", ct);
                await Exec(store.Body, "SET statement_timeout = '2s'", ct);
                try
                {
                    await PgTableTuning.ApplyAsync(store.Body, log, hourly: false, new TuningStartPasses.BuildClock(earlierBuildSeconds), boundedLimitSeconds, ct);
                }
                finally
                {
                    await Exec(holder, "ROLLBACK", CancellationToken.None);
                    await Exec(store.Body, "RESET statement_timeout", CancellationToken.None);
                }
            }

            return (store, log, await BuildRecordsAsync(store.Body, ct));
        }
        catch
        {
            await store.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// #6460: a first start on a brand-new, empty store builds all three large start-path indexes in ONE pass (an empty table
    /// builds instantly, so none of them uses the shared build budget), so the start logs no deferral and the first hourly pass
    /// finds nothing missing to warn about. Before, the first start built one and deferred the other two, and the hourly pass
    /// 30 s later logged two Warnings that read like a fault.
    /// </summary>
    [Fact]
    public async Task AFirstStartOnAnEmptyStore_BuildsEveryLargeIndex_AndLogsNothingThatReadsLikeAFault()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the first-start live test.");

        var ct = TestContext.Current.CancellationToken;
        PgTableTuning.ResetHourlyMissingWarnings();
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);
        foreach (var name in Deferred)
        {
            await Exec(body, "DROP INDEX IF EXISTS collect." + name, ct);
        }

        var start = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start, ct);
        foreach (var name in Deferred)
        {
            Assert.True(await IndexExistsAsync(body, name, ct), name + " should be built by the one start pass: " + start.Joined);
        }

        var hourly = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, hourly, hourly: true, ct);
        Assert.Equal(0, start.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, hourly.CountAtLevel(LogLevel.Warning));
        Assert.DoesNotContain(start.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.DoesNotContain(hourly.Lines, l => l.Contains("is missing", StringComparison.Ordinal));
        Assert.DoesNotContain(start.Lines, l => l.Contains("is missing", StringComparison.Ordinal));

        /* #9827: an empty table builds in no time and pauses nothing, so the start does not announce its builds. */
        Assert.Empty(AnyBuildLines(start));
    }

    /// <summary>
    /// #6460 review: the start-path emptiness check is a bare SELECT and queues behind an ACCESS EXCLUSIVE holder
    /// (drop_chunks, decompress_chunk, VACUUM FULL). It must give up at the create lock bound, not at the 300 s setup
    /// timeout, then fail closed onto the shared build budget (with a Warning naming the table), and the connection must stay
    /// usable: the sweep goes on to the CREATE on the same connection, which finds the table busy too, and a later start builds the index.
    /// #6539: the ALTER TABLE ... SET statements on the same table are bounded the same way, so the WHOLE start returns in well
    /// under a minute (it sat on the first ALTER for the full 300 s setup timeout before), logs the skipped table at Information,
    /// and a later pass applies the setting.
    /// </summary>
    [Fact]
    public async Task AnEmptinessCheck_ThatFindsTheTableLockedExclusively_GivesUpQuickly_AndALaterSweepBuildsTheIndex()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the locked-table emptiness-check live test.");

        var ct = TestContext.Current.CancellationToken;
        PgTableTuning.ResetHourlyMissingWarnings();
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);
        await TuningStartPasses.ConvergeAsync(body, ct);
        await SeedStatsRowsAsync(body, ct);
        await Exec(body, "DROP INDEX collect." + PgTableTuning.QueryStatsRestartRowIndexName, ct);
        await Exec(body, "ALTER TABLE collect.query_stats RESET (autovacuum_vacuum_insert_scale_factor, autovacuum_vacuum_insert_threshold)", ct);
        Assert.False(await InsertTuningAppliedAsync(body, ct));

        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        await Exec(holder, "BEGIN", ct);
        try
        {
            await Exec(holder, "LOCK TABLE collect.query_stats IN ACCESS EXCLUSIVE MODE", ct);
            var busy = new CapturingTestLogger();

            /* The whole start is bounded: the emptiness check (5 s), the CREATE (5 s) and each ALTER TABLE ... SET on the
               locked table (5 s apiece) all give up, about 20 s in all. The token is cut at 60 s: a start that is still
               waiting then (the ALTER waited the full 300 s before #6539) fails the cut assertion below. */
            using var cut = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cut.CancelAfter(TimeSpan.FromSeconds(60));
            try
            {
                await PgTableTuning.ApplyAsync(body, busy, cut.Token);
            }
            catch (OperationCanceledException) when (cut.IsCancellationRequested && !ct.IsCancellationRequested)
            {
            }

            Assert.False(cut.IsCancellationRequested, "the whole start should give up on the locked table in well under 60 s: " + busy.Joined);
            Assert.Contains(busy.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal)
                && l.Contains("collect.query_stats", StringComparison.Ordinal)
                && l.Contains("not applied this pass", StringComparison.Ordinal));
            Assert.False(await InsertTuningAppliedAsync(body, ct), busy.Joined);

            Assert.Contains(busy.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal)
                && l.Contains("collect.query_stats", StringComparison.Ordinal)
                && l.Contains("Could not tell whether", StringComparison.Ordinal));
            Assert.Contains(busy.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal)
                && l.Contains(PgTableTuning.QueryStatsRestartRowIndexName, StringComparison.Ordinal)
                && l.Contains("table was busy", StringComparison.Ordinal));
        }
        finally
        {
            await Exec(holder, "ROLLBACK", CancellationToken.None);
        }

        /* A later start (a fresh connection: the cut-off one is not reused) builds the index once the lock is gone. */
        await using var restart = new NpgsqlConnection(scratch.ConnectionString);
        await restart.OpenAsync(ct);
        var later = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(restart, later, ct);
        Assert.True(await IndexExistsAsync(restart, PgTableTuning.QueryStatsRestartRowIndexName, ct), later.Joined);
        Assert.True(await InsertTuningAppliedAsync(restart, ct), later.Joined);
    }

    private static async Task<bool> InsertTuningAppliedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT COALESCE(array_to_string(c.reloptions, ','), '') LIKE '%insert_scale_factor%' FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            + "WHERE n.nspname = 'collect' AND c.relname = 'query_stats'", c);
        return (bool)(await cmd.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The first-missing index in list order is the earliest start-path index, so the legacy-row index goes before the restart-row ones.</summary>
    [Fact]
    public void TheLegacyRowIndex_IsListedBeforeTheRestartRowIndexes_AndAllThreeShareTheBudget()
    {
        var order = PgTableTuning.Statements.Select((s, i) => (s, i)).ToList();
        int At(string name) => order.First(o => o.s.Contains(name + " ON", StringComparison.Ordinal)).i;
        Assert.True(At(PgTableTuning.LegacyRowIndexName) < At(PgTableTuning.QueryStatsRestartRowIndexName));
        Assert.True(At(PgTableTuning.QueryStatsRestartRowIndexName) < At(PgTableTuning.ProcedureStatsRestartRowIndexName));
        foreach (var name in Deferred)
        {
            Assert.True(PgTableTuning.SharesStartBuildBudget(name), name);
        }
    }

    [Fact]
    public void ABuildThatRanOutOfTime_IsToldApartFromAnyOtherFailure()
    {
        Assert.True(PgTableTuning.IsBuildTimeout(new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"))));
        Assert.True(PgTableTuning.IsBuildTimeout(new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", PostgresErrorCodes.QueryCanceled)));
        Assert.False(PgTableTuning.IsBuildTimeout(new PostgresException("relation does not exist", "ERROR", "ERROR", PostgresErrorCodes.UndefinedTable)));
        Assert.False(PgTableTuning.IsBuildTimeout(new NpgsqlException("connection reset", new System.IO.IOException("reset"))));
        Assert.False(PgTableTuning.IsBuildTimeout(new InvalidOperationException("boom")));
    }

    [Fact]
    public void TheTimeoutWarning_NamesTheIndexTheTableTheElapsedTimeAndTheRetry()
    {
        var message = PgTableTuning.BuildTimeoutMessage("idx_demo", "collect.query_stats", 301.4);
        Assert.Equal(
            "Index collect.idx_demo on collect.query_stats was not built: the build ran out of time after 301 s (limit 300 s) and was rolled back; it is retried at the next start, and each try pauses collection at start for up to 300 s",
            message);
    }

    [Fact]
    public void TheStartPathCatch_UsesTheTimeoutWarning_OnlyForABuildThatTimedOut()
    {
        var src = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgTableTuning.cs").ReplaceLineEndings("\n");
        Assert.Contains("statement.StartsWith(CreateIndexPrefix, StringComparison.Ordinal) && IsBuildTimeout(ex)", src, StringComparison.Ordinal);
        Assert.Contains("BuildTimeoutMessage(", src, StringComparison.Ordinal);
        Assert.Contains("Composer performance-tuning statement failed", src, StringComparison.Ordinal);
    }

    /// <summary>A fully tuned plain store with a row in each table the <see cref="Upgraded"/> indexes are on, so their builds use the shared time.</summary>
    private sealed class SeededStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private SeededStore(ScratchPostgres scratch, NpgsqlConnection body)
        {
            _scratch = scratch;
            Body = body;
        }

        public NpgsqlConnection Body { get; }

        public string ConnectionString => _scratch.ConnectionString;

        public static async Task<SeededStore> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the shared build time live tests.");
            PgTableTuning.ResetHourlyMissingWarnings();
            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            var body = new NpgsqlConnection(scratch.ConnectionString);
            try
            {
                await body.OpenAsync(ct);
                await PgMigrations.MigrateAsync(body, ct);
                await TuningStartPasses.ConvergeAsync(body, ct);
                await SeedStatsRowsAsync(body, ct);
                await Exec(body, @"
INSERT INTO collect.cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES (9795000, now() AT TIME ZONE 'UTC', -9795, 'Seed', now() AT TIME ZONE 'UTC', 30, 5)", ct);
                await Exec(body, @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
SELECT 9795001, t.ts, -9795, 'Seed', 'Db', 'dbo.P', '0xh9795', 1, 1, 'Regular', 'PRIMARY', 1, t.ts, t.ts, 1, 1000, 500, 900, 400, t.ts + interval '1 hour'
FROM (SELECT now() AT TIME ZONE 'UTC' AS ts) AS t", ct);
                return new SeededStore(scratch, body);
            }
            catch
            {
                await body.DisposeAsync();
                await scratch.DisposeAsync();
                throw;
            }
        }

        public async Task DropAsync(CancellationToken ct, params string[] indexes)
        {
            foreach (var name in indexes)
            {
                await Exec(Body, "DROP INDEX collect." + name, ct);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Body.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }

    /// <summary>The rows of the timed-out build record (#9289), 0 when the table does not exist.</summary>
    private static async Task<long> BuildRecordsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using (var exists = new NpgsqlCommand("SELECT to_regclass('collect.index_build_attempts') IS NOT NULL", c))
        {
            if (await exists.ExecuteScalarAsync(ct) is not true)
            {
                return 0;
            }
        }

        await using var cmd = new NpgsqlCommand("SELECT count(*) FROM collect.index_build_attempts", c);
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task SeedStatsRowsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await Exec(c, @"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (6460000, now() AT TIME ZONE 'UTC', -6460, 'Seed', 'Db', 'dbo', 'usp_Seed', '0x01', 1, 1, 1, 300)", ct);
        await Exec(c, @"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (6460001, now() AT TIME ZONE 'UTC', -6460, 'Seed', 'Db', 'QH6460', '0x01', 1, 1, 1, 300)", ct);
    }

    private static async Task<string> OidsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT string_agg(c.relname || ':' || c.oid::text, ',' ORDER BY c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            + "WHERE n.nspname = 'collect' AND c.relname = ANY($1)", c);
        cmd.Parameters.AddWithValue(Deferred.Concat(Upgraded).ToArray());
        return (string)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection c, string name, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1", c);
        cmd.Parameters.AddWithValue(name);
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }
}
