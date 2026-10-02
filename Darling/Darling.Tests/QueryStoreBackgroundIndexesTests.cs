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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4952's two background indexes, without a store: the build-or-skip decision for each, the DDL each ensure runs,
/// that each index's shape is the shape of the read it serves, and that the worker's one delayed task carries all
/// of them.
/// </summary>
public sealed class QueryStoreBackgroundIndexesTests
{
    private static readonly QueryStoreBackgroundIndexes.IndexSpec Probe = QueryStoreBackgroundIndexes.LegacyProbe;
    private static readonly QueryStoreBackgroundIndexes.IndexSpec Wide = QueryStoreBackgroundIndexes.WideServerFirstExec;

    [Theory]
    [InlineData(140000, false, QueryStoreBackgroundIndexes.IndexAction.Build)]
    [InlineData(180006, false, QueryStoreBackgroundIndexes.IndexAction.Build)]
    [InlineData(140000, true, QueryStoreBackgroundIndexes.IndexAction.BuildPerChunk)]
    [InlineData(180006, true, QueryStoreBackgroundIndexes.IndexAction.BuildPerChunk)]
    public void TheLegacyProbeIndex_BuildsOnAnyVersion_PerChunkOnAHypertable_ConcurrentlyOnAPlainTable(
        int serverVersionNum, bool hypertable, QueryStoreBackgroundIndexes.IndexAction expected)
    {
        Assert.Equal(expected, QueryStoreBackgroundIndexes.Decide(Probe, serverVersionNum, hypertable).Action);
    }

    [Theory]
    [InlineData(140000, false, QueryStoreBackgroundIndexes.IndexAction.Build)]
    [InlineData(180006, false, QueryStoreBackgroundIndexes.IndexAction.Build)]
    [InlineData(140000, true, QueryStoreBackgroundIndexes.IndexAction.SkipHypertable)]
    [InlineData(180006, true, QueryStoreBackgroundIndexes.IndexAction.SkipHypertable)]
    public void TheWideBtree_BuildsConcurrentlyOnTheHeap_AndSkipsAHypertable(
        int serverVersionNum, bool hypertable, QueryStoreBackgroundIndexes.IndexAction expected)
    {
        Assert.Equal(expected, QueryStoreBackgroundIndexes.Decide(Wide, serverVersionNum, hypertable).Action);
        Assert.Null(Wide.HypertableCreateSql);
    }

    [Fact]
    public void TheLegacyProbeDdl_IsAPartialBtree_WithThePerChunkOptionBeforeTheWhere_AndNeverConcurrentOnAHypertable()
    {
        var hypertable = Probe.HypertableCreateSql!;
        Assert.Contains("CREATE INDEX IF NOT EXISTS ix_query_store_stats_legacy_server_time", hypertable);
        Assert.Contains("ON collect.query_store_stats (server_id, collection_time)", hypertable);
        Assert.Contains("WITH (timescaledb.transaction_per_chunk)", hypertable);
        Assert.Contains("WHERE interval_start_time_utc IS NULL", hypertable);
        Assert.True(
            hypertable.IndexOf("WITH (timescaledb.transaction_per_chunk)", StringComparison.Ordinal)
                < hypertable.IndexOf("WHERE interval_start_time_utc IS NULL", StringComparison.Ordinal),
            "WITH goes before WHERE in CREATE INDEX");
        Assert.DoesNotContain("CONCURRENTLY", hypertable, StringComparison.Ordinal);

        Assert.Contains("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_query_store_stats_legacy_server_time", Probe.PlainCreateSql);
        Assert.Contains("WHERE interval_start_time_utc IS NULL", Probe.PlainCreateSql);
        Assert.DoesNotContain("transaction_per_chunk", Probe.PlainCreateSql, StringComparison.Ordinal);

        /* TimescaleDB refuses DROP INDEX CONCURRENTLY on a hypertable index, so that arm drops plainly. */
        Assert.Contains("DROP INDEX CONCURRENTLY IF EXISTS", Probe.PlainDropSql);
        Assert.Contains("DROP INDEX IF EXISTS", Probe.HypertableDropSql);
        Assert.DoesNotContain("CONCURRENTLY", Probe.HypertableDropSql, StringComparison.Ordinal);
        Assert.Equal(QueryStoreBackgroundIndexes.LegacyProbeIndexName, Probe.IndexName);
    }

    [Fact]
    public void TheWideBtreeDdl_IsAConcurrentBtreeOnServerAndFirstExecutionTime()
    {
        var sql = Wide.PlainCreateSql;
        Assert.Contains("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_query_store_interval_wide_server_first_exec", sql);
        Assert.Contains("ON collect.query_store_interval_wide (server_id, first_execution_time)", sql);
        Assert.DoesNotContain("brin", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(" WHERE ", sql, StringComparison.Ordinal);
        Assert.Contains("DROP INDEX CONCURRENTLY IF EXISTS", Wide.PlainDropSql);
        Assert.Equal(QueryStoreBackgroundIndexes.WideServerFirstExecIndexName, Wide.IndexName);
    }

    [Fact]
    public void TheLegacyProbeIndex_HasTheProbesOwnPredicateAndKey_SoTheTwoCannotDrift()
    {
        var probe = QueryStoreIntervalWide.HasLegacyRowSql;
        Assert.Contains("FROM collect.query_store_stats", probe);
        Assert.Contains("s.server_id = $1", probe);
        Assert.Contains("s.collection_time >= $2", probe);
        Assert.Contains("s.collection_time <= $3", probe);
        Assert.Contains("s.interval_start_time_utc IS NULL", probe);
        Assert.Contains("(server_id, collection_time)", Probe.PlainCreateSql);
        Assert.Contains("WHERE interval_start_time_utc IS NULL", Probe.PlainCreateSql);
    }

    [Fact]
    public void TheWideBtree_LeadsWithTheServerAndCarriesTheFirstExecutionBoundTheTableReadsFilterOn()
    {
        foreach (var sql in new[] { DarlingDataReader.QueryStoreTopTableSql })
        {
            Assert.Contains("WHERE server_id = $1", sql);
            Assert.Contains("first_execution_time >= $2 - ", sql);
        }

        Assert.Contains("(server_id, first_execution_time)", Wide.PlainCreateSql);
    }

    [Fact]
    public void TheWritersDoUpdateSetList_NeverSetsTheServerOrTheFirstExecutionTime_SoTheWideBtreeKeepsUpdatesHot()
    {
        /* The HOT premise of the wide btree: an update stays heap-only only while no indexed column changes, and the
           btree's two columns are the first key and part of the upsert's identity, so ON CONFLICT never rewrites them. */
        var setColumns = QueryStoreIntervalWideBrinIndexLiveTests.UpsertSetColumns();
        Assert.True(setColumns.Count >= 50, $"the parse found {setColumns.Count} SET columns; the upsert sets about 55");
        Assert.Contains("collection_time", setColumns);
        Assert.DoesNotContain("server_id", setColumns);
        Assert.DoesNotContain("first_execution_time", setColumns);

        var identity = QueryStoreIntervalWide.IdentityColumns.Split(',', StringSplitOptions.TrimEntries);
        Assert.Contains("server_id", identity);
        Assert.Contains("first_execution_time", identity);
    }

    [Fact]
    public void EveryBackgroundIndex_IsRegisteredOnce_Idempotent_AndHasADrop()
    {
        var all = QueryStoreBackgroundIndexes.All;
        Assert.Equal(
            new[]
            {
                QueryStoreIntervalWideBrinIndex.IndexName,
                QueryStoreBackgroundIndexes.WideServerFirstExecIndexName,
                QueryStoreBackgroundIndexes.LegacyProbeIndexName,
            },
            all.Select(spec => spec.IndexName).ToArray());

        foreach (var spec in all)
        {
            Assert.StartsWith("collect.", spec.IndexName, StringComparison.Ordinal);
            Assert.StartsWith("collect.", spec.TableName, StringComparison.Ordinal);
            Assert.Contains("IF NOT EXISTS", spec.PlainCreateSql);
            Assert.Contains("IF NOT EXISTS", spec.HypertableCreateSql ?? spec.PlainCreateSql);
            Assert.Contains("IF EXISTS", spec.PlainDropSql);
            Assert.Contains("IF EXISTS", spec.HypertableDropSql);
        }
    }

    /* The per-chunk build holds a ShareLock on the chunk it scans, which blocks the collector's COPY into the newest
       chunk for that chunk's scan time; the COPY has a 10 s deadline. So the partial index is built only while the
       newest chunk's heap is at or below the limit, and an attempt above it is deferred, not failed (#4952). */
    [Theory]
    [InlineData(0L, QueryStoreBackgroundIndexes.IndexAction.BuildPerChunk)]
    [InlineData(QueryStoreBackgroundIndexes.NewestChunkMaxBytes - 1, QueryStoreBackgroundIndexes.IndexAction.BuildPerChunk)]
    [InlineData(QueryStoreBackgroundIndexes.NewestChunkMaxBytes, QueryStoreBackgroundIndexes.IndexAction.BuildPerChunk)]
    [InlineData(QueryStoreBackgroundIndexes.NewestChunkMaxBytes + 1, QueryStoreBackgroundIndexes.IndexAction.SkipNewestChunkLarge)]
    [InlineData(9L * 1024 * 1024 * 1024, QueryStoreBackgroundIndexes.IndexAction.SkipNewestChunkLarge)]
    public void ThePartialIndex_BuildsPerChunkWhileTheNewestChunkIsAtOrBelowTheLimit_AndDefersAboveIt(
        long newestChunkBytes, QueryStoreBackgroundIndexes.IndexAction expected)
    {
        var decision = QueryStoreBackgroundIndexes.Decide(Probe, 180006, true, newestChunkBytes);
        Assert.Equal(expected, decision.Action);
        if (expected == QueryStoreBackgroundIndexes.IndexAction.SkipNewestChunkLarge)
        {
            Assert.Contains("collect.query_store_stats", decision.Reason, StringComparison.Ordinal);
            Assert.Contains("256 MB", decision.Reason, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheNewestChunkLimit_IsAboutTwoHundredFiftySixMegabytes_AndOnlyThePartialIndexCarriesIt()
    {
        Assert.Equal(256L * 1024 * 1024, QueryStoreBackgroundIndexes.NewestChunkMaxBytes);
        Assert.Equal(TimeSpan.FromHours(1), QueryStoreBackgroundIndexes.RetryInterval);
        Assert.Equal(QueryStoreBackgroundIndexes.NewestChunkMaxBytes, Probe.MaxNewestChunkBytes);
        Assert.Null(Wide.MaxNewestChunkBytes);
        Assert.Null(QueryStoreIntervalWideBrinIndex.Spec.MaxNewestChunkBytes);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(9L * 1024 * 1024 * 1024)]
    public void TheNewestChunksSize_NeverDefersAPlainTable_OrAnIndexWithoutALimit(long newestChunkBytes)
    {
        /* CREATE INDEX CONCURRENTLY on a plain table blocks no writes, so the size is not asked. */
        Assert.Equal(QueryStoreBackgroundIndexes.IndexAction.Build, QueryStoreBackgroundIndexes.Decide(Probe, 180006, false, newestChunkBytes).Action);
        Assert.Equal(QueryStoreBackgroundIndexes.IndexAction.Build, QueryStoreBackgroundIndexes.Decide(Wide, 180006, false, newestChunkBytes).Action);

        /* The other two indexes' verdicts on a hypertable are the ones they had before the limit existed. */
        Assert.Equal(QueryStoreBackgroundIndexes.IndexAction.SkipHypertable, QueryStoreBackgroundIndexes.Decide(Wide, 180006, true, newestChunkBytes).Action);
        Assert.Equal(QueryStoreBackgroundIndexes.IndexAction.SkipHypertable, QueryStoreBackgroundIndexes.Decide(QueryStoreIntervalWideBrinIndex.Spec, 180006, true, newestChunkBytes).Action);

        /* The version floor still comes first. */
        Assert.Equal(
            QueryStoreBackgroundIndexes.IndexAction.SkipServerVersion,
            QueryStoreBackgroundIndexes.Decide(QueryStoreIntervalWideBrinIndex.Spec, 150000, true, newestChunkBytes).Action);
    }

    [Fact]
    public async Task ADeferredAttempt_IsRetriedUntilItSettles_AndASettledIndexIsNeverAttemptedAgain()
    {
        var calls = new List<string>();
        var probeAttempts = 0;

        await QueryStoreBackgroundIndexes.RunDelayedAsync(
            NullLogger.Instance,
            TimeSpan.Zero,
            TimeSpan.FromMilliseconds(1),
            new[] { Wide, Probe },
            (spec, isRetry, _) =>
            {
                calls.Add(isRetry ? $"{spec.IndexName} (retry)" : spec.IndexName);
                var outcome = spec == Probe && ++probeAttempts < 3
                    ? QueryStoreBackgroundIndexes.EnsureOutcome.RetryLater
                    : QueryStoreBackgroundIndexes.EnsureOutcome.Settled;
                return Task.FromResult(outcome);
            },
            CancellationToken.None);

        Assert.Equal(
            new[]
            {
                Wide.IndexName,
                Probe.IndexName,
                Probe.IndexName + " (retry)",
                Probe.IndexName + " (retry)",
            },
            calls);
    }

    [Fact]
    public async Task TheRetry_WaitsTheIntervalAndEndsQuietlyWhenTheServiceStops()
    {
        using var stop = new CancellationTokenSource();
        var calls = 0;
        var logger = new CapturingTestLogger();

        var run = QueryStoreBackgroundIndexes.RunDelayedAsync(
            logger,
            TimeSpan.Zero,
            TimeSpan.FromMinutes(5),
            new[] { Probe },
            (_, _, _) =>
            {
                Interlocked.Increment(ref calls);
                return Task.FromResult(QueryStoreBackgroundIndexes.EnsureOutcome.RetryLater);
            },
            stop.Token);

        await Task.Delay(300, TestContext.Current.CancellationToken);
        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.False(run.IsCompleted, "a deferred index keeps the run waiting for the retry interval");

        stop.Cancel();
        var finished = await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Same(run, finished);
        await run;
        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
    }

    [Fact]
    public async Task AFailedAttempt_IsNotRetriedInTheSameRun_AndDoesNotStopTheNextIndex()
    {
        var calls = new List<string>();
        var logger = new CapturingTestLogger();

        await QueryStoreBackgroundIndexes.RunDelayedAsync(
            logger,
            TimeSpan.Zero,
            TimeSpan.FromMilliseconds(1),
            new[] { Wide, Probe },
            (spec, isRetry, _) =>
            {
                calls.Add(isRetry ? $"{spec.IndexName} (retry)" : spec.IndexName);
                return spec == Wide
                    ? throw new InvalidOperationException("the build failed")
                    : Task.FromResult(QueryStoreBackgroundIndexes.EnsureOutcome.Settled);
            },
            CancellationToken.None);

        Assert.Equal(new[] { Wide.IndexName, Probe.IndexName }, calls);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("retried at the next start", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void ADeferral_IsLoggedOnceAtInformation_AndQuietlyOnEveryRetryAfterIt()
    {
        var logger = new CapturingTestLogger();
        const string reason = "collect.query_store_stats's newest chunk is 5.0 GB, above the 256 MB limit";

        QueryStoreBackgroundIndexes.LogDeferred(logger, Probe, reason, isRetry: false);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Information));
        Assert.Contains(QueryStoreBackgroundIndexes.LegacyProbeIndexName, logger.Joined, StringComparison.Ordinal);
        Assert.Contains("256 MB", logger.Joined, StringComparison.Ordinal);

        QueryStoreBackgroundIndexes.LogDeferred(logger, Probe, reason, isRetry: true);
        QueryStoreBackgroundIndexes.LogDeferred(logger, Probe, reason, isRetry: true);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Information));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public void TheWorker_RunsTheThreeEnsuresInOneDelayedTask_NotThreeConcurrentOnes()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs").Replace("\r\n", "\n");

        Assert.Single(System.Text.RegularExpressions.Regex.Matches(source, @"QueryStoreBackgroundIndexes\.RunDelayedAsync\("));
        Assert.Contains("QueryStoreBackgroundIndexes.All", source);
        Assert.DoesNotContain("QueryStoreIntervalWideBrinIndex.RunDelayedAsync", source);

        /* The loop is sequential and each failure is isolated: a failed index warns and the next one still runs. */
        var engine = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "QueryStoreBackgroundIndexes.cs").Replace("\r\n", "\n");
        Assert.Contains("foreach (var spec in specs)", engine);
        Assert.Contains("catch (Exception ex) when (!cancellationToken.IsCancellationRequested)", engine);
    }
}
