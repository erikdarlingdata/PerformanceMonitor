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
/// The background Query Store indexes without a store: which indexes there are (the BRIN and #4952's btree, none on
/// the raw hypertable), the build-or-skip decision, the DDL each ensure runs, that the btree's shape is the shape of
/// the read it serves, and that the worker's one delayed task makes one in-order attempt at each of them.
/// </summary>
public sealed class QueryStoreBackgroundIndexesTests
{
    private static readonly QueryStoreBackgroundIndexes.IndexSpec Wide = QueryStoreBackgroundIndexes.WideServerFirstExec;
    private static readonly QueryStoreBackgroundIndexes.IndexSpec Brin = QueryStoreIntervalWideBrinIndex.Spec;

    [Theory]
    [InlineData(140000, false, QueryStoreBackgroundIndexes.IndexAction.Build)]
    [InlineData(180006, false, QueryStoreBackgroundIndexes.IndexAction.Build)]
    [InlineData(140000, true, QueryStoreBackgroundIndexes.IndexAction.SkipHypertable)]
    [InlineData(180006, true, QueryStoreBackgroundIndexes.IndexAction.SkipHypertable)]
    public void TheWideBtree_BuildsConcurrentlyOnTheHeap_AndSkipsAHypertable(
        int serverVersionNum, bool hypertable, QueryStoreBackgroundIndexes.IndexAction expected)
    {
        Assert.Equal(expected, QueryStoreBackgroundIndexes.Decide(Wide, serverVersionNum, hypertable).Action);
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

    /* collect.query_store_stats is a hypertable, and an index on one can only be built per chunk, which locks each
       chunk it builds. The Query Store backfill writes backdated rows into older chunks as well as the newest, under
       the collector's 10 s COPY deadline, so no chunk is safe to lock and no background index targets that table. */
    [Fact]
    public void TheBackgroundIndexes_AreTheBrinThenTheWideBtree_AndNoneTargetsTheRawHypertable()
    {
        Assert.Equal(
            new[]
            {
                QueryStoreIntervalWideBrinIndex.IndexName,
                QueryStoreBackgroundIndexes.WideServerFirstExecIndexName,
            },
            QueryStoreBackgroundIndexes.All.Select(spec => spec.IndexName).ToArray());

        foreach (var spec in QueryStoreBackgroundIndexes.All)
        {
            Assert.NotEqual("collect.query_store_stats", spec.TableName);
            Assert.DoesNotContain("query_store_stats", spec.PlainCreateSql, StringComparison.Ordinal);
            Assert.DoesNotContain("query_store_stats", spec.PlainDropSql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EveryBackgroundIndex_IsRegisteredOnce_BuildsConcurrentlyAndIdempotently_AndHasAConcurrentDrop()
    {
        var all = QueryStoreBackgroundIndexes.All;
        Assert.Equal(all.Count, all.Select(spec => spec.IndexName).Distinct(StringComparer.Ordinal).Count());

        foreach (var spec in all)
        {
            Assert.StartsWith("collect.", spec.IndexName, StringComparison.Ordinal);
            Assert.StartsWith("collect.", spec.TableName, StringComparison.Ordinal);

            /* Only the plain form exists: a hypertable takes no per-chunk build, it is skipped. */
            Assert.Contains("CREATE INDEX CONCURRENTLY IF NOT EXISTS", spec.PlainCreateSql);
            Assert.DoesNotContain("transaction_per_chunk", spec.PlainCreateSql, StringComparison.Ordinal);
            Assert.Contains("DROP INDEX CONCURRENTLY IF EXISTS", spec.PlainDropSql);
        }

        /* The version floor comes before the table check, whatever the table is. */
        Assert.Equal(
            QueryStoreBackgroundIndexes.IndexAction.SkipServerVersion,
            QueryStoreBackgroundIndexes.Decide(Brin, 150000, true).Action);
        Assert.Equal(
            QueryStoreBackgroundIndexes.IndexAction.SkipHypertable,
            QueryStoreBackgroundIndexes.Decide(Brin, 180006, true).Action);
    }

    [Fact]
    public async Task TheDelayedRun_AttemptsEachIndexOnceInOrder()
    {
        var calls = new List<string>();

        await QueryStoreBackgroundIndexes.RunDelayedAsync(
            NullLogger.Instance,
            TimeSpan.Zero,
            QueryStoreBackgroundIndexes.All,
            (spec, _) =>
            {
                calls.Add(spec.IndexName);
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Equal(QueryStoreBackgroundIndexes.All.Select(spec => spec.IndexName).ToArray(), calls);
    }

    [Fact]
    public async Task AFailedAttempt_IsNotRetriedInTheSameRun_AndDoesNotStopTheNextIndex()
    {
        var calls = new List<string>();
        var logger = new CapturingTestLogger();

        await QueryStoreBackgroundIndexes.RunDelayedAsync(
            logger,
            TimeSpan.Zero,
            new[] { Wide, Brin },
            (spec, _) =>
            {
                calls.Add(spec.IndexName);
                return spec == Wide
                    ? throw new InvalidOperationException("the build failed")
                    : Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Equal(new[] { Wide.IndexName, Brin.IndexName }, calls);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("retried at the next start", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWorker_RunsTheTwoEnsuresInOneDelayedTask_NotTwoConcurrentOnes()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs").Replace("\r\n", "\n");

        Assert.Single(System.Text.RegularExpressions.Regex.Matches(source, @"QueryStoreBackgroundIndexes\.RunDelayedAsync\("));
        Assert.Contains("QueryStoreBackgroundIndexes.All", source);
        Assert.DoesNotContain("QueryStoreIntervalWideBrinIndex.RunDelayedAsync", source);

        /* The loop is sequential and each failure is isolated: a failed index warns and the next one still runs. One
           attempt per start: the start delay is the only wait, so nothing retries an index inside the run. */
        var engine = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "QueryStoreBackgroundIndexes.cs").Replace("\r\n", "\n");
        Assert.Contains("foreach (var spec in specs)", engine);
        Assert.Contains("catch (Exception ex) when (!cancellationToken.IsCancellationRequested)", engine);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(engine, @"Task\.Delay\("));
        Assert.DoesNotContain("while (", engine, StringComparison.Ordinal);
    }
}
