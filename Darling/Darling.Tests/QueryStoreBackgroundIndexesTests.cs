/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
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
