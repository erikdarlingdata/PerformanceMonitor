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
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
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
        /* Every per-server read of the table, the duration trend once per arm (its two arms are joined by UNION ALL and
           each carries its own server filter and floor). QueryStoreIntervalWideFirstExecFloorTests pins the rest of each
           read's floor: its margin, its position after the time bounds, and that the raw reads carry none. */
        var trendArms = ViewerDataService.QueryStoreDurationTrendTableSql.Split("UNION ALL", StringSplitOptions.None);
        Assert.Equal(2, trendArms.Length);

        var reads = new (string Name, string Sql)[]
        {
            ("DarlingDataReader.QueryStoreTopTableSql", DarlingDataReader.QueryStoreTopTableSql),
            ("ViewerDataService.QueryStoreTopTableSql", ViewerDataService.QueryStoreTopTableSql),
            ("ViewerDataService.QueryStoreDurationTrendTableSql, interval arm", trendArms[0]),
            ("ViewerDataService.QueryStoreDurationTrendTableSql, legacy arm", trendArms[1]),
        };

        foreach (var (name, sql) in reads)
        {
            Assert.True(sql.Contains("WHERE server_id = $1", StringComparison.Ordinal), name + " must filter on the server first");
            Assert.True(sql.Contains("first_execution_time >= $2 - ", StringComparison.Ordinal), name + " must bound first_execution_time on the window start");
        }

        Assert.Contains("(server_id, first_execution_time)", Wide.PlainCreateSql);
    }

    /* The hypertable probe compares the schema and the name as two bound values, so the spelling of a spec's TableName
       is not matched through a concatenation. */
    [Theory]
    [InlineData("collect.query_store_interval_wide", "collect", "query_store_interval_wide")]
    [InlineData("collect.query_store_stats", "collect", "query_store_stats")]
    [InlineData("a.b.c", "a.b", "c")]
    public void ATableName_SplitsAtItsLastDot_IntoItsSchemaAndItsName(string tableName, string schema, string name)
    {
        Assert.Equal((schema, name), QueryStoreBackgroundIndexes.SplitTableName(tableName));
    }

    [Theory]
    [InlineData("query_store_interval_wide")]
    [InlineData(".query_store_interval_wide")]
    [InlineData("collect.")]
    public void ATableNameWithoutBothAParts_IsRejected(string tableName)
    {
        Assert.Throws<ArgumentException>(() => QueryStoreBackgroundIndexes.SplitTableName(tableName));
    }

    [Fact]
    public void EveryBackgroundIndexsTableName_SplitsIntoItsRealSchemaAndName()
    {
        foreach (var spec in QueryStoreBackgroundIndexes.All)
        {
            var (schema, name) = QueryStoreBackgroundIndexes.SplitTableName(spec.TableName);
            Assert.Equal("collect", schema);
            Assert.Equal(spec.TableName, schema + "." + name);
        }
    }

    [Fact]
    public void TheHypertableProbe_ComparesTheSchemaAndTheNameAsTwoParameters_NotAConcatenation()
    {
        var sql = QueryStoreBackgroundIndexes.HypertableSql;
        Assert.Contains("h.hypertable_schema = $1", sql, StringComparison.Ordinal);
        Assert.Contains("h.hypertable_name = $2", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("||", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("'.'", sql, StringComparison.Ordinal);
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
        Assert.Contains(Wide.IndexName, logger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("(shutdown)", logger.Joined, StringComparison.Ordinal);
    }

    /* A failed ensure's warning names the server error's SQLSTATE, so a full disk (53100) reads differently from a lock
       timeout without opening the exception. An error that carries no SQLSTATE names none. */
    [Fact]
    public async Task AFailedEnsure_NamesTheServerErrorsSqlState_InTheWarning()
    {
        var line = await LogOneFailedEnsureAsync(new PostgresException("could not extend file", "ERROR", "ERROR", "53100"));

        Assert.StartsWith("Warning:", line, StringComparison.Ordinal);
        Assert.Contains(nameof(PostgresException) + ", SQLSTATE 53100:", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFailedEnsure_NamesNoSqlState_WhenTheErrorCarriesNone()
    {
        var notFromTheServer = await LogOneFailedEnsureAsync(new InvalidOperationException("the build failed"));
        var noStateOnTheError = await LogOneFailedEnsureAsync(new NpgsqlException("the connection was closed"));

        Assert.DoesNotContain("SQLSTATE", notFromTheServer, StringComparison.Ordinal);
        Assert.Contains(nameof(InvalidOperationException) + ": the build failed", notFromTheServer, StringComparison.Ordinal);
        Assert.DoesNotContain("SQLSTATE", noStateOnTheError, StringComparison.Ordinal);
        Assert.Contains(nameof(NpgsqlException) + ": the connection was closed", noStateOnTheError, StringComparison.Ordinal);
    }

    private static async Task<string> LogOneFailedEnsureAsync(Exception failure)
    {
        var logger = new CapturingTestLogger();

        await QueryStoreBackgroundIndexes.RunDelayedAsync(
            logger,
            TimeSpan.Zero,
            new[] { Wide },
            (_, _) => throw failure,
            CancellationToken.None);

        return Assert.Single(logger.Lines);
    }

    /* The delayed run's three shutdown lines each name the index they are about: the one in progress, or, before the
       delay is over, every index the run would have built. None puts a reason where the index name goes. */
    [Fact]
    public async Task CancelledBeforeItStarted_NamesEveryIndexTheRunWouldHaveBuilt_AndAttemptsNone()
    {
        using var shutdown = new CancellationTokenSource();
        shutdown.Cancel();
        var calls = new List<string>();
        var logger = new CapturingTestLogger();

        await QueryStoreBackgroundIndexes.RunDelayedAsync(
            logger,
            QueryStoreBackgroundIndexes.StartDelay,
            QueryStoreBackgroundIndexes.All,
            (spec, _) =>
            {
                calls.Add(spec.IndexName);
                return Task.CompletedTask;
            },
            shutdown.Token);

        Assert.Empty(calls);
        var line = Assert.Single(logger.Lines);
        Assert.StartsWith("Debug:", line, StringComparison.Ordinal);
        Assert.Contains("cancelled before it started", line, StringComparison.Ordinal);
        foreach (var spec in QueryStoreBackgroundIndexes.All)
        {
            Assert.Contains(spec.IndexName, line, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("(shutdown)", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShutdownDuringABuild_NamesTheIndexInProgress_AndOnlyThatOne()
    {
        using var shutdown = new CancellationTokenSource();
        var logger = new CapturingTestLogger();

        await QueryStoreBackgroundIndexes.RunDelayedAsync(
            logger,
            TimeSpan.Zero,
            new[] { Brin, Wide },
            (spec, token) =>
            {
                if (spec == Wide)
                {
                    shutdown.Cancel();
                    token.ThrowIfCancellationRequested();
                }

                return Task.CompletedTask;
            },
            shutdown.Token);

        var line = Assert.Single(logger.Lines);
        Assert.StartsWith("Information:", line, StringComparison.Ordinal);
        Assert.Contains("cancelled at shutdown", line, StringComparison.Ordinal);
        Assert.Contains("next start retries", line, StringComparison.Ordinal);
        Assert.Contains(Wide.IndexName, line, StringComparison.Ordinal);
        Assert.DoesNotContain(Brin.IndexName, line, StringComparison.Ordinal);
        Assert.DoesNotContain("(shutdown)", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnErrorRaisedWhileShuttingDown_IsInformation_NamingTheIndexInProgressTheErrorAndTheRetry()
    {
        using var shutdown = new CancellationTokenSource();
        var logger = new CapturingTestLogger();

        await QueryStoreBackgroundIndexes.RunDelayedAsync(
            logger,
            TimeSpan.Zero,
            new[] { Brin, Wide },
            (spec, _) =>
            {
                if (spec == Wide)
                {
                    shutdown.Cancel();
                    throw new InvalidOperationException("the connection closed under the build");
                }

                return Task.CompletedTask;
            },
            shutdown.Token);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        var line = Assert.Single(logger.Lines);
        Assert.StartsWith("Information:", line, StringComparison.Ordinal);
        Assert.Contains(Wide.IndexName, line, StringComparison.Ordinal);
        Assert.DoesNotContain(Brin.IndexName, line, StringComparison.Ordinal);
        Assert.Contains("stopped at shutdown", line, StringComparison.Ordinal);
        Assert.Contains(nameof(InvalidOperationException), line, StringComparison.Ordinal);
        Assert.Contains("the connection closed under the build", line, StringComparison.Ordinal);
        Assert.Contains("next start retries", line, StringComparison.Ordinal);
        Assert.DoesNotContain("(shutdown)", line, StringComparison.Ordinal);
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
