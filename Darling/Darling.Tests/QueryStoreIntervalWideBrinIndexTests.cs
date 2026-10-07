/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605's BRIN index ensure, without a store: the build-or-skip decision (PostgreSQL 16 floor, hypertable
/// guard) and the source shape of the SQL and of the worker's scheduling.
/// </summary>
public sealed class QueryStoreIntervalWideBrinIndexTests
{
    [Theory]
    [InlineData(160000, false, QueryStoreIntervalWideBrinIndex.BrinAction.Build)]
    [InlineData(180006, false, QueryStoreIntervalWideBrinIndex.BrinAction.Build)]
    [InlineData(159999, false, QueryStoreIntervalWideBrinIndex.BrinAction.SkipServerVersionBelowSixteen)]
    [InlineData(150013, false, QueryStoreIntervalWideBrinIndex.BrinAction.SkipServerVersionBelowSixteen)]
    [InlineData(140000, false, QueryStoreIntervalWideBrinIndex.BrinAction.SkipServerVersionBelowSixteen)]
    [InlineData(180006, true, QueryStoreIntervalWideBrinIndex.BrinAction.SkipHypertable)]
    [InlineData(160000, true, QueryStoreIntervalWideBrinIndex.BrinAction.SkipHypertable)]
    [InlineData(150000, true, QueryStoreIntervalWideBrinIndex.BrinAction.SkipServerVersionBelowSixteen)]
    public void Decide_BuildsOnlyOnSixteenOrNewer_AndNeverOnAHypertable(
        int serverVersionNum, bool hypertable, QueryStoreIntervalWideBrinIndex.BrinAction expected)
    {
        Assert.Equal(expected, QueryStoreIntervalWideBrinIndex.Decide(serverVersionNum, hypertable).Action);
    }

    [Fact]
    public void Decide_NamesItsReasonForEverySkip()
    {
        var oldServer = QueryStoreIntervalWideBrinIndex.Decide(150000, false).Reason;
        Assert.Contains("150000", oldServer);
        Assert.Contains("non-HOT below PG 16", oldServer);

        var hypertable = QueryStoreIntervalWideBrinIndex.Decide(180000, true).Reason;
        Assert.Contains("hypertable", hypertable);
        Assert.Contains("CONCURRENTLY", hypertable);

        Assert.Empty(QueryStoreIntervalWideBrinIndex.Decide(180000, false).Reason);
    }

    [Fact]
    public void TheBuildSql_IsConcurrent_Idempotent_AndABrinOnCollectionTime()
    {
        var sql = QueryStoreIntervalWideBrinIndex.CreateSql;
        Assert.Contains("CREATE INDEX CONCURRENTLY IF NOT EXISTS", sql);
        Assert.Contains("USING brin (collection_time)", sql);
        Assert.Contains("autosummarize = on", sql);
        Assert.Contains("ix_query_store_interval_wide_collection_time_brin", sql);
        Assert.Contains("DROP INDEX CONCURRENTLY IF EXISTS", QueryStoreIntervalWideBrinIndex.DropSql);
    }

    [Fact]
    public void TheEnsure_ReadsValidityFirst_AndGuardsHypertablesThroughAViewCheck()
    {
        /* The machinery moved to QueryStoreBackgroundIndexes (#4952) so the two new btrees share it; the pins moved with it. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "QueryStoreBackgroundIndexes.cs");

        /* The validity read is what tells an INVALID leftover from a good index; IF NOT EXISTS alone keeps both. */
        Assert.Contains("i.indisvalid", source);
        Assert.Contains("indexValid == false", source);

        /* The hypertable catalog view is queried only after to_regclass says it exists (no TimescaleDB, no view). */
        Assert.Contains("to_regclass('timescaledb_information.hypertables')", source);
        Assert.Contains("if (hasHypertableView)", source);
        Assert.Contains("IndexAction.SkipHypertable", source);
        Assert.Contains("LogWarning", source);
    }

    [Fact]
    public void TheWorker_SchedulesTheEnsureAfterMigrations_WithoutAwaitingItOnTheStartupPath()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs").Replace("\r\n", "\n");

        var migrate = source.IndexOf("PgMigrations.MigrateAsync(migrateConnection", System.StringComparison.Ordinal);
        var launch = source.IndexOf("var queryStoreIndexes = QueryStoreBackgroundIndexes.RunDelayedAsync(", System.StringComparison.Ordinal);
        var drain = source.IndexOf("await queryStoreIndexes;", System.StringComparison.Ordinal);
        var loopStop = source.IndexOf("PerformanceMonitor Darling collection loop stopped", System.StringComparison.Ordinal);

        Assert.True(migrate > 0 && launch > migrate, "the ensure must launch after migrations");
        Assert.True(drain > launch && drain < loopStop, "the ensure is drained only at shutdown, after the collection loop");
        Assert.Single(Regex.Matches(source, @"await queryStoreIndexes;"));
        Assert.Contains("QueryStoreBackgroundIndexes.StartDelay", source);
    }

    [Fact]
    public async Task RunDelayed_CancelledDuringTheDelay_LogsAtDebug_NotInformation()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Timeout=1");
        var logger = new DarlingSelfAlertTests.CapturingLogger();
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(200));

        await QueryStoreBackgroundIndexes.RunDelayedAsync(dataSource, logger, TimeSpan.FromMinutes(20), QueryStoreBackgroundIndexes.All, cts.Token);

        var entry = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Debug, entry.Level);
        Assert.Contains("cancelled before it started", entry.Message);
    }

    [Fact]
    public void RunDelayed_KeepsTheInformationLineOnlyForACancelAfterTheDelay()
    {
        /* A cancel that lands after the delay needs a connection held open mid-handshake; Npgsql then may report
           the stop as its own exception, so that branch is pinned on the source: the flag is set right after the
           delay, the Debug branch is filtered on it, and the Information branch is the unfiltered catch. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "QueryStoreBackgroundIndexes.cs")
            .Replace("\r\n", "\n");

        var delay = source.IndexOf("await Task.Delay(delay, cancellationToken)", System.StringComparison.Ordinal);
        var flag = source.IndexOf("delayFinished = true;", System.StringComparison.Ordinal);
        var debugBranch = source.IndexOf("catch (OperationCanceledException) when (!delayFinished)", System.StringComparison.Ordinal);
        var infoBranch = source.IndexOf("catch (OperationCanceledException)\n", System.StringComparison.Ordinal);

        Assert.True(delay > 0 && flag > delay, "the flag must be set after the delay returns");
        Assert.True(debugBranch > flag && infoBranch > debugBranch, "the filtered Debug catch comes before the Information catch");
        Assert.Contains("logger.LogDebug(", source.Substring(debugBranch, infoBranch - debugBranch));
        Assert.Contains("logger.LogInformation(", source.Substring(infoBranch, 300));
    }

    [Fact]
    public void NoMigrationRung_BuildsABtreeOnTheWideTablesCollectionTime()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var indexOnWide = new Regex(
            @"CREATE\s+(?:UNIQUE\s+)?INDEX[^;]*?ON\s+collect\.query_store_interval_wide\s*\((?<cols>[^)]*)\)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);

        foreach (Match match in indexOnWide.Matches(source))
        {
            Assert.DoesNotContain("collection_time", match.Groups["cols"].Value);
        }

        Assert.True(indexOnWide.Matches(source).Count >= 1, "the scan found no index on the wide table; the pin reads nothing");
    }
}
