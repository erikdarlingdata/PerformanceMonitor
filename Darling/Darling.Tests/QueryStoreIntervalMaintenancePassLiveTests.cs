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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// The hourly partition-maintenance pass (#5571), <see cref="QueryStoreIntervalPartitions.RunMaintenancePassAsync(NpgsqlConnection, DateTime, ILogger, System.Threading.CancellationToken)"/>,
/// on a real store: one table's failure never stops the other table or the pass, a lock timeout is "retry next hour" and
/// not a failure, and a table that is not promoted yet is left alone. The failing table here is a real one that
/// PostgreSQL refuses: a partitioned parent whose partition key is an integer, so the step's <c>FOR VALUES FROM ('2026-...')</c>
/// is an invalid-input error.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreIntervalMaintenancePassLiveTests
{
    private static readonly QueryStoreIntervalPartitions.IntervalTable Broken = new("zz_broken_interval", 9);

    /* A parent that reads as promoted (partitioned, no unbounded legacy) and fails the moment create-ahead names a day. */
    private static Task CreateBrokenParentAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct) =>
        ExecAsync(connection, "CREATE TABLE collect.zz_broken_interval (id integer NOT NULL, first_execution_time timestamp NOT NULL) PARTITION BY RANGE (id)", ct);

    private static IReadOnlyList<QueryStoreIntervalPartitions.IntervalTable> Order(params QueryStoreIntervalPartitions.IntervalTable[] tables) => tables;

    [Fact]
    public async Task ABrokenTableFirst_DoesNotStopTheNext_AndIsCounted()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await CreateBrokenParentAsync(connection, ct);
        await PromoteAsync(connection, Wide, Now.AddDays(-5), logger, ct);
        var later = Now.AddDays(2);

        var pass = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, Order(Broken, Wide), later, logger, ct);

        Assert.Equal(1, pass.Failed);
        Assert.True(pass.Changed > 0, "the wide table still got its days ahead");
        Assert.True(await ExistsAsync(connection, Wide.DayPartition(QueryStoreIntervalPartitions.DayStart(later).AddDays(QueryStoreIntervalPartitions.DaysAhead)), ct));
        Assert.True(logger.Lines.Any(l => l.Level == LogLevel.Warning && l.Message.Contains(Broken.Parent, StringComparison.Ordinal) && l.Message.Contains("maintenance failed", StringComparison.Ordinal)), logger.Dump());
    }

    [Fact]
    public async Task ABrokenTableLast_DoesNotUndoTheOnesBeforeIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await CreateBrokenParentAsync(connection, ct);
        await PromoteAsync(connection, Latest, Now.AddDays(-5), logger, ct);
        var later = Now.AddDays(2);

        var pass = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, Order(Latest, Broken), later, logger, ct);

        Assert.Equal(1, pass.Failed);
        Assert.True(pass.Changed > 0);
        Assert.True(await ExistsAsync(connection, Latest.DayPartition(QueryStoreIntervalPartitions.DayStart(later).AddDays(QueryStoreIntervalPartitions.DaysAhead)), ct));
    }

    [Fact]
    public async Task ATableThatIsNotPromotedYet_IsArmedButNotPartitionedAndIsNotAFailure()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        /* Wide is promoted, latest is not (the migrated shape: the legacy table is still the unbounded partition). */
        await PromoteAsync(connection, Wide, Now.AddDays(-5), logger, ct);
        var latestBefore = (await PartitionNamesAsync(connection, Latest, ct)).OrderBy(n => n, StringComparer.Ordinal).ToList();

        var pass = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, Now.AddDays(2), logger, ct);

        Assert.Equal(0, pass.Failed);
        Assert.True(pass.Changed > 0);
        Assert.Equal(latestBefore, (await PartitionNamesAsync(connection, Latest, ct)).OrderBy(n => n, StringComparer.Ordinal).ToList());
        /* #5571 review H1: the hourly pass arms the legacy CHECK of a table that is not promoted (it does no VALIDATE). */
        var latestState = await QueryStoreIntervalPartitions.ReadStateAsync(connection, Latest, ct);
        Assert.True(latestState.CheckPresent);
        Assert.False(latestState.CheckValid);
        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    [Fact]
    public async Task ALockTimeoutOnOneTable_IsNotAFailure_AndTheOtherTableStillRuns_ThenTheNextPassFinishes()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var blocker = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        /* Wide: legacy bounded at S = 9-28 and expired at Now, so a drop is due. Latest: promoted recently, with only create-ahead work. */
        await PromoteAsync(connection, Wide, Now.AddDays(-12), logger, ct);
        await PromoteAsync(connection, Latest, Now.AddDays(-5), logger, ct);
        var newestLatestDay = QueryStoreIntervalPartitions.DayStart(Now).AddDays(QueryStoreIntervalPartitions.DaysAhead);

        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE {Wide.Parent} IN ACCESS SHARE MODE", ct);
            var blocked = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, Now, logger, ct);

            Assert.Equal(0, blocked.Failed);                                   /* a lock timeout is RetryLater, not a failure */
            Assert.True(await ExistsAsync(connection, Wide.Legacy, ct));       /* the drop gave up */
            Assert.True(await ExistsAsync(connection, Latest.DayPartition(newestLatestDay), ct), "the other table still created its days");
            await hold.RollbackAsync(ct);
        }

        var next = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, Now, logger, ct);
        Assert.Equal(0, next.Failed);
        Assert.True(next.Changed > 0);
        Assert.False(await ExistsAsync(connection, Wide.Legacy, ct), "the next pass drops the expired legacy table");
    }
}
