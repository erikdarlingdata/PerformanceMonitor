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
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7782, through pg_statement_stats' production path: the real <see cref="PgStatementStatsCollector"/> read over a result set
/// shaped like its query, then its real <c>WritePayload</c>, rolled back the way the host does it when the store write is
/// abandoned. The family places a counter restart, and credits an entry created since the previous run, against a pass window on
/// the target's own clock (<c>stats_since</c> against the previous run's <c>target_now</c>, #4428 and #7189), so a rollback has
/// to put that window back with the baselines. An entry the abandoned run first saw keeps the baseline the run gave it unless
/// <c>stats_since</c> says the entry was created since the previous run, in which case the baseline goes and the next run
/// credits the whole count over the restored gap.
/// </summary>
public sealed class PgStatementStatsAbandonedWriteBaselineTests
{
    private static readonly DateTime T0 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime T1 = T0.AddMinutes(1);
    private static readonly DateTime T2 = T0.AddMinutes(2);

    /// <summary>An entry that existed long before the first run, so no run credits it as created in a gap.</summary>
    private static readonly DateTime LongBefore = T0.AddHours(-1);

    private const long QueryA = 1001;
    private const long QueryB = 1002;

    /// <summary>The delta_calls and sample_interval_seconds one row's write stored.</summary>
    private sealed record Stored(long Delta, int Interval);

    /// <summary>
    /// One run at <paramref name="targetNow"/>, which is also its collection time: the read, then the write of every row it
    /// returned. The recorded payloads carry each row's stored delta_calls and sample_interval_seconds, by query id.
    /// </summary>
    private static async Task<(IBaselineRollback? Rows, Dictionary<long, Stored> Stored)> RunAsync(
        CollectorDeltaCalculator deltas, DateTime targetNow, params (long QueryId, long Calls, DateTime? StatsSince)[] entries)
    {
        var collector = CollectorCatalog.All.OfType<PgStatementStatsCollector>().Single();
        var context = DeltaFamilyAbandonedWriteBaselineTests.Context(deltas, targetNow);

        using var table = PgMidReadFaultBaselineRollbackTests.StatementStatsTable(targetNow, entries);
        using var reader = table.CreateDataReader();
        var rows = await collector.ReadAsync(reader, context, CancellationToken.None);

        var stored = new Dictionary<long, Stored>();
        foreach (var row in rows)
        {
            var writer = new RecordingRowWriter();
            collector.WritePayload(row, writer, context);
            stored[row.QueryId] = new Stored((long)writer.Values[27]!, (int)writer.Values[30]!);
        }

        return (rows as IBaselineRollback, stored);
    }

    /// <summary>
    /// What the real <c>WritePayload</c> hands the store for one row read from a result: <c>sample_interval_seconds</c> (ordinal 30) is
    /// the interval the read measured, and <c>database_name</c> (ordinal 31, #7731) is the name the read carried for that row, in the
    /// two positions the table's columns stand in. A row whose database had no name writes NULL there, not an empty string.
    /// </summary>
    [Fact]
    public async Task TheWriter_StoresTheMeasuredIntervalAndTheDatabaseName_InTheLastTwoPayloadColumns()
    {
        var collector = CollectorCatalog.All.OfType<PgStatementStatsCollector>().Single();
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, (QueryA, 100, LongBefore), (QueryB, 100, LongBefore));

        var context = DeltaFamilyAbandonedWriteBaselineTests.Context(deltas, T1);
        using var table = PgMidReadFaultBaselineRollbackTests.StatementStatsTable(T1, (QueryA, 130, (DateTime?)LongBefore), (QueryB, 140, (DateTime?)LongBefore));
        table.Rows[1]["database_name"] = DBNull.Value;
        using var reader = table.CreateDataReader();
        var rows = await collector.ReadAsync(reader, context, CancellationToken.None);

        var stored = new Dictionary<long, RecordingRowWriter>();
        foreach (var row in rows)
        {
            var writer = new RecordingRowWriter();
            collector.WritePayload(row, writer, context);
            stored[row.QueryId] = writer;
        }

        Assert.Equal(collector.PayloadColumns.Count, stored[QueryA].Values.Count);
        Assert.Equal("sample_interval_seconds", collector.PayloadColumns[30].Name);
        Assert.Equal("database_name", collector.PayloadColumns[31].Name);
        Assert.Equal(60, stored[QueryA].Values[30]);
        Assert.Equal("pm_database", stored[QueryA].Values[31]);
        Assert.Equal(60, stored[QueryB].Values[30]);
        Assert.Null(stored[QueryB].Values[31]);
    }

    /// <summary>
    /// Run one baselines the entry. Run two sees its calls fall, with a <c>stats_since</c> inside the gap since run one, so the
    /// restart is credited over that gap; its write is then abandoned and rolled back. Run three must place the same restart
    /// against the window that stood before run two: the whole count over the 120 s since run one, not (0, 0) against run two's
    /// target_now, and not an ordinary delta from run two's baseline.
    /// </summary>
    [Fact]
    public async Task ARestartInTheAbandonedRun_IsCreditedOverTheTargetClockGapThatStoodBeforeIt()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, (QueryA, 100, LongBefore));

        var restartedAt = T0.AddSeconds(30);
        var abandoned = await RunAsync(deltas, T1, (QueryA, 30, restartedAt));
        Assert.Equal(new Stored(30, 60), abandoned.Stored[QueryA]);
        abandoned.Rows?.RollBackBaselines();

        var next = await RunAsync(deltas, T2, (QueryA, 60, restartedAt));
        Assert.Equal(new Stored(60, 120), next.Stored[QueryA]);
    }

    /// <summary>
    /// An entry created between run one and run two (<c>stats_since</c> after run one's target_now) is first seen by the
    /// abandoned run, which credits its whole count over the gap. Its baseline is removed on rollback rather than kept: kept, run
    /// three would difference from the abandoned run's value and lose the count before it. Removed, run three finds no baseline,
    /// and the restored target-clock window credits the whole count over the 120 s since run one.
    /// </summary>
    [Fact]
    public async Task AKeyCreatedSinceThePreviousRun_LosesTheAbandonedRunsBaseline_AndIsCreditedInFullOnTheNextRun()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, (QueryA, 100, LongBefore));

        var createdAt = T0.AddSeconds(30);
        var abandoned = await RunAsync(deltas, T1, (QueryA, 130, LongBefore), (QueryB, 10, createdAt));
        Assert.Equal(new Stored(10, 60), abandoned.Stored[QueryB]);
        abandoned.Rows?.RollBackBaselines();

        var next = await RunAsync(deltas, T2, (QueryA, 160, LongBefore), (QueryB, 40, createdAt));
        Assert.Equal(new Stored(60, 120), next.Stored[QueryA]);
        Assert.Equal(new Stored(40, 120), next.Stored[QueryB]);
    }

    /// <summary>
    /// An entry first seen by the abandoned run whose <c>stats_since</c> is at or before run one's target_now, or absent (below
    /// PostgreSQL 17), says nothing about a count accrued since run one, so its baseline is kept, and run three's delta is one
    /// interval, as it is when nothing fails.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-3600)]
    [InlineData(null)]
    public async Task AKeyNotCreatedSinceThePreviousRun_KeepsTheBaselineTheAbandonedRunGaveIt(int? statsSinceOffsetSeconds)
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, (QueryA, 100, LongBefore));

        var statsSince = statsSinceOffsetSeconds.HasValue ? T0.AddSeconds(statsSinceOffsetSeconds.Value) : (DateTime?)null;
        var abandoned = await RunAsync(deltas, T1, (QueryA, 130, LongBefore), (QueryB, 10, statsSince));
        Assert.Equal(new Stored(0, 0), abandoned.Stored[QueryB]);
        abandoned.Rows?.RollBackBaselines();

        var next = await RunAsync(deltas, T2, (QueryA, 160, LongBefore), (QueryB, 40, statsSince));
        Assert.Equal(new Stored(60, 120), next.Stored[QueryA]);
        Assert.Equal(new Stored(30, 60), next.Stored[QueryB]);
    }
}
