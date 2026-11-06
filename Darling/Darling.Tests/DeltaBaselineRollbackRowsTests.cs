/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared rollback the nine delta families return from their reads. Six advance a row's baselines in WritePayload, as
/// the COPY writes it, and capture every key at the end of the read; three difference each row as the reader yields it and
/// capture each key just before its first delta call. Either way a store write that fails before the rows are stored has
/// advanced baselines for rows it never stored; the host rolls the rows back, and the next run's delta spans both intervals.
/// These drive the real calculator the way a run does, under wait_stats' own groups and key.
/// </summary>
public sealed class DeltaBaselineRollbackRowsTests
{
    private static readonly DateTime T0 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);

    private static readonly string[] Groups = { "wait_stats_tasks", "wait_stats_time", "wait_stats_signal" };

    private const string Key = "PAGEIOLATCH_SH";

    private static CollectorContext Context(CollectorDeltaCalculator deltas, DateTime at) => new()
    {
        ServerId = 7,
        ServerName = "test-server",
        CollectionTime = at,
        Deltas = deltas,
        Target = new CollectorTargetInfo(),
    };

    /// <summary>The write's per-family delta calls for one key; the first family's result is what a test reads.</summary>
    private static (long Delta, int Interval) Advance(CollectorContext context, string key, long value)
    {
        long delta = 0;
        var interval = 0;
        foreach (var group in Groups)
        {
            var d = context.Deltas.CalculateDeltaWithInterval(
                context.ServerId, group, key, value, out var i, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
            if (group == Groups[0])
            {
                delta = d;
                interval = i;
            }
        }

        return (delta, interval);
    }

    /// <summary>A write-time family's run: the read captures every key, then the write advances each key's families.</summary>
    private static (List<string> Rows, long Delta, int Interval) Run(CollectorDeltaCalculator deltas, DateTime at, long value, params string[] keys)
    {
        var context = Context(deltas, at);
        var rows = DeltaBaselineRollbackRows.Capture(new List<string>(keys), context, Groups, k => k);

        (long Delta, int Interval) first = default;
        foreach (var key in keys)
        {
            var result = Advance(context, key, value);
            if (key == keys[0])
            {
                first = result;
            }
        }

        return (rows, first.Delta, first.Interval);
    }

    /// <summary>A drain-loop family's run: each key is captured as the reader yields it, right before its own delta calls.</summary>
    private static (List<string> Rows, Dictionary<string, (long Delta, int Interval)> Deltas) LazyRun(
        CollectorDeltaCalculator deltas, DateTime at, params (string Key, long Value)[] rows)
    {
        var context = Context(deltas, at);
        var kept = new DeltaBaselineRollbackRows<string>(context, Groups);
        var results = new Dictionary<string, (long, int)>(StringComparer.Ordinal);
        foreach (var (key, value) in rows)
        {
            kept.CaptureKey(key);
            results[key] = Advance(context, key, value);
            kept.Add(key);
        }

        return (kept, results);
    }

    /// <summary>
    /// Run one sets the baselines. Run two's write fails after advancing them, so nothing is stored and the host rolls the rows
    /// back. Run three stores a delta that covers both intervals, over the real span since run one.
    /// </summary>
    [Fact]
    public void AnAbandonedWrite_LeavesTheNextRunsDeltaSpanningBothIntervals()
    {
        var deltas = new CollectorDeltaCalculator();
        Run(deltas, T0, 100, Key);

        var abandoned = Run(deltas, T0.AddMinutes(1), 130, Key);
        Assert.Equal(30, abandoned.Delta);
        ((IBaselineRollback)abandoned.Rows).RollBackBaselines();

        var next = Run(deltas, T0.AddMinutes(2), 160, Key);
        Assert.Equal(60, next.Delta);
        Assert.Equal(120, next.Interval);
    }

    /// <summary>The same three runs with no rollback: the abandoned interval is the loss.</summary>
    [Fact]
    public void WithoutTheRollback_TheAbandonedIntervalIsLost()
    {
        var deltas = new CollectorDeltaCalculator();
        Run(deltas, T0, 100, Key);
        Run(deltas, T0.AddMinutes(1), 130, Key);

        var next = Run(deltas, T0.AddMinutes(2), 160, Key);
        Assert.Equal(30, next.Delta);
        Assert.Equal(60, next.Interval);
    }

    /// <summary>A key first seen in the abandoned run keeps the baseline the run gave it, so its next delta is one interval, as it is when nothing fails.</summary>
    [Fact]
    public void AKeyNewInTheAbandonedRun_KeepsTheBaselineTheRunGaveIt()
    {
        var deltas = new CollectorDeltaCalculator();
        Run(deltas, T0, 100, Key);
        var abandoned = Run(deltas, T0.AddMinutes(1), 130, Key, "LCK_M_X");
        ((IBaselineRollback)abandoned.Rows).RollBackBaselines();

        var next = Run(deltas, T0.AddMinutes(2), 160, "LCK_M_X", Key);
        Assert.Equal(30, next.Delta);
        Assert.Equal(60, next.Interval);
    }

    /// <summary>A second rollback does nothing: the snapshot is spent, and a later run's baselines are not disturbed.</summary>
    [Fact]
    public void RollingBackTwice_DoesNotTouchTheNextRunsBaselines()
    {
        var deltas = new CollectorDeltaCalculator();
        Run(deltas, T0, 100, Key);
        var abandoned = Run(deltas, T0.AddMinutes(1), 130, Key);
        ((IBaselineRollback)abandoned.Rows).RollBackBaselines();

        Run(deltas, T0.AddMinutes(2), 160, Key);
        ((IBaselineRollback)abandoned.Rows).RollBackBaselines();

        var next = Run(deltas, T0.AddMinutes(3), 170, Key);
        Assert.Equal(10, next.Delta);
        Assert.Equal(60, next.Interval);
    }

    /// <summary>
    /// The drain-loop shape: a key captured mid-drain, after an earlier key's delta calls already advanced the run's pass
    /// window, is restored to its pre-run baseline, and a key with no baseline before the run keeps the baseline it gave it. The
    /// next run's delta spans both intervals for the first key and one interval for the new one.
    /// </summary>
    [Fact]
    public void AKeyCapturedMidDrain_IsRestored_AndANewOneKeepsItsBaseline()
    {
        var deltas = new CollectorDeltaCalculator();
        LazyRun(deltas, T0, ("LCK_M_X", 10), (Key, 100));

        var abandoned = LazyRun(deltas, T0.AddMinutes(1), ("LCK_M_X", 20), (Key, 130), ("SOS_SCHEDULER_YIELD", 50));
        Assert.Equal((30L, 60), abandoned.Deltas[Key]);
        ((IBaselineRollback)abandoned.Rows).RollBackBaselines();

        var next = LazyRun(deltas, T0.AddMinutes(2), ("LCK_M_X", 30), (Key, 160), ("SOS_SCHEDULER_YIELD", 90));
        Assert.Equal((20L, 120), next.Deltas["LCK_M_X"]);
        Assert.Equal((60L, 120), next.Deltas[Key]);
        Assert.Equal((40L, 60), next.Deltas["SOS_SCHEDULER_YIELD"]);
    }
}
