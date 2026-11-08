/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8146. A collector run reads the ring-buffer header and the session row, compares them with the position the previous run
/// staged, and carries what was lost as counts on the run. These tests drive <see cref="RingBufferLossAccounting.Apply"/> with
/// observations a real read produces and assert the measurements and the staged position that come out.
/// </summary>
public sealed class RingBufferLossAccountingTests
{
    private static readonly DateTime Created = new(2026, 11, 7, 6, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Now = new(2026, 11, 7, 9, 0, 0, DateTimeKind.Utc);

    private static CollectorContext MakeContext(IReadOnlyDictionary<string, string>? state = null, long? maxEvents = null) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = Now,
        Deltas = new RecordingCollectorDeltaCalculator(),
        Target = new CollectorTargetInfo(),
        State = state ?? CollectorContext.NoState,
        RingBufferMaxEvents = maxEvents ?? 1000,
    };

    private static string PriorState(long total, long held, DateTime readAt, long dropped = 0, bool truncated = false) =>
        RingBufferLoss.ToStateValue(new RingBufferPrior(total, held, dropped, Created, readAt, truncated));

    private static Dictionary<string, string> StateWith(string value) =>
        new() { [RingBufferLossAccounting.StateKey] = value };

    private static RingBufferObservation Obs(long? total, long? held, long? limit = 1000, long? dropped = 0, DateTime? created = null) =>
        new(total, held, created ?? Created, dropped, limit, Now);

    private static long? Measured(CollectorContext context, string label) =>
        context.Measurements.Where(m => m.Label == label).Select(m => (long?)m.Value).FirstOrDefault();

    [Fact]
    public void Apply_FirstShred_StagesBaselineOnly()
    {
        var context = MakeContext();

        RingBufferLossAccounting.Apply(context, Obs(30, 30), truncated: false);

        var staged = RingBufferLoss.TryParseState(context.StagedItemState[RingBufferLossAccounting.StateKey]);
        Assert.Equal(30, staged!.Value.Total);
        context.LandStagedItemState();
        Assert.Empty(context.Measurements);
    }

    [Fact]
    public void Apply_EvictionAfterPrior_MeasuresEvictedWindowAndLimit()
    {
        var context = MakeContext(StateWith(PriorState(30, 10, Now.AddSeconds(-60))));

        RingBufferLossAccounting.Apply(context, Obs(55, 10, limit: 10), truncated: false);
        context.LandStagedItemState();

        Assert.Equal(15, Measured(context, "ring_buffer_evicted"));
        Assert.Equal(60, Measured(context, "ring_buffer_loss_window_s"));
        Assert.Equal(10, Measured(context, "ring_buffer_events_limit"));
    }

    [Fact]
    public void Apply_SessionDropsAfterPrior_MeasuresDropped()
    {
        var context = MakeContext(StateWith(PriorState(30, 30, Now.AddSeconds(-60), dropped: 2)));

        RingBufferLossAccounting.Apply(context, Obs(35, 35, dropped: 9), truncated: false);
        context.LandStagedItemState();

        Assert.Equal(7, Measured(context, "xe_session_dropped"));
        Assert.Null(Measured(context, "ring_buffer_evicted"));
    }

    [Fact]
    public void Apply_RestartAfterPrior_MeasuresUnknown()
    {
        var context = MakeContext(StateWith(PriorState(500, 100, Now.AddSeconds(-60))));

        RingBufferLossAccounting.Apply(context, Obs(4, 4, created: Created.AddMinutes(30)), truncated: false);
        context.LandStagedItemState();

        Assert.Equal(1, Measured(context, "ring_buffer_loss_unknown"));
        Assert.Null(Measured(context, "ring_buffer_evicted"));
    }

    [Fact]
    public void Apply_LimitBelowSetting_FlagsOnlyWithLoss()
    {
        var state = StateWith(PriorState(30, 10, Now.AddSeconds(-60)));

        var lossy = MakeContext(state, maxEvents: 5000);
        RingBufferLossAccounting.Apply(lossy, Obs(55, 10, limit: 1000), truncated: false);
        lossy.LandStagedItemState();
        Assert.Equal(1, Measured(lossy, "ring_buffer_limit_below_setting"));

        var clean = MakeContext(state, maxEvents: 5000);
        RingBufferLossAccounting.Apply(clean, Obs(35, 35, limit: 1000), truncated: false);
        clean.LandStagedItemState();
        Assert.Empty(clean.Measurements);
    }

    [Fact]
    public void Apply_NoCountLimit_IsNeverBelowTheSetting()
    {
        var context = MakeContext(StateWith(PriorState(30, 10, Now.AddSeconds(-60))), maxEvents: 5000);

        RingBufferLossAccounting.Apply(context, Obs(55, 10, limit: 0), truncated: false);
        context.LandStagedItemState();

        Assert.Equal(15, Measured(context, "ring_buffer_evicted"));
        Assert.Null(Measured(context, "ring_buffer_limit_below_setting"));
    }

    [Fact]
    public void Apply_GatedRunWithoutPrior_StagesNothingAndMeasuresNothing()
    {
        var context = MakeContext();

        RingBufferLossAccounting.Apply(context, Obs(null, null), truncated: false);

        Assert.Empty(context.StagedItemState);
        context.LandStagedItemState();
        Assert.Empty(context.Measurements);
    }

    [Fact]
    public void Apply_SessionMissing_LeavesPriorUntouched()
    {
        var context = MakeContext(StateWith(PriorState(30, 10, Now.AddSeconds(-60))));

        RingBufferLossAccounting.Apply(context, new RingBufferObservation(null, null, null, null, null, Now), truncated: false);

        Assert.Empty(context.StagedItemState);
        context.LandStagedItemState();
        Assert.Empty(context.Measurements);
    }

    [Fact]
    public void Apply_TwoDatabasesInOneRun_SumsCountsAndKeepsLongestWindow()
    {
        var state = new Dictionary<string, string>
        {
            [RingBufferLossAccounting.KeyFor("a")] = PriorState(100, 10, Now.AddSeconds(-60)),
            [RingBufferLossAccounting.KeyFor("b")] = PriorState(100, 10, Now.AddSeconds(-300)),
        };
        var context = MakeContext(state);

        /* As the per-database loops drive it: staged state dropped before each item's read and landed after its write. */
        context.DropStagedItemState();
        context.CurrentDatabaseName = "a";
        RingBufferLossAccounting.Apply(context, Obs(115, 10), truncated: false);
        context.LandStagedItemState();
        context.DropStagedItemState();
        context.CurrentDatabaseName = "b";
        RingBufferLossAccounting.Apply(context, Obs(117, 10), truncated: false);
        context.LandStagedItemState();

        Assert.Equal(12, Measured(context, "ring_buffer_evicted"));
        Assert.Equal(300, Measured(context, "ring_buffer_loss_window_s"));
        Assert.Contains("xe_ring_position:a", context.PendingState.Keys);
        Assert.Contains("xe_ring_position:b", context.PendingState.Keys);
    }

    /* #8146: a database whose write fails keeps its prior (its staged state is dropped), so the next run counts its loss again.
       Its measurements are dropped with it, or the same events would be counted on this run's note and again on the next. */
    [Fact]
    public void Apply_TwoDatabases_OneWriteFails_OnlyTheLandedDatabaseIsMeasured()
    {
        var state = new Dictionary<string, string>
        {
            [RingBufferLossAccounting.KeyFor("a")] = PriorState(100, 10, Now.AddSeconds(-60)),
            [RingBufferLossAccounting.KeyFor("b")] = PriorState(100, 10, Now.AddSeconds(-300)),
        };
        var context = MakeContext(state);

        context.DropStagedItemState();
        context.CurrentDatabaseName = "a";
        RingBufferLossAccounting.Apply(context, Obs(115, 10), truncated: false);
        context.LandStagedItemState();

        context.DropStagedItemState();
        context.CurrentDatabaseName = "b";
        RingBufferLossAccounting.Apply(context, Obs(117, 10), truncated: false);
        context.DropStagedItemState();

        Assert.Equal(5, Measured(context, "ring_buffer_evicted"));
        Assert.Equal(60, Measured(context, "ring_buffer_loss_window_s"));
        Assert.Contains("xe_ring_position:a", context.PendingState.Keys);
        Assert.DoesNotContain("xe_ring_position:b", context.PendingState.Keys);
    }

    [Fact]
    public void KeyFor_IsThePlainKeyOrOnePerDatabase()
    {
        Assert.Equal("xe_ring_position", RingBufferLossAccounting.KeyFor(null));
        Assert.Equal("xe_ring_position:db1", RingBufferLossAccounting.KeyFor("db1"));
    }

    [Fact]
    public void IsMeasurementsOnly_LossNote_IsFalse()
    {
        Assert.False(CollectorMeasurementNote.IsMeasurementsOnly("ring_buffer_evicted=3"));
        Assert.False(CollectorMeasurementNote.IsMeasurementsOnly("xe_session_dropped=2 ring_buffer_loss_window_s=60"));
        Assert.False(CollectorMeasurementNote.IsMeasurementsOnly("ring_buffer_loss_unknown=1"));
        Assert.True(CollectorMeasurementNote.IsMeasurementsOnly("ring_buffer_events_limit=1000"));
    }

    [Fact]
    public void MeasureMax_KeepsLargest()
    {
        var context = MakeContext();

        context.MeasureMax("some_span_s", 60);
        context.MeasureMax("some_span_s", 300);
        context.MeasureMax("some_span_s", 120);

        Assert.Equal(300, Measured(context, "some_span_s"));
        Assert.Single(context.Measurements);
    }

    [Fact]
    public void MeasureMax_RejectsIllegalLabel()
    {
        var context = MakeContext();

        Assert.Throws<ArgumentException>(() => context.MeasureMax("Not A Label", 1));
        Assert.Throws<ArgumentException>(() => context.MeasureMax(CollectorMeasurementNote.RejectedLabelCount, 1));
    }

    [Fact]
    public void Labels_EqualCommonConstants()
    {
        Assert.Equal(RingBufferLossNote.EvictedLabel, RingBufferLossAccounting.EvictedMeasurement);
        Assert.Equal(RingBufferLossNote.DroppedLabel, RingBufferLossAccounting.DroppedMeasurement);
        Assert.Equal(RingBufferLossNote.UnknownLabel, RingBufferLossAccounting.UnknownMeasurement);
        Assert.Equal(RingBufferLossNote.AtLeastLabel, RingBufferLossAccounting.AtLeastMeasurement);
        Assert.Equal(RingBufferLossNote.MemoryCappedLabel, RingBufferLossAccounting.MemoryCappedMeasurement);
        Assert.Equal(RingBufferLossNote.WindowSecondsLabel, RingBufferLossAccounting.WindowSecondsMeasurement);
        Assert.Equal(RingBufferLossNote.LimitLabel, RingBufferLossAccounting.LimitMeasurement);
        Assert.Equal(RingBufferLossNote.LimitBelowSettingLabel, RingBufferLossAccounting.LimitBelowSettingMeasurement);
    }
}
