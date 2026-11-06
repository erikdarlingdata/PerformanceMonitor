/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7782: pg_statement_stats places a counter restart against a pass window that moves on the target's own clock
/// (<c>PreviousPass</c> under <c>pg_statement_stats_target_clock</c>), not the collection time, so the baseline snapshots
/// cannot put it back. <see cref="CollectorDeltaCalculator.CapturePass"/> captures it before the run rolls it forward,
/// guarded on the time the run writes into it, and a rollback restores what stood before the run.
/// </summary>
public sealed class DeltaBaselinePassWindowRollbackTests
{
    private const string Group = "pg_statement_stats_target_clock";

    private static readonly DateTime T1 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime T2 = T1.AddMinutes(1);
    private static readonly DateTime T3 = T1.AddMinutes(2);

    /// <summary>Run one sets the window. Run two rolls it forward and is abandoned. Run three must see run one's time as the previous pass.</summary>
    [Fact]
    public void AnAbandonedRun_LeavesTheNextRunPlacingAgainstTheWindowThatStoodBeforeIt()
    {
        var deltas = new CollectorDeltaCalculator();
        Assert.Null(deltas.PreviousPass(7, Group, T1));

        var snapshot = deltas.CapturePass(7, Group, T2);
        Assert.Equal(T1, deltas.PreviousPass(7, Group, T2));
        snapshot!.Restore();

        Assert.Equal(T1, deltas.PreviousPass(7, Group, T3));
    }

    /// <summary>A window a later run has already moved past is not touched by an earlier run's restore.</summary>
    [Fact]
    public void ARestoreAfterALaterRun_LeavesTheLaterRunsWindowAlone()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.PreviousPass(7, Group, T1);
        var snapshot = deltas.CapturePass(7, Group, T2);
        deltas.PreviousPass(7, Group, T2);
        deltas.PreviousPass(7, Group, T3);

        snapshot!.Restore();

        Assert.Equal(T3, deltas.PreviousPass(7, Group, T3.AddMinutes(1)));
    }

    /// <summary>A window the run itself created stays, as a key the run first saw does, so the next run is not a first pass again.</summary>
    [Fact]
    public void AWindowTheRunCreated_IsLeftInPlace()
    {
        var deltas = new CollectorDeltaCalculator();
        var snapshot = deltas.CapturePass(7, Group, T2);
        Assert.Null(deltas.PreviousPass(7, Group, T2));

        snapshot!.Restore();

        Assert.Equal(T2, deltas.PreviousPass(7, Group, T3));
    }
}
