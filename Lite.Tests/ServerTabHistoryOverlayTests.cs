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
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5449: the slicer overlay of the selected Top Queries / Top Procedures row. The history rows already carry the
/// per-collection DELTA (<c>delta_*</c>), so each row's own delta is the point. The overlay used to subtract the previous
/// row's delta from each row's delta (it treated the deltas as running totals), so it drew a point only where the work
/// changed between two collections, dropped the first row, and on a store without the idle rows read different from one with them.
/// </summary>
public sealed class ServerTabHistoryOverlayTests
{
    private static readonly DateTime T0 = new(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void ProcOverlay_PlotsEachRowsOwnDelta_NotTheDifferenceFromThePreviousRow()
    {
        var history = new List<ProcedureStatsHistoryRow>
        {
            new() { CollectionTime = T0, DeltaElapsedUs = 500_000 },
            new() { CollectionTime = T0.AddMinutes(1), DeltaElapsedUs = 500_000 },
            new() { CollectionTime = T0.AddMinutes(2), DeltaElapsedUs = 300_000 },
        };

        var points = ServerTab.ComputeProcOverlayPoints(history, "TotalElapsed");

        Assert.Equal(new[] { (T0, 500.0), (T0.AddMinutes(1), 500.0), (T0.AddMinutes(2), 300.0) }, points.ToArray());
    }

    [Fact]
    public void ProcOverlay_AnIdleRowIsNotDrawn_SoBothKindsOfStoreDrawTheSame()
    {
        /* An older store keeps a row of zeros for a quiet minute; a newer one has no row. The overlay draws neither. */
        var kept = new List<ProcedureStatsHistoryRow>
        {
            new() { CollectionTime = T0, DeltaLogicalReads = 40 },
            new() { CollectionTime = T0.AddMinutes(1), DeltaLogicalReads = 0 },
            new() { CollectionTime = T0.AddMinutes(2), DeltaLogicalReads = 40 },
        };
        var left = kept.Where(r => r.DeltaLogicalReads > 0).ToList();

        Assert.Equal(
            ServerTab.ComputeProcOverlayPoints(left, "TotalReads"),
            ServerTab.ComputeProcOverlayPoints(kept, "TotalReads"));
        Assert.Equal(2, ServerTab.ComputeProcOverlayPoints(kept, "TotalReads").Count);
    }

    [Fact]
    public void QueryOverlay_PlotsEachRowsOwnDelta_NotTheDifferenceFromThePreviousRow()
    {
        var history = new List<QueryStatsHistoryRow>
        {
            new() { CollectionTime = T0, DeltaCpuUs = 2_000_000 },
            new() { CollectionTime = T0.AddMinutes(1), DeltaCpuUs = 1_000_000 },
        };

        var points = ServerTab.ComputeQueryOverlayPoints(history, "TotalCpu");

        Assert.Equal(new[] { (T0, 2000.0), (T0.AddMinutes(1), 1000.0) }, points.ToArray());
    }
}
