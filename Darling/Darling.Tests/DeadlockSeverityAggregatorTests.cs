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
using System.Text;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The deadlock-severity aggregation at its NEW home in Common (#2484), reached directly rather than through
/// the viewer.
///
/// <para><see cref="DeadlockSeverityAggregationTests"/> already covers the arithmetic in depth, and because
/// the viewer now delegates, those pins exercise this implementation — which is the real evidence that the
/// move changed nothing. What they cannot cover is the Common entry point itself: they all call the viewer's
/// wrapper, so if that wrapper were ever removed or re-pointed, the shared function the headless service
/// depends on would have no test of its own. That is what this file is for.</para>
/// </summary>
public sealed class DeadlockSeverityAggregatorTests
{
    /// <summary>The proven fixture shape from the viewer's suite: victims are named in the victim-list, not
    /// by an attribute on the process.</summary>
    private static string BuildGraph(params (string Id, int Spid, long WaitTime, bool IsVictim)[] processes)
    {
        var sb = new StringBuilder();
        sb.Append("<deadlock><victim-list>");
        foreach (var p in processes.Where(p => p.IsVictim))
            sb.Append($"<victimProcess id=\"{p.Id}\"/>");
        sb.Append("</victim-list><process-list>");
        foreach (var p in processes)
            sb.Append($"<process id=\"{p.Id}\" spid=\"{p.Spid}\" waittime=\"{p.WaitTime}\"><inputbuf>x</inputbuf></process>");
        sb.Append("</process-list></deadlock>");
        return sb.ToString();
    }

    [Fact]
    public void TheSharedEntryPoint_SumsEveryProcessNotOnlyVictims_AndBucketsByTheMinute()
    {
        var minuteA = new DateTime(2026, 6, 21, 6, 51, 30, DateTimeKind.Unspecified);
        var minuteAAgain = new DateTime(2026, 6, 21, 6, 51, 5, DateTimeKind.Unspecified);
        var minuteB = new DateTime(2026, 6, 21, 6, 53, 0, DateTimeKind.Unspecified);

        /* Fed out of order on purpose: the dictionary rollup does not preserve read order. */
        var graphs = new List<(DateTime? DeadlockTime, string? Xml)>
        {
            (minuteB, BuildGraph(("p1", 55, 100, true))),
            (minuteA, BuildGraph(("p2", 66, 1000, true), ("p3", 77, 3000, false))),
            (minuteAAgain, BuildGraph(("p4", 88, 500, true))),
        };

        var points = DeadlockSeverityAggregator.Aggregate(graphs);

        Assert.Equal(2, points.Count);
        Assert.True(points[0].Time < points[1].Time, "buckets must come back in time order");

        var first = points[0];

        /*
            4500, not 1500. Total wait sums EVERY process in the graphs, victims and blockers alike — the
            Dashboard analyzer's semantics — which is why victim_count is a separate field rather than
            something derivable from the total.
        */
        Assert.Equal(4500, first.TotalWaitMs);
        Assert.Equal(2, first.VictimCount);
        Assert.Equal(3000, first.MaxWaitMs);

        /* Process-weighted: 4500 over the three processes in the bucket, not over the two graphs. */
        Assert.Equal(1500.0, first.AvgWaitMs);
    }

    [Fact]
    public void AGraphThatCannotBeParsed_ContributesNothing_RatherThanAZeroBucket()
    {
        var at = new DateTime(2026, 6, 21, 6, 51, 0, DateTimeKind.Unspecified);
        var graphs = new List<(DateTime? DeadlockTime, string? Xml)>
        {
            (at, "<deadlock></deadlock>"),
            (at, null),
            (at, "not xml at all"),

            /* A graph with no time cannot be placed on the axis at all. */
            (null, BuildGraph(("p1", 55, 100, true))),
        };

        /*
            An empty bucket would read as "a deadlock happened and cost nothing", which is worse than
            silence: absent evidence is not evidence of calm.
        */
        Assert.Empty(DeadlockSeverityAggregator.Aggregate(graphs));
    }
}
