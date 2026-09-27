/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A pure C# table pin for <see cref="WaitStatisticsArtifact.IsIsolatedSingleSampleArtifact"/> (#4476): the
/// four field-evidence spikes from the type's own doc comment, three near-large values that stay under the
/// neighbour-ratio floor or the absolute floor, a missing neighbour on each side, the wrong object, the
/// wrong type id, and a named-instance object name — the same rows the live SQL equivalence pin
/// (<c>WaitStatisticsArtifactSqlEquivalenceLiveTests</c>) sends through the SQL twin.
/// </summary>
public sealed class WaitStatisticsArtifactTests
{
    private const PerfmonCounterKind Gauge = PerfmonCounterKind.Gauge;
    private const PerfmonCounterKind Other = PerfmonCounterKind.Other;

    [Theory]
    /* The four field-evidence shapes (object suffix ObjectName, kind Gauge) — isolated spikes. */
    [InlineData("SQLServer:Wait Statistics", Gauge, 318L, 214_396_451L, 1_021L, true)]
    [InlineData("SQLServer:Wait Statistics", Gauge, 612L, 90_893_408L, 1_021L, true)]
    [InlineData("SQLServer:Wait Statistics", Gauge, 40L, 32_713_827L, 878L, true)]
    [InlineData("SQLServer:Wait Statistics", Gauge, 816L, 127_703_309L, 267L, true)]
    /* Large but not isolated by ratio, or large but under the absolute floor. */
    [InlineData("SQLServer:Wait Statistics", Gauge, 1_000L, 5_000_000L, 5_000_100L, false)]
    [InlineData("SQLServer:Wait Statistics", Gauge, 300L, 200_000L, 300L, false)]
    [InlineData("SQLServer:Wait Statistics", Gauge, 5L, 999_999L, 5L, false)]
    /* A missing neighbour on either side is never judged, however large the ratio. */
    [InlineData("SQLServer:Wait Statistics", Gauge, null, 214_396_451L, 1_021L, false)]
    [InlineData("SQLServer:Wait Statistics", Gauge, 318L, 214_396_451L, null, false)]
    /* Wrong object: the same field shape on a different object never matches. */
    [InlineData("SQLServer:Buffer Manager", Gauge, 318L, 214_396_451L, 1_021L, false)]
    /* Wrong type: only a stored GAUGE type qualifies — a rate row (PERF_COUNTER_BULK_COUNT, 272696576)
       carrying its own large cumulative count is not itself evidence of this artifact. */
    [InlineData("SQLServer:Wait Statistics", Other, 318L, 214_396_451L, 1_021L, false)]
    /* A named instance's object name still matches by suffix. */
    [InlineData("MSSQL$INST1:Wait Statistics", Gauge, 318L, 214_396_451L, 1_021L, true)]
    public void IsIsolatedSingleSampleArtifact_MatchesTheFieldEvidenceShapes(
        string objectName, PerfmonCounterKind kind, long? prev, long value, long? next, bool expected)
    {
        Assert.Equal(expected, WaitStatisticsArtifact.IsIsolatedSingleSampleArtifact(objectName, kind, prev, value, next));
    }
}
