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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9883: a fleet cell that reads raw on a source the hourly-raw-edges route serves says why. The router names the first gate that
/// failed, and the runner notes that word (or the guard's failure) as the read's source reason, so the slow-read log shows the path
/// and the reason instead of a bare "raw". Pure decision logic, no database.
/// </summary>
public sealed class ComposeHourlyEdgesRefusalTests
{
    private static readonly DateTime Now = new(2026, 7, 23, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Start24h = Now.AddHours(-24);

    private static PanelPlan Plan(
        PanelMode mode = PanelMode.TimeSeries, ComposeTimeBucket bucket = ComposeTimeBucket.Hour, ComposeAggregate aggregate = ComposeAggregate.Sum,
        ComposeMeasure? measure = null) =>
        new()
        {
            Measure = measure ?? MeasureCatalog.Measures.First(m => string.Equals(m.Key, "query_worker_us", StringComparison.Ordinal)),
            Unit = "ms",
            Mode = mode,
            Filters = Array.Empty<ComposeFilter>(),
            GroupBy = Array.Empty<ComposeDimension>(),
            Viz = "line",
            TimeBucket = bucket,
            Aggregate = aggregate,
        };

    private static RollupCoverage Coverage(DateTime? floor, DateTime? ceiling)
    {
        var floors = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        if (floor is not null)
        {
            floors[TimescaleSupport.QueryStatsIntervalHourlyView] = floor.Value;
        }

        var ceilings = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        if (ceiling is not null)
        {
            ceilings[TimescaleSupport.QueryStatsIntervalHourlyView] = ceiling.Value;
        }

        return new RollupCoverage(floors, new Dictionary<string, DateTime>(StringComparer.Ordinal), RollupAvailability.All, ceilings);
    }

    private static (ComposeHourlyEdgesCandidate? Candidate, string? Refusal) Decide(
        PanelPlan plan, DateTime? start = null, DateTime? end = null, RollupCoverage? coverage = null)
    {
        var candidate = ComposeSourceRouter.HourlyRawEdgesCandidate(
            plan, Now, start ?? Start24h, end ?? Now, RollupAvailability.All, coverage ?? Coverage(Now.AddDays(-3), Now.AddMinutes(-20)), out var refusal);
        return (candidate, refusal);
    }

    [Fact]
    public void AnEligibleCell_HasACandidate_AndNoRefusal()
    {
        var (candidate, refusal) = Decide(Plan());

        Assert.NotNull(candidate);
        Assert.Null(refusal);
    }

    [Fact]
    public void ATimeSeriesLeftOnAutoAtTwentyFourHours_ReadsMinuteBuckets_AndIsRefusedForItsBucket_WhileAnHourBucketIsServed()
    {
        var (auto, autoRefusal) = Decide(Plan(bucket: ComposeTimeBucket.Auto));
        var (minute, minuteRefusal) = Decide(Plan(bucket: ComposeTimeBucket.Minute));
        var (hour, hourRefusal) = Decide(Plan(bucket: ComposeTimeBucket.Hour));

        Assert.Null(auto);
        Assert.Equal("hourly_edges_bucket", autoRefusal);
        Assert.Null(minute);
        Assert.Equal("hourly_edges_bucket", minuteRefusal);
        Assert.NotNull(hour);
        Assert.Null(hourRefusal);
    }

    [Fact]
    public void ARankedOrScalarCell_IsNotRefusedForItsBucket()
    {
        Assert.NotNull(Decide(Plan(PanelMode.Ranked, ComposeTimeBucket.Auto)).Candidate);
        Assert.NotNull(Decide(Plan(PanelMode.Scalar, ComposeTimeBucket.Auto)).Candidate);
    }

    [Fact]
    public void AnAverageOrCountAggregate_IsRefusedForItsAggregate()
    {
        Assert.Equal("hourly_edges_aggregate", Decide(Plan(aggregate: ComposeAggregate.Avg)).Refusal);
        Assert.Equal("hourly_edges_aggregate", Decide(Plan(aggregate: ComposeAggregate.Count)).Refusal);
        Assert.Null(Decide(Plan(aggregate: ComposeAggregate.Max)).Refusal);
    }

    [Fact]
    public void ARatioMeasure_IsRefusedForItsMeasure()
    {
        var ratio = MeasureCatalog.Measures.First(m => m.Kind == MeasureKind.Ratio && string.Equals(m.SourceTable, "query_stats", StringComparison.Ordinal));

        var (candidate, refusal) = Decide(Plan(measure: ratio));

        Assert.Null(candidate);
        Assert.Equal("hourly_edges_measure", refusal);
    }

    [Fact]
    public void AWindowUnderTwelveHours_IsRefusedForItsLength()
    {
        var (candidate, refusal) = Decide(Plan(), start: Now.AddHours(-6));

        Assert.Null(candidate);
        Assert.Equal("hourly_edges_window_short", refusal);
    }

    [Fact]
    public void AWindowThatDoesNotEndNow_IsRefusedForItsEnd()
    {
        var (candidate, refusal) = Decide(Plan(), start: Now.AddHours(-30), end: Now.AddHours(-6));

        Assert.Null(candidate);
        Assert.Equal("hourly_edges_window_end", refusal);
    }

    [Fact]
    public void ARollupThatDoesNotReachTheWindowStartOrHasNoCeiling_IsRefusedForTheRollup()
    {
        Assert.Equal("hourly_edges_rollup_floor", Decide(Plan(), coverage: Coverage(Now.AddHours(-3), Now.AddMinutes(-20))).Refusal);
        Assert.Equal("hourly_edges_rollup_ceiling", Decide(Plan(), coverage: Coverage(Now.AddDays(-3), null)).Refusal);
        Assert.Equal("hourly_edges_middle_short", Decide(Plan(), coverage: Coverage(Now.AddDays(-3), Now.AddHours(-23).AddMinutes(-30))).Refusal);
    }

    [Fact]
    public void ADecisionTheRouterDoesNotMake_HasNoRefusal()
    {
        /* Older than the raw route: a rollup tier answers, so no hourly-edges decision applies. */
        var (oldCandidate, oldRefusal) = Decide(Plan(), start: Now.AddDays(-10));
        Assert.Null(oldCandidate);
        Assert.Null(oldRefusal);

        /* A source the route does not serve. */
        var other = MeasureCatalog.Measures.First(m => m.SourceTable is not ("query_stats" or "procedure_stats"));
        var (otherCandidate, otherRefusal) = Decide(Plan(measure: other));
        Assert.Null(otherCandidate);
        Assert.Null(otherRefusal);
    }

    [Fact]
    public void TheReadSourceReason_NamesTheSplit_TheRefusal_OrTheGuard_AndOnlyForARawRead()
    {
        Assert.Equal("split_raw_servers", ComposePanelRunner.ReadSourceReason("interval_table", splitRawServers: true, hourlyEdgesCandidate: false, hourlyEdgesRefusal: null));
        Assert.Equal("hourly_edges_bucket", ComposePanelRunner.ReadSourceReason("raw", false, false, "hourly_edges_bucket"));
        Assert.Equal("hourly_edges_guard_not_passed", ComposePanelRunner.ReadSourceReason("raw", false, true, null));
        Assert.Null(ComposePanelRunner.ReadSourceReason("raw", false, false, null));
        Assert.Null(ComposePanelRunner.ReadSourceReason("hourly_edges", false, true, null));
        Assert.Null(ComposePanelRunner.ReadSourceReason("interval_table", false, false, "hourly_edges_bucket"));
    }

    [Fact]
    public void ALedgerMismatchNote_ResolvesToFallbackRaw_AndARankedFaultKeepsItsPlace()
    {
        Assert.Equal(ReadOutcome.FallbackRaw, ReadScope.Resolve(ReadOutcome.Ok, ReadFallback.LedgerMismatch));
        Assert.Equal(ReadOutcome.Timeout, ReadScope.Resolve(ReadOutcome.Timeout, ReadFallback.LedgerMismatch));

        using (var scope = ReadScope.Open(null))
        {
            ReadScope.Note(ReadFallback.GateFailed);
            ReadScope.Note(ReadFallback.LedgerMismatch);
            Assert.Equal(ReadFallback.GateFailed, scope.Fallback);
        }

        using (var scope = ReadScope.Open(null))
        {
            ReadScope.Note(ReadFallback.LedgerMismatch);
            Assert.Equal(ReadFallback.LedgerMismatch, scope.Fallback);
        }
    }
}
