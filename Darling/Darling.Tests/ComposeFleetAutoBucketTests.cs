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
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9883: an Auto bucket resolves from the run's scope and window. A fleet time series of 12 hours or more on a source the hourly
/// rollup serves is drawn by the hour, which is the series the hourly-raw-edges route reads, and the run says so in its note. Every
/// other run resolves from the window alone, and an explicit bucket is never changed. The raw-read refusal tells a minute series
/// that an hour bucket reads the rollup, only when it would. Pure decision logic, no database.
/// </summary>
public sealed class ComposeFleetAutoBucketTests
{
    private static readonly DateTime Now = new(2026, 7, 23, 12, 0, 0, DateTimeKind.Unspecified);
    private const double Hour = 3_600d;
    private const double Day = 86_400d;

    /// <summary>The two walked fleet cells: a summed cumulative measure, a second line, and one <c>eq</c> filter.</summary>
    private const string ProcedureCell =
        "{\"source\":\"procedure_stats\",\"measure\":\"proc_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"BUCKET\",\"viz\":\"line\","
        + "\"overlay\":{\"measure\":\"proc_executions\",\"aggregate\":\"sum\",\"unit\":\"count\"},"
        + "\"filters\":[{\"dimension\":\"object_name\",\"op\":\"eq\",\"value\":\"usp_report\"}]}";

    private const string QueryCell =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"BUCKET\",\"viz\":\"line\","
        + "\"overlay\":{\"measure\":\"query_executions\",\"aggregate\":\"sum\",\"unit\":\"count\"},"
        + "\"filters\":[{\"dimension\":\"query_hash\",\"op\":\"eq\",\"value\":\"0x1A2B3C4D5E6F7081\"}]}";

    public static TheoryData<string> WalkedCells => new() { ProcedureCell, QueryCell };

    private static PanelPlan Cell(string cell, string bucket, string aggregate = "sum")
    {
        var json = cell.Replace("BUCKET", bucket, StringComparison.Ordinal).Replace("\"aggregate\":\"sum\"", $"\"aggregate\":\"{aggregate}\"", StringComparison.Ordinal);
        var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(json)!, []);
        Assert.True(error is null, error);
        return plan!;
    }

    /// <summary>A store whose hourly rollup for the plan's source reaches back three days and is current to twenty minutes ago.</summary>
    private static RollupCoverage Coverage(PanelPlan plan)
    {
        var successor = TimescaleSupport.SuccessorOf(ComposeCaggCatalog.For(plan.Measure.SourceTable)!.HourlyView)!;
        return new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [successor] = Now.AddDays(-3) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All,
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [successor] = Now.AddMinutes(-20) });
    }

    private static (ComposeHourlyEdgesCandidate? Candidate, string? Refusal) Decide(PanelPlan plan, double windowSeconds, bool fleetScope)
    {
        var candidate = ComposeSourceRouter.HourlyRawEdgesCandidate(
            plan, Now, Now.AddSeconds(-windowSeconds), Now, RollupAvailability.All, Coverage(plan), fleetScope, out var refusal);
        return (candidate, refusal);
    }

    /// <summary>Compiles the plan for a run of <paramref name="servers"/>, handing the compiler the verdict a passing count guard
    /// gives for the hour-bucket form of the same cell, so a plan the route can take does take it.</summary>
    private static ComposeCompiled Compile(PanelPlan plan, double windowSeconds, IReadOnlyList<string>? servers)
    {
        var start = Now.AddSeconds(-windowSeconds);
        var coverage = Coverage(plan);
        var proven = ComposeSourceRouter.HourlyRawEdgesCandidate(
            plan with { TimeBucket = ComposeTimeBucket.Hour }, Now, start, Now, RollupAvailability.All, coverage);
        var verdict = proven is null ? null : new ComposeHourlyEdgesVerdict(proven.SourceTable, proven.HourStartUtc, proven.HourEndUtc, servers);
        var (compiled, error) = ComposeCompiler.Compile(
            plan, new ComposeRunContext(servers, start, Now, ComposeRunContext.NoVariables, RollupAvailability.All, Now, coverage, HourlyEdges: verdict));
        Assert.True(error is null, error);
        return compiled!;
    }

    [Theory]
    [MemberData(nameof(WalkedCells))]
    public void AFleetSeriesOnAuto_AtTwentyFourHours_IsDrawnByTheHour_AndTakesTheHourlyRollup(string cell)
    {
        var auto = Cell(cell, "auto");

        Assert.Equal(ComposeTimeBucket.Hour, ComposeSourceRouter.ResolvePanelBucket(auto, Day, fleetScope: true));
        var (candidate, refusal) = Decide(auto, Day, fleetScope: true);
        Assert.NotNull(candidate);
        Assert.Null(refusal);

        var compiled = Compile(auto, Day, servers: null);
        Assert.Equal(ComposeSourceTier.HourlyRawEdges, compiled.Route.Tier);
        Assert.Equal(Compile(Cell(cell, "hour"), Day, servers: null).Sql, compiled.Sql);
        Assert.Equal(ComposeSourceRouter.FleetAutoHourNote, ComposeSourceRouter.FleetAutoHourNoteFor(auto, Day, fleetScope: true));
    }

    [Theory]
    [MemberData(nameof(WalkedCells))]
    public void AFleetSeriesOnAuto_ThatTheRouteDoesNotServe_ReadsRawAtTheSameHourGrain(string cell)
    {
        /* An average is not a shape the rollup route takes, and the grain does not depend on that: the fleet run is still by the hour. */
        var auto = Cell(cell, "auto", aggregate: "avg");

        Assert.Equal("hourly_edges_aggregate", Decide(auto, Day, fleetScope: true).Refusal);
        var compiled = Compile(auto, Day, servers: null);
        Assert.Equal(ComposeSourceTier.Raw, compiled.Route.Tier);
        Assert.Equal(Compile(Cell(cell, "hour", aggregate: "avg"), Day, servers: null).Sql, compiled.Sql);
    }

    [Theory]
    [MemberData(nameof(WalkedCells))]
    public void AServerScopedSeriesOnAuto_AtTwentyFourHours_StaysByTheMinute(string cell)
    {
        var auto = Cell(cell, "auto");
        IReadOnlyList<string> servers = ["sql-a"];

        Assert.False(ComposeSourceRouter.IsFleetScope(servers));
        Assert.Equal(ComposeTimeBucket.Minute, ComposeSourceRouter.ResolvePanelBucket(auto, Day, fleetScope: false));
        Assert.Equal("hourly_edges_bucket", Decide(auto, Day, fleetScope: false).Refusal);

        var compiled = Compile(auto, Day, servers);
        Assert.Equal(ComposeSourceTier.Raw, compiled.Route.Tier);
        Assert.Equal(Compile(Cell(cell, "minute"), Day, servers).Sql, compiled.Sql);
        Assert.Null(ComposeSourceRouter.FleetAutoHourNoteFor(auto, Day, fleetScope: false));
    }

    [Theory]
    [MemberData(nameof(WalkedCells))]
    public void AFleetSeriesOnAuto_UnderTwelveHours_StaysByTheMinute_AndAtTwelveHoursIsByTheHour(string cell)
    {
        var auto = Cell(cell, "auto");

        Assert.Equal(ComposeTimeBucket.Minute, ComposeSourceRouter.ResolvePanelBucket(auto, 11d * Hour, fleetScope: true));
        Assert.Equal(Compile(Cell(cell, "minute"), 11d * Hour, servers: null).Sql, Compile(auto, 11d * Hour, servers: null).Sql);
        Assert.Null(ComposeSourceRouter.FleetAutoHourNoteFor(auto, 11d * Hour, fleetScope: true));

        Assert.Equal(ComposeTimeBucket.Minute, ComposeSourceRouter.ResolvePanelBucket(auto, 12d * Hour - 1d, fleetScope: true));
        Assert.Equal(ComposeTimeBucket.Hour, ComposeSourceRouter.ResolvePanelBucket(auto, 12d * Hour, fleetScope: true));
        Assert.Equal(ComposeSourceTier.HourlyRawEdges, Compile(auto, 12d * Hour, servers: null).Route.Tier);
    }

    [Theory]
    [MemberData(nameof(WalkedCells))]
    public void AFleetSeriesOnAnExplicitMinuteBucket_AtTwentyFourHours_StaysByTheMinute_AndReadsRaw(string cell)
    {
        var minute = Cell(cell, "minute");

        Assert.Equal(ComposeTimeBucket.Minute, ComposeSourceRouter.ResolvePanelBucket(minute, Day, fleetScope: true));
        Assert.Equal("hourly_edges_bucket", Decide(minute, Day, fleetScope: true).Refusal);
        Assert.Equal(ComposeSourceTier.Raw, Compile(minute, Day, servers: null).Route.Tier);
        Assert.Null(ComposeSourceRouter.FleetAutoHourNoteFor(minute, Day, fleetScope: true));
    }

    [Theory]
    [MemberData(nameof(WalkedCells))]
    public void AutoAtThreeDaysAndNinetyDays_IsHourAndDay_ForEveryScope_AndCarriesNoNote(string cell)
    {
        var auto = Cell(cell, "auto");

        foreach (var fleetScope in new[] { true, false })
        {
            Assert.Equal(ComposeTimeBucket.Hour, ComposeSourceRouter.ResolvePanelBucket(auto, 3d * Day, fleetScope));
            Assert.Equal(ComposeTimeBucket.Day, ComposeSourceRouter.ResolvePanelBucket(auto, 90d * Day, fleetScope));
            Assert.Null(ComposeSourceRouter.FleetAutoHourNoteFor(auto, 3d * Day, fleetScope));
            Assert.Null(ComposeSourceRouter.FleetAutoHourNoteFor(auto, 90d * Day, fleetScope));
        }
    }

    [Fact]
    public void TheWindowOnlyRule_IsUnchanged_AndAnExplicitBucketIsNeverResolved()
    {
        foreach (var seconds in new[] { Hour, 11d * Hour, 12d * Hour, Day, 2d * Day, 2d * Day + 1d, 60d * Day, 60d * Day + 1d })
        {
            Assert.Equal(MeasureCatalog.ResolveBucket(ComposeTimeBucket.Auto, seconds), MeasureCatalog.ResolveBucket(ComposeTimeBucket.Auto, seconds, fleetHourlySource: false));
            foreach (var explicitBucket in new[] { ComposeTimeBucket.Minute, ComposeTimeBucket.Hour, ComposeTimeBucket.Day, ComposeTimeBucket.None })
            {
                Assert.Equal(explicitBucket, MeasureCatalog.ResolveBucket(explicitBucket, seconds, fleetHourlySource: true));
            }
        }

        Assert.Equal(ComposeTimeBucket.Minute, MeasureCatalog.ResolveBucket(ComposeTimeBucket.Auto, Day));
        Assert.Equal(ComposeTimeBucket.Hour, MeasureCatalog.ResolveBucket(ComposeTimeBucket.Auto, Day, fleetHourlySource: true));
    }

    [Fact]
    public void AFleetSeriesOnAuto_OverASourceTheHourlyRollupDoesNotServe_StaysByTheMinute_AndCarriesNoNote()
    {
        var measure = MeasureCatalog.Measures.First(m => m.Kind != MeasureKind.Ratio && !ComposeSourceRouter.HourlyRawEdgesEnabledSources.Contains(m.SourceTable));
        var plan = Cell(QueryCell, "auto") with { Measure = measure, Overlay = null, Filters = Array.Empty<ComposeFilter>() };

        Assert.False(ComposeSourceRouter.IsFleetHourlySource(plan, fleetScope: true));
        Assert.Equal(ComposeTimeBucket.Minute, ComposeSourceRouter.ResolvePanelBucket(plan, Day, fleetScope: true));
        Assert.Null(ComposeSourceRouter.FleetAutoHourNoteFor(plan, Day, fleetScope: true));
    }

    [Fact]
    public void NoServerScope_AnEmptyList_IsTheFleet_AndANamedServerIsNot()
    {
        Assert.True(ComposeSourceRouter.IsFleetScope(null));
        Assert.True(ComposeSourceRouter.IsFleetScope(Array.Empty<string>()));
        Assert.False(ComposeSourceRouter.IsFleetScope(["sql-a", "sql-b"]));
    }

    [Theory]
    [MemberData(nameof(WalkedCells))]
    public void TheHourBucketAdvice_IsForAMinuteSeriesTheRouteServesAtHour_AndForNoOtherCell(string cell)
    {
        bool Advice(PanelPlan plan, double windowSeconds, bool fleetScope) =>
            ComposeSourceRouter.HourBucketWouldReadRollup(plan, Now, Now.AddSeconds(-windowSeconds), Now, RollupAvailability.All, Coverage(plan), fleetScope);

        /* A minute series the route takes at hour: an explicit minute bucket on the fleet, and Auto on a named server. */
        Assert.True(Advice(Cell(cell, "minute"), Day, fleetScope: true));
        Assert.True(Advice(Cell(cell, "auto"), Day, fleetScope: false));

        /* Not a minute series, or a shape the route refuses at hour as well. */
        Assert.False(Advice(Cell(cell, "auto"), Day, fleetScope: true));
        Assert.False(Advice(Cell(cell, "hour"), Day, fleetScope: true));
        Assert.False(Advice(Cell(cell, "minute", aggregate: "avg"), Day, fleetScope: true));
        Assert.False(Advice(Cell(cell, "minute"), 11d * Hour, fleetScope: true));
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("procedure_stats")]
    public void TheRawRefusal_AddsTheHourBucketAdvice_AfterItsEstimateLimitAndWindowAdvice_OnlyWhenTold(string collector)
    {
        var limit = QueryStoreWideReadGuard.RawLimitFor(collector, null);
        var estimate = new QueryStoreWideReadGuard.RawEstimate(limit * 2, limit * 2, 40);
        var start = Now.AddDays(-1);

        var plain = QueryStoreWideReadGuard.RawRefusal(estimate, collector, start, Now, limit);
        var advised = QueryStoreWideReadGuard.RawRefusal(estimate, collector, start, Now, limit, hourBucketReadsRollup: true);

        Assert.NotNull(plain);
        Assert.Equal(plain, QueryStoreWideReadGuard.RawRefusal(estimate, collector, start, Now, limit, hourBucketReadsRollup: false));
        Assert.Equal(plain + " " + QueryStoreWideReadGuard.HourBucketAdvice, advised);

        /* A read under the limit is not refused, so it carries no advice either. */
        Assert.Null(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.RawEstimate(limit - 1, limit - 1, 40), collector, start, Now, limit, hourBucketReadsRollup: true));
    }
}
