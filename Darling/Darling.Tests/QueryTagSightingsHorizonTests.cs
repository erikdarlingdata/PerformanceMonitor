/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The retention horizon of <c>collect.query_tag_sightings</c> (#9705): the widest retention of every collector that writes
/// sightings, never narrower than the widest payload-dimension retention. Pure seams, no store: the cutoff choice is driven
/// through <see cref="DarlingRetention.QueryTagSightingsRetentionDays"/> with a resolver that raises one collector, and the set
/// of writers is read from <see cref="CollectorCatalog.All"/> at run time.
/// </summary>
public sealed class QueryTagSightingsHorizonTests
{
    private static readonly string[] ExpectedFeeders =
    {
        "attention", "blocked_process_report", "deadlocks", "default_trace_events", "dmv_blocking_snapshot",
        "long_query_completions", "procedure_stats", "query_snapshots", "query_stats", "query_store",
    };

    private static Func<string, int> Raised(string collector, int days) =>
        name => string.Equals(name, collector, StringComparison.Ordinal)
            ? days
            : CollectorScheduleDefaults.All[name].RetentionDays;

    [Fact]
    public void Feeders_AreTheDefinitionsThatDescribeSightings_ReadFromTheCatalog()
    {
        var feeders = DarlingRetention.QueryTagSightingFeeders();

        Assert.Equal(ExpectedFeeders, feeders.OrderBy(name => name, StringComparer.Ordinal).ToArray());

        /* The set is the catalog's own flag and nothing else: a definition outside it does not claim the flag. */
        var flagged = CollectorCatalog.All.Where(d => d.WritesQueryTagSightings).Select(d => d.Name).OrderBy(n => n, StringComparer.Ordinal);
        Assert.Equal(flagged, feeders.OrderBy(name => name, StringComparer.Ordinal));
        Assert.False(WaitStatsCollector.Instance.WritesQueryTagSightings);
    }

    [Fact]
    public void Defaults_LeaveTheSightingsHorizonAtTheDimensionHorizon()
    {
        var widestFact = DarlingRetention.WidestDimensionFactRetentionDays(null);

        Assert.Equal(30, widestFact);
        Assert.Equal(30, DarlingRetention.QueryTagSightingsRetentionDays(widestFact, null));
        Assert.Equal(30, DarlingRetention.QueryTagSightingsRetentionDays(widestFact, name => CollectorScheduleDefaults.All[name].RetentionDays));
    }

    [Theory]
    [InlineData("deadlocks")]
    [InlineData("query_store")]
    [InlineData("attention")]
    [InlineData("blocked_process_report")]
    [InlineData("dmv_blocking_snapshot")]
    [InlineData("long_query_completions")]
    [InlineData("default_trace_events")]
    [InlineData("query_snapshots")]
    public void RaisingANonDimensionFeeder_WidensTheSightingsHorizon_AndLeavesTheDimensionHorizon(string feeder)
    {
        var resolver = Raised(feeder, 90);
        var widestFact = DarlingRetention.WidestDimensionFactRetentionDays(resolver);

        Assert.Equal(30, widestFact);
        Assert.Equal(90, DarlingRetention.QueryTagSightingsRetentionDays(widestFact, resolver));
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("procedure_stats")]
    public void RaisingADimensionFeeder_MovesBothHorizons(string feeder)
    {
        var resolver = Raised(feeder, 90);
        var widestFact = DarlingRetention.WidestDimensionFactRetentionDays(resolver);

        Assert.Equal(90, widestFact);
        Assert.Equal(90, DarlingRetention.QueryTagSightingsRetentionDays(widestFact, resolver));
    }

    [Fact]
    public void ShorteningEveryFeeder_NeverNarrowsTheSightingsHorizonBelowTheDimensionHorizon()
    {
        /* Each feeder is shortened to 7 days except the dimension pair at 60, so the writers' widest is 60 and a wider dimension
           figure passed in wins: the horizon is never narrower than widestFactRetentionDays. */
        var resolver = new Func<string, int>(name => name is "query_stats" or "procedure_stats" ? 60 : 7);

        Assert.Equal(60, DarlingRetention.QueryTagSightingsRetentionDays(60, resolver));
        Assert.Equal(75, DarlingRetention.QueryTagSightingsRetentionDays(75, resolver));
    }

    /* #9705: the slim rows outlive every usage row a tag read joins a key to. The chunk interval is the margin a hypertable needs,
       because a chunk is dropped only when all of it is past its horizon. */
    private static int HourlyTierDays => (int)Math.Ceiling(PerformanceMonitor.Darling.Storage.TimescaleSupport.HourlyRetentionSpan.TotalDays);

    private static int ChunkDays => PerformanceMonitor.Darling.Storage.TimescaleSupport.ChunkIntervalDays;

    [Fact]
    public void Defaults_KeepASlimRowPastTheHourlyTiers_WhichOutliveTheSightings()
    {
        var sightings = DarlingRetention.QueryTagSightingsRetentionDays(DarlingRetention.WidestDimensionFactRetentionDays(null), null);
        var slim = DarlingRetention.QueryTagSlimSightingsRetentionDays(sightings);

        Assert.True(HourlyTierDays > sightings, "the shipped hourly tiers keep rows longer than the sightings, which is the gap the slim table covers");
        Assert.True(slim > HourlyTierDays + ChunkDays, $"slim {slim} days must outlive an hourly tier row of {HourlyTierDays} days and the chunk that holds it");
    }

    [Theory]
    [InlineData("query_store")]
    [InlineData("deadlocks")]
    [InlineData("query_stats")]
    public void RaisingAFeederPastTheHourlyTiers_MovesTheSlimHorizonWithIt(string feeder)
    {
        var raisedDays = HourlyTierDays + 60;
        var resolver = Raised(feeder, raisedDays);
        var sightings = DarlingRetention.QueryTagSightingsRetentionDays(DarlingRetention.WidestDimensionFactRetentionDays(resolver), resolver);
        var slim = DarlingRetention.QueryTagSlimSightingsRetentionDays(sightings);

        Assert.Equal(raisedDays, sightings);
        Assert.True(slim > raisedDays + ChunkDays, $"slim {slim} days must outlive the raised feeder's rows of {raisedDays} days and the chunk that holds them");
    }

    [Theory]
    [InlineData(7)]
    [InlineData(30)]
    [InlineData(90)]
    [InlineData(400)]
    public void TheSlimHorizon_IsNeverInsideTheSightingsHorizonOrTheHourlyTiers(int sightingsDays)
    {
        var slim = DarlingRetention.QueryTagSlimSightingsRetentionDays(sightingsDays);

        Assert.True(slim > sightingsDays, $"slim {slim} days against sightings {sightingsDays} days");
        Assert.True(slim > HourlyTierDays, $"slim {slim} days against the hourly tiers' {HourlyTierDays} days");
    }
}
