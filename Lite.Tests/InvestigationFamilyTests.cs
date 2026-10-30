/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the investigation family map (#5632, ruling Q9): six families plus <c>other</c>, every metric of
/// <see cref="AlertFamily.MetricFamilies"/> classified, reports and delivered recoveries never investigated, and analysis
/// findings classified by their category.
/// </summary>
public sealed class InvestigationFamilyTests
{
    [Fact]
    public void TheFamiliesAreTheSixDraftFamiliesPlusOther()
    {
        Assert.Equal(
            new[] { "blocking", "query_performance", "resources", "availability", "jobs", "self_monitor", "other" },
            InvestigationFamily.All);
    }

    [Fact]
    public void ChangeWatch_IsQueryPerformance_NotOther()
    {
        /* #6551: the change-watch request's metric is not an alert in the census; it used to fall to `other`, next to custom and external alerts. */
        Assert.Equal(InvestigationFamily.QueryPerformance, InvestigationFamily.Of(PerformanceMonitor.Common.InvestigationTriggerKinds.ChangeWatchMetric));
        Assert.True(InvestigationFamily.StartsInvestigation(PerformanceMonitor.Common.InvestigationTriggerKinds.ChangeWatchMetric));
    }

    [Fact]
    public void EveryPerformanceMetricHasADeliberateFamily()
    {
        /* A new performance alert must be given a family here, not fall to `other` unseen. */
        var performance = AlertFamily.MetricFamilies.Where(p => p.Value == AlertFamily.Performance).Select(p => p.Key).ToList();
        Assert.All(performance, metric => Assert.True(
            InvestigationFamily.PerformanceMetricFamilies.ContainsKey(metric),
            $"'{metric}' is a performance alert with no entry in InvestigationFamily.PerformanceMetricFamilies."));
        Assert.All(InvestigationFamily.PerformanceMetricFamilies.Keys, metric => Assert.Equal(AlertFamily.Performance, AlertFamily.MetricFamilies[metric]));
    }

    [Fact]
    public void EveryCensusMetricMapsToAKnownFamilyOrToNone()
    {
        foreach (var (metric, notificationFamily) in AlertFamily.MetricFamilies)
        {
            var family = InvestigationFamily.Of(metric);
            /* #6003: the alert about a request no agent took never maps to a family (it would open another request). */
            if (notificationFamily == AlertFamily.Reports || AlertFamily.RecoveryPairs.ContainsKey(metric)
                || metric == AlertFamily.InvestigationUnansweredMetric)
            {
                Assert.Null(family);
                Assert.False(InvestigationFamily.StartsInvestigation(metric));
            }
            else
            {
                Assert.Contains(family, InvestigationFamily.All);
                Assert.NotEqual(InvestigationFamily.Other, family);
            }
        }
    }

    [Theory]
    [InlineData("Blocking Detected", "blocking")]
    [InlineData("Deadlocks Detected", "blocking")]
    [InlineData("PostgreSQL Vacuum Horizon Blocked", "blocking")]
    [InlineData("Long-Running Query", "query_performance")]
    [InlineData("Forced Plan Failing", "query_performance")]
    [InlineData("High CPU", "resources")]
    [InlineData("Poison Wait", "resources")]
    [InlineData("Volume Free Space", "resources")]
    [InlineData("Database State", "availability")]
    [InlineData("AG Failover", "availability")]
    [InlineData("PostgreSQL Wraparound Risk", "availability")]
    [InlineData("Failed Agent Job", "jobs")]
    [InlineData("Collection Stopped", "self_monitor")]
    [InlineData("Custom:42", "other")]
    [InlineData("External:pager:disk", "other")]
    [InlineData("Not A Metric Anyone Raises", "other")]
    [InlineData("Analysis: queries [1a2b3c4d]", "query_performance")]
    [InlineData("Analysis: blocking [1a2b3c4d]", "blocking")]
    [InlineData("Analysis: CPU_PRESSURE [1a2b3c4d]", "resources")]
    [InlineData("Analysis: replication_risk [1a2b3c4d]", "availability")]
    [InlineData("Analysis: anomaly [0123abcd]", "resources")]
    [InlineData("Analysis: cpu [0123abcd]", "resources")]
    [InlineData("Analysis: memory [0123abcd]", "resources")]
    [InlineData("Analysis: io [0123abcd]", "resources")]
    [InlineData("Analysis: disk [0123abcd]", "resources")]
    [InlineData("Analysis: jobs [0123abcd]", "jobs")]
    [InlineData("Analysis: config [0123abcd]", "other")]
    [InlineData("Analysis: waits [0123abcd]", "other")]
    [InlineData("Analysis: config_issue [1a2b3c4d]", "other")]
    [InlineData("Analysis: something_new [1a2b3c4d]", "other")]
    public void TheDraftTableAndTheRulingHold(string metric, string family)
    {
        Assert.Equal(family, InvestigationFamily.Of(metric));
    }

    [Theory]
    [InlineData("Server Restored")]
    [InlineData("AG Replica Reconnected")]
    [InlineData("Collector Cost Digest")]
    [InlineData("Fleet Sweep Rollup")]
    [InlineData("Analysis Singles Digest")]
    public void ReportsAndRecoveriesNeverStartAnInvestigation(string metric)
    {
        Assert.Null(InvestigationFamily.Of(metric));
    }

    /// <summary>#6003: the alert raised for a request no agent took has no family, so it can never open another request
    /// (an agent route matching every metric would otherwise expire, alert and open again without end).</summary>
    [Fact]
    public void TheUnansweredRequestAlert_NeverStartsAnInvestigation()
    {
        Assert.Null(InvestigationFamily.Of(AlertFamily.InvestigationUnansweredMetric));
    }

    [Fact]
    public void EveryCategoryFamilyIsAKnownFamily()
    {
        Assert.All(InvestigationFamily.AnalysisCategoryFamilies.Values, family => Assert.Contains(family, InvestigationFamily.All));
        Assert.All(InvestigationFamily.PerformanceMetricFamilies.Values.Where(f => f is not null), family => Assert.Contains(family, InvestigationFamily.All));
    }
}
