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
/// #1225: the shared metric-name classifier that both apps' Alert History grids and the Dashboard
/// sidebar Alert badge rely on. Locks in the resolution suffix set (Cleared/Resolved/Restored, plus
/// Resumed/Restarted/Recovered/Reconnected) — the "Restored" cases (Capture/Server Restored) are the ones
/// the old duplicated inline copies missed, and the last four are the same drift caught one layer down in
/// Darling's self-alert recoveries (#991) — plus the critical (Deadlock/Poison) and warning buckets, over
/// the metric names the alert engines actually emit.
/// </summary>
public class AlertMetricClassifierTests
{
    [Theory]
    [InlineData("Blocking Cleared")]
    [InlineData("Deadlocks Cleared")]
    [InlineData("Poison Waits Cleared")]
    [InlineData("Long-Running Queries Cleared")]
    [InlineData("Long-Running Jobs Cleared")]
    [InlineData("CPU Resolved")]
    [InlineData("tempdb Space Resolved")]
    [InlineData("Volume Free Space Resolved")]
    [InlineData("Capture Restored")]
    [InlineData("Server Restored")]
    /* The suffixes the list had never caught up with. The first three are Darling self-alert recoveries that
       have been shipping mis-classified as live actionable alerts in the history grids; the last three are the
       #991 Availability Group family's recoveries, added alongside the fix rather than as a fifth blind spot. */
    [InlineData("Collection Resumed")]
    [InlineData("Agent Restarted")]
    [InlineData("Compression Job Recovered")]
    [InlineData("AG Replica Reconnected")]
    [InlineData("AG Sync Recovered")]
    [InlineData("AG Data Movement Resumed")]
    public void IsResolution_True_ForEveryResolutionNotice(string metric)
    {
        Assert.True(AlertMetricClassifier.IsResolution(metric));
        Assert.False(AlertMetricClassifier.IsWarning(metric)); // a resolution notice is never a warning
    }

    [Theory]
    [InlineData("Blocking Detected")]
    [InlineData("Deadlocks Detected")]
    [InlineData("High CPU")]
    [InlineData("Poison Wait")]
    [InlineData("Long-Running Query")]
    [InlineData("tempdb Space")]
    [InlineData("Volume Free Space")]
    [InlineData("Long-Running Job")]
    [InlineData("Failed Agent Job")]
    [InlineData("Capture Down")]
    [InlineData("Server Unreachable")]
    [InlineData("Agent Not Running")]
    [InlineData("Compression Job Stuck")]
    /* The AG family's actionable names, pinned against the widened suffix set: none of them may be dragged
       into the resolution bucket by "Reconnected"/"Recovered"/"Resumed" appearing elsewhere in the vocabulary. */
    [InlineData("AG Failover")]
    [InlineData("AG Replica Disconnected")]
    [InlineData("AG Sync Fell Behind")]
    [InlineData("AG Database Suspended")]
    public void IsResolution_False_ForActionableAlerts(string metric)
    {
        Assert.False(AlertMetricClassifier.IsResolution(metric));
    }

    [Theory]
    [InlineData("Deadlocks Detected")]
    [InlineData("Poison Wait")]
    public void IsCritical_True_ForDeadlockAndPoison(string metric)
    {
        Assert.True(AlertMetricClassifier.IsCritical(metric));
    }

    [Theory]
    [InlineData("Blocking Detected")]
    [InlineData("High CPU")]
    [InlineData("Long-Running Query")]
    public void IsWarning_True_ForOrdinaryActionableAlerts(string metric)
    {
        Assert.True(AlertMetricClassifier.IsWarning(metric));
        Assert.False(AlertMetricClassifier.IsCritical(metric));
        Assert.False(AlertMetricClassifier.IsResolution(metric));
    }

    /// <summary>The deliberate INFO reports (#3443's collector-cost digest, #3466's fleet sweep
    /// rollup) must not classify as warnings: both Alert History grids (Lite's <c>AlertsHistoryTab</c>
    /// and the Darling Viewer's) derive their row's <c>IsWarning</c> from this classifier and amber-fill
    /// on it, so without the carve-out the rows <c>AlertSeverity.ForMetric</c> deliberately pins
    /// INFO/blue for email and webhooks reach the desktop grids styled as live actionable alerts — the
    /// design undone through a second, untouched classifier (#3448 review). No predicate is true for
    /// them, so no row trigger fires and they render in the default chrome, which is the grids' only
    /// non-actionable rendering.</summary>
    [Theory]
    [InlineData("Collector Cost Digest")]
    [InlineData("Fleet Sweep Rollup")]
    public void IsWarning_False_ForTheDeliberateInfoReports(string metric)
    {
        Assert.True(AlertMetricClassifier.IsInformational(metric));
        Assert.False(AlertMetricClassifier.IsWarning(metric));
        Assert.False(AlertMetricClassifier.IsCritical(metric));
        Assert.False(AlertMetricClassifier.IsResolution(metric));
    }

    [Fact]
    public void Classifiers_AreFalse_ForNullOrEmpty()
    {
        Assert.False(AlertMetricClassifier.IsResolution(null));
        Assert.False(AlertMetricClassifier.IsResolution(""));
        Assert.False(AlertMetricClassifier.IsCritical(null));
        Assert.False(AlertMetricClassifier.IsCritical(""));
        Assert.False(AlertMetricClassifier.IsInformational(null));
        Assert.False(AlertMetricClassifier.IsInformational(""));
        // IsWarning is the complement of the three signals, so an absent name defaults to warning —
        // matching the long-standing display behavior this classifier replaced.
        Assert.True(AlertMetricClassifier.IsWarning(null));
        Assert.True(AlertMetricClassifier.IsWarning(""));
    }
}
