/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2794: "collection has stopped" is ONE condition and must have ONE definition. The display's Offline band
/// and the alert engine's Collection Stopped window disagreed — 15 minutes against 30 — so a long
/// <c>query_store</c> cycle that stretched a sweep past 15 minutes painted a healthy server with the red
/// Offline overlay while the alert engine (correctly) stayed quiet, and an investigation chased five phantom
/// dark shards. Measured on the production fleet: legitimate sweep stretch reached 19m18s under issue-day
/// load (12m12s post-#2792), while genuine dark events run hours — so the two populations are separable and
/// 30 minutes is the number the alert engine already committed to.
///
/// <para>These pins hold the definitions together BY VALUE across the seams a shared constant cannot reach
/// (a config default is an int, the evaluator's window is a TimeSpan, the band threshold is a TimeSpan) —
/// so any one of them drifting apart goes red here rather than silently re-splitting the condition.
/// Proven red against the pre-#2794 tree, where the first assertion fails 15m != 30m.</para>
/// </summary>
public class CollectionStoppedThresholdAgreementTests
{
    [Fact]
    public void TheDisplaysOffline_AndTheAlertEnginesStopped_AreTheSameWindow() =>
        Assert.Equal(DarlingSelfAlertEvaluator.StaleWindow, ServerHealthThresholds.OfflineThreshold);

    [Fact]
    public void TheConfigDefault_MatchesTheSharedWindow() =>
        Assert.Equal(
            TimeSpan.FromMinutes(new DarlingConfig().Alerts.CollectionStaleMinutes),
            ServerHealthThresholds.OfflineThreshold);

    [Fact]
    public void TheSharedConstant_IsTheWindowBothDeriveFrom() =>
        Assert.Equal(
            TimeSpan.FromMinutes(ServerHealthThresholds.CollectionStoppedMinutesDefault),
            ServerHealthThresholds.OfflineThreshold);

    /// <summary>
    /// The regression itself, as behavior: the worst legitimate sweep stretch the issue measured (19m18s on
    /// a healthy production server mid-<c>query_store</c> cycle) bands Stale — visibly lagged, honestly
    /// amber — never the red Offline overlay that claims the server is dark.
    /// </summary>
    [Fact]
    public void AMeasuredLegitimateSweepStretch_BandsStale_NotOffline()
    {
        var now = new DateTime(2026, 9, 2, 20, 49, 0, DateTimeKind.Utc);
        var lastCollection = now - new TimeSpan(0, 19, 18);

        Assert.Equal(ServerFreshness.Stale, ServerHealthClassifier.ClassifyFreshness(lastCollection, now));
    }
}
