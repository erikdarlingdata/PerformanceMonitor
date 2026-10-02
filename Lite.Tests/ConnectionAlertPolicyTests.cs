/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the SHARED connection-alert decision (<see cref="ConnectionAlertPolicy"/>, #1659) from Lite's suite —
/// mirrored in Darling.Tests, the AzureMasterFallback discipline for classes both apps depend on. The rules
/// replaced Lite's <c>ConnectionEdgeDetector</c> and the inline machine in Darling's
/// <c>DarlingSelfAlertEvaluator</c>: identical edge-only behavior by default (a transition alerts exactly
/// once; the first sighting is a silent baseline), plus the two opt-ins — announce a server already down at
/// first sight, and re-announce a standing outage every N minutes.
/// </summary>
public sealed class ConnectionAlertPolicyTests
{
    private static readonly DateTime Now = new(2026, 7, 26, 12, 0, 0, DateTimeKind.Utc);

    private static ConnectionAlertDecision Decide(
        bool? previous, bool online,
        bool startupOptIn = false, int refireMinutes = 0, DateTime? lastDownUtc = null) =>
        ConnectionAlertPolicy.Decide(
            previous, online, startupOptIn,
            refireMinutes > 0 ? TimeSpan.FromMinutes(refireMinutes) : null,
            lastDownUtc, Now);

    /* ── the classic edge-only semantics, unchanged with both opt-ins off ── */

    [Theory]
    [InlineData(null, true, ConnectionAlertDecision.None)]    /* first sighting online: silent baseline */
    [InlineData(null, false, ConnectionAlertDecision.None)]   /* first sighting down: silent baseline (the #1659 gap, by default) */
    [InlineData(true, true, ConnectionAlertDecision.None)]    /* steady online */
    [InlineData(true, false, ConnectionAlertDecision.Lost)]   /* the edge */
    [InlineData(false, true, ConnectionAlertDecision.Restored)]
    [InlineData(false, false, ConnectionAlertDecision.None)]  /* standing outage: one alert per outage, by default */
    public void Defaults_AreExactlyTheOldEdgeOnlyBehavior(bool? previous, bool online, ConnectionAlertDecision expected) =>
        Assert.Equal(expected, Decide(previous, online));

    /* ── opt-in 1: already down at first sight ── */

    [Fact]
    public void StartupOptIn_AnnouncesAServerAlreadyDownOnItsFirstObservation()
    {
        Assert.Equal(ConnectionAlertDecision.AlreadyDownAtFirstSight, Decide(null, online: false, startupOptIn: true));

        /* A first sighting that is ONLINE stays silent — the opt-in announces outages, not baselines. */
        Assert.Equal(ConnectionAlertDecision.None, Decide(null, online: true, startupOptIn: true));
    }

    /* ── opt-in 2: standing-outage re-fire ── */

    [Fact]
    public void Refire_FiresWhenTheIntervalHasElapsed_AndNotBefore()
    {
        /* Inside the window: quiet. */
        Assert.Equal(ConnectionAlertDecision.None,
            Decide(false, online: false, refireMinutes: 10, lastDownUtc: Now.AddMinutes(-5)));

        /* At/past the window: re-announce. */
        Assert.Equal(ConnectionAlertDecision.StillDown,
            Decide(false, online: false, refireMinutes: 10, lastDownUtc: Now.AddMinutes(-10)));
        Assert.Equal(ConnectionAlertDecision.StillDown,
            Decide(false, online: false, refireMinutes: 10, lastDownUtc: Now.AddMinutes(-60)));
    }

    /// <summary>
    /// Down with NO recorded down alert and re-fire on = due immediately. This is what re-announces an
    /// outage after a mid-outage restart even when the startup opt-in is off: the first sighting
    /// re-baselines silently, and the next offline→offline poll lands here.
    /// </summary>
    [Fact]
    public void Refire_WithNoRecordedDownAlert_IsDueImmediately() =>
        Assert.Equal(ConnectionAlertDecision.StillDown,
            Decide(false, online: false, refireMinutes: 10, lastDownUtc: null));

    [Fact]
    public void Refire_Off_NeverReannounces() =>
        Assert.Equal(ConnectionAlertDecision.None,
            Decide(false, online: false, refireMinutes: 0, lastDownUtc: Now.AddDays(-1)));

    /* ── the opt-ins never disturb the edges themselves ── */

    [Fact]
    public void Restored_AndLost_AreUnchangedByTheOptIns()
    {
        Assert.Equal(ConnectionAlertDecision.Restored,
            Decide(false, online: true, startupOptIn: true, refireMinutes: 10, lastDownUtc: Now.AddMinutes(-60)));
        Assert.Equal(ConnectionAlertDecision.Lost,
            Decide(true, online: false, startupOptIn: true, refireMinutes: 10));
    }

    /* ── #4795: a down alert no channel delivered is due again once its retry time arrives ── */

    private static ConnectionAlertDecision DecideWithRetry(
        bool? previous, bool online, DateTime? retryDueUtc, int refireMinutes = 0, DateTime? lastDownUtc = null) =>
        ConnectionAlertPolicy.Decide(
            previous, online, false,
            refireMinutes > 0 ? TimeSpan.FromMinutes(refireMinutes) : null,
            lastDownUtc, Now, retryDueUtc);

    /// <summary>
    /// The retry is due whatever the re-fire setting says: with re-fire off, a standing outage whose alert
    /// reached no channel has nothing else to bring it back. Not yet due stays quiet, at the due time and after
    /// it is a StillDown.
    /// </summary>
    [Theory]
    [InlineData(0, 1, ConnectionAlertDecision.None)]
    [InlineData(0, 0, ConnectionAlertDecision.StillDown)]
    [InlineData(0, -1, ConnectionAlertDecision.StillDown)]
    [InlineData(10, 1, ConnectionAlertDecision.None)]
    [InlineData(10, 0, ConnectionAlertDecision.StillDown)]
    public void Retry_IsStillDownOnceDue_WhateverTheRefireSetting(
        int refireMinutes, int dueInMinutes, ConnectionAlertDecision expected) =>
        Assert.Equal(
            expected,
            DecideWithRetry(false, false, Now.AddMinutes(dueInMinutes), refireMinutes, lastDownUtc: Now.AddMinutes(-1)));

    /// <summary>The delivered case: no retry pending and re-fire off gives None for as long as the server is down.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(60)]
    [InlineData(60 * 24 * 30)]
    public void NoRetryPending_RefireOff_StaysQuietWhileDown(int minutesSinceLastAlert) =>
        Assert.Equal(
            ConnectionAlertDecision.None,
            DecideWithRetry(false, false, retryDueUtc: null, lastDownUtc: Now.AddMinutes(-minutesSinceLastAlert)));

    /// <summary>A due retry time only matters to a standing outage: an edge, a restore and a silent baseline decide as before.</summary>
    [Fact]
    public void Retry_DoesNotChangeAnEdgeARestoreOrABaseline()
    {
        var due = Now.AddMinutes(-5);
        Assert.Equal(ConnectionAlertDecision.Lost, DecideWithRetry(true, false, due));
        Assert.Equal(ConnectionAlertDecision.Restored, DecideWithRetry(false, true, due));
        Assert.Equal(ConnectionAlertDecision.None, DecideWithRetry(true, true, due));
        Assert.Equal(ConnectionAlertDecision.None, DecideWithRetry(null, false, due));
        Assert.Equal(ConnectionAlertDecision.None, DecideWithRetry(null, true, due));
    }

    /// <summary>A restore ends the outage: with the retry cleared the next outage is quiet after its own edge.</summary>
    [Fact]
    public void Restored_ThenAClearedRetry_LeavesTheNextOutageQuiet()
    {
        Assert.Equal(ConnectionAlertDecision.Restored, DecideWithRetry(false, true, Now.AddMinutes(-1)));
        Assert.Equal(ConnectionAlertDecision.Lost, DecideWithRetry(true, false, retryDueUtc: null));
        Assert.Equal(ConnectionAlertDecision.None, DecideWithRetry(false, false, retryDueUtc: null, lastDownUtc: Now));
    }
}
