/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The Job History tab's Agent indicator must not present absence of signal as a problem. Two states the old
/// Running/Stopped pair collapsed wrongly: a server where Agent has NEVER been observed running (a container
/// built without it, Express, a Linux-minimal image) is "not present", not "Stopped" — nothing stopped, and
/// Lite collects in-process so nothing depends on Agent anyway; and a STALE snapshot is "unknown", not
/// "Stopped" — a server nobody has collected from in days is no evidence Agent is down.
///
/// <para>This is the display-tier twin of the capability gate on Darling's "Agent Not Running" alert, and it
/// mirrors that path's refusal to judge a stale reading.</para>
/// </summary>
public sealed class AgentStatusHeaderHonestyTests
{
    private static AgentStatusRow Row(bool running, bool everSeenRunning, int minutesOld) => new()
    {
        ServerId = 1,
        ServerName = "SQL01",
        AgentRunning = running,
        EverSeenRunning = everSeenRunning,
        CollectionTime = DateTime.UtcNow.AddMinutes(-minutesOld),
    };

    /* The two fixture ages every case in this class is built from, named so the straddle pin below reads the
       SAME values the fixtures use rather than a retyped copy that could drift away from them. */
    private const int CurrentFixtureMinutes = 1;
    private const int StaleFixtureMinutes = 45;

    [Fact]
    public void NeverSeenRunning_ReadsNotPresent_AndIsNotAProblem()
    {
        var row = Row(running: false, everSeenRunning: false, minutesOld: CurrentFixtureMinutes);

        Assert.Equal("not present", row.StatusDisplay);
        Assert.False(row.IsAgentProblem);
    }

    [Fact]
    public void StoppedOnAServerThatRunsAgent_ReadsStopped_AndIsAProblem()
    {
        var row = Row(running: false, everSeenRunning: true, minutesOld: CurrentFixtureMinutes);

        Assert.Equal("Stopped", row.StatusDisplay);
        Assert.True(row.IsAgentProblem);
    }

    [Fact]
    public void Running_ReadsRunning_AndIsNotAProblem()
    {
        var row = Row(running: true, everSeenRunning: true, minutesOld: CurrentFixtureMinutes);

        Assert.Equal("Running", row.StatusDisplay);
        Assert.False(row.IsAgentProblem);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void StaleReading_ReadsUnknown_AndIsNeverAProblem_WhateverItLastSaid(bool everSeenRunning)
    {
        /* Older than the window, which is the headless service's window via the shared constant (#2794);
           the straddle that makes this age meaningful is pinned by
           TheStalenessFixtures_StraddleTheWindowTheyAreMeantToTest. */
        var row = Row(running: false, everSeenRunning: everSeenRunning, minutesOld: StaleFixtureMinutes);

        Assert.True(row.IsStale);
        Assert.Equal("unknown (stale)", row.StatusDisplay);
        Assert.False(row.IsAgentProblem);
    }

    [Fact]
    public void MissingCollectionTime_CountsAsStale_RatherThanCurrent()
    {
        /* A row with no collection time cannot be vouched for; failing closed to "unknown" beats asserting a
           state we cannot date. */
        var row = new AgentStatusRow { AgentRunning = false, EverSeenRunning = true, CollectionTime = null };

        Assert.True(row.IsStale);
        Assert.Equal("unknown (stale)", row.StatusDisplay);
        Assert.False(row.IsAgentProblem);
    }

    /// <summary>
    /// The cases in this class only test what they claim while the fixture ages STRADDLE the window: the
    /// current-reading cases have to sit inside it, the stale-reading case outside it. Widen
    /// <c>ServerHealthThresholds.CollectionStoppedMinutesDefault</c> past <c>StaleFixtureMinutes</c> and
    /// <c>StaleReading_ReadsUnknown_AndIsNeverAProblem_WhateverItLastSaid</c> quietly stops exercising
    /// staleness at all — it would build a row the header considers CURRENT and then assert it is stale,
    /// failing on an out-of-date fixture constant while reporting itself as a behavior break. This pins the
    /// straddle, so the reason arrives with the failure.
    ///
    /// <para>Note what this deliberately does NOT assert. Not that the window is 30 minutes: it is now
    /// DERIVED from the shared constant (#2794), so restating the number here proves nothing about the two
    /// surfaces agreeing, and asserting it against the constant it derives from would be a tautology that can
    /// never fail. Not that it equals the headless service's window either —
    /// <c>DarlingSelfAlertEvaluator.StaleWindow</c> is <c>internal</c> to the service, which this assembly
    /// neither references nor is granted access to, and coupling Lite's tests to the service to reach one
    /// value would cost more than the pin is worth. That agreement is already held from both ends against the
    /// shared constant, the only place the two SKUs meet: <c>CollectionStoppedThresholdAgreementTests</c> here
    /// pins <c>AgentStatusRow.StaleWindow</c> to it, and its twin in <c>Darling.Tests</c> pins
    /// <c>DarlingSelfAlertEvaluator.StaleWindow</c> to it.</para>
    /// </summary>
    [Fact]
    public void TheStalenessFixtures_StraddleTheWindowTheyAreMeantToTest()
    {
        Assert.True(
            TimeSpan.FromMinutes(CurrentFixtureMinutes) < AgentStatusRow.StaleWindow,
            $"The current-reading fixture ({CurrentFixtureMinutes}m) must sit INSIDE StaleWindow "
                + $"({AgentStatusRow.StaleWindow}), or the not-present/Stopped/Running cases are asserting "
                + "against a row the header already considers stale.");

        Assert.True(
            TimeSpan.FromMinutes(StaleFixtureMinutes) >= AgentStatusRow.StaleWindow,
            $"The stale-reading fixture ({StaleFixtureMinutes}m) must sit OUTSIDE StaleWindow "
                + $"({AgentStatusRow.StaleWindow}), or the staleness cases are asserting against a row the "
                + "header still considers current.");
    }

    [Fact]
    public void RunningAlwaysWins_EvenIfEverSeenRunningSomehowLags()
    {
        /* A currently-running Agent is its own proof; the never-seen gate must not suppress the good news. */
        var row = Row(running: true, everSeenRunning: false, minutesOld: CurrentFixtureMinutes);

        Assert.Equal("Running", row.StatusDisplay);
        Assert.False(row.IsAgentProblem);
    }
}
