/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Job History header must only claim what the collected evidence supports (#1720). Two states used to
/// render as the same red "Agent: Stopped" as a genuine outage: a server where Agent has never been observed
/// running (a container built without it, Express, Azure SQL DB), and a reading too old to describe the
/// service now. These pin the two gates and, just as importantly, pin that the REAL alarm still fires — a
/// fix that quiets a false alarm by quieting the true one too is worse than the bug.
/// </summary>
public class AgentHeaderStatusTests
{
    private static readonly DateTime Now = new(2026, 7, 26, 12, 0, 0, DateTimeKind.Utc);

    private static AgentStatusRow Row(
        bool agentRunning,
        bool everSeenRunning,
        DateTime? collectionTime,
        int serverId = 1,
        string serverName = "SQL01") =>
        new()
        {
            ServerId = serverId,
            ServerName = serverName,
            AgentRunning = agentRunning,
            AgentStatusDesc = agentRunning ? "Running" : "Stopped",
            CollectionTime = collectionTime,
            EverSeenRunning = everSeenRunning,
        };

    /* ---------------- gate 1: never observed running ---------------- */

    [Fact]
    public void NeverObservedRunning_IsNeutral_NotStopped()
    {
        var row = Row(agentRunning: false, everSeenRunning: false, collectionTime: Now.AddMinutes(-2));

        var state = AgentHeaderStatus.Classify(row, Now);
        var (text, isAlert) = AgentHeaderStatus.DescribeSingle(row, Now);

        Assert.Equal(AgentHeaderStatus.AgentHeaderState.NeverObserved, state);
        Assert.False(isAlert);
        Assert.DoesNotContain("Stopped", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("never observed", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void NeverObservedRunning_DoesNotClaimAgentIsAbsent()
    {
        /* We can see that Agent has not been observed running. We cannot see whether it is installed, so the
           header must not say "not installed" / "not present" — that is a claim the data does not support. */
        var row = Row(agentRunning: false, everSeenRunning: false, collectionTime: Now.AddMinutes(-2));

        var (text, _) = AgentHeaderStatus.DescribeSingle(row, Now);

        Assert.DoesNotContain("not installed", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("not present", text, StringComparison.OrdinalIgnoreCase);
    }

    /* ---------------- gate 2: staleness ---------------- */

    [Fact]
    public void StaleReading_IsUnknown_NotStopped()
    {
        var row = Row(agentRunning: false, everSeenRunning: true, collectionTime: Now.AddHours(-3));

        var state = AgentHeaderStatus.Classify(row, Now);
        var (text, isAlert) = AgentHeaderStatus.DescribeSingle(row, Now);

        Assert.Equal(AgentHeaderStatus.AgentHeaderState.Unknown, state);
        Assert.False(isAlert);
        Assert.Contains("unknown", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Stopped", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void StaleReading_IsUnknown_EvenWhenAgentWasRunning()
    {
        /* Staleness is about whether we KNOW, not about which way the old reading pointed. A days-old
           "Running" is no more current than a days-old "Stopped". */
        var row = Row(agentRunning: true, everSeenRunning: true, collectionTime: Now.AddDays(-2));

        Assert.Equal(AgentHeaderStatus.AgentHeaderState.Unknown, AgentHeaderStatus.Classify(row, Now));
    }

    [Fact]
    public void NeverCollected_IsUnknown()
    {
        var row = Row(agentRunning: false, everSeenRunning: false, collectionTime: null);

        var (text, isAlert) = AgentHeaderStatus.DescribeSingle(row, Now);

        Assert.Equal(AgentHeaderStatus.AgentHeaderState.Unknown, AgentHeaderStatus.Classify(row, Now));
        Assert.False(isAlert);
        Assert.Contains("never collected", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void StaleWindowBoundary_IsExclusive()
    {
        var justInside = Row(false, true, Now - AgentHeaderStatus.StaleWindow + TimeSpan.FromSeconds(1));
        var exactlyAt = Row(false, true, Now - AgentHeaderStatus.StaleWindow);

        Assert.Equal(AgentHeaderStatus.AgentHeaderState.Stopped, AgentHeaderStatus.Classify(justInside, Now));
        Assert.Equal(AgentHeaderStatus.AgentHeaderState.Unknown, AgentHeaderStatus.Classify(exactlyAt, Now));
    }

    [Fact]
    public void StaleWindowIsLongerThanTheCollectorCadence()
    {
        /* agent_status collects every 5 minutes by default. A stale window at or under that would render a
           perfectly healthy Agent as unknown most of the time. */
        Assert.True(AgentHeaderStatus.StaleWindow > TimeSpan.FromMinutes(5));
    }

    [Fact]
    public void FutureCollectionTime_CountsAsFresh_SoARealOutageStillAlerts()
    {
        /* Clock skew between the monitored server and this host puts the timestamp ahead of now. That is
           nearer than the stale window, so the reading is treated as current — deliberately. Treating a
           skewed clock as "cannot judge" would silently suppress a genuinely stopped Agent on every server
           whose clock runs fast, which is the expensive direction to be wrong in. */
        var row = Row(agentRunning: false, everSeenRunning: true, collectionTime: Now.AddHours(1));

        var (text, isAlert) = AgentHeaderStatus.DescribeSingle(row, Now);

        Assert.Equal(AgentHeaderStatus.AgentHeaderState.Stopped, AgentHeaderStatus.Classify(row, Now));
        Assert.True(isAlert);
        Assert.Contains("Stopped", text, StringComparison.OrdinalIgnoreCase);
    }

    /* ---------------- the real alarm still fires ---------------- */

    [Fact]
    public void SeenRunning_Fresh_AndStopped_StillAlerts()
    {
        var row = Row(agentRunning: false, everSeenRunning: true, collectionTime: Now.AddMinutes(-2));

        var (text, isAlert) = AgentHeaderStatus.DescribeSingle(row, Now);

        Assert.Equal(AgentHeaderStatus.AgentHeaderState.Stopped, AgentHeaderStatus.Classify(row, Now));
        Assert.True(isAlert);
        Assert.Contains("Stopped", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RunningNow_IsItsOwnEvidence_EvenWithoutHistory()
    {
        /* A running Agent proves the capability, so it short-circuits the ever-seen probe exactly as the
           Darling evaluator does — this must never be classified NeverObserved. */
        var row = Row(agentRunning: true, everSeenRunning: false, collectionTime: Now.AddMinutes(-1));

        var (_, isAlert) = AgentHeaderStatus.DescribeSingle(row, Now);

        Assert.Equal(AgentHeaderStatus.AgentHeaderState.Running, AgentHeaderStatus.Classify(row, Now));
        Assert.False(isAlert);
    }

    /* ---------------- fleet roll-up ---------------- */

    [Fact]
    public void FleetRollup_ExcludesNeverObservedFromStoppedCount()
    {
        var rows = new List<AgentStatusRow>
        {
            Row(true, true, Now.AddMinutes(-1), 1, "SQL01"),
            Row(false, false, Now.AddMinutes(-1), 2, "CONTAINER"),
            Row(false, false, Now.AddMinutes(-1), 3, "EXPRESS"),
        };

        var (text, isAlert) = AgentHeaderStatus.DescribeFleet(rows, Now);

        Assert.False(isAlert);
        Assert.DoesNotContain("stopped", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("1/1 running", text);
        Assert.Contains("2 not using Agent", text);
    }

    [Fact]
    public void FleetRollup_ReportsStaleSeparately()
    {
        var rows = new List<AgentStatusRow>
        {
            Row(true, true, Now.AddMinutes(-1), 1, "SQL01"),
            Row(false, true, Now.AddDays(-1), 2, "SQL02"),
        };

        var (text, isAlert) = AgentHeaderStatus.DescribeFleet(rows, Now);

        Assert.False(isAlert);
        Assert.Contains("1 unknown", text);
        Assert.DoesNotContain("stopped", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FleetRollup_StillAlertsOnARealStoppedAgent()
    {
        var rows = new List<AgentStatusRow>
        {
            Row(true, true, Now.AddMinutes(-1), 1, "SQL01"),
            Row(false, true, Now.AddMinutes(-1), 2, "SQL02"),
            Row(false, false, Now.AddMinutes(-1), 3, "CONTAINER"),
        };

        var (text, isAlert) = AgentHeaderStatus.DescribeFleet(rows, Now);

        Assert.True(isAlert);
        Assert.Contains("1/2 running, 1 stopped", text);
        Assert.Contains("1 not using Agent", text);
    }

    [Fact]
    public void FleetRollup_NothingJudgeable_IsNeutral()
    {
        var rows = new List<AgentStatusRow>
        {
            Row(false, false, Now.AddMinutes(-1), 1, "CONTAINER"),
            Row(false, true, Now.AddDays(-1), 2, "SQL02"),
        };

        var (text, isAlert) = AgentHeaderStatus.DescribeFleet(rows, Now);

        Assert.False(isAlert);
        Assert.DoesNotContain("stopped", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("none to report", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FleetRollup_EmptyIsBlank()
    {
        var (text, isAlert) = AgentHeaderStatus.DescribeFleet(new List<AgentStatusRow>(), Now);

        Assert.Equal(string.Empty, text);
        Assert.False(isAlert);
    }
}
