/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What the Job History header is entitled to SAY about a server's SQL Agent, given the collected
/// <c>agent_status</c> evidence (#1720).
///
/// <para>The header used to render a red "Agent: Stopped" for any row whose <c>agent_running</c> was false,
/// which is a claim the data does not support in two common cases: a server where Agent has never run (a
/// container built without it, Express, a Linux-minimal image) is not "stopped", it simply does not use
/// Agent; and a reading hours old says nothing about the service NOW. Both rendered identically to a real
/// outage, so the one state an operator must not learn to ignore was the one they saw constantly.</para>
///
/// <para>This is the presentation-side twin of the discipline #1719 shipped for Darling's "Agent Not Running"
/// alert: judge only on a FRESH reading, and only where Agent has been OBSERVED RUNNING at least once in the
/// retained history. Same two gates, same order, same evidence — the alert stays silent, the header stays
/// neutral. Kept as a pure function so it is testable without standing up WPF.</para>
/// </summary>
public static class AgentHeaderStatus
{
    /// <summary>
    /// How old the newest <c>agent_status</c> row may be and still describe the service NOW.
    ///
    /// <para>30 minutes, matching Darling's <c>DarlingSelfAlertEvaluator.StaleWindow</c> and, through it, the
    /// Dashboard's CollectionStaleThresholdMinutes — the header and the alert must not disagree about whether
    /// the same row is current. That is six cycles of the collector's 5-minute default cadence
    /// (<c>CollectorScheduleDefaults</c>), so a healthy server is never mistaken for a stale one.</para>
    ///
    /// <para>Deliberately NOT <c>ServerHealthThresholds.StaleThreshold</c>: that one is two minutes, derived
    /// from the FASTEST collector's one-minute cadence, and agent_status runs every five. Reusing it would
    /// render an entirely healthy Agent as stale most of the time.</para>
    /// </summary>
    public static readonly TimeSpan StaleWindow = TimeSpan.FromMinutes(30);

    /// <summary>What the evidence supports saying about one server's Agent.</summary>
    public enum AgentHeaderState
    {
        /// <summary>Fresh reading, service running. Neutral.</summary>
        Running,

        /// <summary>Fresh reading, service stopped, and Agent HAS been seen running here. The real alarm.</summary>
        Stopped,

        /// <summary>
        /// Fresh reading, service stopped, and Agent has never been observed running in the retained history.
        /// This server does not use Agent; there is nothing to report. Neutral.
        /// </summary>
        NeverObserved,

        /// <summary>No reading, or one older than <see cref="StaleWindow"/>. We do not know. Neutral.</summary>
        Unknown,
    }

    /// <summary>
    /// Classifies one server's row. A currently-running Agent is its own proof of capability, so it short-
    /// circuits the ever-seen probe exactly as the Darling evaluator does.
    /// </summary>
    public static AgentHeaderState Classify(AgentStatusRow row, DateTime utcNow)
    {
        ArgumentNullException.ThrowIfNull(row);

        if (!row.CollectionTime.HasValue || utcNow - row.CollectionTime.Value >= StaleWindow)
        {
            return AgentHeaderState.Unknown;
        }

        if (row.AgentRunning)
        {
            return AgentHeaderState.Running;
        }

        return row.EverSeenRunning ? AgentHeaderState.Stopped : AgentHeaderState.NeverObserved;
    }

    /// <summary>
    /// The single-server header line, plus whether it should be drawn as an alarm. Only
    /// <see cref="AgentHeaderState.Stopped"/> is an alarm — the other three are ordinary conditions.
    /// </summary>
    public static (string Text, bool IsAlert) DescribeSingle(AgentStatusRow row, DateTime utcNow)
    {
        ArgumentNullException.ThrowIfNull(row);

        return Classify(row, utcNow) switch
        {
            AgentHeaderState.Running =>
                ($"Agent: Running · Next run: {row.NextScheduledRunLocal}", false),

            AgentHeaderState.Stopped =>
                ($"Agent: {row.StatusDisplay} · Next run: {row.NextScheduledRunLocal}", true),

            /* Not "not installed" — we cannot see that. Only that it has never been seen running. */
            AgentHeaderState.NeverObserved =>
                ("Agent: never observed running", false),

            _ => ($"Agent: unknown{DescribeAge(row.CollectionTime, utcNow)}", false),
        };
    }

    /// <summary>
    /// The all-servers roll-up. Only servers we can actually judge count toward the running/stopped tally;
    /// servers that never use Agent and servers whose reading is stale are reported separately rather than
    /// being folded into "stopped", which is what made the old roll-up overstate the number of problems.
    /// </summary>
    public static (string Text, bool IsAlert) DescribeFleet(IReadOnlyList<AgentStatusRow> rows, DateTime utcNow)
    {
        ArgumentNullException.ThrowIfNull(rows);

        if (rows.Count == 0)
        {
            return (string.Empty, false);
        }

        int running = 0, stopped = 0, neverObserved = 0, unknown = 0;

        foreach (var row in rows)
        {
            switch (Classify(row, utcNow))
            {
                case AgentHeaderState.Running: running++; break;
                case AgentHeaderState.Stopped: stopped++; break;
                case AgentHeaderState.NeverObserved: neverObserved++; break;
                default: unknown++; break;
            }
        }

        var judged = running + stopped;

        var text = judged > 0
            ? (stopped > 0
                ? $"Agents: {running}/{judged} running, {stopped} stopped"
                : $"Agents: {running}/{judged} running")
            : "Agents: none to report";

        if (neverObserved > 0)
        {
            text += $", {neverObserved} not using Agent";
        }

        if (unknown > 0)
        {
            text += $", {unknown} unknown";
        }

        return (text, stopped > 0);
    }

    /// <summary>Renders the age of a stale reading, so "unknown" says how far out of date it is.</summary>
    private static string DescribeAge(DateTime? collectionTimeUtc, DateTime utcNow)
    {
        if (!collectionTimeUtc.HasValue)
        {
            return " (never collected)";
        }

        /* Always positive here: a future timestamp (clock skew) is nearer than the stale window, so it
           classifies Fresh and never reaches this method. */
        var age = utcNow - collectionTimeUtc.Value;

        var rendered = age.TotalHours >= 24
            ? $"{(int)age.TotalDays}d"
            : age.TotalMinutes >= 60
                ? $"{(int)age.TotalHours}h"
                : $"{(int)age.TotalMinutes}m";

        return $" (reading {rendered} old)";
    }
}
