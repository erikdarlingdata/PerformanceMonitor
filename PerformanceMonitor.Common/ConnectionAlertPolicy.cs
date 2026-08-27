/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>What one connection observation should announce, if anything.</summary>
public enum ConnectionAlertDecision
{
    /// <summary>Nothing to announce: steady online, a silent baseline, or a standing outage inside the
    /// re-fire window (or with re-fire off).</summary>
    None,

    /// <summary>online → offline: the classic edge.</summary>
    Lost,

    /// <summary>offline → online.</summary>
    Restored,

    /// <summary>The FIRST-ever observation of this server found it already down (#1659, opt-in). Without
    /// this, an app that starts mid-outage never announces the outage at all — there was no edge to see.</summary>
    AlreadyDownAtFirstSight,

    /// <summary>A standing outage re-announcement (#1659, opt-in): the server is still down and the re-fire
    /// interval has elapsed since the last down alert. Callers should deliver this under the SAME metric
    /// name as <see cref="Lost"/> — downstream automation (the reporting user's webhook-driven auto-heal
    /// loop) matches on the metric name, and a re-fire exists precisely to re-trigger it.</summary>
    StillDown,
}

/// <summary>
/// The single definition of when a connection alert fires, shared by Lite's tray/alert loop and Darling's
/// self-alert state machine (#1659) — the <see cref="SqlErrorClassification"/> discipline: these rules lived
/// as two separate implementations, which is how they would drift.
///
/// <para>Plain edge detection fires ONCE per transition and is silent on the first-ever observation. That is
/// correct in isolation and wrong for two real cases: an app that starts while a server is already down
/// (no edge ever existed), and a standing outage driving external automation that needs re-triggering.
/// Both extensions are OPT-IN — the default behavior is byte-for-byte the old edge-only semantics.</para>
///
/// <para>The two opt-ins interlock after a mid-outage restart: even with the startup announcement OFF, the
/// first observation re-baselines silently, and the NEXT poll is offline→offline with no recorded down
/// alert — which the re-fire path treats as due immediately. So re-fire alone eventually re-announces an
/// outage the restart forgot; the startup opt-in only controls whether that happens on sight one.</para>
/// </summary>
public static class ConnectionAlertPolicy
{
    /// <param name="previousOnline">The last observed state, or null when this server has never been observed.</param>
    /// <param name="online">The state this poll observed.</param>
    /// <param name="alertWhenAlreadyDownAtFirstSight">Opt-in: announce a server that is down on its
    /// first-ever observation instead of treating it as a silent baseline.</param>
    /// <param name="refireInterval">Opt-in: re-announce a standing outage each time this much time has
    /// passed since the last down alert. Null or non-positive = off (classic edge-only).</param>
    /// <param name="lastDownAlertUtc">When the last down alert for this server fired, if one has. Callers
    /// stamp it on every Lost / AlreadyDownAtFirstSight / StillDown they deliver and clear it on Restored.
    /// Null while the server is down means the outage has never been announced (a restart lost the state, or
    /// the original edge fired before re-fire was enabled) — the re-fire path treats that as due now.</param>
    /// <param name="nowUtc">The caller's clock, injected so the decision pins under test.</param>
    public static ConnectionAlertDecision Decide(
        bool? previousOnline,
        bool online,
        bool alertWhenAlreadyDownAtFirstSight,
        TimeSpan? refireInterval,
        DateTime? lastDownAlertUtc,
        DateTime nowUtc)
    {
        if (previousOnline is null)
        {
            return !online && alertWhenAlreadyDownAtFirstSight
                ? ConnectionAlertDecision.AlreadyDownAtFirstSight
                : ConnectionAlertDecision.None;
        }

        if (previousOnline == true && !online)
        {
            return ConnectionAlertDecision.Lost;
        }

        if (previousOnline == false && online)
        {
            return ConnectionAlertDecision.Restored;
        }

        /* #2426: the window predicate is AlertRefireWindow's, not a fourth line of this method — the
           AG re-fire needs the identical "non-positive is off, no stamp is due now" rules, and a second
           hand-written copy of them is how the two would eventually disagree. */
        if (previousOnline == false && !online
            && AlertRefireWindow.IsDue(refireInterval, lastDownAlertUtc, nowUtc))
        {
            return ConnectionAlertDecision.StillDown;
        }

        return ConnectionAlertDecision.None;
    }
}
