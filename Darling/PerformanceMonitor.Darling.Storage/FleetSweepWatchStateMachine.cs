/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The watch-item lifecycle for the fleet sweep (#3466): pure transition arithmetic over the
/// hysteresis counters <c>collect.fleet_sweep_watch_items</c> stores, shared here so the sweep
/// engine's gates and the store's rows can never disagree about what a state name means.
///
/// <para><b>Why hysteresis is the contract and not a tuning choice.</b> The evidence day's finding,
/// carried verbatim in #3466's requirements: every escalation gate that fired usefully required
/// consecutive-read confirmation, and every single-sample gate that existed historically had already
/// been documented as noise by prior operators. So a single elevated read cannot OPEN an item and a
/// single quiet read cannot CLOSE one — both directions, because the failure modes differ. A
/// single-sample open pages operators about blips; a single-sample close retires an episode the next
/// sweep would have re-opened, and the report then presents one continuing problem as a parade of
/// new ones, destroying the carried-with-trend reading the sweep exists to provide.</para>
///
/// <para><b>The states.</b> <c>pending</c> — sighted, below the entry bar; the sweep engine tracks
/// it but the report does not carry it as open. <c>open</c> — the entry bar was met THIS sweep (the
/// "opened" transition the report renders). <c>carried</c> — open in a prior sweep and not yet
/// closed, whether this sweep's read held or lapsed below the exit bar (the standing miss count IS
/// the trend the "carried" rendering shows). <c>closed</c> — the exit bar was met; the row keeps its
/// stamps as the record of the episode, and a later sighting starts a fresh entry count rather than
/// resuming the old one, because evidence separated by a confirmed close is evidence of a NEW
/// episode.</para>
///
/// <para><b>Why the two bars are constants here and not store knobs.</b> Both are the spec's own
/// figures: "escalate on two consecutive elevated reads" is the entry bar, and "closed after two
/// quiet hours" is the exit bar at the default hourly cadence — two consecutive quiet SWEEPS, which
/// is the unit the counters actually measure, so the bar scales with the configured cadence instead
/// of silently loosening when an operator sweeps less often. Per-item thresholds are a knob nobody
/// has measured a need for; when one arrives it arrives as data on the watch item with these as the
/// defaults, the #3297 route, rather than as a second compile-time constant to migrate later.</para>
/// </summary>
public static class FleetSweepWatchStateMachine
{
    /// <summary>Sighted, below the entry bar. Not rendered as open — a pending item is the memory
    /// that makes the SECOND consecutive read confirmable, nothing more.</summary>
    public const string Pending = "pending";

    /// <summary>The entry bar was met this sweep — the report's "opened" transition.</summary>
    public const string Open = "open";

    /// <summary>Open in a prior sweep and not yet closed. A lapse below the exit bar stays carried,
    /// with the standing miss count as the trend.</summary>
    public const string Carried = "carried";

    /// <summary>The exit bar was met. The row remains as the episode's record; a re-sighting starts
    /// a fresh entry count.</summary>
    public const string Closed = "closed";

    /// <summary>Consecutive sweeps the condition must hold before an item opens — the spec's
    /// "escalate on two consecutive elevated reads". One elevated read is a blip until a second
    /// sweep confirms it.</summary>
    public const int EntryConsecutiveSweeps = 2;

    /// <summary>Consecutive quiet sweeps before an open item closes — the spec's "closed after two
    /// quiet hours", stated in sweeps so the bar rides the configured cadence.</summary>
    public const int ExitConsecutiveSweeps = 2;

    /// <summary>
    /// One transition's outcome: the state to store, the counters as they now stand, and the two
    /// edge flags the report renders ("opened" / "closed" are per-sweep transitions, not states, so
    /// they are answered here rather than re-derived by comparing rows).
    /// </summary>
    public readonly record struct WatchAdvance(
        string State,
        int ConsecutiveHits,
        int ConsecutiveMisses,
        bool JustOpened,
        bool JustClosed);

    /// <summary>
    /// The first sighting of an item with no row yet. One hit — which opens immediately only if the
    /// entry bar is ever lowered to one, so the bar has exactly one home rather than a copy here.
    /// </summary>
    public static WatchAdvance FirstSighting()
    {
        return 1 >= EntryConsecutiveSweeps
            ? new WatchAdvance(Open, 1, 0, JustOpened: true, JustClosed: false)
            : new WatchAdvance(Pending, 1, 0, JustOpened: false, JustClosed: false);
    }

    /// <summary>
    /// Advances one item by one sweep. <paramref name="conditionHeld"/> is the engine's reading this
    /// sweep; the state and counters are the stored row's. Pure, so the hysteresis boundaries are
    /// unit-testable without a store — <c>FleetSweepStateRungTests</c> pins them exactly.
    /// </summary>
    public static WatchAdvance Advance(
        string state, bool conditionHeld, int consecutiveHits, int consecutiveMisses)
    {
        if (conditionHeld)
        {
            /* A hit after a confirmed close is a NEW episode's first read, never the old episode's
               continuation — resuming the old count would let two blips a week apart open an item
               that no two consecutive sweeps ever confirmed. */
            var hits = state == Closed ? 1 : consecutiveHits + 1;

            if (state == Open || state == Carried)
            {
                /* Already open: the hit extends the episode and clears any standing misses, so a
                   lapse-and-recover cannot creep toward the exit bar across recoveries. */
                return new WatchAdvance(Carried, hits, 0, JustOpened: false, JustClosed: false);
            }

            return hits >= EntryConsecutiveSweeps
                ? new WatchAdvance(Open, hits, 0, JustOpened: true, JustClosed: false)
                : new WatchAdvance(Pending, hits, 0, JustOpened: false, JustClosed: false);
        }

        var misses = consecutiveMisses + 1;

        if (state == Open || state == Carried)
        {
            return misses >= ExitConsecutiveSweeps
                ? new WatchAdvance(Closed, 0, misses, JustOpened: false, JustClosed: true)
                : new WatchAdvance(Carried, 0, misses, JustOpened: false, JustClosed: false);
        }

        /* Pending + miss: the entry count starts over — a single sample cannot open, so an
           interrupted run is no run at all. Closed + miss: closed stays closed; the misses
           accumulate harmlessly on a row nothing reads them from. */
        return new WatchAdvance(
            state == Closed ? Closed : Pending, 0, misses, JustOpened: false, JustClosed: false);
    }
}
