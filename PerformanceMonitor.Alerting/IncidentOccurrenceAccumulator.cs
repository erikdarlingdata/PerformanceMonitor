/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One fingerprint's occurrence accounting as it is persisted between observations (#2216).
/// </summary>
/// <param name="TotalOccurrences">
/// Occurrences of this fingerprint accumulated over the life of the incident. Monotonic until the
/// incident ends.
/// </param>
/// <param name="ObservedWindowCount">
/// The window count this accumulator has ALREADY counted. Its own high-water mark, deliberately
/// separate from <c>RollingCountAlertGate</c>'s watermark: that one advances only when an alert
/// fires (so an event arriving during a cooldown is still reported later), while this one advances
/// on every observation (so the same event is never counted twice). Sharing one watermark between
/// the two jobs would break whichever job lost the argument.
/// </param>
/// <param name="IncidentStartedUtc">
/// When this fingerprint's incident was first observed. Constant for the incident's life; a change
/// is how a consumer distinguishes a new incident from a continuing one.
/// </param>
/// <param name="LastObservedUtc">
/// When this row was last touched. Not display data — it is what makes a row's staleness decidable
/// (see <see cref="IncidentOccurrenceAccumulator.Accumulate"/>), so a row abandoned by a crash
/// cannot silently suppress the next incident on the same fingerprint.
/// </param>
public readonly record struct IncidentOccurrenceState(
    long TotalOccurrences,
    int ObservedWindowCount,
    DateTime IncidentStartedUtc,
    DateTime LastObservedUtc);

/// <summary>
/// Turns the per-fingerprint window gauge on <see cref="AlertIncident.OccurrenceCount"/> into the
/// monotonic per-fingerprint total on <see cref="AlertIncident.TotalOccurrences"/> (#2216).
///
/// <para>
/// The reported problem: <c>OccurrenceCount</c> counts events inside the groupers' rolling read
/// window, so it rises as events arrive and falls as they age out. A consumer that only ever sees
/// throttled deliveries — one per cooldown, per #1154's per-fingerprint cooldown — cannot recover
/// from a sequence of gauge readings how many events actually happened, because a reading of 3
/// followed by a reading of 3 is indistinguishable from nothing-happened and
/// three-happened-while-three-aged-out.
/// </para>
///
/// <para>
/// The accumulation is the gauge's RISE above what has already been counted, per fingerprint:
/// <c>total += window - min(observedWindow, window)</c>. The inner <c>min</c> is the decay — as
/// events age out the gauge falls, and the already-counted mark has to fall with it or a later rise
/// back to the same level would read as no new events. This is the same decay
/// <c>RollingCountAlertGate</c> applies to its own watermark, for the same reason, and it is why the
/// two marks cannot be one mark.
/// </para>
///
/// <para>
/// EXACTNESS BOUND, stated plainly because a counter that quietly undercounts is worse than a gauge
/// that is honestly a gauge: this is a lower bound on occurrences, exact whenever the read window
/// outlives the gap between OBSERVATIONS. The precise loss is one occurrence per event that ages out
/// of the window between two observations — a retirement and an arrival cancel in the gauge, so the
/// arrival is invisible.
/// </para>
///
/// <para>
/// That is why the caller must observe on every SWEEP, not on every delivery. With a one-hour window
/// and a sweep measured in seconds, an event can only be missed if it arrives and ages out inside a
/// single sweep interval, which cannot happen for a window this long — so per-sweep observation is
/// exact in practice. Observing only at delivery time is NOT: any event the window retires during a
/// cooldown masks an arrival, so a long incident under sustained load undercounts by roughly the
/// number of events that aged out while the cooldown was suppressing delivery. That was the shipped
/// behavior in the first cut of #2216 and it made this class's own contract false; the review of
/// PR #2221 caught it.
/// </para>
///
/// <para>
/// What no arrangement of a window gauge can recover is an occurrence that both arrives AND ages out
/// between two observations. Counting those would take an event-identity watermark at the collector,
/// which is a different feature.
/// </para>
///
/// <para>
/// FIRST CONTACT counts the whole window. With no usable persisted state the accumulator cannot know
/// which of the events already in the window it would have counted before, so the total starts at
/// the window count and <c>IncidentStartedUtc</c> is stamped now. That is the same first-read
/// behavior the gauge has always had, and it is why the incident timestamp travels with the total: a
/// consumer seeing the total restart can check whether the incident restarted with it.
/// </para>
///
/// <para>
/// STALENESS is a correctness rule, not housekeeping. State is written when an alert is delivered and
/// deleted when the condition clears, but a host that dies while an incident is active leaves a row
/// behind with no clearing sweep to remove it. If that row were trusted when the same fingerprint
/// recurred weeks later, its high observed-mark would decay to the new window count, the recurrence
/// would read as nothing new, and the total plus the incident timestamp would both describe an
/// incident that ended long ago — an undercount reported with a confident timestamp, which is worse
/// than no total at all. So a row untouched for longer than <c>staleAfter</c> is treated as absent.
/// The horizon belongs to the caller because only the caller knows its read window: any value at or
/// above the window works, since a row inside the window is describing the same events the gauge is.
/// </para>
///
/// <para>
/// Pure function: no clock, no I/O, no store. The caller supplies <c>nowUtc</c> and persists
/// <see cref="Result.States"/> — matching <see cref="RollingCountAlertGate"/>, so both are testable
/// without a host.
/// </para>
/// </summary>
public static class IncidentOccurrenceAccumulator
{
    /// <param name="Incidents">
    /// The input incidents with <see cref="AlertIncident.TotalOccurrences"/> and
    /// <see cref="AlertIncident.IncidentStartedUtc"/> filled in. Same order, same count.
    /// </param>
    /// <param name="States">
    /// The state to persist for this (server, metric) — keyed by dedup fingerprint. This is the
    /// COMPLETE set for the metric: a fingerprint the caller previously persisted and which is
    /// absent here has no events left in the window, so its incident has ended and its row must be
    /// removed rather than left to go stale. The store contract is therefore replace-the-set, not
    /// upsert-each-row.
    /// </param>
    /// <param name="Changed">
    /// True when the caller should write. The accumulator observes every SWEEP, not only the sweeps that
    /// deliver an alert (that is what makes the arithmetic exact), so a naive "always write" would put a
    /// store round trip on every metric of every server on every sweep — on a 52-server fleet at a 30-second
    /// cadence, thousands of writes an hour to record nothing.
    ///
    /// <para>So this is true when something SUBSTANTIVE moved (a total, a mark, a fingerprint appearing or
    /// disappearing) — and additionally on a HEARTBEAT, when the oldest surviving row has not been touched
    /// for half the staleness horizon. The heartbeat is not an optimization detail: staleness is judged from
    /// <see cref="IncidentOccurrenceState.LastObservedUtc"/>, so an incident whose gauge sits perfectly flat
    /// for longer than the horizon would be judged stale and reset ITSELF. Half the horizon means a live
    /// incident is always refreshed at least once before it could expire, at two writes per horizon rather
    /// than one per sweep.</para>
    /// </param>
    public readonly record struct Result(
        IReadOnlyList<AlertIncident> Incidents,
        IReadOnlyDictionary<string, IncidentOccurrenceState> States,
        bool Changed);

    /// <summary>
    /// Accumulates one observation of a metric's incidents. Returns the incidents carrying their
    /// totals plus the full state set to persist.
    /// </summary>
    /// <param name="incidents">
    /// This observation's incidents for ONE (server, metric), each carrying its window count in
    /// <see cref="AlertIncident.OccurrenceCount"/>. Null or empty means the window is empty: the
    /// result clears the metric's state.
    /// </param>
    /// <param name="persisted">
    /// State loaded for the same (server, metric), or null when the host does not persist occurrence
    /// state. A null/empty map degrades to "every delivery is a fresh incident", which reports the
    /// total as equal to the window count — the pre-#2216 information, never a false zero.
    /// </param>
    /// <param name="nowUtc">
    /// Observation time. Stamped as <c>IncidentStartedUtc</c> for new incidents, as
    /// <c>LastObservedUtc</c> for all of them, and used as the staleness reference.
    /// </param>
    /// <param name="staleAfter">
    /// How long a persisted row may go untouched before it is treated as absent (see class remarks).
    /// Pass the groupers' read window or longer. <see cref="TimeSpan.Zero"/> or negative disables
    /// the rule, trusting every row however old — available for tests that want the unguarded
    /// behavior, not for hosts.
    /// </param>
    public static Result Accumulate(
        IReadOnlyList<AlertIncident>? incidents,
        IReadOnlyDictionary<string, IncidentOccurrenceState>? persisted,
        DateTime nowUtc,
        TimeSpan staleAfter)
    {
        int persistedCount = persisted?.Count ?? 0;

        if (incidents is not { Count: > 0 })
        {
            /* Nothing in the window. Any persisted row is a finished incident, so the caller has a
               write to do (a delete) exactly when it had state. */
            return new Result(
                Array.Empty<AlertIncident>(),
                new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal),
                Changed: persistedCount > 0);
        }

        var states = new Dictionary<string, IncidentOccurrenceState>(incidents.Count, StringComparer.Ordinal);
        var accumulated = new List<AlertIncident>(incidents.Count);
        bool substantive = false;
        bool heartbeatDue = false;

        foreach (var incident in incidents)
        {
            /* A blank fingerprint cannot be keyed (AlertFingerprint returns null rather than an
               empty key, and IncidentCooldown filters blanks defensively for the same reason). Pass
               it through untouched instead of inventing a key that would pool unrelated incidents
               under one total. */
            if (incident is null || string.IsNullOrEmpty(incident.DedupKey))
            {
                accumulated.Add(incident!);
                continue;
            }

            /* Negative cannot arrive from the groupers, but clamping here means a bad count can only
               fail to advance the total, never walk it backwards — the one property a monotonic
               counter has to keep. */
            int window = incident.OccurrenceCount > 0 ? incident.OccurrenceCount : 0;

            /* The working set is consulted BEFORE the persisted one: a fingerprint repeated within a
               single observation then accumulates against the mark its first appearance just set,
               instead of counting the same window twice against a stale mark. */
            bool usable = states.TryGetValue(incident.DedupKey, out var prior);
            if (!usable && persisted is not null && persisted.TryGetValue(incident.DedupKey, out prior))
            {
                usable = !IsStale(prior, nowUtc, staleAfter);
            }

            long total;
            int observed;
            DateTime started;

            if (usable)
            {
                /* Decay first (see class remarks), then count only the rise above the decayed mark. */
                observed = prior.ObservedWindowCount < window ? prior.ObservedWindowCount : window;
                total = prior.TotalOccurrences + (window - observed);
                observed = window;
                started = prior.IncidentStartedUtc;
            }
            else
            {
                total = window;
                observed = window;
                started = nowUtc;
            }

            accumulated.Add(incident with
            {
                TotalOccurrences = total,
                IncidentStartedUtc = started,
            });

            if (!substantive)
            {
                /* A fingerprint the store has never seen, or one whose numbers moved. The touch timestamp is
                   deliberately NOT part of this comparison — it moves on every sweep, and treating that as a
                   change is exactly the always-write behavior the heartbeat exists to avoid. */
                substantive = !usable
                    || total != prior.TotalOccurrences
                    || observed != prior.ObservedWindowCount
                    || started != prior.IncidentStartedUtc;
            }

            if (!heartbeatDue && usable && staleAfter > TimeSpan.Zero
                && nowUtc - prior.LastObservedUtc >= HeartbeatFraction(staleAfter))
            {
                heartbeatDue = true;
            }

            states[incident.DedupKey] = new IncidentOccurrenceState(total, observed, started, nowUtc);
        }

        /* A fingerprint that was persisted and is no longer in the window is a finished incident — its
           removal is substantive even when every surviving fingerprint held steady. */
        if (!substantive && persistedCount != states.Count)
        {
            substantive = true;
        }

        /* No keyable incidents at all: a write only if there is persisted state to clear. */
        if (states.Count == 0)
        {
            return new Result(accumulated, states, Changed: persistedCount > 0);
        }

        return new Result(accumulated, states, Changed: substantive || heartbeatDue);
    }

    /* Half the staleness horizon. A live incident is refreshed at least once before it could expire, and a
       flat gauge costs two writes per horizon instead of one per sweep. Anything closer to the horizon risks
       a sweep landing after expiry; anything much shorter gives the writes back. */
    private static TimeSpan HeartbeatFraction(TimeSpan staleAfter) =>
        TimeSpan.FromTicks(staleAfter.Ticks / 2);

    /* A row untouched for longer than the horizon is describing events that have left the window, so
       it cannot inform this observation. Rows stamped in the FUTURE are trusted rather than
       discarded: that is a clock going backwards (NTP correction, host migration), and treating a
       live incident as stale would reset a total and its start time on a machine whose clock simply
       stepped. A zero or negative horizon disables the rule. */
    private static bool IsStale(IncidentOccurrenceState state, DateTime nowUtc, TimeSpan staleAfter)
    {
        if (staleAfter <= TimeSpan.Zero)
        {
            return false;
        }

        return nowUtc - state.LastObservedUtc > staleAfter;
    }
}
