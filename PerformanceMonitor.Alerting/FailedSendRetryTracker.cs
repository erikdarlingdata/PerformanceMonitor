/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// When an alert that no channel delivered is due again (#4795), per alert key. A connection or availability group
/// alert is an edge, and its own state machine has nothing that brings it back once the edge has been taken, so
/// the app records here what its send reported and asks here when to try again.
///
/// <para><see cref="Record"/> takes the send's <see cref="AlertDelivery"/>. When every channel failed
/// (<see cref="FailedSendBackoff.EveryChannelFailed"/>) the key is due again after
/// <see cref="AlertEngine.ChannelFailureRetryDelay"/> of the failures in a row: a minute, doubling, never more than
/// the cap the caller passes (the alert cooldown). Any other answer (delivered, partly delivered, muted,
/// throttled, folded, unreported) clears the key and its streak, because a retry of a partly delivered alert would
/// send it a second time down the channel that worked. <see cref="Clear"/> ends a key without a send, for the
/// outage that came back on its own, and <see cref="ClearPrefix"/> ends every key of one scope, for a server removed
/// from monitoring.</para>
///
/// <para>In memory only, and safe to call from several threads (Lite records from the continuation of a send
/// task). A restart forgets every pending retry, which is the same state a restart leaves the edge itself in.</para>
/// </summary>
public sealed class FailedSendRetryTracker
{
    /// <summary>The one family every key of this tracker is counted under in the shared streak bookkeeping.</summary>
    private const string Family = "retry";

    /// <summary>One key's pending retry: when it is due, and the cap it was recorded under, which is the longest wait
    /// <see cref="Record"/> can set (the delay is never more than the cap), so the furthest ahead of the clock a due
    /// time is ever stamped.</summary>
    private readonly record struct Pending(DateTime DueUtc, TimeSpan Cap);

    private readonly FailedSendBackoff _streaks = new();
    private readonly ConcurrentDictionary<string, Pending> _due = new(StringComparer.Ordinal);

    /// <inheritdoc cref="Record(string, AlertDelivery?, DateTime, TimeSpan, out TimeSpan, out int)"/>
    public bool Record(string key, AlertDelivery? delivery, DateTime nowUtc, TimeSpan cap) =>
        Record(key, delivery, nowUtc, cap, out _, out _);

    /// <summary>
    /// Records one send's answer for <paramref name="key"/> and returns true when every channel failed, in which
    /// case the key is due again <paramref name="delay"/> after <paramref name="nowUtc"/> and
    /// <paramref name="failures"/> is its place in the streak. Returns false, with the key cleared, for any other
    /// answer.
    /// </summary>
    public bool Record(
        string key, AlertDelivery? delivery, DateTime nowUtc, TimeSpan cap, out TimeSpan delay, out int failures)
    {
        if (!FailedSendBackoff.EveryChannelFailed(delivery))
        {
            Clear(key);
            delay = TimeSpan.Zero;
            failures = 0;
            return false;
        }

        delay = _streaks.RecordFailure(Family, key, nowUtc, cap, out failures);
        _due[key] = new Pending(nowUtc + delay, cap);
        return true;
    }

    /// <summary>When the key is due again, as of <paramref name="nowUtc"/>, or null when nothing is pending for it. The
    /// value to hand a policy that compares the clock with a due time (<c>nowUtc >= due</c>), read with the same clock
    /// reading the policy gets. #4732: a stamp more than the cap it was recorded under ahead of <paramref name="nowUtc"/>
    /// can only come from a wall clock that stepped backwards after it was stamped, and is returned as
    /// <paramref name="nowUtc"/>, so the retry is due now rather than a whole step later; any other stamp is returned
    /// as it was written. The rule <see cref="RetryPending"/> applies, from the same copy of it.</summary>
    public DateTime? DueUtc(string key, DateTime nowUtc) =>
        _due.TryGetValue(key, out var pending) ? ClampDue(pending, nowUtc) : null;

    /// <summary>When the key is due again as <see cref="Record"/> stamped it, whatever the clock reads now, or null when
    /// nothing is pending for it. Not for judging a retry, because a clock that stepped backwards since the stamp leaves
    /// it further ahead than a wait can be (<see cref="DueUtc(string, DateTime)"/> is the one that allows for that):
    /// it is here for a test that pins what the record wrote.</summary>
    public DateTime? StampedDueUtc(string key) => _due.TryGetValue(key, out var pending) ? pending.DueUtc : null;

    /// <summary>True from a send that reached no channel until its retry is due: the alert is waiting, and whatever
    /// state it would be judged on should be left as it was. #4732: a due time more than the cap it was recorded
    /// under ahead of <paramref name="nowUtc"/> can only come from a wall clock that stepped backwards after it was
    /// stamped (a due time is never more than the cap ahead when it is written), so the retry counts as due now
    /// instead of holding the alert back until the clock catches up. This is the rule of
    /// <c>CollectorCadence.ClampDue</c>, written out here because this project does not reference the collectors;
    /// a test pins the two equal.</summary>
    public bool RetryPending(string key, DateTime nowUtc) =>
        _due.TryGetValue(key, out var pending) && nowUtc < ClampDue(pending, nowUtc);

    /// <summary>The one copy of the backward-step rule, for <see cref="RetryPending"/> and
    /// <see cref="DueUtc(string, DateTime)"/>: the stamp, unless it is more than the cap it was recorded under ahead of
    /// <paramref name="nowUtc"/>, in which case <paramref name="nowUtc"/>.</summary>
    private static DateTime ClampDue(Pending pending, DateTime nowUtc) =>
        pending.DueUtc - nowUtc > pending.Cap ? nowUtc : pending.DueUtc;

    /// <summary>Ends the key: nothing is pending, and the next failure starts at a minute again.</summary>
    public void Clear(string key)
    {
        _streaks.RecordDelivered(Family, key);
        _due.TryRemove(key, out _);
    }

    /// <summary>
    /// Ends every key that starts with <paramref name="prefix"/> (compared ordinally), each as <see cref="Clear"/>
    /// does. For a scope that is going away as a whole, such as every alert of one server removed from monitoring:
    /// left behind, its pending retries would page for an outage the server no longer has, and its streaks would
    /// lengthen the waits of the next one. An empty prefix is refused, because it would end every key.
    /// </summary>
    public void ClearPrefix(string prefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(prefix);

        foreach (var key in _due.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                Clear(key);
            }
        }
    }
}
