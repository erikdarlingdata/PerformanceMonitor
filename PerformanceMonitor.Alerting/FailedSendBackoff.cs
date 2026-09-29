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
/// The streak bookkeeping behind "an alert whose every channel failed is tried again sooner than its cooldown"
/// (#4752). A family stamps its cooldown before it delivers, so a send that reached nobody (an HTTP 429 or 5xx,
/// a timeout, an unreachable mail server) used to leave the alert silent for the whole cooldown. The caller
/// counts each such send here, per (family, key), and gets back how long to wait before trying again: a minute,
/// doubling with each further failure in a row (1, 2, 4 ... minutes), never longer than the cap it passes, which
/// is the family's own cooldown.
///
/// <para><b>What ends a streak.</b> <see cref="RecordDelivered"/> drops the pair, and so does time: a failure
/// more than twice the cap after the previous one starts over at a minute. Twice, not once, because a live
/// streak's gap is a little over one cap: at the cap the delay equals the cap, and the retry runs on the first
/// sweep after it. Without the time rule, a failure long after the condition cleared inherited the streak the
/// clear left behind and waited the longest delay for its first retry.</para>
///
/// <para>In memory only, and safe to call from several threads. A restart starts every streak over, which is the
/// safe direction: the first retry after a restart is only ever earlier.</para>
/// </summary>
public sealed class FailedSendBackoff
{
    /// <summary>
    /// How many tracked pairs it takes before <see cref="RecordFailure"/> also drops the streaks that can no
    /// longer matter. A delivery is the only other thing that drops a pair, and a family keyed per run (a
    /// long-running job) leaves one behind for every run that ended undelivered.
    /// </summary>
    private const int PruneAbove = 1024;

    private readonly ConcurrentDictionary<(string Family, string Key), Streak> _streaks = new();

    /// <summary>One pair's run of failures: how many, when the last one was, and the cap it was given.</summary>
    private readonly record struct Streak(int Failures, DateTime LastFailureUtc, TimeSpan Cap);

    /// <summary>
    /// True when the send was attempted and NOTHING reached an operator (#4752): <see cref="AlertDelivery.Sent"/>
    /// is false and <see cref="AlertDelivery.SendError"/> is set. A failed email records as channel
    /// <c>email</c> with its send error, and a webhook-only fan-out whose every post failed as
    /// <see cref="AlertDelivery.ChannelFailed"/>; both have that shape. A PARTIAL failure (one channel
    /// delivered, another did not) has <c>Sent</c> true and is not retried, because a retry would send the
    /// alert a second time down the channel that worked. <c>null</c> (unreported) stays "delivered", as do a
    /// muted fire, a throttled or folded one and one no channel applies to: none of them carries a
    /// <c>SendError</c>.
    /// </summary>
    public static bool EveryChannelFailed(AlertDelivery? delivery) =>
        delivery is { Sent: false, SendError: not null };

    /// <summary>
    /// Counts one more failed send for (<paramref name="family"/>, <paramref name="key"/>) and returns how long to
    /// wait before it is tried again: <see cref="AlertEngine.ChannelFailureRetryDelay"/> of the failures in a
    /// row, capped at <paramref name="cap"/>. When the previous failure for the pair is more than twice
    /// <paramref name="cap"/> before <paramref name="nowUtc"/>, the streak has lapsed and this is failure one
    /// again.
    /// </summary>
    public TimeSpan RecordFailure(string family, string key, DateTime nowUtc, TimeSpan cap) =>
        RecordFailure(family, key, nowUtc, cap, out _);

    /// <summary>
    /// <see cref="RecordFailure"/>, also reporting the failure's place in the streak for a log line.
    /// </summary>
    public TimeSpan RecordFailure(string family, string key, DateTime nowUtc, TimeSpan cap, out int failures)
    {
        var streak = _streaks.AddOrUpdate(
            (family, key),
            _ => new Streak(1, nowUtc, cap),
            (_, previous) => nowUtc - previous.LastFailureUtc > cap + cap
                ? new Streak(1, nowUtc, cap)
                : new Streak(previous.Failures < int.MaxValue ? previous.Failures + 1 : previous.Failures, nowUtc, cap));

        if (_streaks.Count > PruneAbove)
        {
            Prune(nowUtc);
        }

        failures = streak.Failures;
        return AlertEngine.ChannelFailureRetryDelay(streak.Failures, cap);
    }

    /// <summary>
    /// A send for (<paramref name="family"/>, <paramref name="key"/>) reached an operator, was muted or reported
    /// nothing: the failure streak is over, and the next failure starts again at a minute.
    /// </summary>
    public void RecordDelivered(string family, string key) => _streaks.TryRemove((family, key), out _);

    /// <summary>How many (family, key) pairs are in a failure streak right now.</summary>
    public int TrackedCount => _streaks.Count;

    /// <summary>
    /// Drops every streak whose last failure is more than twice its own cap ago. Such a streak is already the
    /// same as none, because the next failure would restart it, so this changes no answer; it only keeps a
    /// family keyed per run from growing the table for as long as the process lives. Each streak is judged by
    /// the cap it was recorded with, and removed only if it has not been updated since it was read.
    /// </summary>
    private void Prune(DateTime nowUtc)
    {
        foreach (var entry in _streaks)
        {
            if (nowUtc - entry.Value.LastFailureUtc > entry.Value.Cap + entry.Value.Cap)
            {
                _streaks.TryRemove(entry);
            }
        }
    }
}
