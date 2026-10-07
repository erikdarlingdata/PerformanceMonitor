/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Per-incident-fingerprint delivery cooldown shared by the email (<see cref="EmailSendCore"/>) and
/// webhook (<see cref="WebhookAlertService"/>) send paths (#1154). Replaces the old per-
/// (serverId, metricName) cooldown that silently dropped a genuinely distinct incident arriving inside
/// the window: the cooldown is now keyed per #1140 dedup fingerprint, so a DISTINCT fingerprint is
/// delivered while a REPEAT of the same fingerprint stays suppressed (#1091). Alerts with no
/// fingerprintable incidents (Incidents null/empty — CPU/memory/poison-wait/tempdb/failed-job) fall back
/// to the metric-level key, byte-identical to the pre-#1154 behavior.
/// </summary>
public sealed class IncidentCooldown
{
    private readonly ConcurrentDictionary<string, DateTime> _cooldowns = new();

    /* ClearMetric's sticky half: the clear instant per metric-level key. Written ONLY by ClearMetric
       (the key shape is exactly the "{prefix}{server}:{metric}" fallback key — a fingerprinted key is
       never found here, and the seed's lookup of one is a miss feeding the "no clear" answer). Read by
       EvaluateAsync to keep a pre-clear history row from re-seeding an entry the clear just removed:
       the PR review's point 2, which reproduced as "down, back, down" where the DOWN that follows a
       delivered resolve still met the first outage's in-memory stamp that ClearMetric deleted — until
       the seed rebuilt it from the alert history. */
    private readonly ConcurrentDictionary<string, DateTime> _clearedAtUtc = new();

    private readonly string _keyPrefix;
    private readonly Func<string, string, string?, Task<DateTime?>>? _seedLastSentUtc;

    /// <param name="keyPrefix">
    /// Per-channel prefix so the two channels' keys never collide in shape: <c>""</c> for email,
    /// <c>"webhook:"</c> for the webhook path (matching the pre-#1154 key strings).
    /// </param>
    /// <param name="seedLastSentUtc">
    /// Lazy restart seed: <c>(serverId, metricName, dedupKey) =&gt;</c> the last successful send time for
    /// that key, or <c>null</c> if none. <c>dedupKey</c> is <c>null</c> for the metric-level fallback. A
    /// <c>null</c> delegate disables seeding entirely (the webhook no-store path — preserves
    /// <c>WebhookCooldownSeedTests.NullHistoryStore_NoSeed_AttemptsPost</c>).
    /// </param>
    public IncidentCooldown(string keyPrefix, Func<string, string, string?, Task<DateTime?>>? seedLastSentUtc)
    {
        _keyPrefix = keyPrefix;
        _seedLastSentUtc = seedLastSentUtc;
    }

    /// <summary>
    /// Decides whether to send, seeding unseen keys from history and evicting stale keys first. Does NOT
    /// stamp — the caller stamps via <see cref="Stamp"/> only after a successful send, so a failed send
    /// never starts the cooldown. Sends when AT LEAST ONE candidate key is fresh (outside the window); a
    /// successful send then stamps EVERY candidate key, so an in-cooldown incident that rode along on a
    /// fresh sibling has its timer refreshed and never re-alerts standalone next cycle.
    /// <para>#3313: the per-fingerprint verdicts the "at least one" reduction is computed FROM are reported
    /// as <see cref="Decision.DeliverableDedupKeys"/>, because "rode along on a fresh sibling" is the
    /// behaviour that made an already-delivered incident reappear on the next card. The reduction stays —
    /// posting is still all-or-nothing per alert — but the render no longer has to treat it as the only
    /// answer available.</para>
    /// </summary>
    /// <param name="scope">
    /// #5469: an identity folded into the metric-level fallback key only ("{server}:{metric}:{scope}"), so
    /// two incidents of one metric on one server hold separate windows. Null (every caller but the AG pair
    /// under PagerDuty auto-resolve) leaves the key byte-identical to today's. A scoped key is never seeded
    /// from the alert history: the history answers per (server, metric, fingerprint) and carries no scope,
    /// so a seed would hand one replica's send time to its siblings. After a restart a scoped key therefore
    /// starts as a first notice, which can re-post once inside a window and never swallows a notice.
    /// </param>
    public async Task<Decision> EvaluateAsync(
        string serverId, string metricName, IReadOnlyList<AlertIncident>? incidents, TimeSpan window,
        string? scope = null)
    {
        var now = DateTime.UtcNow;
        Evict(now, window);

        var keys = BuildKeys(serverId, metricName, incidents, scope);

        bool anyFresh = false;
        bool anyFirstNotice = false;
        bool fingerprinted = false;
        var deliverable = new List<string>(keys.Count);

        foreach (var (key, dedupKey) in keys)
        {
            var scopedFallback = scope is not null && dedupKey is null;
            if (_seedLastSentUtc is not null && !scopedFallback && !_cooldowns.ContainsKey(key))
            {
                var lastSent = await _seedLastSentUtc(serverId, metricName, dedupKey);

                /* A row older than the recorded clear is the row the clear was FOR — re-seeding it would
                   undo ClearMetric whenever the map entry is gone from memory but the history row is not
                   (the pair-of-points this fix exists for). A seed newer than the clear still applies: a
                   genuinely new send happened after the resolve, and that send's row re-arms the window
                   normally. */
                if (lastSent.HasValue &&
                    (!_clearedAtUtc.TryGetValue(key, out var clearedAtUtc) || lastSent.Value > clearedAtUtc))
                    _cooldowns.TryAdd(key, lastSent.Value);
            }

            /* Read once and keep BOTH halves. The absence of an entry after the seed attempt is the only
               place in the pipeline that can tell "never delivered on this channel" from "delivered, and
               the window has elapsed" — the map holds an entry only because Stamp wrote one after a
               successful send or because the seed answered with a real one. The freshness test discards
               that distinction, and #3430's aggregate ceiling turns on it. */
            var everSent = _cooldowns.TryGetValue(key, out var last);
            if (everSent && last > now)
            {
                /* #4732: a send time AHEAD of the clock (it stepped back since the send, or the seed above read a
                   history row from before the step) is replaced by this reading and counted from there, so the
                   repeat is due one window after the first evaluation that sees the step, not the step plus the
                   window. Compare-and-swap: a Stamp that landed meanwhile is newer than either. (This assembly
                   does not reference PerformanceMonitor.Common, where LastFiredStamp holds the rule for maps.) */
                _cooldowns.TryUpdate(key, now, last);
                last = now;
            }

            var fresh = !everSent || now - last >= window;
            if (fresh)
                anyFresh = true;
            if (!everSent)
                anyFirstNotice = true;

            if (dedupKey is not null)
            {
                fingerprinted = true;
                if (fresh)
                    deliverable.Add(dedupKey);
            }
        }

        /* null, not empty, on the metric-level fallback: "this alert has no fingerprints to filter" and
           "every one of its fingerprints is in cooldown" are different answers, and the render has to be
           able to tell them apart (an empty list would read as "show nothing"). */
        return new Decision(
            anyFresh, keys.Select(k => k.Key).ToList(), now, fingerprinted ? deliverable : null, anyFirstNotice);
    }

    /// <summary>
    /// Stamps every candidate key from a <see cref="Decision"/> to its evaluation time. Call ONLY after a
    /// successful send (a failed send must not start the cooldown).
    /// </summary>
    public void Stamp(Decision decision)
    {
        foreach (var key in decision.Keys)
            _cooldowns[key] = decision.EvaluatedAtUtc;
    }

    /// <summary>
    /// Forgets the metric-level fallback entry for (<paramref name="serverId"/>, <paramref name="metricName"/>)
    /// on THIS cooldown's key space. The PagerDuty auto-resolve recovery calls this for its pair's FIRING
    /// metric after a delivered close: the stamp sitting on that key is a relic of an incident that no longer
    /// exists, and without the clear the next firing of the same metric meets the old window and never
    /// announces. Call only on a DELIVERED close — evaluating (not sending) a recovery must not re-arm
    /// anything.
    /// <para>
    /// The clear is sticky across history re-seeds for PRE-clear rows only: the clear instant is recorded
    /// beside the removal, and EvaluateAsync's seed branch ignores any history row older than it — without
    /// that the PagerDuty close's next firing would read the pre-clear send straight back out of the alert
    /// history (both production paths seed) and be throttled against the incident that no longer exists. A
    /// seeded time ending up NEWER than the clear still applies — a genuinely new send happened after the
    /// resolve, and it re-arms the window normally. A restart discards the record along with the in-memory
    /// cooldown and the seed re-arms from history; that is the restart class the seed deliberately accepts,
    /// not a reopened window.
    /// </para>
    /// <para>
    /// The clear is METRIC-level — one key per server and metric — unless <paramref name="scope"/> names
    /// the same identity <see cref="EvaluateAsync"/> keyed the entry with (#5469: the AG pair's replica,
    /// under PagerDuty auto-resolve). Then it forgets only that replica's entry, so one replica's reconnect
    /// does not re-arm a sibling that is still down.
    /// </para>
    /// </summary>
    public void ClearMetric(string serverId, string metricName, string? scope = null)
    {
        var key = scope is null
            ? $"{_keyPrefix}{serverId}:{metricName}"
            : $"{_keyPrefix}{serverId}:{metricName}:{scope}";
        _clearedAtUtc[key] = DateTime.UtcNow;
        _cooldowns.TryRemove(key, out _);
    }

    // Distinct non-blank fingerprints -> one "{prefix}{server}:{metric}:{dedupKey}" key each; no
    // fingerprint -> the single "{prefix}{server}:{metric}" fallback key (pre-#1154 behavior). A blank
    // DedupKey can't occur (AlertFingerprint returns null incidents, filtered upstream) but is excluded
    // defensively so it never produces a "…::" key.
    private List<(string Key, string? DedupKey)> BuildKeys(
        string serverId, string metricName, IReadOnlyList<AlertIncident>? incidents, string? scope)
    {
        var dedupKeys = incidents?
            .Select(i => i.DedupKey)
            .Where(k => !string.IsNullOrEmpty(k))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (dedupKeys is { Count: > 0 })
            return dedupKeys
                .Select(d => ($"{_keyPrefix}{serverId}:{metricName}:{d}", (string?)d))
                .ToList();

        /* #5469: the scope joins the fallback key only. The AG pair is stateless (no incidents), so it always
           lands here; a null scope is the pre-existing key byte for byte. */
        return new List<(string, string?)>
        {
            (scope is null ? $"{_keyPrefix}{serverId}:{metricName}" : $"{_keyPrefix}{serverId}:{metricName}:{scope}",
             (string?)null)
        };
    }

    /* Drop entries past 2x the window so the per-fingerprint dict stays bounded — any entry past 1x is
       already re-fire-eligible, so doubling only adds clock-skew margin and can never evict a key that
       could still suppress. A key dropped here that later recurs is re-seeded from history on next touch;
       that's a wash, not a bug. Mirrors AnalysisNotificationService's prune idiom.
       The cleared-at map lives under the same rule: a clear at least two windows old names an incident
       that cannot still be re-seeding from inside the window, so both maps face the same pruneBefore.
       Pruning a CLEAR can un-cover a history row (the seed would apply again) — but only for a row at
       least two windows old by the same measure, which re-fires anyway. */
    private void Evict(DateTime now, TimeSpan window)
    {
        var pruneBefore = now - TimeSpan.FromTicks(window.Ticks * 2);
        foreach (var entry in _cooldowns)
        {
            if (entry.Value < pruneBefore)
                _cooldowns.TryRemove(entry.Key, out _);
        }

        foreach (var entry in _clearedAtUtc)
        {
            if (entry.Value < pruneBefore)
                _clearedAtUtc.TryRemove(entry.Key, out _);
        }
    }

    /// <summary>Live key count, for tests asserting eviction keeps the dict bounded.</summary>
    internal int TrackedKeyCount => _cooldowns.Count;

    /// <summary>Live cleared-entry count, for tests asserting the clear record faces the same eviction bound.</summary>
    internal int TrackedClearCount => _clearedAtUtc.Count;

    /// <summary>The send decision plus the candidate keys to stamp on a successful send.</summary>
    /// <param name="DeliverableDedupKeys">
    /// #3313: the #1140 dedup fingerprints that were OUTSIDE their own window — the incidents a channel
    /// should render, as opposed to the whole set the alert happens to carry. <c>null</c> when the alert
    /// carried no fingerprint and was evaluated on the metric-level fallback key, which is "nothing to
    /// filter" rather than "nothing to show". Fed to <see cref="IncidentDeliveryFilter.ForDelivery"/>.
    /// <para>A member of the same value as <see cref="ShouldSend"/> rather than a second call the caller
    /// has to remember to make: the two answers come from one pass over one key set, and a render deciding
    /// freshness for itself could disagree with the decision that let it post.</para>
    /// </param>
    /// <param name="AnyFirstNotice">
    /// #3430: whether at least one candidate key had NO prior successful send on this channel — neither an
    /// entry this process stamped nor one the history-store seed answered with. It is a statement about what
    /// the seed could find, not a claim that the incident is new to the world: a delivery aged out of
    /// <c>config_alert_log</c>'s retention reads as a first notice, and a channel constructed with no history
    /// store (the seed delegate is null) reads EVERY cold key as one.
    /// <para>Both of those err toward "first notice", which is the direction that costs a post rather than an
    /// announcement — <see cref="RepeatDeliveryBudget"/> exempts a first notice from its per-metric ceiling
    /// precisely so a never-announced incident can never be folded into another server's card, which is the
    /// bug #1154 removed.</para>
    /// <para>Required rather than defaulted. A default would have to be one of the two answers, and the
    /// wrong one silently classifies a never-announced incident as a repeat — which is the only input that
    /// lets the aggregate ceiling drop something. The compiler finding a second producer is cheaper than
    /// that.</para>
    /// </param>
    public sealed record Decision(
        bool ShouldSend,
        IReadOnlyList<string> Keys,
        DateTime EvaluatedAtUtc,
        IReadOnlyList<string>? DeliverableDedupKeys,
        bool AnyFirstNotice);
}
