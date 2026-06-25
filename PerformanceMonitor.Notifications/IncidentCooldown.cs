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
    /// </summary>
    public async Task<Decision> EvaluateAsync(
        string serverId, string metricName, IReadOnlyList<AlertIncident>? incidents, TimeSpan window)
    {
        var now = DateTime.UtcNow;
        Evict(now, window);

        var keys = BuildKeys(serverId, metricName, incidents);

        bool anyFresh = false;
        foreach (var (key, dedupKey) in keys)
        {
            if (_seedLastSentUtc is not null && !_cooldowns.ContainsKey(key))
            {
                var lastSent = await _seedLastSentUtc(serverId, metricName, dedupKey);
                if (lastSent.HasValue)
                    _cooldowns.TryAdd(key, lastSent.Value);
            }

            if (!_cooldowns.TryGetValue(key, out var last) || now - last >= window)
                anyFresh = true;
        }

        return new Decision(anyFresh, keys.Select(k => k.Key).ToList(), now);
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

    // Distinct non-blank fingerprints -> one "{prefix}{server}:{metric}:{dedupKey}" key each; no
    // fingerprint -> the single "{prefix}{server}:{metric}" fallback key (pre-#1154 behavior). A blank
    // DedupKey can't occur (AlertFingerprint returns null incidents, filtered upstream) but is excluded
    // defensively so it never produces a "…::" key.
    private List<(string Key, string? DedupKey)> BuildKeys(
        string serverId, string metricName, IReadOnlyList<AlertIncident>? incidents)
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

        return new List<(string, string?)> { ($"{_keyPrefix}{serverId}:{metricName}", (string?)null) };
    }

    /* Drop entries past 2x the window so the per-fingerprint dict stays bounded — any entry past 1x is
       already re-fire-eligible, so doubling only adds clock-skew margin and can never evict a key that
       could still suppress. A key dropped here that later recurs is re-seeded from history on next touch;
       that's a wash, not a bug. Mirrors AnalysisNotificationService's prune idiom. */
    private void Evict(DateTime now, TimeSpan window)
    {
        var pruneBefore = now - TimeSpan.FromTicks(window.Ticks * 2);
        foreach (var entry in _cooldowns)
        {
            if (entry.Value < pruneBefore)
                _cooldowns.TryRemove(entry.Key, out _);
        }
    }

    /// <summary>Live key count, for tests asserting eviction keeps the dict bounded.</summary>
    internal int TrackedKeyCount => _cooldowns.Count;

    /// <summary>The send decision plus the candidate keys to stamp on a successful send.</summary>
    public sealed record Decision(bool ShouldSend, IReadOnlyList<string> Keys, DateTime EvaluatedAtUtc);
}
