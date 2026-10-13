// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The Warning-to-Critical escalation rule for a two-level alert (#5641), as a helper a fire site opts into.
///
/// <para><b>Why.</b> Every alert holds a per-(server, metric) cooldown between deliveries, so a Warning that
/// worsens to Critical inside the window used to be held for the rest of it. On the 2026-10-09 incident the
/// Runnable Queue Warning was delivered at 15:43:45 and the Critical only at about 15:48:45, four seconds before
/// High CPU. The alert exists to be early, so the escalation is delivered at once.</para>
///
/// <para><b>The rule.</b> While an incident is open and the highest tier delivered for it is Warning, the first
/// evaluation graded Critical is an escalation: the fire site lets it past its own cooldown and flags the context
/// (<see cref="AlertContext.EscalationBypassesCooldown"/>) so the email and webhook cooldowns and the repeat
/// ceiling let it through too. Delivering it records Critical, which restarts the clock. After that, Critical
/// evaluations follow the normal cooldown, and Critical going back to Warning delivers nothing new (the highest
/// tier stays Critical until the incident closes). <see cref="Close"/> ends the incident.</para>
///
/// <para><b>Failed sends.</b> A fire whose every channel failed delivered nothing, so the fire site puts the tier
/// back (<see cref="Restore"/>); the retry is still the escalation.</para>
///
/// <para><b>Restart gap (round 2 of #5709, deliberate).</b> The tier lives in memory only. After a restart in the
/// middle of an incident the engine does not know a Warning was delivered, so the first Critical is an ordinary first
/// fire, while the email and webhook cooldowns are seeded from the alert history and may hold it for the rest of
/// their window. The seed seam (<c>IAlertHistoryStore.GetLastEmailSentUtcAsync</c> and
/// <c>GetLastWebhookSentUtcAsync</c>) returns a timestamp only, never a severity, lives in the delivery layer (the
/// engine holds no history store), and every store implements it; carrying a tier through it would be a new
/// persistence path in both apps, so the gap is accepted and the cooldown is the fallback.</para>
///
/// <para><b>Opt-in.</b> One instance per alert family, held by the engine; the fire site calls
/// <see cref="IsEscalation"/> before its cooldown test, <see cref="Record"/> when it fires and <see cref="Close"/>
/// when the incident clears. <see cref="IsOptedIn"/> names the metrics that do so, for the one downstream reader
/// that has only the metric name (the Darling viewer's tray-toast cooldown). Runnable Queue is the only member.
/// Every other alert with a Warning and a Critical level keeps the old behaviour: a worsening inside the window
/// waits for the cooldown.</para>
/// </summary>
public sealed class TierEscalation
{
    private static readonly HashSet<string> s_optedIn = new(StringComparer.Ordinal)
    {
        RunnableQueueEvaluator.MetricName,
    };

    /// <summary>The metrics whose Warning-to-Critical escalation is delivered past the cooldown.</summary>
    public static IReadOnlyCollection<string> OptedInMetrics => s_optedIn;

    /// <summary>True when <paramref name="metricName"/> opted into the escalation rule.</summary>
    public static bool IsOptedIn(string? metricName) => metricName is not null && s_optedIn.Contains(metricName);

    private readonly ConcurrentDictionary<string, AlertSeverityLevel> _highestDelivered = new(StringComparer.Ordinal);

    /// <summary>
    /// True when <paramref name="tier"/> is higher than anything delivered for the open incident at
    /// <paramref name="key"/>. False when nothing was delivered yet (the ordinary first fire follows the normal
    /// cooldown), when the tier equals the highest delivered, and when it is lower.
    /// </summary>
    public bool IsEscalation(string key, AlertSeverityLevel tier) =>
        _highestDelivered.TryGetValue(key, out var highest) && tier > highest;

    /// <summary>The highest tier recorded for the open incident at <paramref name="key"/>, or null when none.</summary>
    public AlertSeverityLevel? Highest(string key) =>
        _highestDelivered.TryGetValue(key, out var highest) ? highest : null;

    /// <summary>Records a fire at <paramref name="tier"/> for the open incident; the highest tier wins.</summary>
    public void Record(string key, AlertSeverityLevel tier) =>
        _highestDelivered.AddOrUpdate(key, tier, (_, highest) => tier > highest ? tier : highest);

    /// <summary>
    /// Puts the tier back to <paramref name="prior"/> (the value <see cref="Highest"/> returned before the fire) after
    /// a fire whose every channel failed (round 2 of #5709, the way the worsening gates' <c>RestoreAlertedLevel</c>
    /// does): nothing was delivered, so the retry a minute later must still be the escalation and carry the bypass
    /// flag. Without it the retry reads as an ordinary Critical and the email and webhook cooldowns, stamped by the
    /// Warning, hold it for the rest of their window. A null <paramref name="prior"/> removes the entry.
    /// </summary>
    public void Restore(string key, AlertSeverityLevel? prior)
    {
        if (prior is { } tier)
        {
            _highestDelivered[key] = tier;
        }
        else
        {
            _highestDelivered.TryRemove(key, out _);
        }
    }

    /// <summary>Ends the incident at <paramref name="key"/>: the next fire starts from nothing delivered.</summary>
    public void Close(string key) => _highestDelivered.TryRemove(key, out _);
}
