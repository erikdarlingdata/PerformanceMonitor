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

    /// <summary>Records a fire at <paramref name="tier"/> for the open incident; the highest tier wins.</summary>
    public void Record(string key, AlertSeverityLevel tier) =>
        _highestDelivered.AddOrUpdate(key, tier, (_, highest) => tier > highest ? tier : highest);

    /// <summary>Ends the incident at <paramref name="key"/>: the next fire starts from nothing delivered.</summary>
    public void Close(string key) => _highestDelivered.TryRemove(key, out _);
}
