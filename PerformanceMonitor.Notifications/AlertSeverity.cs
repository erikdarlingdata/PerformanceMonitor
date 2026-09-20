/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Severity tier an alert site can force, independent of the metric-name map, for metrics whose
/// seriousness is graded at runtime rather than fixed (e.g. "Volume Free Space": WARNING below the
/// configured threshold, CRITICAL when critically low — #1136). Carried on
/// <see cref="AlertContext.SeverityOverride"/>; <c>null</c> falls back to the per-metric map.
/// </summary>
public enum AlertSeverityLevel
{
    Warning,
    Critical
}

/// <summary>
/// Single source of truth for per-metric alert severity styling: accent/hex color, badge text,
/// and the webhook emoji. Both the email body (<see cref="EmailTemplateBuilder"/>) and the
/// webhook payloads (<see cref="WebhookAlertService"/>) consume this so the two maps cannot
/// drift apart (adding a metric to one and forgetting the other was a real foot-gun).
/// </summary>
internal static class AlertSeverity
{
    /// <param name="overrideLevel">
    /// When non-null, forces the tier regardless of <paramref name="metricName"/> — for metrics
    /// graded at runtime (#1136). <c>null</c> uses the per-metric map below.
    /// </param>
    /// <returns>
    /// (HexColor, BadgeText, Emoji) for the metric. Unknown metrics fall back to INFO-blue.
    /// Email ignores the emoji; webhooks use all three.
    /// </returns>
    public static (string HexColor, string BadgeText, string Emoji) ForMetric(
        string metricName,
        AlertSeverityLevel? overrideLevel = null) => overrideLevel switch
    {
        AlertSeverityLevel.Critical => ("#DC2626", "CRITICAL", "\U0001F534"),
        AlertSeverityLevel.Warning => ("#D97706", "WARNING", "\U0001F7E0"),
        _ => metricName switch
        {
            "Blocking Detected" => ("#D97706", "ALERT", "\U0001F7E0"),
            // #1839 total-blocked-wait gate — same tier as the count gate it sits beside; without an arm
            // here it would render INFO-blue in email and webhooks (the #1136 fall-through).
            "Blocking Wait Time" => ("#D97706", "ALERT", "\U0001F7E0"),
            /* #3653 (A8e): GRADED at the engine's fire site now — Warning by default, Critical when the
               hour's deadlock rate reaches the health band's Critical tier (the #3368 measured pair the
               fleet card bands on). Every live fire carries the tier on Context.SeverityOverride, so this
               arm styles only the override-less render: alert-history replays of rows written BEFORE
               #3653, every one of which rendered red by name. Red is therefore the faithful replay, not a
               default for the metric. */
            "Deadlocks Detected" => ("#DC2626", "ALERT", "\U0001F534"),
            // Blocking/deadlock capture is broken — emailed/webhooked, and fired as an Error toast,
            // so it must not render INFO-blue like an unmapped metric (mirrors the #1136 gap fix).
            "Capture Down" => ("#DC2626", "CRITICAL", "\U0001F534"),
            /* #3653 (A8e): graded at the fire site — Warning, or Critical at the CPU health band's Critical
               bar (95% of a fixed host, the card's ladder). Like the deadlock arm above, this styles only
               pre-#3653 replays, all of which were amber. */
            "High CPU" => ("#F59E0B", "WARNING", "\U0001F7E1"),
            /* #3539 A4: GRADED at both engines' fire sites now — Warning at one task/backend continuously
               stuck across the ten-minute window, Critical at ten (PoisonWaitEvaluator's shared bars; the
               PostgreSQL twin has graded since #2711, SQL Server fired presence-flat CRITICAL until #3539).
               Every live fire carries the tier on Context.SeverityOverride, so this arm styles only the
               override-less render — the alert-history replay of a row written BEFORE #3539, and every
               such SQL Server row WAS a CRITICAL fire. CRITICAL is therefore the faithful replay, not a
               default for the metric; a new fire site for this name must pass its grade explicitly. */
            "Poison Wait" => ("#DC2626", "CRITICAL", "\U0001F534"),
            "Long-Running Query" => ("#D97706", "WARNING", "\U0001F7E0"),
            /* #3653 (A8e): fires with an explicit WARNING override now (and only Warning — the engine's fire
               site says why no Critical tier exists), so this arm styles pre-#3653 replays, which were amber. */
            "tempdb Space" => ("#D97706", "WARNING", "\U0001F7E0"),
            "Long-Running Job" => ("#D97706", "WARNING", "\U0001F7E0"),
            // Emailed/webhooked and fired as a Warning toast — was falling through to INFO-blue.
            "Failed Agent Job" => ("#D97706", "WARNING", "\U0001F7E0"),
            // Database state deviated from its baseline/expected state. Always fired with an explicit
            // severity override (WARNING, or CRITICAL for SUSPECT/RECOVERY_PENDING/EMERGENCY — see
            // DatabaseStateTokens.SeverityFor), so this arm only styles the override-less alert-history
            // replay; default it to WARNING-amber rather than the INFO-blue fall-through (#1136 gap).
            "Database State" => ("#D97706", "WARNING", "\U0001F7E0"),
            "Volume Free Space" => ("#D97706", "WARNING", "\U0001F7E0"),
            "Server Unreachable" => ("#DC2626", "CRITICAL", "\U0001F534"),
            "Server Restored" => ("#16A34A", "RESOLVED", "\U0001F7E2"),
            // Availability Group family (#991). "AG Replica Reconnected" fires with severity null on purpose,
            // so its arm here is load-bearing (green/RESOLVED, matching "Server Restored"). The other four pass
            // an explicit override at the fire site and reach this map only via a renderer that has none — an
            // alert-history replay — which is exactly the INFO-blue fall-through the #1136 gap fix was about.
            /* #2090 (gotqn): the next batch of #1136 fall-throughs — every one of these fires with an
               explicit severity at its site (now plumbed through AlertOutcome.Severity by the deliverer),
               so these arms exist for the renderer that has no context: alert-history replays. All six
               self-alerts fire Critical at their sites; PVS deliberately ships without a severity tier
               (#1984), so its replay arm is WARNING-amber like Database State. */
            "Collection Stopped" => ("#DC2626", "CRITICAL", "\U0001F534"),
            "Agent Not Running" => ("#DC2626", "CRITICAL", "\U0001F534"),
            "Store Disk Pressure" => ("#DC2626", "CRITICAL", "\U0001F534"),
            "Store Runtime Upgrade" => ("#DC2626", "CRITICAL", "\U0001F534"),
            "Compression Job Stuck" => ("#DC2626", "CRITICAL", "\U0001F534"),
            /* #3443: the collector-cost DIGEST is the one metric in this map that is INFO-blue on
               purpose. It fires with no severity override precisely so this arm decides, because the
               product's only outbound channel is the alerting one and INFO is the only tier the rendering
               layer has for "read this, do not act on it" — AlertSeverityLevel itself has two members.
               DECLARED rather than left to the unmapped fall-through below, which renders identically:
               the #1136 and #2090 work was a sweep to eliminate metrics reaching that fall-through by
               accident, so a digest relying on it would be corrected into a WARNING by the next sweep and
               the demotion would silently undo itself. */
            "Collector Cost Digest" => ("#2eaef1", "INFO", "\U0001F535"),
            /* #3466 lane 4: the fleet sweep's daily rollup joins the digest as the second deliberate
               INFO arm, for the digest's exact reason — it is a report to read, not a condition to act
               on, and the declaration is what keeps the next fall-through sweep from promoting it. */
            "Fleet Sweep Rollup" => ("#2eaef1", "INFO", "\U0001F535"),
            /* #3712: the analysis singles digest is the third deliberate INFO arm, on the same reasoning — the
               once-a-day copy of findings that were kept OFF the paging channels must not itself render as a
               page; the declaration keeps the next fall-through sweep from promoting it. */
            "Analysis Singles Digest" => ("#2eaef1", "INFO", "\U0001F535"),
            /* #3783: the store's two physical-health conditions are INFO by design, and the first CONDITIONS
               (entered and left, with a resolution row) rather than reports to take the tier. A dimension's
               TOAST file holding slack is disk the maintainer may reclaim in a maintenance window with a verb
               that takes an ACCESS EXCLUSIVE lock; the checkpointer's sync phase running long is a WAL-sizing /
               refresh-slicing decision (#3802, #3745). Neither is a page. Declared, for the digest's reason:
               the next fall-through sweep must not promote them to WARNING. */
            "Store TOAST Slack" => ("#2eaef1", "INFO", "\U0001F535"),
            "Store Checkpointer Pressure" => ("#2eaef1", "INFO", "\U0001F535"),
            "Version Store (PVS)" => ("#D97706", "WARNING", "\U0001F7E0"),
            "AG Failover" => ("#D97706", "WARNING", "\U0001F7E0"),
            "AG Replica Disconnected" => ("#DC2626", "CRITICAL", "\U0001F534"),
            "AG Replica Reconnected" => ("#16A34A", "RESOLVED", "\U0001F7E2"),
            "AG Sync Fell Behind" => ("#D97706", "WARNING", "\U0001F7E0"),
            "AG Database Suspended" => ("#D97706", "WARNING", "\U0001F7E0"),
            _ => ("#2eaef1", "INFO", "\U0001F535")
        }
    };
}
