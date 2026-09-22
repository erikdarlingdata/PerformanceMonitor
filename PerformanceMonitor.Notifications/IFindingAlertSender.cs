/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The seam through which the shared <see cref="AnalysisNotificationService"/> dispatches
/// a composed finding alert and seeds its per-finding cooldown.
/// <para>
/// Implemented by each app's <c>EmailAlertService</c> (the per-app orchestrator shell).
/// This is the boundary that keeps the <b>record cadence</b> per-app under Approach B
/// (plan §4.6): the shared service composes the message and manages the cooldown, while
/// the implementation decides which alert-history rows to write —
/// </para>
/// <list type="bullet">
///   <item>Lite writes one combined <c>config_alert_log</c> row (its
///   <c>TrySendAlertEmailAsync</c> always records).</item>
///   <item>Dashboard writes per-channel rows and, when no channel is configured, a "tray"
///   fallback row (the fallback that previously lived inline in Dashboard's
///   <c>AnalysisNotificationService</c>). It cannot live in <c>TrySendAlertEmailAsync</c>
///   itself because the threshold-alert path records its own "tray" row separately, so the
///   fallback belongs to the analysis path only — here.</item>
/// </list>
/// </summary>
public interface IFindingAlertSender
{
    /// <summary>
    /// Latest <c>alert_time</c> for (serverId, metricName) over rows that were a DELIVERED page —
    /// seeds the analysis per-finding #2054 hold across restarts. #3916: a hold must be earned by a
    /// delivery, so a row that reached no one seeds nothing. Forwards to
    /// <see cref="IAlertHistoryStore.GetLastDeliveredPageUtcAsync"/>.
    /// </summary>
    Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName);

    /// <summary>
    /// Sends a composed analysis-finding alert through this app's channels and records it
    /// per this app's alert-history cadence. Never throws.
    /// <para>#3712: when <see cref="FindingAlert.Route"/> is <see cref="FindingRoute.Digest"/> the
    /// implementation consults NO channel — no email, no webhook fan-out, no tray — and records the row
    /// with <see cref="AlertDelivery.RoutedToDigest"/>. The routing record itself
    /// (<see cref="AlertContext.Routing"/>) is already on the context when the alert arrives; the sender's
    /// only job on that arm is to keep the channels shut and write the row.</para>
    /// <para>#3916: returns the <see cref="AlertDelivery"/> the row recorded (the digest arm returns
    /// <see cref="AlertDelivery.RoutedToDigest"/>), so the service can tell a page that reached someone
    /// from one that reached no one — only the former may arm the #2054 hold. Null ONLY when the sender
    /// caught an exception, which is read as not delivered.</para>
    /// </summary>
    Task<AlertDelivery?> SendFindingAlertAsync(FindingAlert alert);

    /// <summary>
    /// #3916: sends ONE message naming every page in <paramref name="named"/> (already ordered newest first,
    /// then by severity) — the analysis service's over-the-cap arm of a hold-back window — and records ONE row
    /// per named incident under that incident's own metric name, each carrying the SUMMARY's real
    /// <see cref="AlertDelivery"/>, so the restart seed (<see cref="GetLastDeliveredPageUtcAsync"/>) finds every
    /// named story. Returns that delivery; null only when the sender caught. Never throws.
    /// </summary>
    Task<AlertDelivery?> SendFindingSummaryAsync(IReadOnlyList<FindingAlert> named);
}

/// <summary>
/// The shared composition of a #3916 page summary: one message naming every held page, and the row each
/// named page records. Every SKU's <see cref="IFindingAlertSender.SendFindingSummaryAsync"/> composes
/// through here so the three cannot drift in what a summary says.
/// </summary>
public static class FindingSummary
{
    /// <summary>The summary message's metric name (the send core's cooldown/budget key alongside the
    /// per-incident fingerprints on the context).</summary>
    public const string MetricName = "Analysis: page summary";

    /// <summary>The fingerprint kind each named page carries on the summary's context.</summary>
    public const string IncidentKind = "analysis";

    /// <summary>Composes the one summary message: a server label, a headline value, and a context with one
    /// detail item and one dedup incident per named page, so the send core's per-fingerprint cooldown
    /// (#1154) and repeat budget (#3430) treat each named page individually.</summary>
    public static (string ServerName, string CurrentValue, AlertContext Context) Compose(IReadOnlyList<FindingAlert> named)
    {
        var servers = named.Select(a => a.ServerName).Distinct(StringComparer.Ordinal).ToList();
        var serverName = servers.Count == 1 ? servers[0] : $"{servers.Count} servers";
        var currentValue = $"{named.Count} analysis findings held in one window";
        var context = new AlertContext { Incidents = new List<AlertIncident>() };
        foreach (var a in named)
        {
            var item = new AlertDetailItem { Heading = $"{a.ServerName} — {a.MetricName}" };
            item.Fields.Add(("Severity", a.Severity.ToString("F2", CultureInfo.InvariantCulture)));
            item.Fields.Add(("Finding", a.CurrentValue));
            context.Details.Add(item);
            var incident = AlertFingerprint.ForKey(a.ServerName, IncidentKind, $"{a.ServerId}:{a.MetricName}",
                new[] { a.MetricName });
            if (incident is not null)
                context.Incidents.Add(incident);
        }
        return (serverName, currentValue, context);
    }

    /// <summary>The detail text a named page's own row persists: its own prose, noting the summary.</summary>
    public static string RowDetailText(FindingAlert alert, int summaryCount) =>
        $"{alert.DetailText}\n  Delivered in a summary of {summaryCount}.";
}

/// <summary>
/// A composed analysis-finding alert, ready to dispatch. Carries both the display strings
/// and the numeric severity/threshold so each app's <see cref="IFindingAlertSender"/>
/// implementation can persist its native row shape without loss.
/// <para>
/// <b>The prose has two destinations, and they are not the same value.</b> <c>DetailText</c> is what
/// the alert-history row PERSISTS — <c>config_alert_log.detail_text</c>, read by the MCP alert reader,
/// the triage endpoint, the Viewer's detail pane and the Dashboard's no-channel tray row, and PARSED by
/// <c>AlertMuteContext.PopulateFromDetailText</c> for the mute pre-fill. <see cref="DeliveredProse"/> is
/// what a delivery CHANNEL renders. Separating them is what lets a producer whose prose merely restates
/// its own structured context stop delivering the same facts twice without changing a persisted column
/// that every one of those surfaces depends on.
/// </para>
/// </summary>
/// <param name="DeliverDetailText">
/// Whether a delivery channel renders <c>DetailText</c> as its own prose section. False when the prose
/// restates <c>Context</c> under different labels: the channels all render the structured context, so
/// both together print every fact twice. The PRODUCER answers this, because only the producer knows how
/// it built the pair — <c>AlertDetailText.ProseForDelivery</c> compares the two texts and cannot see it
/// when the labels and separators differ.
/// </param>
/// <param name="Route">
/// #3712: where the corroboration gate sent this finding. <see cref="FindingRoute.Page"/> is the pre-#3712
/// path — the channels run and the row records what they did. <see cref="FindingRoute.Digest"/> means no
/// channel is consulted and the row records <see cref="AlertDelivery.RoutedToDigest"/>. Trailing and
/// defaulted to Page so a caller that predates the gate (the deprecated Dashboard, the test-send paths) keeps
/// its behaviour byte for byte; the shared notification service always states it.
/// </param>
public sealed record FindingAlert(
    string MetricName,
    string ServerName,
    string CurrentValue,
    string ThresholdValue,
    string ServerId,
    AlertContext Context,
    double Severity,
    double NotifyThreshold,
    string DetailText,
    bool DeliverDetailText,
    FindingRoute Route = FindingRoute.Page)
{
    /// <summary>
    /// The prose a delivery channel should render for this alert, or null when the structured
    /// <c>Context</c> already carries every fact it states.
    /// <para>Resolved once here rather than at each <see cref="IFindingAlertSender"/>, so both SKUs read
    /// one answer instead of deriving it twice from the same flag.</para>
    /// </summary>
    public string? DeliveredProse => DeliverDetailText ? DetailText : null;
}
