/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
    /// Latest <c>alert_time</c> for (serverId, metricName) across any channel/result —
    /// seeds the analysis per-finding cooldown across restarts.
    /// </summary>
    Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName);

    /// <summary>
    /// Sends a composed analysis-finding alert through this app's channels and records it
    /// per this app's alert-history cadence. Never throws.
    /// </summary>
    Task SendFindingAlertAsync(FindingAlert alert);
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
/// that five reading surfaces depend on.
/// </para>
/// </summary>
/// <param name="DeliverDetailText">
/// Whether a delivery channel renders <c>DetailText</c> as its own prose section. False when the prose
/// restates <c>Context</c> under different labels: the channels all render the structured context, so
/// both together print every fact twice. The PRODUCER answers this, because only the producer knows how
/// it built the pair — <c>AlertDetailText.ProseForDelivery</c> compares the two texts and cannot see it
/// when the labels and separators differ.
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
    bool DeliverDetailText)
{
    /// <summary>
    /// The prose a delivery channel should render for this alert, or null when the structured
    /// <c>Context</c> already carries every fact it states.
    /// <para>Resolved once here rather than at each <see cref="IFindingAlertSender"/>, so both SKUs read
    /// one answer instead of deriving it twice from the same flag.</para>
    /// </summary>
    public string? DeliveredProse => DeliverDetailText ? DetailText : null;
}
