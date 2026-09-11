/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The alert's flat detail block, and the one rule every delivery channel applies before rendering it.
/// <para>
/// Two different things arrive at a channel builder under the name "detail". <see cref="AlertContext.Details"/>
/// is STRUCTURED — headings, labelled fields, code blocks — and every channel here has always rendered it.
/// An alert record's <c>DetailText</c> is PROSE: what happened, what it costs, and the operator action that
/// clears it. Self-alerts (store disk pressure, capture down, agent not running, collection health,
/// retention held) carry only the prose and fire with <c>context: null</c>, so a channel that renders only
/// the structured collection delivers a metric name, a value and a threshold with the remedy discarded.
/// </para>
/// </summary>
public static class AlertDetailText
{
    /// <summary>
    /// Flattens an <see cref="AlertContext"/> into the plain-text detail block persisted in alert history
    /// and rendered in plain-text notification bodies. Null when there is nothing to render.
    /// <para>Lives here rather than beside the context builders because
    /// <see cref="ProseForDelivery"/> has to compare against it and this project cannot see the Alerting
    /// project. <c>AlertContextBuilders.ContextToDetailText</c> forwards to it, so there is exactly one
    /// implementation and the comparison cannot drift away from the text it is comparing to.</para>
    /// </summary>
    public static string? Flatten(AlertContext? context)
    {
        if (context == null || context.Details.Count == 0)
        {
            return null;
        }

        var sb = new System.Text.StringBuilder();
        foreach (var detail in context.Details)
        {
            if (sb.Length > 0)
            {
                sb.AppendLine();
            }

            sb.AppendLine(detail.Heading);
            foreach (var (label, value) in detail.Fields)
            {
                sb.AppendLine($"  {label}: {value}");
            }
        }

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// The prose a channel should render, or null when there is none to add.
    /// <para>
    /// Every engine alert's detail text IS <see cref="Flatten"/> of its own context — <c>AlertEngine</c>
    /// builds it that way at eleven fire sites — so rendering both would print the same content twice in
    /// the alerts that already read correctly (blocking, deadlocks, long-running queries, low disk, jobs).
    /// Suppressing on that equality keeps those deliveries byte-for-byte unchanged while the prose-only
    /// alerts gain their detail. It is not the same as suppressing whenever a context is present: the
    /// #2109 AG database alerts carry BOTH a structured context and independent prose naming the
    /// <c>SET HADR RESUME</c> remedy, and that prose must still be delivered.
    /// </para>
    /// <para>
    /// Compared against the shared <see cref="Flatten"/> rather than a local reimplementation, so the two
    /// texts are equal by construction. Should they ever disagree anyway, the alert is delivered twice —
    /// visible and harmless — rather than having its remedy dropped again, which is the failure this whole
    /// path exists to stop.
    /// </para>
    /// </summary>
    public static string? ProseForDelivery(string? detailText, AlertContext? context)
    {
        if (string.IsNullOrWhiteSpace(detailText))
        {
            return null;
        }

        var trimmed = detailText.Trim();
        var flattened = Flatten(context);

        return string.Equals(trimmed, flattened?.Trim(), StringComparison.Ordinal) ? null : trimmed;
    }
}
