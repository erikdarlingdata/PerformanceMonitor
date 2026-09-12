/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// How deadlock/blocking alerts are delivered (#1141). <see cref="Summary"/> is the default — one
/// batched notification per alert cycle listing all incidents. <see cref="PerEvent"/> sends one
/// notification per distinct incident so downstream automation (e.g. a Logic App) can open/track one
/// ticket per incident and count recurrences via the #1140 dedup fingerprint.
/// </summary>
public enum AlertNotificationMode
{
    Summary = 0,
    PerEvent = 1
}

/// <summary>
/// #1236: resolves the effective alert delivery mode for a single server. A per-server override wins;
/// a null override inherits the global <see cref="AlertNotificationMode"/> default. Centralized here so
/// Lite and Dashboard apply identical precedence (e.g. Per-event for one noisy prod box while the global
/// default stays Summary everywhere else).
/// </summary>
public static class AlertDeliveryModeResolver
{
    public static AlertNotificationMode Resolve(AlertNotificationMode? perServerOverride, AlertNotificationMode global)
        => perServerOverride ?? global;
}

/// <summary>
/// Splits a built alert <see cref="AlertContext"/> into per-incident messages for #1141 Per-event mode.
/// Each distinct incident (already grouped + fingerprinted by #1140) becomes one message carrying that
/// single incident; when the incident count exceeds the per-cycle cap, the overflow incidents are
/// batched into a final "+N more" message so no fingerprint is ever dropped (the requester's "don't
/// silently truncate"). Recurrence handling is left to the existing edge-triggered alert gating + the
/// consumer's fingerprint dedup — this helper only shapes delivery.
/// </summary>
public static class PerEventNotification
{
    /// <summary>One per-event notification to send: the single-incident (or overflow) context plus the
    /// "current value" string the caller passes to its alert sender. <see cref="NumericValue"/> carries
    /// the same value as a number for the history stores (#1830): the overflow message's text
    /// ("+N more incident(s) this cycle") is not parseable, and a store falling back to text-parsing
    /// it silently recorded 0.</summary>
    public sealed record Message(AlertContext Context, string CurrentValue, bool IsOverflow, double? NumericValue = null);

    /// <summary>
    /// Produces one message per incident (capped at <paramref name="maxPerCycle"/>), with a trailing
    /// overflow message carrying any remaining incidents. Returns an empty list when the source has no
    /// incidents — the caller then falls back to a single Summary send. Never mutates <paramref name="source"/>.
    /// </summary>
    public static List<Message> Split(AlertContext source, int maxPerCycle)
    {
        var messages = new List<Message>();
        if (source?.Incidents is not { Count: > 0 } incidents)
            return messages;

        var cap = Math.Max(1, maxPerCycle);

        foreach (var incident in incidents.Take(cap))
        {
            var carried = new List<AlertIncident> { incident };
            var ctx = NewContext(source, carried);
            ctx.Incidents = carried;
            // includeDetailFields: true — the per-event card has room for this one incident's full
            // forensic detail (Victim SQL / Processes / queries), which Summary's batched card splits
            // across the builder's own items.
            ctx.Details.Add(AlertIncidentRenderer.BuildItem(incident, "Incident", includeDetailFields: true));
            messages.Add(new Message(ctx, DescribeIncident(incident), IsOverflow: false, NumericValue: incident.OccurrenceCount));
        }

        var overflow = incidents.Skip(cap).ToList();
        if (overflow.Count > 0)
        {
            var ctx = NewContext(source, overflow);
            ctx.Incidents = new List<AlertIncident>(overflow);
            for (int n = 0; n < overflow.Count; n++)
                ctx.Details.Add(AlertIncidentRenderer.BuildItem(overflow[n], $"Incident {n + 1} of {overflow.Count}", includeDetailFields: true));
            messages.Add(new Message(ctx, $"+{overflow.Count} more incident(s) this cycle", IsOverflow: true, NumericValue: overflow.Count));
        }

        return messages;
    }

    /* A fresh context for the incidents ONE message carries: the source's severity override, plus the
       forensic attachment (deadlock_graph.xml / blocked_process_report.xml) belonging to those incidents so
       per-event email keeps the file #1146 put there.

       #3330: the attachment comes from the incidents, not from the source. Copying the source's put the
       FIRST graph in the window on all N messages, so an alert carrying five fingerprints sent five emails
       whose attachments described one of them — the other four documented a deadlock their own card did not
       mention. The overflow message resolves against its own batch for the same reason. */
    private static AlertContext NewContext(AlertContext source, IReadOnlyList<AlertIncident> carried)
    {
        var attachment = AlertIncidentAttachment.ForIncidents(carried);
        return new AlertContext
        {
            SeverityOverride = source.SeverityOverride,
            AttachmentXml = attachment?.Xml,
            AttachmentFileName = attachment?.FileName
        };
    }

    // The alert's "current value" for a single-incident card: the occurrence count (a number, matching
    // the Summary card's count), not the involved-objects string (which already shows as its own fact).
    private static string DescribeIncident(AlertIncident incident) => incident.OccurrenceCount.ToString();
}
