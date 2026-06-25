/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Projects the structured <see cref="AlertContext.Incidents"/> (#1140) into renderable
/// <see cref="AlertDetailItem"/>s so the dedup fingerprint + involved objects appear on every
/// surface (Teams facts, Slack fields, email HTML/plaintext, in-app dialog) WITHOUT touching any
/// renderer — they all iterate <see cref="AlertContext.Details"/>.
/// <para>
/// Items are appended (not prepended) so the existing detail order — e.g. the finding path's
/// Diagnosis → Advice → T-SQL → drill-down — is preserved. Downstream automation keys on the fact
/// <b>name</b> ("Dedup Key"), and incidents are the only producer of that fact, so the first
/// "Dedup Key" fact is always the primary incident regardless of absolute position.
/// </para>
/// </summary>
public static class AlertIncidentRenderer
{
    /// <summary>
    /// Sets <paramref name="context"/>.Incidents and appends one detail item per incident. No-op when
    /// there are no incidents. Call once from each alert builder after computing the incidents.
    /// </summary>
    public static void Apply(AlertContext context, IReadOnlyList<AlertIncident>? incidents)
    {
        if (incidents is not { Count: > 0 })
            return;

        context.Incidents = new List<AlertIncident>(incidents);

        for (int n = 0; n < incidents.Count; n++)
        {
            var heading = incidents.Count == 1 ? "Incident" : $"Incident {n + 1} of {incidents.Count}";
            // Summary mode leaves the forensic DetailFields off — the builder already lists the
            // per-incident detail in its own items, so including them here would duplicate.
            context.Details.Add(BuildItem(incidents[n], heading, includeDetailFields: false));
        }
    }

    /// <summary>
    /// Builds one detail item for an incident. When <paramref name="includeDetailFields"/> is true the
    /// incident's forensic <see cref="AlertIncident.DetailFields"/> are emitted first — used by #1141
    /// Per-event delivery, where each card carries a single incident and has room for its full detail
    /// (Victim SQL / Processes / queries) rather than only the dedup metadata.
    /// </summary>
    public static AlertDetailItem BuildItem(AlertIncident incident, string heading, bool includeDetailFields)
    {
        var item = new AlertDetailItem { Heading = heading };
        if (includeDetailFields && incident.DetailFields is { Count: > 0 })
        {
            foreach (var f in incident.DetailFields)
                item.Fields.Add((f.Label, f.Value));
        }
        item.Fields.Add(("Dedup Key", incident.DedupKey));
        item.Fields.Add(("Involved Objects",
            incident.InvolvedObjects.Count > 0 ? string.Join(", ", incident.InvolvedObjects) : "(unresolved)"));
        if (incident.OccurrenceCount > 1)
            item.Fields.Add(("Occurrences", incident.OccurrenceCount.ToString()));
        if (!string.IsNullOrEmpty(incident.WaitRange))
            item.Fields.Add(("Wait Range", incident.WaitRange));
        return item;
    }
}
