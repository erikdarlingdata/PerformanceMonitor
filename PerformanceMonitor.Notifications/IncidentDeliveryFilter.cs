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

namespace PerformanceMonitor.Notifications;

/// <summary>
/// #3313: narrows an alert to the incidents a channel should actually RENDER, given the per-fingerprint
/// <see cref="IncidentCooldown"/> decision that let the alert post at all.
/// <para>
/// <see cref="IncidentCooldown"/> answers "does this alert post", and #1154 makes that answer "yes if ANY
/// fingerprint is outside its own window" so a genuinely distinct incident is never throttled by an
/// unrelated prior one. Nothing answered the second question — WHICH incidents the card then contains — so
/// the channel builders rendered every entry in <see cref="AlertContext.Incidents"/>, including ones still
/// inside their own window that had been delivered minutes earlier. One new fingerprint dragged every
/// co-resident stale fingerprint back into the channel with it. Measured on a 42-server store: a card
/// delivered fingerprint <c>bf7e29b4</c> alone, then 41 minutes later re-delivered it as "Deadlock 2 of 2"
/// riding on a new fingerprint, with <c>Total Occurrences: 1</c> proving it had not recurred — it was simply
/// still inside the rolling read window. Raising the cooldown cannot fix that: the repeat is a passenger on
/// another fingerprint's card, not a second firing of its own.
/// </para>
/// <para>
/// <b>Delivery filters; persistence does not.</b> This returns a COPY and never mutates its input, so the
/// <c>config_alert_log</c> row its caller writes afterwards — <c>detail_text</c> and the context JSON the MCP
/// reader, the triage page, the Viewer's detail pane and <c>AlertMuteContext.PopulateFromDetailText</c> all
/// read — still carries every incident. The same split #3317 shipped for analysis prose
/// (<c>EmailAlertService</c>'s <c>deliverProse</c>), for the same reason.
/// </para>
/// <para>
/// <b>Per-event mode (#1141) does not RE-DELIVER a stale incident, and this filter does not change it.</b>
/// <c>PerEventNotification.Split</c> already emits one message per incident, each carrying a single-incident
/// context, and each goes through its own send — so an in-window fingerprint fails its own
/// <see cref="IncidentCooldown.Decision.ShouldSend"/> and never posts. Its trailing "+N more" overflow
/// message is the one per-event payload carrying several fingerprints, and it is filtered here like any
/// other batch because this sits at the delivery boundary every message crosses.
/// <para>Scoped to re-delivery deliberately: per-event mode had its own separate defect in what it
/// ATTACHED (#3330 — every message carried the first graph in the window), fixed where the splitter builds
/// each message rather than here.</para>
/// </para>
/// </summary>
public static class IncidentDeliveryFilter
{
    /// <summary>
    /// Heading of the one-line footer standing in for the omitted incidents. A footer rather than silence:
    /// an operator reading a one-incident card needs to know the server still has others open, and the
    /// alternative — restating each of them — is the defect.
    /// </summary>
    public const string OtherIncidentsHeading = "Other Incidents";

    /// <summary>Label of the footer's single fact. A fact name is a consumer API (see
    /// <see cref="AlertIncidentRenderer"/>), so it is declared here rather than spelled inline.</summary>
    public const string OtherIncidentsLabel = "Still Open";

    /// <summary>
    /// The delivery-scoped view of an alert.
    /// </summary>
    /// <param name="Context">
    /// What the channel builders should render: the input instance itself when nothing is filtered (so an
    /// unfiltered alert is byte-identical to pre-#3313), otherwise a copy carrying the deliverable incidents,
    /// their detail items, every non-incident item, and the footer.
    /// </param>
    /// <param name="Prose">
    /// The alert's flat prose detail to render, resolved through <see cref="AlertDetailText.ProseForDelivery"/>
    /// against the UNFILTERED context — see the remarks on <see cref="ForDelivery"/> for why that basis is
    /// the only correct one.
    /// </param>
    /// <param name="SuppressedIncidentCount">
    /// How many incidents were held back, so a caller can log or count them. Zero whenever
    /// <see cref="Context"/> is the input instance.
    /// </param>
    public readonly record struct Render(AlertContext? Context, string? Prose, int SuppressedIncidentCount);

    /// <summary>
    /// Filters <paramref name="context"/> down to the incidents named by <paramref name="deliverableDedupKeys"/>
    /// and resolves the prose to deliver alongside them.
    /// <para>
    /// <paramref name="deliverableDedupKeys"/> is <see cref="IncidentCooldown.Decision.DeliverableDedupKeys"/>:
    /// the fingerprints that were outside their own window at evaluation time. <c>null</c> means the alert
    /// carried no fingerprint at all and was evaluated on the metric-level fallback key (CPU, memory,
    /// poison wait, tempdb, failed job, and the #2109 AG database alerts, whose context is one plain detail
    /// item and no incidents) — there is nothing to filter, so the input is returned unchanged.
    /// </para>
    /// <para>
    /// <b>The prose is resolved against the unfiltered context, and that is load-bearing.</b> Every engine
    /// alert's <c>detailText</c> IS <see cref="AlertDetailText.Flatten"/> of its own context, which is how
    /// <see cref="AlertDetailText.ProseForDelivery"/> recognises it as a restatement and suppresses it.
    /// Comparing it against the FILTERED copy instead would find them unequal and print the whole flattened
    /// alert as prose — resurrecting, in prose, exactly the stale incidents just removed from the structure.
    /// </para>
    /// <para>
    /// Five channel builders re-run <see cref="AlertDetailText.ProseForDelivery"/> on whatever prose they are
    /// handed, against the context they are handed. That is a no-op on a null prose and on any prose that
    /// differs from that context's own flatten — but rather than leave it argued, the one shape that would
    /// break it (a prose equal to the FILTERED copy's flatten) takes the unfiltered path below. Failing that
    /// way costs a repeated incident; failing the other way would discard an operator's only copy of a
    /// remedy, which is #3296.
    /// </para>
    /// </summary>
    public static Render ForDelivery(
        AlertContext? context, string? detailText, IReadOnlyList<string>? deliverableDedupKeys)
    {
        var prose = AlertDetailText.ProseForDelivery(detailText, context);

        if (context is null || deliverableDedupKeys is null || context.Incidents is not { Count: > 0 } incidents)
        {
            return new Render(context, prose, 0);
        }

        var deliverable = new HashSet<string>(deliverableDedupKeys, StringComparer.Ordinal);

        var kept = new List<AlertIncident>(incidents.Count);
        var suppressedKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var incident in incidents)
        {
            /* A blank DedupKey produces no cooldown key at all (IncidentCooldown.BuildKeys excludes it), so
               it can never have been throttled and must never be filtered out on a key it does not have. */
            if (string.IsNullOrEmpty(incident.DedupKey) || deliverable.Contains(incident.DedupKey))
            {
                kept.Add(incident);
                continue;
            }

            suppressedKeys.Add(incident.DedupKey);
        }

        /* Nothing to hold back, or nothing left to show. The second case cannot arise from a ShouldSend
           decision (it says at least one key was fresh), so it means the caller paired a context with
           another evaluation's key set; rendering the whole alert is the direction that over-reports rather
           than delivering an empty card. */
        if (suppressedKeys.Count == 0 || kept.Count == 0)
        {
            return new Render(context, prose, 0);
        }

        var suppressedCount = incidents.Count - kept.Count;

        /* #3330: the attachment for the incidents this card KEEPS, resolved through the same rule
           PerEventNotification.NewContext uses. The alert-level AttachmentXml is the first graph in the
           window, which once this filter narrows the card may belong to an incident the card no longer
           mentions — the mismatch #3324 made reachable in Summary mode. Null when none of the kept
           incidents carries one, which is a real outcome on the blocking arm (a chain seen only by the DMV
           fallback has no report) and not a reason to fall back to a stale one. */
        var attachment = AlertIncidentAttachment.ForIncidents(kept);

        var rendered = new AlertContext
        {
            /* Severity is carried for the same reason PerEventNotification.NewContext carries it: it drives
               every channel's accent and describes the alert, not one incident. */
            SeverityOverride = context.SeverityOverride,
            AttachmentXml = attachment?.Xml,
            AttachmentFileName = attachment?.FileName,
            Incidents = kept
        };

        /* An incident's detail item is identified by the Dedup Key fact AlertIncidentRenderer.BuildItem
           always emits — the sole producer of that fact name in the tree, and the name itself comes from
           there rather than a copy of the string. Every other item (advice prose, remediation T-SQL, a
           deadlock victim the fingerprint could not parse, a builder's own data section) carries no such
           fact and is kept: those describe the whole alert, not one fingerprint. */
        foreach (var item in context.Details)
        {
            if (!NamesAnyOf(item, suppressedKeys))
            {
                rendered.Details.Add(item);
            }
        }

        rendered.Details.Add(new AlertDetailItem
        {
            Heading = OtherIncidentsHeading,
            Fields = new List<(string Label, string Value)>
            {
                (OtherIncidentsLabel,
                 string.Format(
                     CultureInfo.InvariantCulture,
                     "{0} other incident(s) still open (already reported)",
                     suppressedCount))
            }
        });

        if (prose is not null &&
            string.Equals(prose, AlertDetailText.Flatten(rendered)?.Trim(), StringComparison.Ordinal))
        {
            return new Render(context, prose, 0);
        }

        return new Render(rendered, prose, suppressedCount);
    }

    private static bool NamesAnyOf(AlertDetailItem item, HashSet<string> dedupKeys)
    {
        foreach (var (label, value) in item.Fields)
        {
            if (string.Equals(label, AlertIncidentRenderer.DedupKeyFactName, StringComparison.Ordinal) &&
                dedupKeys.Contains(value))
            {
                return true;
            }
        }

        return false;
    }
}
