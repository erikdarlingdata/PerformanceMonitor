/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The statement filter for alert notifications (#5320, part of #4348): every statement or plan an alert would
/// carry out to a channel (email body, webhook fields, the XML attachment, the history row) goes through the
/// same <see cref="SensitiveStatements"/> judge the MCP tools and the web pages use.
///
/// <para><b>Where it runs.</b> <c>AlertEngine.FireAsync</c> applies it to every <see cref="AlertOutcome"/> before
/// the firing is logged or delivered, and both deliverers apply it again at their own entry (#5320), so a caller that
/// hands an outcome straight to a deliverer (the PostgreSQL families, the self alerts, the custom alert rules) is
/// covered too; the filter remembers the instances it judged, so an engine alert is not judged twice (see "Already judged"). The two finding senders apply it to a <see cref="FindingAlert"/>
/// before they compose a message or a history row. Mute rules are evaluated BEFORE the fire, on the raw
/// text, so a rule keyed on a statement still matches; only what leaves the process is filtered.</para>
///
/// <para><b>What is judged.</b> Every detail item's heading, field values, record summaries and texts, and body
/// (through <see cref="SensitiveStatements.Text"/>); the alert-level attachment and each incident's attachment
/// (through <see cref="SensitiveStatements.Xml(string?, int)"/>); each incident's forensic field values. A value
/// that is not named comes back as the SAME instance, and an alert with nothing named comes back as the same
/// object, so the common case allocates nothing.</para>
///
/// <para><b>Already judged.</b> The filter keeps the outcome instances it judged in a private
/// <see cref="ConditionalWeakTable{TKey, TValue}"/>; there is no flag on <see cref="AlertOutcome"/> a caller could set. An
/// instance in the table passes through <see cref="Apply(AlertOutcome)"/> untouched. A <c>with</c> copy is a NEW
/// instance, so text added to a judged outcome after the fact is judged again, and an outcome a caller builds is always
/// judged. Only <c>AlertEngine</c> (same assembly) and the test projects can mark an instance without judging it
/// (<see cref="MarkJudged(AlertOutcome)"/> is internal).</para>
///
/// <para><b>Names.</b> A custom alert rule's display name is judged (it rides subjects and titles, and the resolution
/// row's title is judged by the evaluator that builds it). The server name, the metric name and the metric-name keyed
/// title of a record are routing and pairing keys and stay as typed, and the local log lines (which never leave the
/// machine) are not filtered.</para>
///
/// <para><b>Budget.</b> One 1.5 s <c>JudgeBudget</c> per <see cref="Apply(AlertOutcome)"/> call, shared by every value
/// it judges. Past it a value is withheld unjudged (the marker), never passed. A list of finding alerts shares one
/// budget across the list.</para>
///
/// <para><b>Failure.</b> Never throws and never lets the input through after a failure: the alert is delivered
/// with its detail items cleared, its attachments dropped, each incident's forensic fields and attachment
/// dropped (the dedup keys stay, so cooldown and per-event splitting still work), and its detail text and short
/// message set to <see cref="SensitiveStatements.PlaceholderText"/>.</para>
/// </summary>
public static class AlertStatementFilter
{
    /// <summary>
    /// The outcome with its context, detail text and short message judged. Returns the SAME instance when
    /// nothing is named.
    /// <para><b>Detail text.</b> When the original <see cref="AlertOutcome.DetailText"/> is exactly the flattening of
    /// the original context (every engine alert), it is rebuilt from the filtered context, so the flat text and the
    /// structured detail say the same thing; otherwise (a self-alert's own prose, an analysis finding's prose) it
    /// is judged whole. <see cref="AlertOutcome.ShortMessage"/> is judged whole.</para>
    /// </summary>
    public static AlertOutcome Apply(AlertOutcome outcome)
    {
        ArgumentNullException.ThrowIfNull(outcome);

        /* #5320: an outcome this filter already judged (the engine's FireAsync marks its own) is not judged again
           at the deliverer's choke point. Only instances in the table count: a `with` copy or a caller-built
           outcome is judged. */
        if (Judged.TryGetValue(outcome, out _))
        {
            return outcome;
        }

        try
        {
            var budget = new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget);
            var context = ApplyCore(outcome.Context, budget);

            var detailText = outcome.DetailText;
            if (!string.IsNullOrEmpty(detailText))
            {
                detailText = outcome.Context is not null
                    && string.Equals(detailText, AlertContextBuilders.ContextToDetailText(outcome.Context), StringComparison.Ordinal)
                        ? (ReferenceEquals(context, outcome.Context) ? detailText : AlertContextBuilders.ContextToDetailText(context))
                        : SensitiveStatements.TextUnder(budget, detailText);
            }

            var shortMessage = SensitiveStatements.TextUnder(budget, outcome.ShortMessage);

            /* A custom alert rule's name is user-written and rides the subject and title (#5320). */
            var displayName = SensitiveStatements.TextUnder(budget, outcome.DisplayName);

            if (ReferenceEquals(context, outcome.Context)
                && ReferenceEquals(displayName, outcome.DisplayName)
                && ReferenceEquals(detailText, outcome.DetailText)
                && ReferenceEquals(shortMessage, outcome.ShortMessage))
            {
                return Remember(outcome);
            }

            return Remember(outcome with { Context = context, DetailText = detailText, ShortMessage = shortMessage, DisplayName = displayName });
        }
#pragma warning disable CA1031 // fail closed: the filter never lets the input through after a failure
        catch (Exception)
#pragma warning restore CA1031
        {
            return Remember(outcome with
            {
                Context = Cleared(outcome.Context),
                DetailText = string.IsNullOrEmpty(outcome.DetailText) ? outcome.DetailText : SensitiveStatements.PlaceholderText,
                ShortMessage = string.IsNullOrEmpty(outcome.ShortMessage) ? outcome.ShortMessage : SensitiveStatements.PlaceholderText,
                DisplayName = string.IsNullOrEmpty(outcome.DisplayName) ? outcome.DisplayName : SensitiveStatements.PlaceholderText,
            });
        }
    }

    /// <summary>
    /// The outcome <see cref="Apply(AlertOutcome)"/> returns, marked as judged (#5320). <c>AlertEngine.FireAsync</c>
    /// calls this after <see cref="Apply(AlertOutcome)"/> (which already remembers what it returns, so this is a
    /// belt for the engine's own call site). Always the SAME instance. Internal: marking without judging is for the
    /// engine and the test projects only, and a caller outside the assembly cannot fake "already filtered" (M2, #5360).
    /// </summary>
    internal static AlertOutcome MarkJudged(AlertOutcome outcome)
    {
        ArgumentNullException.ThrowIfNull(outcome);
        return Remember(outcome);
    }

    /* The instances this filter judged. Weak keys: an outcome nobody holds is collected, and the table never grows. */
    private static readonly ConditionalWeakTable<AlertOutcome, object> Judged = new();
    private static readonly object Mark = new();

    private static AlertOutcome Remember(AlertOutcome outcome)
    {
        Judged.TryAdd(outcome, Mark);
        return outcome;
    }

    /// <summary>
    /// The context judged under one budget of its own: the same instance when nothing is named, otherwise a copy
    /// with the named values replaced. Never throws.
    /// </summary>
    public static AlertContext? Apply(AlertContext? context)
    {
        if (context is null)
        {
            return null;
        }

        try
        {
            return ApplyCore(context, new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget));
        }
#pragma warning disable CA1031 // fail closed
        catch (Exception)
#pragma warning restore CA1031
        {
            return Cleared(context);
        }
    }

    /// <summary>
    /// A finding alert with its context and detail text judged (an analysis finding quotes the statements of
    /// its drill-down). Returns the SAME instance when nothing is named. Never throws.
    /// </summary>
    public static FindingAlert Apply(FindingAlert alert)
    {
        ArgumentNullException.ThrowIfNull(alert);
        return ApplyFinding(alert, new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget));
    }

    /// <summary>The finding alerts of one summary message, judged under ONE budget.</summary>
    public static IReadOnlyList<FindingAlert> Apply(IReadOnlyList<FindingAlert> alerts)
    {
        ArgumentNullException.ThrowIfNull(alerts);

        var budget = new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget);
        List<FindingAlert>? result = null;
        for (var i = 0; i < alerts.Count; i++)
        {
            var judged = ApplyFinding(alerts[i], budget);
            if (ReferenceEquals(judged, alerts[i]))
            {
                continue;
            }

            result ??= new List<FindingAlert>(alerts);
            result[i] = judged;
        }

        return result ?? alerts;
    }

    private static FindingAlert ApplyFinding(FindingAlert alert, SensitiveStatements.JudgeBudget budget)
    {
        try
        {
            var context = ApplyCore(alert.Context, budget) ?? alert.Context;
            var detailText = SensitiveStatements.TextUnder(budget, alert.DetailText) ?? alert.DetailText;
            return ReferenceEquals(context, alert.Context) && ReferenceEquals(detailText, alert.DetailText)
                ? alert
                : alert with { Context = context, DetailText = detailText };
        }
#pragma warning disable CA1031 // fail closed
        catch (Exception)
#pragma warning restore CA1031
        {
            return alert with
            {
                Context = Cleared(alert.Context) ?? alert.Context,
                DetailText = string.IsNullOrEmpty(alert.DetailText) ? alert.DetailText : SensitiveStatements.PlaceholderText,
            };
        }
    }

    private static AlertContext? ApplyCore(AlertContext? context, SensitiveStatements.JudgeBudget budget)
    {
        if (context is null)
        {
            return null;
        }

        var details = Rewrite(context.Details, item => ApplyItem(item, budget));
        var attachmentXml = SensitiveStatements.Xml(context.AttachmentXml, budget);
        var incidents = Rewrite(context.Incidents, incident => ApplyIncident(incident, budget));

        if (ReferenceEquals(details, context.Details)
            && ReferenceEquals(attachmentXml, context.AttachmentXml)
            && ReferenceEquals(incidents, context.Incidents))
        {
            return context;
        }

        var copy = context.ShallowCopy();
        copy.Details = details!;
        copy.AttachmentXml = attachmentXml;
        copy.Incidents = incidents;
        return copy;
    }

    private static AlertDetailItem ApplyItem(AlertDetailItem item, SensitiveStatements.JudgeBudget budget)
    {
        var heading = SensitiveStatements.TextUnder(budget, item.Heading) ?? item.Heading;
        var body = SensitiveStatements.TextUnder(budget, item.Body);

        List<(string Label, string Value)>? fields = null;
        for (var i = 0; i < item.Fields.Count; i++)
        {
            var judged = SensitiveStatements.TextUnder(budget, item.Fields[i].Value) ?? item.Fields[i].Value;
            if (ReferenceEquals(judged, item.Fields[i].Value))
            {
                continue;
            }

            fields ??= new List<(string Label, string Value)>(item.Fields);
            fields[i] = (item.Fields[i].Label, judged);
        }

        var records = Rewrite(item.Records, record => ApplyRecord(record, budget));

        if (ReferenceEquals(heading, item.Heading)
            && ReferenceEquals(body, item.Body)
            && fields is null
            && ReferenceEquals(records, item.Records))
        {
            return item;
        }

        var copy = item.ShallowCopy();
        copy.Heading = heading;
        copy.Body = body;
        copy.Fields = fields ?? item.Fields;
        copy.Records = records!;
        return copy;
    }

    private static AlertDetailRecord ApplyRecord(AlertDetailRecord record, SensitiveStatements.JudgeBudget budget)
    {
        var summary = SensitiveStatements.TextUnder(budget, record.Summary) ?? record.Summary;

        List<(string Label, string Text)>? texts = null;
        for (var i = 0; i < record.Texts.Count; i++)
        {
            var judged = SensitiveStatements.TextUnder(budget, record.Texts[i].Text) ?? record.Texts[i].Text;
            if (ReferenceEquals(judged, record.Texts[i].Text))
            {
                continue;
            }

            texts ??= new List<(string Label, string Text)>(record.Texts);
            texts[i] = (record.Texts[i].Label, judged);
        }

        return ReferenceEquals(summary, record.Summary) && texts is null
            ? record
            : record with { Summary = summary, Texts = texts ?? record.Texts };
    }

    private static AlertIncident ApplyIncident(AlertIncident incident, SensitiveStatements.JudgeBudget budget)
    {
        List<AlertIncidentField>? fields = null;
        if (incident.DetailFields is { Count: > 0 } source)
        {
            for (var i = 0; i < source.Count; i++)
            {
                var judged = SensitiveStatements.TextUnder(budget, source[i].Value) ?? source[i].Value;
                if (ReferenceEquals(judged, source[i].Value))
                {
                    continue;
                }

                fields ??= new List<AlertIncidentField>(source);
                fields[i] = source[i] with { Value = judged };
            }
        }

        var attachment = incident.Attachment;
        if (attachment is not null)
        {
            var xml = SensitiveStatements.Xml(attachment.Xml, budget);
            if (!ReferenceEquals(xml, attachment.Xml))
            {
                attachment = attachment with { Xml = xml! };
            }
        }

        return fields is null && ReferenceEquals(attachment, incident.Attachment)
            ? incident
            : incident with { DetailFields = fields ?? incident.DetailFields, Attachment = attachment };
    }

    /// <summary>A list with each element rewritten, or the SAME list when no element changed.</summary>
    private static List<T>? Rewrite<T>(List<T>? list, Func<T, T> rewrite)
        where T : class
    {
        if (list is null)
        {
            return null;
        }

        List<T>? result = null;
        for (var i = 0; i < list.Count; i++)
        {
            var judged = rewrite(list[i]);
            if (ReferenceEquals(judged, list[i]))
            {
                continue;
            }

            result ??= new List<T>(list);
            result[i] = judged;
        }

        return result ?? list;
    }

    /// <summary>
    /// The failure shape: a copy with no detail items, no attachment, and every incident reduced to its identity
    /// (dedup key, objects, counts) without forensic fields or an attachment.
    /// </summary>
    private static AlertContext? Cleared(AlertContext? context)
    {
        if (context is null)
        {
            return null;
        }

        var copy = context.ShallowCopy();
        copy.Details = new List<AlertDetailItem>();
        copy.AttachmentXml = null;
        copy.AttachmentFileName = null;
        if (context.Incidents is { } incidents)
        {
            var stripped = new List<AlertIncident>(incidents.Count);
            foreach (var incident in incidents)
            {
                stripped.Add(incident with { DetailFields = null, Attachment = null });
            }

            copy.Incidents = stripped;
        }

        return copy;
    }
}
