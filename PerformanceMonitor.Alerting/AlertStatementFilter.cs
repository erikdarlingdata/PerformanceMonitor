/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
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
/// (through <see cref="SensitiveStatements.Xml(string?, int)"/>); each incident's involved objects and forensic field values. A value
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
/// <para><b>Names.</b> A custom alert rule's display name is judged (it rides subjects and titles), and so is the
/// name on the resolution rows the rule's evaluator writes (<c>CustomAlertEvaluator.JudgedRuleName</c>), because those
/// rows go out to the history store. The server name and the metric name are routing and pairing keys and stay as
/// typed, and local log lines (which never leave the machine) are not filtered.</para>
///
/// <para><b>Budget.</b> One <c>JudgeBudget</c> per <see cref="Apply(AlertOutcome)"/> call, shared by every value
/// it judges. It starts at 1.5 s and earns 0.5 s per 1,048,576 characters of each distinct document it judges (the
/// alert's attachment, each incident's attachment, a long detail value), up to 10 s, so several incidents with large
/// reports do not starve each other and one alert still stalls for a bounded time. A document that appears twice
/// (the alert's attachment is also its first incident's) is judged once per budget and the repeat reuses the first
/// result (#5477). Past the limit a value is withheld unjudged (the marker), never passed. A list of finding alerts
/// shares one budget across the list.</para>
///
/// <para><b>Failure.</b> Never throws and never lets the input through after a failure: the alert is delivered
/// with its detail items cleared, its attachments dropped, each incident's objects, forensic fields and attachment
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

        WaitForWarmUp();
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
            /* #5320 L2: an alert with no detail text (a per-event PostgreSQL deadlock or blocking alert) would
               otherwise be delivered cleared with no reason, so its context carries the neutral sentence as a
               detail item, and a context-less one carries it as its detail text. */
            var noDetailText = string.IsNullOrEmpty(outcome.DetailText);
            return Remember(outcome with
            {
                Context = Cleared(outcome.Context, withReason: noDetailText),
                DetailText = noDetailText && outcome.Context is not null ? outcome.DetailText : SensitiveStatements.PlaceholderText,
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

        WaitForWarmUp();
        try
        {
            return ApplyCore(context, new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget));
        }
#pragma warning disable CA1031 // fail closed
        catch (Exception)
#pragma warning restore CA1031
        {
            return Cleared(context, withReason: true);
        }
    }

    /// <summary>
    /// A finding alert with its context and detail text judged (an analysis finding quotes the statements of
    /// its drill-down). Returns the SAME instance when nothing is named. Never throws.
    /// </summary>
    public static FindingAlert Apply(FindingAlert alert)
    {
        ArgumentNullException.ThrowIfNull(alert);
        WaitForWarmUp();
        return ApplyFinding(alert, new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget));
    }

    /// <summary>The finding alerts of one summary message, judged under ONE budget.</summary>
    public static IReadOnlyList<FindingAlert> Apply(IReadOnlyList<FindingAlert> alerts)
    {
        ArgumentNullException.ThrowIfNull(alerts);

        WaitForWarmUp();
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
                Context = Cleared(alert.Context, withReason: string.IsNullOrEmpty(alert.DetailText)) ?? alert.Context,
                DetailText = string.IsNullOrEmpty(alert.DetailText) ? alert.DetailText : SensitiveStatements.PlaceholderText,
            };
        }
    }

    /// <summary>
    /// Runs a blocked-process report and a deadlock graph of about 64 KB each, built here from made-up names, through the
    /// alert path (and through the MCP and web sweep path) on a background thread, so the first real alert does not
    /// pay for the first calls: the XML reader, the lazily built statement patterns and the filter's own code are built
    /// here, and the hot methods are called enough times, with pauses between the rounds so the runtime's background
    /// compiler gets to replace their first, unoptimized code (#5477). At least 40 judged documents, but never more than
    /// about 2 seconds, off the startup path. Never throws and logs nothing; the task it returns always completes normally.
    /// </summary>
    /// <param name="probe">What to run in place of the default; a test passes one that throws.</param>
    public static Task WarmUpAsync(Action? probe = null)
    {
        var warmUp = Task.Run(async () =>
        {
            /* The warm-up's own Apply and Json calls never wait on the warm-up they belong to. The flag is async-local,
               so it follows the warm-up across its awaits and never reaches a caller's thread. */
            s_insideWarmUp.Value = true;
            await RunWarmUpAsync(probe).ConfigureAwait(false);
        });
        Volatile.Write(ref s_warmUp, warmUp);
        return warmUp;
    }

    /// <summary>The longest an alert waits for a warm-up that is still running (#5478); then it is judged as it would be cold.</summary>
    internal static readonly TimeSpan WarmUpWaitLimit = TimeSpan.FromSeconds(3);

    private static Task? s_warmUp;
    private static readonly AsyncLocal<bool> s_insideWarmUp = new();

    /// <summary>
    /// #5478: the warm-up starts at the same moment as the first stored alerts are read (Lite judges the stored fleet
    /// as soon as its window is up), and the first walks of a large report in a cold process can cost more than the
    /// judging budget, which would clear the report. So an alert that arrives while the warm-up is still running waits
    /// for it, but never longer than <see cref="WarmUpWaitLimit"/>. A warm-up that finished, threw, was never started,
    /// or is the caller itself costs nothing. Never throws.
    /// </summary>
    private static void WaitForWarmUp() => WaitForWarmUp(Volatile.Read(ref s_warmUp), WarmUpWaitLimit);

    internal static bool WaitForWarmUp(Task? warmUp, TimeSpan limit)
    {
        if (warmUp is null || warmUp.IsCompleted || s_insideWarmUp.Value)
        {
            return true;
        }

        try
        {
            return warmUp.Wait(limit);
        }
#pragma warning disable CA1031 // a failed wait changes nothing: the alert is judged as it would be cold
        catch (Exception)
#pragma warning restore CA1031
        {
            return false;
        }
    }

    private static async Task RunWarmUpAsync(Action? probe)
    {
        try
        {
            if (probe is not null)
            {
                probe();
            }
            else
            {
                await WarmUpDocumentsAsync().ConfigureAwait(false);
            }
        }
#pragma warning disable CA1031 // a warm-up that fails changes nothing: the first real call does the same work
        catch (Exception)
#pragma warning restore CA1031
        {
        }
    }

    private const int WarmUpRounds = 4;
    private const int WarmUpCallsPerRound = 12;
    private const int WarmUpDocumentChars = 64 * 1024;
    private static readonly TimeSpan s_warmUpPause = TimeSpan.FromMilliseconds(150);
    private static readonly TimeSpan s_warmUpLimit = TimeSpan.FromSeconds(2);

    private static async Task WarmUpDocumentsAsync()
    {
        var started = Stopwatch.GetTimestamp();
        var documents = new[]
        {
            WarmUpDocument(blocked: true, comment: false),
            WarmUpDocument(blocked: true, comment: true),
            WarmUpDocument(blocked: false, comment: false),
            WarmUpDocument(blocked: false, comment: true),
        };
        var plan = JsonSerializer.Serialize(new { plan = documents[1] });

        for (var round = 0; round < WarmUpRounds; round++)
        {
            for (var call = 0; call < WarmUpCallsPerRound; call++)
            {
                if (Stopwatch.GetElapsedTime(started) > s_warmUpLimit)
                {
                    return;
                }

                _ = Apply(new AlertContext { AttachmentXml = documents[call % documents.Length] });
                if (call % 4 == 3)
                {
                    // The MCP and web sweeps read through Json, which builds its own walkers.
                    _ = SensitiveStatements.Json(plan);
                }
            }

            await Task.Delay(s_warmUpPause).ConfigureAwait(false);
        }
    }

    /// <summary>A report of about <see cref="WarmUpDocumentChars"/> characters with plain statements; with
    /// <paramref name="comment"/> the statements hold a line comment, which is the shape the full walk (not the
    /// raw-text shortcut) handles.</summary>
    private static string WarmUpDocument(bool blocked, bool comment)
    {
        var text = new StringBuilder(WarmUpDocumentChars + 512);
        text.Append(blocked
            ? "<blocked-process-report monitorLoop=\"1\"><blocked-process>"
            : "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list>");
        for (var i = 0; text.Length < WarmUpDocumentChars; i++)
        {
            var statement = "SELECT warm_a, warm_b FROM dbo.warm_table_" + i.ToString(CultureInfo.InvariantCulture)
                + (comment ? " -- warm-up note" : string.Empty) + " WHERE warm_id = 7;";
            if (blocked)
            {
                text.Append("<process id=\"process").Append(i.ToString(CultureInfo.InvariantCulture))
                    .Append("\" status=\"suspended\" waitresource=\"KEY: 5:1\"><executionStack><frame line=\"1\">")
                    .Append(statement).Append("</frame></executionStack><inputbuf>").Append(statement)
                    .Append("</inputbuf></process>");
            }
            else
            {
                text.Append("<process id=\"process").Append(i.ToString(CultureInfo.InvariantCulture))
                    .Append("\" status=\"suspended\" isolationlevel=\"read committed\"><executionStack><frame line=\"1\">")
                    .Append(statement).Append("</frame></executionStack><inputbuf>").Append(statement)
                    .Append("</inputbuf></process>");
            }
        }

        text.Append(blocked ? "</blocked-process></blocked-process-report>" : "</process-list></deadlock>");
        return text.ToString();
    }

    internal static AlertContext? ApplyCore(AlertContext? context, SensitiveStatements.JudgeBudget budget)
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
        /* #5320 L1: a null element has nothing to judge; it must not make the judge throw (and then the
           failure path throw again). */
        if (incident is null)
        {
            return incident!;
        }

        /* #5320 M1: the objects an incident is about are text too. The PostgreSQL blocking and deadlock builders
           put the root query and the victim statement there, and the objects reach the email table, the webhook
           payload and the history row's context JSON. The dedup key is an identity or a hash and is not judged. */
        List<string>? objects = null;
        var involved = incident.InvolvedObjects;
        if (involved is { Count: > 0 })
        {
            for (var i = 0; i < involved.Count; i++)
            {
                var judged = SensitiveStatements.TextUnder(budget, involved[i]) ?? involved[i];
                if (ReferenceEquals(judged, involved[i]))
                {
                    continue;
                }

                objects ??= new List<string>(involved);
                objects[i] = judged;
            }
        }

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

        return objects is null && fields is null && ReferenceEquals(attachment, incident.Attachment)
            ? incident
            : incident with
            {
                InvolvedObjects = (IReadOnlyList<string>?)objects ?? incident.InvolvedObjects,
                DetailFields = fields ?? incident.DetailFields,
                Attachment = attachment,
            };
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
    /// (dedup key, counts) without its objects, forensic fields or an attachment. With <paramref name="withReason"/>
    /// the copy carries one detail item holding <see cref="SensitiveStatements.PlaceholderText"/>, so a cleared alert
    /// that has no detail text of its own still says why it has no detail.
    /// </summary>
    private static AlertContext? Cleared(AlertContext? context, bool withReason)
    {
        if (context is null)
        {
            return null;
        }

        var copy = context.ShallowCopy();
        copy.Details = new List<AlertDetailItem>();
        if (withReason)
        {
            copy.Details.Add(new AlertDetailItem { Heading = SensitiveStatements.PlaceholderText });
        }

        copy.AttachmentXml = null;
        copy.AttachmentFileName = null;
        if (context.Incidents is { } incidents)
        {
            var stripped = new List<AlertIncident>(incidents.Count);
            foreach (var incident in incidents)
            {
                /* A null element (#5320 L1) stays as it is: there is nothing in it to leak. */
                stripped.Add(incident is null ? incident! : incident with { InvolvedObjects = Array.Empty<string>(), DetailFields = null, Attachment = null });
            }

            copy.Incidents = stripped;
        }

        return copy;
    }
}
