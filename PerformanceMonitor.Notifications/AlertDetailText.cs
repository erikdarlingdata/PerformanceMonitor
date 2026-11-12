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
/// <para>
/// <b>Scope against the never-on-a-webhook T-SQL rule.</b> That rule
/// (<c>WebhookAlertService.RedactForWebhook</c>, and the per-channel <c>IsCodeBlock</c> substitution) targets
/// GENERATED remediation payloads: <see cref="AlertDetailItem.IsCodeBlock"/> bodies and
/// <see cref="AlertDetailItem.Remediation"/> actions built by <c>FactRemediation</c> — multi-statement
/// scripts, two of the seven shapes destructive, carrying a two-sided risk-disclosure header that has to be
/// read before the script is run. The prose here is none of those: it is authored per alert, and where it
/// names a command at all (the #2109 AG alerts' <c>SET HADR RESUME</c>) the command IS the recovery action
/// and the sentence IS the alert. Redacting it would deliver "see email or the in-app dialog" to an operator
/// whose only channel is the one being redacted, which is the #3296 defect over again.
/// <para>A generated command never reaches this text, which is what makes the boundary hold. That is a
/// checked fact, not an author's habit — see
/// <c>Lite.Tests.AnalysisNotificationTests.TheGeneratedRemediationCommand_RidesOnlyInTheCodeBlockItem_NeverInTheProseDetailText</c>.</para>
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

        /* #5642: the persisted detail text carries the sample-age line, once, as its last line. A context
           with no Details still flattens to null (CPU's context has none by design, #3653 A8e): its hand-built
           text carries the line itself, built with SampleAgeLine, and the channel renderers print the typed
           field only when the prose does not already carry it. */
        var ageLine = SampleAgeLine(context.NewestSample);
        if (ageLine is not null)
        {
            sb.AppendLine();
            sb.AppendLine(ageLine);
        }

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// The one-line sample age (#5642): <c>Newest Query Store sample: 15:43:29 UTC, 5m 20s old</c>, or for a
    /// source that ends in " reading" (High CPU: the breaching reading the card's percentage came from, which a
    /// later clear sample can outrun) <c>CPU reading: 15:43:29 UTC, 5m 20s old</c>. Built from the
    /// age the engine computed ONCE when the alert fired and persisted with it, never from the clock at render
    /// time, so a history row opened a day later still says how old the sample was when the alert went out.
    /// Null when there is no sample to describe.
    /// </summary>
    public static string? SampleAgeLine(AlertNewestSample? sample)
    {
        if (sample is null || string.IsNullOrWhiteSpace(sample.Source))
        {
            return null;
        }

        return string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"{LineLead(sample.Source)}: {sample.InstantUtc:HH:mm:ss} UTC, {FormatAge(sample.Age)} old");
    }

    /// <summary>
    /// The sample-age line a channel renderer should print for <paramref name="context"/>, or null when it has
    /// none or <paramref name="prose"/> already carries it (#5642). CPU's hand-built detail text carries the
    /// line itself while its context holds no Details, so a renderer that printed both the prose and the typed
    /// field would say it twice; every other alert's prose is either suppressed as a restatement of the
    /// context, which this line is not part of, or independent prose that never carries it.
    /// </summary>
    public static string? SampleAgeLineFor(AlertContext? context, string? prose)
    {
        var line = SampleAgeLine(context?.NewestSample);
        if (line is null)
        {
            return null;
        }

        return prose is not null && s_ageLine.IsMatch(prose) ? null : line;
    }

    /// <summary>
    /// The sample-age line a surface that holds only a persisted row (its <c>context_json</c> and
    /// <c>detail_text</c>) should show, or null when the row carries no age or <paramref name="prose"/> already
    /// has the line (#5642). Reads the carrier the engine persisted at fire time; never measures against the
    /// clock, so a row opened a day later still says how old the sample was when the alert went out.
    /// </summary>
    public static string? SampleAgeLineFromJson(string? contextJson, string? prose)
    {
        var line = SampleAgeLine(AlertContextSerializer.TryReadNewestSample(contextJson));
        if (line is null)
        {
            return null;
        }

        return prose is not null && s_ageLine.IsMatch(prose) ? null : line;
    }

    /// <summary>
    /// The one-sentence reason the alert was routed as it was (#7124): why a held row went to the Daily Digest, or
    /// why a finding paged. Read from the routing decision the engine persisted in the context, through the same
    /// reader the MCP history read uses (<c>routing_reason</c>). Null when the row carries no routing, no context,
    /// or an empty reason, so a window or tooltip shows nothing rather than a blank row.
    /// </summary>
    public static string? RoutingReasonFromJson(string? contextJson)
    {
        var reason = AlertContextSerializer.TryReadRouting(contextJson)?.Reason;
        return string.IsNullOrWhiteSpace(reason) ? null : reason.Trim();
    }

    /// <summary>
    /// The detail text to persist and deliver for an alert whose context carries a sample age (#5642): the
    /// text unchanged when it already carries the line (the engine built it with the age stamped) or the
    /// context has none; the line appended once when it does not (a text built before the age was stamped);
    /// and the line alone when the text is empty. One choke point in each deliverer, so the history row's
    /// <c>detail_text</c> says what the notification said whichever way the engine built the text.
    /// </summary>
    public static string? WithSampleAge(string? detailText, AlertContext? context)
    {
        var line = SampleAgeLine(context?.NewestSample);
        if (line is null)
        {
            return detailText;
        }

        if (string.IsNullOrWhiteSpace(detailText))
        {
            return line;
        }

        return s_ageLine.IsMatch(detailText) ? detailText : detailText.TrimEnd() + "\n\n" + line;
    }

    /// <summary>
    /// <paramref name="text"/> without any sample-age line (#5642), so two texts that differ only by that line
    /// compare equal. The persisted detail text carries the line; a context the incident filter rebuilt, or one
    /// whose age was stamped after the text was built, may not.
    /// </summary>
    public static string StripSampleAge(string text) => s_ageLine.Replace(text, string.Empty).Trim();

    /// <summary>The words before the time: "Newest {source} sample", or the source itself when it names a
    /// reading ("CPU reading"). alerts.js keeps the same rule.</summary>
    private static string LineLead(string source) =>
        source.EndsWith(" reading", StringComparison.Ordinal) ? source : $"Newest {source} sample";

    private static readonly System.Text.RegularExpressions.Regex s_ageLine = new(
        @"(?m)^[ \t]*(?:Newest [^\r\n:]+ sample|[^\r\n:]+ reading): \d\d:\d\d:\d\d UTC, [^\r\n]+ old[ \t]*(\r?\n|$)",
        System.Text.RegularExpressions.RegexOptions.CultureInvariant | System.Text.RegularExpressions.RegexOptions.Compiled,
        TimeSpan.FromSeconds(1));

    private static string FormatAge(TimeSpan age)
    {
        if (age < TimeSpan.Zero)
        {
            age = TimeSpan.Zero;
        }

        if (age.TotalMinutes < 1)
        {
            return string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{(int)age.TotalSeconds}s");
        }

        if (age.TotalHours < 1)
        {
            return string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{(int)age.TotalMinutes}m {age.Seconds}s");
        }

        if (age.TotalDays < 1)
        {
            return string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{(int)age.TotalHours}h {age.Minutes}m");
        }

        return string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{(int)age.TotalDays}d {age.Hours}h");
    }

    /// <summary>
    /// The prose a channel should render, or null when there is none to add.
    /// <para>
    /// Every engine alert's detail text IS <see cref="Flatten"/> of its own context — <c>AlertEngine</c>
    /// builds each <c>detailText</c> local that way, via <c>AlertContextBuilders.ContextToDetailText</c> —
    /// so rendering both would print the same content twice in the alerts that already read correctly
    /// (blocking, deadlocks, long-running queries, low disk, jobs). Suppressing on that equality keeps
    /// those deliveries byte-for-byte unchanged while the prose-only alerts gain their detail. It is not
    /// the same as suppressing whenever a context is present: the #2109 AG database alerts carry BOTH a
    /// structured context and independent prose naming the <c>SET HADR RESUME</c> remedy, and that prose
    /// must still be delivered.
    /// </para>
    /// <para>
    /// Compared against the shared <see cref="Flatten"/> rather than a local reimplementation, so the two
    /// texts are equal by construction. Should they ever disagree anyway, the alert is delivered twice —
    /// visible and harmless — rather than having its remedy dropped again, which is the failure this whole
    /// path exists to stop.
    /// </para>
    /// <para>
    /// <b>Textual equality is not the only reason a prose is redundant, and this function only sees that
    /// one.</b> A producer that formats the same facts as its own context under different labels defeats
    /// the comparison while delivering pure duplication — <c>FindingMessageFormatter</c> does exactly
    /// that, its <c>DetailText</c> and its <c>Diagnosis</c> item being two renderings of one set of
    /// values. Widening the comparison to "every fact in the context appears somewhere in the prose"
    /// would drop the #2109 remedy above, which is the one case this gate was built NOT to drop. So that
    /// second kind of redundancy is declared by the producer instead, on
    /// <c>FindingAlert.DeliverDetailText</c>, and resolved before a channel is reached.
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

        if (string.Equals(trimmed, flattened?.Trim(), StringComparison.Ordinal))
        {
            return null;
        }

        /* #5642: the same restatement when the two texts differ only by the sample-age line (a text built
           before the age was stamped, or a context rebuilt without it). The channel prints the line from
           the typed field, so nothing is lost by suppressing the prose. */
        if (flattened is not null &&
            string.Equals(StripSampleAge(trimmed), StripSampleAge(flattened), StringComparison.Ordinal))
        {
            return null;
        }

        return trimmed;
    }
}
