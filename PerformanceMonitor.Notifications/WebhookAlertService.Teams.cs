/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Notifications;

public partial class WebhookAlertService
{
    /// <summary>Posts to Teams. Returns null when the post succeeded, or the error text when it did not —
    /// a bool loses the reason, and the reason is what the alert log's <c>send_error</c> carries on a
    /// <see cref="AlertDelivery.ChannelFailed"/> row. <paramref name="webhookUrl"/> is the ROUTED destination
    /// (#3598) — the parent's URL when no route touched this firing — while the proxy stays the parent's:
    /// a route says where a family lands, not how the channel type is reached.</summary>
    private async Task<string?> TrySendTeamsAlertAsync(
        IAlertSettings settings,
        string webhookUrl,
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertContext? context,
        string? triageUrl,
        string? detailText,
        DateTime nowUtc,
        string? displayName = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var payload = BuildTeamsPayload(metricName, serverName, currentValue, thresholdValue, _branding, context: context, triageUrl: triageUrl,
                detailText: detailText, displayName: displayName, nowUtc: nowUtc);
            var error = await PostWebhookAsync(webhookUrl, payload, settings.TeamsProxyAddress, cancellationToken: cancellationToken, bodyLog: _logger);

            if (error != null)
            {
                _consecutiveTeamsFailures++;
                _lastTeamsError = error;

                if (_consecutiveTeamsFailures <= FailingChannelThreshold)
                    _logger.LogError($"TEAMS WEBHOOK FAILED ({_consecutiveTeamsFailures}x): {error}");
                else if (_consecutiveTeamsFailures % 50 == 0)
                    _logger.LogError($"TEAMS WEBHOOK STILL FAILING: {_consecutiveTeamsFailures} failures. Last: {error}");

                return error;
            }

            if (_consecutiveTeamsFailures > 0)
                _logger.LogInformation($"Teams webhook recovered after {_consecutiveTeamsFailures} failure(s)");

            _consecutiveTeamsFailures = 0;
            _lastTeamsError = null;
            _logger.LogInformation($"Teams webhook sent for {metricName} on {serverName}");
            return null;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* #4752: the caller's own cancel is a stop request, not this channel's failure. Passing it on
               keeps it out of the failure count and the Error log, and lets the deliverer's shutdown path
               skip the history row. A timeout never gets here: the post returns it as its error text. */
            throw;
        }
        catch (Exception ex)
        {
            _consecutiveTeamsFailures++;
            _lastTeamsError = ex.Message;
            _logger.LogError($"Teams webhook error: {ex.Message}");
            return ex.Message;
        }
    }

    /// <summary>
    /// #2710: the Datadog-parity <c>resource_name</c> tag — the first incident's involved objects,
    /// joined. Mirrors the SAME "first incident is the correlation anchor" precedent
    /// <see cref="DerivePagerDutyDedupKey"/> already uses for an alert carrying more than one
    /// fingerprint: whichever incident PagerDuty's dedup_key names is also the one a human reading
    /// the tag should look at first. Null when the alert carries no fingerprintable incident (alert
    /// type not wired to #1140's <see cref="AlertContext.Incidents"/>, or the objects were
    /// unresolved) — a top-level tag naming nothing would read worse than the tag being absent.
    /// <see cref="AlertFingerprint.ForObjects"/>'s callers filter out blank objects before they ever
    /// reach here, but a caller of the sibling <see cref="AlertFingerprint.ForKey"/> overload can pass
    /// an unfiltered blank display object (review catch: <c>AlertContextBuilders.VolumeFreeSpaceIncidents</c>
    /// / <c>AnomalousJobIncidents</c> pass the raw mount point / job name) — so the join is checked
    /// for blank the same way <see cref="AlertFingerprint.ForKey"/> already checks <c>Database</c>,
    /// rather than trusting every caller to have pre-filtered.
    /// </summary>
    private static string? DeriveResourceName(AlertContext? context)
    {
        if (context?.Incidents is not { Count: > 0 } incidents)
            return null;

        var objects = incidents[0].InvolvedObjects;
        if (objects.Count == 0)
            return null;

        var joined = string.Join(", ", objects);
        return string.IsNullOrWhiteSpace(joined) ? null : joined;
    }

    /// <summary>
    /// #2710: the Datadog-parity <c>env</c>-adjacent database scope — the first incident's
    /// <see cref="AlertIncident.Database"/>, the SAME incident <see cref="DeriveResourceName"/>
    /// reads (kept as its own tag rather than folded into resource_name: per #2361's doc comment on
    /// <see cref="AlertIncident.Database"/>, it is WHERE the resource lives, not what it is). Null on
    /// every alert type <see cref="AlertIncident.Database"/> already documents as unscoped (a volume
    /// or a job is not database-scoped) or with no incident at all.
    /// </summary>
    private static string? DeriveResourceDatabase(AlertContext? context) =>
        context?.Incidents is { Count: > 0 } incidents ? incidents[0].Database : null;

    /// <summary>
    /// Slack mrkdwn treats <c>&lt;!channel&gt;</c>, <c>&lt;@U123&gt;</c> and <c>&lt;https://...|label&gt;</c> as live
    /// mentions and masked links, so alert text (a title, a detail, a server label, a query) is escaped before it is
    /// placed in a mrkdwn string: <c>&amp;</c>, <c>&lt;</c> and <c>&gt;</c> become their entities, which Slack shows as the
    /// characters themselves. Darling's own formatting (<c>*Server:*</c> and the like) is written outside this call.
    /// </summary>
    internal static string SlackEscape(string? text) =>
        string.IsNullOrEmpty(text)
            ? text ?? string.Empty
            : text.Replace("&", "&amp;", StringComparison.Ordinal)
                  .Replace("<", "&lt;", StringComparison.Ordinal)
                  .Replace(">", "&gt;", StringComparison.Ordinal);

    /// <summary>
    /// A MessageCard text and fact value render markdown, so alert text is placed in them with the characters that form
    /// links, images, emphasis and mentions backslash-escaped: <c>[ ] * `</c>, a <c>_</c> that is not inside a
    /// word (markdown reads <c>query_stats</c> literally), a <c>~</c> in a <c>~~</c> pair, and a backslash that precedes
    /// punctuation (markdown reads that pair as an escape, so a path such as <c>C:\temp</c> is left as it is).
    /// <c>&amp;</c>, <c>&lt;</c> and <c>&gt;</c> become the entities <c>&amp;amp;</c>, <c>&amp;lt;</c> and <c>&amp;gt;</c>
    /// (the ampersand first, so no entity is escaped twice): an entity shows as the character under a markdown
    /// reader and under a reader of the card's limited inline HTML alike.
    /// A MessageCard has no plain-text field, so the escape is the smaller change. Darling's own formatting (the
    /// <c>_label_</c> and backtick spans of a record) is written outside this call.
    /// </summary>
    internal static string TeamsEscape(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text ?? string.Empty;
        }

        static bool IsWordChar(string s, int i) => i >= 0 && i < s.Length && char.IsLetterOrDigit(s[i]);
        static bool IsAsciiPunctuation(char c) => c is >= '!' and <= '/' or >= ':' and <= '@' or >= '[' and <= '`' or >= '{' and <= '~';

        var builder = new StringBuilder(text.Length + 8);
        for (var i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            switch (ch)
            {
                case '&':
                    builder.Append("&amp;");
                    continue;
                case '<':
                    builder.Append("&lt;");
                    continue;
                case '>':
                    builder.Append("&gt;");
                    continue;
            }

            var escape = ch switch
            {
                '[' or ']' or '*' or '`' => true,
                '_' => !(IsWordChar(text, i - 1) && IsWordChar(text, i + 1)),
                '~' => (i > 0 && text[i - 1] == '~') || (i + 1 < text.Length && text[i + 1] == '~'),
                '\\' => i + 1 < text.Length && IsAsciiPunctuation(text[i + 1]),
                _ => false,
            };
            if (escape)
            {
                builder.Append('\\');
            }

            builder.Append(ch);
        }

        return builder.ToString();
    }

    /// <summary>
    /// A record text as the inside of a single-backtick code span: a backtick becomes a straight quote (it would
    /// end the span), and each run of line breaks (CR, LF, CR LF, NEL, and the line and paragraph separators) becomes one
    /// space. A markdown reader ends a paragraph, and with it an open span, at a blank line, so the text stays on one
    /// line and the span closes where the card closes it.
    /// </summary>
    internal static string TeamsCodeSpanText(string? text) =>
        string.IsNullOrEmpty(text)
            ? string.Empty
            : TeamsLineBreakRun.Replace(text.Replace('`', '\''), " ");

    private static readonly Regex TeamsLineBreakRun = new(@"[\r\n\u0085\p{Zl}\p{Zp}]+", RegexOptions.CultureInvariant);

    /// <summary>
    /// The title an alert shows in a header or subject. An alert another service submitted (#5654) is marked
    /// <c>[External: source] title</c>, so it cannot pass as one of Darling's own alerts.
    /// </summary>
    internal static string ExternalTitle(string title, AlertContext? context) =>
        context?.External is { } external
            ? $"[External: {external.Source}] {title}"
            : title;

    /// <summary>
    /// Builds an O365 MessageCard payload for Teams incoming webhooks.
    /// The themeColor property renders as a colored accent bar at the top of the card.
    /// <para>#2710: a non-null <paramref name="triageUrl"/> adds a <c>potentialAction</c> OpenUri button —
    /// the MessageCard-native link affordance — pointing at the computed triage page. Null (base URL unset,
    /// or a test send) renders exactly the pre-#2710 card.</para>
    /// <para>#3297: <paramref name="detailText"/> is the alert's flat prose detail. It becomes its own
    /// <c>Details</c> section ahead of the per-incident ones, because a multi-paragraph remedy in a
    /// label/value fact renders as an unreadable column — <c>text</c> is the MessageCard-native place for
    /// prose, and the snooze footer already uses it.</para>
    /// <para>#3612: this card carries NO analogue of the Slack block budget, on purpose. The MessageCard
    /// format has no section-count limit (its reference offers "don't include more than 10 sections" as a
    /// readability guideline, not a rejection), and the connector's one hard ceiling is the ~28 KB payload.
    /// The analysis page that Slack lost renders here at 16.6 KB with its 21 items and 17.4 KB at the 25
    /// items the heaviest shape the story can produce (fifteen incidents, the most three five-row
    /// drill-downs can surface) — about 210 bytes per incident, so the cap is some fifty incidents away
    /// from any shape a producer can emit. <c>SlackDetailsSizeTests</c> pins the heaviest shape against
    /// the cap so a producer that widens the page finds out there rather than in a lost delivery.</para>
    /// </summary>
    internal static string BuildTeamsPayload(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertBranding branding,
        bool isTest = false,
        AlertContext? context = null,
        string? triageUrl = null,
        string? detailText = null,
        string? displayName = null,
        DateTime? nowUtc = null)
    {
        var (hexColor, badgeText, emoji) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var prose = AlertDetailText.ProseForDelivery(detailText, context);
        /* Title/summary show the human name when present; ForMetric above stays on the immutable metric
           name (the severity key), and a null/empty display name renders the metric name unchanged. */
        var titleName = ExternalTitle(string.IsNullOrEmpty(displayName) ? metricName : displayName, context);
        var themeColor = hexColor.TrimStart('#');
        /* One instant, with the local rendering DERIVED from it rather than read separately: two reads can
           land either side of a second, which would have the card's own "Time (UTC)" and "Time (Local)"
           facts naming two different seconds of one alert. SpecifyKind because an injected clock may hand
           back an Unspecified DateTime, which ToLocalTime would otherwise treat as already local. */
        var utcNow = nowUtc ?? DateTime.UtcNow;
        var localNow = DateTime.SpecifyKind(utcNow, DateTimeKind.Utc).ToLocalTime();

        var facts = new List<object>();

        if (isTest)
        {
            facts.Add(new { name = "Status", value = "Webhook configuration is working correctly" });
            facts.Add(new { name = "Sent at", value = localNow.ToString("yyyy-MM-dd HH:mm:ss") });
        }
        else
        {
            facts.Add(new { name = "Server", value = TeamsEscape(serverName) });
            var resourceName = DeriveResourceName(context);
            if (resourceName is not null)
                facts.Add(new { name = "Resource", value = TeamsEscape(resourceName) });
            var database = DeriveResourceDatabase(context);
            if (!string.IsNullOrEmpty(database))
                facts.Add(new { name = "Database", value = TeamsEscape(database) });
            facts.Add(new { name = "Current Value", value = TeamsEscape(currentValue) });
            /* #5825: an alert resolution has no threshold, and an empty fact says nothing. */
            if (!string.IsNullOrEmpty(thresholdValue))
                facts.Add(new { name = "Threshold", value = TeamsEscape(thresholdValue) });
            facts.Add(new { name = "Time (UTC)", value = utcNow.ToString("yyyy-MM-dd HH:mm:ss") });
            facts.Add(new { name = "Time (Local)", value = localNow.ToString("yyyy-MM-dd HH:mm:ss") });
        }

        /* #2108: each Fields-carrying detail item becomes its OWN section further down, so a
           multi-incident alert reads as labeled, self-contained units instead of one flat fact
           list where a victim's fields and its fingerprint's fields drift apart. Advice prose and
           remediation-T-SQL items stay folded into the lead section's facts — they are commentary
           on the whole alert, not incidents. */
        var itemSections = new List<object>();

        /* #3297: ahead of the per-incident sections — it is the actionable half of the alert. */
        if (prose is not null)
        {
            itemSections.Add(new { activityTitle = "Details", text = TeamsEscape(prose), markdown = true });
        }

        if (context?.Details != null)
        {
            foreach (var detail in context.Details)
            {
                if (detail.IsCodeBlock)
                {
                    /* Remediation T-SQL: point at the email / in-app dialog, never inline it. */
                    facts.Add(new { name = detail.Heading, value = TsqlWebhookHint });
                    continue;
                }

                if (!string.IsNullOrEmpty(detail.Body))
                {
                    /* Advice prose: one "Advice" fact for the headline, then a fact per
                       Investigation/Remediation paragraph (split on the blank line).
                       Body is exclusive with Fields, matching the email and Slack surfaces
                       which skip Fields when Body is present. */
                    facts.Add(new { name = "Advice", value = TeamsEscape(detail.Heading) });
                    foreach (var para in detail.Body.Split("\n\n", StringSplitOptions.RemoveEmptyEntries))
                    {
                        var (label, text) = SplitProseLabel(para);
                        facts.Add(new { name = TeamsEscape(label), value = TeamsEscape(text) });
                    }
                    continue;
                }

                var itemFacts = new List<object>();
                if (detail.Records.Count > 0)
                {
                    /* #3644: a record-shaped detail is one fact per ROW — name "#N", value the summary line
                       over the text(s) in backticks — rather than one fact per attribute (seven per row). The
                       MessageCard fact list is a name/value table, so the flat fields did not interleave here
                       the way Slack's grid did; the compact list is the same reading order in a third of the
                       rows, and the same row numbers a reader sees on Slack and in the email. Fact values
                       render markdown; a MessageCard supports inline code but no fenced block, and the
                       text's own backticks would end the span early, so they are replaced with a straight
                       quote (a T-SQL text never carries one; a PostgreSQL text rarely). */
                    foreach (var record in detail.Records)
                    {
                        var value = new StringBuilder(TeamsEscape(record.Summary));
                        foreach (var (label, text) in record.Texts)
                        {
                            if (value.Length > 0)
                            {
                                value.Append("  \n");
                            }

                            if (record.Texts.Count > 1)
                            {
                                value.Append('_').Append(TeamsEscape(label)).Append("_  \n");
                            }

                            value.Append('`').Append(TeamsCodeSpanText(text)).Append('`');
                        }

                        itemFacts.Add(new { name = string.Create(CultureInfo.InvariantCulture, $"#{record.Ordinal}"), value = value.ToString() });
                    }
                }
                else
                {
                    foreach (var (label, value) in detail.Fields)
                    {
                        itemFacts.Add(new { name = TeamsEscape(label), value = TeamsEscape(value) });
                    }
                }

                itemSections.Add(new
                {
                    activityTitle = TeamsEscape(detail.Heading),
                    facts = itemFacts,
                    markdown = true
                });
            }
        }

        /* #5642: the newest-sample age, once, after the details: the fire-time age persisted with the alert. */
        var ageLine = AlertDetailText.SampleAgeLineFor(context, prose);
        if (ageLine is not null)
        {
            itemSections.Add(new { text = ageLine, markdown = true });
        }

        var title = isTest
            ? $"{emoji} TEST — {metricName}"
            : $"{emoji} {badgeText} — {TeamsEscape(titleName)}";

        var sections = new List<object>
        {
            new
            {
                activityTitle = title,
                activitySubtitle = isTest ? branding.EditionName : $"{branding.EditionName} — {TeamsEscape(serverName)}",
                facts,
                markdown = true
            }
        };
        sections.AddRange(itemSections);

        if (!isTest && branding.SnoozeHint is not null)
        {
            sections.Add(new { text = branding.SnoozeHint });
        }

        var summary = isTest
            ? "[SQL Monitor] Test Notification"
            : $"[SQL Monitor] {badgeText}: {titleName} on {serverName}";

        /* #2710: the OpenUri action carries its schema keys as REAL "@type" (a Dictionary, because a C#
           @-identifier only escapes the keyword — the existing card's `@type` serializes as "type", a
           looseness Teams tolerates on the envelope but potentialAction is stricter about). Two shapes
           rather than a nullable property, because System.Text.Json serializes a null member and a
           "potentialAction": null key is exactly the kind of half-present field connectors choke on. */
        object card = triageUrl is null
            ? new
            {
                @type = "MessageCard",
                @context = "http://schema.org/extensions",
                themeColor,
                summary,
                sections
            }
            : new
            {
                @type = "MessageCard",
                @context = "http://schema.org/extensions",
                themeColor,
                summary,
                sections,
                potentialAction = new object[]
                {
                    new Dictionary<string, object>
                    {
                        ["@type"] = "OpenUri",
                        ["name"] = TriageLink.LinkLabel(triageUrl),
                        ["targets"] = new object[] { new { os = "default", uri = triageUrl } }
                    }
                }
            };

        return JsonSerializer.Serialize(card, s_jsonOptions);
    }
}
