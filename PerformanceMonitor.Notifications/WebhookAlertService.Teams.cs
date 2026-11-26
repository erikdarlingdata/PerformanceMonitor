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
    /// a route says where a family lands, not how the channel type is reached. The outcome is counted against every
    /// health entry that names the destination (<see cref="EntriesNaming"/>).</summary>
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
        TimeSpan postSpacing = default,
        CancellationToken cancellationToken = default)
    {
        var entries = EntriesNaming(settings, NotificationRouter.TeamsChannel, webhookUrl);
        try
        {
            var payload = BuildTeamsPayload(metricName, serverName, currentValue, thresholdValue, _branding, context: context, triageUrl: triageUrl,
                detailText: detailText, displayName: displayName, nowUtc: nowUtc);
            var error = await PostPacedAsync(
                NotificationRouter.TeamsChannel, webhookUrl, webhookUrl, payload, settings.TeamsProxyAddress, headers: null, signing: null, postSpacing, cancellationToken);

            if (error != null)
            {
                RecordChannelFailure(NotificationRouter.TeamsChannel, entries, error);
                return error;
            }

            RecordChannelRecovered(NotificationRouter.TeamsChannel, entries);
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
            RecordChannelException(NotificationRouter.TeamsChannel, entries, ex);
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

    /// <summary>#7202: the most bytes the Details prose of a Teams card may take on the wire, once markdown-escaped and JSON-encoded
    /// (a non-ASCII character or an <c>&amp; &lt; &gt; + '</c> becomes a six-byte <c>\uXXXX</c>). Teams refuses a posted
    /// message past about 28 KB ("Request Entity too large"), so a long flat detail (a list of hundreds of names) gets 20 KB and
    /// the rest of the card keeps room for its facts and sections.</summary>
    internal const int TeamsProseMaxBytes = 20_000;

    /// <summary>
    /// #7202: cuts already-escaped prose to <see cref="TeamsProseMaxBytes"/> at a line break (else a space) and says so. The cut
    /// is made on the escaped text, where a break is never inside an escape (an escape is a backslash and one punctuation
    /// character, or an entity with no space in it) and no backtick span is open (<see cref="TeamsEscape"/> escapes every
    /// backtick). Prose with no break at all inside the ceiling is dropped for the notice alone, rather than cut mid-token.
    /// </summary>
    internal static string CapTeamsProse(string escaped)
    {
        /* The card is serialized with the default JavaScriptEncoder, which writes every non-ASCII character and every & < > + '
           as a six-byte \uXXXX, so the budget is measured on that encoded form, not on raw UTF-8. */
        if (JsonEncodedText.Encode(escaped).EncodedUtf8Bytes.Length <= TeamsProseMaxBytes)
        {
            return escaped;
        }

        const string notice = "\n\n_The rest of this detail is cut to fit the Teams size limit._";
        var budget = TeamsProseMaxBytes - JsonEncodedText.Encode(notice).EncodedUtf8Bytes.Length;
        var bytes = 0;
        var end = 0;
        var cut = -1;
        while (end < escaped.Length)
        {
            var width = char.IsHighSurrogate(escaped[end]) && end + 1 < escaped.Length ? 2 : 1;
            bytes += JsonEncodedText.Encode(escaped.AsSpan(end, width)).EncodedUtf8Bytes.Length;
            if (bytes > budget)
            {
                break;
            }

            if (width == 1 && escaped[end] is '\n' or ' ')
            {
                cut = end;
            }

            end += width;
        }

        return (cut > 0 ? escaped[..cut].TrimEnd() : string.Empty) + notice;
    }

    /// <summary>
    /// An Adaptive Card <c>TextBlock</c>, and a fact's title and value, render markdown, so alert text is placed in them with
    /// the characters that form links, images, emphasis and mentions backslash-escaped: <c>[ ] * `</c>, a <c>_</c> that is not
    /// inside a word (markdown reads <c>query_stats</c> literally), a <c>~</c> in a <c>~~</c> pair, and a backslash that precedes
    /// punctuation (markdown reads that pair as an escape, so a path such as <c>C:\temp</c> is left as it is).
    /// <c>&amp;</c>, <c>&lt;</c> and <c>&gt;</c> become the entities <c>&amp;amp;</c>, <c>&amp;lt;</c> and <c>&amp;gt;</c>
    /// (the ampersand first, so no entity is escaped twice): a markdown reader shows an entity as the character, and
    /// an Adaptive Card takes no HTML.
    /// A <c>TextBlock</c> has no plain-text mode, so the text is escaped. Darling's own formatting (the <c>_label_</c> of a
    /// record) is written outside this call, and a record's text goes in a <c>TextRun</c>, which is not markdown.
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
    /// A line of Teams markdown that opens with a heading marker (<c>#</c> runs), a bullet (<c>-</c> or <c>+</c>), an ordered-list
    /// number (<c>1.</c> or <c>1)</c>) or a rule (three or more <c>-</c> or <c>=</c>) is read as that structure and loses the marker.
    /// A marker counts only when white space or the end of the line follows it, so <c>#1</c>, <c>-5</c>, <c>3.5</c> and <c>==</c> are text.
    /// </summary>
    private static readonly Regex TeamsLineStartMarker = new(
        @"^(?<marker>#+|[-+]|[0-9]+[.)]|-{3,}|={3,})(?=\s|$)",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    /// <summary>
    /// The spaces and tabs a line of Teams markdown opens with. Teams shows a line without them, and a markdown reader that
    /// follows CommonMark reads a line indented four or more columns as a code block, so a line is sent without them.
    /// </summary>
    private static readonly Regex TeamsLineIndent = new(@"^[ \t]+", RegexOptions.Multiline | RegexOptions.CultureInvariant);

    /// <summary>
    /// Backslash-escapes the marker at the start of each line of already-escaped Teams prose, so a line that a source wrote as
    /// <c>- 5 retries left</c> or <c># of failed rows: 3</c> shows with its marker. Every punctuation character of a marker is
    /// escaped (<c>\#\#</c>, <c>\-\-\-</c>), and the digits of a number stay as they are (<c>1\.</c>). A line's indentation is
    /// dropped first (<see cref="TeamsLineIndent"/>), so a marker after it is at the line start. Markdown reads a backslash before ASCII punctuation as an escape, as it does for the escapes in
    /// <see cref="TeamsEscape"/>. A marker in the middle of a line is not at a line start and is left alone.
    /// </summary>
    internal static string TeamsEscapeLineStarts(string escaped)
    {
        if (string.IsNullOrEmpty(escaped))
        {
            return escaped;
        }

        return TeamsLineStartMarker.Replace(TeamsLineIndent.Replace(escaped, string.Empty), match =>
        {
            var marker = match.Groups["marker"].Value;
            var builder = new StringBuilder();
            foreach (var ch in marker)
            {
                if (ch is >= '0' and <= '9')
                {
                    builder.Append(ch);
                }
                else
                {
                    builder.Append('\\').Append(ch);
                }
            }

            return builder.ToString();
        });
    }

    /// <summary>
    /// A record text as the text of a monospace <c>TextRun</c>: each run of line breaks (CR, LF, CR LF, NEL, and the line and
    /// paragraph separators) becomes one space, so the text reads as one wrapped line. A <c>TextRun</c> shows its text as it
    /// is (it is not markdown), so nothing else is changed and nothing is escaped.
    /// </summary>
    internal static string TeamsRunText(string? text) =>
        string.IsNullOrEmpty(text)
            ? string.Empty
            : TeamsLineBreakRun.Replace(text, " ");

    private static readonly Regex TeamsLineBreakRun = new(@"[\r\n\u0085\p{Zl}\p{Zp}]+", RegexOptions.CultureInvariant);

    /// <summary>
    /// The title an alert shows in a header or subject. An alert another service submitted (#5654) is marked
    /// <c>[External: source] title</c>, so it cannot pass as one of Darling's own alerts.
    /// </summary>
    internal static string ExternalTitle(string title, AlertContext? context) =>
        context?.External is { } external
            ? $"[External: {external.Source}] {title}"
            : title;

    /// <summary>The Adaptive Card schema version the Teams card declares: the one Microsoft's own webhook example declares, and
    /// the lowest that has every element the card uses (a styled <c>Container</c> with <c>bleed</c>, a <c>TextBlock</c>, a
    /// <c>FactSet</c>, a <c>RichTextBlock</c> and <c>Action.OpenUrl</c>).</summary>
    internal const string TeamsCardVersion = "1.2";

    /// <summary>The most bytes a Teams card may take on the wire. Microsoft documents that a posted message past about 28 KB
    /// fails with "Request Entity too large".</summary>
    internal const int TeamsPayloadMaxBytes = 28 * 1024;

    /// <summary>
    /// The Adaptive Card container style nearest a severity color: <c>attention</c> for red, <c>warning</c> for amber,
    /// <c>good</c> for green and <c>accent</c> for blue. An Adaptive Card has no free color, so the header band takes the
    /// style whose color is closest to the one the email and Slack show. A color that does not parse reads as blue.
    /// </summary>
    internal static string TeamsCardStyle(string hexColor)
    {
        if (!int.TryParse(hexColor.AsSpan().TrimStart('#'), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var rgb))
        {
            return "accent";
        }

        static int Distance(int a, int b)
        {
            var (red, green, blue) = (((a >> 16) & 0xFF) - ((b >> 16) & 0xFF), ((a >> 8) & 0xFF) - ((b >> 8) & 0xFF), (a & 0xFF) - (b & 0xFF));
            return (red * red) + (green * green) + (blue * blue);
        }

        (int Rgb, string Style)[] anchors = [(0xDC2626, "attention"), (0xD97706, "warning"), (0x16A34A, "good"), (0x2EAEF1, "accent")];
        return anchors.MinBy(anchor => Distance(anchor.Rgb, rgb)).Style;
    }

    /// <summary>A wrapping <c>TextBlock</c>. Its text is markdown, so alert text in it is escaped by the caller.</summary>
    private static Dictionary<string, object> TeamsTextBlock(string text, bool bold = false, bool subtle = false, string? size = null, string? spacing = null)
    {
        var block = new Dictionary<string, object> { ["type"] = "TextBlock", ["text"] = text, ["wrap"] = true };
        if (bold)
        {
            block["weight"] = "Bolder";
        }

        if (subtle)
        {
            block["isSubtle"] = true;
        }

        if (size is not null)
        {
            block["size"] = size;
        }

        if (spacing is not null)
        {
            block["spacing"] = spacing;
        }

        return block;
    }

    /// <summary>How the lines of an external alert's detail are set in the card's <c>Details</c> section.</summary>
    internal enum TeamsDetailLines
    {
        /// <summary>One <c>TextBlock</c> whose lines are joined by a doubled line feed, the break Teams documents for a new line
        /// in a TextBlock outside a list. A TextBlock's markdown shows a lone line feed as a space, and a blank line shows the
        /// same gap as one break.</summary>
        DoubledLineFeed,

        /// <summary>One <c>TextBlock</c> per line, which does not depend on how Teams reads a line feed in markdown. A blank line
        /// sends no block.</summary>
        BlockPerLine,
    }

    /// <summary>The form the card sends. This is the one place that chooses it.</summary>
    internal const TeamsDetailLines TeamsExternalDetailLines = TeamsDetailLines.DoubledLineFeed;

    /// <summary>
    /// The blocks under the <c>Details</c> heading for already-escaped prose, cut by <see cref="CapTeamsProse"/>. An engine
    /// alert's prose is one <c>TextBlock</c> as it is. An external alert's detail has its line-start markers escaped
    /// (<see cref="TeamsEscapeLineStarts"/>), each line is sent without its leading spaces and tabs, and its lines are set in
    /// <paramref name="form"/>. Markdown shows a run of spaces inside a line as one in either form.
    /// </summary>
    internal static List<object> TeamsDetailBlocks(string escapedProse, bool external, TeamsDetailLines form)
    {
        if (!external)
        {
            return [TeamsTextBlock(CapTeamsProse(escapedProse), spacing: "Small")];
        }

        var lines = TeamsEscapeLineStarts(escapedProse);
        if (form == TeamsDetailLines.DoubledLineFeed)
        {
            return [TeamsTextBlock(CapTeamsProse(lines.Replace("\n", "\n\n", StringComparison.Ordinal)), spacing: "Small")];
        }

        var blocks = new List<object>();
        foreach (var line in CapTeamsProse(lines).Split('\n'))
        {
            if (line.Length > 0)
            {
                blocks.Add(TeamsTextBlock(line, spacing: blocks.Count == 0 ? "Small" : "None"));
            }
        }

        return blocks;
    }

    /// <summary>One section of the card: a <c>Container</c> under a separator line.</summary>
    private static object TeamsSection(List<object> items) => new { type = "Container", separator = true, items };

    /// <summary>
    /// Builds the Teams payload: one Adaptive Card in the message envelope a Teams webhook takes
    /// (<c>type: message</c>, and an <c>attachments</c> entry whose <c>contentType</c> is
    /// <c>application/vnd.microsoft.card.adaptive</c>, whose <c>contentUrl</c> is null and whose <c>content</c> is the card).
    /// A Workflows webhook shows a MessageCard's text but not its buttons, and shows an Adaptive Card whole, so every Teams
    /// URL gets this one format.
    /// <para>The card reads top to bottom: a header band in the severity's style (<see cref="TeamsCardStyle"/>) holding the
    /// title and the edition and server line, the lead facts, the <c>Details</c> prose, one section per incident, the
    /// newest-sample age line, and the snooze footer. The card asks for the full message width.</para>
    /// <para>#2710: a non-null <paramref name="triageUrl"/> adds an <c>Action.OpenUrl</c> button pointing at the computed
    /// triage page, the one action kind a webhook card needs and Teams supports there. Null (base URL unset, a test send, or
    /// Lite, which has no triage page) sends a card with no <c>actions</c>.</para>
    /// <para>#3297: <paramref name="detailText"/> is the alert's flat prose detail. It becomes its own <c>Details</c> section
    /// ahead of the per-incident ones, because a multi-paragraph remedy in a label/value fact renders as an unreadable
    /// column.</para>
    /// <para>#3612: this card carries NO analogue of the Slack block budget, on purpose. An Adaptive Card has no
    /// element-count limit, and the one hard ceiling is the payload size (<see cref="TeamsPayloadMaxBytes"/>).
    /// <c>SlackDetailsSizeTests</c> pins the heaviest shape the analysis page can produce (fifteen incidents, 25 items)
    /// against that ceiling, so a producer that widens the page finds out there rather than in a lost delivery.</para>
    /// <para>#7202: that reasoning covers the analysis page only. A flat detail that lists one name per database can run to
    /// hundreds of kilobytes, so the Details prose is cut by <see cref="CapTeamsProse"/> to <see cref="TeamsProseMaxBytes"/>.</para>
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
        /* The title shows the human name when present; ForMetric above stays on the immutable metric
           name (the severity key), and a null/empty display name renders the metric name unchanged. */
        var titleName = ExternalTitle(string.IsNullOrEmpty(displayName) ? metricName : displayName, context);
        /* One instant, with the local rendering DERIVED from it rather than read separately: two reads can
           land either side of a second, which would have the card's own "Time (UTC)" and "Time (Local)"
           facts naming two different seconds of one alert. SpecifyKind because an injected clock may hand
           back an Unspecified DateTime, which ToLocalTime would otherwise treat as already local. */
        var utcNow = nowUtc ?? DateTime.UtcNow;
        var localNow = DateTime.SpecifyKind(utcNow, DateTimeKind.Utc).ToLocalTime();

        var facts = new List<object>();

        if (isTest)
        {
            facts.Add(new { title = "Status", value = "Webhook configuration is working correctly" });
            facts.Add(new { title = "Sent at", value = localNow.ToString("yyyy-MM-dd HH:mm:ss") });
        }
        else
        {
            facts.Add(new { title = "Server", value = TeamsEscape(serverName) });
            var resourceName = DeriveResourceName(context);
            if (resourceName is not null)
                facts.Add(new { title = "Resource", value = TeamsEscape(resourceName) });
            var database = DeriveResourceDatabase(context);
            if (!string.IsNullOrEmpty(database))
                facts.Add(new { title = "Database", value = TeamsEscape(database) });
            facts.Add(new { title = "Current Value", value = TeamsEscape(currentValue) });
            /* #5825: an alert resolution has no threshold, and an empty fact says nothing. */
            if (!string.IsNullOrEmpty(thresholdValue))
                facts.Add(new { title = "Threshold", value = TeamsEscape(thresholdValue) });
            facts.Add(new { title = "Time (UTC)", value = utcNow.ToString("yyyy-MM-dd HH:mm:ss") });
            facts.Add(new { title = "Time (Local)", value = localNow.ToString("yyyy-MM-dd HH:mm:ss") });
        }

        /* #2108: each Fields-carrying detail item becomes its OWN section further down, so a
           multi-incident alert reads as labeled, self-contained units instead of one flat fact
           list where a victim's fields and its fingerprint's fields drift apart. Advice prose and
           remediation-T-SQL items stay folded into the lead facts — they are commentary
           on the whole alert, not incidents. */
        var itemSections = new List<object>();

        /* #3297: ahead of the per-incident sections — it is the actionable half of the alert. */
        if (prose is not null)
        {
            /* #9576: an external alert's detail keeps its line breaks, in the form TeamsExternalDetailLines names. Only the text a
               source sent carries breaks this way: an engine alert's prose is left exactly as it is. Each line starts a paragraph
               of its own, so a line that opens with a heading, list or rule marker keeps the marker as text (#9607). */
            var detailBlocks = new List<object> { TeamsTextBlock("Details", bold: true) };
            detailBlocks.AddRange(TeamsDetailBlocks(TeamsEscape(prose), context?.External is not null, TeamsExternalDetailLines));
            itemSections.Add(TeamsSection(detailBlocks));
        }

        if (context?.Details != null)
        {
            foreach (var detail in context.Details)
            {
                if (detail.IsCodeBlock)
                {
                    /* Remediation T-SQL: point at the email / in-app dialog, never inline it. */
                    facts.Add(new { title = detail.Heading, value = TsqlWebhookHint });
                    continue;
                }

                if (!string.IsNullOrEmpty(detail.Body))
                {
                    /* Advice prose: one "Advice" fact for the headline, then a fact per
                       Investigation/Remediation paragraph (split on the blank line).
                       Body is exclusive with Fields, matching the email and Slack surfaces
                       which skip Fields when Body is present. */
                    facts.Add(new { title = "Advice", value = TeamsEscape(detail.Heading) });
                    foreach (var para in detail.Body.Split("\n\n", StringSplitOptions.RemoveEmptyEntries))
                    {
                        var (label, text) = SplitProseLabel(para);
                        facts.Add(new { title = TeamsEscape(label), value = TeamsEscape(text) });
                    }
                    continue;
                }

                var items = new List<object> { TeamsTextBlock(TeamsEscape(detail.Heading), bold: true) };
                var itemFacts = new List<object>();
                void FlushFacts()
                {
                    if (itemFacts.Count > 0)
                    {
                        items.Add(new { type = "FactSet", spacing = "Small", facts = itemFacts });
                        itemFacts = [];
                    }
                }

                if (detail.Records.Count > 0)
                {
                    /* #3644: a record-shaped detail is one fact per ROW — title "#N", value the summary line — rather
                       than one fact per attribute (seven per row): the same reading order in a third of the rows, and the
                       same row numbers a reader sees on Slack and in the email. A row's text(s) follow its fact, each in
                       a monospace TextRun: Teams' TextBlock markdown has no code span, and a TextRun shows a query text
                       exactly as it is. A row with two or more texts labels each one in italics. */
                    foreach (var record in detail.Records)
                    {
                        itemFacts.Add(new { title = string.Create(CultureInfo.InvariantCulture, $"#{record.Ordinal}"), value = TeamsEscape(record.Summary) });
                        if (record.Texts.Count == 0)
                        {
                            continue;
                        }

                        FlushFacts();
                        foreach (var (label, text) in record.Texts)
                        {
                            if (record.Texts.Count > 1)
                            {
                                items.Add(TeamsTextBlock($"_{TeamsEscape(label)}_", spacing: "Small"));
                            }

                            items.Add(new
                            {
                                type = "RichTextBlock",
                                spacing = "Small",
                                inlines = new object[] { new { type = "TextRun", text = TeamsRunText(text), fontType = "Monospace" } }
                            });
                        }
                    }
                }
                else
                {
                    foreach (var (label, value) in detail.Fields)
                    {
                        itemFacts.Add(new { title = TeamsEscape(label), value = TeamsEscape(value) });
                    }
                }

                FlushFacts();
                itemSections.Add(TeamsSection(items));
            }
        }

        /* #5642: the newest-sample age, once, after the details: the fire-time age persisted with the alert. */
        var ageLine = AlertDetailText.SampleAgeLineFor(context, prose);
        if (ageLine is not null)
        {
            itemSections.Add(TeamsSection([TeamsTextBlock(ageLine)]));
        }

        var title = isTest
            ? $"{emoji} TEST — {metricName}"
            : $"{emoji} {badgeText} — {TeamsEscape(titleName)}";

        var body = new List<object>
        {
            /* The severity color: an Adaptive Card has no accent bar, so the header is a band in the nearest container style. */
            new
            {
                type = "Container",
                style = TeamsCardStyle(hexColor),
                bleed = true,
                items = new object[]
                {
                    TeamsTextBlock(title, bold: true, size: "Large"),
                    TeamsTextBlock(isTest ? branding.EditionName : $"{branding.EditionName} — {TeamsEscape(serverName)}", subtle: true, spacing: "None")
                }
            },
            new { type = "FactSet", facts }
        };
        body.AddRange(itemSections);

        if (!isTest && branding.SnoozeHint is not null)
        {
            body.Add(TeamsSection([TeamsTextBlock(branding.SnoozeHint)]));
        }

        /* A Dictionary, because "$schema" is not a C# identifier, and because "actions" is left out rather than sent
           empty or null when there is no triage link. */
        var card = new Dictionary<string, object>
        {
            ["$schema"] = "http://adaptivecards.io/schemas/adaptive-card.json",
            ["type"] = "AdaptiveCard",
            ["version"] = TeamsCardVersion,
            ["msteams"] = new { width = "Full" },
            ["body"] = body
        };
        if (triageUrl is not null)
        {
            card["actions"] = new object[] { new { type = "Action.OpenUrl", title = TriageLink.LinkLabel(triageUrl), url = triageUrl } };
        }

        /* The envelope is exactly what the webhook's schema names: type, and attachments of contentType, contentUrl (always
           null) and content. */
        var message = new
        {
            type = "message",
            attachments = new object[]
            {
                new { contentType = "application/vnd.microsoft.card.adaptive", contentUrl = (string?)null, content = card }
            }
        };

        return JsonSerializer.Serialize(message, s_jsonOptions);
    }
}
