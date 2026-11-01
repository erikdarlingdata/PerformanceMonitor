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
    /// <summary>Posts to Slack. Null when the post succeeded, the error text when it did not — see
    /// <see cref="TrySendTeamsAlertAsync"/>, including for the routed <paramref name="webhookUrl"/>.</summary>
    private async Task<string?> TrySendSlackAlertAsync(
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
            var payload = BuildSlackPayload(metricName, serverName, currentValue, thresholdValue, _branding, context: context, triageUrl: triageUrl,
                detailText: detailText, displayName: displayName, nowUtc: nowUtc);
            var error = await PostWebhookAsync(webhookUrl, payload, settings.SlackProxyAddress, cancellationToken: cancellationToken, bodyLog: _logger);

            if (error != null)
            {
                _consecutiveSlackFailures++;
                _lastSlackError = error;

                if (_consecutiveSlackFailures <= FailingChannelThreshold)
                    _logger.LogError($"SLACK WEBHOOK FAILED ({_consecutiveSlackFailures}x): {error}");
                else if (_consecutiveSlackFailures % 50 == 0)
                    _logger.LogError($"SLACK WEBHOOK STILL FAILING: {_consecutiveSlackFailures} failures. Last: {error}");

                return error;
            }

            if (_consecutiveSlackFailures > 0)
                _logger.LogInformation($"Slack webhook recovered after {_consecutiveSlackFailures} failure(s)");

            _consecutiveSlackFailures = 0;
            _lastSlackError = null;
            _logger.LogInformation($"Slack webhook sent for {metricName} on {serverName}");
            return null;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* #4752: the caller's cancel passes through, as in the Teams send. */
            throw;
        }
        catch (Exception ex)
        {
            _consecutiveSlackFailures++;
            _lastSlackError = ex.Message;
            _logger.LogError($"Slack webhook error: {ex.Message}");
            return ex.Message;
        }
    }

    /// <summary>
    /// Slack's documented ceiling on the <c>fields</c> array of one <c>section</c> block. A section over it
    /// is rejected as invalid_blocks, which fails the WHOLE message rather than degrading it — so a body
    /// that grows past it loses the alert entirely, silently from the reader's side.
    /// </summary>
    private const int SlackSectionFieldLimit = 10;

    /// <summary>
    /// Appends <paramref name="fields"/> as however many <c>section</c> blocks it takes to keep each one
    /// inside <see cref="SlackSectionFieldLimit"/>. Consecutive sections carry no divider between them, so
    /// a split reads as one continued block.
    ///
    /// <para>The field count per item is not bounded by anything upstream: an incident item already emits
    /// its forensic detail, its dedup metadata, occurrence counts and an incident start, and #3442's
    /// per-party deadlock facts add up to five more. Splitting here rather than capping per producer means
    /// no producer has to know what every other producer contributed to the same item.</para>
    ///
    /// <para>An empty list appends nothing: a section with neither <c>text</c> nor a non-empty
    /// <c>fields</c> is itself invalid.</para>
    /// </summary>
    private static void AddSlackFieldSections(List<object> blocks, List<object> fields)
    {
        for (var i = 0; i < fields.Count; i += SlackSectionFieldLimit)
        {
            var take = Math.Min(SlackSectionFieldLimit, fields.Count - i);
            blocks.Add(new { type = "section", fields = fields.GetRange(i, take) });
        }
    }

    /// <summary>
    /// Slack's documented ceiling on ONE text object's characters — per text object, not per message, so a
    /// section block's mrkdwn text is bound by it however few blocks the message carries. Exceeding it
    /// rejects the WHOLE message (HTTP 400, spelled <c>invalid_attachments</c> when the blocks ride inside
    /// the colored attachment), not just the oversized block. Measured live (#3493), on the first two
    /// nights of digest delivery: both big-fleet stores' 20-mover Collector Cost Digests failed exactly
    /// this way while the small store's 1-mover digest delivered — and one store's Fleet Sweep Rollup
    /// delivered through the SAME webhook 80 milliseconds after its digest failed, so the webhook was
    /// never the suspect. The single <c>*Details*</c> section was over this ceiling.
    /// </summary>
    internal const int SlackTextObjectLimit = 3000;

    /// <summary>
    /// Slack's documented ceiling on blocks per message. A fifty-first block fails the delivery as surely
    /// as an oversized text object does, and it fails it with the SAME error string — which is how #3612
    /// hid behind #3493 for two builds. Both splitters spend only what the payload's other blocks leave
    /// over: the prose splitter (<see cref="AddSlackProseSections"/>) since #3493, the structured details
    /// (<see cref="AddSlackDetailBlocks"/>) since #3612.
    ///
    /// <para>Measured live (#3612): three analysis pages — the product's highest-severity compound
    /// findings, five-fact stories rooted in a plan regression — died with <c>invalid_attachments</c>
    /// across two builds while a three-fact page delivered through the same webhook the same hour. The
    /// monitoring seat's read of the persisted <c>context_json</c> named the separator: every text
    /// object on the failed pages was under its cap (their longest body was 2,122 characters, and a
    /// DELIVERED page carried a 2,835-character one), and the failed pages carried 19–21 detail items against
    /// the largest delivered page's 18. At roughly two blocks per item plus the fixed head and tail,
    /// that is the fifty-block line. The fixture in <c>SlackDetailsSizeTests</c> reproduces the shape
    /// from source: ten items the story's drill-downs produce (33 blocks) plus one two-block incident
    /// item per distinct query hash, so seven or more incidents cross the limit.</para>
    /// </summary>
    internal const int SlackMessageBlockLimit = 50;

    /// <summary>
    /// Slack's documented ceiling on ONE text object inside a section's <c>fields</c> — tighter than the
    /// <see cref="SlackTextObjectLimit"/> a section's own text gets. Applied to every detail field
    /// (<see cref="SlackFieldText"/>) as the field-side half of the #3612 discipline. No observed page
    /// has approached it (the analysis formatter truncates drill-down values at 300 characters and the
    /// longest measured field text is 318), so this is hygiene rather than the fix; the fix is the block
    /// budget.
    /// </summary>
    internal const int SlackFieldTextLimit = 2000;

    /// <summary>
    /// The most of a detail's heading a Slack section carries. Headings are producer literals and
    /// humanized drill-down keys — the longest observed is an advice headline under 100 characters — so
    /// this never fires on a real page; it exists because the body splitter sizes its per-section
    /// capacity as the text-object ceiling minus the heading's width, and an unbounded heading would
    /// leave it no capacity at all.
    /// </summary>
    private const int SlackDetailHeadingLimit = 500;

    /// <summary>Slack's cap on a <c>header</c> block's text, in characters.</summary>
    internal const int SlackHeaderTextLimit = 150;

    /// <summary>
    /// The most one record of a record-shaped detail (<see cref="AlertDetailItem.Records"/>, #3644) may
    /// occupy in a section's text: its bold summary line plus its code-blocked text(s), markup included.
    /// Sized so that a record ALWAYS fits a section beside the detail's heading — the heading is bounded at
    /// <see cref="SlackDetailHeadingLimit"/> and wrapped in <c>*…*\n</c> (four more units) — which is
    /// what lets <see cref="AddSlackRecordSections"/> pack records greedily and open a fresh section on the
    /// first one that does not fit, with no record ever cut between sections. No producer comes near it:
    /// the widest drill-down row (Regressed Queries, twelve scalars and a 500-character text) is under
    /// 900. A text that would push a record past it is cut with the count stated, through
    /// <see cref="SlackBoundedText"/> — the same note, in the same characters, as a cut field.
    /// </summary>
    private const int SlackRecordLimit = SlackTextObjectLimit - SlackDetailHeadingLimit - 4;

    /// <summary>The most of a record's summary line a Slack section carries before the texts take the rest
    /// of the record's room. The producer packs a row's scalars into it — twelve labelled numbers is under
    /// 400 — so this never fires on a real page; it exists so the texts' share below is always positive.</summary>
    private const int SlackRecordSummaryLimit = 1000;

    /// <summary>
    /// What the stated-omission item for dropped details costs: its own divider, so it reads as the
    /// final item rather than as a continuation of whichever detail happened to be last, and one section.
    /// </summary>
    private const int SlackDetailOmissionCost = 2;

    /// <summary>How much of the omitted details' heading list the stated-omission item quotes before
    /// cutting it, so a message with dozens of dropped items cannot blow the omission section past the
    /// text-object ceiling it exists to respect.</summary>
    private const int SlackOmittedHeadingsLimit = 1000;

    /// <summary>
    /// Where every stated omission points: the full document has always lived in the alert row (the
    /// prose as <c>detail_text</c>, the details as <c>context_json</c>) and in the email body, the way
    /// <see cref="TsqlWebhookHint"/> already points there. One string, so the three omission sites
    /// (prose lines, dropped details, truncated fields) cannot drift into three different directions.
    /// </summary>
    private const string SlackOmissionPointer = " - see email or in-app Alert Details for the full text.";

    /// <summary>The prose sections' leading header — on the FIRST section only; see
    /// <see cref="AddSlackProseSections"/> for why continuations carry nothing.</summary>
    private const string SlackProseHeader = "*Details*\n";

    /// <summary>Marks the continuation halves of a single line hard-split by
    /// <see cref="SplitProseIntoSectionSafeLines"/>, so the reader sees one line that was cut rather
    /// than two that were written.</summary>
    private const string SlackProseContinuationMarker = "(cont.) ";

    /// <summary>How much of the first omitted line the stated-omission line quotes. Enough to identify a
    /// digest mover (collector, server and the headline figures all sit in the line's first stretch);
    /// bounded so a pathological line cannot blow the omission line past the very ceiling it exists to
    /// respect.</summary>
    private const int SlackOmissionFragmentLimit = 120;

    /// <summary>
    /// The room the omission-path packing holds back in its final block for the stated-omission line:
    /// the line's fixed text (~93 chars), a 13-digit separator-grouped line count, and the quoted
    /// fragment at its cap plus its own truncation ellipsis — 231 worst case, rounded up.
    /// </summary>
    private const int SlackOmissionReserve = 256;

    /// <summary>
    /// Appends <paramref name="prose"/> as however many mrkdwn <c>section</c> blocks it takes to keep
    /// every text object inside <see cref="SlackTextObjectLimit"/> — the
    /// <see cref="AddSlackFieldSections"/> precedent one level up (#3493). The split lands on LINE
    /// boundaries, because the long-prose producers are line-oriented documents (the Collector Cost
    /// Digest is one mover per line) and a line cut mid-thought misstates a figure. Consecutive sections
    /// carry no divider between them, so a split reads as one continued document; only the first section
    /// leads with <paramref name="header"/>, and continuations carry nothing — a repeated header would
    /// read as several detail sections rather than one that continued.
    ///
    /// <para><b>One splitter, two callers (#3612).</b> The header is a parameter because the alert's
    /// flat prose (<see cref="SlackProseHeader"/>) and a structured detail's <see cref="AlertDetailItem.Body"/>
    /// (<c>*Heading*</c>) are the same thing to Slack — one mrkdwn section under one ceiling — and #3493
    /// covered only the first. The analysis pages carry their whole substance as details and deliver no
    /// prose at all, so a second, detail-side splitter would have been a second contract to keep in step;
    /// instead <see cref="AddSlackDetailBlocks"/> routes every body through this one, and the omission
    /// wording, the continuation marker and the packing arithmetic are shared by construction.</para>
    ///
    /// <para><b>The block budget, and what yields to it.</b> <paramref name="blockBudget"/> is what the
    /// payload's other blocks leave under <see cref="SlackMessageBlockLimit"/>, and the PROSE is what
    /// degrades when it cannot fit — never the structured details. The precedence is decided by what the
    /// real payload shapes carry: the producers with long prose (the digest, the self-alerts) fire with
    /// no structured context at all, and the alerts with heavy per-incident details carry prose that is
    /// short or suppressed as redundant (<see cref="AlertDetailText.ProseForDelivery"/>), so the yielding
    /// branch never costs a real payload both halves at once. Since #3612 the details are themselves
    /// bounded, and they reserve one block for the prose when there is one, so every caller hands in a
    /// budget of at least one — and the packer floors it at one locally as well, so the invariant the
    /// repack loop depends on (it terminates by reaching its final block) does not live only in three
    /// call sites' subtraction (review note on #3618).</para>
    ///
    /// <para><b>Degrading is stated, never silent.</b> A prose the budget cannot hold keeps as many whole
    /// lines as fit and ends with one omission line naming HOW MANY lines were dropped and quoting the
    /// first of them — the producers rank their lines most-significant-first (the digest orders movers by
    /// magnitude), so the first dropped line is the headline of what the reader is not seeing, and an
    /// omission note that is itself vague would recreate the silent-truncation problem one level up. The
    /// full text has always lived in the alert row and the email body, so the omission line points there
    /// (<see cref="SlackOmissionPointer"/>) the way <see cref="TsqlWebhookHint"/> already does.</para>
    ///
    /// <para>A budget of one matches the pre-#3493 block cost exactly (the old single section also cost
    /// one block), so a payload whose OTHER blocks already crowd the message limit is no worse off than
    /// it ever was — the prose does not decide that verdict.</para>
    /// </summary>
    private static void AddSlackProseSections(List<object> blocks, string header, string prose, int blockBudget)
    {
        /* Uniform per-section line capacity, sized to the first section (the only one carrying the
           header): continuations run a header's width under the ceiling, which keeps the packing
           single-pass. The header is bounded by its producers (SlackDetailHeadingLimit for a detail's
           heading), so the capacity is always comfortably positive. */
        var capacity = SlackTextObjectLimit - header.Length;

        /* The one-section fast path IS the pre-#3493 rendering, byte for byte — 1-mover digests and
           every ordinary alert take it, and so does every detail body an analysis page has ever carried
           (the longest measured is 2,835 characters), so their payloads do not change shape at all. */
        if (prose.Length <= capacity)
        {
            blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = header + prose } });
            return;
        }

        /* The floor is local on purpose: PackProseLines' repack loop terminates by reaching its final
           block, which a budget of zero would never present, and a budget below one cannot render any
           section anyway. Every caller already hands in at least one; this keeps that true if one of
           them is edited. */
        var lines = SplitProseIntoSectionSafeLines(prose, capacity);
        var texts = PackProseLines(lines, capacity, Math.Max(1, blockBudget));

        for (var i = 0; i < texts.Count; i++)
        {
            var text = i == 0 ? header + texts[i] : texts[i];
            blocks.Add(new { type = "section", text = new { type = "mrkdwn", text } });
        }
    }

    /// <summary>
    /// The prose, as lines that each fit a section on their own. A single line longer than
    /// <paramref name="capacity"/> — pathological, but a delivery that fails over it would be this bug
    /// again — hard-splits at character boundaries (whole characters, through <see cref="SlackCutLength"/>,
    /// since #3622), every continuation piece marked with <see cref="SlackProseContinuationMarker"/>.
    /// </summary>
    private static List<string> SplitProseIntoSectionSafeLines(string prose, int capacity)
    {
        var lines = new List<string>();
        foreach (var raw in prose.Split('\n'))
        {
            if (raw.Length <= capacity)
            {
                lines.Add(raw);
                continue;
            }

            var start = 0;
            while (start < raw.Length)
            {
                var prefix = start == 0 ? string.Empty : SlackProseContinuationMarker;
                /* #3622: the piece ends on a character boundary, never between the halves of a surrogate
                   pair or through a combining sequence — the index arithmetic alone put an emoji's two
                   halves in two sections, and the reader saw two replacement glyphs and no emoji. The
                   loop always advances: the capacity is in the thousands and the marker is eight, so the
                   cut can never back off to nothing. */
                var take = SlackCutLength(raw.AsSpan(start), capacity - prefix.Length);
                lines.Add(prefix + raw.Substring(start, take));
                start += take;
            }
        }

        return lines;
    }

    /// <summary>
    /// Packs <paramref name="lines"/> greedily into section texts of at most <paramref name="capacity"/>
    /// characters. When the packing fits <paramref name="blockBudget"/> that is the answer; when it does
    /// not, the lines are repacked into exactly the budget with the final section reserving
    /// <see cref="SlackOmissionReserve"/> for the stated-omission line, which then closes the document.
    /// </summary>
    private static List<string> PackProseLines(List<string> lines, int capacity, int blockBudget)
    {
        var texts = new List<string>();
        var sb = new StringBuilder();
        foreach (var line in lines)
        {
            if (sb.Length > 0 && sb.Length + 1 + line.Length > capacity)
            {
                texts.Add(sb.ToString());
                sb.Clear();
            }

            if (sb.Length > 0)
            {
                sb.Append('\n');
            }

            sb.Append(line);
        }

        if (sb.Length > 0)
        {
            texts.Add(sb.ToString());
        }

        if (texts.Count <= blockBudget)
        {
            return texts;
        }

        /* Over budget: repack into exactly blockBudget sections, the last one holding room back for the
           omission line. Blocks before the last pack identically to the pass above, so this can never
           fit MORE lines than the pass that already overflowed — the omission line is always earned. */
        texts.Clear();
        sb.Clear();
        var placed = 0;
        while (placed < lines.Count)
        {
            var line = lines[placed];
            var finalBlock = texts.Count == blockBudget - 1;
            var reserve = finalBlock ? SlackOmissionReserve + 1 : 0;
            var joiner = sb.Length > 0 ? 1 : 0;

            if (sb.Length + joiner + line.Length + reserve > capacity)
            {
                if (finalBlock)
                {
                    break;
                }

                texts.Add(sb.ToString());
                sb.Clear();
                continue;
            }

            if (joiner == 1)
            {
                sb.Append('\n');
            }

            sb.Append(line);
            placed++;
        }

        var dropped = lines.Count - placed;
        var firstDropped = lines[placed];
        var fragment = firstDropped.Length <= SlackOmissionFragmentLimit
            ? firstDropped
            : firstDropped[..SlackCutLength(firstDropped, SlackOmissionFragmentLimit)] + "...";
        var noun = dropped == 1 ? "line" : "lines";
        var omission = string.Create(CultureInfo.InvariantCulture,
            $"... and {dropped:N0} more {noun}, first omitted: \"{fragment}\"{SlackOmissionPointer}");

        if (sb.Length > 0)
        {
            sb.Append('\n');
        }

        sb.Append(omission);
        texts.Add(sb.ToString());
        return texts;
    }

    /// <summary>
    /// Appends <paramref name="details"/> as divider-led block runs — the rendering every structured
    /// detail has always had — inside <paramref name="blockBudget"/>, dropping WHOLE details from the
    /// end and stating what was dropped when they cannot all fit. The detail-side half of the #3493
    /// discipline, added by #3612 after three analysis pages were lost to it.
    ///
    /// <para><b>Why the block total is the fix, and why it is a budget on blocks rather than on facts or
    /// details.</b> An analysis page carries its whole finding as details: a Diagnosis item, the advice,
    /// the remediation T-SQL pointer, one item per drill-down the story's fact keys attach (a five-fact
    /// plan-regression story attaches seven, and three of them are 30–40 fields wide, so four or five
    /// blocks each), and one incident item per distinct query hash the drill-downs surfaced (up to fifteen).
    /// Measured from source for that story: two head blocks, 33 blocks for the ten fixed items, two
    /// blocks per incident, and two footer blocks — 37 + 2×incidents — so the seventh incident is the
    /// fifty-first block. The count that matters is blocks: a four-fact story with wider drill-downs can
    /// reach the line with fewer items, and the SAME 18 items can land either side of it depending on how
    /// many fields each carries, which is why the budget is computed from the rendered runs and not from
    /// any count upstream of them.</para>
    ///
    /// <para><b>What drops first.</b> Producers order their details most-essential-first — the analysis
    /// path emits Diagnosis, then Advice, then the T-SQL pointer, then the drill-downs, then the incident
    /// fingerprints; the engine alerts emit the finding's own items before the per-incident ones — so
    /// keeping the longest PREFIX that fits keeps the finding and drops the drill-down's tail. A body
    /// detail that reaches the boundary is not dropped whole: it takes the blocks that remain and states
    /// its own line omission, because the advice is the finding and a shortened advice beats a missing
    /// one. No detail is skipped to make room for a smaller one behind it; a reader who sees
    /// "Incident 6 of 11" and then the omission item knows exactly where the message stopped. The
    /// omission item's two blocks are held back only while something can still be dropped: the LAST
    /// detail is offered the whole remainder, so a page whose only degradation is a shortened body
    /// carries no omission item — the body's own line omission is the whole truth of it — and no item is
    /// dropped to make room for an announcement that costs what the item did.</para>
    ///
    /// <para><b>The omission is stated, and it names the items.</b> The final run is a divider and one
    /// section naming how many details were dropped and listing their headings — the headings ARE the
    /// identity of a detail ("Regressed Queries", "Incident 7 of 11"), so a reader knows what to open
    /// the alert for — and pointing at the surfaces that carry the whole document
    /// (<see cref="SlackOmissionPointer"/>): the alert row's <c>context_json</c> renders in full in-app
    /// and in the email, neither of which has a block limit. Its cost
    /// (<see cref="SlackDetailOmissionCost"/>) is charged against the budget before any detail is
    /// kept, so the omission item is always affordable when it is needed.</para>
    ///
    /// <para><b>A page that fits is byte-identical.</b> When every run fits the budget the runs are
    /// appended exactly as the pre-#3612 loop appended them — the same anonymous shapes in the same
    /// order — so every engine alert and every analysis page that delivered before this change
    /// serializes to the same bytes after it. The text-object caps the runs apply
    /// (<see cref="RenderSlackDetail"/>) only change a payload that would have been rejected.</para>
    /// </summary>
    private static void AddSlackDetailBlocks(List<object> blocks, List<AlertDetailItem> details, int blockBudget)
    {
        if (details.Count == 0)
        {
            return;
        }

        /* First pass: every detail rendered whole, each body free to take as many sections as it needs.
           This is the answer whenever it fits, and it is the pre-#3612 rendering. */
        var runs = new List<List<object>>(details.Count);
        var total = 0;
        foreach (var detail in details)
        {
            var run = RenderSlackDetail(detail, bodyBlockBudget: int.MaxValue);
            runs.Add(run);
            total += run.Count;
        }

        if (total <= blockBudget)
        {
            foreach (var run in runs)
            {
                blocks.AddRange(run);
            }

            return;
        }

        /* Over budget: keep the longest prefix whose runs fit beside the omission item. A body detail at
           the boundary is re-rendered into whatever remains rather than dropped — see the doc block. The
           omission item's cost is bounded by the budget itself so a degenerate budget (one block) still
           states the omission rather than overflowing; the real budgets are in the forties. A budget of
           zero or less leaves no block to state anything in, so nothing is appended — the only honest
           rendering of a budget that cannot exist: the head is two blocks and the footer at most two, so
           the caller hands in forty-five or more, and a change to that composition is what this comment
           is for. */
        var omissionCost = Math.Min(SlackDetailOmissionCost, Math.Max(0, blockBudget));
        var spent = 0;
        var kept = 0;
        while (kept < details.Count)
        {
            /* The omission item is only owed when something AFTER this detail is dropped, so the last
               detail is offered the whole remainder, reserve included: a last body shrinks into two more
               blocks, and a last small item that fits in the reclaimed two is kept instead of being
               replaced by an omission item of the same cost that would announce it (review catch on
               #3618). For every other detail the reserve stands, and a body shrunk into it fills the
               remainder exactly, so the detail after it necessarily drops and the item is earned. */
            var last = kept == details.Count - 1;
            var remaining = blockBudget - spent - (last ? 0 : omissionCost);
            var run = runs[kept];
            if (run.Count > remaining)
            {
                var detail = details[kept];
                /* One divider plus at least one section is the least a body detail can cost. */
                if (string.IsNullOrEmpty(detail.Body) || detail.IsCodeBlock || remaining < 2)
                {
                    break;
                }

                run = RenderSlackDetail(detail, bodyBlockBudget: remaining - 1);
                if (run.Count > remaining)
                {
                    break;
                }
            }

            blocks.AddRange(run);
            spent += run.Count;
            kept++;
        }

        /* Nothing dropped — the boundary body's own line omission already states its cut — or no block
           to state a drop in: either way an omission item here would be a false statement. */
        var dropped = details.Count - kept;
        if (dropped == 0 || omissionCost == 0)
        {
            return;
        }

        var list = new StringBuilder();
        for (var i = kept; i < details.Count; i++)
        {
            if (list.Length > 0)
            {
                list.Append("; ");
            }

            list.Append(details[i].Heading);
        }

        var headings = SlackEscapedBounded(list.ToString(), SlackOmittedHeadingsLimit);

        var noun = dropped == 1 ? "detail" : "details";
        var omission = string.Create(CultureInfo.InvariantCulture,
            $"*Omitted from this message*\n{dropped:N0} more {noun} did not fit Slack's {SlackMessageBlockLimit}-block message limit: {headings}{SlackOmissionPointer}");

        if (omissionCost == SlackDetailOmissionCost)
        {
            blocks.Add(new { type = "divider" });
        }

        blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = omission } });
    }

    /// <summary>
    /// One detail's block run: its leading divider, then the shape its kind has always rendered — a
    /// fixed pointer for remediation T-SQL (never inlined on a chat surface), a <c>*Heading*</c>-led
    /// mrkdwn section for a body, record sections for a record-shaped detail (#3644, below), or field
    /// sections (heading first, then <c>*label:*</c> fields) for everything else. #3612 bounds each text
    /// object in the run: the body goes through the prose splitter (<see cref="AddSlackProseSections"/>)
    /// with the heading as its header and <paramref name="bodyBlockBudget"/> as its block budget, and every
    /// field through <see cref="SlackFieldText"/>. A detail inside every cap renders the pre-#3612 bytes.
    /// </summary>
    private static List<object> RenderSlackDetail(AlertDetailItem detail, int bodyBlockBudget)
    {
        var run = new List<object> { new { type = "divider" } };
        var heading = SlackEscapedHeading(detail.Heading);

        if (detail.IsCodeBlock)
        {
            /* Remediation T-SQL: point at the email / in-app dialog, never inline it. */
            run.Add(new { type = "section", text = new { type = "mrkdwn", text = $"*{heading}*\n{TsqlWebhookHint}" } });
            return run;
        }

        if (!string.IsNullOrEmpty(detail.Body))
        {
            /* Advice prose flows as mrkdwn sections; the synthesized Body is
               "Investigation: ...\n\nRemediation: ..." which Slack renders verbatim. One section when it
               fits the ceiling (every observed body does); split on line boundaries when it does not. */
            AddSlackProseSections(run, $"*{heading}*\n", SlackEscape(detail.Body), bodyBlockBudget);
            return run;
        }

        if (detail.Records.Count > 0)
        {
            /* #3644: a repeating record is a list of rows, not a grid of pairs. The fields this detail also
               carries are the same content, and they are what every other surface renders. */
            AddSlackRecordSections(run, heading, detail.Records);
            return run;
        }

        var detailFields = new List<object>();
        detailFields.Add(new { type = "mrkdwn", text = $"*{heading}*" });

        foreach (var (label, value) in detail.Fields)
        {
            detailFields.Add(new { type = "mrkdwn", text = SlackFieldText(label, value) });
        }

        AddSlackFieldSections(run, detailFields);
        return run;
    }

    /// <summary>
    /// A record-shaped detail (#3644) as mrkdwn sections: <c>*Heading*</c> leads the first, and each record
    /// is one visual unit under it — a bold line <c>*#N · Database: … · Total Cpu Ms: 3,088,689 · …*</c>
    /// over its text in a triple-backtick code block — stacked top to bottom. Read live on a production High
    /// CPU page: the same rows as seven <c>fields</c> per query put query #1's text beside query #2's hash and
    /// #3's database beside #2's SQL, because Slack fills a section's <c>fields</c> two across in submission
    /// order; a section's <c>text</c> has one column, so a record cannot be pulled apart by its neighbour,
    /// and a long text dislocates nothing but itself. The code block also takes the SQL OUT of mrkdwn
    /// interpretation, which a field never did (<c>&gt;</c> at a line start is a quote, <c>*</c> pairs
    /// bold).
    ///
    /// <para><b>Block cost: the divider plus as many sections as the records pack into under the
    /// text-object ceiling — ONE for every drill-down the producer emits.</b> #3644 sketched one section per
    /// record (three records, three sections); that costs MORE than the fields did for a narrow record —
    /// three three-attribute rows are ten fields, one section, two blocks, and would become four — and the
    /// #3612 budget is measured in blocks, so a layout fix that widened the card would push incidents off
    /// the page to buy readability. Packing does not: records are appended to the current section while
    /// the section stays inside <see cref="SlackTextObjectLimit"/>, and a record that would cross it opens
    /// the next section (no heading on a continuation — a repeated heading would read as a second detail),
    /// so the cost is <c>1 + ceil(records ÷ what fits)</c>, at most one section per record and never more
    /// than the fields cost. Each record is bounded by <see cref="SlackRecordLimit"/>, which is what
    /// guarantees any record fits a section with the heading; the fixture's six record-shaped drill-downs
    /// (3 rows each, texts at the collector's 500) measure 474 to 2,614 characters and one section apiece,
    /// so the five-fact page's fixed items fall from 33 blocks to 20 and the message from 37 + 2×incidents
    /// to 24 + 2×incidents — thirteen incidents fit where six did, and the three production pages #3612
    /// could deliver only by dropping incidents now deliver whole (<c>SlackDetailsSizeTests</c>). Visually
    /// one section holding three records and three sections holding one each are the same stack; Slack
    /// puts no rule between consecutive sections.</para>
    ///
    /// <para><b>Inside a record.</b> The summary is bounded at <see cref="SlackRecordSummaryLimit"/> with a
    /// trailing ellipsis (it never fires; see the constant), then the texts share what is left of the
    /// record's room equally, each through <see cref="SlackBoundedText"/> so a cut states its count in
    /// characters the reader can count, on a whole character (#3622). One text carries no label — the
    /// heading says what the drill-down is and the summary says whose row this is; "Query Text" would add
    /// nothing — and two or more (a blocking chain's blocked and blocking SQL) are each led by their label
    /// in italics so the reader can tell them apart.</para>
    /// </summary>
    private static void AddSlackRecordSections(List<object> run, string heading, List<AlertDetailRecord> records)
    {
        var section = new StringBuilder("*").Append(heading).Append('*');

        void Flush()
        {
            run.Add(new { type = "section", text = new { type = "mrkdwn", text = section.ToString() } });
            section.Clear();
        }

        foreach (var record in records)
        {
            var unit = SlackRecordText(record);
            if (section.Length + 1 + unit.Length > SlackTextObjectLimit)
            {
                Flush();
                section.Append(unit);
            }
            else
            {
                section.Append('\n').Append(unit);
            }
        }

        Flush();
    }

    /// <summary>One record's unit of text — see <see cref="AddSlackRecordSections"/> — inside
    /// <see cref="SlackRecordLimit"/>.</summary>
    private static string SlackRecordText(AlertDetailRecord record)
    {
        var summary = SlackEscapedBounded(record.Summary, SlackRecordSummaryLimit);
        var unit = new StringBuilder();
        unit.Append("*#").Append(record.Ordinal.ToString(CultureInfo.InvariantCulture));
        if (summary.Length > 0)
        {
            unit.Append(" · ").Append(summary);
        }

        unit.Append('*');

        if (record.Texts.Count == 0)
        {
            return unit.ToString();
        }

        /* Each text's share of the record's remaining room, after its own markup: the code fence
           ("\n```\n" + "\n```", nine units) and, when there are several, the italic label line. */
        var labelled = record.Texts.Count > 1;
        var markup = 0;
        foreach (var (label, _) in record.Texts)
        {
            markup += 9 + (labelled ? label.Length + 3 : 0);
        }

        var share = Math.Max(0, (SlackRecordLimit - unit.Length - markup) / record.Texts.Count);
        foreach (var (label, text) in record.Texts)
        {
            if (labelled)
            {
                unit.Append("\n_").Append(SlackEscape(label)).Append('_');
            }

            unit.Append("\n```\n").Append(SlackBoundedText(string.Empty, text, share)).Append("\n```");
        }

        return unit.ToString();
    }

    /// <summary>A detail heading bounded to <see cref="SlackDetailHeadingLimit"/>; unchanged for every
    /// heading a producer has ever emitted. The cut lands on a whole character (#3622): a heading IS the
    /// detail's identity, and one ending in half an emoji or a letter shorn of its accent names a
    /// different thing.</summary>
    private static string SlackHeading(string heading) =>
        heading.Length <= SlackDetailHeadingLimit ? heading : heading[..SlackCutLength(heading, SlackDetailHeadingLimit)] + "...";

    /// <summary>
    /// A heading as mrkdwn: cut as raw text (<see cref="SlackHeading"/>), then escaped, then held to what a
    /// field's <c>*heading*</c> may carry (<see cref="SlackFieldTextLimit"/> less the two asterisks) at the last
    /// whole entity, because a heading made of <c>&amp;</c> grows fivefold when it is escaped.
    /// </summary>
    private static string SlackEscapedHeading(string heading) =>
        SlackEscapedBounded(SlackHeading(heading), SlackFieldTextLimit - 2);

    /// <summary>
    /// <paramref name="raw"/> escaped for mrkdwn and inside <paramref name="limit"/> characters of escaped text.
    /// The text is cut BEFORE it is escaped, on a whole character, so a cut never lands inside an entity; one that
    /// still has to be cut ends in <c>...</c>.
    /// </summary>
    private static string SlackEscapedBounded(string? raw, int limit)
    {
        if (string.IsNullOrEmpty(raw))
        {
            return string.Empty;
        }

        var escaped = SlackEscape(raw);
        return escaped.Length <= limit
            ? escaped
            : SlackEscape(raw[..SlackEscapedFit(raw, limit)]) + "...";
    }

    /// <summary>
    /// The length of the longest leading stretch of <paramref name="raw"/>, ending on a whole character, whose
    /// escaped form (<see cref="SlackEscape"/>) is at most <paramref name="limit"/> characters.
    /// </summary>
    private static int SlackEscapedFit(string raw, int limit)
    {
        var cost = 0;
        var fit = 0;
        while (fit < raw.Length)
        {
            var next = raw[fit] switch { '&' => 5, '<' or '>' => 4, _ => 1 };
            if (cost + next > limit)
            {
                break;
            }

            cost += next;
            fit++;
        }

        return SlackCutLength(raw, fit);
    }

    /// <summary>
    /// One detail field's text object, <c>*label:*</c> over its value, inside
    /// <see cref="SlackFieldTextLimit"/>. A value too long for the field keeps its leading stretch and
    /// states the cut inline — how many characters were omitted and where the whole value lives — the
    /// same posture as the prose and detail omissions, in the field's own text because a field has no
    /// room for a closing line. Two passes size the kept stretch: the first against the longest count
    /// the note could carry, the second against the count it actually carries, so the result never
    /// exceeds the limit. Every field inside the limit renders the pre-#3612 bytes.
    ///
    /// <para>#3622: the kept stretch ends on a whole character (<see cref="SlackCutLength"/>), and the
    /// omitted count is stated in the characters a reader would count — text elements, so one emoji is
    /// one, a letter with its combining accent is one — not in UTF-16 units. Kept plus omitted is the
    /// value's own character count, which is the only arithmetic a reader can check. The count can
    /// only be at or below the UTF-16 count the first sizing pass allowed for, so the note can only be
    /// narrower than the room held for it, and the field stays inside its limit.</para>
    /// </summary>
    private static string SlackFieldText(string label, string value) =>
        SlackBoundedText($"*{SlackEscape(label)}:*\n", value, SlackFieldTextLimit);

    /// <summary>
    /// <paramref name="prefix"/> over <paramref name="value"/> inside <paramref name="limit"/>, the cut
    /// stated inline when there is one — the mechanics of <see cref="SlackFieldText"/>, which is this at
    /// <see cref="SlackFieldTextLimit"/> with a <c>*label:*</c> prefix, lifted out (#3644) so a record's
    /// code-blocked text (<see cref="SlackRecordText"/>, empty prefix, the record's share) states its cut in
    /// exactly the words and the characters a cut field does. One note, one arithmetic, two sites.
    /// </summary>
    private static string SlackBoundedText(string prefix, string rawValue, int limit)
    {
        /* The value arrives raw and is escaped after it is cut (see SlackEscapedBounded), so the cut never
           lands inside an entity and the omitted count is in the characters the reader would count. */
        var escapedValue = SlackEscape(rawValue);
        if (prefix.Length + escapedValue.Length <= limit)
        {
            return prefix + escapedValue;
        }

        static string Note(int omitted) => string.Create(CultureInfo.InvariantCulture,
            $"... ({omitted:N0} more characters{SlackOmissionPointer.TrimEnd('.')})");

        /* The label is producer-controlled and short; a label that alone crowds the field is bounded so
           the arithmetic below stays positive. */
        if (prefix.Length > limit / 2)
        {
            prefix = prefix[..SlackCutLength(prefix, limit / 2)];
        }

        var keep = SlackEscapedFit(rawValue, Math.Max(0, limit - prefix.Length - Note(rawValue.Length).Length));
        /* Counting the omitted characters IS a walk of the omitted tail — the pre-#3622 subtraction counted
           units, which is the thing that was wrong — so this costs the tail's length, once, on the
           truncation path only. Every producer bounds its values upstream (the analysis formatter cuts
           drill-down text at 300 characters; the longest measured field is 318), so the tail is short in
           practice and the cost is the value's own length in the worst case (review note on #3625). */
        var note = Note(new StringInfo(rawValue[keep..]).LengthInTextElements);
        return prefix + SlackEscape(rawValue[..keep]) + note;
    }

    /// <summary>
    /// The length of the longest leading stretch of <paramref name="text"/> that fits inside
    /// <paramref name="limit"/> UTF-16 units without cutting through a character (#3622). Every Slack text
    /// cut in this builder passes through here — the prose hard split and the omission line's quoted
    /// fragment (#3493); the detail heading, the field label, the field value and the omitted-headings
    /// list (#3612) — because each of them sized its cut in UTF-16 units, and a cut landing between the
    /// two halves of a surrogate pair (any emoji, any supplementary-plane character, in a query text, a
    /// database name or an advice string) left an unpaired surrogate at the boundary. <c>JsonSerializer</c>
    /// does not emit that as a lone escape and Slack does not reject it — measured here, System.Text.Json
    /// substitutes U+FFFD, the same relaxation <see cref="EscapeForJson"/> already relies on — so the
    /// payload delivered, and what it delivered was wrong: the reader saw a replacement glyph (\uFFFD) that
    /// was never in the value, the "leading stretch" a cut field claims to show was no longer a prefix of
    /// the value, the hard split lost the character outright (each half became its own glyph, one per
    /// section), and the field's stated "N more characters" counted UTF-16 units the reader cannot see.
    /// #3622 expected the invalid-payload door; the door it actually opened is a payload that lies.
    ///
    /// <para><b>Grapheme, not code point, at every site.</b> The cut lands on a text-element boundary — an
    /// extended grapheme cluster, the unit a reader counts as one character: an emoji with its skin-tone
    /// modifier, a base letter with its combining accent, a CR LF pair — so no site can leave half a
    /// visible character behind, and the one omission that is stated in characters
    /// (<see cref="SlackFieldText"/>) cuts and counts in the same unit. The recommendation on #3622 was a
    /// code-point cut at the two long-prose sites on cost grounds, and that trade does not exist: the walk
    /// is bounded by <paramref name="limit"/>, not by the text — it stops at the first element that would
    /// cross the limit, and the window it hands the segmenter ends two units past the limit rather than at
    /// the end of the text (see the loop) — so it costs the same at a 120-character fragment as at a
    /// 3,000-character section, and one rule at six sites is cheaper to keep true than two. The one input a whole-element cut cannot
    /// serve is a single element wider than the whole limit — a run of combining marks with no base, the
    /// "Zalgo text" a query comment can carry — where keeping whole elements would keep nothing and the
    /// hard-split loop would never advance; there the cut falls back to the code-point boundary (one unit
    /// before a surrogate pair, else the limit itself), because a valid payload showing a broken glyph
    /// beats a delivery that hangs. Callers pass limits in the hundreds and thousands, so the fallback
    /// always keeps at least one unit.</para>
    /// </summary>
    internal static int SlackCutLength(ReadOnlySpan<char> text, int limit)
    {
        if (text.Length <= limit)
        {
            return text.Length;
        }

        if (limit <= 0)
        {
            return 0;
        }

        /* text.Length > limit here, so the window is never empty while cut <= limit, and every element is
           at least one unit wide, so the walk terminates at the first element that would cross.

           The window handed to the segmenter ends two units past the limit, not at the end of the text.
           Two units hold any scalar that straddles the limit whole, and grapheme boundaries are decided
           between one scalar and the next (every rule's context is to the LEFT), so every boundary
           decision at or before the limit is the one the full text would make: an element that ends
           inside the window ends where the full text ends it, and an element that reaches the window's
           end has crossed the limit however the full text would segment the rest of it. That is what
           makes the cost claim in the doc block true — without the window, one run of combining marks
           makes this call scan to the end of the text before concluding that it crosses (review note on
           #3625). */
        var cut = 0;
        while (true)
        {
            var window = text.Slice(cut, Math.Min(text.Length - cut, limit - cut + 2));
            var element = StringInfo.GetNextTextElementLength(window);
            if (cut + element > limit)
            {
                break;
            }

            cut += element;
        }

        if (cut > 0)
        {
            return SlackBeforeEntity(text, cut);
        }

        /* No whole element fits: a code-point boundary keeps the payload valid and the caller moving. */
        return char.IsHighSurrogate(text[limit - 1]) && char.IsLowSurrogate(text[limit]) ? limit - 1 : limit;
    }

    /// <summary>
    /// <paramref name="cut"/>, moved back to the start of the entity (<c>&amp;amp;</c>, <c>&amp;lt;</c>, <c>&amp;gt;</c>) it
    /// would otherwise fall inside, so escaped text is never cut into half an entity. Unchanged when it falls
    /// on no entity, or when backing off would leave nothing.
    /// </summary>
    private static int SlackBeforeEntity(ReadOnlySpan<char> text, int cut)
    {
        for (var back = 1; back <= 4 && back <= cut; back++)
        {
            var start = cut - back;
            if (text[start] != '&')
            {
                continue;
            }

            var tail = text[start..];
            if ((tail.StartsWith("&amp;", StringComparison.Ordinal) && back < 5)
                || (tail.StartsWith("&lt;", StringComparison.Ordinal) && back < 4)
                || (tail.StartsWith("&gt;", StringComparison.Ordinal) && back < 4))
            {
                return start > 0 ? start : cut;
            }
        }

        return cut;
    }

    /// <summary>
    /// Builds a Slack incoming webhook payload with a colored attachment sidebar.
    /// Uses Slack Block Kit for rich formatting.
    /// <para>#2710: a non-null <paramref name="triageUrl"/> adds a context block holding a mrkdwn link to the
    /// computed triage page, placed above the "Sent by" context footer. #6965: a link, not a url button, because
    /// a url button still sends Slack interaction payloads that a webhook-only app cannot answer. Null renders
    /// the pre-#2710 payload.</para>
    /// <para>#3297: <paramref name="detailText"/> is the alert's flat prose detail, rendered as mrkdwn
    /// <c>section</c> blocks directly under the field block — Block Kit's home for prose, and above
    /// the per-incident dividers so the remedy reads before the drill-down. #3493: as many section blocks
    /// as its size needs rather than one, each inside Slack's per-text-object ceiling and all of them
    /// inside the message's block budget — see <see cref="AddSlackProseSections"/>.</para>
    /// <para>#3612: the structured details are under the same discipline — every body through the same
    /// splitter, every field inside the field cap, and the whole run inside the block budget the head and
    /// footer leave, dropping whole details from the end with the omission stated — see
    /// <see cref="AddSlackDetailBlocks"/>. The message never exceeds <see cref="SlackMessageBlockLimit"/>
    /// blocks by construction, whichever half is heavy.</para>
    /// </summary>
    internal static string BuildSlackPayload(
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
        /* Human name in the header when present; ForMetric above keeps the immutable metric-name key. */
        var titleName = ExternalTitle(string.IsNullOrEmpty(displayName) ? metricName : displayName, context);
        /* One instant, local derived from it — see the Teams builder's note. */
        var utcNow = nowUtc ?? DateTime.UtcNow;
        var localNow = DateTime.SpecifyKind(utcNow, DateTimeKind.Utc).ToLocalTime();

        var title = isTest
            ? $"{emoji} TEST — {metricName}"
            : $"{emoji} {badgeText} — {titleName}";

        /* A header's text is capped by Slack at 150 characters; a longer one is cut on a whole character and
           ends in an ellipsis. */
        if (title.Length > SlackHeaderTextLimit)
        {
            title = title[..SlackCutLength(title, SlackHeaderTextLimit - 3)] + "...";
        }

        var blocks = new List<object>
        {
            new
            {
                type = "header",
                text = new { type = "plain_text", text = title, emoji = true }
            }
        };

        var fields = new List<object>();

        if (isTest)
        {
            fields.Add(new { type = "mrkdwn", text = "*Status:*\nWebhook configuration is working correctly" });
            fields.Add(new { type = "mrkdwn", text = $"*Sent at:*\n{localNow:yyyy-MM-dd HH:mm:ss}" });
        }
        else
        {
            fields.Add(new { type = "mrkdwn", text = $"*Server:*\n{SlackEscape(serverName)}" });
            var resourceName = DeriveResourceName(context);
            if (resourceName is not null)
                fields.Add(new { type = "mrkdwn", text = $"*Resource:*\n{SlackEscape(resourceName)}" });
            var database = DeriveResourceDatabase(context);
            if (!string.IsNullOrEmpty(database))
                fields.Add(new { type = "mrkdwn", text = $"*Database:*\n{SlackEscape(database)}" });
            fields.Add(new { type = "mrkdwn", text = $"*Current Value:*\n{SlackEscape(currentValue)}" });
            /* #5825: an alert resolution has no threshold, and an empty field says nothing. */
            if (!string.IsNullOrEmpty(thresholdValue))
                fields.Add(new { type = "mrkdwn", text = $"*Threshold:*\n{SlackEscape(thresholdValue)}" });
            fields.Add(new { type = "mrkdwn", text = $"*Time (UTC):*\n{utcNow:yyyy-MM-dd HH:mm:ss}" });
            fields.Add(new { type = "mrkdwn", text = $"*Time (Local):*\n{localNow:yyyy-MM-dd HH:mm:ss}" });
        }

        AddSlackFieldSections(blocks, fields);

        /* #3493 / #3612: the message is composed in three runs and budgeted in the order of what is
           FIXED first. The head (above: one header block and one field section, since the lead fields
           never exceed ten) and the footer (below: the triage button when there is one, and the context
           line — one or two blocks) are small and cannot yield; the structured details are bounded next,
           against what the head and footer leave under the 50-block limit less one block held for the
           prose when there is one; the prose is packed last into exactly what remains. Composed out of
           visual order because a budget can only count blocks that already exist. The visual order is
           unchanged: head, prose, details, footer — exactly where each has always rendered. */
        var footerBlocks = new List<object>();

        /* #5642: the newest-sample age, one small context block ahead of the triage link; it counts
           against the budget below with the rest of the footer. */
        var slackAgeLine = AlertDetailText.SampleAgeLineFor(context, prose);
        if (slackAgeLine is not null)
        {
            footerBlocks.Add(new
            {
                type = "context",
                elements = new object[] { new { type = "mrkdwn", text = slackAgeLine } }
            });
        }

        /* #2710: the triage-page link, above the footer so it reads as part of the alert rather than the
           boilerplate. #6965: a mrkdwn <url|label> link in a context block, not a url button. A url button
           still makes Slack send an interaction payload to the app, so a webhook-only app with no
           Interactivity Request URL showed "This app is not configured to handle interactive responses" on
           every click. A text link opens the page and sends nothing. The label and the URL are escaped for
           mrkdwn, and a pipe in the URL (it can only come from the operator's base URL, the query is
           percent-encoded) is percent-encoded so it cannot end the link target early. */
        if (triageUrl is not null)
        {
            var slackLinkUrl = SlackEscape(triageUrl).Replace("|", "%7C", StringComparison.Ordinal);
            footerBlocks.Add(new
            {
                type = "context",
                elements = new object[]
                {
                    new { type = "mrkdwn", text = $"<{slackLinkUrl}|{SlackEscape(TriageLink.LinkLabel(triageUrl))}>" }
                }
            });
        }

        var contextElements = new List<object>
        {
            new { type = "mrkdwn", text = $"Sent by {branding.EditionName}" }
        };
        if (!isTest && branding.SnoozeHint is not null)
        {
            contextElements.Add(new { type = "mrkdwn", text = branding.SnoozeHint });
        }

        footerBlocks.Add(new
        {
            type = "context",
            elements = contextElements
        });

        /* #3612: the structured details, inside the budget the head and footer leave — less the one
           block the prose needs to state itself when there is prose. The analysis pages that were lost
           carry their whole finding here and deliver no prose, so on them this budget is the fix; see
           AddSlackDetailBlocks for what drops first and how the omission is stated. */
        var detailBlocks = new List<object>();
        if (context?.Details is { Count: > 0 } details)
        {
            var proseReserve = prose is not null ? 1 : 0;
            AddSlackDetailBlocks(detailBlocks, details,
                blockBudget: SlackMessageBlockLimit - blocks.Count - footerBlocks.Count - proseReserve);
        }

        /* #3297: the prose detail, before the per-incident dividers; #3493: split across as many section
           blocks as its size needs, inside the block budget the head, details and footer leave over —
           at least one, because the details reserved it. */
        if (prose is not null)
        {
            AddSlackProseSections(blocks, SlackProseHeader, SlackEscape(prose),
                blockBudget: SlackMessageBlockLimit - blocks.Count - detailBlocks.Count - footerBlocks.Count);
        }

        blocks.AddRange(detailBlocks);
        blocks.AddRange(footerBlocks);

        var payload = new
        {
            attachments = new object[]
            {
                new { color = hexColor, blocks }
            }
        };

        return JsonSerializer.Serialize(payload, s_jsonOptions);
    }
}
