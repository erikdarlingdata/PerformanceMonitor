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
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7656: what the thread parent says about a request: the request, the server, the first metric, the severity, what opened
/// it (<c>alert</c>, <c>follow_up</c>, ...) and the web dashboard's base URL, empty for no link.</summary>
public sealed record SlackRequestFacts(Guid RequestId, string ServerName, string Metric, string Severity, string Trigger, string BaseUrl);

/// <summary>#7656: one post as it goes to Slack: the plain <c>text</c> (what a notification and a search show) and the blocks.</summary>
public sealed record SlackThreadPostContent(string Text, IReadOnlyList<object> Blocks);

/// <summary>
/// #7656: the posts of a request's thread, built from service data. Every collected value (a server name, a metric, an agent's text,
/// an object name) goes through <see cref="Bounded"/>: escaped by <see cref="Escape"/>, the one helper, before it is placed in mrkdwn,
/// so <c>&lt;!channel&gt;</c> and masked links in collected text show as characters, and held to its bound AFTER escaping, cut on a
/// whole text element (the #3625 rule), so a bound holds against Slack's 3,000-character section text whatever the value holds, and
/// no emoji is cut in half. Free text follows <c>mcp.dataDetail</c>: <c>full</c> as stored, <c>maskLiterals</c> through the prose
/// masker, <c>metadataOnly</c> left out, so the thread then carries verdicts, states and links.
/// </summary>
public static class SlackThreadPosts
{
    /// <summary>The web route of one request; the id rides in the hash query, as the report notice links it.</summary>
    private const string RequestRoute = "/#/investigations?request=";

    private const int StatementLimit = 1500;
    private const int TextLimit = 300;

    /// <summary>The one escape helper: <c>&amp;</c>, <c>&lt;</c> and <c>&gt;</c> become entities. Null and empty read as empty.</summary>
    public static string Escape(string? text) => WebhookAlertService.SlackEscape(text);

    /// <summary><paramref name="text"/> escaped and inside <paramref name="limit"/> characters of escaped text, cut on a whole text
    /// element and ending in <c>...</c> when it had to be cut. The webhook builders' own bound (<c>WebhookAlertService.SlackEscapedBounded</c>).</summary>
    internal static string Bounded(string? text, int limit) => WebhookAlertService.SlackEscapedBounded(text, limit);

    /// <summary>The post key of the thread parent. One per request.</summary>
    public const string RequestKey = "request";

    /// <summary>The post key of one journal row: its event and its journal id, so a mark reads in the journal as the row it posted.</summary>
    public static string EventKey(string eventKind, long journalId) => eventKind + ":" + journalId.ToString(CultureInfo.InvariantCulture);

    /// <summary>The post key of a change whose result never came: the <c>action_called</c> journal id, posted once as result unknown.</summary>
    public static string UnknownKey(long calledId) => "action_unknown:" + calledId.ToString(CultureInfo.InvariantCulture);

    /// <summary>The once key of one post of one request (journal <c>once_key</c>): written before the send, so a replayed pass and a
    /// second service on the same store post it once between them.</summary>
    public static string OnceKey(Guid requestId, string postKey) => "slack_thread:" + requestId.ToString("D") + ":" + postKey;

    /// <summary>The thread parent: the request, where it came from and a link to it.</summary>
    public static SlackThreadPostContent RequestPost(SlackRequestFacts facts, McpDataDetail mode)
    {
        ArgumentNullException.ThrowIfNull(facts);
        var server = Bounded(facts.ServerName, TextLimit);
        var metric = Bounded(facts.Metric, TextLimit);
        var severity = Bounded(facts.Severity, 50);
        var trigger = Bounded(facts.Trigger, 50);
        var text = $"Investigation request: {metric} on {server} ({severity})";
        var blocks = new List<object>
        {
            Section($"*Investigation request opened*\n*Server:* {server}\n*Metric:* {metric}\n*Severity:* {severity}\n*Opened by:* {trigger}"),
        };
        AddLink(blocks, RequestLink(facts.BaseUrl, facts.RequestId), "Open the request");
        return new SlackThreadPostContent(text, blocks);
    }

    /// <summary>An agent's claim: who, and which claim.</summary>
    public static SlackThreadPostContent ClaimPost(string agentName, int claimNo)
    {
        var agent = Bounded(agentName, TextLimit);
        var text = $"Claimed by {agent} (claim {claimNo.ToString(CultureInfo.InvariantCulture)})";
        return new SlackThreadPostContent(text, [Section(text)]);
    }

    /// <summary>The report, with the #6979 notice's content and its Slack bounds: the lead (verdict, server, metric) in one section, then
    /// the notice's own body blocks (the root cause's first line at the mode in a section of its own, the changes in another, each
    /// bounded after escaping), then the links. No section passes Slack's 3,000-character text limit.</summary>
    public static SlackThreadPostContent ReportPost(ReportNoticeContent content, McpDataDetail mode)
    {
        ArgumentNullException.ThrowIfNull(content);
        var verdict = Bounded(content.Verdict, 100);
        var server = Bounded(content.ServerName, TextLimit);
        var blocks = new List<object>
        {
            Section($"*Agent report: {verdict}*\n*Server:* {server}\n*Metric:* {Bounded(content.Metric, TextLimit)}"),
        };
        var rootCause = InvestigationEventDelivery.RootCauseFirstLine(content.RootCauseFirstLine, mode);
        blocks.AddRange(WebhookAlertService.ReportNoticeSlackBodyBlocks(content with { RootCauseFirstLine = rootCause }));
        var (requestUrl, changesUrl) = WebhookAlertService.ReportNoticeLinks(content);
        AddLinks(blocks, (requestUrl, "Open the request"), (changesUrl, "Open the agent changes"));
        return new SlackThreadPostContent($"Agent report: {verdict} on {server}", blocks);
    }

    /// <summary>One agent change: the tool, the object at the mode, and how it ended.</summary>
    public static SlackThreadPostContent ChangePost(string tool, string objectText, string result, McpDataDetail mode)
    {
        var line = ChangeLine(tool, objectText, result, mode);
        return new SlackThreadPostContent("Agent change: " + Bounded(tool, 50) + " " + Bounded(result, 50), [Section("*Agent change*\n" + line)]);
    }

    /// <summary>A proposal, when it is made and at each decision: the kind, the server, the state, when it expires (the card, when it is
    /// made), the statement at the mode (<c>metadataOnly</c> withholds it and the link stands in), and the link to the proposal page.</summary>
    public static SlackThreadPostContent ProposalPost(string tool, string serverName, string state, string? statement, string? proposalUrl, McpDataDetail mode, DateTime? expiresUtc = null)
    {
        var kind = Bounded(tool, 50);
        var server = Bounded(serverName, TextLimit);
        var stateText = Bounded(state, 50);
        var lines = new List<string> { $"*Proposed action: {kind}*", $"*Server:* {server}", $"*State:* {stateText}" };
        if (expiresUtc is DateTime expires)
        {
            lines.Add("*Expires:* " + expires.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC");
        }

        if (!string.IsNullOrWhiteSpace(statement) && mode != McpDataDetail.MetadataOnly)
        {
            var shown = mode == McpDataDetail.MaskLiterals ? McpDataDetailMasker.MaskLiterals(statement) : statement;
            lines.Add("```" + Bounded(shown, StatementLimit) + "```");
        }

        var blocks = new List<object> { Section(string.Join("\n", lines)) };
        AddLink(blocks, proposalUrl, "Open the proposal");
        return new SlackThreadPostContent($"Proposed action: {kind} on {server} ({stateText})", blocks);
    }

    /// <summary>How a request ended without a resolution: closed by a person, expired, or out of claims. <paramref name="actor"/> is the
    /// person when one acted, and <paramref name="reason"/> what they or the service said.</summary>
    public static SlackThreadPostContent EndPost(string state, string? actor, string? reason)
    {
        var stateText = Bounded(state, 50);
        var text = stateText;
        if (!string.IsNullOrWhiteSpace(actor) && !string.Equals(actor, "service", StringComparison.Ordinal))
        {
            text += " by " + Bounded(actor, TextLimit);
        }

        if (!string.IsNullOrWhiteSpace(reason))
        {
            text += ": " + Bounded(reason, TextLimit);
        }

        return new SlackThreadPostContent(text, [Section("*" + stateText + "*" + text[stateText.Length..])]);
    }

    /// <summary>The resolution: every metric of the request cleared.</summary>
    public static SlackThreadPostContent ResolutionPost(string metrics)
    {
        var text = "Resolved: " + Bounded(metrics, TextLimit);
        return new SlackThreadPostContent(text, [Section("*Resolved*\n" + Bounded(metrics, TextLimit))]);
    }

    /// <summary>The request's web link, or null without a usable base URL (the same rule as the report notice's links).</summary>
    internal static string? RequestLink(string? baseUrl, Guid requestId)
    {
        var trimmed = baseUrl?.Trim().TrimEnd('/');
        if (string.IsNullOrEmpty(trimmed)
            || !Uri.TryCreate(trimmed, UriKind.Absolute, out var uri)
            || (!string.Equals(uri.Scheme, "http", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase)))
        {
            return null;
        }

        return trimmed + RequestRoute + Uri.EscapeDataString(requestId.ToString("D"));
    }

    /* The object goes through the #6979 rule: kept at full, masked as prose otherwise, an all-digit id kept. */
    private static string ChangeLine(string tool, string objectText, string result, McpDataDetail mode)
    {
        var toolText = Bounded(tool?.Replace('`', '\''), 50);
        var target = Bounded(InvestigationEventDelivery.ChangeObject(objectText ?? "", null, mode), 100);
        var resultText = Bounded(result, 100);
        return $"• `{toolText}`" + (target.Length > 0 ? $" {target}" : "") + (resultText.Length > 0 ? $": {resultText}" : "");
    }

    private static object Section(string mrkdwn) => new { type = "section", text = new { type = "mrkdwn", text = mrkdwn } };

    private static void AddLink(List<object> blocks, string? url, string label) => AddLinks(blocks, (url, label));

    /* A link target is escaped and its pipe percent-encoded, so a URL cannot end the mrkdwn link early. */
    private static void AddLinks(List<object> blocks, params (string? Url, string Label)[] links)
    {
        var elements = links
            .Where(l => !string.IsNullOrWhiteSpace(l.Url))
            .Select(l => (object)new { type = "mrkdwn", text = $"<{Escape(l.Url).Replace("|", "%7C", StringComparison.Ordinal)}|{l.Label}>" })
            .ToArray();
        if (elements.Length > 0)
        {
            blocks.Add(new { type = "context", elements });
        }
    }
}
