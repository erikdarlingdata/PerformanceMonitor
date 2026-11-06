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
/// an object name) goes through <see cref="Escape"/>, the one helper, before it is placed in mrkdwn, so <c>&lt;!channel&gt;</c> and
/// masked links in collected text show as characters. Free text follows <c>mcp.dataDetail</c>: <c>full</c> as stored,
/// <c>maskLiterals</c> through the prose masker, <c>metadataOnly</c> left out, so the thread then carries verdicts, states and links.
/// </summary>
public static class SlackThreadPosts
{
    /// <summary>The web route of one request; the id rides in the hash query, as the report notice links it.</summary>
    private const string RequestRoute = "/#/investigations?request=";

    private const int RootCauseLimit = 1500;
    private const int StatementLimit = 1500;
    private const int TextLimit = 300;

    /// <summary>The one escape helper: <c>&amp;</c>, <c>&lt;</c> and <c>&gt;</c> become entities. Null and empty read as empty.</summary>
    public static string Escape(string? text) => WebhookAlertService.SlackEscape(text);

    /// <summary>The post key of the thread parent. One per request.</summary>
    public const string RequestKey = "request";

    /// <summary>The post key of the report. One per request: a request reports once.</summary>
    public const string ReportKey = "report";

    /// <summary>The post key of the resolution. One per request.</summary>
    public const string ResolvedKey = "resolved";

    /// <summary>The post key of one journal row: its event and its journal id, so a mark reads in the journal as the row it posted.</summary>
    public static string EventKey(string eventKind, long journalId) => eventKind + ":" + journalId.ToString(CultureInfo.InvariantCulture);

    /// <summary>The post key of a change whose result never came: the <c>action_called</c> journal id, posted once as result unknown.</summary>
    public static string UnknownKey(long calledId) => "action_unknown:" + calledId.ToString(CultureInfo.InvariantCulture);

    /// <summary>The post key of one claim, by claim number.</summary>
    public static string ClaimKey(int claimNo) => "claim:" + claimNo.ToString(CultureInfo.InvariantCulture);

    /// <summary>The post key of one agent change, by its <c>action_called</c> journal id.</summary>
    public static string ChangeKey(long calledId) => "change:" + calledId.ToString(CultureInfo.InvariantCulture);

    /// <summary>The post key of one proposal state: a proposal posts when it is made and again at each decision.</summary>
    public static string ProposalKey(Guid proposalId, string state) => "proposal:" + proposalId.ToString("D") + ":" + state;

    /// <summary>The once key of one post of one request (journal <c>once_key</c>): written before the send, so a replayed pass and a
    /// second service on the same store post it once between them.</summary>
    public static string OnceKey(Guid requestId, string postKey) => "slack_thread:" + requestId.ToString("D") + ":" + postKey;

    /// <summary>The thread parent: the request, where it came from and a link to it.</summary>
    public static SlackThreadPostContent RequestPost(SlackRequestFacts facts, McpDataDetail mode)
    {
        ArgumentNullException.ThrowIfNull(facts);
        var server = Escape(Clip(facts.ServerName, TextLimit));
        var metric = Escape(Clip(facts.Metric, TextLimit));
        var severity = Escape(Clip(facts.Severity, 50));
        var trigger = Escape(Clip(facts.Trigger, 50));
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
        var agent = Escape(Clip(agentName, TextLimit));
        var text = $"Claimed by {agent} (claim {claimNo.ToString(CultureInfo.InvariantCulture)})";
        return new SlackThreadPostContent(text, [Section(text)]);
    }

    /// <summary>The report, with the #6979 notice's content: the verdict, the root cause's first line at the mode, the changes and the links.</summary>
    public static SlackThreadPostContent ReportPost(ReportNoticeContent content, McpDataDetail mode)
    {
        ArgumentNullException.ThrowIfNull(content);
        var verdict = Escape(Clip(content.Verdict, 100));
        var server = Escape(Clip(content.ServerName, TextLimit));
        var lines = new List<string> { $"*Agent report: {verdict}*", $"*Server:* {server}", $"*Metric:* {Escape(Clip(content.Metric, TextLimit))}" };
        var rootCause = InvestigationEventDelivery.RootCauseFirstLine(content.RootCauseFirstLine, mode);
        if (!string.IsNullOrWhiteSpace(rootCause))
        {
            lines.Add("*Root cause:* " + Escape(Clip(rootCause, RootCauseLimit)));
        }

        if (content.Changes.Count > 0)
        {
            lines.Add($"*Changes ({content.Changes.Count.ToString(CultureInfo.InvariantCulture)}):*");
            foreach (var change in content.Changes.Take(WebhookAlertService.ReportNoticeChangeLimit))
            {
                lines.Add(ChangeLine(change.Tool, change.Object, change.Result, mode));
            }

            if (content.Changes.Count > WebhookAlertService.ReportNoticeChangeLimit)
            {
                lines.Add($"_and {(content.Changes.Count - WebhookAlertService.ReportNoticeChangeLimit).ToString(CultureInfo.InvariantCulture)} more_");
            }
        }

        var blocks = new List<object> { Section(string.Join("\n", lines)) };
        var (requestUrl, changesUrl) = WebhookAlertService.ReportNoticeLinks(content);
        AddLinks(blocks, (requestUrl, "Open the request"), (changesUrl, "Open the agent changes"));
        return new SlackThreadPostContent($"Agent report: {verdict} on {server}", blocks);
    }

    /// <summary>One agent change: the tool, the object at the mode, and how it ended.</summary>
    public static SlackThreadPostContent ChangePost(string tool, string objectText, string result, McpDataDetail mode)
    {
        var line = ChangeLine(tool, objectText, result, mode);
        return new SlackThreadPostContent("Agent change: " + Escape(Clip(tool, 50)) + " " + Escape(Clip(result, 50)), [Section("*Agent change*\n" + line)]);
    }

    /// <summary>A proposal, when it is made and at each decision: the kind, the server, the state, the statement at the mode
    /// (<c>metadataOnly</c> withholds it and the link stands in), and the link to the proposal page.</summary>
    public static SlackThreadPostContent ProposalPost(string tool, string serverName, string state, string? statement, string? proposalUrl, McpDataDetail mode)
    {
        var kind = Escape(Clip(tool, 50));
        var server = Escape(Clip(serverName, TextLimit));
        var stateText = Escape(Clip(state, 50));
        var lines = new List<string> { $"*Proposed action: {kind}*", $"*Server:* {server}", $"*State:* {stateText}" };
        if (!string.IsNullOrWhiteSpace(statement) && mode != McpDataDetail.MetadataOnly)
        {
            var shown = mode == McpDataDetail.MaskLiterals ? McpDataDetailMasker.MaskLiterals(statement) : statement;
            lines.Add("```" + Escape(Clip(shown, StatementLimit)) + "```");
        }

        var blocks = new List<object> { Section(string.Join("\n", lines)) };
        AddLink(blocks, proposalUrl, "Open the proposal");
        return new SlackThreadPostContent($"Proposed action: {kind} on {server} ({stateText})", blocks);
    }

    /// <summary>How a request ended without a resolution: closed by a person, expired, or out of claims. <paramref name="actor"/> is the
    /// person when one acted, and <paramref name="reason"/> what they or the service said.</summary>
    public static SlackThreadPostContent EndPost(string state, string? actor, string? reason)
    {
        var stateText = Escape(Clip(state, 50));
        var text = stateText;
        if (!string.IsNullOrWhiteSpace(actor) && !string.Equals(actor, "service", StringComparison.Ordinal))
        {
            text += " by " + Escape(Clip(actor, TextLimit));
        }

        if (!string.IsNullOrWhiteSpace(reason))
        {
            text += ": " + Escape(Clip(reason, TextLimit));
        }

        return new SlackThreadPostContent(text, [Section("*" + stateText + "*" + text[stateText.Length..])]);
    }

    /// <summary>The resolution: every metric of the request cleared.</summary>
    public static SlackThreadPostContent ResolutionPost(string metrics)
    {
        var text = "Resolved: " + Escape(Clip(metrics, TextLimit));
        return new SlackThreadPostContent(text, [Section("*Resolved*\n" + Escape(Clip(metrics, TextLimit)))]);
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
        var toolText = Escape(Clip(tool?.Replace('`', '\''), 50));
        var target = Escape(Clip(InvestigationEventDelivery.ChangeObject(objectText ?? "", null, mode), 100));
        var resultText = Escape(Clip(result, 100));
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

    private static string Clip(string? text, int limit)
    {
        if (string.IsNullOrEmpty(text))
        {
            return "";
        }

        return text.Length <= limit ? text : text[..limit] + "...";
    }
}
