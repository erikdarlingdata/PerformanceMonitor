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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// #6979: the report notice. An agent's report on an investigation request, posted to a Slack or Teams destination with the
/// changes the agent made and links to the request and to the Admin agent-changes tab. Its own payloads, not the alert card:
/// the card carries a title, a server, one value and one detail, and a report needs more rows than that.
/// </summary>
public partial class WebhookAlertService : IReportNoticeSends
{
    /// <summary>The most changes a notice lists; the rest are counted in one line.</summary>
    internal const int ReportNoticeChangeLimit = 10;

    /* Each part of a change line is cut before it is escaped, so ten lines stay inside Slack's 3000-character section text. */
    private const int ReportNoticeToolLimit = 50;
    private const int ReportNoticeObjectLimit = 100;
    private const int ReportNoticeResultLimit = 100;
    private const int ReportNoticeRootCauseLimit = 1500;

    /// <summary>The web route of one request; the id rides in the hash query, like the triage link.</summary>
    private const string ReportNoticeRequestRoute = "/#/investigations?request=";

    /// <summary>The web route of the Admin agent-changes tab (<c>#/admin/{tab}</c>).</summary>
    private const string ReportNoticeChangesRoute = "/#/admin/agent-changes";

    /// <inheritdoc />
    public async Task<string?> TrySendReportNoticeAsync(
        string channel, string destination, ReportNoticeContent content, CancellationToken cancellationToken = default)
    {
        if (channel != NotificationRouter.SlackChannel && channel != NotificationRouter.TeamsChannel)
        {
            return $"{channel} does not carry a report notice";
        }

        try
        {
            var settings = _settings.SnapshotForDelivery();
            var slack = channel == NotificationRouter.SlackChannel;
            var payload = slack ? BuildReportNoticeSlackPayload(content, _branding) : BuildReportNoticeTeamsPayload(content);
            var error = await PostWebhookAsync(
                destination, payload, slack ? settings.SlackProxyAddress : settings.TeamsProxyAddress,
                cancellationToken: cancellationToken, bodyLog: _logger);
            RecordReportNoticeHealth(slack, error);
            return error;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            RecordReportNoticeHealth(channel == NotificationRouter.SlackChannel, ex.Message);
            return ex.Message;
        }
    }

    /// <summary>
    /// #6979: a notice send counts toward the channel's health as an alert send does (the consecutive-failure count and the last
    /// error that <see cref="GetChannelFailureCounts"/> serves, which the "Notification Channel Failing" self-alert reads).
    /// It logs nothing: the caller logs a failed notice once, with the request id, the channel and the error.
    /// </summary>
    private void RecordReportNoticeHealth(bool slack, string? error)
    {
        if (slack)
        {
            _consecutiveSlackFailures = error is null ? 0 : _consecutiveSlackFailures + 1;
            _lastSlackError = error;
        }
        else
        {
            _consecutiveTeamsFailures = error is null ? 0 : _consecutiveTeamsFailures + 1;
            _lastTeamsError = error;
        }
    }

    /// <summary>
    /// PURE: the request link and the agent-changes link for a base URL, each null when the base is empty or not an absolute
    /// http or https URL, as <see cref="TriageLink.Build"/> decides. The changes link is null as well when the notice lists no
    /// change, because the tab then has nothing of this request to show.
    /// </summary>
    internal static (string? Request, string? Changes) ReportNoticeLinks(ReportNoticeContent content)
    {
        var trimmed = content.BaseUrl?.Trim().TrimEnd('/');
        if (string.IsNullOrEmpty(trimmed)
            || !Uri.TryCreate(trimmed, UriKind.Absolute, out var uri)
            || (!string.Equals(uri.Scheme, "http", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase)))
        {
            return (null, null);
        }

        var request = trimmed + ReportNoticeRequestRoute + Uri.EscapeDataString(content.RequestId.ToString("D", CultureInfo.InvariantCulture));
        return (request, content.Changes.Count > 0 ? trimmed + ReportNoticeChangesRoute : null);
    }

    private static string ReportNoticeClip(string? text, int limit)
    {
        if (string.IsNullOrEmpty(text))
        {
            return string.Empty;
        }

        return text.Length <= limit ? text : text[..SlackCutLength(text, limit)] + "...";
    }

    private static string ReportNoticeMoreLine(int count) =>
        count == 1 ? "and 1 more change" : $"and {count} more changes";

    private static string ReportNoticeTitle(ReportNoticeContent content) =>
        $"Agent report: {content.Verdict}";

    /// <summary>The Slack payload of a report notice: a header, the lead fields, the root cause's first line when there is one,
    /// the changes (at most <see cref="ReportNoticeChangeLimit"/>, then a count of the rest) and the links.</summary>
    internal static string BuildReportNoticeSlackPayload(ReportNoticeContent content, AlertBranding branding)
    {
        var title = ReportNoticeClip(ReportNoticeTitle(content), SlackHeaderTextLimit - 3);
        var blocks = new List<object>
        {
            new { type = "header", text = new { type = "plain_text", text = title, emoji = true } },
            new
            {
                type = "section",
                fields = new object[]
                {
                    new { type = "mrkdwn", text = $"*Server:*\n{SlackEscapedBounded(content.ServerName, SlackFieldTextLimit - 12)}" },
                    new { type = "mrkdwn", text = $"*Metric:*\n{SlackEscapedBounded(content.Metric, SlackFieldTextLimit - 12)}" },
                    new { type = "mrkdwn", text = $"*Verdict:*\n{SlackEscapedBounded(content.Verdict, SlackFieldTextLimit - 12)}" },
                },
            },
        };

        if (!string.IsNullOrWhiteSpace(content.RootCauseFirstLine))
        {
            blocks.Add(new
            {
                type = "section",
                text = new { type = "mrkdwn", text = "*Root cause:*\n" + SlackEscapedBounded(content.RootCauseFirstLine, ReportNoticeRootCauseLimit) },
            });
        }

        if (content.Changes.Count > 0)
        {
            var lines = new List<string> { $"*Changes ({content.Changes.Count}):*" };
            foreach (var change in content.Changes.Take(ReportNoticeChangeLimit))
            {
                var tool = SlackEscapedBounded(change.Tool?.Replace('`', '\''), ReportNoticeToolLimit);
                var target = SlackEscapedBounded(change.Object, ReportNoticeObjectLimit);
                var result = SlackEscapedBounded(change.Result, ReportNoticeResultLimit);
                lines.Add($"• `{tool}`" + (target.Length > 0 ? $" {target}" : "") + (result.Length > 0 ? $": {result}" : ""));
            }

            if (content.Changes.Count > ReportNoticeChangeLimit)
            {
                lines.Add("_" + ReportNoticeMoreLine(content.Changes.Count - ReportNoticeChangeLimit) + "_");
            }

            blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = string.Join("\n", lines) } });
        }

        var (requestUrl, changesUrl) = ReportNoticeLinks(content);
        var links = new List<object>();
        if (requestUrl is not null)
        {
            links.Add(new { type = "mrkdwn", text = $"<{SlackLinkTarget(requestUrl)}|Open the request>" });
        }

        if (changesUrl is not null)
        {
            links.Add(new { type = "mrkdwn", text = $"<{SlackLinkTarget(changesUrl)}|Open the agent changes>" });
        }

        if (links.Count > 0)
        {
            blocks.Add(new { type = "context", elements = links.ToArray() });
        }

        blocks.Add(new { type = "context", elements = new object[] { new { type = "mrkdwn", text = $"Sent by {branding.EditionName}" } } });
        var color = content.Verdict == "needs_person" ? "#D13438" : "#0078D4";
        return JsonSerializer.Serialize(new { attachments = new object[] { new { color, blocks } } }, s_jsonOptions);
    }

    /// <summary>A link target inside a mrkdwn <c>&lt;url|label&gt;</c>: escaped, and a pipe percent-encoded so it cannot end the target early.</summary>
    private static string SlackLinkTarget(string url) => SlackEscape(url).Replace("|", "%7C", StringComparison.Ordinal);

    /// <summary>The Teams MessageCard of a report notice. The links are markdown in the text, not <c>potentialAction</c> buttons:
    /// Microsoft says MessageCard buttons do not work through a Workflows webhook.</summary>
    internal static string BuildReportNoticeTeamsPayload(ReportNoticeContent content)
    {
        var facts = new List<object>
        {
            new { name = "Server", value = TeamsEscape(ReportNoticeClip(content.ServerName, 200)) },
            new { name = "Metric", value = TeamsEscape(ReportNoticeClip(content.Metric, 200)) },
            new { name = "Verdict", value = TeamsEscape(ReportNoticeClip(content.Verdict, 200)) },
        };
        var sections = new List<object>
        {
            new { activityTitle = TeamsEscape(ReportNoticeTitle(content)), facts, markdown = true },
        };

        if (!string.IsNullOrWhiteSpace(content.RootCauseFirstLine))
        {
            sections.Add(new
            {
                activityTitle = "Root cause",
                text = TeamsEscape(ReportNoticeClip(content.RootCauseFirstLine, ReportNoticeRootCauseLimit)),
                markdown = true,
            });
        }

        if (content.Changes.Count > 0)
        {
            var lines = new List<string>();
            foreach (var change in content.Changes.Take(ReportNoticeChangeLimit))
            {
                var tool = TeamsEscape(ReportNoticeClip(change.Tool, ReportNoticeToolLimit));
                var target = TeamsEscape(ReportNoticeClip(change.Object, ReportNoticeObjectLimit));
                var result = TeamsEscape(ReportNoticeClip(change.Result, ReportNoticeResultLimit));
                lines.Add($"- {tool}" + (target.Length > 0 ? $" {target}" : "") + (result.Length > 0 ? $": {result}" : ""));
            }

            if (content.Changes.Count > ReportNoticeChangeLimit)
            {
                lines.Add("- " + ReportNoticeMoreLine(content.Changes.Count - ReportNoticeChangeLimit));
            }

            sections.Add(new { activityTitle = $"Changes ({content.Changes.Count})", text = string.Join("\n\n", lines), markdown = true });
        }

        var (requestUrl, changesUrl) = ReportNoticeLinks(content);
        var links = new List<string>();
        if (requestUrl is not null)
        {
            links.Add($"[Open the request]({requestUrl})");
        }

        if (changesUrl is not null)
        {
            links.Add($"[Open the agent changes]({changesUrl})");
        }

        if (links.Count > 0)
        {
            sections.Add(new { text = string.Join(" | ", links), markdown = true });
        }

        var card = new
        {
            @type = "MessageCard",
            @context = "http://schema.org/extensions",
            themeColor = content.Verdict == "needs_person" ? "D13438" : "0078D4",
            summary = $"[SQL Monitor] {ReportNoticeTitle(content)} on {content.ServerName}",
            sections,
        };
        return JsonSerializer.Serialize(card, s_jsonOptions);
    }
}
