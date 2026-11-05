/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6979: the report notice payloads for Slack and Teams, parsed from the JSON they send: the lead facts, the changes (ten, then
/// a count of the rest), the links (none without a base URL, no agent-changes link without a change), the root cause line left
/// out when null, the collected text escaped, and a channel that carries no notice refused.
/// </summary>
public sealed class ReportNoticePayloadTests
{
    private static readonly Guid RequestId = Guid.Parse("11111111-2222-3333-4444-555555555555");
    private const string Base = "https://dash.example.test:5443/";

    private static ReportNoticeContent Content(int changes = 1, string? rootCause = "Blocking chain from one session", string baseUrl = Base) =>
        new(RequestId, "needs_person", "srv-one", "Blocking", rootCause,
            Enumerable.Range(1, changes).Select(i => new ReportNoticeChange("set_config", $"object {i}", "done")).ToList(), baseUrl);

    /// <summary>Every string value in the JSON, so a test reads the text the channel would show rather than the encoded body.</summary>
    private static List<string> Strings(string json)
    {
        var all = new List<string>();
        void Walk(JsonElement e)
        {
            switch (e.ValueKind)
            {
                case JsonValueKind.String:
                    all.Add(e.GetString()!);
                    break;
                case JsonValueKind.Array:
                    foreach (var x in e.EnumerateArray()) Walk(x);
                    break;
                case JsonValueKind.Object:
                    foreach (var p in e.EnumerateObject()) Walk(p.Value);
                    break;
            }
        }

        using var doc = JsonDocument.Parse(json);
        Walk(doc.RootElement);
        return all;
    }

    private static string Slack(ReportNoticeContent c) => string.Join("\n", Strings(WebhookAlertService.BuildReportNoticeSlackPayload(c, DarlingAlertDeliverer.Branding)));

    private static string Teams(ReportNoticeContent c) => string.Join("\n", Strings(WebhookAlertService.BuildReportNoticeTeamsPayload(c)));

    private const string RequestLink = "https://dash.example.test:5443/#/investigations?request=11111111-2222-3333-4444-555555555555";
    private const string ChangesLink = "https://dash.example.test:5443/#/admin/agent-changes";

    [Fact]
    public void Slack_OneChange_ListsIt_AndLinksToTheRequestAndTheAgentChangesTab()
    {
        var text = Slack(Content());
        Assert.Contains("srv-one", text);
        Assert.Contains("Blocking", text);
        Assert.Contains("needs_person", text);
        Assert.Contains("Blocking chain from one session", text);
        Assert.Contains("`set_config` object 1: done", text);
        Assert.Contains($"<{RequestLink}|", text);
        Assert.Contains($"<{ChangesLink}|", text);
    }

    [Fact]
    public void Teams_OneChange_ListsIt_AndLinksInTheText_NotInAButton()
    {
        var json = WebhookAlertService.BuildReportNoticeTeamsPayload(Content());
        var text = Teams(Content());
        Assert.Contains("srv-one", text);
        Assert.Contains("needs_person", text);
        Assert.Contains("Blocking chain from one session", text);
        Assert.Contains("set_config object 1: done", text);
        Assert.Contains($"[Open the request]({RequestLink})", text);
        Assert.Contains($"[Open the agent changes]({ChangesLink})", text);
        Assert.DoesNotContain("potentialAction", json);
    }

    [Fact]
    public void TwelveChanges_RenderTen_AndOneLineCountsTheOtherTwo()
    {
        var slack = Slack(Content(changes: 12));
        Assert.Contains("object 10", slack);
        Assert.DoesNotContain("object 11", slack);
        Assert.Contains("and 2 more changes", slack);
        Assert.Contains("Changes (12)", slack);

        var teams = Teams(Content(changes: 12));
        Assert.Contains("object 10", teams);
        Assert.DoesNotContain("object 11", teams);
        Assert.Contains("and 2 more changes", teams);
    }

    [Fact]
    public void TenChanges_HaveNoCountLine()
    {
        Assert.DoesNotContain("more change", Slack(Content(changes: 10)));
        Assert.DoesNotContain("more change", Teams(Content(changes: 10)));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not a url")]
    public void NoUsableBaseUrl_GivesNoLinks(string baseUrl)
    {
        Assert.DoesNotContain("#/investigations", Slack(Content(baseUrl: baseUrl)));
        Assert.DoesNotContain("#/admin", Slack(Content(baseUrl: baseUrl)));
        Assert.DoesNotContain("#/investigations", Teams(Content(baseUrl: baseUrl)));
        Assert.DoesNotContain("#/admin", Teams(Content(baseUrl: baseUrl)));
    }

    [Fact]
    public void NoChanges_LinksToTheRequestOnly()
    {
        foreach (var text in new[] { Slack(Content(changes: 0)), Teams(Content(changes: 0)) })
        {
            Assert.Contains("#/investigations?request=", text);
            Assert.DoesNotContain("#/admin/agent-changes", text);
            Assert.DoesNotContain("Changes (", text);
        }
    }

    [Fact]
    public void ANullRootCause_LeavesThatLineOut()
    {
        Assert.DoesNotContain("Root cause", Slack(Content(rootCause: null)));
        Assert.DoesNotContain("Root cause", Teams(Content(rootCause: null)));
        Assert.Contains("Root cause", Slack(Content()));
        Assert.Contains("Root cause", Teams(Content()));
    }

    [Fact]
    public void CollectedText_IsEscaped_ForSlackMrkdwn()
    {
        var c = Content(rootCause: "<!channel> see <https://evil.test|click> & more") with
        {
            Changes = new[] { new ReportNoticeChange("set_config", "<@U123> `x`", "<b>done</b>") },
        };
        var text = Slack(c);
        Assert.DoesNotContain("<!channel>", text);
        Assert.DoesNotContain("<@U123>", text);
        Assert.DoesNotContain("<https://evil.test", text);
        Assert.Contains("&lt;!channel&gt; see &lt;https://evil.test|click&gt; &amp; more", text);
        Assert.Contains("&lt;@U123&gt;", text);
        Assert.Contains("&lt;b&gt;done&lt;/b&gt;", text);
    }

    [Fact]
    public void CollectedText_IsEscaped_ForTeams()
    {
        var c = Content(rootCause: "<b>bold</b> [click](https://evil.test) *x*") with
        {
            Changes = new[] { new ReportNoticeChange("set_config", "<img src=x> & `y`", "done") },
        };
        var text = Teams(c);
        Assert.DoesNotContain("<b>", text);
        Assert.DoesNotContain("<img", text);
        Assert.Contains("&lt;b&gt;bold&lt;/b&gt; \\[click\\](https://evil.test) \\*x\\*", text);
        Assert.Contains("&lt;img src=x&gt; &amp; \\`y\\`", text);
    }

    [Fact]
    public void ALongChangeObject_IsCut_SoTenLinesFitSlacksSectionLimit()
    {
        var big = new string('<', 400);
        var c = Content() with { Changes = Enumerable.Range(0, 12).Select(_ => new ReportNoticeChange(big, big, big)).ToList() };
        using var doc = JsonDocument.Parse(WebhookAlertService.BuildReportNoticeSlackPayload(c, DarlingAlertDeliverer.Branding));
        var blocks = doc.RootElement.GetProperty("attachments")[0].GetProperty("blocks").EnumerateArray();
        foreach (var block in blocks.Where(b => b.TryGetProperty("text", out var t) && t.ValueKind == JsonValueKind.Object))
        {
            Assert.True(block.GetProperty("text").GetProperty("text").GetString()!.Length <= 3000);
        }
    }

    [Theory]
    [InlineData("email")]
    [InlineData("pagerduty")]
    [InlineData("generic")]
    [InlineData("")]
    public async Task AnyOtherChannel_ReturnsAnError(string channel)
    {
        var service = new WebhookAlertService(new DarlingAlertSettings(new DarlingConfig()), DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);
        var error = await service.TrySendReportNoticeAsync(channel, "https://hooks.example.test/x", Content());
        Assert.NotNull(error);
        Assert.Contains("does not carry a report notice", error);
    }
}
