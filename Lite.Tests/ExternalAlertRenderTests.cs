using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5654: the Slack and Teams cards show alert text as plain text, and an alert another service submitted is marked as
/// external in its title. The renderer is shared by Lite and Darling, so these pin both the new behaviour and that a
/// built-in alert's own text renders as before.
/// </summary>
public class ExternalAlertRenderTests
{
    private static readonly AlertBranding Branding = new("Performance Monitor Lite", null);

    private const string SlackInjection = "<!channel> run <https://example.invalid|the fix>";
    private const string TeamsInjection = "[Acknowledge](https://example.invalid)";

    private static AlertContext External(string source = "billing") =>
        new() { External = new AlertExternalOrigin(source, "queue-depth", 7) };

    private static IEnumerable<string> Strings(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                yield return element.GetString()!;
                break;
            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                    foreach (var text in Strings(item)) yield return text;
                break;
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                    foreach (var text in Strings(property.Value)) yield return text;
                break;
        }
    }

    private static List<string> AllStrings(string payload) => Strings(JsonDocument.Parse(payload).RootElement).ToList();

    /* A MessageCard's summary is the plain-text notification preview, not markdown, so card-text checks leave it out. */
    private static List<string> CardStrings(string payload)
    {
        var root = JsonDocument.Parse(payload).RootElement;
        return root.EnumerateObject().Where(p => p.Name != "summary").SelectMany(p => Strings(p.Value)).ToList();
    }

    /* ---------------- Slack ---------------- */

    [Fact]
    public void Slack_AlertText_CannotFormALiveMentionOrAMaskedLink()
    {
        /* The header is a plain_text object, which Slack never parses for mentions or links, so only the mrkdwn
           strings are checked here. */
        var payload = WebhookAlertService.BuildSlackPayload(
            "External:billing:queue-depth", "SRV1", "warning", "", Branding,
            context: External(), detailText: SlackInjection, displayName: "Queue");

        var strings = AllStrings(payload);

        Assert.DoesNotContain(strings, text => text.Contains("<!channel>", StringComparison.Ordinal));
        Assert.DoesNotContain(strings, text => text.Contains("<https://", StringComparison.Ordinal));
        Assert.Contains(strings, text => text.Contains("&lt;!channel&gt; run &lt;https://example.invalid|the fix&gt;", StringComparison.Ordinal));
    }

    [Fact]
    public void Slack_ServerLabel_IsEscaped_AndTheFixedLabelIsNot()
    {
        var payload = WebhookAlertService.BuildSlackPayload(
            "External:billing:queue-depth", "<!here> A&B", "warning", "", Branding, context: External());

        var strings = AllStrings(payload);

        Assert.Contains("*Server:*\n&lt;!here&gt; A&amp;B", strings);
        Assert.DoesNotContain(strings, text => text.Contains("<!here>", StringComparison.Ordinal));
    }

    [Fact]
    public void Slack_ADetailsFieldAndBody_AreEscaped()
    {
        var context = External();
        context.Details.Add(new AlertDetailItem
        {
            Heading = "Heading <!channel>",
            Fields = { ("Label <@U1>", "value <https://example.invalid|x>") },
        });
        context.Details.Add(new AlertDetailItem { Heading = "Advice", Body = "Investigation: <!everyone>" });

        var payload = WebhookAlertService.BuildSlackPayload(
            "External:billing:queue-depth", "SRV1", "warning", "", Branding, context: context);

        var strings = AllStrings(payload);
        Assert.DoesNotContain(strings, text => text.Contains("<!", StringComparison.Ordinal) || text.Contains("<@", StringComparison.Ordinal)
            || text.Contains("<https", StringComparison.Ordinal));
        Assert.Contains(strings, text => text.Contains("Investigation: &lt;!everyone&gt;", StringComparison.Ordinal));
        Assert.Contains("*Label &lt;@U1&gt;:*\nvalue &lt;https://example.invalid|x&gt;", strings);
    }

    [Fact]
    public void Slack_ABuiltInAlert_RendersAsBefore()
    {
        var payload = WebhookAlertService.BuildSlackPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, detailText: "CPU at 95% for 5 minutes");

        var strings = AllStrings(payload);

        Assert.Contains("*Server:*\nSRV1", strings);
        Assert.Contains("*Current Value:*\n95%", strings);
        Assert.Contains(strings, text => text.EndsWith("CPU at 95% for 5 minutes", StringComparison.Ordinal));
        Assert.DoesNotContain(strings, text => text.Contains("External", StringComparison.Ordinal));
    }

    [Fact]
    public void Slack_AnExternalAlertHeader_NamesTheSource_AndABuiltInHeaderDoesNot()
    {
        var external = JsonDocument.Parse(WebhookAlertService.BuildSlackPayload(
            "External:billing:queue-depth", "PROD01", "warning", "", Branding,
            context: External(), displayName: "Server Unreachable"));
        var builtIn = JsonDocument.Parse(WebhookAlertService.BuildSlackPayload(
            "Server Unreachable", "PROD01", "down", "", Branding, displayName: "Server Unreachable"));

        Assert.EndsWith("[External: billing] Server Unreachable", Header(external), StringComparison.Ordinal);
        Assert.EndsWith(" — Server Unreachable", Header(builtIn), StringComparison.Ordinal);
        Assert.DoesNotContain("External", Header(builtIn), StringComparison.Ordinal);

        static string Header(JsonDocument document) => document.RootElement
            .GetProperty("attachments")[0].GetProperty("blocks")[0].GetProperty("text").GetProperty("text").GetString()!;
    }

    /* ---------------- Teams ---------------- */

    [Fact]
    public void Teams_AlertText_CannotFormALinkOrAMention()
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            "External:billing:queue-depth", "SRV1", "warning", "", Branding,
            context: External(), detailText: TeamsInjection + " <at>everyone</at>", displayName: "Queue " + TeamsInjection);

        var strings = CardStrings(payload);

        Assert.Contains(strings, text => text.Contains(@"\[Acknowledge\](https://example.invalid)", StringComparison.Ordinal));
        Assert.DoesNotContain(strings, text => text.Contains("[Acknowledge](", StringComparison.Ordinal) && !text.Contains(@"\[Acknowledge\](", StringComparison.Ordinal));
        Assert.Contains(strings, text => text.Contains(@"\<at\>everyone\</at\>", StringComparison.Ordinal));
        Assert.DoesNotContain(strings, text => text.Contains("<at>", StringComparison.Ordinal));
    }

    [Fact]
    public void Teams_AServerLabelAndDetailFields_AreEscaped()
    {
        var context = External();
        context.Details.Add(new AlertDetailItem
        {
            Heading = "Detail [x](https://example.invalid)",
            Fields = { ("Label", "value [y](https://example.invalid)") },
        });

        var payload = WebhookAlertService.BuildTeamsPayload(
            "External:billing:queue-depth", "[SRV](https://example.invalid)", "warning", "", Branding, context: context);

        var strings = CardStrings(payload);
        Assert.DoesNotContain(strings, text => text.Contains("](",StringComparison.Ordinal) && !text.Contains(@"\](", StringComparison.Ordinal));
        Assert.Contains(@"value \[y\](https://example.invalid)", strings);
    }

    [Fact]
    public void Teams_ABuiltInAlert_RendersAsBefore()
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, detailText: "CPU at 95% for 5 minutes");

        var root = JsonDocument.Parse(payload).RootElement;
        var section = root.GetProperty("sections")[0];
        var facts = section.GetProperty("facts").EnumerateArray()
            .ToDictionary(f => f.GetProperty("name").GetString()!, f => f.GetProperty("value").GetString()!);

        Assert.Equal("SRV1", facts["Server"]);
        Assert.Equal("95%", facts["Current Value"]);
        Assert.Equal("CPU at 95% for 5 minutes", root.GetProperty("sections")[1].GetProperty("text").GetString());
        Assert.DoesNotContain("External", payload, StringComparison.Ordinal);
    }

    [Fact]
    public void Teams_AnExternalAlertTitle_NamesTheSource_AndABuiltInTitleDoesNot()
    {
        var external = JsonDocument.Parse(WebhookAlertService.BuildTeamsPayload(
            "External:billing:queue-depth", "PROD01", "warning", "", Branding,
            context: External(), displayName: "Server Unreachable")).RootElement;
        var builtIn = JsonDocument.Parse(WebhookAlertService.BuildTeamsPayload(
            "Server Unreachable", "PROD01", "down", "", Branding, displayName: "Server Unreachable")).RootElement;

        Assert.EndsWith(@"\[External: billing\] Server Unreachable", external.GetProperty("sections")[0].GetProperty("activityTitle").GetString(), StringComparison.Ordinal);
        Assert.Contains("[External: billing] Server Unreachable", external.GetProperty("summary").GetString(), StringComparison.Ordinal);
        Assert.DoesNotContain("External", builtIn.GetProperty("summary").GetString(), StringComparison.Ordinal);
    }

    /* ---------------- PagerDuty and email ---------------- */

    [Fact]
    public void PagerDuty_AnExternalAlertSummary_NamesTheSource_AndABuiltInSummaryDoesNot()
    {
        var external = JsonDocument.Parse(WebhookAlertService.BuildPagerDutyPayload(
            "External:billing:queue-depth", "PROD01", "7", "", Branding, "rk",
            context: External(), displayName: "Queue depth")).RootElement;
        var resolve = JsonDocument.Parse(WebhookAlertService.BuildPagerDutyPayload(
            "External:billing:queue-depth Resolved", "PROD01", "", "", Branding, "rk",
            context: External(), displayName: "Queue depth", autoResolve: true, resolveIncident: true)).RootElement;
        var builtIn = JsonDocument.Parse(WebhookAlertService.BuildPagerDutyPayload(
            "Server Unreachable", "PROD01", "down", "", Branding, "rk", displayName: "Server Unreachable")).RootElement;

        Assert.StartsWith("[External: billing] Queue depth on PROD01", Summary(external), StringComparison.Ordinal);
        Assert.StartsWith("[External: billing] Queue depth on PROD01", Summary(resolve), StringComparison.Ordinal);
        Assert.Equal("resolve", resolve.GetProperty("event_action").GetString());
        Assert.StartsWith("Server Unreachable on PROD01", Summary(builtIn), StringComparison.Ordinal);
        Assert.DoesNotContain("External", Summary(builtIn), StringComparison.Ordinal);
        /* The mark is in the title only: the dedup key stays on the metric name. */
        Assert.Equal("PROD01:External:billing:queue-depth", external.GetProperty("dedup_key").GetString());

        static string Summary(JsonElement payload) => payload.GetProperty("payload").GetProperty("summary").GetString()!;
    }

    [Fact]
    public void Email_AnExternalAlertBodyTitle_NamesTheSource_AndABuiltInBodyDoesNot()
    {
        var (externalHtml, externalPlain) = EmailTemplateBuilder.BuildAlertEmail(
            "External:billing:queue-depth", "PROD01", "7", "", 15, Branding,
            context: External(), displayName: "Queue depth");
        var (builtInHtml, builtInPlain) = EmailTemplateBuilder.BuildAlertEmail(
            "Server Unreachable", "PROD01", "down", "", 15, Branding, displayName: "Server Unreachable");

        Assert.Contains("[External: billing] Queue depth", externalHtml, StringComparison.Ordinal);
        Assert.Contains("[External: billing] Queue depth", externalPlain, StringComparison.Ordinal);
        Assert.DoesNotContain("External", builtInHtml, StringComparison.Ordinal);
        Assert.DoesNotContain("External", builtInPlain, StringComparison.Ordinal);
    }

    /* ---------------- The escapes themselves ---------------- */

    [Theory]
    [InlineData("a & b < c > d", "a &amp; b &lt; c &gt; d")]
    [InlineData("&lt;", "&amp;lt;")]
    [InlineData("", "")]
    public void SlackEscape_ReplacesAmpersandAndAngleBrackets(string input, string expected) =>
        Assert.Equal(expected, WebhookAlertService.SlackEscape(input));

    [Theory]
    [InlineData(@"[a](b) <at> *x* _y_ `c`", @"\[a\](b) \<at\> \*x\* \_y\_ \`c\`")]
    [InlineData("~~z~~ ~5 ms", @"\~\~z\~\~ ~5 ms")]
    [InlineData("query_stats PAGEIOLATCH_SH", "query_stats PAGEIOLATCH_SH")]
    [InlineData(@"C:\temp\file.mdf", @"C:\temp\file.mdf")]
    [InlineData(@"\[x\]", @"\\\[x\\\]")]
    [InlineData("", "")]
    public void TeamsEscape_EscapesLinkEmphasisAndMentionCharacters_AndLeavesOrdinaryTextAlone(string input, string expected) =>
        Assert.Equal(expected, WebhookAlertService.TeamsEscape(input));
}
