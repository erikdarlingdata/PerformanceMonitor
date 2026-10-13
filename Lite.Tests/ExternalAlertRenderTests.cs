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
        Assert.Contains(strings, text => text.Contains("&lt;at&gt;everyone&lt;/at&gt;", StringComparison.Ordinal));
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
    [InlineData(@"[a](b) <at> *x* _y_ `c`", @"\[a\](b) &lt;at&gt; \*x\* \_y\_ \`c\`")]
    [InlineData("a & b < c > d", "a &amp; b &lt; c &gt; d")]
    [InlineData("&lt;", "&amp;lt;")]
    [InlineData(@"\<", @"\\&lt;")]
    [InlineData("~~z~~ ~5 ms", @"\~\~z\~\~ ~5 ms")]
    [InlineData("query_stats PAGEIOLATCH_SH", "query_stats PAGEIOLATCH_SH")]
    [InlineData(@"C:\temp\file.mdf", @"C:\temp\file.mdf")]
    [InlineData(@"\[x\]", @"\\\[x\\\]")]
    [InlineData("", "")]
    public void TeamsEscape_EscapesLinkEmphasisAndMentionCharacters_AndLeavesOrdinaryTextAlone(string input, string expected) =>
        Assert.Equal(expected, WebhookAlertService.TeamsEscape(input));

    /* ---------------- Header length, record code spans and escaped cuts ---------------- */

    private const string LongestSource = "abcdefghijklmnopqrstuvwxyz01234";

    private static string SlackHeaderText(string payload) => JsonDocument.Parse(payload).RootElement
        .GetProperty("attachments")[0].GetProperty("blocks")[0].GetProperty("text").GetProperty("text").GetString()!;

    [Theory]
    [InlineData("T")]
    [InlineData("\U0001F600")]
    public void Slack_AHeaderIsHeldToSlacksLimit_OnAWholeCharacter_WithAnEllipsis(string unit)
    {
        Assert.Equal(31, LongestSource.Length);
        var title = string.Concat(Enumerable.Repeat(unit, 200 / unit.Length));
        Assert.Equal(200, title.Length);

        var header = SlackHeaderText(WebhookAlertService.BuildSlackPayload(
            "External:" + LongestSource + ":queue-depth", "SRV1", "warning", "", Branding,
            context: External(LongestSource), displayName: title));

        Assert.True(header.Length <= 150, $"{header.Length} characters");
        Assert.EndsWith("...", header, StringComparison.Ordinal);
        Assert.Contains("[External: " + LongestSource + "] ", header, StringComparison.Ordinal);
        for (var i = 0; i < header.Length; i++)
        {
            Assert.True(!char.IsSurrogate(header[i]) || (char.IsHighSurrogate(header[i]) && i + 1 < header.Length && char.IsLowSurrogate(header[i + 1]))
                || (char.IsLowSurrogate(header[i]) && i > 0 && char.IsHighSurrogate(header[i - 1])), $"split pair at {i}");
        }
    }

    [Fact]
    public void Slack_AHeaderInsideTheLimit_IsUnchanged()
    {
        var header = SlackHeaderText(WebhookAlertService.BuildSlackPayload(
            "External:billing:queue-depth", "SRV1", "warning", "", Branding,
            context: External(), displayName: "Queue depth"));

        Assert.EndsWith("[External: billing] Queue depth", header, StringComparison.Ordinal);
        Assert.DoesNotContain("...", header, StringComparison.Ordinal);
    }

    private static AlertContext WithRecord(string summary, params string[] texts)
    {
        var context = External();
        var item = new AlertDetailItem { Heading = "Top Queries" };
        item.Records.Add(new AlertDetailRecord(1, summary, texts.Select(t => ("Query Text", t)).ToList()));
        context.Details.Add(item);
        return context;
    }

    private static string TeamsRecordValue(AlertContext context)
    {
        using var doc = JsonDocument.Parse(WebhookAlertService.BuildTeamsPayload(
            "External:billing:queue-depth", "SRV1", "warning", "", Branding, context: context));
        var section = Assert.Single(doc.RootElement.GetProperty("sections").EnumerateArray(),
            s => s.TryGetProperty("activityTitle", out var t) && t.GetString() == "Top Queries");
        return Assert.Single(section.GetProperty("facts").EnumerateArray()).GetProperty("value").GetString()!;
    }

    [Fact]
    public void Teams_ARecordTextWithABlankLine_StaysInOneCodeSpan()
    {
        var text = "SELECT 1 /*\n\n[Open the runbook](https://example.invalid)\r\n\r\n*/ `x`\r- y\u0085z";

        var value = TeamsRecordValue(WithRecord("", text));

        Assert.Equal("`SELECT 1 /* [Open the runbook](https://example.invalid) */ 'x' - y z`", value);
        Assert.DoesNotContain('\n', value);
        Assert.DoesNotContain('\r', value);
        Assert.Equal(2, value.Count(c => c == '`'));
    }

    [Fact]
    public void Teams_ARecordWithTwoTexts_KeepsEachInItsOwnOneLineSpan()
    {
        var value = TeamsRecordValue(WithRecord("", "a\n\nb", "c`d\n"));

        Assert.Equal("_Query Text_  \n`a b`  \n_Query Text_  \n`c'd `", value);
    }

    private static bool HasHalfEntity(string text) =>
        System.Text.RegularExpressions.Regex.IsMatch(text, "&(?!(amp|lt|gt);)");

    [Fact]
    public void Slack_ARecordSummary_IsCutBeforeItIsEscaped()
    {
        var summary = new string('&', 1200);
        var strings = AllStrings(WebhookAlertService.BuildSlackPayload(
            "External:billing:queue-depth", "SRV1", "warning", "", Branding, context: WithRecord(summary, "q")));

        var section = Assert.Single(strings, s => s.Contains("*#1 · ", StringComparison.Ordinal));
        Assert.Contains("*#1 · " + string.Concat(Enumerable.Repeat("&amp;", 200)) + "...*", section, StringComparison.Ordinal);
        Assert.False(HasHalfEntity(section));
        Assert.True(section.Length <= 3000);
    }

    [Fact]
    public void Slack_AFieldValueAndARecordText_AreCutBeforeTheyAreEscaped_AndCountRawCharacters()
    {
        var value = new string('&', 1500);
        var context = External();
        context.Details.Add(new AlertDetailItem { Heading = "Fields", Fields = { ("Label", value) } });
        context.Details.Add(WithRecord("", value).Details[0]);

        var strings = AllStrings(WebhookAlertService.BuildSlackPayload(
            "External:billing:queue-depth", "SRV1", "warning", "", Branding, context: context));

        var field = Assert.Single(strings, s => s.StartsWith("*Label:*", StringComparison.Ordinal));
        var recordText = Assert.Single(strings, s => s.Contains("*#1", StringComparison.Ordinal));
        foreach (var text in new[] { field, recordText })
        {
            Assert.False(HasHalfEntity(text), text);
            var match = System.Text.RegularExpressions.Regex.Match(text, @"\.\.\. \((\d[\d,]*) more characters");
            Assert.True(match.Success, text);
            var omitted = int.Parse(match.Groups[1].Value.Replace(",", ""), System.Globalization.CultureInfo.InvariantCulture);
            var kept = System.Text.RegularExpressions.Regex.Matches(text, "&amp;").Count;
            Assert.Equal(1500, kept + omitted);
        }

        Assert.True(field.Length <= 2000, $"{field.Length} characters");
    }

    [Fact]
    public void Slack_AnAmpersandHeavyHeading_StaysInsideTheFieldLimit()
    {
        var context = External();
        context.Details.Add(new AlertDetailItem { Heading = new string('&', 450), Fields = { ("Label", "v") } });

        var strings = AllStrings(WebhookAlertService.BuildSlackPayload(
            "External:billing:queue-depth", "SRV1", "warning", "", Branding, context: context));

        var heading = Assert.Single(strings, s => s.StartsWith("*&amp;&amp;", StringComparison.Ordinal));
        Assert.True(heading.Length <= 2000, $"{heading.Length} characters");
        Assert.False(HasHalfEntity(heading));
    }
}
