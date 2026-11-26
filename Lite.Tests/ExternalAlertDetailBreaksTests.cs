using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9576: the detail of an alert another service submitted reaches the drain with its line breaks, and each channel shows them its
/// own way: Slack mrkdwn keeps the line feed, a Teams card's TextBlock needs a doubled one, email HTML turns it into a break tag after
/// the text is encoded, plain-text email and the Generic webhook keep the line feed. Text a source sends is still escaped.
/// </summary>
public class ExternalAlertDetailBreaksTests
{
    private static readonly AlertBranding Branding = new("Performance Monitor Lite", null);

    private const string Metric = "External:billing:report";

    private static AlertContext External() => new() { External = new AlertExternalOrigin("billing", "report", 7) };

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

    [Fact]
    public void Slack_ADetailWithBreaks_KeepsEachLineFeedInItsSection_AndStillEscapesTheText()
    {
        var payload = WebhookAlertService.BuildSlackPayload(
            Metric, "SRV1", "7", "", Branding,
            context: External(), detailText: "Report\nrow one <b>&\n\nrow two", displayName: "Report");

        var section = Assert.Single(AllStrings(payload), text => text.Contains("row one", StringComparison.Ordinal));

        Assert.Contains("Report\nrow one &lt;b&gt;&amp;\n\nrow two", section, StringComparison.Ordinal);
        Assert.DoesNotContain("<b>", section, StringComparison.Ordinal);
    }

    [Fact]
    public void Teams_AnExternalDetailWithBreaks_GetsTheDoubledLineFeedATextBlockNeeds_AndStillEscapesTheText()
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            Metric, "SRV1", "7", "", Branding,
            context: External(), detailText: "Report\nrow one <b>&\nrow two", displayName: "Report");

        var details = Assert.Single(AllStrings(payload), text => text.Contains("row one", StringComparison.Ordinal) && text.Contains("row two", StringComparison.Ordinal));

        Assert.Contains("Report\n\nrow one &lt;b&gt;&amp;\n\nrow two", details, StringComparison.Ordinal);
        Assert.DoesNotContain("<b>", details, StringComparison.Ordinal);
    }

    [Fact]
    public void Teams_ABuiltInAlertsProse_IsLeftExactlyAsItWas()
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            "Server Unreachable", "SRV1", "down", "", Branding, detailText: "first\nsecond");

        Assert.Contains(AllStrings(payload), text => text.Contains("first\nsecond", StringComparison.Ordinal));
        Assert.DoesNotContain(AllStrings(payload), text => text.Contains("first\n\nsecond", StringComparison.Ordinal));
    }

    private static string TeamsDetails(string detailText, AlertContext? context)
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            Metric, "SRV1", "7", "", Branding, context: context, detailText: detailText, displayName: "Report");

        return Assert.Single(TeamsCard.Parse(payload).Section("Details").Texts);
    }

    [Fact]
    public void Teams_AnExternalDetailLineThatStartsWithAMarker_KeepsTheMarkerAsText()
    {
        var details = TeamsDetails("# a\n- b\n+ c\n1. d\n--- e", External());

        Assert.Equal("\\# a\n\n\\- b\n\n\\+ c\n\n1\\. d\n\n\\-\\-\\- e", details);
    }

    [Fact]
    public void Teams_AnIndentedLine_IsSentWithoutItsIndent_AndAMarkerAfterTheIndentIsEscaped()
    {
        var details = TeamsDetails("Report\n  - one\n  ## two\n    + three\n  2) four\n==== five\n-\n#", External());

        Assert.Equal(
            "Report\n\n\\- one\n\n\\#\\# two\n\n\\+ three\n\n2\\) four\n\n\\=\\=\\=\\= five\n\n\\-\n\n\\#",
            details);
    }

    [Fact]
    public void Teams_AMarkerOnlyCountsAtTheStartOfALine()
    {
        const string detail = "a - b\nx # y\n3.5\n#1 priority\n-5 degrees\n+1 added\n--x\n1.5 GB\n2 items\n== done";

        var details = TeamsDetails(detail, External());

        Assert.Equal(detail.Replace("\n", "\n\n", StringComparison.Ordinal), details);
    }

    [Fact]
    public void Teams_AMarkerIsEscapedAfterTheTextIsEscaped_SoTheOtherEscapesStillApply()
    {
        var details = TeamsDetails("- a <b> & [c]\n# *d*", External());

        Assert.Equal("\\- a &lt;b&gt; &amp; \\[c\\]\n\n\\# \\*d\\*", details);
    }

    [Fact]
    public void Teams_ABuiltInAlertsProseWithMarkerLines_IsLeftExactlyAsItWas()
    {
        const string prose = "# a\n- b\n+ c\n1. d\n--- e";

        Assert.Equal(prose, TeamsDetails(prose, context: null));
    }

    [Fact]
    public void Teams_FiftyExternalMarkerLines_StayUnderTheProseCeilingWithEveryMarkerKept()
    {
        var detail = string.Join("\n", Enumerable.Range(1, 50).Select(index => $"{(index % 2 == 0 ? "-" : "#")} row {index}"));

        var details = TeamsDetails(detail, External());

        var paragraphs = details.Split("\n\n");
        Assert.Equal(50, paragraphs.Length);
        Assert.All(paragraphs, paragraph => Assert.Matches(@"^\\[-#] row \d+$", paragraph));
        Assert.True(JsonEncodedText.Encode(details).EncodedUtf8Bytes.Length <= WebhookAlertService.TeamsProseMaxBytes);
    }

    [Fact]
    public void Teams_ALongExternalDetailOfMarkerLines_IsCutWithinTheCeilingWithTheMarkerOfEveryKeptLineIntact()
    {
        var detail = string.Join("\n", Enumerable.Range(1, 50).Select(index => $"- {new string('x', 450)} {index}"));

        var details = TeamsDetails(detail, External());

        Assert.True(JsonEncodedText.Encode(details).EncodedUtf8Bytes.Length <= WebhookAlertService.TeamsProseMaxBytes);
        Assert.EndsWith("_The rest of this detail is cut to fit the Teams size limit._", details, StringComparison.Ordinal);
        var kept = details.Split("\n\n").SkipLast(1).ToList();
        Assert.NotEmpty(kept);
        Assert.All(kept, paragraph => Assert.StartsWith("\\-", paragraph, StringComparison.Ordinal));
    }

    [Fact]
    public void Email_AnExternalDetailWithBreaks_BecomesBreakTagsAfterTheTextIsEncoded()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            Metric, "SRV1", "7", "", 15, Branding,
            context: External(), detailText: "Report\nrow one <b>x</b> & y\nrow two", displayName: "Report");

        Assert.Contains("Report<br>row one &lt;b&gt;x&lt;/b&gt; &amp; y<br>row two", html, StringComparison.Ordinal);
        Assert.DoesNotContain("<b>x</b>", html, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_ABlankLineInAnExternalDetail_StillStartsANewParagraph()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            Metric, "SRV1", "7", "", 15, Branding,
            context: External(), detailText: "Report\npart one\n\npart two", displayName: "Report");

        Assert.Contains("Report<br>part one</p>", html, StringComparison.Ordinal);
        Assert.Contains(">part two</p>", html, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_PlainText_KeepsTheLineFeeds_AndIndentsEveryLine()
    {
        var (_, plain) = EmailTemplateBuilder.BuildAlertEmail(
            Metric, "SRV1", "7", "", 15, Branding,
            context: External(), detailText: "Report\nrow one\nrow two", displayName: "Report");

        Assert.Contains("\r\n  Report\n  row one\n  row two\r\n", plain, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_PlainText_AnExternalDetailKeepsItsOwnSpacesBehindTheIndent_ParagraphByParagraph()
    {
        var (_, plain) = EmailTemplateBuilder.BuildAlertEmail(
            Metric, "SRV1", "7", "", 15, Branding,
            context: External(), detailText: "Report\n  indented   gap\n    deeper\n\nNotes:\n- retry queue is 14", displayName: "Report");

        Assert.Contains("\r\n  Report\n    indented   gap\n      deeper\r\n", plain, StringComparison.Ordinal);
        Assert.Contains("\r\n  Notes:\n  - retry queue is 14\r\n", plain, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_PlainText_AOneLineExternalDetail_ReadsAsItDid()
    {
        var (_, plain) = EmailTemplateBuilder.BuildAlertEmail(
            Metric, "SRV1", "7", "", 15, Branding,
            context: External(), detailText: "Report - 93 percent used", displayName: "Report");

        Assert.Contains("\r\n  Report - 93 percent used\r\n", plain, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_PlainText_ABuiltInAlertsProse_KeepsItsFirstLineIndentOnly()
    {
        var (_, plain) = EmailTemplateBuilder.BuildAlertEmail(
            "Server Unreachable", "SRV1", "down", "", 15, Branding, detailText: "first\n  second");

        Assert.Contains("\r\n  first\n  second\r\n", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("\n    second", plain, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_AnExternalDetailsLeadingSpacesAndSpaceRuns_BecomeNonBreakingSpaces_AfterTheTextIsEncoded()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            Metric, "SRV1", "7", "", 15, Branding,
            context: External(), detailText: "Report\n  indented   gap", displayName: "Report");

        Assert.Contains("Report<br>&nbsp;&nbsp;indented&nbsp;&nbsp;&nbsp;gap</p>", html, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_ASingleSpaceBetweenWords_StaysAPlainSpace_AndMarkupInTheDetailIsStillEncoded()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            Metric, "SRV1", "7", "", 15, Branding,
            context: External(), detailText: "Report\n one <b>x</b> & y  z\nrow two", displayName: "Report");

        Assert.Contains("Report<br>&nbsp;one &lt;b&gt;x&lt;/b&gt; &amp; y&nbsp;&nbsp;z<br>row two</p>", html, StringComparison.Ordinal);
        Assert.DoesNotContain("<b>x</b>", html, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_AnExternalDetailParagraphThatStartsWithSpaces_StartsWithNonBreakingSpaces()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            Metric, "SRV1", "7", "", 15, Branding,
            context: External(), detailText: "Report\n\n    column  one\n    column  two", displayName: "Report");

        Assert.Contains(">&nbsp;&nbsp;&nbsp;&nbsp;column&nbsp;&nbsp;one<br>&nbsp;&nbsp;&nbsp;&nbsp;column&nbsp;&nbsp;two</p>", html, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_AOneLineExternalDetail_ReadsAsItDid()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            Metric, "SRV1", "7", "", 15, Branding,
            context: External(), detailText: "Report - 93 percent used", displayName: "Report");

        Assert.Contains(">Report - 93 percent used</p>", html, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_ABuiltInAlertsProseWithSpaceRuns_GetsNoNonBreakingSpaces()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            "Server Unreachable", "SRV1", "down", "", 15, Branding, detailText: "first  second\n  third");

        Assert.Contains(">first  second\n  third</p>", html, StringComparison.Ordinal);
    }

    [Fact]
    public void Email_ABuiltInAlertsProse_GetsNoBreakTags()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            "Server Unreachable", "SRV1", "down", "", 15, Branding, detailText: "first\nsecond");

        Assert.Contains("first\nsecond", html, StringComparison.Ordinal);
        Assert.DoesNotContain("first<br>second", html, StringComparison.Ordinal);
    }

    [Fact]
    public void Generic_TheDetailTokenCarriesTheRawLineFeeds()
    {
        var payload = WebhookAlertService.BuildGenericPayload(
            Metric, "SRV1", "7", "", Branding, context: External(),
            bodyTemplate: "{\"detail\": \"{{detail}}\"}", detailText: "Report\nrow one\nrow two");

        Assert.Equal("Report\nrow one\nrow two", JsonDocument.Parse(payload).RootElement.GetProperty("detail").GetString());
    }
}
